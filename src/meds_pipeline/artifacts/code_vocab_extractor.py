"""Extract a deterministic diagnosis/procedure code vocabulary from MEDS parquet."""

from __future__ import annotations

import hashlib
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Sequence

import pandas as pd

from meds_pipeline.artifacts.embed_text import (
    PAD_CODE,
    PAD_ID,
    UNK_CODE,
    UNK_EMBED_TEXT,
    UNK_ID,
    build_embed_text,
    resolve_code_system,
)
from meds_pipeline.etl.code_descriptions import clean_description

TARGET_EVENT_TYPES = ("diagnosis", "procedures")
SCAN_COLUMNS = ("event_type", "code", "code_system", "source_table", "value_text")
LAB_EVENT_TYPE = "lab"

_LAB_RAW_SEPARATOR_RE = re.compile(r"\s*\|\s*raw\s*=", flags=re.IGNORECASE)
_RESULT_NUMERIC_RE = re.compile(
    r"^\s*(?:<=|>=|<|>)?\s*[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:\s*[%A-Za-z0-9./^_-]+)?\s*$"
)
_COMMON_RESULT_VALUES = {
    "abnormal",
    "cancelled",
    "canceled",
    "detected",
    "invalid",
    "negative",
    "normal",
    "not detected",
    "pending",
    "positive",
    "see comment",
    "see comments",
    "trace",
}


@dataclass(frozen=True)
class CodeVocabResult:
    """Vocabulary table plus fingerprints for artifact provenance."""

    vocab: pd.DataFrame
    vocab_hash: str
    meds_data_fingerprint: str
    n_codes: int
    vocab_size: int


def list_meds_bucket_files(meds_data: str | Path) -> list[Path]:
    """Return sorted ``bucket*.parquet`` paths under a patient-bucketed dataset."""
    root = Path(meds_data)
    if not root.exists():
        raise FileNotFoundError(f"MEDS data path does not exist: {root}")
    if root.is_file():
        if root.suffix != ".parquet":
            raise ValueError(f"Expected a parquet file or bucket directory: {root}")
        return [root]
    files = sorted(root.glob("bucket*.parquet"))
    if not files:
        files = sorted(root.glob("**/bucket*.parquet"))
    if not files:
        raise FileNotFoundError(f"No bucket*.parquet files found under {root}")
    return files


def meds_data_fingerprint(paths: Sequence[Path]) -> str:
    """Hash path names, sizes, and mtimes for reproducibility checks."""
    digest = hashlib.sha256()
    for path in sorted(paths, key=lambda p: str(p)):
        stat = path.stat()
        payload = f"{path.name}\t{stat.st_size}\t{int(stat.st_mtime_ns)}\n"
        digest.update(payload.encode("utf-8"))
    return digest.hexdigest()


def vocab_hash_from_codes(codes: Iterable[str]) -> str:
    ordered = sorted(str(code) for code in codes)
    return hashlib.sha256("\n".join(ordered).encode("utf-8")).hexdigest()


def clean_value_text_for_embedding(event_type: object, value_text: object) -> str | None:
    """Return the description text used for embeddings, with lab raw values removed."""
    if str(event_type).strip().lower() == LAB_EVENT_TYPE:
        return clean_lab_value_text(value_text)
    return clean_description(value_text)


def clean_lab_value_text(value_text: object) -> str | None:
    """Return only the lab test identity from old or new lab value_text formats."""
    if pd.isna(value_text):
        return None

    text = " ".join(str(value_text).split()).strip()
    if not text:
        return None

    lower = text.lower()
    for prefix in ("test=", "label="):
        if lower.startswith(prefix):
            lab_name = text[len(prefix) :].strip()
            raw_match = _LAB_RAW_SEPARATOR_RE.search(lab_name)
            if raw_match:
                lab_name = lab_name[: raw_match.start()].strip()
            return clean_description(lab_name)

    if _LAB_RAW_SEPARATOR_RE.search(text):
        return None

    cleaned = clean_description(text)
    if not cleaned or _looks_like_lab_result(cleaned):
        return None
    return cleaned


def _looks_like_lab_result(text: str) -> bool:
    normalized = " ".join(str(text).split()).strip()
    if not normalized:
        return True

    lower = normalized.lower()
    if lower in _COMMON_RESULT_VALUES:
        return True
    if _RESULT_NUMERIC_RE.match(normalized):
        return True
    if set(normalized) <= {"_"}:
        return True
    if lower.startswith("hold.") or ("hold" in lower and "discard" in lower):
        return True
    return False


def extract_code_vocab(
    meds_data: str | Path,
    *,
    event_types: Sequence[str] = TARGET_EVENT_TYPES,
) -> CodeVocabResult:
    """Scan MEDS parquet and return a PAD/UNK-prefixed code vocabulary table."""
    paths = list_meds_bucket_files(meds_data)
    fingerprint = meds_data_fingerprint(paths)
    allowed = {str(et) for et in event_types}

    n_events: Counter[str] = Counter()
    event_type_counts: dict[str, Counter[str]] = defaultdict(Counter)
    code_systems: dict[str, set[str]] = defaultdict(set)
    descriptions: dict[str, set[str]] = defaultdict(set)
    source_tables: dict[str, set[str]] = defaultdict(set)

    for path in paths:
        schema_columns = set(_parquet_columns(path))
        columns = [c for c in SCAN_COLUMNS if c in schema_columns]
        if "code" not in columns or "event_type" not in columns:
            continue
        df = pd.read_parquet(path, columns=columns)
        mask = df["event_type"].astype(str).isin(allowed)
        if not mask.any():
            continue
        sub = df.loc[mask, columns].copy()
        sub["code"] = sub["code"].astype("string").str.strip()
        sub = sub[sub["code"].notna() & sub["code"].ne("") & ~sub["code"].isin(["nan", "None", "<NA>"])]
        if sub.empty:
            continue

        sub["event_type"] = sub["event_type"].astype("string")
        if "code_system" not in sub.columns:
            sub["code_system"] = pd.Series(pd.NA, index=sub.index, dtype="string")
        if "source_table" not in sub.columns:
            sub["source_table"] = pd.Series(pd.NA, index=sub.index, dtype="string")
        if "value_text" not in sub.columns:
            sub["value_text"] = pd.Series(pd.NA, index=sub.index, dtype="string")

        n_events.update(sub["code"].astype(str).tolist())

        et_pairs = sub.groupby(["code", "event_type"], sort=False).size().reset_index(name="count")
        for row in et_pairs.itertuples(index=False):
            event_type_counts[str(row.code)][str(row.event_type)] += int(row.count)

        # Resolve code_system once per unique (code, raw_system) pair.
        system_pairs = sub[["code", "code_system"]].drop_duplicates()
        for row in system_pairs.itertuples(index=False):
            code_systems[str(row.code)].add(
                resolve_code_system(str(row.code), row.code_system)
            )

        src = sub[["code", "source_table"]].dropna(subset=["source_table"]).drop_duplicates()
        for row in src.itertuples(index=False):
            text = str(row.source_table).strip()
            if text and text.lower() not in {"nan", "none", "<na>"}:
                source_tables[str(row.code)].add(text)

        # Unique value_text per code to avoid cleaning millions of duplicates.
        desc_pairs = sub[["code", "event_type", "value_text"]].drop_duplicates()
        for row in desc_pairs.itertuples(index=False):
            cleaned = clean_value_text_for_embedding(row.event_type, row.value_text)
            if cleaned:
                descriptions[str(row.code)].add(cleaned)

    real_codes = sorted(n_events.keys())
    vhash = vocab_hash_from_codes(real_codes)

    rows: list[dict] = [
        {
            "code_id": PAD_ID,
            "code": PAD_CODE,
            "event_type": "pad",
            "code_system": "pad",
            "description": pd.NA,
            "embed_text": "",
            "has_description": False,
            "source_tables": "",
            "n_events": 0,
            "n_description_variants": 0,
        },
        {
            "code_id": UNK_ID,
            "code": UNK_CODE,
            "event_type": "unknown",
            "code_system": "unknown",
            "description": pd.NA,
            "embed_text": UNK_EMBED_TEXT,
            "has_description": False,
            "source_tables": "",
            "n_events": 0,
            "n_description_variants": 0,
        },
    ]

    next_id = UNK_ID + 1
    for code in real_codes:
        et_counter = event_type_counts[code]
        event_type = sorted(et_counter.items(), key=lambda item: (-item[1], item[0]))[0][0]
        systems = sorted(code_systems[code])
        code_system = systems[0] if systems else resolve_code_system(code, None)
        desc_variants = sorted(descriptions[code])
        description = desc_variants[0] if desc_variants else None
        embed_text = build_embed_text(
            event_type=event_type,
            code_system=code_system,
            code=code,
            description=description,
        )
        rows.append(
            {
                "code_id": next_id,
                "code": code,
                "event_type": event_type,
                "code_system": code_system,
                "description": description if description is not None else pd.NA,
                "embed_text": embed_text,
                "has_description": description is not None,
                "source_tables": ";".join(sorted(source_tables[code])),
                "n_events": int(n_events[code]),
                "n_description_variants": len(desc_variants),
            }
        )
        next_id += 1

    vocab = pd.DataFrame(rows)
    vocab["code_id"] = vocab["code_id"].astype("int64")
    vocab["code"] = vocab["code"].astype("string")
    vocab["event_type"] = vocab["event_type"].astype("string")
    vocab["code_system"] = vocab["code_system"].astype("string")
    vocab["description"] = vocab["description"].astype("string")
    vocab["embed_text"] = vocab["embed_text"].astype("string")
    vocab["has_description"] = vocab["has_description"].astype(bool)
    vocab["source_tables"] = vocab["source_tables"].astype("string")
    vocab["n_events"] = vocab["n_events"].astype("int64")
    vocab["n_description_variants"] = vocab["n_description_variants"].astype("int64")

    return CodeVocabResult(
        vocab=vocab,
        vocab_hash=vhash,
        meds_data_fingerprint=fingerprint,
        n_codes=len(real_codes),
        vocab_size=len(vocab),
    )


def _parquet_columns(path: Path) -> list[str]:
    import pyarrow.parquet as pq

    return list(pq.read_schema(path).names)
