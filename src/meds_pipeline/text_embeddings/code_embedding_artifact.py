"""Encode vocabularies and save, load, and validate code-embedding artifacts."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

from .base import TextEncoder

from meds_pipeline.text_embeddings.embed_text import (
    MISSING_DESCRIPTION_SENTINEL,
    PAD_CODE,
    PAD_ID,
    TEXT_TEMPLATE,
    TEXT_TEMPLATE_VERSION,
    UNK_CODE,
    UNK_EMBED_TEXT,
    UNK_ID,
    build_embed_text,
)

ARTIFACT_VERSION = 1
ARTIFACT_KIND = "code_embeddings"
ARTIFACT_SCHEMA_VERSION = "code_embeddings.v1"
LEGACY_SCHEMA_VERSION = "legacy_biolord_v1"
DEFAULT_ARTIFACT_NAME = "dx_proc"
MANIFEST_FILENAME = "manifest.json"
VOCAB_FILENAME = "code_vocab.parquet"
EMBEDDINGS_FILENAME = "embeddings.npy"

REQUIRED_VOCAB_COLUMNS = (
    "code_id",
    "code",
    "event_type",
    "code_system",
    "description",
    "embed_text",
    "has_description",
)


@dataclass(frozen=True)
class LoadedCodeEmbeddingArtifact:
    """Validated code-embedding artifact contents."""

    directory: Path
    vocab: pd.DataFrame
    embeddings: np.ndarray
    manifest: dict[str, Any]


def encode_vocab_embeddings(
    embed_texts: Sequence[str],
    encoder: TextEncoder,
    *,
    pad_id: int = PAD_ID,
    batch_size: int = 64,
    show_progress_bar: bool = True,
) -> np.ndarray:
    """Validate the encoder contract and apply the vocabulary's PAD convention."""
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")
    matrix = np.asarray(
        encoder.encode(
            embed_texts, batch_size=batch_size, show_progress_bar=show_progress_bar
        ),
        dtype=np.float32,
    )
    expected_shape = (len(embed_texts), encoder.spec.embed_dim)
    if encoder.spec.embed_dim <= 0 or matrix.shape != expected_shape:
        raise ValueError(
            f"Encoder output shape {matrix.shape} != expected {expected_shape}"
        )
    if not np.isfinite(matrix).all():
        raise ValueError("Encoder output contains NaN or Inf")
    matrix = matrix.copy()
    if 0 <= pad_id < len(matrix):
        matrix[pad_id] = 0.0
    return matrix


def artifact_directory(
    output_root: str | Path,
    artifact_name: str = DEFAULT_ARTIFACT_NAME,
) -> Path:
    """Return the stable publish path for a code-embedding artifact.

    The directory name is human-readable (default: ``dx_proc``). The content
    fingerprint lives in ``manifest.json`` as ``vocab_hash``.
    """
    return (
        Path(output_root).expanduser().resolve()
        / ARTIFACT_KIND
        / f"v{ARTIFACT_VERSION}"
        / str(artifact_name)
    )


def embed_text_hash(embed_texts: Sequence[str]) -> str:
    """SHA256 over the joined ``embed_text`` rows in ``code_id`` order."""
    joined = "\n".join(str(text) for text in embed_texts)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def embedding_hash(embeddings: np.ndarray) -> str:
    """SHA256 over the raw float32 embedding bytes (C-contiguous)."""
    array = np.ascontiguousarray(np.asarray(embeddings, dtype=np.float32))
    return hashlib.sha256(array.tobytes()).hexdigest()


def build_manifest(
    *,
    source: str,
    meds_data_path: str | Path,
    meds_data_fingerprint: str,
    vocab_hash: str,
    n_codes: int,
    vocab_size: int,
    encoder_kind: str,
    encoder_model: str,
    encoder_revision: str | None,
    embed_dim: int,
    embed_text_hash: str,
    embedding_hash: str,
    normalize_embeddings: bool = True,
    pooling: str | None = None,
    event_types: Sequence[str] = ("diagnosis", "procedures"),
    creation_commit: str = "unknown",
    generated_at: str | None = None,
    sentence_transformers_version: str | None = None,
    torch_version: str | None = None,
    device: str | None = None,
) -> dict[str, Any]:
    return {
        "artifact_schema_version": ARTIFACT_SCHEMA_VERSION,
        "artifact_version": ARTIFACT_VERSION,
        "artifact_kind": ARTIFACT_KIND,
        "source": str(source),
        "event_types": list(event_types),
        "meds_data_path": str(meds_data_path),
        "meds_data_fingerprint": str(meds_data_fingerprint),
        "vocab_hash": str(vocab_hash),
        "embed_text_hash": str(embed_text_hash),
        "embedding_hash": str(embedding_hash),
        "n_codes": int(n_codes),
        "vocab_size": int(vocab_size),
        "pad_id": PAD_ID,
        "unk_id": UNK_ID,
        "encoder_kind": str(encoder_kind),
        "encoder_model": str(encoder_model),
        "encoder_revision": None if encoder_revision is None else str(encoder_revision),
        "embed_dim": int(embed_dim),
        "normalize_embeddings": bool(normalize_embeddings),
        "pooling": pooling,
        "text_template_version": TEXT_TEMPLATE_VERSION,
        "text_template": TEXT_TEMPLATE,
        "missing_description_sentinel": MISSING_DESCRIPTION_SENTINEL,
        "unk_embed_text": UNK_EMBED_TEXT,
        "pad_embedding_policy": "zeros",
        "sentence_transformers_version": sentence_transformers_version,
        "torch_version": torch_version,
        "device": device,
        "generated_at": generated_at
        or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "creation_commit": str(creation_commit),
    }


def normalize_manifest(manifest: Mapping[str, Any]) -> dict[str, Any]:
    """Return a manifest with encoder_* fields, upgrading legacy biolord_* keys.

    Legacy artifacts (written before the pluggable-encoder refactor) used
    ``biolord_model`` / ``biolord_revision`` and had no schema version. Those
    are mapped onto the ``sentence_transformers`` backend and tagged with
    ``legacy_biolord_v1`` so downstream code only deals with one shape.
    """
    out = dict(manifest)
    if "encoder_kind" in out and "encoder_model" in out:
        out.setdefault("artifact_schema_version", ARTIFACT_SCHEMA_VERSION)
        return out

    if "biolord_model" in out:
        out["artifact_schema_version"] = LEGACY_SCHEMA_VERSION
        out["encoder_kind"] = "sentence_transformers"
        out["encoder_model"] = out.get("biolord_model")
        out["encoder_revision"] = out.get("biolord_revision")
        out.setdefault("pooling", None)
    return out


def validate_artifact(
    vocab: pd.DataFrame,
    embeddings: np.ndarray,
    manifest: Mapping[str, Any],
) -> None:
    """Raise ValueError if vocab / embeddings / manifest are inconsistent.

    Accepts both the current ``code_embeddings.v1`` schema and legacy
    ``biolord_*`` manifests (the latter are normalized before validation).
    """
    if not isinstance(manifest, Mapping):
        raise ValueError("manifest must be a mapping")

    manifest = normalize_manifest(manifest)

    required_manifest = {
        "artifact_kind",
        "source",
        "vocab_hash",
        "n_codes",
        "vocab_size",
        "pad_id",
        "unk_id",
        "encoder_kind",
        "encoder_model",
        "embed_dim",
        "text_template_version",
        "unk_embed_text",
    }
    missing_manifest = sorted(required_manifest - set(manifest))
    if missing_manifest:
        raise ValueError(f"manifest missing fields: {missing_manifest}")

    missing_cols = [c for c in REQUIRED_VOCAB_COLUMNS if c not in vocab.columns]
    if missing_cols:
        raise ValueError(f"code_vocab missing columns: {missing_cols}")

    vocab_size = int(manifest["vocab_size"])
    embed_dim = int(manifest["embed_dim"])
    if len(vocab) != vocab_size:
        raise ValueError(
            f"vocab rows ({len(vocab)}) != manifest.vocab_size ({vocab_size})"
        )
    if embeddings.ndim != 2:
        raise ValueError(f"embeddings must be 2-D, got shape {embeddings.shape}")
    if embeddings.shape[0] != vocab_size:
        raise ValueError(
            f"embeddings rows ({embeddings.shape[0]}) != vocab_size ({vocab_size})"
        )
    if embeddings.shape[1] != embed_dim:
        raise ValueError(
            f"embeddings dim ({embeddings.shape[1]}) != manifest.embed_dim ({embed_dim})"
        )
    if embeddings.dtype != np.float32:
        raise ValueError(f"embeddings dtype must be float32, got {embeddings.dtype}")

    code_ids = vocab["code_id"].to_numpy()
    expected_ids = np.arange(vocab_size, dtype=code_ids.dtype)
    if not np.array_equal(code_ids, expected_ids):
        raise ValueError("code_id must be contiguous 0..vocab_size-1 in row order")

    real = vocab.loc[vocab["code_id"] >= UNK_ID + 1, "code"].astype(str)
    if real.duplicated().any():
        raise ValueError("duplicate codes among real vocabulary rows")

    if int(manifest["pad_id"]) != PAD_ID or int(vocab.iloc[0]["code_id"]) != PAD_ID:
        raise ValueError("PAD must occupy code_id=0")
    if str(vocab.iloc[0]["code"]) != PAD_CODE:
        raise ValueError(f"PAD code must be {PAD_CODE!r}")
    if not np.allclose(embeddings[0], 0.0):
        raise ValueError("PAD embedding (row 0) must be all zeros")

    if int(manifest["unk_id"]) != UNK_ID or int(vocab.iloc[1]["code_id"]) != UNK_ID:
        raise ValueError("UNK must occupy code_id=1")
    if str(vocab.iloc[1]["code"]) != UNK_CODE:
        raise ValueError(f"UNK code must be {UNK_CODE!r}")
    if str(vocab.iloc[1]["embed_text"]) != str(
        manifest.get("unk_embed_text", UNK_EMBED_TEXT)
    ):
        raise ValueError("UNK embed_text does not match manifest")

    if int(manifest["n_codes"]) != vocab_size - 2:
        raise ValueError(
            f"manifest.n_codes ({manifest['n_codes']}) != vocab_size-2 ({vocab_size - 2})"
        )

    if not np.isfinite(embeddings).all():
        raise ValueError("embeddings contain NaN or Inf")

    # Spot-check embed_text reconstruction for real rows.
    for row in vocab.loc[vocab["code_id"] >= UNK_ID + 1].itertuples(index=False):
        description = None if pd.isna(row.description) else str(row.description)
        expected = build_embed_text(
            event_type=str(row.event_type),
            code_system=str(row.code_system),
            code=str(row.code),
            description=description,
        )
        if str(row.embed_text) != expected:
            raise ValueError(
                f"embed_text mismatch for code={row.code!r}: "
                f"stored={row.embed_text!r} expected={expected!r}"
            )

    # Verify content hashes when present (legacy artifacts may omit them).
    embed_texts = vocab["embed_text"].fillna("").astype(str).tolist()
    if manifest.get("embed_text_hash"):
        expected_text_hash = embed_text_hash(embed_texts)
        if str(manifest["embed_text_hash"]) != expected_text_hash:
            raise ValueError(
                "embed_text_hash mismatch: manifest "
                f"{manifest['embed_text_hash']!r} != computed {expected_text_hash!r}"
            )
    if manifest.get("embedding_hash"):
        expected_emb_hash = embedding_hash(embeddings)
        if str(manifest["embedding_hash"]) != expected_emb_hash:
            raise ValueError(
                "embedding_hash mismatch: manifest "
                f"{manifest['embedding_hash']!r} != computed {expected_emb_hash!r}"
            )


def save_artifact_files(
    directory: str | Path,
    *,
    vocab: pd.DataFrame,
    embeddings: np.ndarray,
    manifest: Mapping[str, Any],
) -> Path:
    """Write artifact files into ``directory`` (must not already exist as complete)."""
    target = Path(directory)
    target.mkdir(parents=True, exist_ok=True)
    validate_artifact(vocab, embeddings, manifest)

    vocab_path = target / VOCAB_FILENAME
    emb_path = target / EMBEDDINGS_FILENAME
    manifest_path = target / MANIFEST_FILENAME

    vocab.to_parquet(vocab_path, index=False)
    np.save(emb_path, np.asarray(embeddings, dtype=np.float32))
    manifest_path.write_text(
        json.dumps(dict(manifest), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return target


def publish_artifact(
    output_root: str | Path,
    *,
    vocab: pd.DataFrame,
    embeddings: np.ndarray,
    manifest: Mapping[str, Any],
    artifact_name: str = DEFAULT_ARTIFACT_NAME,
    overwrite: bool = False,
) -> Path:
    """Atomically publish an artifact under ``code_embeddings/v1/{artifact_name}/``.

    Refuses to replace an existing artifact directory unless ``overwrite`` is
    set, so different encoders do not silently clobber each other's output.
    """
    target = artifact_directory(output_root, artifact_name)
    target.parent.mkdir(parents=True, exist_ok=True)

    if target.exists() and not overwrite:
        raise FileExistsError(
            f"Artifact already exists at {target}. Pass overwrite=True "
            "(CLI: --overwrite) to replace it, or use a different --artifact-name."
        )

    temporary = Path(
        tempfile.mkdtemp(prefix=f".{target.name}.tmp-", dir=str(target.parent))
    )
    try:
        save_artifact_files(
            temporary,
            vocab=vocab,
            embeddings=embeddings,
            manifest=manifest,
        )
        if target.exists():
            shutil.rmtree(target)
        os.replace(temporary, target)
        return target
    except Exception:
        if temporary.exists():
            shutil.rmtree(temporary, ignore_errors=True)
        raise


def load_artifact(path: str | Path) -> LoadedCodeEmbeddingArtifact:
    """Load and validate an artifact directory."""
    directory = Path(path).expanduser().resolve()
    if not directory.is_dir():
        raise FileNotFoundError(f"Artifact directory not found: {directory}")

    manifest_path = directory / MANIFEST_FILENAME
    vocab_path = directory / VOCAB_FILENAME
    emb_path = directory / EMBEDDINGS_FILENAME
    for required in (manifest_path, vocab_path, emb_path):
        if not required.is_file():
            raise FileNotFoundError(f"Missing artifact file: {required}")

    raw_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest = normalize_manifest(raw_manifest)
    vocab = pd.read_parquet(vocab_path)
    embeddings = np.load(emb_path)
    validate_artifact(vocab, embeddings, manifest)
    return LoadedCodeEmbeddingArtifact(
        directory=directory,
        vocab=vocab,
        embeddings=embeddings,
        manifest=manifest,
    )


def git_commit_or_unknown(repo_root: str | Path | None = None) -> str:
    """Best-effort git SHA for provenance; returns ``unknown`` on failure."""
    try:
        import subprocess

        cwd = str(repo_root) if repo_root is not None else None
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=cwd,
            check=True,
            capture_output=True,
            text=True,
        )
        return result.stdout.strip() or "unknown"
    except Exception:
        return "unknown"
