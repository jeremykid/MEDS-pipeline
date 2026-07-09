from __future__ import annotations

import logging
from pathlib import Path
from typing import Iterable, Mapping, Sequence

import pandas as pd

logger = logging.getLogger(__name__)

SOURCE_ROOT = Path(__file__).resolve().parents[2]
RESOURCE_DIR = SOURCE_ROOT / "resource"

UNKNOWN_DESCRIPTION_VALUES = {
    "",
    "unknown",
    "unknown code",
    "not found",
    "nan",
    "none",
    "<na>",
}


def default_ahs_codebook_paths() -> dict[str, Path]:
    return {
        "icd10ca": RESOURCE_DIR / "ICD_Code_Eng_Desc_10CA2026_V1_0.txt",
        "cci": RESOURCE_DIR / "CCI_Code_Eng_Desc_CCI2026_V1_0.txt",
    }


def clean_description(value: object) -> str | None:
    if pd.isna(value):
        return None

    text = " ".join(str(value).split()).strip()
    if text.lower() in UNKNOWN_DESCRIPTION_VALUES:
        return None

    return text.replace(" | ", "; ")


def append_description_to_value_text(
    value_text: pd.Series | Sequence[object] | None,
    descriptions: pd.Series | Sequence[object] | None,
) -> pd.Series:
    if descriptions is None:
        if value_text is None:
            return pd.Series(dtype="string")
        return pd.Series(value_text).astype("string")

    desc = pd.Series(descriptions).astype("string")
    desc = desc.str.replace(r"\s+", " ", regex=True).str.strip()
    desc = desc.mask(desc.str.lower().isin(UNKNOWN_DESCRIPTION_VALUES))
    desc_part = "desc=" + desc

    if value_text is None:
        base = pd.Series([pd.NA] * len(desc_part), index=desc_part.index, dtype="string")
    else:
        base = pd.Series(value_text, index=desc_part.index).astype("string")
        base = base.str.replace(r"\s+", " ", regex=True).str.strip()
        base = base.mask(base.str.lower().isin(UNKNOWN_DESCRIPTION_VALUES))

    out = base.copy()
    has_base = out.notna()
    has_desc = desc_part.notna()
    out.loc[has_base & has_desc] = out.loc[has_base & has_desc] + " | " + desc_part.loc[has_base & has_desc]
    out.loc[~has_base & has_desc] = desc_part.loc[~has_base & has_desc]
    return out.astype("string")


def descriptions_to_value_text(
    descriptions: pd.Series | Sequence[object] | None,
    *,
    index: pd.Index | None = None,
) -> pd.Series:
    """Return cleaned code descriptions for MEDS value_text, with missing descriptions as NA."""
    if descriptions is None:
        if index is not None:
            return pd.Series([pd.NA] * len(index), index=index, dtype="string")
        return pd.Series(dtype="string")

    desc = pd.Series(descriptions).map(clean_description)
    return pd.Series(desc, index=desc.index, dtype="string")


def lookup_descriptions(
    codes: Iterable[object],
    mapper: object | None,
    *,
    default: str = "Unknown",
) -> pd.Series:
    code_series = pd.Series(codes)
    if mapper is None or code_series.empty:
        return pd.Series([pd.NA] * len(code_series), index=code_series.index, dtype="string")

    normalized = code_series.astype("string").str.strip()
    unique_codes = normalized.dropna().unique().tolist()
    description_map = {
        code: clean_description(mapper.get_description(code, default=default))
        for code in unique_codes
        if code
    }
    return normalized.map(description_map).astype("string")


class _MIMICRegistryLookup:
    def __init__(self, registry: object, fallback_mapper: str) -> None:
        self.registry = registry
        self.fallback_mapper = fallback_mapper

    def get_description(self, code: str, default: str = "Unknown") -> str:
        return self.registry.get_description(
            self.fallback_mapper,
            code,
            default=default,
        )


def load_optional_mimic_code_mapper(
    cfg: Mapping[str, object],
    code_type: str,
) -> object | None:
    """Load MIMIC ICD diagnosis/procedure mappers from configured dictionary files."""
    if code_type not in {"diagnosis", "procedure"}:
        raise ValueError(f"Unsupported MIMIC code type: {code_type}")

    diagnosis_path = None
    procedure_path = None
    if code_type == "diagnosis":
        diagnosis_path = _find_configured_path(cfg, "d_icd_diagnoses", ())
        if diagnosis_path is None:
            logger.warning("No MIMIC diagnosis dictionary configured or found")
            return None
    else:
        procedure_path = _find_configured_path(cfg, "d_icd_procedures", ())
        if procedure_path is None:
            logger.warning("No MIMIC procedure dictionary configured or found")
            return None

    try:
        from mimic_codde_mapper import init_mimic_mappers

        registry = init_mimic_mappers(
            diagnosis_path=str(diagnosis_path) if diagnosis_path else None,
            procedure_path=str(procedure_path) if procedure_path else None,
        )
        return _MIMICRegistryLookup(registry, f"{code_type}_10")
    except Exception as exc:
        logger.warning("Could not load MIMIC %s descriptions: %s", code_type, exc)
        return None


def load_optional_code_mapper(
    cfg: Mapping[str, object],
    mapper_key: str,
    *,
    default_paths: Sequence[str | Path] = (),
) -> object | None:
    path = _find_configured_path(cfg, mapper_key, default_paths)
    if path is None:
        logger.warning("No %s code description file configured or found", mapper_key)
        return None

    try:
        from canada_code_mapper import CodeMapper

        mapper = CodeMapper.from_file(path)
        logger.info("Loaded %s code descriptions from %s", mapper_key, path)
        return mapper
    except Exception as exc:
        logger.warning("Could not load %s code descriptions from %s: %s", mapper_key, path, exc)
        return None


def _find_configured_path(
    cfg: Mapping[str, object],
    mapper_key: str,
    default_paths: Sequence[str | Path],
) -> Path | None:
    candidates: list[object] = []

    for group_name in ("code_description_paths", "mapping_paths", "raw_paths"):
        group = cfg.get(group_name)
        if isinstance(group, Mapping):
            candidates.extend(
                [
                    group.get(mapper_key),
                    group.get(f"{mapper_key}_descriptions"),
                    group.get(f"{mapper_key}_description"),
                    group.get(f"{mapper_key}_path"),
                ]
            )

    candidates.extend(default_paths)
    for candidate in candidates:
        if not candidate:
            continue
        for path in _candidate_paths(candidate):
            if path.exists():
                return path

    return None


def _candidate_paths(candidate: object) -> list[Path]:
    path = Path(str(candidate)).expanduser()
    if path.is_absolute():
        return [path]
    return [
        Path.cwd() / path,
        SOURCE_ROOT / path,
        SOURCE_ROOT.parent / path,
    ]
