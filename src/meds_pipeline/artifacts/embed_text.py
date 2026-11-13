"""Deterministic BioLORD embed-text templates for MEDS clinical codes."""

from __future__ import annotations

from typing import Optional

import pandas as pd

from meds_pipeline.etl.code_descriptions import clean_description

PAD_ID = 0
UNK_ID = 1
PAD_CODE = "<PAD>"
UNK_CODE = "UNKNOWN"

TEXT_TEMPLATE_VERSION = "v1"
TEXT_TEMPLATE = "{event_type} | {code_system} | {code} | {description_or_no_description}"
MISSING_DESCRIPTION_SENTINEL = "[no description]"
UNK_EMBED_TEXT = f"unknown | unknown | {UNK_CODE} | {MISSING_DESCRIPTION_SENTINEL}"


def parse_code_system(code: str) -> str:
    """Infer a code_system label from a MEDS composite code string."""
    parts = str(code).split("//")
    if len(parts) < 2:
        return "unknown"

    # DIAGNOSIS//ICD//{ver}//{raw} or PROCEDURE//ICD//{ver}//{raw}
    if len(parts) >= 4 and parts[1] == "ICD":
        return f"ICD//{parts[2]}"

    # DIAGNOSIS//ICD10CA//{raw}, PROCEDURE//CCI//{raw}, LAB//LOINC//{raw}, ...
    if len(parts) >= 3:
        return parts[1]

    return "unknown"


def resolve_code_system(code: str, code_system: object | None = None) -> str:
    """Prefer a non-empty parquet code_system; otherwise parse from ``code``."""
    if code_system is not None and not (isinstance(code_system, float) and pd.isna(code_system)):
        try:
            if pd.isna(code_system):
                pass
            else:
                text = str(code_system).strip()
                if text and text.lower() not in {"nan", "none", "<na>", "nat"}:
                    return text
        except (TypeError, ValueError):
            text = str(code_system).strip()
            if text and text.lower() not in {"nan", "none", "<na>", "nat"}:
                return text
    return parse_code_system(code)


def build_embed_text(
    *,
    event_type: str,
    code_system: str,
    code: str,
    description: Optional[str] = None,
) -> str:
    """Build the v1 BioLORD input string for one clinical code."""
    cleaned = clean_description(description) if description is not None else None
    desc_part = cleaned if cleaned else MISSING_DESCRIPTION_SENTINEL
    return (
        f"{str(event_type).strip()} | "
        f"{str(code_system).strip()} | "
        f"{str(code).strip()} | "
        f"{desc_part}"
    )
