from __future__ import annotations

from pathlib import Path
from typing import Dict, Optional, Set

import pandas as pd

from ..base import ComponentETL
from ..registry import register


@register("labs")
class MIMICLabs(ComponentETL):
    """
    Component: MIMIC-IV hosp.labevents -> MEDS event stream.

    Expected raw path keys in config (first existing key will be used):
      - raw_paths.labtests
      - raw_paths.labs
      - raw_paths.labevents

    Output format:
      - event_type: "lab"
      - code: LAB//MIMIC//{itemid}
      - code_system: MIMIC_LAB_ITEMID

    Notes:
      - Reads labevents in chunks to reduce memory pressure.
      - Preserves numeric value from `valuenum`; falls back to parsing `value`.
      - Preserves inequality comparator from `value` (<, <=, >, >=) when present.
    """

    CHUNK_SIZE = 1_000_000

    @staticmethod
    def _clean_string(series: pd.Series) -> pd.Series:
        s = series.where(series.notna(), "").astype(str).str.strip()
        return s.where(~s.isin(["", "nan", "None", "NaN", "<NA>"]), None)

    @staticmethod
    def _int_string(series: pd.Series) -> pd.Series:
        s = pd.to_numeric(series, errors="coerce").astype("Int64").astype(str)
        return s.replace({"<NA>": None, "nan": None, "NaN": None, "None": None})

    @staticmethod
    def _extract_comparator(value: pd.Series) -> pd.Series:
        extracted = value.where(value.notna(), "").astype(str).str.extract(
            r"^\s*(<=|>=|<|>)", expand=False
        )
        return extracted.where(extracted.notna(), None)

    @staticmethod
    def _extract_numeric_from_value(value: pd.Series) -> pd.Series:
        return pd.to_numeric(
            value.where(value.notna(), "").astype(str).str.extract(r"([+-]?\d*\.?\d+)", expand=False),
            errors="coerce",
        )

    def _resolve_labevents_path(self) -> str:
        raw_paths = self.cfg.get("raw_paths", {})
        for key in ("labtests", "labs", "labevents"):
            path = raw_paths.get(key)
            if path:
                return path
        raise KeyError(
            "Missing lab path in config. Expected one of: "
            "raw_paths.labtests/raw_paths.labs/raw_paths.labevents"
        )

    def _resolve_d_labitems_path(self, labevents_path: str) -> Optional[str]:
        raw_paths = self.cfg.get("raw_paths", {})
        configured = raw_paths.get("d_labitems")
        if configured:
            return configured

        inferred = Path(labevents_path).with_name("d_labitems.csv.gz")
        if inferred.exists():
            return str(inferred)

        return None

    def _load_label_map(self, labevents_path: str) -> Dict[int, str]:
        d_labitems_path = self._resolve_d_labitems_path(labevents_path)
        if not d_labitems_path:
            return {}

        try:
            d_labitems = pd.read_csv(d_labitems_path, compression="gzip", low_memory=False)
        except Exception as exc:
            print(f"⚠️  Warning: failed to load d_labitems from {d_labitems_path}: {exc}")
            return {}

        if "itemid" not in d_labitems.columns or "label" not in d_labitems.columns:
            return {}

        itemids = pd.to_numeric(d_labitems["itemid"], errors="coerce").astype("Int64")
        labels = self._clean_string(d_labitems["label"])
        mapped = pd.DataFrame({"itemid": itemids, "label": labels}).dropna(subset=["itemid"])
        mapped = mapped.drop_duplicates(subset=["itemid"], keep="first")

        return {
            int(row.itemid): str(row.label)
            for row in mapped.itertuples(index=False)
            if pd.notna(row.label)
        }

    def _transform_chunk(self, chunk: pd.DataFrame, label_map: Dict[int, str]) -> pd.DataFrame:
        subject_id = self._int_string(chunk["subject_id"])
        time = pd.to_datetime(chunk["charttime"], errors="coerce")

        itemid_numeric = pd.to_numeric(chunk["itemid"], errors="coerce").astype("Int64")
        itemid_str = self._int_string(chunk["itemid"])

        value = (
            self._clean_string(chunk["value"])
            if "value" in chunk.columns
            else pd.Series([None] * len(chunk), index=chunk.index)
        )
        value_num_src = (
            pd.to_numeric(chunk["valuenum"], errors="coerce")
            if "valuenum" in chunk.columns
            else pd.Series(float("nan"), index=chunk.index)
        )
        value_num_fallback = self._extract_numeric_from_value(value)
        value_num = value_num_src.where(value_num_src.notna(), value_num_fallback)

        unit = (
            self._clean_string(chunk["valueuom"])
            if "valueuom" in chunk.columns
            else pd.Series([None] * len(chunk), index=chunk.index)
        )
        comparator = self._extract_comparator(value)

        code = "LAB//MIMIC//" + itemid_str
        out = pd.DataFrame(
            {
                "subject_id": subject_id,
                "time": time,
                "event_type": "lab",
                "code": code,
                "code_system": "MIMIC_LAB_ITEMID",
                "value_num": value_num,
                "comparator": comparator,
                "unit": unit,
                "value_text": value,
                "source_table": "hosp.labevents",
            },
            index=chunk.index,
        )

        if "hadm_id" in chunk.columns:
            out["encounter_id"] = self._int_string(chunk["hadm_id"])

        if "labevent_id" in chunk.columns:
            out["provenance_id"] = self._int_string(chunk["labevent_id"])
        else:
            out["provenance_id"] = (pd.RangeIndex(len(chunk)) + 1).astype(str)

        if label_map:
            label_series = itemid_numeric.map(label_map)
            label_series = self._clean_string(label_series)

            has_label = label_series.notna()
            has_raw = out["value_text"].notna() & (out["value_text"].fillna("").astype(str).str.strip() != "")

            with_label_and_raw = has_label & has_raw
            with_label_only = has_label & ~has_raw

            out.loc[with_label_and_raw, "value_text"] = (
                "label="
                + label_series[with_label_and_raw].astype(str)
                + " | raw="
                + out.loc[with_label_and_raw, "value_text"].fillna("").astype(str)
            )
            out.loc[with_label_only, "value_text"] = "label=" + label_series[with_label_only].astype(str)

        valid = out["subject_id"].notna() & out["time"].notna() & itemid_str.notna()
        out = out[valid].reset_index(drop=True)

        out = out[out["subject_id"].fillna("").astype(str).str.strip() != ""]
        out = out[out["code"].fillna("").astype(str).str.strip() != ""]

        return out

    def _filter_by_patient_limit(
        self,
        chunk: pd.DataFrame,
        keep_subjects: Set[str],
        max_patients: Optional[int],
    ) -> pd.DataFrame:
        subject = self._int_string(chunk["subject_id"])
        valid_subject = subject.notna() & (subject.fillna("").astype(str).str.strip() != "")

        if not max_patients:
            return chunk.loc[valid_subject].copy()

        if len(keep_subjects) < max_patients:
            ordered_unique = pd.unique(subject[valid_subject])
            for sid in ordered_unique:
                sid_str = str(sid)
                if sid_str not in keep_subjects:
                    keep_subjects.add(sid_str)
                    if len(keep_subjects) >= max_patients:
                        break

        keep_mask = valid_subject & subject.fillna("").astype(str).isin(keep_subjects)
        return chunk.loc[keep_mask].copy()

    def run_core(self) -> pd.DataFrame:
        path = self._resolve_labevents_path()
        max_patients = self.base_cfg.get("max_patients")
        show_progress = self.base_cfg.get("show_progress", True)
        chunk_size = int(self.base_cfg.get("labs_chunksize", self.CHUNK_SIZE))

        if show_progress:
            print("📖 Loading lab events data (chunked)...")

        label_map = self._load_label_map(path)

        header_cols = pd.read_csv(path, compression="gzip", nrows=0).columns.tolist()
        preferred_cols = [
            "labevent_id",
            "subject_id",
            "hadm_id",
            "itemid",
            "charttime",
            "value",
            "valuenum",
            "valueuom",
        ]
        usecols = [c for c in preferred_cols if c in header_cols]

        required = {"subject_id", "itemid", "charttime"}
        missing_required = sorted(required.difference(usecols))
        if missing_required:
            raise KeyError(f"Missing required labevents columns: {missing_required}")

        reader = pd.read_csv(
            path,
            compression="gzip",
            usecols=usecols,
            chunksize=chunk_size,
            low_memory=False,
        )

        keep_subjects: Set[str] = set()
        parts = []
        processed_rows = 0
        emitted_rows = 0

        for i, chunk in enumerate(reader, start=1):
            processed_rows += len(chunk)

            chunk = self._filter_by_patient_limit(chunk, keep_subjects, max_patients)
            if chunk.empty:
                continue

            out_chunk = self._transform_chunk(chunk, label_map)
            if out_chunk.empty:
                continue

            emitted_rows += len(out_chunk)
            parts.append(out_chunk)

            if show_progress and (i == 1 or i % 10 == 0):
                patient_msg = f", selected_patients={len(keep_subjects):,}" if max_patients else ""
                print(
                    f"   └─ Chunks={i:,}, rows_read={processed_rows:,}, "
                    f"rows_emitted={emitted_rows:,}{patient_msg}"
                )

        if not parts:
            return pd.DataFrame(
                columns=[
                    "subject_id",
                    "time",
                    "event_type",
                    "code",
                    "code_system",
                    "value_num",
                    "comparator",
                    "unit",
                    "value_text",
                    "source_table",
                    "provenance_id",
                    "encounter_id",
                ]
            )

        out = pd.concat(parts, ignore_index=True)

        if show_progress:
            patients = out["subject_id"].nunique() if "subject_id" in out.columns else 0
            print(f"   ✅ Generated {len(out):,} lab events for {patients:,} patients")

        return out
