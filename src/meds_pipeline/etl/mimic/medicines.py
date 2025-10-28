# src/meds_pipeline/etl/mimic/medicines.py
from __future__ import annotations

from typing import List

import numpy as np
import pandas as pd

from ..base import ComponentETL
from ..registry import register

@register("medicines")
class MIMICMedicines(ComponentETL):
    """
    Component: MIMIC-IV hosp.prescriptions → MEDS event stream
    Produces `medication.order` events at order start time.

    Expected raw columns (best-effort; code handles missing columns gracefully):
      - subject_id, hadm_id
      - starttime, stoptime (we use starttime; no stop event here)
      - ndc (preferred code) or drug (fallback)
      - dose_val_rx, dose_unit_rx, route, frequency
      - pharmacy_id or row_id (used as provenance_id when available)
    """

    # ---- helpers ----------------------------------------------------------------
    @staticmethod
    def _pick_code_and_system(df: pd.DataFrame) -> pd.DataFrame:
        """
        Pick `code` and `code_system`:
          - If `ndc` available and not null/empty -> code=ndc, code_system='NDC'
          - Else -> code=drug (string), code_system='LOCAL'
        """
        ndc_exists = "ndc" in df.columns
        if ndc_exists:
            ndc_clean = df["ndc"].astype(str).str.strip()
            use_ndc = ndc_clean.notna() & (ndc_clean != "") & (ndc_clean.str.lower() != "nan")
        else:
            use_ndc = pd.Series(False, index=df.index)

        drug_series = df["drug"].astype(str).str.strip() if "drug" in df.columns else pd.Series("", index=df.index)

        code = np.where(use_ndc, ndc_clean, drug_series)
        code_system = np.where(use_ndc, "NDC", "LOCAL")

        out = pd.DataFrame({"code": code, "code_system": code_system})
        return out

    @staticmethod
    def _assemble_value_text(df: pd.DataFrame) -> pd.Series:
        """
        Join free-text attributes for readability/auditing:
          "dose_val_rx dose_unit_rx | route | frequency"
        Only include parts that exist & are non-empty.
        """
        parts: List[pd.Series] = []
        dose_val = df["dose_val_rx"].astype(str).str.strip() if "dose_val_rx" in df.columns else None
        dose_unit = df["dose_unit_rx"].astype(str).str.strip() if "dose_unit_rx" in df.columns else None
        route = df["route"].astype(str).str.strip() if "route" in df.columns else None
        freq = df["frequency"].astype(str).str.strip() if "frequency" in df.columns else None

        # dose part: "10 mg"
        if dose_val is not None and dose_unit is not None:
            dose_pair = (dose_val.replace("nan", "").replace("", np.nan).fillna(""))
            unit_pair = (dose_unit.replace("nan", "").replace("", np.nan).fillna(""))
            dose_text = (dose_pair + " " + unit_pair).str.strip()
            dose_text = dose_text.replace("", np.nan).fillna("")
            parts.append(dose_text)
        elif dose_val is not None:
            parts.append(dose_val.replace("nan", "").fillna(""))

        if route is not None:
            parts.append(route.replace("nan", "").fillna(""))

        if freq is not None:
            parts.append(freq.replace("nan", "").fillna(""))

        if not parts:
            return pd.Series([""] * len(df), index=df.index)

        # Join using " | ", but skip empty chunks
        stacked = pd.concat(parts, axis=1)
        stacked = stacked.replace("", np.nan)
        joined = stacked.apply(lambda r: " | ".join([x for x in r.tolist() if isinstance(x, str)]) if r.notna().any() else "", axis=1)
        return joined

    # ---- core/plus --------------------------------------------------------------
    def run_core(self) -> pd.DataFrame:
        path = self.cfg["raw_paths"]["medication"]
        df = self._read_csv_with_progress(path, "Loading medication data")

        # time: prefer starttime; fallback to ordertime if present
        time_col = "starttime" if "starttime" in df.columns else ("ordertime" if "ordertime" in df.columns else None)
        if time_col is None:
            raise KeyError("No `starttime`/`ordertime` column found in hosp.prescriptions")

        time = pd.to_datetime(df[time_col], errors="coerce")

        code_block = self._pick_code_and_system(df)

        out = pd.DataFrame(
            {
                "subject_id": df["subject_id"],
                "time": time,
                "event_type": "medication.order",
                "code": code_block["code"],
                "code_system": code_block["code_system"],
            }
        )

        # Drop rows without time or subject_id/code
        out = out.dropna(subset=["subject_id", "time"])
        out = out[out["code"].astype(str).str.strip() != ""].reset_index(drop=True)
        return out

    def run_plus(self) -> pd.DataFrame:
        path = self.cfg["raw_paths"]["medication"]
        df = self._read_csv_with_progress(path, "Loading medication data for PLUS format")

        # Generate core data without reloading
        # time: prefer starttime; fallback to ordertime if present
        time_col = "starttime" if "starttime" in df.columns else ("ordertime" if "ordertime" in df.columns else None)
        if time_col is None:
            raise KeyError("No `starttime`/`ordertime` column found in hosp.prescriptions")

        time = pd.to_datetime(df[time_col], errors="coerce")
        code_block = self._pick_code_and_system(df)

        core = pd.DataFrame(
            {
                "subject_id": df["subject_id"],
                "time": time,
                "event_type": "medication.order",
                "code": code_block["code"],
                "code_system": code_block["code_system"],
            }
        )

        # Drop rows without time or subject_id/code
        core = core.dropna(subset=["subject_id", "time"])
        core = core[core["code"].astype(str).str.strip() != ""].reset_index(drop=True)

        # encounter_id
        if "hadm_id" in df.columns:
            core["encounter_id"] = df.loc[core.index, "hadm_id"].astype(str)

        # numeric dose & unit
        if "dose_val_rx" in df.columns:
            # Try parse numeric; non-numeric becomes NaN
            core["value_num"] = pd.to_numeric(df.loc[core.index, "dose_val_rx"], errors="coerce")

        if "dose_unit_rx" in df.columns:
            core["unit"] = df.loc[core.index, "dose_unit_rx"].astype(str).str.strip()

        # value_text = "dose unit | route | frequency"
        core["value_text"] = self._assemble_value_text(df.loc[core.index])

        # provenance/source
        core["source_table"] = "hosp.prescriptions"
        if "pharmacy_id" in df.columns:
            core["provenance_id"] = df.loc[core.index, "pharmacy_id"].astype(str)
        elif "row_id" in df.columns:
            core["provenance_id"] = df.loc[core.index, "row_id"].astype(str)

        return core
