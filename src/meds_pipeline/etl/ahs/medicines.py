# src/meds_pipeline/etl/ahs/medicines.py
from __future__ import annotations

from typing import List

import numpy as np
import pandas as pd

from ..base import ComponentETL
from ..registry import register
import pyreadstat

@register("medicines")
class AHSMedicines(ComponentETL):
    """
    Component: medicines (pin records) → MEDS event stream
    Produces `medication.dispense` events at dispense date.

    Expected raw columns (best-effort; code is defensive to missing columns):
      - PATID
      - DSPN_DATE
      - DSPN_AMT_QTY, DSPN_AMT_UNT_MSR_CD
      - DSPN_DAY_SUPPLY_QTY
      - DRUG_DIN (preferred code)
      - SUPP_DRUG_ATC_CODE (fallback code; also useful for value_text)
      - (Optional) INST / SITE, etc.
    """

    # ---------------------------- helpers ------------------------------------- #
    @staticmethod
    def _pick_code_and_system(df: pd.DataFrame) -> pd.DataFrame:
        """
        Choose code/code_system for each row:
          - If DRUG_DIN present & non-empty → code=DRUG_DIN, code_system='DIN'
          - Else if SUPP_DRUG_ATC_CODE present → code=ATC, code_system='ATC'
          - Else mark as empty (later dropped)
        """
        din = df["DRUG_DIN"].astype(str).str.strip() if "DRUG_DIN" in df.columns else pd.Series("", index=df.index)
        atc = (
            df["SUPP_DRUG_ATC_CODE"].astype(str).str.strip()
            if "SUPP_DRUG_ATC_CODE" in df.columns
            else pd.Series("", index=df.index)
        )

        use_din = din.notna() & (din != "") & (din.str.lower() != "nan")
        use_atc = (~use_din) & atc.notna() & (atc != "") & (atc.str.lower() != "nan")

        code = np.where(use_din, din, np.where(use_atc, atc, ""))
        code_system = np.where(use_din, "DIN", np.where(use_atc, "ATC", ""))

        return pd.DataFrame({"code": code, "code_system": code_system})

    @staticmethod
    def _assemble_value_text(df: pd.DataFrame) -> pd.Series:
        """
        Human-readable text for auditing, e.g.:
          "day_supply=30 | ATC=A02BA02"
        Only includes available, non-empty parts.
        """
        parts: List[pd.Series] = []

        if "DSPN_DAY_SUPPLY_QTY" in df.columns:
            ds = df["DSPN_DAY_SUPPLY_QTY"]
            ds_txt = ("day_supply=" + ds.astype("Int64").astype(str)).where(ds.notna(), "")
            parts.append(ds_txt)

        if "SUPP_DRUG_ATC_CODE" in df.columns:
            atc = df["SUPP_DRUG_ATC_CODE"].astype(str).str.strip()
            atc = atc.where(atc.notna() & (atc != "") & (atc.str.lower() != "nan"), "")
            atc_txt = ("ATC=" + atc).where(atc != "", "")
            parts.append(atc_txt)

        if not parts:
            return pd.Series([""] * len(df), index=df.index)

        stacked = pd.concat(parts, axis=1).replace("", np.nan)
        joined = stacked.apply(
            lambda r: " | ".join([x for x in r.tolist() if isinstance(x, str)]) if r.notna().any() else "",
            axis=1,
        )
        return joined

    # ------------------------------ Core -------------------------------------- #
    def run_core(self) -> pd.DataFrame:
        path = self.cfg["raw_paths"]["medication"]
        # df = pd.read_csv(path)
        df, meta = pyreadstat.read_sas7bdat(path, output_format="pandas")
        # subject_id
        if "PATID" not in df.columns:
            print (df.columns.tolist())
            raise KeyError("AHS PIN expects column `PATID`")
        subject = df["PATID"].astype("Int64").astype(str)

        # time
        if "DSPN_DATE" not in df.columns:
            raise KeyError("AHS PIN expects column `DSPN_DATE`")
        time = pd.to_datetime(df["DSPN_DATE"], errors="coerce")

        # code & code_system
        code_block = self._pick_code_and_system(df)

        out = pd.DataFrame(
            {
                "subject_id": subject,
                "time": time,
                "event_type": "medication.order",
                "code": code_block["code"],
                "code_system": code_block["code_system"],
            }
        )

        # Drop unusable rows (no subject/time/code)
        out = out.dropna(subset=["time"])
        out = out[out["subject_id"].astype(str).str.strip() != ""]
        out = out[out["code"].astype(str).str.strip() != ""].reset_index(drop=True)
        return out

    # ------------------------------ Plus -------------------------------------- #
    def run_plus(self) -> pd.DataFrame:
        path = self.cfg["raw_paths"]["medication"]
        # df = pd.read_csv(path)
        df, meta = pyreadstat.read_sas7bdat(path, output_format="pandas")
        
        # Apply the same filtering logic as run_core to keep data aligned
        if "PATID" not in df.columns:
            print(df.columns.tolist())
            raise KeyError("AHS PIN expects column `PATID`")
        subject = df["PATID"].astype("Int64").astype(str)

        # time
        if "DSPN_DATE" not in df.columns:
            raise KeyError("AHS PIN expects column `DSPN_DATE`")
        time = pd.to_datetime(df["DSPN_DATE"], errors="coerce")

        # code & code_system
        code_block = self._pick_code_and_system(df)

        # Create core structure
        core = pd.DataFrame(
            {
                "subject_id": subject,
                "time": time,
                "event_type": "medication.order",
                "code": code_block["code"],
                "code_system": code_block["code_system"],
            }
        )

        # Apply the same filtering as run_core to keep indices aligned
        valid_mask = (
            core["time"].notna() &
            (core["subject_id"].astype(str).str.strip() != "") &
            (core["code"].astype(str).str.strip() != "")
        )
        
        # Filter both core and original df to maintain alignment
        core = core[valid_mask].reset_index(drop=True)
        df_filtered = df[valid_mask].reset_index(drop=True)

        # Now we can safely use the aligned indices
        # numeric quantity (value_num)
        if "DSPN_AMT_QTY" in df_filtered.columns:
            core["value_num"] = pd.to_numeric(df_filtered["DSPN_AMT_QTY"], errors="coerce")

        # unit
        if "DSPN_AMT_UNT_MSR_CD" in df_filtered.columns:
            core["unit"] = df_filtered["DSPN_AMT_UNT_MSR_CD"].astype(str).str.strip()

        # value_text (day supply + ATC if available)
        core["value_text"] = self._assemble_value_text(df_filtered)

        # provenance & source
        core["source_table"] = "PIN"
        # If there is an explicit row identifier in your data, replace this with that column.
        core["provenance_id"] = (core.index.astype(int) + 1).astype(str)  # 1-based row index as fallback

        # (Optional) site column if available in your feed, e.g., INST
        if "INST" in df_filtered.columns:
            core["site"] = df_filtered["INST"].astype(str).str.strip()

        return core
