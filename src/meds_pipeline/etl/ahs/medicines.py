# src/meds_pipeline/etl/ahs/medicines.py
from __future__ import annotations

from typing import List

import numpy as np
import pandas as pd

from ..base import ComponentETL
from ..registry import register
import pyreadstat
# TODO 关于 medicine，我想和其他 component 一样，不用 plus 这个function 只需要 run_core 然后 code 只要 
# Code 这种格式 MEDICINE//ATC//{SUPP_DRUG_ATC_CODE}  可以吗？
# example of PIN
'''
   PATID  DSPN_DATE  DSPN_AMT_QTY DSPN_AMT_UNT_MSR_CD  DSPN_DAY_SUPPLY_QTY  \
0    1.0 2008-04-01          60.0                 TAB                 30.0   
1    1.0 2008-04-01          60.0                 TAB                 30.0   
2    1.0 2008-04-01          60.0                 TAB                 30.0   
3    1.0 2008-04-01          60.0                 TAB                 30.0   
4    1.0 2008-04-18          30.0                 TAB                 30.0   

   DRUG_DIN SUPP_DRUG_ATC_CODE  
0  02212331            A02BA02  
1  02242681            B01AA03  
2  02242681            B01AA03  
3  02212331            A02BA02  
4  02010909            G04CB01  
'''


@register("medicines")
class AHSMedicines(ComponentETL):
    """
    Component: medicines (pin records) → MEDS event stream
    Produces `medication.dispense` events at dispense date.

    Expected raw columns (best-effort; code is defensive to missing columns):
      - PATID
      - DSPN_DATE
      - SUPP_DRUG_ATC_CODE (required for code generation)
      - DSPN_AMT_QTY, DSPN_AMT_UNT_MSR_CD (optional, for value_num/unit)
      - DSPN_DAY_SUPPLY_QTY (optional, for value_text)
      - INST (optional, for site)
    
    Output format:
      - code: MEDICINE//ATC//{SUPP_DRUG_ATC_CODE}
      - code_system: ATC
      - Rows without valid SUPP_DRUG_ATC_CODE are dropped
    """

    # ---------------------------- helpers ------------------------------------- #

    @staticmethod
    def _assemble_value_text(df: pd.DataFrame) -> pd.Series:
        """
        Human-readable text for auditing, e.g.:
          "amt=60 | unit=TAB | day_supply=30"
        Only includes available, non-empty parts.
        """
        parts: List[pd.Series] = []

        # amount quantity
        if "DSPN_AMT_QTY" in df.columns:
            amt = pd.to_numeric(df["DSPN_AMT_QTY"], errors="coerce")
            amt_txt = ("amt=" + amt.astype(str)).where(amt.notna(), "")
            parts.append(amt_txt)

        # amount unit
        if "DSPN_AMT_UNT_MSR_CD" in df.columns:
            unit = df["DSPN_AMT_UNT_MSR_CD"].astype(str).str.strip()
            unit = unit.where(unit.notna() & (unit != "") & (unit.str.lower() != "nan"), "")
            unit_txt = ("unit=" + unit).where(unit != "", "")
            parts.append(unit_txt)

        # day supply
        if "DSPN_DAY_SUPPLY_QTY" in df.columns:
            ds = df["DSPN_DAY_SUPPLY_QTY"]
            ds_txt = ("day_supply=" + ds.astype("Int64").astype(str)).where(ds.notna(), "")
            parts.append(ds_txt)

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
        """
        Converts AHS PIN medication data to MEDS core format.
        Only uses SUPP_DRUG_ATC_CODE for code generation (MEDICINE//ATC//{code}).
        Rows without valid ATC codes are dropped.
        """
        path = self.cfg["raw_paths"]["medicines"]
        df, meta = pyreadstat.read_sas7bdat(path, output_format="pandas")
        
        # Validate required columns
        if "PATID" not in df.columns:
            print(df.columns.tolist())
            raise KeyError("AHS PIN expects column `PATID`")
        if "DSPN_DATE" not in df.columns:
            raise KeyError("AHS PIN expects column `DSPN_DATE`")
        
        # subject_id
        subject = df["PATID"].astype("Int64").astype(str)
        
        # time
        time = pd.to_datetime(df["DSPN_DATE"], errors="coerce")
        
        # code: MEDICINE//ATC//{SUPP_DRUG_ATC_CODE}
        atc = df.get("SUPP_DRUG_ATC_CODE", pd.Series([""] * len(df))).astype(str).str.strip()
        valid_atc = atc.notna() & (atc != "") & (atc.str.lower() != "nan")
        code = ("MEDICINE//ATC//" + atc).where(valid_atc, "")
        
        # Build core DataFrame
        out = pd.DataFrame({
            "subject_id": subject,
            "time": time,
            "event_type": "medication.dispense",
            "code": code,
            "code_system": "ATC",
        })
        
        # Optional: value_num (medication amount)
        if "DSPN_AMT_QTY" in df.columns:
            out["value_num"] = pd.to_numeric(df["DSPN_AMT_QTY"], errors="coerce")
        
        # Optional: unit (amount unit measure)
        if "DSPN_AMT_UNT_MSR_CD" in df.columns:
            out["unit"] = df["DSPN_AMT_UNT_MSR_CD"].astype(str).str.strip()
        
        # Optional: value_text (human-readable metadata)
        out["value_text"] = self._assemble_value_text(df)
        
        # Metadata
        out["source_table"] = "PIN"
        out["provenance_id"] = (out.index.astype(int) + 1).astype(str)  # 1-based row index
        
        # Optional: site (institution)
        if "INST" in df.columns:
            out["site"] = df["INST"].astype(str).str.strip()
        
        # Drop unusable rows (no subject/time/code)
        out = out.dropna(subset=["time"])
        out = out[out["subject_id"].astype(str).str.strip() != ""]
        out = out[out["code"].astype(str).str.strip() != ""].reset_index(drop=True)
        
        return out


