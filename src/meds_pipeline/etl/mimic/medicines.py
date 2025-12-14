# src/meds_pipeline/etl/mimic/medicines.py
from __future__ import annotations

from typing import List

import numpy as np
import pandas as pd

from ..base import ComponentETL
from ..registry import register

# TODO 关于 mimic medicine，我想和其他 component 一样，不用 plus 这个function 只需要 run_core 然后 code 只要 
# Code 这种格式 MEDICINE//NDC//{ndc}  可以吗？
# 在 hosp/prescriptions/ 这个数据库里 
'''
  pre_df = pd.read_csv("/data/padmalab_external/special_project/physionet.org/files/mimiciv/3.1/hosp/prescriptions.csv.gz", compression='gzip')
Index(['subject_id', 'hadm_id', 'pharmacy_id', 'poe_id', 'poe_seq',
       'order_provider_id', 'starttime', 'stoptime', 'drug_type', 'drug',
       'formulary_drug_cd', 'gsn', 'ndc', 'prod_strength', 'form_rx',
       'dose_val_rx', 'dose_unit_rx', 'form_val_disp', 'form_unit_disp',
       'doses_per_24_hrs', 'route'],
      dtype='object')
   subject_id   hadm_id  pharmacy_id       poe_id  poe_seq order_provider_id  \
0    10000032  22595853     12775705  10000032-55     55.0            P85UQ1   
1    10000032  22595853     18415984  10000032-42     42.0            P23SJA   
2    10000032  22595853     23637373  10000032-35     35.0            P23SJA   
3    10000032  22595853     26862314  10000032-41     41.0            P23SJA   
4    10000032  22595853     30740602  10000032-27     27.0            P23SJA   

             starttime             stoptime drug_type  \
0  2180-05-08 08:00:00  2180-05-07 22:00:00      MAIN   
1  2180-05-07 02:00:00  2180-05-07 22:00:00      MAIN   
2  2180-05-07 01:00:00  2180-05-07 09:00:00      MAIN   
3  2180-05-07 01:00:00  2180-05-07 01:00:00      MAIN   
4  2180-05-07 00:00:00  2180-05-07 22:00:00      MAIN   

                          drug  ...     gsn           ndc    prod_strength  \
0                   Furosemide  ...  008209  5.107901e+10      40mg Tablet   
1      Ipratropium Bromide Neb  ...  021700  4.879801e+08       2.5mL Vial   
2                   Furosemide  ...  008208  5.107901e+10      20mg Tablet   
3           Potassium Chloride  ...  001275  2.450041e+08  10mEq ER Tablet   
...
3              1.0     PO  
4              3.0     IV  
'''
@register("medicines")
class MIMICMedicines(ComponentETL):
    """
    Component: MIMIC-IV hosp.prescriptions → MEDS event stream
    Produces `medication.order` events at order start time.

    Expected raw columns (best-effort; code handles missing columns gracefully):
      - subject_id, hadm_id
      - starttime (required for time)
      - ndc (required for code generation)
      - dose_val_rx, dose_unit_rx, route, frequency (optional, for metadata)
      - pharmacy_id (optional, used as provenance_id when available)
    
    Output format:
      - code: MEDICINE//NDC//{ndc}
      - code_system: NDC
      - Rows without valid NDC are dropped
    """

    # ---- helpers ----------------------------------------------------------------
    @staticmethod
    def _normalize_ndc(ndc_series: pd.Series) -> pd.Series:
        """
        Normalize NDC codes to avoid scientific notation issues.
        Converts numeric NDC values (like 5.107901e+10) to integer strings.
        """
        def _norm_ndc(x):
            if pd.isna(x):
                return ""
            try:
                # Try to convert to float first, then to int if it's integer-like
                xf = float(x)
                if xf.is_integer():
                    return str(int(xf))
                return str(int(xf))  # Force to int even if has decimals
            except (ValueError, OverflowError):
                # If conversion fails, just stringify
                return str(x).strip()
        
        return ndc_series.apply(_norm_ndc).astype(str).str.strip()

    @staticmethod
    def _pick_code_and_system(df: pd.DataFrame) -> pd.DataFrame:
        """
        Only use NDC. Return code formatted as MEDICINE//NDC//{ndc} and code_system='NDC'.
        Missing/empty NDC -> empty string (will be dropped later).
        """
        if "ndc" not in df.columns:
            ndc_series = pd.Series([""] * len(df), index=df.index)
        else:
            ndc_series = MIMICMedicines._normalize_ndc(df["ndc"])
            ndc_series = ndc_series.where(~ndc_series.isin(["", "nan", "None", "NaN"]), "")

        code = ndc_series.apply(lambda c: f"MEDICINE//NDC//{c}" if c != "" else "")
        code_system = np.where(code != "", "NDC", "")

        return pd.DataFrame({"code": code, "code_system": code_system}, index=df.index)

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

    # ---- core --------------------------------------------------------------
    def run_core(self) -> pd.DataFrame:
        """
        Converts MIMIC-IV hosp.prescriptions to MEDS core format.
        Only uses NDC for code generation (MEDICINE//NDC//{ndc}).
        Rows without valid NDC codes are dropped.
        Uses only starttime (ignores stoptime).
        """
        path = self.cfg["raw_paths"]["medicines"]
        df = self._read_csv_with_progress(path, "Loading medication data")

        # Validate required columns
        if "starttime" not in df.columns:
            raise KeyError("No `starttime` column found in hosp.prescriptions")

        # time: only use starttime
        time = pd.to_datetime(df["starttime"], errors="coerce")

        # code: MEDICINE//NDC//{ndc}
        code_block = self._pick_code_and_system(df)

        # Build core DataFrame
        out = pd.DataFrame({
            "subject_id": df["subject_id"],
            "time": time,
            "event_type": "medication.order",
            "code": code_block["code"],
            "code_system": code_block["code_system"],
        })

        # Drop rows without time or subject_id or code BEFORE adding optional fields
        out = out.dropna(subset=["subject_id", "time"])
        out = out[out["code"].astype(str).str.strip() != ""]
        
        # Now filter df to match the valid indices
        valid_indices = out.index
        df_valid = df.loc[valid_indices]
        out = out.reset_index(drop=True)
        df_valid = df_valid.reset_index(drop=True)

        # Optional: encounter_id (hadm_id)
        if "hadm_id" in df_valid.columns:
            out["encounter_id"] = df_valid["hadm_id"].astype(str)

        # Optional: value_num (dose quantity)
        if "dose_val_rx" in df_valid.columns:
            out["value_num"] = pd.to_numeric(df_valid["dose_val_rx"], errors="coerce")

        # Optional: unit (dose unit)
        if "dose_unit_rx" in df_valid.columns:
            out["unit"] = df_valid["dose_unit_rx"].astype(str).str.strip()

        # Optional: value_text (human-readable metadata)
        out["value_text"] = self._assemble_value_text(df_valid)

        # Metadata
        out["source_table"] = "hosp.prescriptions"
        if "pharmacy_id" in df_valid.columns:
            out["provenance_id"] = df_valid["pharmacy_id"].astype(str)
        elif "row_id" in df_valid.columns:
            out["provenance_id"] = df_valid["row_id"].astype(str)
        else:
            out["provenance_id"] = (out.index.astype(int) + 1).astype(str)

        return out


