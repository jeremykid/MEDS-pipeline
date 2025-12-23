# src/meds_pipeline/etl/mimic/medicines.py
from __future__ import annotations

from typing import List

import numpy as np
import pandas as pd

from ..base import ComponentETL
from ..registry import register

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
        Join free-text & numeric attributes for readability/auditing. Produce a
        compact, human-readable summary containing as much useful metadata as possible.

        Examples:
          "dose=10 mg | form=1 tablet | strength=40mg Tablet | route=IV | freq=3x/day | ndc=51079010000"

        Rules:
          - Only include fields that exist and are non-empty.
          - Skip values that are null/empty/"nan"/"None".
          - Keep input index; return pd.Series of strings.
        """
        parts: List[pd.Series] = []

        def _clean(col: str) -> pd.Series:
            s = df[col].astype(str).str.strip()
            s = s.where(~s.isin(["", "nan", "None", "NaN"]), np.nan)
            return s

        # dose value + unit -> "dose=10 mg"
        dose_val = _clean("dose_val_rx") if "dose_val_rx" in df.columns else None
        dose_unit = _clean("dose_unit_rx") if "dose_unit_rx" in df.columns else None
        if dose_val is not None and dose_unit is not None:
            dose_comb = (dose_val.fillna("") + " " + dose_unit.fillna("")).str.strip()
            dose_comb = dose_comb.where(dose_comb != "", np.nan)
            parts.append(("dose=" + dose_comb).where(dose_comb.notna(), ""))
        elif dose_val is not None:
            parts.append(("dose=" + dose_val).where(dose_val.notna(), ""))
        elif dose_unit is not None:
            parts.append(("dose_unit=" + dose_unit).where(dose_unit.notna(), ""))

        # form display (dispensed quantity + unit) -> "form=1 tablet"
        if "form_val_disp" in df.columns or "form_unit_disp" in df.columns:
            form_val = _clean("form_val_disp") if "form_val_disp" in df.columns else None
            form_unit = _clean("form_unit_disp") if "form_unit_disp" in df.columns else None
            if form_val is not None and form_unit is not None:
                form_comb = (form_val.fillna("") + " " + form_unit.fillna("")).str.strip()
                form_comb = form_comb.where(form_comb != "", np.nan)
                parts.append(("form=" + form_comb).where(form_comb.notna(), ""))
            elif form_val is not None:
                parts.append(("form=" + form_val).where(form_val.notna(), ""))
            elif form_unit is not None:
                parts.append(("form_unit=" + form_unit).where(form_unit.notna(), ""))

        # product strength (free text)
        if "prod_strength" in df.columns:
            ps = _clean("prod_strength")
            parts.append(("strength=" + ps).where(ps.notna(), ""))

        # frequency / doses per 24 hrs
        if "frequency" in df.columns:
            freq = _clean("frequency")
            parts.append(("freq=" + freq).where(freq.notna(), ""))
        if "doses_per_24_hrs" in df.columns:
            d24 = _clean("doses_per_24_hrs")
            parts.append(("doses_per_24hrs=" + d24).where(d24.notna(), ""))

        # route
        if "route" in df.columns:
            route = _clean("route")
            parts.append(("route=" + route).where(route.notna(), ""))

        # drug free-text
        if "drug" in df.columns:
            drug = _clean("drug")
            parts.append(("drug=" + drug).where(drug.notna(), ""))

        # formulary code / gsn
        if "formulary_drug_cd" in df.columns:
            fcd = _clean("formulary_drug_cd")
            parts.append(("formulary=" + fcd).where(fcd.notna(), ""))
        if "gsn" in df.columns:
            gsn = _clean("gsn")
            parts.append(("gsn=" + gsn).where(gsn.notna(), ""))

        # normalized ndc (use existing normalizer)
        # if "ndc" in df.columns:
        #     ndc_norm = MIMICMedicines._normalize_ndc(df["ndc"])
        #     ndc_norm = ndc_norm.replace({"": np.nan})
        #     parts.append(("ndc=" + ndc_norm).where(ndc_norm.notna(), ""))

        if not parts:
            return pd.Series([""] * len(df), index=df.index)

        # Join using " | ", skipping empty chunks
        stacked = pd.concat(parts, axis=1).replace("", np.nan)
        joined = stacked.apply(
            lambda r: " | ".join([x for x in r.tolist() if isinstance(x, str) and x.strip() != ""]) if r.notna().any() else "",
            axis=1,
        )
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
            dose_num = pd.to_numeric(df_valid["dose_val_rx"], errors="coerce")
            if "doses_per_24_hrs" in df_valid.columns:
                d24 = pd.to_numeric(df_valid["doses_per_24_hrs"], errors="coerce")
                prod = dose_num * d24
                # 如果乘积为 NaN（doses_per_24_hrs 缺失或不可乘），回退到原始 dose_val_rx
                out["value_num"] = prod.where(prod.notna(), dose_num)
            else:
                out["value_num"] = dose_num

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


