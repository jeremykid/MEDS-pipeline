# src/meds_pipeline/etl/mimic/admissions.py
import pandas as pd
from ..base import ComponentETL
from ..registry import register

@register("admissions")
class MIMICAdmissions(ComponentETL):
    def run_core(self) -> pd.DataFrame:
        path = self.cfg["raw_paths"]["admissions"]
        df = self._read_csv_with_progress(path, "Loading admission data")
        
        # Build admission codes with format: ADMIT//HOSP//{admission_type}
        admit_codes = "ADMIT//HOSP//" + df["admission_type"].astype(str).fillna("")
        
        # start
        start = pd.DataFrame({
            "subject_id": df["subject_id"],
            "time": pd.to_datetime(df["admittime"], errors="coerce"),
            "event_type": "encounter.start",
            "code": admit_codes,
            "code_system": "EVENT",
        })
        
        # Build discharge codes with format: DISCHARGE//HOSP//{discharge_location}
        discharge_codes = "DISCHARGE//HOSP//" + df["discharge_location"].astype(str).fillna("")
        
        # end
        end = pd.DataFrame({
            "subject_id": df["subject_id"],
            "time": pd.to_datetime(df["dischtime"], errors="coerce"),
            "event_type": "encounter.end",
            "code": discharge_codes,
            "code_system": "EVENT",
        })
        out = pd.concat([start, end], ignore_index=True)
        return out

    # ADMIT//HOSP//{admission_type}, ADMIT//ED//{admission_type}, DISCHARGE//HOSP//{discharge_location}
    def run_plus(self) -> pd.DataFrame:
        path = self.cfg["raw_paths"]["admissions"]
        df = self._read_csv_with_progress(path, "Loading admission data for PLUS format")
        plus_start_cols = {
            "encounter_class": df["admission_type"],
            "source_table": "hosp.admissions",
        }
        plus_end_cols = {
            "encounter_class": df["discharge_location"],
            "source_table": "hosp.admissions",
        }
        core = self.run_core().reset_index(drop=True)
        for k, v in plus_start_cols.items():
            core.loc[core["event_type"].isin(["encounter.start"]), k] = v
        for k, v in plus_end_cols.items():
            core.loc[core["event_type"].isin(["encounter.end"]), k] = v

        return core