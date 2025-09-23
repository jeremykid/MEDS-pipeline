# src/meds_pipeline/etl/mimic/admissions.py
import pandas as pd
import pyreadstat
from ..base import ComponentETL
from ..registry import register

@register("admissions")
class AHSAdmissions(ComponentETL):
    def run_core(self) -> pd.DataFrame:
        path = self.cfg["raw_paths"]["admissions"]
        # df = pd.read_csv(path)
        df, meta = pyreadstat.read_sas7bdat(path, output_format="pandas")
        # print (df.columns)
        start = pd.DataFrame({
            "subject_id": df["PATID"],
            "time": pd.to_datetime(df["ADMITDATE_DT"]),
            "event_type": "encounter.start",
            "code": "ADMIT",
            "code_system": "EVENT",
        })

        end = pd.DataFrame({
            "subject_id": df["PATID"],
            "time": pd.to_datetime(df["DISDATE_DT"]),
            "event_type": "encounter.end",
            "code": "DISCHARGE",
            "code_system": "EVENT",
        })
        out = pd.concat([start, end], ignore_index=True)
        return out
    
    def run_plus(self) -> pd.DataFrame:
        path = self.cfg["raw_paths"]["admissions"]
        df, meta = pyreadstat.read_sas7bdat(path, output_format="pandas")
        plus_start_cols = {
            # "encounter_id": df["hadm_id"].astype(str),
            "encounter_class": df["ADMITCAT"],
            # "value_text": df["discharge_location"],
            "source_table": "rmt22884_dad_20211105",
        }
        plus_end_cols = {
            # "encounter_id": df["hadm_id"].astype(str),
            "encounter_class": df["DISP"],
            # "value_text": df["discharge_location"],
            "source_table": "rmt22884_dad_20211105",
        }
        core = self.run_core().reset_index(drop=True)
        for k, v in plus_start_cols.items():
            core.loc[core["event_type"].isin(["encounter.start"]), k] = v
        for k, v in plus_end_cols.items():
            core.loc[core["event_type"].isin(["encounter.end"]), k] = v

        return core