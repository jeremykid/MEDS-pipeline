# src/meds_pipeline/etl/mimic/admissions.py
import pandas as pd
from ..base import ComponentETL
from ..registry import register

@register("admissions")
class MIMICAdmissions(ComponentETL):
    def run_core(self) -> pd.DataFrame:
        path = self.cfg["raw_paths"]["admissions"]
        df = pd.read_csv(path)
        # start
        start = pd.DataFrame({
            "subject_id": df["subject_id"],
            "time": pd.to_datetime(df["admittime"]),
            "event_type": "encounter.start",
            "code": "ADMIT",
            "code_system": "EVENT",
        })
        # end
        end = pd.DataFrame({
            "subject_id": df["subject_id"],
            "time": pd.to_datetime(df["dischtime"]),
            "event_type": "encounter.end",
            "code": "DISCHARGE",
            "code_system": "EVENT",
        })
        out = pd.concat([start, end], ignore_index=True)
        return out

    def run_plus(self) -> pd.DataFrame:
        path = self.cfg["raw_paths"]["admissions"]
        df = pd.read_csv(path)
        plus_cols = {
            "encounter_id": df["hadm_id"].astype(str),
            "encounter_class": df["admission_type"],
            "value_text": df["discharge_location"],
            "source_table": "hosp.admissions",
        }
        core = self.run_core().reset_index(drop=True)
        for k, v in plus_cols.items():
            core.loc[core["event_type"].isin(["encounter.start","encounter.end"]), k] = v
        return core
