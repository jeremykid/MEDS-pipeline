# src/meds_pipeline/etl/ahs/censor.py
from __future__ import annotations

import pandas as pd

from ..base import ComponentETL
from ..registry import register


@register("censor")
class AHSCensor(ComponentETL):
    """
    Component: AHS demographics death/censor -> MEDS event stream

    Input: rmt22884_demographics_with_death.parquet (same file as demographics component)
    Relevant columns: PATID, death_date

    Transformation:
      - death_date is NaT   -> code = 'MEDS_CENSOR', time = 2021-09-30
      - death_date has value -> code = 'MEDS_DEATH',  time = death_date

    Output MEDS-Core columns:
      subject_id, time, event_type, code, code_system,
      value_text, source_table, provenance_id
    """

    CENSOR_DATE = pd.Timestamp("2021-09-30")

    def run_core(self) -> pd.DataFrame:
        path = self.cfg["raw_paths"]["demographics"]
        df = pd.read_parquet(path, columns=["PATID", "death_date"])

        # Validate
        if "PATID" not in df.columns:
            raise KeyError(f"AHS censor expects column PATID, got {list(df.columns)}")

        # subject_id
        df["subject_id"] = df["PATID"].astype("Int64").astype(str)

        # Parse death_date
        df["death_date"] = pd.to_datetime(df["death_date"], errors="coerce")

        # Deduplicate (one row per patient)
        df = df.drop_duplicates(subset=["subject_id"], keep="first").reset_index(drop=True)

        # Assign code and time based on death_date
        is_dead = df["death_date"].notna()

        df["code"] = "MEDS_CENSOR"
        df.loc[is_dead, "code"] = "MEDS_DEATH"

        df["time"] = self.CENSOR_DATE
        df.loc[is_dead, "time"] = df.loc[is_dead, "death_date"]

        # Fill remaining MEDS columns
        df["event_type"] = "demographics.death"
        df["code_system"] = "MEDS"
        df["value_text"] = "source=censor"
        df["source_table"] = "Censor"
        df["provenance_id"] = df["subject_id"]

        out = df[[
            "subject_id", "time", "event_type", "code",
            "code_system", "value_text", "source_table", "provenance_id"
        ]].copy().reset_index(drop=True)

        # Log summary
        n_censor = (out["code"] == "MEDS_CENSOR").sum()
        n_death = (out["code"] == "MEDS_DEATH").sum()
        print(f"   Censor summary: {n_censor:,} MEDS_CENSOR + {n_death:,} MEDS_DEATH = {len(out):,} total")

        return out
