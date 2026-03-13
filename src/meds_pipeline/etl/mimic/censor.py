'''
censor:               '/data/padmalab_external/special_project/Weijie_Code/Discharged_ECG_Death_label/mimic_censor_death_label.csv.gz'

	patient_id	censor_time	death_event
0	10000032	2180-09-09 23:59:59	True
1	10000068	2160-03-04 06:26:00	False
2	10000084	2161-02-13 23:59:59	True
3	10000108	2163-09-28 09:04:00	False
4	10000115	2154-12-17 16:59:00	False

MIMIC Censor							
subject_id	time	event_type	code	code_system	value_text	source_table	provenance_id
10000068	2160-03-04 06:26:00	demographics.death	MEDS_CENSOR	MEDS	source=censor	Censor	10000068
10000032	2180-09-09 23:59:59	demographics.death	MEDS_DEATH	MEDS	source=censor	Censor	10000032
10000084	2161-02-13 23:59:59	demographics.death	MEDS_DEATH	MEDS	source=censor	Censor	10000084
'''
from __future__ import annotations

import pandas as pd

from ..base import ComponentETL
from ..registry import register


@register("censor")
class MIMICCensor(ComponentETL):
    """
    Component: MIMIC censor/death label → MEDS event stream

    Input CSV columns: patient_id, censor_time, death_event
    
    Transformation:
      - death_event == False → code = 'MEDS_CENSOR'  (patient censored, not dead)
      - death_event == True  → code = 'MEDS_DEATH'   (patient died)

    Output MEDS-Core columns:
      subject_id, time, event_type, code, code_system, value_text, source_table, provenance_id
    """

    def run_core(self) -> pd.DataFrame:
        path = self.cfg["raw_paths"]["censor"]
        df = self._read_csv_with_progress(path, "Loading censor data")

        # Validate required columns
        required = ["patient_id", "censor_time", "death_event"]
        for col in required:
            if col not in df.columns:
                raise KeyError(f"MIMIC censor expects column `{col}`, got {list(df.columns)}")

        # subject_id
        df["subject_id"] = df["patient_id"].astype(str)

        # time
        df["time"] = pd.to_datetime(df["censor_time"], errors="coerce")

        # event_type
        df["event_type"] = "demographics.death"

        # code: MEDS_DEATH if death_event is True, else MEDS_CENSOR
        death_flag = df["death_event"].astype(str).str.strip().str.lower() == "true"
        df["code"] = "MEDS_CENSOR"
        df.loc[death_flag, "code"] = "MEDS_DEATH"

        # code_system
        df["code_system"] = "MEDS"

        # value_text
        df["value_text"] = "source=censor"

        # source_table
        df["source_table"] = "Censor"

        # provenance_id
        df["provenance_id"] = df["subject_id"]

        out = df[["subject_id", "time", "event_type", "code",
                   "code_system", "value_text", "source_table", "provenance_id"]].copy()

        # Drop rows with NaT time
        out = out.dropna(subset=["time"]).reset_index(drop=True)

        # Log summary
        n_censor = (out["code"] == "MEDS_CENSOR").sum()
        n_death = (out["code"] == "MEDS_DEATH").sum()
        print(f"   📊 Censor summary: {n_censor:,} MEDS_CENSOR + {n_death:,} MEDS_DEATH = {len(out):,} total")

        return out