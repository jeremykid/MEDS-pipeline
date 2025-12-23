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
        
        # Build admission codes with format: ADMIT//HOSP//{ADMITCAT} @TODO creating map for ADMITCAT?
        # admit_codes = "ADMIT//HOSP//" + df["ADMITCAT"].astype(str).fillna("")
        admit_codes = "ADMIT//HOSP//" + df["ADMITCAT"].apply(self.get_ADMITCAT)
        
        start = pd.DataFrame({
            "subject_id": df["PATID"],
            "time": pd.to_datetime(df["ADMITDATE_DT"], errors="coerce"),
            "event_type": "encounter.start",
            "code": admit_codes,
            "code_system": "EVENT",
        })

        # Build discharge codes with format: DISCHARGE//HOSP//{DISP}
        # discharge_codes = "DISCHARGE//HOSP//" + df["DISP"].astype(str).fillna("")
        discharge_codes = "DISCHARGE//HOSP//" + df["DISP"].apply(self.get_SEPI_DISPOS)
                
        end = pd.DataFrame({
            "subject_id": df["PATID"],
            "time": pd.to_datetime(df["DISDATE_DT"], errors="coerce"),
            "event_type": "encounter.end",
            "code": discharge_codes,
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

    def get_SEPI_DISPOS(self, DISP) -> str:        
        SEPI_DISPOS_MAP = {
            "1": "Discharged - visit concluded",
            "2": "Discharged from program or clinic - will not return for further care. (This refers only to the last visit of a service recipient discharged from a treatment program at which he/she has been seen for repeat services)",
            "3": "Left against medical advice (Intended care not completed)",
            "4": "Service recipient admitted as an inpatient to Critical Care Unit or OR in own facility",
            "5": "Service recipient admitted as an inpatient to other area in own facility",
            "6": "Service recipient transferred to another acute care facility (includes psychiatric, rehab, oncology and pediatric facilities)",
            "7": "DAA (died after arrival) - Service recipient expired in ambulatory care service",
            "8": "DOA (dead on arrival) - Service recipient dead on arrival to ambulatory care service",
            "9": "Left without being seen. (Not seen by a professional service provider)",
            "0": "No doctor available - service recipient asked to return later",
            "01": "Discharged Home (private dwelling, not an institution; no support services",
            "02": "Patient registered, left without being seen (LWBS), or treated by a service provider (before triage if ED visit)",
            "03": "Patient triaged and then left the emergency department before further assessment by a service provider (for example, physician, nurse, allied health provider) (patient registered)",
            "04": "Patient triaged (if ED Visit), registered and assessed by a service provider (for example, physician) and left without treatment",
            "05": "Patient triaged (if ED Visit), registered, and assessed by a service provider and treatment initiated; left against medical advice (LAMA) before treatment completed",
            "06": "Admitted into reporting facility as an in-patient to critical care unit or operating room directly from an ambulatory care visit functional centre",
            "07": "Admitted into reporting facility as an in-patient to another unit of the reporting facility directly from an ambulatory care visit functional centre",
            "08": "Transferred to another acute care facility directly from an ambulatory care visit functional centre. Includes transfers to another acute care facility with entry through the emergency department",
            "09": "Transferred to another non-acute care facility directly from an ambulatory care visit functional centre (for example, stand-alone rehabilitation or stand-alone mental health facility).",
            "10": "Death after arrival (DAA)—Patient expires after initiation of the ambulatory care visit. Resuscitative measures (for example, cardiopulmonary resuscitation or CPR) may occur during the visit but are not successful.",
            "11": "Death On Arrival (DOA)—Patient is dead on arrival to the ambulatory care service. Generally there is no intent to resuscitate (for example, perform CPR). Includes cases where the patient is brought in for pronouncement of death.",
            "12": "Intra facility transfer to day surgery",
            "13": "Intra-facility transfer to the emergency department",
            "14": "Intra-facility transfer to clinic",
            "15": "Discharged to place of residence (Institution for example, Nursing or Retirement Home or Chronic Care; Private Dwelling with Home Care, VON, Meals on Wheels, etc.; or Jail)",
        }
        return SEPI_DISPOS_MAP.get(str(DISP), "Unknown")
    
    def get_ADMITCAT(self, ADMITCAT) -> str:
        ADMITCAT_MAP = {
            'E': 'emergency department',
            'A': 'advanced ambulatory',
            'U': 'urgent care',
            'O': 'other ambulatory'
        }
        return ADMITCAT_MAP.get(str(ADMITCAT), "Unknown")