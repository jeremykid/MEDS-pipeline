# src/meds_pipeline/etl/mimic/diagnosis.py
import pandas as pd
from ..base import ComponentETL
from ..registry import register

'''
MIMIC-IV Diagnosis ETL Component

Expected merged data structure after joining diagnoses_icd, d_icd_diagnoses, and admissions:
subject_id	hadm_id	seq_num	icd_code	icd_version	long_title	admittime	dischtime
10000032	22595853	1	5723	9	Portal hypertension	2180-05-06 22:23:00	2180-05-07 17:15:00
10000032	22595853	2	78959	9	Other ascites	2180-05-06 22:23:00	2180-05-07 17:15:00
10000032	22595853	3	5715	9	Cirrhosis of liver without mention of alcohol	2180-05-06 22:23:00	2180-05-07 17:15:00

Note: icd_version can be either 9 (ICD-9) or 10 (ICD-10)
'''
@register("diagnosis")
class MIMICDiagnosis(ComponentETL):
    def run_core(self) -> pd.DataFrame:
        # Load and merge diagnosis data
        d_icd_diagnoses = pd.read_csv(self.cfg["raw_paths"]["d_icd_diagnoses"], compression='gzip')
        # print (d_icd_diagnoses.columns.tolist())
        d_icd_diagnoses = d_icd_diagnoses.drop('icd_version',axis = 1)

        diagnoses_icd = pd.read_csv(self.cfg["raw_paths"]["diagnoses_icd"], compression='gzip')
        diagnoses_icd = diagnoses_icd.merge(d_icd_diagnoses, on='icd_code', how='left')
        
        # Load admission data for timing
        path = self.cfg["raw_paths"]["admissions"]
        admissions_df = self._read_csv_with_progress(path, "Loading admission data")
        diagnoses_icd = diagnoses_icd.merge(
            admissions_df[['hadm_id', 'admittime', 'dischtime']], 
            on='hadm_id', 
            how='left'
        )
        # print (diagnoses_icd.columns.tolist())

        # Create MEDS core structure
        out = pd.DataFrame({
            "subject_id": diagnoses_icd["subject_id"].astype(str),
            "time": pd.to_datetime(diagnoses_icd["admittime"], errors="coerce"),
            "event_type": "diagnosis",
            "code": diagnoses_icd["icd_code"].astype(str),
            "code_system": diagnoses_icd["icd_version"].apply(lambda x: f"ICD-{x}" if pd.notna(x) else "ICD"),
        })
        
        # Filter out invalid records
        out = out.dropna(subset=["subject_id", "time"])
        out = out[out["subject_id"].astype(str).str.strip() != ""]
        out = out[out["code"].astype(str).str.strip() != ""].reset_index(drop=True)
        return out
    
    def run_plus(self) -> pd.DataFrame:
        # Load and merge diagnosis data (same as core)
        d_icd_diagnoses = pd.read_csv(self.cfg["raw_paths"]["d_icd_diagnoses"], compression='gzip')
        diagnoses_icd = pd.read_csv(self.cfg["raw_paths"]["diagnoses_icd"], compression='gzip')
        diagnoses_icd = diagnoses_icd.merge(d_icd_diagnoses, on='icd_code', how='left')
        
        path = self.cfg["raw_paths"]["admissions"]
        admissions_df = self._read_csv_with_progress(path, "Loading admission data")
        diagnoses_icd = diagnoses_icd.merge(
            admissions_df[['hadm_id', 'admittime', 'dischtime']], 
            on='hadm_id', 
            how='left'
        )
        
        # Get core data with same filtering
        core = self.run_core().reset_index(drop=True)
        
        # Apply same filtering to original data to maintain alignment
        valid_mask = (
            diagnoses_icd["subject_id"].notna() &
            pd.to_datetime(diagnoses_icd["admittime"], errors="coerce").notna() &
            (diagnoses_icd["subject_id"].astype(str).str.strip() != "") &
            (diagnoses_icd["icd_code"].astype(str).str.strip() != "")
        )
        df_filtered = diagnoses_icd[valid_mask].reset_index(drop=True)
        
        # Add diagnosis-specific metadata
        if "hadm_id" in df_filtered.columns:
            core["hadm_id"] = df_filtered["hadm_id"].astype(str)
            
        if "seq_num" in df_filtered.columns:
            core["seq_num"] = df_filtered["seq_num"].astype(str)
            
        if "long_title" in df_filtered.columns:
            core["diagnosis_description"] = df_filtered["long_title"].astype(str)
            
        if "dischtime" in df_filtered.columns:
            core["discharge_time"] = pd.to_datetime(df_filtered["dischtime"], errors="coerce")
            
        # Create value_text with diagnosis metadata
        core["value_text"] = self._assemble_diagnosis_metadata(df_filtered)
        
        # Provenance tracking
        core["source_table"] = "DIAGNOSES_ICD"
        core["provenance_id"] = (core.index.astype(int) + 1).astype(str)
        
        return core
    
    @staticmethod
    def _assemble_diagnosis_metadata(df: pd.DataFrame) -> pd.Series:
        """
        Create human-readable metadata for diagnosis records, e.g.:
        "hadm_id=22595853 | seq=1 | icd_version=9 | desc=Portal hypertension"
        """
        parts = []
        
        if "hadm_id" in df.columns:
            hadm_txt = "hadm_id=" + df["hadm_id"].astype(str)
            parts.append(hadm_txt)
            
        if "seq_num" in df.columns:
            seq_txt = "seq=" + df["seq_num"].astype(str)
            parts.append(seq_txt)
            
        if "icd_version" in df.columns:
            version_txt = "icd_version=" + df["icd_version"].astype(str)
            parts.append(version_txt)
            
        if "long_title" in df.columns:
            # Truncate long descriptions for readability
            desc = df["long_title"].astype(str).str[:50] + "..."
            desc_txt = "desc=" + desc
            parts.append(desc_txt)
            
        if not parts:
            return pd.Series([""] * len(df), index=df.index)
            
        # Combine all parts with " | " separator
        combined = pd.concat(parts, axis=1)
        metadata = combined.apply(
            lambda row: " | ".join([str(x) for x in row.tolist() if pd.notna(x) and str(x).strip()]),
            axis=1
        )
        return metadata
