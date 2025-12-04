# src/meds_pipeline/etl/mimic/diagnosis.py
import pandas as pd
from ..base import ComponentETL
from ..registry import register

'''
MIMIC-IV Diagnosis ETL Component

Handles both hospital (inpatient) and ED diagnoses from MIMIC-IV.

Data sources:
1. Hospital diagnoses: hosp/diagnoses_icd.csv.gz
   - Links to admissions via hadm_id
   - Contains diagnoses from hospital admissions
   
2. ED diagnoses: ed/diagnoses_icd.csv.gz  
   - Links to ED stays via stay_id
   - Contains diagnoses from emergency department visits

Expected merged data structure after joining diagnoses_icd, d_icd_diagnoses, and admissions/edstays:
subject_id	hadm_id	seq_num	icd_code	icd_version	long_title	admittime	dischtime
10000032	22595853	1	5723	9	Portal hypertension	2180-05-06 22:23:00	2180-05-07 17:15:00
10000032	22595853	2	78959	9	Other ascites	2180-05-06 22:23:00	2180-05-07 17:15:00
10000032	22595853	3	5715	9	Cirrhosis of liver without mention of alcohol	2180-05-06 22:23:00	2180-05-07 17:15:00

Note: icd_version can be either 9 (ICD-9) or 10 (ICD-10)
'''
@register("diagnosis")
class MIMICDiagnosis(ComponentETL):
    
    @staticmethod
    def _build_diagnosis_code(icd_code: str, icd_version: int) -> str:
        """
        Build MEDS-compliant diagnosis code in the format:
        DIAGNOSIS//ICD//9//code or DIAGNOSIS//ICD//10//code
        
        Args:
            icd_code: The raw ICD code
            icd_version: 9 or 10 indicating ICD version
            
        Returns:
            Formatted diagnosis code string
        
        Examples:
            - ICD-9: "3089" -> "DIAGNOSIS//ICD//9//3089"
            - ICD-10: "S32431A" -> "DIAGNOSIS//ICD//10//S32431A"
        """
        if pd.isna(icd_version) or pd.isna(icd_code):
            return None
        
        icd_code_str = str(icd_code).strip()
        if not icd_code_str:
            return None
        
        icd_version_int = int(icd_version)
        if icd_version_int == 9:
            return f"DIAGNOSIS//ICD//9//{icd_code_str}"
        elif icd_version_int == 10:
            return f"DIAGNOSIS//ICD//10//{icd_code_str}"
        else:
            # Fallback for unexpected versions
            return f"DIAGNOSIS//ICD//{icd_version_int}//{icd_code_str}"
    
    def _load_hospital_diagnoses(self) -> pd.DataFrame:
        """Load hospital (inpatient) diagnoses from hosp/diagnoses_icd.csv.gz"""
        # Load diagnosis dictionary
        d_icd_diagnoses = pd.read_csv(self.cfg["raw_paths"]["d_icd_diagnoses"], compression='gzip')
        d_icd_diagnoses = d_icd_diagnoses.drop('icd_version', axis=1, errors='ignore')
        
        # Load hospital diagnoses
        hosp_diagnoses = pd.read_csv(self.cfg["raw_paths"]["hosp_diagnoses_icd"], compression='gzip')
        hosp_diagnoses = hosp_diagnoses.merge(d_icd_diagnoses, on='icd_code', how='left')
        
        # Load admission data for timing
        admissions_df = self._read_csv_with_progress(
            self.cfg["raw_paths"]["admissions"], 
            "Loading admission data"
        )
        hosp_diagnoses = hosp_diagnoses.merge(
            admissions_df[['hadm_id', 'admittime', 'dischtime']], 
            on='hadm_id', 
            how='left'
        )
        
        # Add source marker
        hosp_diagnoses['diagnosis_source'] = 'INPATIENT'
        hosp_diagnoses['time_col'] = hosp_diagnoses['admittime']
        
        return hosp_diagnoses
    
    def _load_ed_diagnoses(self) -> pd.DataFrame:
        """Load ED diagnoses from ed/diagnoses_icd.csv.gz if available"""
        # Check if ED diagnoses path exists in config
        if 'ed_diagnoses_icd' not in self.cfg["raw_paths"]:
            return pd.DataFrame()  # Return empty DataFrame if not available
        
        # Load diagnosis dictionary
        d_icd_diagnoses = pd.read_csv(self.cfg["raw_paths"]["d_icd_diagnoses"], compression='gzip')
        d_icd_diagnoses = d_icd_diagnoses.drop('icd_version', axis=1, errors='ignore')
        
        # Load ED diagnoses
        ed_diagnoses = pd.read_csv(self.cfg["raw_paths"]["ed_diagnoses_icd"], compression='gzip')
        ed_diagnoses = ed_diagnoses.merge(d_icd_diagnoses, on='icd_code', how='left')
        
        # Load ED stay data for timing
        ed_stays = self._read_csv_with_progress(
            self.cfg["raw_paths"]["ed"], 
            "Loading ED stay data"
        )
        
        # Normalize join keys to avoid dtype/whitespace mismatch
        if 'stay_id' in ed_diagnoses.columns:
            ed_diagnoses['stay_id'] = ed_diagnoses['stay_id'].astype(str).str.strip()
        if 'stay_id' in ed_stays.columns:
            ed_stays['stay_id'] = ed_stays['stay_id'].astype(str).str.strip()
        
        # Merge with suffixes so we can coalesce subject_id safely
        ed_diagnoses = ed_diagnoses.merge(
            ed_stays[['stay_id', 'subject_id', 'intime', 'outtime']], 
            on='stay_id', 
            how='left',
            suffixes=('', '_stays')
        )
        
        # Coalesce subject_id: prefer ed_diagnoses.subject_id if present, else subject_id_stays
        if 'subject_id_stays' in ed_diagnoses.columns:
            if 'subject_id' in ed_diagnoses.columns:
                ed_diagnoses['subject_id'] = ed_diagnoses['subject_id'].where(
                    ed_diagnoses['subject_id'].notna(),
                    ed_diagnoses['subject_id_stays']
                )
            else:
                ed_diagnoses['subject_id'] = ed_diagnoses['subject_id_stays']
            ed_diagnoses = ed_diagnoses.drop(columns=['subject_id_stays'])
        
        # Ensure subject_id is string for downstream pipeline
        if 'subject_id' in ed_diagnoses.columns:
            ed_diagnoses['subject_id'] = ed_diagnoses['subject_id'].astype(str)
        
        # Fill time_col from intime (from stays) for consistent MEDS time
        ed_diagnoses['diagnosis_source'] = 'ED'
        if 'intime' in ed_diagnoses.columns:
            ed_diagnoses['time_col'] = ed_diagnoses['intime']
        else:
            ed_diagnoses['time_col'] = pd.NaT
        
        if 'outtime' in ed_diagnoses.columns:
            ed_diagnoses['dischtime'] = ed_diagnoses['outtime']
        
        return ed_diagnoses
    
    def run_core(self) -> pd.DataFrame:
        # Load both hospital and ED diagnoses
        hosp_diagnoses = self._load_hospital_diagnoses()
        ed_diagnoses = self._load_ed_diagnoses()
        
        # Combine both sources
        all_diagnoses = pd.concat([hosp_diagnoses, ed_diagnoses], ignore_index=True)
        
        # Build MEDS-compliant diagnosis codes
        all_diagnoses['meds_code'] = all_diagnoses.apply(
            lambda row: self._build_diagnosis_code(row['icd_code'], row['icd_version']),
            axis=1
        )
        
        # Create MEDS core structure
        out = pd.DataFrame({
            "subject_id": all_diagnoses["subject_id"].astype(str),
            "time": pd.to_datetime(all_diagnoses["time_col"], errors="coerce"),
            "event_type": "diagnosis",
            "code": all_diagnoses["meds_code"].astype(str),
            "diagnosis_source": all_diagnoses["diagnosis_source"],
        })
        
        # Filter out invalid records
        out = out.dropna(subset=["subject_id", "time", "code"])
        out = out[out["subject_id"].astype(str).str.strip() != ""]
        out = out[out["code"].astype(str).str.strip() != ""]
        out = out[out["code"] != "None"].reset_index(drop=True)
        
        return out
    
    def run_plus(self) -> pd.DataFrame:
        """
        DEPRECATED: MEDS-PLUS export has been removed. Use run_core() instead.
        This method is kept for backward compatibility but will raise an error.
        """
        raise RuntimeError(
            "MEDS-PLUS export has been removed. Only MEDS-CORE is supported. "
            "Please use run_core() instead."
        )
    
    @staticmethod
    def _assemble_diagnosis_metadata(df: pd.DataFrame) -> pd.Series:
        """
        Create human-readable metadata for diagnosis records, e.g.:
        "source=INPATIENT | hadm_id=22595853 | seq=1 | icd_version=9 | desc=Portal hypertension"
        or
        "source=ED | stay_id=33258284 | seq=1 | icd_version=10 | desc=Abdominal pain"
        """
        parts = []
        
        # Add diagnosis source
        if "diagnosis_source" in df.columns:
            source_txt = "source=" + df["diagnosis_source"].astype(str)
            parts.append(source_txt)
        
        # Add encounter ID (hadm_id for hospital, stay_id for ED)
        if "hadm_id" in df.columns:
            hadm_txt = "hadm_id=" + df["hadm_id"].fillna("").astype(str)
            parts.append(hadm_txt)
        
        if "stay_id" in df.columns:
            stay_txt = "stay_id=" + df["stay_id"].fillna("").astype(str)
            parts.append(stay_txt)
            
        if "seq_num" in df.columns:
            seq_txt = "seq=" + df["seq_num"].astype(str)
            parts.append(seq_txt)
            
        if "icd_version" in df.columns:
            version_txt = "icd_version=" + df["icd_version"].astype(str)
            parts.append(version_txt)
            
        if "long_title" in df.columns:
            # Truncate long descriptions for readability
            desc = df["long_title"].fillna("").astype(str)
            desc = desc.apply(lambda x: x[:50] + "..." if len(x) > 50 else x)
            desc_txt = "desc=" + desc
            parts.append(desc_txt)
            
        if not parts:
            return pd.Series([""] * len(df), index=df.index)
            
        # Combine all parts with " | " separator
        combined = pd.concat(parts, axis=1)
        metadata = combined.apply(
            lambda row: " | ".join([str(x) for x in row.tolist() if pd.notna(x) and str(x).strip() and str(x) != ""]),
            axis=1
        )
        return metadata
