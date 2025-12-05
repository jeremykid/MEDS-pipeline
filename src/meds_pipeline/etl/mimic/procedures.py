# src/meds_pipeline/etl/mimic/procedures.py
import pandas as pd
from ..base import ComponentETL
from ..registry import register

'''
MIMIC-IV Procedures ETL Component

Handles procedures from MIMIC-IV hospital data.

Data source:
1. Hospital procedures: hosp/procedures_icd.csv.gz
   - Links to admissions via hadm_id
   - Contains ICD procedure codes from hospital admissions
   
Expected data structure from procedures_icd.csv:
subject_id	hadm_id	seq_num	chartdate	icd_code	icd_version
10000032	22595853	1	2180-05-07	9904	9
10000032	22595853	2	2180-05-07	8872	9
10001217	26221548	1	2157-09-12	0040	10

Note: 
- icd_version can be either 9 (ICD-9-PCS) or 10 (ICD-10-PCS)
- chartdate is used as the timestamp for the procedure event
- seq_num represents the order of procedures within the admission
'''

@register("procedures")
class MIMICProcedures(ComponentETL):
    
    @staticmethod
    def _build_procedure_code(icd_code: str, icd_version: int) -> str:
        """
        Build MEDS-compliant procedure code in the format:
        PROCEDURE//ICD//9//code or PROCEDURE//ICD//10//code
        
        Args:
            icd_code: The raw ICD procedure code
            icd_version: 9 or 10 indicating ICD version
            
        Returns:
            Formatted procedure code string
        
        Examples:
            - ICD-9: "4611" -> "PROCEDURE//ICD//9//4611"
            - ICD-10: "30243V1" -> "PROCEDURE//ICD//10//30243V1"
        """
        if pd.isna(icd_version) or pd.isna(icd_code):
            return None
        
        icd_code_str = str(icd_code).strip()
        if not icd_code_str:
            return None
        
        icd_version_int = int(icd_version)
        if icd_version_int == 9:
            return f"PROCEDURE//ICD//9//{icd_code_str}"
        elif icd_version_int == 10:
            return f"PROCEDURE//ICD//10//{icd_code_str}"
        else:
            # Fallback for unexpected versions
            return f"PROCEDURE//ICD//{icd_version_int}//{icd_code_str}"
    
    def run_core(self) -> pd.DataFrame:
        """
        Load procedures_icd data, generate MEDS-compliant codes, and output MEDS-CORE format.
        
        Returns:
            DataFrame with columns: subject_id, time, event_type, code, value
        """
        # Load procedures data
        procedures_df = self._read_csv_with_progress(
            self.cfg["raw_paths"]["procedures_icd"],
            "Loading procedures data"
        )
        
        # Build MEDS-compliant procedure codes
        procedures_df['meds_code'] = procedures_df.apply(
            lambda row: self._build_procedure_code(row['icd_code'], row['icd_version']),
            axis=1
        )
        
        # Create 'value' column for procedure sequence number (string dtype)
        # Priority: use existing seq_num from procedures_icd if available
        seq_candidates = ['seq_num', 'sequence_num', 'proc_seq', 'procedure_sequence', 'seq']
        found_seq = None
        for col in seq_candidates:
            if col in procedures_df.columns:
                found_seq = col
                break
        
        if found_seq:
            # Convert existing sequence to string, preserve pd.NA for missing values
            procedures_df['value'] = pd.to_numeric(
                procedures_df[found_seq], 
                errors='coerce'
            ).astype('Int64').astype("string")
        else:
            # Derive sequence by grouping within each admission
            grp_keys = ['subject_id', 'hadm_id']
            
            # Sort by admission and time for stable ordering
            sort_cols = grp_keys.copy()
            if 'chartdate' in procedures_df.columns:
                sort_cols.append('chartdate')
            procedures_df = procedures_df.sort_values(sort_cols).reset_index(drop=True)
            
            # Generate sequence within each admission
            procedures_df['value'] = (
                procedures_df.groupby(grp_keys).cumcount() + 1
            ).astype('Int64').astype("string")
        
        # Ensure value is string dtype
        procedures_df['value'] = procedures_df['value'].astype("string")
        
        # Use chartdate as the event time
        # chartdate represents when the procedure was performed/charted
        time_col = 'chartdate' if 'chartdate' in procedures_df.columns else None
        if time_col is None:
            raise ValueError("chartdate column not found in procedures_icd data")
        
        # Create MEDS core structure
        out = pd.DataFrame({
            "subject_id": procedures_df["subject_id"].astype(str),
            "time": pd.to_datetime(procedures_df[time_col], errors="coerce"),
            "event_type": "procedures",
            "code": procedures_df["meds_code"].astype(str),
            "value": procedures_df["value"],  # string dtype sequence number
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
