# src/meds_pipeline/etl/ahs/procedures.py
import pandas as pd
import pyreadstat
from ..base import ComponentETL
from ..registry import register

'''
AHS Procedures ETL Component

Data source:
- DAD (Discharge Abstract Database): SAS format with PROCCODE1-PROCCODE20
  * Hospital/inpatient procedures
  * Uses CCI (Canadian Classification of Health Interventions) coding system
  * Each PROCCODE has corresponding PROCSTDT{n}_DT for procedure date
  * Falls back to ADMITDATE_DT if procedure date is missing

Expected DAD columns:
PATID	ADMITDATE_DT	DISDATE_DT	PROCCODE1	PROCSTDT1_DT	PROCCODE2	PROCSTDT2_DT	...	PROCCODE20	PROCSTDT20_DT
1343.0	2021-08-19	2021-09-04	1HZ53HAGP	2021-08-20	1HT53LA	2021-08-21	...

Note: 
- CCI codes are used in Canadian healthcare system
- PROCSTDT{n}_DT represents the start date/time of the procedure
- If PROCSTDT{n}_DT is NaT, we use ADMITDATE_DT as fallback
'''

@register("procedures")
class AHSProcedures(ComponentETL):
    
    @staticmethod
    def _build_procedure_code(proc_code: str) -> str:
        """
        Build MEDS-compliant procedure code in the format:
        PROCEDURE//CCI//code
        
        Args:
            proc_code: The raw CCI procedure code
            
        Returns:
            Formatted procedure code string
        
        Examples:
            - "1HZ53HAGP" -> "PROCEDURE//CCI//1HZ53HAGP"
            - "1HT53LA" -> "PROCEDURE//CCI//1HT53LA"
        """
        if pd.isna(proc_code) or str(proc_code).strip() == "":
            return None
        
        proc_code_str = str(proc_code).strip()
        return f"PROCEDURE//CCI//{proc_code_str}"
    
    def _load_dad_data(self):
        """Load DAD data from SAS file"""
        dad_path = "/data/padmalab_external/special_project/AHS_Data_Release_2/rmt22884_dad_20211105.sas7bdat"
        dad_df, meta = pyreadstat.read_sas7bdat(dad_path, output_format="pandas")
        return dad_df
    
    def _extract_procedure_codes(self, df):
        """
        Extract procedure codes from DAD data.
        
        Melts PROCCODE1-PROCCODE20 into long format, with corresponding PROCSTDT{n}_DT as event time.
        Falls back to ADMITDATE_DT if procedure date is missing.
        
        Args:
            df: DataFrame containing procedure codes
        
        Returns:
            DataFrame with melted procedure codes and event times
        """
        # Define procedure code and date columns
        proc_code_cols = [f'PROCCODE{i}' for i in range(1, 21)]  # PROCCODE1-PROCCODE20
        proc_date_cols = [f'PROCSTDT{i}_DT' for i in range(1, 21)]  # PROCSTDT1_DT-PROCSTDT20_DT
        
        # Keep only existing columns
        existing_proc_cols = [col for col in proc_code_cols if col in df.columns]
        existing_date_cols = [col for col in proc_date_cols if col in df.columns]
        
        if not existing_proc_cols:
            raise KeyError("No procedure code columns (PROCCODE1-PROCCODE20) found in DAD data")
        
        # Prepare base columns
        id_vars = ['PATID', 'ADMITDATE_DT']
        
        # Create list to hold melted data
        melted_rows = []
        
        # Process each procedure code column and its corresponding date
        for i in range(1, 21):
            proc_col = f'PROCCODE{i}'
            date_col = f'PROCSTDT{i}_DT'
            
            if proc_col not in df.columns:
                continue
            
            # Create subset with relevant columns
            subset_cols = ['PATID', 'ADMITDATE_DT', proc_col]
            if date_col in df.columns:
                subset_cols.append(date_col)
            
            subset = df[subset_cols].copy()
            
            # Filter out empty/null procedure codes
            subset = subset[subset[proc_col].notna()]
            subset = subset[subset[proc_col].astype(str).str.strip() != '']
            
            if len(subset) == 0:
                continue
            
            # Rename columns to standard names
            subset = subset.rename(columns={proc_col: 'procedure_code'})
            
            # Set event time: use PROCSTDT{n}_DT if available and not NaT, otherwise use ADMITDATE_DT
            if date_col in subset.columns:
                subset['event_time'] = pd.to_datetime(subset[date_col], errors='coerce')
                # Fill NaT values with ADMITDATE_DT
                subset['event_time'] = subset['event_time'].fillna(
                    pd.to_datetime(subset['ADMITDATE_DT'], errors='coerce')
                )
            else:
                # If no procedure date column, use admission date
                subset['event_time'] = pd.to_datetime(subset['ADMITDATE_DT'], errors='coerce')
            
            # Add sequence number
            subset['sequence_num'] = i
            
            # Keep only needed columns
            subset = subset[['PATID', 'event_time', 'procedure_code', 'sequence_num']]
            
            melted_rows.append(subset)
        
        if not melted_rows:
            # Return empty DataFrame with correct schema
            return pd.DataFrame(columns=['PATID', 'event_time', 'procedure_code', 'sequence_num'])
        
        # Combine all melted data
        melted = pd.concat(melted_rows, ignore_index=True)
        
        return melted
    
    def run_core(self) -> pd.DataFrame:
        """
        Load procedures_icd data, generate MEDS-compliant codes, and output MEDS-CORE format.
        
        Returns:
            DataFrame with columns: subject_id, time, event_type, code, value
        """
        # Load DAD data
        dad_df = self._load_dad_data()
        
        # Extract procedure codes
        procedures = self._extract_procedure_codes(dad_df)
        
        # Build MEDS-compliant procedure codes
        procedures['meds_code'] = procedures['procedure_code'].apply(self._build_procedure_code)
        
        # Create 'value' column for procedure sequence number (string dtype)
        # Use the sequence_num extracted from PROCCODE column index
        procedures['value'] = procedures['sequence_num'].astype('Int64').astype("string")
        
        # Create MEDS core structure
        out = pd.DataFrame({
            "subject_id": procedures["PATID"].astype(str),
            "time": pd.to_datetime(procedures["event_time"], errors="coerce"),
            "event_type": "procedures",
            "code": procedures["meds_code"].astype(str),
            "value": procedures["value"],  # string dtype sequence number
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