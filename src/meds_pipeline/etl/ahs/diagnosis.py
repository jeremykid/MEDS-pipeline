# src/meds_pipeline/etl/ahs/diagnosis.py
import pandas as pd
import pyreadstat
from ..base import ComponentETL
from ..registry import register

'''
AHS Diagnosis ETL Component

Data sources:
- DAD (Discharge Abstract Database): SAS format with DXCODE1-DXCODE25
  * Hospital/inpatient diagnoses
  * Uses ICD-10-CA coding system
  
- ED (Emergency Department): Pickle format with DXCODE1-DXCODE10
  * Emergency department diagnoses
  * Uses ICD-10-CA coding system

Expected DAD columns:
PATID	ADMITDATE_DT	DISDATE_DT	AGE_ADMIT	SEX	DXCODE1	DXCODE2	...	DXCODE25
1343.0	2021-08-19	2021-09-04	83.0	M	M1000	M130	M6593	M1196	Z751	...

Expected ED columns:
PATID	VISIT_DATE_DT	DXCODE1	DXCODE2	...	DXCODE10
46	2021-05-24	R53	...
128	2021-05-19	F151	F155	...

Note: All diagnosis codes appear to be ICD-10-CA (Canadian version)
'''

@register("diagnosis")
class AHSDiagnosis(ComponentETL):
    
    @staticmethod
    def _build_diagnosis_code(icd_code: str) -> str:
        """
        Build MEDS-compliant diagnosis code in the format:
        DIAGNOSIS//ICD10CA//code
        
        Args:
            icd_code: The raw ICD-10-CA code
            
        Returns:
            Formatted diagnosis code string
        """
        if pd.isna(icd_code) or str(icd_code).strip() == "":
            return None
        
        return f"DIAGNOSIS//ICD10CA//{icd_code}"
    
    def _load_dad_data(self):
        """Load DAD data from SAS file"""
        dad_path = "/data/padmalab_external/special_project/AHS_Data_Release_2/rmt22884_dad_20211105.sas7bdat"
        dad_df, meta = pyreadstat.read_sas7bdat(dad_path, output_format="pandas")
        return dad_df
    
    def _load_ed_data(self):
        """Load ED data from pickle file with compatibility handling"""
        ed_path = "/data/padmalab_external/special_project/AHS_Data_Release_2/rmt22884_ed_20211105.pickle"
        try:
            ed_df = pd.read_pickle(ed_path)
            return ed_df
        except (ModuleNotFoundError, AttributeError) as e:
            print(f"⚠️  ED pickle compatibility issue: {e}")
            print("🔄 Attempting to load with pandas compatibility mode...")
            try:
                import pandas.compat.pickle_compat as pc
                with open(ed_path, 'rb') as f:
                    ed_df = pc.load(f)
                print("✅ Successfully loaded ED data with compatibility mode")
                return ed_df
            except Exception as final_error:
                raise RuntimeError(f"Failed to load ED pickle file: {final_error}")
    
    def _extract_diagnosis_codes(self, df, source_type):
        """
        Extract diagnosis codes from DAD or ED data.
        
        Args:
            df: DataFrame containing diagnosis codes
            source_type: 'DAD' or 'ED' to determine number of DXCODE columns
        
        Returns:
            DataFrame with melted diagnosis codes and diagnosis_source column
        """
        if source_type == 'DAD':
            dx_columns = [f'DXCODE{i}' for i in range(1, 26)]  # DXCODE1-DXCODE25
            time_col = 'ADMITDATE_DT'
            diagnosis_source = 'INPATIENT'
        elif source_type == 'ED':
            dx_columns = [f'DXCODE{i}' for i in range(1, 11)]  # DXCODE1-DXCODE10
            time_col = 'VISIT_DATE_DT'
            diagnosis_source = 'ED'
        else:
            raise ValueError(f"Unknown source_type: {source_type}")
        
        # Keep only existing columns
        existing_dx_cols = [col for col in dx_columns if col in df.columns]
        
        if not existing_dx_cols:
            raise KeyError(f"No diagnosis code columns found in {source_type} data")
        
        # Prepare data for melting
        id_vars = ['PATID', time_col]
        df_subset = df[id_vars + existing_dx_cols].copy()
        
        # Melt diagnosis codes into long format
        melted = df_subset.melt(
            id_vars=id_vars,
            value_vars=existing_dx_cols,
            var_name='dx_sequence',
            value_name='diagnosis_code'
        )
        
        # Extract sequence number from column name (DXCODE1 -> 1)
        melted['sequence_num'] = melted['dx_sequence'].str.extract(r'DXCODE(\d+)').astype(int)
        
        # Add source information
        melted['source_table'] = source_type
        melted['diagnosis_source'] = diagnosis_source
        
        # Filter out empty/null diagnosis codes
        melted = melted[melted['diagnosis_code'].notna()]
        melted = melted[melted['diagnosis_code'].astype(str).str.strip() != '']
        
        return melted
    
    def run_core(self) -> pd.DataFrame:
        # Load both data sources
        dad_df = self._load_dad_data()
        ed_df = self._load_ed_data()
        
        # Extract diagnosis codes from both sources
        dad_diagnoses = self._extract_diagnosis_codes(dad_df, 'DAD')
        ed_diagnoses = self._extract_diagnosis_codes(ed_df, 'ED')
        
        # Rename time columns to standardize
        dad_diagnoses = dad_diagnoses.rename(columns={'ADMITDATE_DT': 'event_time'})
        ed_diagnoses = ed_diagnoses.rename(columns={'VISIT_DATE_DT': 'event_time'})
        
        # Combine both datasets
        all_diagnoses = pd.concat([dad_diagnoses, ed_diagnoses], ignore_index=True)
        
        # Build MEDS-compliant diagnosis codes
        all_diagnoses['meds_code'] = all_diagnoses['diagnosis_code'].apply(self._build_diagnosis_code)
        
        # Create MEDS core structure
        out = pd.DataFrame({
            "subject_id": all_diagnoses["PATID"].astype(str),
            "time": pd.to_datetime(all_diagnoses["event_time"], errors="coerce"),
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
        "source=INPATIENT | table=DAD | seq=1 | dx_col=DXCODE1"
        or
        "source=ED | table=ED | seq=5 | dx_col=DXCODE5"
        """
        parts = []
        
        # Add diagnosis source (INPATIENT vs ED)
        if "diagnosis_source" in df.columns:
            diag_source_txt = "source=" + df["diagnosis_source"].astype(str)
            parts.append(diag_source_txt)
        
        # Add source table (DAD vs ED)
        if "source_table" in df.columns:
            table_txt = "table=" + df["source_table"].astype(str)
            parts.append(table_txt)
            
        if "sequence_num" in df.columns:
            seq_txt = "seq=" + df["sequence_num"].astype(str)
            parts.append(seq_txt)
            
        if "dx_sequence" in df.columns:
            dx_col_txt = "dx_col=" + df["dx_sequence"].astype(str)
            parts.append(dx_col_txt)
            
        if not parts:
            return pd.Series([""] * len(df), index=df.index)
            
        # Combine all parts with " | " separator
        combined = pd.concat(parts, axis=1)
        metadata = combined.apply(
            lambda row: " | ".join([str(x) for x in row.tolist() if pd.notna(x) and str(x).strip()]),
            axis=1
        )
        return metadata
