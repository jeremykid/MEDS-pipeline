
# src/meds_pipeline/etl/ahs/ecgs.py
import pandas as pd
import pickle
import re
from ..base import ComponentETL
from ..registry import register

'''
AHS ECG ETL Component

Data source: /data/padmalab_external/special_project/AHS_Data_Release_2/rmt22884_ecg_20211105_df.pickle

Expected columns:
- PATID: Patient identifier (maps to subject_id)
- ecgId: ECG identifier (contains GUID in N'...' format)
- dateAcquired: Timestamp of ECG recording (maps to time)
- dateConfirmed: Confirmation timestamp

Example data:
PATID	ecgId	dateAcquired	dateConfirmed
219126	N'407be100-3b36-11dc-4823-000206d60029'	2007-07-25 23:02:25	2007-07-27 21:59:36
219126	N'b44d0080-71ed-11dc-4823-00102de50029'	2007-10-03 14:11:31	2007-10-04 21:34:01
219126	N'ee6b9780-6f8c-11dc-4823-003316050029'	2007-09-30 13:33:47	2007-10-05 22:24:21
'''

@register("ecgs")
class AHSECGs(ComponentETL):
    
    @staticmethod
    def _clean_ecg_id(ecg_id_series):
        """
        Extract GUID from N'...' format ECG IDs.
        Example: N'407be100-3b36-11dc-4823-000206d60029' -> 407be100-3b36-11dc-4823-000206d60029
        """
        def clean_single_id(ecg_id):
            if pd.isna(ecg_id):
                return ""
            ecg_str = str(ecg_id)
            # Remove N' prefix and ' suffix if present
            match = re.search(r"N'([^']+)'", ecg_str)
            if match:
                return match.group(1)
            # Fallback: remove N' and trailing '
            if ecg_str.startswith("N'"):
                ecg_str = ecg_str[2:]
            if ecg_str.endswith("'"):
                ecg_str = ecg_str[:-1]
            return ecg_str.strip()
        
        return ecg_id_series.apply(clean_single_id)
    
    def _load_ecg_data(self):
        """Load ECG data from pickle file with compatibility handling"""
        path = self.cfg["raw_paths"]["ecg"]  # Use "ecg" key to match config
        
        try:
            # Try normal pickle loading first
            with open(path, 'rb') as f:
                df = pickle.load(f)
            return df
        except (ModuleNotFoundError, AttributeError) as e:
            # Handle pandas version compatibility issues
            print(f"⚠️  Pickle compatibility issue detected: {e}")
            print("🔄 Attempting to load with pandas compatibility mode...")
            
            try:
                # Try with pandas.compat
                import pandas.compat.pickle_compat as pc
                with open(path, 'rb') as f:
                    df = pc.load(f)
                print("✅ Successfully loaded with pandas compatibility mode")
                return df
            except Exception:
                # Fallback: try loading with protocol=2 or older
                try:
                    # Try reading as pandas directly
                    df = pd.read_pickle(path)
                    print("✅ Successfully loaded with pd.read_pickle")
                    return df
                except Exception as final_error:
                    raise RuntimeError(
                        f"Failed to load pickle file {path}. "
                        f"This might be due to pandas version incompatibility. "
                        f"Original error: {e}, Final error: {final_error}"
                    )
    
    def run_core(self) -> pd.DataFrame:
        df = self._load_ecg_data()
        
        # Validate required columns
        required_cols = ["PATID", "dateAcquired", "ecgId"]
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            raise KeyError(f"Missing required columns: {missing_cols}")
        
        # Clean ECG IDs (remove N'...' wrapper)
        clean_ecg_ids = self._clean_ecg_id(df["ecgId"])
        
        # Build valid mask for filtering
        valid_mask = (
            df["PATID"].notna() &
            pd.to_datetime(df["dateAcquired"], errors="coerce").notna() &
            (df["PATID"].astype(str).str.strip() != "")
        )
        
        # Apply filtering to original dataframe and cleaned IDs
        df_filtered = df[valid_mask].reset_index(drop=True)
        clean_ecg_ids_filtered = clean_ecg_ids[valid_mask].reset_index(drop=True)
        
        # Create core MEDS structure with value_text containing ECG ID
        out = pd.DataFrame({
            "subject_id": df_filtered["PATID"].astype(str),
            "time": pd.to_datetime(df_filtered["dateAcquired"], errors="coerce"),
            "event_type": "ECG",
            "code": "ECG//WAVEFORM",  # Standard code for ECG recordings
            "code_system": "AHS_ECG",
            "value_text": clean_ecg_ids_filtered.astype(str),  # ECG ID for waveform extraction
        })
        
        # Add provenance tracking
        out["source_table"] = "AHS_ECG"
        out["provenance_id"] = (out.index.astype(int) + 1).astype(str)
        
        return out

