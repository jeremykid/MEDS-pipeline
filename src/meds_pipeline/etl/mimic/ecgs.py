# src/meds_pipeline/etl/mimic/ecg.py
import pandas as pd
from ..base import ComponentETL
from ..registry import register

'''
MIMIC-IV ECG ETL Component

Data source: /data/padmalab_external/special_project/physionet.org/files/mimic-iv-ecg/1.0/record_list.csv

Expected columns:
- subject_id: Patient identifier
- study_id: ECG study identifier  
- file_name: ECG file name
- ecg_time: Timestamp of ECG recording
- path: File path for ECG waveform data (for feature extraction)

Example data:
subject_id	study_id	file_name	ecg_time	path
10000032	40689238	40689238	2180-07-23 08:44:00	files/p1000/p10000032/s40689238/40689238
10000032	44458630	44458630	2180-07-23 09:54:00	files/p1000/p10000032/s44458630/44458630
10000032	49036311	49036311	2180-08-06 09:07:00	files/p1000/p10000032/s49036311/49036311
'''
@register("ecgs")
class MIMICECGs(ComponentETL):
    def run_core(self) -> pd.DataFrame:
        path = self.cfg["raw_paths"]["ecgs"]
        df = self._read_csv_with_progress(path, "Loading ECG data")
        
        # Validate required columns
        required_cols = ["subject_id", "ecg_time", "path"]
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            raise KeyError(f"Missing required columns: {missing_cols}")
        
        # Build valid mask for filtering
        valid_mask = (
            df["subject_id"].notna() &
            pd.to_datetime(df["ecg_time"], errors="coerce").notna() &
            (df["subject_id"].astype(str).str.strip() != "")
        )
        
        # Apply filtering to original dataframe
        df_filtered = df[valid_mask].reset_index(drop=True)
        
        # Create core MEDS structure with value_text containing ECG path
        out = pd.DataFrame({
            "subject_id": df_filtered["subject_id"].astype(str),
            "time": pd.to_datetime(df_filtered["ecg_time"], errors="coerce"),
            "event_type": "ECG",
            "code": "ECG//WAVEFORM",  # Standard code for ECG recordings
            "code_system": "MIMIC_ECG",
            "value_text": df_filtered["path"].astype(str),  # ECG path for waveform extraction
        })
        
        # Add provenance tracking
        out["source_table"] = "ECG_RECORDS"
        out["provenance_id"] = (out.index.astype(int) + 1).astype(str)
        
        return out

