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
        
        # Create core MEDS structure
        out = pd.DataFrame({
            "subject_id": df["subject_id"].astype(str),
            "time": pd.to_datetime(df["ecg_time"], errors="coerce"),
            "event_type": "ecg_recording",
            "code": "ECG",  # Standard code for ECG recordings
            "code_system": "MIMIC_ECG",
        })
        
        # Filter out invalid records
        out = out.dropna(subset=["subject_id", "time"])
        out = out[out["subject_id"].astype(str).str.strip() != ""].reset_index(drop=True)
        return out
    
    def run_plus(self) -> pd.DataFrame:
        path = self.cfg["raw_paths"]["ecgs"]
        df = self._read_csv_with_progress(path, "Loading ECG data")
        
        # Get core data with same filtering
        core = self.run_core().reset_index(drop=True)
        
        # Apply same filtering to original df to maintain alignment
        valid_mask = (
            df["subject_id"].notna() &
            pd.to_datetime(df["ecg_time"], errors="coerce").notna() &
            (df["subject_id"].astype(str).str.strip() != "")
        )
        df_filtered = df[valid_mask].reset_index(drop=True)
        
        # Add ECG-specific metadata for feature extraction
        if "study_id" in df_filtered.columns:
            core["study_id"] = df_filtered["study_id"].astype(str)
            
        if "file_name" in df_filtered.columns:
            core["file_name"] = df_filtered["file_name"].astype(str)
            
        # Store ECG file path for waveform feature extraction (most important field)
        if "path" in df_filtered.columns:
            core["ecg_file_path"] = df_filtered["path"].astype(str)
            
        # Add value_text with other ECG metadata for auditing (excluding path)
        core["value_text"] = self._assemble_ecg_metadata(df_filtered)
        
        # Provenance tracking
        core["source_table"] = "ECG_RECORDS"
        core["provenance_id"] = (core.index.astype(int) + 1).astype(str)
        
        return core
    
    @staticmethod
    def _assemble_ecg_metadata(df: pd.DataFrame) -> pd.Series:
        """
        Create human-readable metadata for ECG records (excluding path), e.g.:
        "study_id=40689238 | file=40689238"
        Path is stored separately in ecg_file_path field for feature extraction.
        """
        parts = []
        
        if "study_id" in df.columns:
            study_txt = "study_id=" + df["study_id"].astype(str)
            parts.append(study_txt)
            
        if "file_name" in df.columns:
            file_txt = "file=" + df["file_name"].astype(str)
            parts.append(file_txt)
            
        # Note: path is intentionally excluded and stored separately in ecg_file_path
            
        if not parts:
            return pd.Series([""] * len(df), index=df.index)
            
        # Combine all parts with " | " separator
        combined = pd.concat(parts, axis=1)
        metadata = combined.apply(
            lambda row: " | ".join([str(x) for x in row.tolist() if pd.notna(x) and str(x).strip()]),
            axis=1
        )
        return metadata
