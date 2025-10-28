# src/meds_pipeline/etl/mimic/eds.py
import pandas as pd
from ..base import ComponentETL
from ..registry import register

'''
MIMIC-IV Emergency Department (ED) ETL Component

Data source: edstays.csv.gz format

Expected columns:
subject_id	hadm_id	stay_id	intime	outtime	gender	race	arrival_transport	disposition
10000032	22595853.0	33258284	2180-05-06 19:17:00	2180-05-06 23:30:00	F	WHITE	AMBULANCE	ADMITTED
10000032	22841357.0	38112554	2180-06-26 15:54:00	2180-06-26 21:31:00	F	WHITE	AMBULANCE	ADMITTED
10000032	25742920.0	35968195	2180-08-05 20:58:00	2180-08-06 01:44:00	F	WHITE	AMBULANCE	ADMITTED
10000032	29079034.0	32952584	2180-07-22 16:24:00	2180-07-23 05:54:00	F	WHITE	AMBULANCE	HOME
10000032	29079034.0	39399961	2180-07-23 05:54:00	2180-07-23 14:00:00	F	WHITE	AMBULANCE	ADMITTED

Note: Each row represents an ED stay with entry and exit times
'''

@register("eds")
class MIMICEDs(ComponentETL):
    def run_core(self) -> pd.DataFrame:
        path = self.cfg["raw_paths"]["ed"]  # Use "ed" key to match config
        df = self._read_csv_with_progress(path, "Loading ED stay data")
        
        # Validate required columns
        required_cols = ["subject_id", "intime", "outtime"]
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            raise KeyError(f"Missing required columns: {missing_cols}")
        
        # ED entry events
        entry = pd.DataFrame({
            "subject_id": df["subject_id"].astype(str),
            "time": pd.to_datetime(df["intime"], errors="coerce"),
            "event_type": "ed.entry",
            "code": "ED_ENTRY",
            "code_system": "EVENT",
        })
        
        # ED exit events
        exit = pd.DataFrame({
            "subject_id": df["subject_id"].astype(str),
            "time": pd.to_datetime(df["outtime"], errors="coerce"),
            "event_type": "ed.exit",
            "code": "ED_EXIT", 
            "code_system": "EVENT",
        })
        
        # Combine entry and exit events
        out = pd.concat([entry, exit], ignore_index=True)
        
        # Filter out invalid records
        out = out.dropna(subset=["subject_id", "time"])
        out = out[out["subject_id"].astype(str).str.strip() != ""].reset_index(drop=True)
        
        return out

    def run_plus(self) -> pd.DataFrame:
        path = self.cfg["raw_paths"]["ed"]  # Use "ed" key to match config
        df = self._read_csv_with_progress(path, "Loading ED stay data for PLUS format")
        
        # Get core data with same filtering
        core = self.run_core().reset_index(drop=True)
        
        # Apply same filtering to maintain alignment
        valid_mask = (
            df["subject_id"].notna() &
            pd.to_datetime(df["intime"], errors="coerce").notna() &
            pd.to_datetime(df["outtime"], errors="coerce").notna() &
            (df["subject_id"].astype(str).str.strip() != "")
        )
        df_filtered = df[valid_mask].reset_index(drop=True)
        
        # Prepare plus columns for both entry and exit events
        plus_cols = {}
        
        if "stay_id" in df_filtered.columns:
            # Use stay_id as encounter identifier
            stay_ids = pd.concat([df_filtered["stay_id"], df_filtered["stay_id"]], ignore_index=True)
            plus_cols["encounter_id"] = stay_ids.astype(str)
        
        if "hadm_id" in df_filtered.columns:
            # Hospital admission ID (may be null for ED-only visits)
            hadm_ids = pd.concat([df_filtered["hadm_id"], df_filtered["hadm_id"]], ignore_index=True)
            plus_cols["hadm_id"] = hadm_ids.astype(str)
        
        if "arrival_transport" in df_filtered.columns:
            # Transportation method for entry events
            transport = pd.concat([df_filtered["arrival_transport"], df_filtered["arrival_transport"]], ignore_index=True)
            plus_cols["arrival_transport"] = transport.astype(str)
        
        if "disposition" in df_filtered.columns:
            # Disposition for exit events
            disposition = pd.concat([df_filtered["disposition"], df_filtered["disposition"]], ignore_index=True)
            plus_cols["disposition"] = disposition.astype(str)
        
        if "gender" in df_filtered.columns:
            # Patient demographics
            gender = pd.concat([df_filtered["gender"], df_filtered["gender"]], ignore_index=True)
            plus_cols["gender"] = gender.astype(str)
            
        if "race" in df_filtered.columns:
            race = pd.concat([df_filtered["race"], df_filtered["race"]], ignore_index=True)
            plus_cols["race"] = race.astype(str)
        
        # Add source table
        plus_cols["source_table"] = "ed.edstays"
        
        # Create value_text with ED stay metadata
        plus_cols["value_text"] = self._assemble_ed_metadata(df_filtered)
        
        # Provenance tracking
        plus_cols["provenance_id"] = (core.index.astype(int) + 1).astype(str)
        
        # Apply plus columns to core DataFrame
        for k, v in plus_cols.items():
            if k == "value_text":
                # Special handling for value_text - duplicate for entry/exit pairs
                value_text_full = pd.concat([v, v], ignore_index=True)
                core[k] = value_text_full
            else:
                core[k] = v
            
        return core
    
    @staticmethod
    def _assemble_ed_metadata(df: pd.DataFrame) -> pd.Series:
        """
        Create human-readable metadata for ED stays, e.g.:
        "stay_id=33258284 | transport=AMBULANCE | disposition=ADMITTED | duration=4h13m"
        """
        parts = []
        
        if "stay_id" in df.columns:
            stay_txt = "stay_id=" + df["stay_id"].astype(str)
            parts.append(stay_txt)
            
        if "arrival_transport" in df.columns:
            transport_txt = "transport=" + df["arrival_transport"].astype(str)
            parts.append(transport_txt)
            
        if "disposition" in df.columns:
            disp_txt = "disposition=" + df["disposition"].astype(str)
            parts.append(disp_txt)
        
        # Calculate ED stay duration
        if "intime" in df.columns and "outtime" in df.columns:
            intime = pd.to_datetime(df["intime"], errors="coerce")
            outtime = pd.to_datetime(df["outtime"], errors="coerce")
            duration = outtime - intime
            
            # Format duration as hours and minutes
            duration_hours = duration.dt.total_seconds() / 3600
            hours = duration_hours.astype(int)
            minutes = ((duration_hours - hours) * 60).astype(int)
            duration_txt = "duration=" + hours.astype(str) + "h" + minutes.astype(str) + "m"
            parts.append(duration_txt)
            
        if not parts:
            return pd.Series([""] * len(df), index=df.index)
            
        # Combine all parts with " | " separator
        combined = pd.concat(parts, axis=1)
        metadata = combined.apply(
            lambda row: " | ".join([str(x) for x in row.tolist() if pd.notna(x) and str(x).strip() and str(x) != 'nan']),
            axis=1
        )
        return metadata

