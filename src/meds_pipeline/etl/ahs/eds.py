# src/meds_pipeline/etl/ahs/eds.py
import pandas as pd
from ..base import ComponentETL
from ..registry import register

'''
AHS Emergency Department (ED) ETL Component

Data source: ED visits from pickle file

Expected columns:
PATID	VISIT_DATE_DT	DXCODE1	DXCODE2	...	DXCODE10	PROCCODE1	PROCCODE2	...	PROCCODE10
46	2021-05-24	R53	...
128	2021-05-19	F151	F155	...
572	2021-05-23	H609	...
1072	2021-05-22	J9691	I500	E875	...	3GY10VA	3IP30DD
1232	2021-05-25	R1012	E1023	N0839	...	3OT10VA	3GY10VA

Note: This represents ED visit encounters, separate from hospital admissions
'''

@register("eds")
class AHSEDs(ComponentETL):
    
    def _load_ed_data(self):
        """Load ED data from pickle file with compatibility handling"""
        path = self.cfg["raw_paths"]["ed"]
        try:
            ed_df = pd.read_pickle(path)
            return ed_df
        except (ModuleNotFoundError, AttributeError) as e:
            print(f"⚠️  ED pickle compatibility issue: {e}")
            print("🔄 Attempting to load with pandas compatibility mode...")
            try:
                import pandas.compat.pickle_compat as pc
                with open(path, 'rb') as f:
                    ed_df = pc.load(f)
                print("✅ Successfully loaded ED data with compatibility mode")
                return ed_df
            except Exception as final_error:
                raise RuntimeError(f"Failed to load ED pickle file: {final_error}")
    
    def run_core(self) -> pd.DataFrame:
        df = self._load_ed_data()
        
        # Validate required columns
        required_cols = ["PATID", "VISIT_DATE_DT"]
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            raise KeyError(f"Missing required columns: {missing_cols}")
        
        # Create ED visit encounters (only start events since ED visits typically don't have discharge times)
        ed_visits = pd.DataFrame({
            "subject_id": df["PATID"].astype(str),
            "time": pd.to_datetime(df["VISIT_DATE_DT"], errors="coerce"),
            "event_type": "ed_visit",
            "code": "ED_VISIT",
            "code_system": "AHS_ED",
        })
        
        # Filter out invalid records
        ed_visits = ed_visits.dropna(subset=["subject_id", "time"])
        ed_visits = ed_visits[ed_visits["subject_id"].astype(str).str.strip() != ""].reset_index(drop=True)
        
        return ed_visits
    
    def run_plus(self) -> pd.DataFrame:
        df = self._load_ed_data()
        
        # Get core data with same filtering
        core = self.run_core().reset_index(drop=True)
        
        # Apply same filtering to maintain alignment
        valid_mask = (
            df["PATID"].notna() &
            pd.to_datetime(df["VISIT_DATE_DT"], errors="coerce").notna() &
            (df["PATID"].astype(str).str.strip() != "")
        )
        df_filtered = df[valid_mask].reset_index(drop=True)
        
        # Add ED-specific metadata
        core["source_table"] = "AHS_ED"
        core["encounter_type"] = "emergency_department"
        
        # Count number of diagnosis and procedure codes for this visit
        dx_cols = [col for col in df_filtered.columns if col.startswith('DXCODE') and col != 'DXCODE1']
        proc_cols = [col for col in df_filtered.columns if col.startswith('PROCCODE')]
        
        if dx_cols:
            # Count non-null diagnosis codes
            dx_count = df_filtered[['DXCODE1'] + dx_cols].notna().sum(axis=1)
            core["diagnosis_count"] = dx_count.astype(str)
        
        if proc_cols:
            # Count non-null procedure codes
            proc_count = df_filtered[proc_cols].notna().sum(axis=1)
            core["procedure_count"] = proc_count.astype(str)
        
        # Create value_text with visit summary
        core["value_text"] = self._assemble_ed_metadata(df_filtered)
        
        # Provenance tracking
        core["provenance_id"] = (core.index.astype(int) + 1).astype(str)
        
        return core
    
    @staticmethod
    def _assemble_ed_metadata(df: pd.DataFrame) -> pd.Series:
        """
        Create human-readable metadata for ED visits, e.g.:
        "patid=46 | visit_date=2021-05-24 | dx_count=1 | proc_count=0"
        """
        parts = []
        
        if "PATID" in df.columns:
            patid_txt = "patid=" + df["PATID"].astype(str)
            parts.append(patid_txt)
            
        if "VISIT_DATE_DT" in df.columns:
            visit_txt = "visit_date=" + pd.to_datetime(df["VISIT_DATE_DT"], errors="coerce").dt.strftime('%Y-%m-%d')
            parts.append(visit_txt)
        
        # Count diagnosis codes
        dx_cols = [col for col in df.columns if col.startswith('DXCODE')]
        if dx_cols:
            dx_count = df[dx_cols].notna().sum(axis=1)
            dx_count_txt = "dx_count=" + dx_count.astype(str)
            parts.append(dx_count_txt)
        
        # Count procedure codes
        proc_cols = [col for col in df.columns if col.startswith('PROCCODE')]
        if proc_cols:
            proc_count = df[proc_cols].notna().sum(axis=1)
            proc_count_txt = "proc_count=" + proc_count.astype(str)
            parts.append(proc_count_txt)
            
        if not parts:
            return pd.Series([""] * len(df), index=df.index)
            
        # Combine all parts with " | " separator
        combined = pd.concat(parts, axis=1)
        metadata = combined.apply(
            lambda row: " | ".join([str(x) for x in row.tolist() if pd.notna(x) and str(x).strip() and str(x) != 'NaT']),
            axis=1
        )
        return metadata								
