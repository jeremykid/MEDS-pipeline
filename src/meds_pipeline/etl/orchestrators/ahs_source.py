# src/meds_pipeline/etl/orchestrators/ahs_source.py
import importlib
import sys
import pandas as pd
from tqdm import tqdm
from ..base import DataSourceETL
from ..registry import build_components, REGISTRY

MODULE_PREFIX = "meds_pipeline.etl.ahs"

class AHSSourceETL(DataSourceETL):
    def __init__(self, component_names, cfg, base_cfg):
        for n in component_names:
            importlib.import_module(f"{MODULE_PREFIX}.{n}")

        print("AFTER import, REGISTRY keys:", list(REGISTRY.keys()), file=sys.stderr)

        super().__init__(build_components(component_names, cfg, base_cfg))
        self.cfg = cfg
        self.base_cfg = base_cfg

    def to_meds_core(self) -> pd.DataFrame:
        show_progress = self.base_cfg.get("show_progress", True)
        max_patients = self.base_cfg.get("max_patients", None)
        
        parts = []
        if show_progress:
            print(f"\n🚀 Processing {len(self.components)} components...")
            if max_patients:
                print(f"📊 Patient limit: {max_patients}")
            print("="*60)
            
        for i, c in enumerate(self.components):
            if show_progress:
                print(f"\n📋 Component {i+1}/{len(self.components)}: {c.name}")
                
            df = c.run_core()
            
            if show_progress:
                rows = len(df)
                patients = df['subject_id'].nunique() if 'subject_id' in df.columns else 0
                print(f"   ✅ Generated {rows:,} rows for {patients:,} patients")
                
            parts.append(df)
        
        if show_progress:
            print(f"\n🔗 Combining all components...")
            
        result = pd.concat(parts, ignore_index=True)
        
        # Apply patient limit if requested (keep all rows for the first N unique subject_id in order of appearance)
        max_patients = self.base_cfg.get("max_patients", None)
        if max_patients:
            # preserve first-seen order of subject_id (exclude nulls)
            subj_order = result['subject_id'].dropna().astype(str).tolist()
            # keep unique while preserving order
            seen = set()
            ordered_unique = []
            for s in subj_order:
                if s not in seen:
                    seen.add(s)
                    ordered_unique.append(s)
                    if len(ordered_unique) >= max_patients:
                        break
            keep_subjs = set(ordered_unique)
            orig_rows = len(result)
            result = result[result['subject_id'].astype(str).isin(keep_subjs)].reset_index(drop=True)
            if show_progress:
                print(f"   🔧 Applied patient limit: kept {len(keep_subjs):,} patients "
                      f"({len(result):,} rows of original {orig_rows:,})")
        
        # Normalize data types to prevent parquet conversion errors
        if 'subject_id' in result.columns:
            result['subject_id'] = result['subject_id'].astype("string")
        if 'time' in result.columns:
            result['time'] = pd.to_datetime(result['time'], errors='coerce')
        
        # Fix value_num mixed types (some components use float, others use string)
        # Convert all to string to prevent pyarrow ArrowTypeError
        if 'value_num' in result.columns:
            # First convert to string, handling NaN/None properly
            result['value_num'] = result['value_num'].apply(
                lambda x: str(x) if pd.notna(x) else None
            ).astype("string")
        
        if show_progress:
            total_rows = len(result)
            total_patients = result['subject_id'].nunique() if 'subject_id' in result.columns else 0
            print(f"   ✅ Final result: {total_rows:,} rows for {total_patients:,} patients")
            
        return result

    def to_meds_plus(self) -> pd.DataFrame:
        """
        DEPRECATED: MEDS-PLUS export has been removed. Use to_meds_core() instead.
        This method is kept for backward compatibility but will raise an error.
        """
        raise RuntimeError(
            "MEDS-PLUS export has been removed. Only MEDS-CORE is supported. "
            "Please use to_meds_core() instead."
        )
