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
        
        if show_progress:
            total_rows = len(result)
            total_patients = result['subject_id'].nunique() if 'subject_id' in result.columns else 0
            print(f"   ✅ Final result: {total_rows:,} rows for {total_patients:,} patients")
            
        return result

    def to_meds_plus(self) -> pd.DataFrame:
        show_progress = self.base_cfg.get("show_progress", True)
        max_patients = self.base_cfg.get("max_patients", None)
        
        parts = []
        if show_progress:
            print(f"\n🚀 Processing {len(self.components)} components for MEDS-PLUS...")
            if max_patients:
                print(f"📊 Patient limit: {max_patients}")
            print("="*60)
            
        for i, c in enumerate(self.components):
            if show_progress:
                print(f"\n📋 Component {i+1}/{len(self.components)}: {c.name}")
                
            df = c.run_plus()
            
            if show_progress:
                rows = len(df)
                patients = df['subject_id'].nunique() if 'subject_id' in df.columns else 0
                print(f"   ✅ Generated {rows:,} rows for {patients:,} patients")
                
            parts.append(df)
        
        if show_progress:
            print(f"\n🔗 Combining all components...")
            
        result = pd.concat(parts, ignore_index=True)
        
        if show_progress:
            total_rows = len(result)
            total_patients = result['subject_id'].nunique() if 'subject_id' in result.columns else 0
            print(f"   ✅ Final result: {total_rows:,} rows for {total_patients:,} patients")
            
        return result
