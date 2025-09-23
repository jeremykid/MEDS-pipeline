# src/meds_pipeline/etl/orchestrators/mimic_source.py
import importlib
import sys
import pandas as pd
from ..base import DataSourceETL
from ..registry import build_components, REGISTRY

MODULE_PREFIX = "meds_pipeline.etl.mimic"

class MIMICSourceETL(DataSourceETL):
    def __init__(self, component_names, cfg, base_cfg):
        for n in component_names:
            importlib.import_module(f"{MODULE_PREFIX}.{n}")

        print("AFTER import, REGISTRY keys:", list(REGISTRY.keys()), file=sys.stderr)

        super().__init__(build_components(component_names, cfg, base_cfg))
        self.cfg = cfg
        self.base_cfg = base_cfg

    def to_meds_core(self) -> pd.DataFrame:
        parts = [c.run_core() for c in self.components]
        return pd.concat(parts, ignore_index=True)

    def to_meds_plus(self) -> pd.DataFrame:
        parts = [c.run_plus() for c in self.components]
        return pd.concat(parts, ignore_index=True)
