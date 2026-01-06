"""
MIMIC Code Mapper

A code-to-description mapping system for MIMIC-IV medical coding standards:
- ICD-9/ICD-10 Diagnosis codes
- ICD-9/ICD-10 Procedure codes

Data source: MIMIC-IV (PhysioNet)
https://physionet.org/content/mimiciv/

This module can be used within MEDS-pipeline or as a standalone library.
"""

from .mapper import MIMICCodeMapper
from .registry import MIMICMapperRegistry, init_mimic_mappers

__version__ = "0.1.0"

__all__ = ["MIMICCodeMapper", "MIMICMapperRegistry", "init_mimic_mappers"]
