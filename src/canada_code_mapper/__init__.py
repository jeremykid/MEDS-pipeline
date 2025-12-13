"""
Canada Code Mapper

A focused code-to-description mapping system for Canadian medical coding standards:
- ICD-10-CA (International Classification of Diseases, 10th Revision, Canada)
- CCI (Canadian Classification of Health Interventions)

Data source: CIHI (Canadian Institute for Health Information)
https://secure.cihi.ca/estore/productSeries.htm?pc=PCC84

This module can be used within MEDS-pipeline or as a standalone library.
"""

from .mapper import CodeMapper
from .registry import MapperRegistry

__version__ = "0.1.0"

__all__ = ["CodeMapper", "MapperRegistry"]
