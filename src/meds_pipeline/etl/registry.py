# src/meds_pipeline/etl/registry.py
from typing import Dict, Type
from .base import ComponentETL
REGISTRY: Dict[str, Type[ComponentETL]] = {}

def register(name: str):
    def deco(cls):
        print(f"REGISTER component: {name} -> {cls.__name__}")
        REGISTRY[name] = cls
        cls.name = name
        return cls
    return deco

def build_components(names, cfg, base_cfg):
    return [REGISTRY[n](cfg, base_cfg) for n in names]
