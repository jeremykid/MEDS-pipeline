#!/usr/bin/env python3
"""
Independent script to split patients into development and holdout sets.

This script reads patient IDs from the data source and splits them into
60% development set and 40% holdout set using a random split with seed=42.

Usage:
    PYTHONPATH=src python src/split_patients.py --source mimic --cfg mimic.yaml
    PYTHONPATH=src python src/split_patients.py --source ahs --cfg ahs.yaml
    PYTHONPATH=src python src/split_patients.py --source mimic --cfg mimic.yaml --output custom_split.parquet
"""

import argparse
import sys
from pathlib import Path
import yaml

# Add src to path to import meds_pipeline modules
sys.path.insert(0, str(Path(__file__).parent))

from meds_pipeline.utils.patient_split import get_all_patient_ids, split_patients, save_split


def load_config(path_or_pkg_rel: str) -> dict:
    """
    Load YAML configuration file.
    
    Parameters
    ----------
    path_or_pkg_rel : str
        Path to config file or package-relative path
        
    Returns
    -------
    dict
        Configuration dictionary
    """
    p = Path(path_or_pkg_rel)
    
    # Try absolute or relative path first
    if p.exists():
        with open(p, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    
    # Try in configs directory relative to script
    configs_dir = Path(__file__).parent / "meds_pipeline" / "configs"
    config_path = configs_dir / path_or_pkg_rel
    if config_path.exists():
        with open(config_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    
    # Try package-relative path (for installed packages)
    try:
        from importlib.resources import files
        pkg_root = files("meds_pipeline.configs")
        with open(pkg_root / path_or_pkg_rel, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    except (ImportError, FileNotFoundError):
        pass
    
    # If all else fails, raise error
    raise FileNotFoundError(f"Could not find config file: {path_or_pkg_rel}")


def main():
    parser = argparse.ArgumentParser(
        description="Split patients into development (60%) and holdout (40%) sets",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    parser.add_argument(
        "--source",
        type=str,
        choices=["mimic", "ahs"],
        required=True,
        help="Data source: 'mimic' or 'ahs'"
    )
    
    parser.add_argument(
        "--cfg",
        type=str,
        default=None,
        help="Path to source-specific config file (e.g., mimic.yaml or ahs.yaml). "
             "If not provided, defaults to {source}.yaml in configs directory."
    )
    
    parser.add_argument(
        "--base",
        type=str,
        default="base.yaml",
        help="Path to base config file (default: base.yaml)"
    )
    
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Output path for split Parquet file. "
             "If not provided, defaults to configs/{source}_split.parquet"
    )
    
    parser.add_argument(
        "--dev-ratio",
        type=float,
        default=0.6,
        help="Ratio of patients for development set (default: 0.6)"
    )
    
    parser.add_argument(
        "--format",
        type=str,
        choices=["parquet", "yaml"],
        default="parquet",
        help="Output format: 'parquet' (default) or 'yaml' (legacy)"
    )
    
    args = parser.parse_args()
    
    # Determine config file path
    if args.cfg is None:
        args.cfg = f"{args.source}.yaml"
    
    # Load configurations
    print(f"📋 Loading configuration files...")
    cfg = load_config(args.cfg)
    base_cfg = load_config(args.base)
    
    # Get seed from base config
    seed = base_cfg.get("seed", 42)
    print(f"   Using seed: {seed}")
    
    # Determine output path
    if args.output is None:
        # Default to configs directory
        configs_dir = Path(__file__).parent / "meds_pipeline" / "configs"
        extension = "parquet" if args.format == "parquet" else "yaml"
        output_path = configs_dir / f"{args.source}_split.{extension}"
    else:
        output_path = Path(args.output)
    
    print(f"📊 Source: {args.source}")
    print(f"📁 Config: {args.cfg}")
    print(f"💾 Output: {output_path} ({args.format} format)")
    print("=" * 60)
    
    # Get all patient IDs
    patient_ids = get_all_patient_ids(args.source, cfg)
    
    # Split patients
    split_result = split_patients(patient_ids, dev_ratio=args.dev_ratio, seed=seed)
    
    # Save split
    save_split(split_result, str(output_path), seed=seed, dev_ratio=args.dev_ratio, format=args.format)
    
    print("=" * 60)
    print("✅ Patient split completed successfully!")


if __name__ == "__main__":
    main()
