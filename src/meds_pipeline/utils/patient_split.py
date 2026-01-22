"""Patient split utilities for MEDS pipeline."""

import pandas as pd
import numpy as np
import json
from pathlib import Path
from typing import List, Dict, Any, Optional
import pyreadstat


def get_all_patient_ids(source: str, cfg: Dict[str, Any]) -> List[str]:
    """
    Get all unique patient IDs from the data source.
    
    Parameters
    ----------
    source : str
        Data source name ("mimic" or "ahs")
    cfg : Dict[str, Any]
        Configuration dictionary containing raw_paths and keys
        
    Returns
    -------
    List[str]
        List of unique patient IDs as strings
    """
    # Get the patient ID column name from config
    patient_id_col = cfg["keys"]["subject_id"]
    
    # Get admissions file path
    admissions_path = cfg["raw_paths"]["admissions"]
    
    print(f"📖 Reading patient IDs from: {admissions_path}")
    
    # Read data based on file extension
    if admissions_path.endswith('.csv.gz') or admissions_path.endswith('.csv'):
        # MIMIC uses CSV files
        df = pd.read_csv(admissions_path, low_memory=False)
    elif admissions_path.endswith('.sas7bdat'):
        # AHS uses SAS files
        df, meta = pyreadstat.read_sas7bdat(admissions_path, output_format="pandas")
    elif admissions_path.endswith('.pickle'):
        # AHS might use pickle files
        df = pd.read_pickle(admissions_path)
    else:
        raise ValueError(f"Unsupported file format: {admissions_path}")
    
    # Extract unique patient IDs
    if patient_id_col not in df.columns:
        raise ValueError(f"Column '{patient_id_col}' not found in admissions file. Available columns: {list(df.columns)}")
    
    patient_ids = df[patient_id_col].dropna().unique().astype(str).tolist()
    
    print(f"   ✅ Found {len(patient_ids):,} unique patients")
    
    return sorted(patient_ids)  # Sort for reproducibility


def split_patients(patient_ids: List[str], dev_ratio: float = 0.6, seed: int = 42) -> Dict[str, List[str]]:
    """
    Split patient IDs into development and holdout sets.
    
    Parameters
    ----------
    patient_ids : List[str]
        List of all patient IDs
    dev_ratio : float, default=0.6
        Ratio of patients for development set (remainder goes to holdout)
    seed : int, default=42
        Random seed for reproducibility
        
    Returns
    -------
    Dict[str, List[str]]
        Dictionary with 'development' and 'holdout' keys containing patient ID lists
    """
    # Set random seed
    np.random.seed(seed)
    
    # Shuffle patient IDs
    shuffled_ids = patient_ids.copy()
    np.random.shuffle(shuffled_ids)
    
    # Calculate split point
    n_total = len(shuffled_ids)
    n_dev = int(n_total * dev_ratio)
    
    # Split
    development = sorted(shuffled_ids[:n_dev])
    holdout = sorted(shuffled_ids[n_dev:])
    
    print(f"   ✅ Split complete:")
    print(f"      - Development: {len(development):,} patients ({len(development)/n_total*100:.1f}%)")
    print(f"      - Holdout: {len(holdout):,} patients ({len(holdout)/n_total*100:.1f}%)")
    
    return {
        "development": development,
        "holdout": holdout
    }


def save_split(
    split_dict: Dict[str, Any], 
    output_path: str, 
    seed: int = 42, 
    dev_ratio: float = 0.6,
    format: str = "parquet"
) -> None:
    """
    Save patient split to Parquet file (with metadata JSON).
    
    Parameters
    ----------
    split_dict : Dict[str, Any]
        Dictionary containing 'development' and 'holdout' patient ID lists
    output_path : str
        Path to output Parquet file (metadata JSON will be saved alongside)
    seed : int, default=42
        Random seed used for split
    dev_ratio : float, default=0.6
        Development ratio used
    format : str, default="parquet"
        Output format ("parquet" or "yaml" for backward compatibility)
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    if format == "parquet":
        # Create DataFrame with patient_id as index and split as column
        all_patients = split_dict["development"] + split_dict["holdout"]
        split_values = (["development"] * len(split_dict["development"]) + 
                       ["holdout"] * len(split_dict["holdout"]))
        
        # Create DataFrame
        df = pd.DataFrame({
            "split": pd.Categorical(split_values, categories=["development", "holdout"])
        }, index=pd.Index(all_patients, name="patient_id"))
        
        # Sort by index for consistency
        df = df.sort_index()
        
        # Save to Parquet
        df.to_parquet(output_path, index=True, engine='pyarrow')
        
        # Save metadata as JSON (same name but .json extension)
        metadata_path = output_path.with_suffix('.json')
        metadata = {
            "seed": seed,
            "dev_ratio": dev_ratio,
            "holdout_ratio": 1.0 - dev_ratio,
            "total_patients": len(all_patients),
            "dev_patients": len(split_dict["development"]),
            "holdout_patients": len(split_dict["holdout"]),
            "format": "parquet",
            "schema": {
                "index_name": "patient_id",
                "index_type": "string",
                "columns": {
                    "split": {
                        "type": "categorical",
                        "categories": ["development", "holdout"]
                    }
                }
            }
        }
        
        with open(metadata_path, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=2)
        
        print(f"💾 Saved split to: {output_path}")
        print(f"💾 Saved metadata to: {metadata_path}")
        print(f"   DataFrame shape: {df.shape[0]:,} patients, {df.shape[1]} column(s)")
        
    else:  # YAML format for backward compatibility
        import yaml
        output_data = {
            "seed": seed,
            "dev_ratio": dev_ratio,
            "holdout_ratio": 1.0 - dev_ratio,
            "total_patients": len(split_dict["development"]) + len(split_dict["holdout"]),
            "dev_patients": len(split_dict["development"]),
            "holdout_patients": len(split_dict["holdout"]),
            "development": split_dict["development"],
            "holdout": split_dict["holdout"]
        }
        
        with open(output_path, 'w', encoding='utf-8') as f:
            yaml.dump(output_data, f, default_flow_style=False, allow_unicode=True, sort_keys=False)
        
        print(f"💾 Saved split to: {output_path}")


def load_split(split_path: str) -> pd.DataFrame:
    """
    Load patient split from Parquet file.
    
    Parameters
    ----------
    split_path : str
        Path to Parquet file containing patient split
        
    Returns
    -------
    pd.DataFrame
        DataFrame with patient_id as index and 'split' column (categorical)
    """
    split_path = Path(split_path)
    df = pd.read_parquet(split_path, engine='pyarrow')
    
    # Ensure split column is categorical
    if 'split' in df.columns:
        df['split'] = df['split'].astype('category')
    
    return df
