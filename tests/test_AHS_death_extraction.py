"""
AHS Death Extraction Module

Extracts death information from multiple AHS data sources:
- Vital Status (priority 1)
- Registry (priority 2, requires DEATH_IND == 1)
- DAD (priority 3, requires DISP in {7, 8, 10, 11})

Usage:
    PYTHONPATH=src python3 tests/test_AHS_death_extraction.py --cfg ahs.yaml --verbose --show-sample
Or as a module:
    from tests.test_death_extraction import build_death_table
    df = build_death_table("ahs.yaml")
"""

from pathlib import Path
from typing import Tuple, Optional, Dict, Any, Callable
import pandas as pd
import pyreadstat
import yaml
import logging
import argparse
from importlib.resources import files

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s: %(message)s'
)
logger = logging.getLogger(__name__)


def read_file(path: str | Path, raise_on_missing: bool = False) -> Tuple[pd.DataFrame, Optional[Dict[str, Any]]]:
    """
    Read a file and return (df, meta).
    
    Args:
        path: Path to the file
        raise_on_missing: If True, raise FileNotFoundError; if False, return empty DataFrame
    
    Returns:
        Tuple of (DataFrame, metadata dict or None)
        
    Supported formats:
        - SAS files (.sas7bdat) returns (df, meta) from pyreadstat.read_sas7bdat
        - Pickle files (.pickle, .pkl) returns (df, None)
        - CSV files (.csv) returns (df, None)
        - Parquet files (.parquet) returns (df, None)
    """
    p = Path(path)
    if not p.exists():
        if raise_on_missing:
            raise FileNotFoundError(f"File not found: {p}")
        else:
            logger.warning(f"File not found: {p}, returning empty DataFrame")
            return pd.DataFrame(), None

    name_lower = p.name.lower()
    suffix = p.suffix.lower()

    try:
        # SAS (handle typical extension and possible misspelling)
        if suffix == ".sas7bdat" or name_lower.endswith(".sas7bdat") or name_lower.endswith(".sas7dbat"):
            df, meta = pyreadstat.read_sas7bdat(str(p), output_format="pandas")
            logger.debug(f"Read SAS file: {p}, shape: {df.shape}")
            return df, meta

        # Pickle
        if suffix in (".pickle", ".pkl"):
            df = pd.read_pickle(str(p))
            logger.debug(f"Read pickle file: {p}, shape: {df.shape}")
            return df, None

        # Common fallbacks
        if suffix == ".csv":
            df = pd.read_csv(str(p))
            logger.debug(f"Read CSV file: {p}, shape: {df.shape}")
            return df, None
            
        if suffix == ".parquet":
            df = pd.read_parquet(str(p))
            logger.debug(f"Read parquet file: {p}, shape: {df.shape}")
            return df, None

        raise ValueError(f"Unsupported file type: {p.suffix}")
        
    except Exception as e:
        logger.error(f"Error reading file {p}: {e}")
        if raise_on_missing:
            raise
        return pd.DataFrame(), None

def _load_config(path_or_pkg_rel: str) -> dict:
    """Load YAML configuration from file or package resource."""
    p = Path(path_or_pkg_rel)
    if p.exists():  
        return yaml.safe_load(p.read_text(encoding="utf-8"))

    pkg_root = files("meds_pipeline.configs")
    return yaml.safe_load((pkg_root / path_or_pkg_rel).read_text(encoding="utf-8"))


def normalize_patid_series(s: pd.Series) -> pd.Series:
    """
    Normalize a PATID series to nullable Int64.
    
    Args:
        s: Input series (may contain mixed types, NaN, etc.)
        
    Returns:
        Normalized series with dtype Int64
    """
    return pd.to_numeric(s, errors="coerce").dropna().astype("Int64")


def _assign_death_from_source(
    all_patids_df: pd.DataFrame,
    src_df: pd.DataFrame,
    date_col: str,
    source_name: str,
    filter_mask: Optional[Callable[[pd.DataFrame], pd.Series]] = None
) -> None:
    """
    Assign death dates from a source dataframe to all_patids_df (in-place).
    
    Only updates rows where death_date is currently NaT (respects priority).
    
    Args:
        all_patids_df: Master dataframe with PATID, death_date, death_source columns
        src_df: Source dataframe containing PATID and death date column
        date_col: Name of the date column in src_df
        source_name: Name to assign to death_source column
        filter_mask: Optional function to filter src_df rows before processing
    """
    if not isinstance(src_df, pd.DataFrame) or "PATID" not in src_df.columns:
        logger.warning(f"Source {source_name}: invalid dataframe or missing PATID column")
        return
    
    src = src_df
    if filter_mask is not None:
        try:
            mask = filter_mask(src_df)
            src = src.loc[mask]
        except Exception as e:
            logger.warning(f"Source {source_name}: filter failed: {e}")
            return
    
    if src.empty:
        logger.debug(f"Source {source_name}: no rows after filtering")
        return
    
    if date_col not in src.columns:
        logger.warning(f"Source {source_name}: date column '{date_col}' not found")
        return
    
    # Prepare source data
    tmp = src[["PATID", date_col]].copy()
    tmp["PATID"] = normalize_patid_series(tmp["PATID"])
    tmp[date_col] = pd.to_datetime(tmp[date_col], errors="coerce")
    tmp = tmp.dropna(subset=["PATID", date_col]).drop_duplicates(subset=["PATID"])
    
    if tmp.empty:
        logger.debug(f"Source {source_name}: no valid death records after normalization")
        return
    
    # Merge with all_patids and update where death_date is still NaT
    merged = all_patids_df[["PATID", "death_date"]].merge(tmp, on="PATID", how="left")
    mask = merged["death_date"].isna() & merged[date_col].notna()
    
    if mask.any():
        count = mask.sum()
        all_patids_df.loc[mask, "death_date"] = merged.loc[mask, date_col].values
        all_patids_df.loc[mask, "death_source"] = source_name
        logger.info(f"Source {source_name}: assigned {count} death dates")
    else:
        logger.debug(f"Source {source_name}: no new deaths to assign")


def build_death_table(cfg_path: str = "ahs.yaml") -> pd.DataFrame:
    """
    Build a death table from AHS data sources.
    
    Priority order:
        1. Vital Status (uses DEATHDATE column)
        2. Registry (requires DEATH_IND == 1, uses PERS_REAP_END_DATE)
        3. DAD (requires DISP in {7,8,10,11}, uses DISDATE_DT)
    
    Args:
        cfg_path: Path to AHS configuration YAML file
        
    Returns:
        DataFrame with columns:
            - PATID (Int64): Patient ID
            - death_date (datetime64[ns]): Death date (NaT if no death recorded)
            - death_source (string): Source of death information (NA if no death)
    """
    logger.info(f"Loading configuration from: {cfg_path}")
    cfg = _load_config(cfg_path)
    
    # Load source dataframes
    sources = {}
    
    logger.info("Loading Vital Status data...")
    vital_status_df, _ = read_file(cfg["raw_paths"]["vital_status"])
    sources['vital_status'] = vital_status_df
    
    logger.info("Loading Registry data...")
    registry_df, _ = read_file(cfg["raw_paths"]["registry"])
    sources['registry'] = registry_df
    
    logger.info("Loading DAD (admissions) data...")
    dad_df, _ = read_file(cfg["raw_paths"]["admissions"])
    sources['dad'] = dad_df
    
    logger.info("Loading ECG data (for PATID union)...")
    ecg_df, _ = read_file(cfg["raw_paths"]["ecg"])
    sources['ecg'] = ecg_df
    
    # Build union of all PATIDs from available sources
    logger.info("Building PATID union from all sources...")
    patid_series = []
    for name, df in sources.items():
        if isinstance(df, pd.DataFrame) and not df.empty and "PATID" in df.columns:
            patid_series.append(df["PATID"])
            logger.debug(f"  {name}: {len(df)} rows, {df['PATID'].nunique()} unique PATIDs")
    
    if not patid_series:
        logger.warning("No valid PATIDs found in any source, returning empty DataFrame")
        return pd.DataFrame(columns=["PATID", "death_date", "death_source"])
    
    # Combine and normalize all PATIDs
    all_patids = pd.concat(patid_series, ignore_index=True)
    all_patids = normalize_patid_series(all_patids).drop_duplicates().sort_values().reset_index(drop=True)
    
    logger.info(f"Total unique PATIDs: {len(all_patids):,}")
    
    # Initialize death table
    all_patids_df = pd.DataFrame({
        "PATID": all_patids,
        "death_date": pd.NaT,
        "death_source": pd.NA
    })
    
    # Apply death assignment in priority order
    logger.info("Assigning death dates by priority...")
    
    # Priority 1: Vital Status (use DEATHDATE or DETHDATE column)
    if not vital_status_df.empty:
        # Try both common column name variants
        date_col = None
        for col in ["DEATHDATE", "DETHDATE"]:
            if col in vital_status_df.columns:
                date_col = col
                break
        if date_col:
            _assign_death_from_source(all_patids_df, vital_status_df, date_col, "vital_status")
        else:
            logger.warning("Vital Status: no death date column found (tried DEATHDATE, DETHDATE)")
    
    # Priority 2: Registry (require DEATH_IND == 1, use PERS_REAP_END_DATE)
    if not registry_df.empty and "DEATH_IND" in registry_df.columns:
        # Find plausible date column
        possible_dates = ["PERS_REAP_END_DATE", "PERS_REAP_END_DT", "PERS_REAP_END"]
        date_col = next((c for c in possible_dates if c in registry_df.columns), None)
        
        if date_col:
            def registry_filter(df):
                # Handle DEATH_IND as int or string
                return pd.to_numeric(df["DEATH_IND"], errors="coerce").fillna(0).astype(int) == 1
            
            _assign_death_from_source(all_patids_df, registry_df, date_col, "registry", filter_mask=registry_filter)
        else:
            logger.warning(f"Registry: no death date column found (tried {possible_dates})")
    
    # Priority 3: DAD (require DISP in {7, 8, 10, 11}, use DISDATE_DT)
    if not dad_df.empty and "DISP" in dad_df.columns and "DISDATE_DT" in dad_df.columns:
        def dad_filter(df):
            # Handle numeric or string DISP values
            return df["DISP"].astype(str).str.strip().isin({"7", "8", "10", "11"})
        
        _assign_death_from_source(all_patids_df, dad_df, "DISDATE_DT", "DAD", filter_mask=dad_filter)
    
    # Summary statistics
    total_deaths = all_patids_df["death_date"].notna().sum()
    logger.info(f"Total deaths identified: {total_deaths:,} out of {len(all_patids_df):,} patients ({100*total_deaths/len(all_patids_df):.2f}%)")
    
    if total_deaths > 0:
        source_counts = all_patids_df["death_source"].value_counts()
        logger.info("Death source distribution:")
        for source, count in source_counts.items():
            logger.info(f"  {source}: {count:,}")
    
    return all_patids_df


def main():
    """Main entry point for command-line usage."""
    parser = argparse.ArgumentParser(
        description="Extract death information from AHS data sources",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Use default config (ahs.yaml in current directory or package)
  PYTHONPATH=src python3 tests/test_death_extraction.py
  
  # Specify config path
  PYTHONPATH=src python3 tests/test_death_extraction.py --cfg src/meds_pipeline/configs/ahs.yaml
  
  # Show sample and save output
  PYTHONPATH=src python3 tests/test_death_extraction.py --cfg ahs.yaml --show-sample --save death_table.parquet
        """
    )
    
    parser.add_argument(
        "--cfg",
        type=str,
        default="ahs.yaml",
        help="Path to AHS configuration YAML file (default: ahs.yaml)"
    )
    
    parser.add_argument(
        "--show-sample",
        action="store_true",
        help="Display sample of death table (first 20 rows)"
    )
    
    parser.add_argument(
        "--save",
        type=str,
        default='/data/padmalab_external/special_project/AHS_Data_Release_2/rmt22884_death_20251124.pickle',
        help="Save death table to specified parquet file"
    )
    
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable debug logging"
    )
    
    args = parser.parse_args()
    
    if args.verbose:
        logger.setLevel(logging.DEBUG)
    
    # Build death table
    logger.info("="*60)
    logger.info("AHS Death Extraction")
    logger.info("="*60)
    
    death_table = build_death_table(args.cfg)
    
    # Display summary
    logger.info("="*60)
    logger.info("Summary")
    logger.info("="*60)
    logger.info(f"Total patients: {len(death_table):,}")
    logger.info(f"Patients with death dates: {death_table['death_date'].notna().sum():,}")
    logger.info(f"Patients without death dates: {death_table['death_date'].isna().sum():,}")
    
    if death_table['death_date'].notna().any():
        logger.info("\nDeath source distribution:")
        for source, count in death_table['death_source'].value_counts().items():
            logger.info(f"  {source}: {count:,}")
    
    # Show sample if requested
    if args.show_sample:
        logger.info("\n" + "="*60)
        logger.info("Sample (first 20 rows)")
        logger.info("="*60)
        print(death_table.head(20).to_string(index=False))
    
    # Save if requested
    if args.save:
        output_path = Path(args.save)
        death_table.to_parquet(output_path, index=False)
        logger.info(f"\nSaved death table to: {output_path}")
        logger.info(f"File size: {output_path.stat().st_size / 1024 / 1024:.2f} MB")
    
    return death_table


if __name__ == "__main__":
    main()