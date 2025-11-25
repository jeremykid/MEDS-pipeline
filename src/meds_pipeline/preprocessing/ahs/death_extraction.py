"""
AHS Death Extraction (preprocessing)

This module provides a reusable preprocessing function to build a death table
from AHS data sources (vital status, registry, DAD). It is extracted from
`tests/test_AHS_death_extraction.py` so it can be reused by the pipeline.

Public API:
    - build_death_table(cfg_path: str = "ahs.yaml") -> pd.DataFrame
    - main() CLI wrapper for convenience

The implementation preserves logging, error handling, and file-format
support (SAS, pickle, csv, parquet).
"""

from pathlib import Path
from typing import Tuple, Optional, Dict, Any, Callable
import pandas as pd
import pyreadstat
import yaml
import logging
import argparse
from importlib.resources import files

# Configure logging for the module (consumer can change level)
logger = logging.getLogger(__name__)
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter('%(levelname)s: %(message)s'))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


def read_file(path: str | Path, raise_on_missing: bool = False) -> Tuple[pd.DataFrame, Optional[Dict[str, Any]]]:
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
        if suffix == ".sas7bdat" or name_lower.endswith(".sas7bdat") or name_lower.endswith(".sas7dbat"):
            df, meta = pyreadstat.read_sas7bdat(str(p), output_format="pandas")
            logger.debug(f"Read SAS file: {p}, shape: {df.shape}")
            return df, meta

        if suffix in (".pickle", ".pkl"):
            df = pd.read_pickle(str(p))
            logger.debug(f"Read pickle file: {p}, shape: {df.shape}")
            return df, None

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
    p = Path(path_or_pkg_rel)
    if p.exists():
        return yaml.safe_load(p.read_text(encoding="utf-8"))

    pkg_root = files("meds_pipeline.configs")
    return yaml.safe_load((pkg_root / path_or_pkg_rel).read_text(encoding="utf-8"))


def normalize_patid_series(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce").dropna().astype("Int64")


def _assign_death_from_source(
    all_patids_df: pd.DataFrame,
    src_df: pd.DataFrame,
    date_col: str,
    source_name: str,
    filter_mask: Optional[Callable[[pd.DataFrame], pd.Series]] = None
) -> None:
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

    tmp = src[["PATID", date_col]].copy()
    tmp["PATID"] = normalize_patid_series(tmp["PATID"])
    tmp[date_col] = pd.to_datetime(tmp[date_col], errors="coerce")
    tmp = tmp.dropna(subset=["PATID", date_col]).drop_duplicates(subset=["PATID"])

    if tmp.empty:
        logger.debug(f"Source {source_name}: no valid death records after normalization")
        return

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
    logger.info(f"Loading configuration from: {cfg_path}")
    cfg = _load_config(cfg_path)

    sources = {}

    logger.info("Loading Vital Status data...")
    vital_status_df, _ = read_file(cfg["raw_paths"]["vital_status"]) if "raw_paths" in cfg and "vital_status" in cfg["raw_paths"] else (pd.DataFrame(), None)
    sources['vital_status'] = vital_status_df

    logger.info("Loading Registry data...")
    registry_df, _ = read_file(cfg["raw_paths"]["registry"]) if "raw_paths" in cfg and "registry" in cfg["raw_paths"] else (pd.DataFrame(), None)
    sources['registry'] = registry_df

    logger.info("Loading DAD (admissions) data...")
    dad_df, _ = read_file(cfg["raw_paths"]["admissions"]) if "raw_paths" in cfg and "admissions" in cfg["raw_paths"] else (pd.DataFrame(), None)
    sources['dad'] = dad_df

    logger.info("Loading ECG data (for PATID union)...")
    ecg_df, _ = read_file(cfg["raw_paths"]["ecg"]) if "raw_paths" in cfg and "ecg" in cfg["raw_paths"] else (pd.DataFrame(), None)
    sources['ecg'] = ecg_df

    logger.info("Building PATID union from all sources...")
    patid_series = []
    for name, df in sources.items():
        if isinstance(df, pd.DataFrame) and not df.empty and "PATID" in df.columns:
            patid_series.append(df["PATID"])
            logger.debug(f"  {name}: {len(df)} rows, {df['PATID'].nunique()} unique PATIDs")

    if not patid_series:
        logger.warning("No valid PATIDs found in any source, returning empty DataFrame")
        return pd.DataFrame(columns=["PATID", "death_date", "death_source"])

    all_patids = pd.concat(patid_series, ignore_index=True)
    all_patids = normalize_patid_series(all_patids).drop_duplicates().sort_values().reset_index(drop=True)

    logger.info(f"Total unique PATIDs: {len(all_patids):,}")

    all_patids_df = pd.DataFrame({
        "PATID": all_patids,
        "death_date": pd.NaT,
        "death_source": pd.NA
    })

    logger.info("Assigning death dates by priority...")

    if not vital_status_df.empty:
        date_col = None
        for col in ["DEATHDATE", "DETHDATE"]:
            if col in vital_status_df.columns:
                date_col = col
                break
        if date_col:
            _assign_death_from_source(all_patids_df, vital_status_df, date_col, "vital_status")
        else:
            logger.warning("Vital Status: no death date column found (tried DEATHDATE, DETHDATE)")

    if not registry_df.empty and "DEATH_IND" in registry_df.columns:
        possible_dates = ["PERS_REAP_END_DATE", "PERS_REAP_END_DT", "PERS_REAP_END"]
        date_col = next((c for c in possible_dates if c in registry_df.columns), None)

        if date_col:
            def registry_filter(df):
                return pd.to_numeric(df["DEATH_IND"], errors="coerce").fillna(0).astype(int) == 1

            _assign_death_from_source(all_patids_df, registry_df, date_col, "registry", filter_mask=registry_filter)
        else:
            logger.warning(f"Registry: no death date column found (tried {possible_dates})")

    if not dad_df.empty and "DISP" in dad_df.columns and "DISDATE_DT" in dad_df.columns:
        def dad_filter(df):
            return df["DISP"].astype(str).str.strip().isin({"7", "8", "10", "11"})

        _assign_death_from_source(all_patids_df, dad_df, "DISDATE_DT", "DAD", filter_mask=dad_filter)

    total_deaths = all_patids_df["death_date"].notna().sum()
    logger.info(f"Total deaths identified: {total_deaths:,} out of {len(all_patids_df):,} patients ({100*total_deaths/len(all_patids_df):.2f}%)")

    if total_deaths > 0:
        source_counts = all_patids_df["death_source"].value_counts()
        logger.info("Death source distribution:")
        for source, count in source_counts.items():
            logger.info(f"  {source}: {count:,}")

    return all_patids_df


def main():
    parser = argparse.ArgumentParser(
        description="Extract death information from AHS data sources (preprocessing)",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument("--cfg", type=str, default="ahs.yaml", help="Path to AHS configuration YAML file")
    parser.add_argument("--show-sample", action="store_true", help="Display sample of death table (first 20 rows)")
    parser.add_argument("--save", type=str, default=None, help="Save death table to specified parquet file")
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging")

    args = parser.parse_args()

    if args.verbose:
        logger.setLevel(logging.DEBUG)

    death_table = build_death_table(args.cfg)

    logger.info(f"Total patients: {len(death_table):,}")
    logger.info(f"Patients with death dates: {death_table['death_date'].notna().sum():,}")
    logger.info(f"Patients without death dates: {death_table['death_date'].isna().sum():,}")

    if death_table['death_date'].notna().any():
        logger.info("\nDeath source distribution:")
        for source, count in death_table['death_source'].value_counts().items():
            logger.info(f"  {source}: {count:,}")

    if args.show_sample:
        print(death_table.head(20).to_string(index=False))

    if args.save:
        output_path = Path(args.save)
        death_table.to_parquet(output_path, index=False)
        logger.info(f"Saved death table to: {output_path}")

    return death_table


if __name__ == "__main__":
    # Example command-line invocations (choose one):
    # 1) Run as a script (when working in the repo):
    #    PYTHONPATH=src python3 src/meds_pipeline/preprocessing/ahs/death_extraction.py \
    #        --cfg src/meds_pipeline/configs/ahs.yaml --save death_table.parquet --verbose
    # 2) Run as a module (installed package / importable path):
    #    python -m meds_pipeline.preprocessing.ahs.death_extraction \
    #        --cfg src/meds_pipeline/configs/ahs.yaml --show-sample
    main()
