import pandas as pd
import numpy as np

def build_patient_death_table(hosp_df: pd.DataFrame,
                              patient_df: pd.DataFrame,
                              consistency_tolerance_days: float = 1.0) -> pd.DataFrame:
    """
    Build a patient-level death table from MIMIC-IV admissions (hosp_df)
    and patients (patient_df), and check the consistency between two
    death time sources (dod vs deathtime).

    Parameters
    ----------
    hosp_df : pd.DataFrame
        Typically mimiciv_hosp.admissions. It should contain at least:
        - subject_id
        - hospital_expire_flag
        - deathtime

    patient_df : pd.DataFrame
        Typically mimiciv_hosp.patients. It should contain at least:
        - subject_id
        - dod

    consistency_tolerance_days : float, default 1.0
        Absolute difference threshold (in days) under which dod and
        in-hospital deathtime are considered "consistent".

    Returns
    -------
    death_table : pd.DataFrame
        One row per subject_id, with the following columns (main ones):
        - subject_id
        - dod                    # death date from patients table
        - in_hosp_deathtime      # earliest in-hospital death time from admissions
        - n_death_admissions     # number of admissions ending in death
        - death_in_patients      # bool: death recorded in patients
        - death_in_hosp          # bool: in-hospital death recorded in admissions
        - death_time             # unified death time (dod preferred, else in_hosp_deathtime)
        - time_diff_days         # dod - in_hosp_deathtime in days (if both available)
        - is_consistent          # bool: within the given tolerance
    """

    hosp = hosp_df.copy()
    pats = patient_df.copy()

    # Ensure datetime types
    if 'deathtime' in hosp.columns:
        hosp['deathtime'] = pd.to_datetime(hosp['deathtime'], errors='coerce')
    else:
        hosp['deathtime'] = pd.NaT

    if 'dod' in pats.columns:
        pats['dod'] = pd.to_datetime(pats['dod'], errors='coerce')
    else:
        pats['dod'] = pd.NaT

    # Aggregate in-hospital deaths to patient level:
    # keep only admissions where hospital_expire_flag = 1 and deathtime is not null
    hosp_death = (
        hosp.loc[(hosp.get('hospital_expire_flag', 0) == 1) & hosp['deathtime'].notna()]
        .groupby('subject_id', as_index=False)
        .agg(
            in_hosp_deathtime=('deathtime', 'min'),   # earliest in-hospital death
            n_death_admissions=('deathtime', 'size')  # number of death admissions
        )
    )

    # Keep only subject_id and dod from patients
    pats_death = pats[['subject_id', 'dod']].copy()

    # Merge both sources at patient level
    death_table = pats_death.merge(hosp_death, on='subject_id', how='outer')

    # Flags indicating whether each source has a death record
    death_table['death_in_patients'] = death_table['dod'].notna()
    death_table['death_in_hosp'] = death_table['in_hosp_deathtime'].notna()

    # Unified death_time: prefer dod, fallback to in_hosp_deathtime
    death_table['death_time'] = death_table['dod'].combine_first(
        death_table['in_hosp_deathtime']
    )

    # Compute time difference only when both dod and in-hospital deathtime are available
    both_mask = death_table['dod'].notna() & death_table['in_hosp_deathtime'].notna()
    death_table['time_diff_days'] = np.nan

    death_table.loc[both_mask, 'time_diff_days'] = (
        (death_table.loc[both_mask, 'dod'] -
         death_table.loc[both_mask, 'in_hosp_deathtime'])
        .dt.total_seconds() / (3600 * 24)
    )

    # Consistency flag: both available and within tolerance
    death_table['is_consistent'] = False
    death_table.loc[both_mask, 'is_consistent'] = (
        death_table.loc[both_mask, 'time_diff_days'].abs() <= consistency_tolerance_days
    )

    return death_table

from pathlib import Path
import yaml
import logging
import pyreadstat
from typing import Tuple, Optional, Dict, Any, Callable

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

        # Support CSV files (including compressed .csv.gz, .gz)
        if suffix == ".csv" or suffix == ".gz" or name_lower.endswith(".csv.gz"):
            df = pd.read_csv(str(p), compression="infer")
            logger.debug(f"Read CSV file (possibly compressed): {p}, shape: {df.shape}")
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

if __name__ == "__main__":
    from importlib.resources import files
    cfg = _load_config('mimic.yaml')
    hosp_df, _ = read_file(cfg["raw_paths"]["admissions"]) if "raw_paths" in cfg and "admissions" in cfg["raw_paths"] else (pd.DataFrame(), None)
    patient_df, _ = read_file(cfg["raw_paths"]["patients"]) if "raw_paths" in cfg and "patients" in cfg["raw_paths"] else (pd.DataFrame(), None)

    # Check that files were loaded successfully
    if hosp_df.empty:
        logger.error("Admissions file empty or failed to read; check path and read_file support for compression.")
        raise SystemExit(1)
    if patient_df.empty:
        logger.error("Patients file empty or failed to read; check path and read_file support for compression.")
        raise SystemExit(1)

    logger.info(f"Loaded admissions: {len(hosp_df):,} rows")
    logger.info(f"Loaded patients: {len(patient_df):,} rows")

    death_table = build_patient_death_table(hosp_df, patient_df, consistency_tolerance_days=1)

    # Check overall consistency:
    death_table['is_consistent'].value_counts(dropna=False)

    # find inconsistent cases
    inconsistent = death_table[(death_table['death_in_patients']) &
                            (death_table['death_in_hosp']) &
                            (~death_table['is_consistent'])]

    print (inconsistent[['subject_id', 'dod', 'in_hosp_deathtime', 'time_diff_days']])
    print (death_table)