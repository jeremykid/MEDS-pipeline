# src/meds_pipeline/etl/base.py
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

import pandas as pd


class ComponentETL(ABC):
    """
    A single ETL *component* that converts one source table / domain
    (e.g., admissions, diagnoses_icd, procedures_icd) into a MEDS-formatted
    event stream (a pandas DataFrame).

    Design goals:
    - Keep each component small and focused: one source → one event type (or a few closely related event types).
    - Return a MEDS-compatible DataFrame (Core or Plus columns).
    - Be stateless beyond constructor config (avoid side effects).

    Required MEDS-Core columns:
      - subject_id : str
      - time       : datetime64[ns] (timezone-naive is fine)
      - event_type : str (e.g., "diagnosis", "encounter.start")
      - code       : str (e.g., ICD10 code, local code)
      - code_system: str (e.g., "ICD10", "ICD9", "LOINC", "ATC", "NDC", "LOCAL")

    Optional MEDS-Plus columns (recommended for traceability/auditing):
      - encounter_id, encounter_class, value_num, unit, value_text,
        source_table, provenance_id, site

    Notes:
    - Each component should *only* know how to read its own raw inputs and
      produce MEDS rows. Writing to disk, validation, or orchestration should be
      handled by higher-level classes.
    """

    # Public name of the component, used in CLI and registry lookups
    # e.g., "admissions", "diagnoses_icd"
    name: str

    def __init__(self, cfg: Dict[str, Any], base_cfg: Dict[str, Any]) -> None:
        """
        Parameters
        ----------
        cfg : Dict[str, Any]
            Source-specific config (paths, column names, code-system defaults).
            Example keys: {"raw_paths": {...}, "mapping": {...}}
        base_cfg : Dict[str, Any]
            Global/base config (output_dir, format, compression, partition_cols).
        """
        self.cfg = cfg
        self.base_cfg = base_cfg

    @abstractmethod
    def run_core(self) -> pd.DataFrame:
        """
        Build and return a MEDS-Core dataframe.

        The returned DataFrame MUST include at least the 5 core columns:
          ["subject_id", "time", "event_type", "code", "code_system"]

        Implementations SHOULD:
          - Ensure `time` is a pandas datetime (pd.to_datetime).
          - Fill event_type with a consistent token (e.g., "diagnosis").
          - Normalize code/code_system pairs (e.g., "ICD10", not mixed labels).
          - Avoid writing to disk here—just return the DataFrame.
        """
        raise NotImplementedError

    def run_plus(self) -> pd.DataFrame:
        """
        Build and return a MEDS-Plus dataframe.

        Default behavior:
          - Calls `run_core()` and returns its output unchanged.

        Override in components where Plus columns are available or desirable:
          - Add columns like encounter_id, encounter_class, value_num, unit,
            value_text, source_table, provenance_id, site, etc.
        """
        return self.run_core()

    def _read_csv_with_progress(self, path: str, desc: str = None) -> pd.DataFrame:
        """
        Read CSV file with optional progress bar and patient limiting.

        Parameters
        ----------
        path : str
            Path to the CSV file
        desc : str, optional
            Description for the progress bar

        Returns
        -------
        pd.DataFrame
            DataFrame with optional patient limiting applied
        """
        show_progress = self.base_cfg.get("show_progress", True)
        max_patients = self.base_cfg.get("max_patients", None)

        if desc is None:
            desc = f"Reading {self.name}"

        if show_progress:
            print(f"📖 {desc}...")

        # Read the file
        df = pd.read_csv(path, low_memory=False)

        if show_progress:
            print(f"   └─ Loaded {len(df):,} rows")

        # Apply patient limiting if specified
        if max_patients and 'subject_id' in df.columns:
            original_patients = df['subject_id'].nunique()
            unique_patients = df['subject_id'].unique()[:max_patients]
            df = df[df['subject_id'].isin(unique_patients)]

            if show_progress:
                final_patients = df['subject_id'].nunique()
                print(f"   └─ Limited to {final_patients:,}/{original_patients:,} patients, {len(df):,} rows")

        return df


class DataSourceETL(ABC):
    """
    A *data-source orchestrator* that owns a set of ComponentETL instances
    and is responsible for composing their outputs into a single MEDS table.

    Responsibilities:
      - Instantiate and hold a list of components for a given source (e.g., MIMIC or AHS).
      - Provide two public methods:
          * to_meds_core() : concatenate component Core outputs
          * to_meds_plus() : concatenate component Plus outputs
      - Do NOT write to disk here—return DataFrames; writing/validation can be done by caller.

    Typical usage:
      orchestrator = MIMICSourceETL(["admissions", "diagnoses_icd"], cfg, base_cfg)
      df_core = orchestrator.to_meds_core()
      df_plus = orchestrator.to_meds_plus()
    """

    def __init__(self, components: List[ComponentETL]) -> None:
        """
        Parameters
        ----------
        components : List[ComponentETL]
            The concrete components to be executed for this data source.
            Order generally does not matter, but can be controlled by caller.
        """
        self.components = components

    @abstractmethod
    def to_meds_core(self) -> pd.DataFrame:
        """
        Execute all components and return the concatenated MEDS-Core DataFrame.

        Required behavior:
          - Call `run_core()` on each component.
          - Concatenate (pd.concat) and return the combined DataFrame.
          - Leave validation (schema checks) to upstream code.
        """
        raise NotImplementedError

    @abstractmethod
    def to_meds_plus(self) -> pd.DataFrame:
        """
        Execute all components and return the concatenated MEDS-Plus DataFrame.

        Required behavior:
          - Call `run_plus()` on each component.
          - Concatenate (pd.concat) and return the combined DataFrame.
          - Leave validation (schema checks) to upstream code.
        """
        raise NotImplementedError
