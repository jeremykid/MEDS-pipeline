# src/meds_pipeline/etl/mimic/demographic.py
from __future__ import annotations

import pandas as pd

from ..base import ComponentETL
from ..registry import register


@register("demographics")
class MIMICDemographics(ComponentETL):
    """
    Component: MIMIC-IV patients → MEDS event stream
    Produces two types of events:
      1. Birth event (code='MEDS_BIRTH', time calculated from anchor_year - anchor_age)
      2. Sex event (code='GENDER//M|F|O', time=NaT)

    Note: Death/censor events are now handled by the separate "censor" component.

    Expected raw columns (from patients.csv.gz):
      - subject_id (required)
      - gender (values: 'M', 'F', or other)
      - anchor_year (required for birth calculation)
      - anchor_age (required for birth calculation)
    
    Output format:
      - Birth: code='MEDS_BIRTH', code_system='MEDS', event_type='demographics.birth'
      - Sex: code='GENDER//M|F|O', code_system='GENDER', event_type='demographics.sex'
    
    Note: MIMIC doesn't provide exact DOB for privacy. We estimate birth year as:
          birth_year = anchor_year - anchor_age
          and use YYYY-01-01 as birth date.
    """

    # ---------------------------- Core -------------------------------------- #
    def run_core(self) -> pd.DataFrame:
        """
        Converts MIMIC-IV patients to MEDS core format.
        Generates birth and sex events for each patient.
        Death/censor events are handled by the separate "censor" component.
        """
        path = self.cfg["raw_paths"]["demographics"]
        df = self._read_csv_with_progress(path, "Loading demographics data")
        
        # Validate required columns
        if "subject_id" not in df.columns:
            raise KeyError("MIMIC patients expects column `subject_id`")
        
        # Deduplicate by subject_id (keep first occurrence)
        df = df.drop_duplicates(subset=["subject_id"], keep="first").reset_index(drop=True)
        
        # subject_id
        subject = df["subject_id"].astype(str)
        
        # Collect all events (birth + sex)
        events = []
        
        # -------------------- Birth Events -------------------- #
        birth_events = self._create_birth_events(df, subject)
        if not birth_events.empty:
            events.append(birth_events)
        
        # -------------------- Sex Events -------------------- #
        sex_events = self._create_sex_events(df, subject)
        if not sex_events.empty:
            events.append(sex_events)
        
        # Combine all events
        if not events:
            # Return empty DataFrame with correct schema
            return pd.DataFrame(columns=[
                "subject_id", "time", "event_type", "code", "code_system"
            ])
        
        out = pd.concat(events, ignore_index=True)
        
        # Drop rows with missing subject_id
        out = out[out["subject_id"].astype(str).str.strip() != ""].reset_index(drop=True)
        
        return out
    
    def _create_birth_events(self, df: pd.DataFrame, subject: pd.Series) -> pd.DataFrame:
        """
        Create birth events using anchor_year - anchor_age.
        MIMIC doesn't provide exact DOB, so we estimate birth year and use YYYY-01-01.
        """
        # Check if required columns exist
        if "anchor_year" not in df.columns or "anchor_age" not in df.columns:
            return pd.DataFrame()
        
        # Calculate birth year
        anchor_year = pd.to_numeric(df["anchor_year"], errors="coerce")
        anchor_age = pd.to_numeric(df["anchor_age"], errors="coerce")
        
        birth_year = anchor_year - anchor_age
        
        # Create birth date as YYYY-01-01
        time = birth_year.apply(
            lambda y: pd.Timestamp(f"{int(y)}-01-01") if pd.notna(y) and y > 1900 else pd.NaT
        )
        
        birth_df = pd.DataFrame({
            "subject_id": subject,
            "time": time,
            "event_type": "demographics.birth",
            "code": "MEDS_BIRTH",
            "code_system": "MEDS",
        })
        
        # Add value_text to indicate calculation method
        birth_df["value_text"] = "source=anchor_year-anchor_age | estimated"
        
        # Metadata
        birth_df["source_table"] = "patients"
        birth_df["provenance_id"] = subject.astype(str)
        
        # Drop rows with NaT time
        birth_df = birth_df.dropna(subset=["time"]).reset_index(drop=True)
        
        return birth_df
    
    def _create_sex_events(self, df: pd.DataFrame, subject: pd.Series) -> pd.DataFrame:
        """
        Create sex/gender events.
        Maps: M -> GENDER//M, F -> GENDER//F, other -> GENDER//O
        """
        if "gender" not in df.columns:
            return pd.DataFrame()
        
        # Normalize gender values
        gender = df["gender"].astype(str).str.strip().str.upper()
        
        # Map to GENDER codes
        def map_gender(g):
            if g in ["M", "MALE"]:
                return "GENDER//M"
            elif g in ["F", "FEMALE"]:
                return "GENDER//F"
            else:
                return "GENDER//O"
        
        code = gender.apply(map_gender)
        
        sex_df = pd.DataFrame({
            "subject_id": subject,
            "time": pd.NaT,  # Sex has no time
            "event_type": "demographics.sex",
            "code": code,
            "code_system": "GENDER",
        })
        
        # Add original gender value for auditing
        sex_df["value_text"] = "gender=" + df["gender"].astype(str).str.strip()
        
        # Metadata
        sex_df["source_table"] = "patients"
        sex_df["provenance_id"] = subject.astype(str)
        
        return sex_df
