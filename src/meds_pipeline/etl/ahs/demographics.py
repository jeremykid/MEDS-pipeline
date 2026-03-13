# src/meds_pipeline/etl/ahs/demographic.py
from __future__ import annotations

import pandas as pd

from ..base import ComponentETL
from ..registry import register


@register("demographics")
class AHSDemographics(ComponentETL):
    """
    Component: AHS demographics → MEDS event stream
    Produces two types of events:
      1. Birth event (code='MEDS_BIRTH', time=DOB or Birth_Year-01-01)
      2. Sex event (code='GENDER//M|F|O', time=NaT)

    Note: Death/censor events are now handled by the separate "censor" component.

    Expected raw columns:
      - PATID (required)
      - DOB (preferred for birth date) or Birth_Year (fallback)
      - SEX (for gender, values: 'M', 'F', or other)
    
    Output format:
      - Birth: code='MEDS_BIRTH', code_system='MEDS', event_type='demographics.birth'
      - Sex: code='GENDER//M|F|O', code_system='GENDER', event_type='demographics.sex'
    """

    # ---------------------------- Core -------------------------------------- #
    def run_core(self) -> pd.DataFrame:
        """
        Converts AHS demographics to MEDS core format.
        Generates birth and sex events for each patient.
        """
        path = self.cfg["raw_paths"]["demographics"]
        df = pd.read_parquet(path)
        
        # Deduplicate by PATID (keep first occurrence)
        if "PATID" not in df.columns:
            raise KeyError("AHS demographics expects column `PATID`")
        df = df.drop_duplicates(subset=["PATID"], keep="first").reset_index(drop=True)
        
        # subject_id
        subject = df["PATID"].astype("Int64").astype(str)
        
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
        Create birth events using DOB (preferred) or Birth_Year (fallback).
        """
        time = None
        source = None
        
        # Try DOB first
        if "DOB" in df.columns:
            time = pd.to_datetime(df["DOB"], errors="coerce")
            source = "DOB"
        
        # Fallback to Birth_Year if DOB is not available or all NaT
        if (time is None or time.isna().all()) and "Birth_Year" in df.columns:
            birth_year = pd.to_numeric(df["Birth_Year"], errors="coerce")
            # Create date as YYYY-01-01
            time = birth_year.apply(
                lambda y: pd.Timestamp(f"{int(y)}-01-01") if pd.notna(y) else pd.NaT
            )
            source = "Birth_Year"
        
        # If no valid time source, return empty
        if time is None or time.isna().all():
            return pd.DataFrame()
        
        birth_df = pd.DataFrame({
            "subject_id": subject,
            "time": time,
            "event_type": "demographics.birth",
            "code": "MEDS_BIRTH",
            "code_system": "MEDS",
        })
        
        # Add value_text to indicate source
        if source:
            birth_df["value_text"] = f"source={source}"
        
        # Metadata
        birth_df["source_table"] = "demographics"
        birth_df["provenance_id"] = (birth_df.index.astype(int) + 1).astype(str)
        
        # Drop rows with NaT time
        birth_df = birth_df.dropna(subset=["time"]).reset_index(drop=True)
        
        return birth_df
    
    def _create_sex_events(self, df: pd.DataFrame, subject: pd.Series) -> pd.DataFrame:
        """
        Create sex/gender events.
        Maps: M/Male -> GENDER//M, F/Female -> GENDER//F, other -> GENDER//O
        """
        if "SEX" not in df.columns:
            return pd.DataFrame()
        
        # Normalize sex values
        sex = df["SEX"].astype(str).str.strip().str.upper()
        
        # Map to GENDER codes
        def map_gender(s):
            if s in ["M", "MALE"]:
                return "GENDER//M"
            elif s in ["F", "FEMALE"]:
                return "GENDER//F"
            else:
                return "GENDER//O"
        
        code = sex.apply(map_gender)
        
        sex_df = pd.DataFrame({
            "subject_id": subject,
            "time": pd.NaT,  # Sex has no time
            "event_type": "demographics.sex",
            "code": code,
            "code_system": "GENDER",
        })
        
        # Add original SEX value for auditing
        sex_df["value_text"] = "SEX=" + df["SEX"].astype(str).str.strip()
        
        # Metadata
        sex_df["source_table"] = "demographics"
        sex_df["provenance_id"] = (sex_df.index.astype(int) + 1).astype(str)
        
        return sex_df

