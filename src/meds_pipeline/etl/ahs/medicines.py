# src/meds_pipeline/etl/ahs/medicines.py
from __future__ import annotations

from typing import Optional, Set

import pandas as pd

from ..base import ComponentETL
from ..registry import register
import pyreadstat
'''
   PATID  DSPN_DATE  DSPN_AMT_QTY DSPN_AMT_UNT_MSR_CD  DSPN_DAY_SUPPLY_QTY  \
0    1.0 2008-04-01          60.0                 TAB                 30.0   
1    1.0 2008-04-01          60.0                 TAB                 30.0   
2    1.0 2008-04-01          60.0                 TAB                 30.0   
3    1.0 2008-04-01          60.0                 TAB                 30.0   
4    1.0 2008-04-18          30.0                 TAB                 30.0   

   DRUG_DIN SUPP_DRUG_ATC_CODE  
0  02212331            A02BA02  
1  02242681            B01AA03  
2  02242681            B01AA03  
3  02212331            A02BA02  
4  02010909            G04CB01  
'''


@register("medicines")
class AHSMedicines(ComponentETL):
    """
    Component: medicines (pin records) → MEDS event stream
    Produces `medication.dispense` events at dispense date.

    Expected raw columns (best-effort; code is defensive to missing columns):
      - PATID
      - DSPN_DATE
      - SUPP_DRUG_ATC_CODE (required for code generation)
      - DSPN_AMT_QTY, DSPN_AMT_UNT_MSR_CD (optional, for value_num/unit)
      - DSPN_DAY_SUPPLY_QTY (optional, for value_text)
      - INST (optional, for site)
    
    Output format:
      - code: MEDICINE//ATC//{SUPP_DRUG_ATC_CODE}
      - code_system: ATC
      - Rows without valid SUPP_DRUG_ATC_CODE are dropped
    """

    CHUNK_SIZE = 1_000_000

    # ---------------------------- helpers ------------------------------------- #

    @staticmethod
    def _clean_string(series: pd.Series) -> pd.Series:
        s = series.astype("string").str.strip()
        return s.mask(s.isin(["", "nan", "NaN", "None", "<NA>"]))

    @staticmethod
    def _append_text_part(base: pd.Series, part: pd.Series) -> pd.Series:
        part = part.astype("string")
        has_part = part.notna() & (part.str.strip() != "")
        has_base = base.notna() & (base.str.strip() != "")

        out = base.copy()
        out = out.mask(~has_base & has_part, part)
        out = out.mask(has_base & has_part, base + " | " + part)
        return out.fillna("")

    @staticmethod
    def _assemble_value_text(df: pd.DataFrame) -> pd.Series:
        """
        Human-readable text for auditing, e.g.:
          "amt=60 | unit=TAB | day_supply=30"
        Only includes available, non-empty parts.
        """
        result = pd.Series([""] * len(df), index=df.index, dtype="string")

        # amount quantity
        if "DSPN_AMT_QTY" in df.columns:
            amt = pd.to_numeric(df["DSPN_AMT_QTY"], errors="coerce")
            amt_txt = ("amt=" + amt.astype("string")).where(amt.notna(), "")
            result = AHSMedicines._append_text_part(result, amt_txt)

        # amount unit
        if "DSPN_AMT_UNT_MSR_CD" in df.columns:
            unit = AHSMedicines._clean_string(df["DSPN_AMT_UNT_MSR_CD"])
            unit_txt = ("unit=" + unit).where(unit.notna(), "")
            result = AHSMedicines._append_text_part(result, unit_txt)

        # day supply
        if "DSPN_DAY_SUPPLY_QTY" in df.columns:
            ds = pd.to_numeric(df["DSPN_DAY_SUPPLY_QTY"], errors="coerce")
            ds_txt = ("day_supply=" + ds.astype("Int64").astype("string")).where(ds.notna(), "")
            result = AHSMedicines._append_text_part(result, ds_txt)

        return result.astype(str)

    @staticmethod
    def _empty_output() -> pd.DataFrame:
        return pd.DataFrame(
            columns=[
                "subject_id",
                "time",
                "event_type",
                "code",
                "code_system",
                "value_num",
                "unit",
                "value_text",
                "source_table",
                "provenance_id",
                "site",
            ]
        )

    def _filter_by_patient_limit(
        self,
        chunk: pd.DataFrame,
        keep_subjects: Set[str],
        max_patients: Optional[int],
    ) -> pd.DataFrame:
        subject = self._subject_id_string(chunk["PATID"])
        valid_subject = subject.notna() & (subject.fillna("").astype(str).str.strip() != "")

        patient_ids = self.base_cfg.get("patient_ids")
        if patient_ids:
            keep_subjects.update(str(patient_id) for patient_id in patient_ids)
            return chunk.loc[valid_subject & subject.astype(str).isin(keep_subjects)].copy()

        if not max_patients:
            return chunk.loc[valid_subject].copy()

        if len(keep_subjects) < max_patients:
            ordered_unique = pd.unique(subject[valid_subject])
            for sid in ordered_unique:
                sid_str = str(sid)
                if sid_str not in keep_subjects:
                    keep_subjects.add(sid_str)
                    if len(keep_subjects) >= max_patients:
                        break

        return chunk.loc[valid_subject & subject.astype(str).isin(keep_subjects)].copy()

    def _transform_chunk(self, df: pd.DataFrame, row_offset: int = 0) -> pd.DataFrame:
        # Validate required columns
        if "PATID" not in df.columns:
            print(df.columns.tolist())
            raise KeyError("AHS PIN expects column `PATID`")
        if "DSPN_DATE" not in df.columns:
            raise KeyError("AHS PIN expects column `DSPN_DATE`")

        # subject_id
        subject = self._subject_id_string(df["PATID"])

        # time
        time = pd.to_datetime(df["DSPN_DATE"], errors="coerce")

        # code: MEDICINE//ATC//{SUPP_DRUG_ATC_CODE}
        atc = self._clean_string(
            df.get("SUPP_DRUG_ATC_CODE", pd.Series([""] * len(df), index=df.index))
        )
        valid_atc = atc.notna() & (atc.str.strip() != "")
        code = ("MEDICINE//ATC//" + atc).where(valid_atc, "")

        valid = (
            subject.notna()
            & (subject.astype(str).str.strip() != "")
            & time.notna()
            & (code.astype(str).str.strip() != "")
        )

        df_valid = df.loc[valid].reset_index(drop=True)
        out = pd.DataFrame({
            "subject_id": subject.loc[valid].reset_index(drop=True),
            "time": time.loc[valid].reset_index(drop=True),
            "event_type": "medication.dispense",
            "code": code.loc[valid].reset_index(drop=True),
            "code_system": "ATC",
        })

        # Optional: value_num (medication amount)
        if "DSPN_AMT_QTY" in df_valid.columns:
            out["value_num"] = pd.to_numeric(df_valid["DSPN_AMT_QTY"], errors="coerce")

        # Optional: unit (amount unit measure)
        if "DSPN_AMT_UNT_MSR_CD" in df_valid.columns:
            out["unit"] = self._clean_string(df_valid["DSPN_AMT_UNT_MSR_CD"]).fillna("")

        # Optional: value_text (human-readable metadata)
        out["value_text"] = self._assemble_value_text(df_valid)

        # Metadata
        out["source_table"] = "PIN"
        source_rows = pd.Series(
            range(row_offset + 1, row_offset + len(df) + 1),
            index=df.index,
            dtype="int64",
        )
        out["provenance_id"] = source_rows.loc[valid].reset_index(drop=True).astype(str)

        # Optional: site (institution)
        if "INST" in df_valid.columns:
            out["site"] = self._clean_string(df_valid["INST"]).fillna("")

        return out.reset_index(drop=True)

    def iter_core(self):
        path = self.cfg["raw_paths"]["medicines"]
        max_patients = self.base_cfg.get("max_patients")
        show_progress = self.base_cfg.get("show_progress", True)
        chunk_size = int(self.base_cfg.get("medicines_chunksize", self.CHUNK_SIZE))

        if show_progress:
            print("📖 Loading AHS medication data (chunked)...")

        _, meta = pyreadstat.read_sas7bdat(path, metadataonly=True)
        header_cols = list(meta.column_names)
        preferred_cols = [
            "PATID",
            "DSPN_DATE",
            "SUPP_DRUG_ATC_CODE",
            "DSPN_AMT_QTY",
            "DSPN_AMT_UNT_MSR_CD",
            "DSPN_DAY_SUPPLY_QTY",
            "INST",
        ]
        usecols = [column for column in preferred_cols if column in header_cols]

        required = {"PATID", "DSPN_DATE"}
        missing_required = sorted(required.difference(usecols))
        if missing_required:
            raise KeyError(f"Missing required AHS PIN columns: {missing_required}")

        reader = pyreadstat.read_file_in_chunks(
            pyreadstat.read_sas7bdat,
            path,
            chunksize=chunk_size,
            output_format="pandas",
            usecols=usecols,
        )

        keep_subjects: Set[str] = set()
        processed_rows = 0
        emitted_rows = 0
        emitted_patients: Set[str] = set()

        for i, item in enumerate(reader, start=1):
            chunk = item[0] if isinstance(item, tuple) else item
            row_offset = processed_rows
            processed_rows += len(chunk)

            chunk = self._filter_by_patient_limit(chunk, keep_subjects, max_patients)
            if chunk.empty:
                continue

            out_chunk = self._transform_chunk(chunk, row_offset=row_offset)
            if out_chunk.empty:
                continue

            emitted_rows += len(out_chunk)
            emitted_patients.update(out_chunk["subject_id"].dropna().astype(str).unique())

            if show_progress and (i == 1 or i % 10 == 0):
                patient_msg = f", selected_patients={len(keep_subjects):,}" if max_patients else ""
                print(
                    f"   └─ Chunks={i:,}, rows_read={processed_rows:,}, "
                    f"rows_emitted={emitted_rows:,}{patient_msg}"
                )
            yield out_chunk

        if show_progress:
            print(
                f"   ✅ Generated {emitted_rows:,} medication dispense events "
                f"for {len(emitted_patients):,} patients"
            )

    # ------------------------------ Core -------------------------------------- #
    def run_core(self) -> pd.DataFrame:
        """
        Converts AHS PIN medication data to MEDS core format.
        Only uses SUPP_DRUG_ATC_CODE for code generation (MEDICINE//ATC//{code}).
        Rows without valid ATC codes are dropped.
        """
        parts = list(self.iter_core())
        if not parts:
            return self._empty_output()
        return pd.concat(parts, ignore_index=True)
