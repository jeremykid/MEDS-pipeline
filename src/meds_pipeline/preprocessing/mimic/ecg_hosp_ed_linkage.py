from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


DEFAULT_INPUT_PATH = Path(
    "/data/padmalab_external/special_project/physionet.org/files/"
    "mimic-iv-ecg-ext-icd-labels/1.0.1/records_w_diag_icd10.csv"
)
DEFAULT_ADMISSIONS_PATH = Path(
    "/data/padmalab_external/special_project/physionet.org/files/"
    "mimiciv/3.1/hosp/admissions.csv.gz"
)
DEFAULT_EDSTAYS_PATH = Path(
    "/data/padmalab_external/special_project/physionet.org/files/"
    "mimic-iv-ed/2.2/ed/edstays.csv.gz"
)
DEFAULT_OUTPUT_PATH = Path(
    "/data/padmalab_external/special_project/meds_pipeline_output/mimic/"
    "mimic_ecg_hosp_ed_linkage.parquet"
)

SOURCE_COLUMNS = [
    "subject_id",
    "study_id",
    "ecg_time",
    "ed_stay_id",
    "ed_hadm_id",
    "hosp_hadm_id",
]
BASE_LINKAGE_COLUMNS = ["subject_id", "study_id", "ecg_time", "hadm_id", "stay_id"]
LINKAGE_COLUMNS = [
    "subject_id",
    "study_id",
    "ecg_time",
    "hadm_id",
    "hosp_start_time",
    "hosp_end_time",
    "stay_id",
    "ed_start_time",
    "ed_end_time",
]


def _normalize_id(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").astype("Int64")


def _normalize_time(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce")


def _empty_linkage_like(base: pd.DataFrame) -> pd.DataFrame:
    out = base.copy()
    out["hadm_id"] = pd.Series(pd.NA, index=out.index, dtype="Int64")
    out["stay_id"] = pd.Series(pd.NA, index=out.index, dtype="Int64")
    return out[BASE_LINKAGE_COLUMNS]


def _prepare_admissions(admissions: pd.DataFrame) -> pd.DataFrame:
    required = ["hadm_id", "admittime", "dischtime"]
    missing = [col for col in required if col not in admissions.columns]
    if missing:
        raise KeyError(f"Missing required admissions columns: {missing}")

    out = admissions[required].copy()
    out["hadm_id"] = _normalize_id(out["hadm_id"])
    out["hosp_start_time"] = _normalize_time(out["admittime"])
    out["hosp_end_time"] = _normalize_time(out["dischtime"])
    return (
        out.dropna(subset=["hadm_id"])
        .drop_duplicates("hadm_id")
        [["hadm_id", "hosp_start_time", "hosp_end_time"]]
    )


def _prepare_edstays(edstays: pd.DataFrame) -> pd.DataFrame:
    required = ["stay_id", "intime", "outtime"]
    missing = [col for col in required if col not in edstays.columns]
    if missing:
        raise KeyError(f"Missing required ED stay columns: {missing}")

    out = edstays[required].copy()
    out["stay_id"] = _normalize_id(out["stay_id"])
    out["ed_start_time"] = _normalize_time(out["intime"])
    out["ed_end_time"] = _normalize_time(out["outtime"])
    return (
        out.dropna(subset=["stay_id"])
        .drop_duplicates("stay_id")
        [["stay_id", "ed_start_time", "ed_end_time"]]
    )


def build_mimic_ecg_hosp_ed_linkage(
    records: pd.DataFrame,
    admissions: pd.DataFrame | None = None,
    edstays: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Build a de-duplicated ECG to hospital/ED linkage index.

    The source file exposes ED links as ``ed_stay_id``/``ed_hadm_id`` and
    hospital links as ``hosp_hadm_id``. Admissions and ED stays add the
    encounter start/end times when those source rows are available.
    """
    missing_columns = [col for col in SOURCE_COLUMNS if col not in records.columns]
    if missing_columns:
        raise KeyError(f"Missing required columns: {missing_columns}")

    df = records[SOURCE_COLUMNS].copy()
    for column in [
        "subject_id",
        "study_id",
        "ed_stay_id",
        "ed_hadm_id",
        "hosp_hadm_id",
    ]:
        df[column] = _normalize_id(df[column])
    df["ecg_time"] = _normalize_time(df["ecg_time"])

    df = df.dropna(subset=["subject_id", "study_id"]).reset_index(drop=True)
    base = df[["subject_id", "study_id", "ecg_time"]]

    ed_present = df["ed_stay_id"].notna() | df["ed_hadm_id"].notna()
    hosp_present = df["hosp_hadm_id"].notna()
    hadm_conflict = (
        df["ed_hadm_id"].notna()
        & df["hosp_hadm_id"].notna()
        & df["ed_hadm_id"].ne(df["hosp_hadm_id"])
    ).fillna(False)

    parts: list[pd.DataFrame] = []

    if ed_present.any():
        ed_rows = base.loc[ed_present].copy()
        ed_rows["hadm_id"] = df.loc[ed_present, "ed_hadm_id"].combine_first(
            df.loc[ed_present, "hosp_hadm_id"]
        )
        ed_rows["stay_id"] = df.loc[ed_present, "ed_stay_id"]
        parts.append(ed_rows[BASE_LINKAGE_COLUMNS])

    hosp_only_or_conflict = hosp_present & (~ed_present | hadm_conflict)
    if hosp_only_or_conflict.any():
        hosp_rows = base.loc[hosp_only_or_conflict].copy()
        hosp_rows["hadm_id"] = df.loc[hosp_only_or_conflict, "hosp_hadm_id"]
        hosp_rows["stay_id"] = pd.Series(
            pd.NA, index=hosp_rows.index, dtype="Int64"
        )
        parts.append(hosp_rows[BASE_LINKAGE_COLUMNS])

    unlinked = ~ed_present & ~hosp_present
    if unlinked.any():
        parts.append(_empty_linkage_like(base.loc[unlinked]))

    if parts:
        linkage = pd.concat(parts, ignore_index=True)
    else:
        linkage = pd.DataFrame(
            {
                "subject_id": pd.Series(dtype="Int64"),
                "study_id": pd.Series(dtype="Int64"),
                "ecg_time": pd.Series(dtype="datetime64[ns]"),
                "hadm_id": pd.Series(dtype="Int64"),
                "stay_id": pd.Series(dtype="Int64"),
            }
        )

    linkage = (
        linkage.drop_duplicates()
        .sort_values(
            ["subject_id", "study_id", "hadm_id", "stay_id"], na_position="last"
        )
        .reset_index(drop=True)
    )
    for column in ["subject_id", "study_id", "hadm_id", "stay_id"]:
        linkage[column] = _normalize_id(linkage[column])
    linkage["ecg_time"] = _normalize_time(linkage["ecg_time"])

    if admissions is not None:
        linkage = linkage.merge(
            _prepare_admissions(admissions), on="hadm_id", how="left"
        )
    else:
        linkage["hosp_start_time"] = pd.NaT
        linkage["hosp_end_time"] = pd.NaT

    if edstays is not None:
        linkage = linkage.merge(_prepare_edstays(edstays), on="stay_id", how="left")
    else:
        linkage["ed_start_time"] = pd.NaT
        linkage["ed_end_time"] = pd.NaT

    for column in [
        "ecg_time",
        "hosp_start_time",
        "hosp_end_time",
        "ed_start_time",
        "ed_end_time",
    ]:
        linkage[column] = _normalize_time(linkage[column])

    return linkage[LINKAGE_COLUMNS]


def read_mimic_ecg_hosp_ed_linkage_input(path: str | Path) -> pd.DataFrame:
    return pd.read_csv(path, usecols=SOURCE_COLUMNS)


def read_mimic_admissions(path: str | Path) -> pd.DataFrame:
    return pd.read_csv(path, usecols=["hadm_id", "admittime", "dischtime"])


def read_mimic_edstays(path: str | Path) -> pd.DataFrame:
    return pd.read_csv(path, usecols=["stay_id", "intime", "outtime"])


def write_mimic_ecg_hosp_ed_linkage(
    input_path: str | Path = DEFAULT_INPUT_PATH,
    output_path: str | Path = DEFAULT_OUTPUT_PATH,
    admissions_path: str | Path = DEFAULT_ADMISSIONS_PATH,
    edstays_path: str | Path = DEFAULT_EDSTAYS_PATH,
    compression: str = "snappy",
) -> dict[str, int | str]:
    records = read_mimic_ecg_hosp_ed_linkage_input(input_path)
    admissions = read_mimic_admissions(admissions_path)
    edstays = read_mimic_edstays(edstays_path)
    linkage = build_mimic_ecg_hosp_ed_linkage(records, admissions, edstays)

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    linkage.to_parquet(output_path, index=False, compression=compression)

    return {
        "input_path": str(input_path),
        "admissions_path": str(admissions_path),
        "edstays_path": str(edstays_path),
        "output_path": str(output_path),
        "source_rows": int(len(records)),
        "linkage_rows": int(len(linkage)),
        "unique_studies": int(linkage["study_id"].nunique()),
        "with_hadm_id": int(linkage["hadm_id"].notna().sum()),
        "with_stay_id": int(linkage["stay_id"].notna().sum()),
        "with_ecg_time": int(linkage["ecg_time"].notna().sum()),
        "with_hosp_time": int(linkage["hosp_start_time"].notna().sum()),
        "with_ed_time": int(linkage["ed_start_time"].notna().sum()),
        "unlinked_rows": int(
            (linkage["hadm_id"].isna() & linkage["stay_id"].isna()).sum()
        ),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build the MIMIC ECG hospital/ED linkage parquet index."
    )
    parser.add_argument("--input", default=str(DEFAULT_INPUT_PATH))
    parser.add_argument("--admissions", default=str(DEFAULT_ADMISSIONS_PATH))
    parser.add_argument("--edstays", default=str(DEFAULT_EDSTAYS_PATH))
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT_PATH))
    parser.add_argument("--compression", default="snappy")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    stats = write_mimic_ecg_hosp_ed_linkage(
        input_path=args.input,
        output_path=args.output,
        admissions_path=args.admissions,
        edstays_path=args.edstays,
        compression=args.compression,
    )
    for key, value in stats.items():
        print(f"{key}: {value}")


if __name__ == "__main__":
    main()
