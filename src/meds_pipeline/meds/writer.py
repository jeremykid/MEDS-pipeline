from __future__ import annotations

import hashlib
import json
from pathlib import Path
import shutil

import pandas as pd
import pyarrow.parquet as pq


REQUIRED_STRING_COLUMNS = ("subject_id", "event_type", "code", "code_system")
TIME_COLUMN = "time"
MINIMUM_REQUIRED_COLUMNS = ("subject_id", TIME_COLUMN, "event_type", "code")
OPTIONAL_STABLE_STRING_COLUMNS = (
    "value_num",
    "value_text",
    "unit",
    "comparator",
    "source_table",
    "provenance_id",
    "encounter_id",
    "site",
    "ecg_id",
)
STABLE_MEDS_CORE_COLUMNS = (
    "subject_id",
    "time",
    "event_type",
    "code",
    "code_system",
    *OPTIONAL_STABLE_STRING_COLUMNS,
)
PATIENT_SORT_COLUMNS = ("subject_id", "time", "event_type", "code")
STAGING_DIR_NAME = "_staging_meds_core_by_patient"
COMPONENTS_DIR_NAME = "components"
MANIFEST_FILE_NAME = "manifest.json"
PATIENT_INDEX_SUFFIX = "_patient_index.parquet"


def normalize_meds_core_schema(df: pd.DataFrame) -> pd.DataFrame:
    """Return a MEDS-Core dataframe with stable dtypes for parquet output."""
    missing_required = [
        column
        for column in MINIMUM_REQUIRED_COLUMNS
        if column not in df.columns
    ]
    if missing_required:
        raise ValueError(
            "MEDS-Core output is missing required columns: "
            + ", ".join(missing_required)
        )

    out = df.copy(deep=False)

    for column in REQUIRED_STRING_COLUMNS:
        if column not in out.columns:
            out[column] = pd.Series(pd.NA, index=out.index, dtype="string")
        out[column] = out[column].astype("string")

    out[TIME_COLUMN] = _normalize_time_column(out[TIME_COLUMN])

    for column in OPTIONAL_STABLE_STRING_COLUMNS:
        if column not in out.columns:
            out[column] = pd.Series(pd.NA, index=out.index, dtype="string")
        else:
            out[column] = out[column].astype("string")

    stable_columns = [column for column in STABLE_MEDS_CORE_COLUMNS if column in out.columns]
    extra_columns = [column for column in out.columns if column not in stable_columns]
    return out[stable_columns + extra_columns]


def sort_meds_core_for_patient_access(df: pd.DataFrame) -> pd.DataFrame:
    """Return MEDS rows in deterministic patient timeline order."""
    missing_sort_columns = [column for column in PATIENT_SORT_COLUMNS if column not in df.columns]
    if missing_sort_columns:
        raise ValueError(
            "MEDS-Core output is missing patient sort columns: "
            + ", ".join(missing_sort_columns)
        )

    return df.sort_values(
        list(PATIENT_SORT_COLUMNS),
        kind="mergesort",
        na_position="last",
    ).reset_index(drop=True)


def assign_patient_buckets(
    df: pd.DataFrame,
    num_buckets: int,
    bucket_column: str = "bucket",
) -> pd.DataFrame:
    """Return a copy with a stable zero-padded bucket label per subject_id."""
    if num_buckets <= 0:
        raise ValueError("num_buckets must be greater than 0")
    if "subject_id" not in df.columns:
        raise ValueError("MEDS-Core output is missing required column: subject_id")

    out = df.copy(deep=False)
    bucket_width = max(3, len(str(num_buckets - 1)))
    subject_ids = out["subject_id"].astype("string").fillna("<NA>").astype(str)
    bucket_by_subject = {
        subject_id: _bucket_label(subject_id, num_buckets, bucket_width)
        for subject_id in pd.unique(subject_ids)
    }
    out[bucket_column] = subject_ids.map(bucket_by_subject).astype("string")
    return out


def write_patient_bucketed_parquet(
    df: pd.DataFrame,
    output_dir: str | Path,
    source: str,
    num_buckets: int = 256,
    compression: str = "snappy",
) -> Path:
    """Write a flat patient-bucketed MEDS parquet dataset and return its directory."""
    normalized = normalize_meds_core_schema(df)
    sorted_df = sort_meds_core_for_patient_access(normalized)
    bucketed = assign_patient_buckets(sorted_df, num_buckets)

    output_dir = Path(output_dir)
    dataset_dir = output_dir / f"{source}_meds_core_by_patient"
    if dataset_dir.exists():
        shutil.rmtree(dataset_dir)
    dataset_dir.mkdir(parents=True, exist_ok=True)
    index_path = patient_index_path(output_dir, source)
    if index_path.exists():
        index_path.unlink()

    data_columns = [column for column in bucketed.columns if column != "bucket"]
    index_parts = []
    for bucket, bucket_df in bucketed.groupby("bucket", sort=True, observed=True):
        output_path = final_bucket_file_path(dataset_dir, str(bucket))
        bucket_df.loc[:, data_columns].to_parquet(
            output_path,
            index=False,
            compression=compression,
        )
        index_parts.append(_patient_index_for_bucket(source, str(bucket), output_path, bucket_df))

    write_patient_index(output_dir, source, index_parts, compression=compression)
    return dataset_dir


def staging_root(output_dir: str | Path) -> Path:
    return Path(output_dir) / STAGING_DIR_NAME


def components_staging_root(output_dir: str | Path) -> Path:
    return staging_root(output_dir) / COMPONENTS_DIR_NAME


def component_staging_dir(output_dir: str | Path, component: str) -> Path:
    return components_staging_root(output_dir) / component


def manifest_path(output_dir: str | Path) -> Path:
    return staging_root(output_dir) / MANIFEST_FILE_NAME


def patient_index_path(output_dir: str | Path, source: str) -> Path:
    return Path(output_dir) / f"{source}{PATIENT_INDEX_SUFFIX}"


def final_bucket_file_path(dataset_dir: str | Path, bucket: str) -> Path:
    return Path(dataset_dir) / f"bucket{bucket}.parquet"


def load_staging_manifest(output_dir: str | Path) -> dict:
    path = manifest_path(output_dir)
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def reset_bucketed_staging(output_dir: str | Path) -> None:
    root = staging_root(output_dir)
    if root.exists():
        shutil.rmtree(root)


def reset_component_bucketed_staging(output_dir: str | Path, component: str) -> None:
    comp_dir = component_staging_dir(output_dir, component)
    if comp_dir.exists():
        shutil.rmtree(comp_dir)


def write_component_bucketed_staging(
    df: pd.DataFrame,
    output_dir: str | Path,
    component: str,
    num_buckets: int,
    compression: str = "snappy",
    patient_ids: list[str] | None = None,
) -> dict:
    """Write one component's normalized rows into patient-bucketed staging."""
    reset_component_bucketed_staging(output_dir, component)
    return write_component_bucketed_staging_chunk(
        df,
        output_dir=output_dir,
        component=component,
        num_buckets=num_buckets,
        compression=compression,
        patient_ids=patient_ids,
        part_number=0,
    )


def write_component_bucketed_staging_chunk(
    df: pd.DataFrame,
    output_dir: str | Path,
    component: str,
    num_buckets: int,
    compression: str = "snappy",
    patient_ids: list[str] | None = None,
    part_number: int = 0,
) -> dict:
    """Append one normalized component chunk into patient-bucketed staging."""
    normalized = normalize_meds_core_schema(df)
    if patient_ids is not None:
        keep = set(str(patient_id) for patient_id in patient_ids)
        normalized = normalized[normalized["subject_id"].astype(str).isin(keep)]

    sorted_df = sort_meds_core_for_patient_access(normalized)
    bucketed = assign_patient_buckets(sorted_df, num_buckets)

    comp_dir = component_staging_dir(output_dir, component)
    comp_dir.mkdir(parents=True, exist_ok=True)

    data_columns = [column for column in bucketed.columns if column != "bucket"]
    bucket_counts = {}
    for bucket, bucket_df in bucketed.groupby("bucket", sort=True, observed=True):
        bucket_dir = comp_dir / f"bucket={bucket}"
        bucket_dir.mkdir(parents=True, exist_ok=True)
        bucket_df.loc[:, data_columns].to_parquet(
            bucket_dir / f"part-{part_number:03d}.parquet",
            index=False,
            compression=compression,
        )
        bucket_counts[str(bucket)] = int(len(bucket_df))

    return {
        "name": component,
        "row_count": int(len(sorted_df)),
        "buckets": bucket_counts,
    }


def update_staging_manifest(
    output_dir: str | Path,
    source: str,
    num_buckets: int,
    compression: str,
    component_stats: list[dict],
    patient_ids: list[str] | None = None,
    reset: bool = False,
) -> dict:
    """Record staged component metadata while preserving component order."""
    if reset:
        manifest = {}
    else:
        manifest = load_staging_manifest(output_dir)

    existing_components = manifest.get("components", [])
    by_name = {component["name"]: component for component in existing_components}
    ordered_names = [component["name"] for component in existing_components]

    for stats in component_stats:
        name = stats["name"]
        by_name[name] = stats
        if name not in ordered_names:
            ordered_names.append(name)

    manifest = {
        "source": source,
        "num_buckets": int(num_buckets),
        "compression": compression,
        "patient_ids": [str(patient_id) for patient_id in patient_ids]
        if patient_ids is not None
        else manifest.get("patient_ids"),
        "components": [by_name[name] for name in ordered_names if name in by_name],
    }

    path = manifest_path(output_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return manifest


def staged_component_names(output_dir: str | Path) -> list[str]:
    manifest = load_staging_manifest(output_dir)
    names = [component["name"] for component in manifest.get("components", [])]
    if names:
        return names

    root = components_staging_root(output_dir)
    if not root.exists():
        return []
    return sorted(path.name for path in root.iterdir() if path.is_dir())


def finalize_patient_bucketed_parquet(
    output_dir: str | Path,
    source: str,
    num_buckets: int,
    compression: str = "snappy",
    component_names: list[str] | None = None,
    patient_ids: list[str] | None = None,
) -> dict:
    """Merge component staging bucket-by-bucket into a flat final dataset."""
    output_dir = Path(output_dir)
    if component_names is None:
        component_names = staged_component_names(output_dir)
    if not component_names:
        raise ValueError("No staged components found to finalize")

    all_columns = _collect_staged_columns(output_dir, component_names)
    if not all_columns:
        raise ValueError("No staged parquet files found to finalize")

    dataset_dir = output_dir / f"{source}_meds_core_by_patient"
    if dataset_dir.exists():
        shutil.rmtree(dataset_dir)
    dataset_dir.mkdir(parents=True, exist_ok=True)
    index_path = patient_index_path(output_dir, source)
    if index_path.exists():
        index_path.unlink()

    keep = set(str(patient_id) for patient_id in patient_ids) if patient_ids is not None else None
    total_rows = 0
    bucket_counts = {}
    index_parts = []
    for bucket in _bucket_labels(num_buckets):
        frames = []
        for component in component_names:
            for path in _staged_bucket_part_paths(output_dir, component, bucket):
                frames.append(pd.read_parquet(path))

        if not frames:
            continue

        bucket_df = pd.concat(frames, ignore_index=True)
        if keep is not None:
            bucket_df = bucket_df[bucket_df["subject_id"].astype(str).isin(keep)]
        if bucket_df.empty:
            continue

        bucket_df = normalize_meds_core_schema(bucket_df)
        bucket_df = _align_to_final_columns(bucket_df, all_columns)
        bucket_df = sort_meds_core_for_patient_access(bucket_df)

        output_path = final_bucket_file_path(dataset_dir, bucket)
        bucket_df.to_parquet(
            output_path,
            index=False,
            compression=compression,
        )
        index_parts.append(_patient_index_for_bucket(source, bucket, output_path, bucket_df))
        bucket_counts[bucket] = int(len(bucket_df))
        total_rows += int(len(bucket_df))

    write_patient_index(output_dir, source, index_parts, compression=compression)

    return {
        "dataset_dir": str(dataset_dir),
        "patient_index_path": str(patient_index_path(output_dir, source)),
        "components": component_names,
        "row_count": total_rows,
        "buckets": bucket_counts,
    }


def _normalize_time_column(series: pd.Series) -> pd.Series:
    time = pd.to_datetime(series, errors="coerce")
    if isinstance(time.dtype, pd.DatetimeTZDtype):
        time = time.dt.tz_convert(None)
    return time.astype("datetime64[ns]")


def _bucket_label(subject_id: str, num_buckets: int, bucket_width: int) -> str:
    digest = hashlib.blake2b(subject_id.encode("utf-8"), digest_size=8).digest()
    bucket = int.from_bytes(digest, byteorder="big", signed=False) % num_buckets
    return f"{bucket:0{bucket_width}d}"


def _bucket_labels(num_buckets: int) -> list[str]:
    if num_buckets <= 0:
        raise ValueError("num_buckets must be greater than 0")
    width = max(3, len(str(num_buckets - 1)))
    return [f"{bucket:0{width}d}" for bucket in range(num_buckets)]


def _collect_staged_columns(output_dir: Path, component_names: list[str]) -> list[str]:
    columns: list[str] = []
    seen = set()

    for column in STABLE_MEDS_CORE_COLUMNS:
        columns.append(column)
        seen.add(column)

    for component in component_names:
        for path in sorted(component_staging_dir(output_dir, component).glob("bucket=*/part-*.parquet")):
            schema = pq.read_schema(path)
            for column in schema.names:
                if column not in seen:
                    columns.append(column)
                    seen.add(column)

    return columns


def _staged_bucket_part_paths(output_dir: Path, component: str, bucket: str) -> list[Path]:
    bucket_dir = component_staging_dir(output_dir, component) / f"bucket={bucket}"
    return sorted(bucket_dir.glob("part-*.parquet"))


def _align_to_final_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    out = df.copy(deep=False)
    stable = set(STABLE_MEDS_CORE_COLUMNS)
    for column in columns:
        if column not in out.columns:
            dtype = "datetime64[ns]" if column == TIME_COLUMN else "string"
            out[column] = pd.Series(pd.NA, index=out.index, dtype=dtype)
        elif column not in stable and column != TIME_COLUMN:
            out[column] = out[column].astype("string")
    return out[columns]



def _patient_index_for_bucket(
    source: str,
    bucket: str,
    output_path: Path,
    bucket_df: pd.DataFrame,
) -> pd.DataFrame:
    index = (
        bucket_df.groupby("subject_id", sort=True, observed=True)
        .agg(
            row_count=("subject_id", "size"),
            min_time=(TIME_COLUMN, "min"),
            max_time=(TIME_COLUMN, "max"),
        )
        .reset_index()
    )
    index.insert(0, "source", source)
    index.insert(2, "bucket", str(bucket))
    index.insert(3, "file_path", f"{output_path.parent.name}/{output_path.name}")
    return _normalize_patient_index_schema(index)


def _empty_patient_index() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "source": pd.Series(dtype="string"),
            "subject_id": pd.Series(dtype="string"),
            "bucket": pd.Series(dtype="string"),
            "file_path": pd.Series(dtype="string"),
            "row_count": pd.Series(dtype="int64"),
            "min_time": pd.Series(dtype="datetime64[ns]"),
            "max_time": pd.Series(dtype="datetime64[ns]"),
        }
    )


def _normalize_patient_index_schema(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy(deep=False)
    for column in ("source", "subject_id", "bucket", "file_path"):
        out[column] = out[column].astype("string")
    out["row_count"] = out["row_count"].astype("int64")
    out["min_time"] = _normalize_time_column(out["min_time"])
    out["max_time"] = _normalize_time_column(out["max_time"])
    return out[["source", "subject_id", "bucket", "file_path", "row_count", "min_time", "max_time"]]


def write_patient_index(
    output_dir: str | Path,
    source: str,
    index_parts: list[pd.DataFrame],
    compression: str = "snappy",
) -> Path:
    path = patient_index_path(output_dir, source)
    path.parent.mkdir(parents=True, exist_ok=True)
    if index_parts:
        index = pd.concat(index_parts, ignore_index=True)
        index = _normalize_patient_index_schema(index)
    else:
        index = _empty_patient_index()
    index.to_parquet(path, index=False, compression=compression)
    return path
