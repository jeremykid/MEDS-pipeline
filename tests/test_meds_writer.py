import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from meds_pipeline.meds.writer import (
    OPTIONAL_STABLE_STRING_COLUMNS,
    REQUIRED_STRING_COLUMNS,
    assign_patient_buckets,
    finalize_patient_bucketed_parquet,
    normalize_meds_core_schema,
    patient_index_path,
    reset_component_bucketed_staging,
    staged_component_names,
    sort_meds_core_for_patient_access,
    update_staging_manifest,
    write_component_bucketed_staging,
    write_component_bucketed_staging_chunk,
    write_patient_bucketed_parquet,
)


def _meds_core_df(**overrides):
    data = {
        "subject_id": [1001, 1002],
        "time": ["2020-01-01 00:00:00", "2020-01-02 12:30:00"],
        "event_type": ["lab", "diagnosis"],
        "code": ["LAB//A", "DX//B"],
        "code_system": ["LOCAL", None],
    }
    data.update(overrides)
    return pd.DataFrame(data)


def test_normalize_meds_core_schema_adds_missing_optional_string_columns():
    out = normalize_meds_core_schema(_meds_core_df())

    for column in REQUIRED_STRING_COLUMNS + OPTIONAL_STABLE_STRING_COLUMNS:
        assert column in out.columns
        assert str(out[column].dtype) == "string"

    assert str(out["time"].dtype) == "datetime64[ns]"
    assert out["value_text"].isna().all()
    assert pd.isna(out.loc[1, "code_system"])


def test_normalize_meds_core_schema_fills_missing_code_system_column():
    df = _meds_core_df()
    df = df.drop(columns=["code_system"])

    out = normalize_meds_core_schema(df)

    assert "code_system" in out.columns
    assert str(out["code_system"].dtype) == "string"
    assert out["code_system"].isna().all()


def test_normalize_meds_core_schema_casts_mixed_value_num_without_stringifying_nulls():
    df = _meds_core_df(
        subject_id=[1001, 1002, 1003, 1004, 1005, 1006],
        time=[
            "2020-01-01",
            "2020-01-02",
            "2020-01-03",
            "2020-01-04",
            "2020-01-05",
            "2020-01-06",
        ],
        event_type=["lab"] * 6,
        code=["LAB//A"] * 6,
        code_system=["LOCAL"] * 6,
        value_num=[1, 2.5, "3.5", None, np.nan, pd.NA],
    )

    out = normalize_meds_core_schema(df)

    assert str(out["value_num"].dtype) == "string"
    assert out["value_num"].iloc[:3].tolist() == ["1", "2.5", "3.5"]
    assert out["value_num"].iloc[3:].isna().all()


def test_normalize_meds_core_schema_writes_all_null_text_columns_as_arrow_string(tmp_path):
    out = normalize_meds_core_schema(_meds_core_df(value_text=[None, pd.NA]))

    path = tmp_path / "all_null_text.parquet"
    out.to_parquet(path, index=False)

    schema = pq.read_schema(path)
    assert schema.field("value_text").type == pa.string()
    assert schema.field("time").type == pa.timestamp("ns")


def test_normalized_chunks_keep_string_schema_when_one_chunk_is_all_null(tmp_path):
    df = _meds_core_df(
        subject_id=[1001, 1002, 1003, 1004],
        time=["2020-01-01", "2020-01-02", "2020-01-03", "2020-01-04"],
        event_type=["lab"] * 4,
        code=["LAB//A"] * 4,
        code_system=["LOCAL"] * 4,
        value_text=["has text", "also text", None, pd.NA],
    )
    out = normalize_meds_core_schema(df)

    first_path = tmp_path / "part_001.parquet"
    second_path = tmp_path / "part_002.parquet"
    out.iloc[:2].to_parquet(first_path, index=False)
    out.iloc[2:].to_parquet(second_path, index=False)

    assert pq.read_schema(first_path).field("value_text").type == pa.string()
    assert pq.read_schema(second_path).field("value_text").type == pa.string()


def test_assign_patient_buckets_keeps_each_subject_in_one_stable_bucket():
    df = normalize_meds_core_schema(
        _meds_core_df(
            subject_id=[1001, 1001, 1002, 1002],
            time=["2020-01-01", "2020-01-02", "2020-01-01", "2020-01-02"],
            event_type=["lab", "diagnosis", "lab", "diagnosis"],
            code=["LAB//A", "DX//B", "LAB//A", "DX//B"],
            code_system=["LOCAL", "ICD10", "LOCAL", "ICD10"],
        )
    )

    first = assign_patient_buckets(df, 16)
    second = assign_patient_buckets(df.sample(frac=1, random_state=42), 16)

    assert first.groupby("subject_id")["bucket"].nunique().max() == 1
    expected = first.drop_duplicates("subject_id").set_index("subject_id")["bucket"]
    actual = second.drop_duplicates("subject_id").set_index("subject_id")["bucket"]
    assert actual.to_dict() == expected.to_dict()


def test_sort_meds_core_for_patient_access_orders_patient_timelines():
    df = normalize_meds_core_schema(
        _meds_core_df(
            subject_id=[1002, 1001, 1001, 1002],
            time=["2020-01-02", "2020-01-03", "2020-01-01", "2020-01-01"],
            event_type=["z", "b", "a", "a"],
            code=["Z", "B", "A", "A"],
            code_system=["LOCAL"] * 4,
        )
    )

    out = sort_meds_core_for_patient_access(df)

    assert out[["subject_id", "time", "event_type", "code"]].astype(str).values.tolist() == [
        ["1001", "2020-01-01", "a", "A"],
        ["1001", "2020-01-03", "b", "B"],
        ["1002", "2020-01-01", "a", "A"],
        ["1002", "2020-01-02", "z", "Z"],
    ]


def test_write_patient_bucketed_parquet_writes_sorted_schema_stable_dataset(tmp_path):
    df = _meds_core_df(
        subject_id=[1002, 1001, 1001, 1002],
        time=["2020-01-02", "2020-01-03", "2020-01-01", "2020-01-01"],
        event_type=["z", "b", "a", "a"],
        code=["Z", "B", "A", "A"],
        code_system=["LOCAL"] * 4,
        value_text=[pd.NA, "late", pd.NA, pd.NA],
    )

    dataset_dir = write_patient_bucketed_parquet(df, tmp_path, "mimic", num_buckets=16)
    parquet_files = sorted(dataset_dir.glob("bucket*.parquet"))

    assert parquet_files
    assert not list(dataset_dir.glob("bucket=*"))
    schemas = [pq.read_schema(path) for path in parquet_files]
    assert all("bucket" not in schema.names for schema in schemas)
    assert all(schema.field("value_text").type == pa.string() for schema in schemas)
    assert all(schema.field("time").type == pa.timestamp("ns") for schema in schemas)

    out = pd.concat([pd.read_parquet(path) for path in parquet_files], ignore_index=True)
    assert out.groupby("subject_id").ngroups == 2
    assert all(group["time"].is_monotonic_increasing for _, group in out.groupby("subject_id"))

    index = pd.read_parquet(patient_index_path(tmp_path, "mimic"))
    assert index.columns.tolist() == [
        "source",
        "subject_id",
        "bucket",
        "file_path",
        "row_count",
        "min_time",
        "max_time",
    ]
    assert set(index["subject_id"].astype(str)) == {"1001", "1002"}
    assert set(index["file_path"].astype(str)) <= {f"mimic_meds_core_by_patient/{path.name}" for path in parquet_files}
    assert int(index["row_count"].sum()) == len(out)


def test_two_stage_component_staging_finalize_and_incremental_refresh(tmp_path):
    labs = _meds_core_df(
        subject_id=[1002, 1001, 1001],
        time=["2020-01-02", "2020-01-03", "2020-01-01"],
        event_type=["lab", "lab", "lab"],
        code=["LAB//B", "LAB//C", "LAB//A"],
        code_system=["LOCAL"] * 3,
        value_text=[pd.NA, "late", pd.NA],
    )
    procedures = _meds_core_df(
        subject_id=[1001, 1003],
        time=["2020-01-02", "2020-01-01"],
        event_type=["procedures", "procedures"],
        code=["PROC//A", "PROC//B"],
    ).drop(columns=["code_system"])
    patient_ids = ["1001", "1002"]

    stats = [
        write_component_bucketed_staging(labs, tmp_path, "labs", 8, patient_ids=patient_ids),
        write_component_bucketed_staging(
            procedures,
            tmp_path,
            "procedures",
            8,
            patient_ids=patient_ids,
        ),
    ]
    update_staging_manifest(
        tmp_path,
        source="mimic",
        num_buckets=8,
        compression="snappy",
        component_stats=stats,
        patient_ids=patient_ids,
        reset=True,
    )

    assert staged_component_names(tmp_path) == ["labs", "procedures"]

    result = finalize_patient_bucketed_parquet(
        tmp_path,
        source="mimic",
        num_buckets=8,
        patient_ids=patient_ids,
    )
    files = sorted((tmp_path / "mimic_meds_core_by_patient").glob("bucket*.parquet"))
    schemas = [pq.read_schema(path) for path in files]
    out = pd.concat([pd.read_parquet(path) for path in files], ignore_index=True)

    assert result["row_count"] == 4
    assert all(schema.equals(schemas[0]) for schema in schemas)
    assert set(out["subject_id"].astype(str)) == {"1001", "1002"}
    assert "1003" not in set(out["subject_id"].astype(str))
    assert out.groupby("subject_id")["time"].apply(lambda s: s.is_monotonic_increasing).all()
    assert out.loc[out["event_type"] == "procedures", "code_system"].isna().all()

    index = pd.read_parquet(patient_index_path(tmp_path, "mimic"))
    assert set(index["subject_id"].astype(str)) == {"1001", "1002"}
    assert int(index["row_count"].sum()) == result["row_count"]
    assert set(index["file_path"].astype(str)) <= {f"mimic_meds_core_by_patient/{path.name}" for path in files}

    refreshed_labs = _meds_core_df(
        subject_id=[1001],
        time=["2020-01-04"],
        event_type=["lab"],
        code=["LAB//REFRESHED"],
        code_system=["LOCAL"],
    )
    refreshed_stats = [
        write_component_bucketed_staging(
            refreshed_labs,
            tmp_path,
            "labs",
            8,
            patient_ids=patient_ids,
        )
    ]
    update_staging_manifest(
        tmp_path,
        source="mimic",
        num_buckets=8,
        compression="snappy",
        component_stats=refreshed_stats,
        patient_ids=patient_ids,
        reset=False,
    )
    refreshed = finalize_patient_bucketed_parquet(
        tmp_path,
        source="mimic",
        num_buckets=8,
        patient_ids=patient_ids,
    )
    refreshed_files = sorted((tmp_path / "mimic_meds_core_by_patient").glob("bucket*.parquet"))
    refreshed_out = pd.concat(
        [pd.read_parquet(path) for path in refreshed_files],
        ignore_index=True,
    )

    assert refreshed["row_count"] == 2
    assert set(refreshed_out["code"].astype(str)) == {"LAB//REFRESHED", "PROC//A"}

    refreshed_index = pd.read_parquet(patient_index_path(tmp_path, "mimic"))
    assert refreshed_index["subject_id"].astype(str).tolist() == ["1001"]
    assert int(refreshed_index["row_count"].sum()) == refreshed["row_count"]


def test_finalize_reads_multiple_staged_parts_per_component_bucket(tmp_path):
    first_chunk = _meds_core_df(
        subject_id=[1001, 1002],
        time=["2020-01-03", "2020-01-01"],
        event_type=["lab", "lab"],
        code=["LAB//LATE", "LAB//B"],
        code_system=["LOCAL", "LOCAL"],
        value_text=[pd.NA, "text"],
    )
    second_chunk = _meds_core_df(
        subject_id=[1001],
        time=["2020-01-01"],
        event_type=["lab"],
        code=["LAB//EARLY"],
        code_system=["LOCAL"],
        value_text=[pd.NA],
    )

    reset_component_bucketed_staging(tmp_path, "labs")
    stats = []
    stats.append(write_component_bucketed_staging_chunk(first_chunk, tmp_path, "labs", 1, part_number=0))
    stats.append(write_component_bucketed_staging_chunk(second_chunk, tmp_path, "labs", 1, part_number=1))
    merged_stats = {
        "name": "labs",
        "row_count": sum(stat["row_count"] for stat in stats),
        "buckets": {"000": sum(stat["buckets"].get("000", 0) for stat in stats)},
    }
    update_staging_manifest(
        tmp_path,
        source="mimic",
        num_buckets=1,
        compression="snappy",
        component_stats=[merged_stats],
        reset=True,
    )

    result = finalize_patient_bucketed_parquet(tmp_path, source="mimic", num_buckets=1)
    output_path = tmp_path / "mimic_meds_core_by_patient" / "bucket000.parquet"
    schema = pq.read_schema(output_path)
    out = pd.read_parquet(output_path)

    assert result["row_count"] == 3
    assert schema.field("value_text").type == pa.string()
    assert out["code"].tolist() == ["LAB//EARLY", "LAB//LATE", "LAB//B"]
