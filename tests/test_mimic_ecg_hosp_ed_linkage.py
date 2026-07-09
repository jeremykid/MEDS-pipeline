import pandas as pd

from meds_pipeline.preprocessing.mimic.ecg_hosp_ed_linkage import (
    LINKAGE_COLUMNS,
    build_mimic_ecg_hosp_ed_linkage,
    write_mimic_ecg_hosp_ed_linkage,
)


def test_build_mimic_ecg_hosp_ed_linkage_keeps_requested_contract():
    records = pd.DataFrame(
        {
            "subject_id": [1, 1, 2, 3, 4, 4],
            "study_id": [11, 12, 13, 14, 15, 15],
            "ecg_time": [
                "2100-01-01 01:00:00",
                "2100-01-02 01:00:00",
                "2100-01-03 01:00:00",
                "2100-01-04 01:00:00",
                "2100-01-05 01:00:00",
                "2100-01-05 01:00:00",
            ],
            "ed_stay_id": [101, None, None, 104, 105, 105],
            "ed_hadm_id": [201, None, None, 204, 205, 205],
            "hosp_hadm_id": [None, 202, None, 204, 305, 305],
        }
    )
    admissions = pd.DataFrame(
        {
            "hadm_id": [201, 202, 204, 205, 305],
            "admittime": [
                "2100-01-01 00:30:00",
                "2100-01-02 00:30:00",
                "2100-01-04 00:30:00",
                "2100-01-05 00:30:00",
                "2100-01-06 00:30:00",
            ],
            "dischtime": [
                "2100-01-01 04:00:00",
                "2100-01-02 04:00:00",
                "2100-01-04 04:00:00",
                "2100-01-05 04:00:00",
                "2100-01-06 04:00:00",
            ],
        }
    )
    edstays = pd.DataFrame(
        {
            "stay_id": [101, 104, 105],
            "intime": [
                "2100-01-01 00:00:00",
                "2100-01-04 00:00:00",
                "2100-01-05 00:00:00",
            ],
            "outtime": [
                "2100-01-01 02:00:00",
                "2100-01-04 02:00:00",
                "2100-01-05 02:00:00",
            ],
        }
    )

    linkage = build_mimic_ecg_hosp_ed_linkage(records, admissions, edstays)

    assert linkage.columns.tolist() == LINKAGE_COLUMNS
    assert {str(linkage[col].dtype) for col in ["subject_id", "study_id", "hadm_id", "stay_id"]} == {"Int64"}
    assert str(linkage["ecg_time"].dtype) == "datetime64[ns]"
    assert str(linkage["hosp_start_time"].dtype) == "datetime64[ns]"
    assert str(linkage["ed_start_time"].dtype) == "datetime64[ns]"

    expected = pd.DataFrame(
        {
            "subject_id": pd.Series([1, 1, 2, 3, 4, 4], dtype="Int64"),
            "study_id": pd.Series([11, 12, 13, 14, 15, 15], dtype="Int64"),
            "ecg_time": pd.to_datetime(
                [
                    "2100-01-01 01:00:00",
                    "2100-01-02 01:00:00",
                    "2100-01-03 01:00:00",
                    "2100-01-04 01:00:00",
                    "2100-01-05 01:00:00",
                    "2100-01-05 01:00:00",
                ]
            ),
            "hadm_id": pd.Series([201, 202, None, 204, 205, 305], dtype="Int64"),
            "hosp_start_time": pd.to_datetime(
                [
                    "2100-01-01 00:30:00",
                    "2100-01-02 00:30:00",
                    None,
                    "2100-01-04 00:30:00",
                    "2100-01-05 00:30:00",
                    "2100-01-06 00:30:00",
                ]
            ),
            "hosp_end_time": pd.to_datetime(
                [
                    "2100-01-01 04:00:00",
                    "2100-01-02 04:00:00",
                    None,
                    "2100-01-04 04:00:00",
                    "2100-01-05 04:00:00",
                    "2100-01-06 04:00:00",
                ]
            ),
            "stay_id": pd.Series([101, None, None, 104, 105, None], dtype="Int64"),
            "ed_start_time": pd.to_datetime(
                [
                    "2100-01-01 00:00:00",
                    None,
                    None,
                    "2100-01-04 00:00:00",
                    "2100-01-05 00:00:00",
                    None,
                ]
            ),
            "ed_end_time": pd.to_datetime(
                [
                    "2100-01-01 02:00:00",
                    None,
                    None,
                    "2100-01-04 02:00:00",
                    "2100-01-05 02:00:00",
                    None,
                ]
            ),
        }
    )
    pd.testing.assert_frame_equal(linkage, expected)


def test_write_mimic_ecg_hosp_ed_linkage_writes_parquet(tmp_path):
    records = pd.DataFrame(
        {
            "subject_id": [1],
            "study_id": [11],
            "ecg_time": ["2100-01-01 01:00:00"],
            "ed_stay_id": [101],
            "ed_hadm_id": [201],
            "hosp_hadm_id": [None],
        }
    )
    admissions = pd.DataFrame(
        {
            "hadm_id": [201],
            "admittime": ["2100-01-01 00:30:00"],
            "dischtime": ["2100-01-01 04:00:00"],
        }
    )
    edstays = pd.DataFrame(
        {
            "stay_id": [101],
            "intime": ["2100-01-01 00:00:00"],
            "outtime": ["2100-01-01 02:00:00"],
        }
    )
    input_path = tmp_path / "records_w_diag_icd10.csv"
    admissions_path = tmp_path / "admissions.csv.gz"
    edstays_path = tmp_path / "edstays.csv.gz"
    output_path = tmp_path / "mimic_ecg_hosp_ed_linkage.parquet"
    records.to_csv(input_path, index=False)
    admissions.to_csv(admissions_path, index=False, compression="gzip")
    edstays.to_csv(edstays_path, index=False, compression="gzip")

    stats = write_mimic_ecg_hosp_ed_linkage(
        input_path,
        output_path,
        admissions_path=admissions_path,
        edstays_path=edstays_path,
    )
    out = pd.read_parquet(output_path)

    assert stats["source_rows"] == 1
    assert stats["linkage_rows"] == 1
    assert stats["with_ecg_time"] == 1
    assert stats["with_hosp_time"] == 1
    assert stats["with_ed_time"] == 1
    assert out[LINKAGE_COLUMNS].to_dict("records") == [
        {
            "subject_id": 1,
            "study_id": 11,
            "ecg_time": pd.Timestamp("2100-01-01 01:00:00"),
            "hadm_id": 201,
            "hosp_start_time": pd.Timestamp("2100-01-01 00:30:00"),
            "hosp_end_time": pd.Timestamp("2100-01-01 04:00:00"),
            "stay_id": 101,
            "ed_start_time": pd.Timestamp("2100-01-01 00:00:00"),
            "ed_end_time": pd.Timestamp("2100-01-01 02:00:00"),
        }
    ]
