import pandas as pd

from meds_pipeline.etl.mimic.labs import MIMICLabs


def test_mimic_labs_basic_transform(tmp_path):
    data = pd.DataFrame(
        {
            "labevent_id": [1, 2, 3],
            "subject_id": [1001, 1001, None],
            "hadm_id": [2001, 2001, 2002],
            "itemid": [50931, 50931, 50932],
            "charttime": ["2180-03-23 11:51:00", "2180-03-23 12:00:00", "2180-03-23 12:01:00"],
            "value": ["95", "<0.10", "10"],
            "valuenum": [95.0, None, 10.0],
            "valueuom": ["mg/dL", "mg/dL", "mg/dL"],
        }
    )
    csv_path = tmp_path / "labevents.csv.gz"
    data.to_csv(csv_path, index=False, compression="gzip")

    cfg = {"raw_paths": {"labtests": str(csv_path)}}
    base_cfg = {"show_progress": False}
    out = MIMICLabs(cfg, base_cfg).run_core()

    assert len(out) == 2
    assert set(out["event_type"]) == {"lab"}
    assert set(out["code"]) == {"LAB//MIMIC//50931"}
    assert set(out["code_system"]) == {"MIMIC_LAB_ITEMID"}
    assert out["provenance_id"].tolist() == ["1", "2"]
    assert out["encounter_id"].tolist() == ["2001", "2001"]

    row_with_comparator = out[out["comparator"] == "<"].iloc[0]
    assert abs(float(row_with_comparator["value_num"]) - 0.10) < 1e-9


def test_mimic_labs_max_patients_keeps_first_seen_subjects(tmp_path):
    data = pd.DataFrame(
        {
            "labevent_id": [1, 2, 3, 4],
            "subject_id": [1001, 1002, 1001, 1003],
            "hadm_id": [2001, 2002, 2001, 2003],
            "itemid": [50931, 50931, 50932, 50933],
            "charttime": [
                "2180-03-23 11:51:00",
                "2180-03-23 11:52:00",
                "2180-03-24 10:00:00",
                "2180-03-24 10:05:00",
            ],
            "value": ["95", "96", "97", "98"],
            "valuenum": [95.0, 96.0, 97.0, 98.0],
            "valueuom": ["mg/dL", "mg/dL", "mg/dL", "mg/dL"],
        }
    )
    csv_path = tmp_path / "labevents.csv.gz"
    data.to_csv(csv_path, index=False, compression="gzip")

    cfg = {"raw_paths": {"labtests": str(csv_path)}}
    base_cfg = {"show_progress": False, "max_patients": 1}
    out = MIMICLabs(cfg, base_cfg).run_core()

    assert sorted(out["subject_id"].unique().tolist()) == ["1001"]
    assert len(out) == 2
