import pandas as pd

from meds_pipeline.etl.ahs.labs import AHSLabs


def test_ahs_labs_outputs_value_num_only(tmp_path, monkeypatch):
    src = pd.DataFrame(
        {
            "PATID": [1, 1, None, 2],
            "TEST_VRFY_DTTM": [
                "2012-04-26 08:57:00",
                "2012-04-26 12:47:00",
                "2012-04-26 12:48:00",
                "2012-04-26 12:49:00",
            ],
            "TEST_CD": [b"HGB", b"XYZ", b"HGB", b"K"],
            "TEST_NM": [b"Hemoglobin", b"Unknown Test", b"Hemoglobin", b"Potassium"],
            "TEST_RSLT": [b"110", b"<0.10", b"120", b"abc"],
            "TEST_UOFM": [b"g/L", None, b"g/L", b""],
        }
    )

    def _mock_read_parquet(_):
        return src.copy()

    monkeypatch.setattr(pd, "read_parquet", _mock_read_parquet)

    fake_path = tmp_path / "labs.parquet"
    fake_path.write_text("placeholder", encoding="utf-8")

    cfg = {"raw_paths": {"labs": str(fake_path)}}
    base_cfg = {"show_progress": False}
    out = AHSLabs(cfg, base_cfg).run_core()

    assert "value_num" in out.columns
    assert "numeric_value" not in out.columns
    assert set(out["event_type"]) == {"lab"}
    assert set(out["source_table"]) == {"rmt22884_lab"}
    assert "value_text" in out.columns
    assert "provenance_id" in out.columns

    # Row with missing PATID should be filtered out.
    assert sorted(out["subject_id"].tolist()) == ["1", "1", "2"]

    # Comparator/value parsing
    row_lt = out[out["comparator"] == "<"].iloc[0]
    assert abs(float(row_lt["value_num"]) - 0.10) < 1e-9

    # Unit fallback from mapping for K when TEST_UOFM is empty.
    row_k = out[out["code"] == "LAB//LOINC//2823-3"].iloc[0]
    assert row_k["unit"] == "mmol/L"
