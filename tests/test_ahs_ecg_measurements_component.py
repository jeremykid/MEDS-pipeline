import pandas as pd

from meds_pipeline.etl.ahs.ecg_measurements import AHSECGMeasurements


def test_ahs_ecg_measurements_outputs_value_num_only(monkeypatch):
    ecg_records = pd.DataFrame(
        {
            "PATID": [101, 102],
            "ecgId": ["N'abc-1'", "N'def-2'"],
            "dateAcquired": ["2021-01-01 10:00:00", "2021-01-02 10:00:00"],
        }
    )
    measurements = pd.DataFrame(
        {
            "ecgId": ["abc-1", "def-2"],
            "heartrate": [60, None],
            "qrsdur": [100, 110],
        }
    )

    cfg = {"raw_paths": {"ecg": "/tmp/ecg.pkl", "ecg_measurements": "/tmp/meas.pkl"}}
    base_cfg = {"show_progress": False}
    etl = AHSECGMeasurements(cfg, base_cfg)

    def _mock_load(path, _name):
        return ecg_records.copy() if path.endswith("ecg.pkl") else measurements.copy()

    monkeypatch.setattr(etl, "_load_pickle", _mock_load)

    out = etl.run_core()

    assert "value_num" in out.columns
    assert "numeric_value" not in out.columns
    assert set(out["event_type"]) == {"ECG"}
    assert set(out["code_system"]) == {"AHS_ECG"}
    assert set(out["source_table"]) == {"Globalmeasurements"}
    assert set(out["code"]) == {"ECG//HR", "ECG//QRS"}
    assert len(out) == 3
