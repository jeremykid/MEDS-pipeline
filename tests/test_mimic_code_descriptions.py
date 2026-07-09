from __future__ import annotations

import pandas as pd

from meds_pipeline.etl.mimic.diagnosis import MIMICDiagnosis
from meds_pipeline.etl.mimic.procedures import MIMICProcedures


class FakeMapper:
    def __init__(self, descriptions: dict[str, str]) -> None:
        self.descriptions = descriptions

    def get_description(self, code: str, default: str = "Unknown") -> str:
        return self.descriptions.get(code, default)


def test_mimic_hospital_diagnosis_value_text_is_only_mapper_description(monkeypatch) -> None:
    etl = MIMICDiagnosis(cfg={"raw_paths": {}}, base_cfg={})
    hosp = pd.DataFrame(
        {
            "subject_id": ["100"],
            "hadm_id": ["200"],
            "seq_num": [1],
            "icd_code": ["I509"],
            "icd_version": ["10"],
            "source_table": ["hosp.diagnoses_icd"],
            "time_col": ["2020-01-01 00:00:00"],
        }
    )

    monkeypatch.setattr(etl, "_load_hospital_diagnoses", lambda: hosp)
    monkeypatch.setattr(etl, "_load_ed_diagnoses", lambda: pd.DataFrame())
    monkeypatch.setattr(
        etl,
        "_load_diagnosis_mapper",
        lambda: FakeMapper({"DIAGNOSIS//ICD//10//I509": "Heart failure, unspecified"}),
    )

    out = etl.run_core()

    assert out["code"].iloc[0] == "DIAGNOSIS//ICD//10//I509"
    assert out["value_text"].iloc[0] == "Heart failure, unspecified"


def test_mimic_ed_diagnosis_value_text_prefers_icd_title(monkeypatch) -> None:
    etl = MIMICDiagnosis(cfg={"raw_paths": {}}, base_cfg={})
    ed = pd.DataFrame(
        {
            "subject_id": ["101"],
            "stay_id": ["300"],
            "seq_num": [2],
            "icd_code": ["R079"],
            "icd_version": ["10"],
            "icd_title": ["Chest pain, unspecified"],
            "source_table": ["ed.diagnosis"],
            "time_col": ["2020-01-02 00:00:00"],
        }
    )

    monkeypatch.setattr(etl, "_load_hospital_diagnoses", lambda: pd.DataFrame())
    monkeypatch.setattr(etl, "_load_ed_diagnoses", lambda: ed)
    monkeypatch.setattr(
        etl,
        "_load_diagnosis_mapper",
        lambda: FakeMapper({"DIAGNOSIS//ICD//10//R079": "Mapper fallback title"}),
    )

    out = etl.run_core()

    assert out["code"].iloc[0] == "DIAGNOSIS//ICD//10//R079"
    assert out["value_text"].iloc[0] == "Chest pain, unspecified"


def test_mimic_procedure_value_text_is_only_mapper_description(monkeypatch) -> None:
    etl = MIMICProcedures(cfg={"raw_paths": {"procedures_icd": "procedures_icd.csv.gz"}}, base_cfg={})
    procedures = pd.DataFrame(
        {
            "subject_id": ["100"],
            "hadm_id": ["200"],
            "seq_num": [1],
            "chartdate": ["2020-01-02"],
            "icd_code": ["0JH60BZ"],
            "icd_version": ["10"],
        }
    )

    monkeypatch.setattr(pd, "read_csv", lambda *args, **kwargs: procedures.copy())
    monkeypatch.setattr(
        etl,
        "_load_procedure_mapper",
        lambda: FakeMapper({"PROCEDURE//ICD//10//0JH60BZ": "Insertion of pacemaker lead"}),
    )

    out = etl.run_core()

    assert out["code"].iloc[0] == "PROCEDURE//ICD//10//0JH60BZ"
    assert out["value_text"].iloc[0] == "Insertion of pacemaker lead"


def test_unknown_mimic_code_sets_value_text_to_na(monkeypatch) -> None:
    etl = MIMICDiagnosis(cfg={"raw_paths": {}}, base_cfg={})
    hosp = pd.DataFrame(
        {
            "subject_id": ["100"],
            "hadm_id": ["200"],
            "seq_num": [1],
            "icd_code": ["ZZZ"],
            "icd_version": ["10"],
            "source_table": ["hosp.diagnoses_icd"],
            "time_col": ["2020-01-01 00:00:00"],
        }
    )

    monkeypatch.setattr(etl, "_load_hospital_diagnoses", lambda: hosp)
    monkeypatch.setattr(etl, "_load_ed_diagnoses", lambda: pd.DataFrame())
    monkeypatch.setattr(etl, "_load_diagnosis_mapper", lambda: FakeMapper({}))

    out = etl.run_core()

    assert pd.isna(out["value_text"].iloc[0])
