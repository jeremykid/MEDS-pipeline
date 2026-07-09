from __future__ import annotations

from pathlib import Path

import pandas as pd

from canada_code_mapper import CodeMapper
from meds_pipeline.etl.ahs.diagnosis import AHSDiagnosis
from meds_pipeline.etl.ahs.procedures import AHSProcedures


RESOURCE_DIR = Path(__file__).resolve().parents[1] / "src" / "resource"


def test_fixed_width_canadian_resources_parse_code_as_first_token() -> None:
    cci = CodeMapper.from_file(RESOURCE_DIR / "CCI_Code_Eng_Desc_CCI2026_V1_0.txt")

    description = cci.get_description("PROCEDURE//CCI//1GZ31CAND")

    assert "positive pressure" in description
    assert cci.code_exists("1GZ31CAND") is True

    icd = CodeMapper.from_file(RESOURCE_DIR / "ICD_Code_Eng_Desc_10CA2026_V1_0.txt")
    assert icd.get_description("F103") == (
        "Mental and behavioural disorders due to use of alcohol, withdrawal state"
    )


def test_ahs_diagnosis_value_text_is_only_description(monkeypatch) -> None:
    class FakeMapper:
        def get_description(self, code: str, default: str = "Unknown") -> str:
            return {"M1000": "Idiopathic gout, multiple sites"}.get(code, default)

    etl = AHSDiagnosis(cfg={}, base_cfg={})
    dad = pd.DataFrame(
        {
            "PATID": ["1"],
            "ADMITDATE_DT": ["2020-01-01"],
            "DXCODE1": ["M1000"],
        }
    )
    ed = pd.DataFrame(
        {
            "PATID": ["2"],
            "VISIT_DATE_DT": ["2020-02-01"],
            "DXCODE1": [""],
        }
    )

    monkeypatch.setattr(etl, "_load_dad_data", lambda: dad)
    monkeypatch.setattr(etl, "_load_ed_data", lambda: ed)
    monkeypatch.setattr(etl, "_load_diagnosis_mapper", lambda: FakeMapper())

    out = etl.run_core()

    assert out["code"].iloc[0] == "DIAGNOSIS//ICD10CA//M1000"
    assert out["value_text"].iloc[0] == "Idiopathic gout, multiple sites"


def test_ahs_procedure_value_text_is_only_description(monkeypatch) -> None:
    class FakeMapper:
        def get_description(self, code: str, default: str = "Unknown") -> str:
            return {"1GZ31CAND": "Ventilation with positive pressure"}.get(code, default)

    etl = AHSProcedures(cfg={}, base_cfg={})
    dad = pd.DataFrame(
        {
            "PATID": ["1"],
            "ADMITDATE_DT": ["2020-01-01"],
            "PROCCODE1": ["1GZ31CAND"],
            "PROCSTDT1_DT": ["2020-01-02"],
        }
    )

    monkeypatch.setattr(etl, "_load_dad_data", lambda: dad)
    monkeypatch.setattr(etl, "_load_procedure_mapper", lambda: FakeMapper())

    out = etl.run_core()

    assert out["code"].iloc[0] == "PROCEDURE//CCI//1GZ31CAND"
    assert out["value_text"].iloc[0] == "Ventilation with positive pressure"
