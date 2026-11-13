"""Tests for the pluggable encoder registry."""

from __future__ import annotations

import pytest

from meds_pipeline.text_embeddings.registry import (
    available_encoders,
    create_encoder,
    default_model_for,
)
from meds_pipeline.text_embeddings.encoders import MockEncoder


def test_available_encoders_registers_only_expected_backends():
    kinds = available_encoders()
    assert "sentence_transformers" in kinds
    assert "mock" in kinds
    # ClinicalBERT is intentionally not registered in this PR.
    assert "clinicalbert" not in kinds


def test_default_model_for_known_and_unknown():
    assert default_model_for("sentence_transformers") == "FremyCompany/BioLORD-2023"
    with pytest.raises(ValueError, match="Unknown encoder kind"):
        default_model_for("nope")


def test_create_encoder_mock_backend():
    encoder = create_encoder("mock", model="mock-x", embed_dim=6)
    assert isinstance(encoder, MockEncoder)
    assert encoder.spec.encoder_kind == "mock"
    assert encoder.spec.model_name == "mock-x"
    assert encoder.spec.embed_dim == 6


def test_create_encoder_unknown_kind_raises():
    with pytest.raises(ValueError, match="Unknown encoder kind"):
        create_encoder("clinicalbert", model="emilyalsentzer/Bio_ClinicalBERT")


def test_create_encoder_uses_default_model_when_none():
    encoder = create_encoder("mock", model=None)
    assert encoder.spec.model_name == default_model_for("mock")
