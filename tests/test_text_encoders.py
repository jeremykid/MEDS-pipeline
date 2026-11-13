"""Encoder contracts and backend substitution without downloads or GPUs."""

import sys
from dataclasses import replace
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from build_code_embeddings import main
from meds_pipeline.text_embeddings import register_encoder
from meds_pipeline.text_embeddings import registry
from meds_pipeline.text_embeddings.code_embedding_artifact import (
    encode_vocab_embeddings,
    load_artifact,
)
from meds_pipeline.text_embeddings.encoders import MockEncoder


@pytest.mark.parametrize("normalize", [True, False])
def test_sentence_transformer_loader_and_encode_options(monkeypatch, normalize):
    calls = {}

    class StubSentenceTransformer:
        def __init__(self, model, *, device, revision):
            calls["load"] = (model, device, revision)

        def eval(self):
            calls["eval"] = True

        def get_sentence_embedding_dimension(self):
            return 5

        def __getitem__(self, index):
            return SimpleNamespace(
                auto_model=SimpleNamespace(
                    config=SimpleNamespace(_commit_hash="loaded-commit")
                )
            )

        def encode(self, texts, **kwargs):
            calls["encode"] = kwargs
            return np.array([[len(text)] * 5 for text in texts], dtype=np.float64)

    monkeypatch.setitem(
        sys.modules,
        "sentence_transformers",
        SimpleNamespace(SentenceTransformer=StubSentenceTransformer),
    )
    encoder = registry.create_encoder(
        "sentence_transformers",
        model="alternate-model",
        device="cpu",
        revision="requested-tag",
        normalize=normalize,
    )
    vectors = encoder.encode(["one", "longer"], batch_size=2, show_progress_bar=False)
    assert calls["load"] == ("alternate-model", "cpu", "requested-tag")
    assert calls["eval"] is True
    assert calls["encode"]["normalize_embeddings"] is normalize
    assert calls["encode"]["batch_size"] == 2
    assert encoder.spec.revision == "loaded-commit"
    assert encoder.spec.normalize_embeddings is normalize
    assert encoder.spec.embed_dim == 5
    assert vectors.dtype == np.float32
    np.testing.assert_array_equal(vectors[:, 0], [3, 6])
    assert encoder.encode([]).shape == (0, 5)


def test_mock_normalization_and_unsupported_options():
    normalized = registry.create_encoder("mock", normalize=True).encode(["text"])
    raw = registry.create_encoder("mock", normalize=False).encode(["text"])
    np.testing.assert_allclose(np.linalg.norm(normalized, axis=1), 1, rtol=1e-6)
    assert not np.allclose(raw, normalized)
    with pytest.raises(TypeError, match="unexpected keyword"):
        registry.create_encoder("mock", unsupported_option=42)
    with pytest.raises(TypeError, match="unexpected keyword"):
        registry.create_encoder("sentence_transformers", pooling="unsupported")
    with pytest.raises(ValueError, match="revision"):
        registry.create_encoder("mock", revision="unavailable")


def test_custom_encoder_cli_roundtrip(tmp_path, monkeypatch):
    monkeypatch.setattr(registry, "ENCODER_REGISTRY", registry.ENCODER_REGISTRY.copy())
    calls = {}

    def factory(*, model, device, revision, normalize):
        calls.update(model=model, device=device, revision=revision, normalize=normalize)
        encoder = MockEncoder(
            embed_dim=7, model_name=model, normalize_embeddings=normalize
        )
        encoder.spec = replace(
            encoder.spec, encoder_kind="custom", revision=revision, pooling="mean"
        )
        return encoder

    register_encoder("custom", factory, default_model="custom-default")
    with pytest.raises(ValueError, match="already registered"):
        register_encoder("custom", factory, default_model="another")

    meds = tmp_path / "input.parquet"
    pd.DataFrame(
        [
            {
                "event_type": "diagnosis",
                "code": "DIAGNOSIS//ICD//10//I10",
                "value_text": "Hypertension",
            },
            {
                "event_type": "procedures",
                "code": "PROCEDURE//CCI//A",
                "value_text": "Procedure A",
            },
            {
                "event_type": "lab",
                "code": "LAB//MIMIC//50931",
                "value_text": "label=Glucose | raw=95",
            },
        ]
    ).to_parquet(meds, index=False)
    output = tmp_path / "output"
    assert (
        main(
            [
                "--source",
                "mimic",
                "--meds-data",
                str(meds),
                "--output-root",
                str(output),
                "--encoder",
                "custom",
                "--revision",
                "custom-commit",
                "--device",
                "cpu",
                "--no-normalize-embeddings",
                "--event-types",
                "diagnosis,procedures,lab",
                "--artifact-name",
                "custom_codes",
            ]
        )
        == 0
    )
    assert calls == dict(
        model="custom-default", device="cpu", revision="custom-commit", normalize=False
    )
    artifact = load_artifact(output / "code_embeddings/v1/custom_codes")
    assert artifact.embeddings.shape == (5, 7)
    assert artifact.embeddings.dtype == np.float32
    assert not artifact.embeddings[0].any()
    manifest = artifact.manifest
    assert manifest["encoder_kind"] == "custom"
    assert manifest["encoder_revision"] == "custom-commit"
    assert manifest["normalize_embeddings"] is False
    assert manifest["pooling"] == "mean"
    assert artifact.vocab.loc[
        artifact.vocab.event_type == "lab", "description"
    ].tolist() == ["Glucose"]


@pytest.mark.parametrize(
    "bad_output", [np.ones((2, 4)), np.ones(3), np.full((3, 4), np.nan)]
)
def test_invalid_encoder_outputs_rejected_before_publish(bad_output):
    encoder = MockEncoder(embed_dim=4)
    encoder.encode = lambda *args, **kwargs: bad_output
    with pytest.raises(ValueError, match="Encoder output"):
        encode_vocab_embeddings(["pad", "unknown", "code"], encoder)
