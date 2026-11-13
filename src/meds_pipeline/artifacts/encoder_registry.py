"""Registry mapping encoder backend keys to encoder implementations."""

from __future__ import annotations

from typing import Callable

from meds_pipeline.artifacts.text_encoders import (
    DEFAULT_BIOLORD_MODEL,
    EmbeddingEncoder,
    MockEncoder,
    SentenceTransformerEncoder,
)

# Default model per backend. ClinicalBERT / SapBERT are intentionally not
# registered yet; adding one is a new entry here plus an encoder class.
_DEFAULT_MODELS = {
    "sentence_transformers": DEFAULT_BIOLORD_MODEL,
    "mock": "mock-encoder",
}


def _make_sentence_transformers(
    *, model: str, device: str | None, normalize: bool, **kwargs
) -> EmbeddingEncoder:
    del kwargs
    return SentenceTransformerEncoder(
        model, device=device, normalize_embeddings=normalize
    )


def _make_mock(*, model: str, device: str | None, normalize: bool, **kwargs) -> EmbeddingEncoder:
    del device, normalize
    embed_dim = int(kwargs.get("embed_dim", 8))
    return MockEncoder(embed_dim=embed_dim, model_name=model)


ENCODER_REGISTRY: dict[str, Callable[..., EmbeddingEncoder]] = {
    "sentence_transformers": _make_sentence_transformers,
    "mock": _make_mock,
}


def available_encoders() -> list[str]:
    return sorted(ENCODER_REGISTRY)


def default_model_for(kind: str) -> str:
    if kind not in _DEFAULT_MODELS:
        raise ValueError(
            f"Unknown encoder kind {kind!r}. Available: {available_encoders()}"
        )
    return _DEFAULT_MODELS[kind]


def create_encoder(
    kind: str,
    *,
    model: str | None = None,
    device: str | None = None,
    normalize: bool = True,
    **kwargs,
) -> EmbeddingEncoder:
    """Instantiate an encoder backend by registry key."""
    if kind not in ENCODER_REGISTRY:
        raise ValueError(
            f"Unknown encoder kind {kind!r}. Available: {available_encoders()}"
        )
    resolved_model = model or default_model_for(kind)
    factory = ENCODER_REGISTRY[kind]
    return factory(model=resolved_model, device=device, normalize=normalize, **kwargs)
