"""Register encoder factories independently of MEDS vocabulary processing."""

from typing import Callable

from .base import TextEncoder
from .encoders import DEFAULT_BIOLORD_MODEL, MockEncoder, SentenceTransformerEncoder

ENCODER_REGISTRY: dict[str, tuple[Callable[..., TextEncoder], str]] = {}


def register_encoder(
    kind: str, factory: Callable[..., TextEncoder], *, default_model: str
) -> None:
    """Factories accept model, device, revision, normalize and explicit backend options."""
    if not kind or not default_model:
        raise ValueError("Encoder kind and default model must be non-empty")
    if kind in ENCODER_REGISTRY:
        raise ValueError(f"Encoder kind already registered: {kind!r}")
    ENCODER_REGISTRY[kind] = (factory, default_model)


def available_encoders() -> list[str]:
    return sorted(ENCODER_REGISTRY)


def default_model_for(kind: str) -> str:
    if kind not in ENCODER_REGISTRY:
        raise ValueError(
            f"Unknown encoder kind {kind!r}. Available: {available_encoders()}"
        )
    return ENCODER_REGISTRY[kind][1]


def create_encoder(
    kind: str,
    *,
    model: str | None = None,
    device: str | None = None,
    revision: str | None = None,
    normalize: bool = True,
    **options,
) -> TextEncoder:
    default_model = default_model_for(kind)
    factory = ENCODER_REGISTRY[kind][0]
    return factory(
        model=model or default_model,
        device=device,
        revision=revision,
        normalize=normalize,
        **options,
    )


def _make_sentence_transformers(*, model, device, revision, normalize) -> TextEncoder:
    return SentenceTransformerEncoder(
        model, device=device, revision=revision, normalize_embeddings=normalize
    )


def _make_mock(*, model, device, revision, normalize, embed_dim=8) -> TextEncoder:
    if revision is not None:
        raise ValueError("The mock encoder does not support revision selection")
    return MockEncoder(
        embed_dim=embed_dim, model_name=model, normalize_embeddings=normalize
    )


register_encoder(
    "sentence_transformers",
    _make_sentence_transformers,
    default_model=DEFAULT_BIOLORD_MODEL,
)
register_encoder("mock", _make_mock, default_model="mock-encoder")
