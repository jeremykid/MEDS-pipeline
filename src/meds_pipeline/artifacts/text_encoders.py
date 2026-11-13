"""Pluggable offline text encoders for MEDS code embeddings.

An *encoder backend* (``encoder_kind``) is the implementation used to turn
``embed_text`` strings into vectors. The *model* (``model_name``) is the actual
embedding model loaded by that backend. For example, the default backend
``sentence_transformers`` currently loads the model ``FremyCompany/BioLORD-2023``;
BioLORD is one model among many, not the backend itself.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Protocol, Sequence

import numpy as np

DEFAULT_ENCODER_KIND = "sentence_transformers"
DEFAULT_BIOLORD_MODEL = "FremyCompany/BioLORD-2023"
DEFAULT_EMBED_DIM = 768


@dataclass(frozen=True)
class EncoderSpec:
    """Reproducibility metadata describing how an encoder produced vectors."""

    encoder_kind: str  # backend: "sentence_transformers", "mock"
    model_name: str  # actual HF / ST model id
    revision: str | None  # nullable; production should pin when available
    embed_dim: int
    normalize_embeddings: bool
    pooling: str | None = None  # None for ST; "mean"/"cls" for future BERT backends


class EmbeddingEncoder(Protocol):
    spec: EncoderSpec

    def encode(
        self,
        texts: Sequence[str],
        *,
        batch_size: int = 64,
        show_progress_bar: bool = True,
    ) -> np.ndarray: ...


class SentenceTransformerEncoder:
    """sentence-transformers backend (default model: BioLORD-2023)."""

    def __init__(
        self,
        model_name: str = DEFAULT_BIOLORD_MODEL,
        *,
        device: str | None = None,
        normalize_embeddings: bool = True,
    ) -> None:
        try:
            from sentence_transformers import SentenceTransformer
        except ImportError as exc:
            raise ImportError(
                "sentence-transformers is required for the sentence_transformers "
                "encoder. Install with: pip install -r requirements-biolord.txt"
            ) from exc

        self.model = SentenceTransformer(model_name, device=device)
        self.model.eval()
        dim_fn = getattr(self.model, "get_embedding_dimension", None)
        if callable(dim_fn):
            embed_dim = int(dim_fn())
        else:
            embed_dim = int(self.model.get_sentence_embedding_dimension())

        self.spec = EncoderSpec(
            encoder_kind="sentence_transformers",
            model_name=model_name,
            revision=resolve_model_revision(model_name),
            embed_dim=embed_dim,
            normalize_embeddings=bool(normalize_embeddings),
            pooling=None,
        )

    def encode(
        self,
        texts: Sequence[str],
        *,
        batch_size: int = 64,
        show_progress_bar: bool = True,
    ) -> np.ndarray:
        if not texts:
            return np.zeros((0, self.spec.embed_dim), dtype=np.float32)
        vectors = self.model.encode(
            list(texts),
            batch_size=batch_size,
            show_progress_bar=show_progress_bar,
            convert_to_numpy=True,
            normalize_embeddings=self.spec.normalize_embeddings,
        )
        return np.asarray(vectors, dtype=np.float32)


class MockEncoder:
    """Deterministic stand-in for unit tests (no HF / GPU dependency)."""

    def __init__(self, embed_dim: int = 8, model_name: str = "mock-encoder") -> None:
        self.spec = EncoderSpec(
            encoder_kind="mock",
            model_name=model_name,
            revision="mock",
            embed_dim=int(embed_dim),
            normalize_embeddings=True,
            pooling=None,
        )

    def encode(
        self,
        texts: Sequence[str],
        *,
        batch_size: int = 64,
        show_progress_bar: bool = False,
    ) -> np.ndarray:
        del batch_size, show_progress_bar
        rows = []
        for text in texts:
            seed = int.from_bytes(
                hashlib.sha256(text.encode("utf-8")).digest()[:8],
                byteorder="big",
                signed=False,
            )
            rng = np.random.default_rng(seed)
            vec = rng.standard_normal(self.spec.embed_dim).astype(np.float32)
            norm = float(np.linalg.norm(vec))
            if norm > 0:
                vec = vec / norm
            rows.append(vec)
        if not rows:
            return np.zeros((0, self.spec.embed_dim), dtype=np.float32)
        return np.stack(rows, axis=0)


def encode_vocab_embeddings(
    embed_texts: Sequence[str],
    encoder: EmbeddingEncoder,
    *,
    pad_id: int = 0,
    batch_size: int = 64,
    show_progress_bar: bool = True,
) -> np.ndarray:
    """Encode vocab embed_texts; force PAD row to zeros."""
    matrix = encoder.encode(
        embed_texts,
        batch_size=batch_size,
        show_progress_bar=show_progress_bar,
    )
    if matrix.shape[0] != len(embed_texts):
        raise ValueError(
            f"encoder returned {matrix.shape[0]} rows for {len(embed_texts)} texts"
        )
    matrix = np.asarray(matrix, dtype=np.float32).copy()
    if 0 <= pad_id < matrix.shape[0]:
        matrix[pad_id] = 0.0
    return matrix


def resolve_model_revision(model_name: str) -> str | None:
    """Best-effort Hugging Face revision / commit id; None if unavailable."""
    try:
        from huggingface_hub import model_info

        info = model_info(model_name)
        sha = getattr(info, "sha", None)
        if sha:
            return str(sha)
    except Exception:
        pass
    return None
