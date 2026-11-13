"""Text encoder implementations; model dependencies are loaded on demand."""

from __future__ import annotations

import hashlib
from typing import Sequence

import numpy as np

from .base import EncoderSpec

DEFAULT_ENCODER_KIND = "sentence_transformers"
DEFAULT_BIOLORD_MODEL = "FremyCompany/BioLORD-2023"


class SentenceTransformerEncoder:
    def __init__(
        self,
        model_name: str = DEFAULT_BIOLORD_MODEL,
        *,
        device: str | None = None,
        revision: str | None = None,
        normalize_embeddings: bool = True,
    ) -> None:
        try:
            from sentence_transformers import SentenceTransformer
        except ImportError as exc:
            raise ImportError(
                "Install text encoder dependencies with: pip install -r requirements.txt"
            ) from exc

        self.model = SentenceTransformer(model_name, device=device, revision=revision)
        self.model.eval()
        dim_fn = getattr(self.model, "get_embedding_dimension", None)
        if not callable(dim_fn):
            dim_fn = self.model.get_sentence_embedding_dimension
        embed_dim = int(dim_fn())
        # Read the loaded weights' revision, not the remote repository's current HEAD.
        first_module = self.model[0]
        config = getattr(getattr(first_module, "auto_model", None), "config", None)
        loaded_revision = getattr(config, "_commit_hash", None)
        self.spec = EncoderSpec(
            encoder_kind=DEFAULT_ENCODER_KIND,
            model_name=model_name,
            revision=loaded_revision or revision,
            embed_dim=embed_dim,
            normalize_embeddings=bool(normalize_embeddings),
            pooling=None,  # Pooling comes from the SentenceTransformer model config.
        )

    def encode(
        self,
        texts: Sequence[str],
        *,
        batch_size: int = 64,
        show_progress_bar: bool = True,
    ) -> np.ndarray:
        if len(texts) == 0:
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
    """Deterministic, model-free encoder for tests and CLI smoke checks."""

    def __init__(
        self,
        embed_dim: int = 8,
        model_name: str = "mock-encoder",
        *,
        normalize_embeddings: bool = True,
    ) -> None:
        if embed_dim <= 0:
            raise ValueError("embed_dim must be positive")
        self.spec = EncoderSpec(
            encoder_kind="mock",
            model_name=model_name,
            revision="mock",
            embed_dim=int(embed_dim),
            normalize_embeddings=normalize_embeddings,
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
                hashlib.sha256(text.encode("utf-8")).digest()[:8], "big"
            )
            vec = (
                np.random.default_rng(seed)
                .standard_normal(self.spec.embed_dim)
                .astype(np.float32)
            )
            norm = float(np.linalg.norm(vec))
            if self.spec.normalize_embeddings and norm > 0:
                vec = vec / norm
            rows.append(vec)
        if not rows:
            return np.zeros((0, self.spec.embed_dim), dtype=np.float32)
        return np.stack(rows)
