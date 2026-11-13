"""Deprecated module. Use ``meds_pipeline.artifacts.text_encoders`` instead.

Kept as a backward-compatible re-export so existing imports of
``BioLORDEncoder`` continue to work. ``BioLORDEncoder`` is now an alias for
:class:`SentenceTransformerEncoder`.
"""

from __future__ import annotations

import warnings

from meds_pipeline.artifacts.text_encoders import (
    DEFAULT_BIOLORD_MODEL,
    DEFAULT_EMBED_DIM,
    EmbeddingEncoder,
    EncoderSpec,
    MockEncoder,
    SentenceTransformerEncoder,
    encode_vocab_embeddings,
    resolve_model_revision,
)

warnings.warn(
    "meds_pipeline.artifacts.biolord_encoder is deprecated; "
    "import from meds_pipeline.artifacts.text_encoders instead.",
    DeprecationWarning,
    stacklevel=2,
)

# Backward-compatible alias.
BioLORDEncoder = SentenceTransformerEncoder

__all__ = [
    "DEFAULT_BIOLORD_MODEL",
    "DEFAULT_EMBED_DIM",
    "BioLORDEncoder",
    "EmbeddingEncoder",
    "EncoderSpec",
    "MockEncoder",
    "SentenceTransformerEncoder",
    "encode_vocab_embeddings",
    "resolve_model_revision",
]
