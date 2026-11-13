"""MEDS-independent contract for replaceable text encoders."""

from dataclasses import dataclass
from typing import Protocol, Sequence

import numpy as np


@dataclass(frozen=True)
class EncoderSpec:
    encoder_kind: str
    model_name: str
    revision: str | None
    embed_dim: int
    normalize_embeddings: bool
    pooling: str | None = None


class TextEncoder(Protocol):
    spec: EncoderSpec

    def encode(
        self,
        texts: Sequence[str],
        *,
        batch_size: int = 64,
        show_progress_bar: bool = True,
    ) -> np.ndarray:
        """Return finite float32 [N, D] vectors in input order, including N=0."""
        ...
