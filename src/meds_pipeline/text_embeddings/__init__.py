"""Offline text embeddings for MEDS codes with replaceable encoder backends."""

from .base import EncoderSpec, TextEncoder
from .registry import available_encoders, create_encoder, register_encoder

__all__ = [
    "EncoderSpec",
    "TextEncoder",
    "available_encoders",
    "create_encoder",
    "register_encoder",
]
