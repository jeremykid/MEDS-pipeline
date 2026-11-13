#!/usr/bin/env python3
"""Build offline code-embedding artifacts from MEDS-CORE parquet.

Default encoder backend: ``sentence_transformers``.
Default model: ``FremyCompany/BioLORD-2023``.
BioLORD is the default model, not the only one; swap it with ``--model`` or add
a new backend via ``meds_pipeline.text_embeddings.registry``.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np

# Allow running as ``python src/build_code_embeddings.py`` without installing.
_SRC = Path(__file__).resolve().parent
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from meds_pipeline.text_embeddings.code_embedding_artifact import (  # noqa: E402
    DEFAULT_ARTIFACT_NAME,
    artifact_directory,
    build_manifest,
    embed_text_hash,
    embedding_hash,
    encode_vocab_embeddings,
    git_commit_or_unknown,
    publish_artifact,
    validate_artifact,
)
from meds_pipeline.text_embeddings.code_vocab_extractor import (  # noqa: E402
    TARGET_EVENT_TYPES,
    extract_code_vocab,
)
from meds_pipeline.text_embeddings.embed_text import PAD_ID  # noqa: E402
from meds_pipeline.text_embeddings.registry import (  # noqa: E402
    available_encoders,
    create_encoder,
    default_model_for,
)
from meds_pipeline.text_embeddings.encoders import (  # noqa: E402
    DEFAULT_ENCODER_KIND,
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Extract MEDS clinical codes from parquet and cache "
            "text-encoder embeddings as a reproducible artifact."
        )
    )
    parser.add_argument(
        "--source",
        choices=("mimic", "ahs"),
        required=True,
        help="Dataset source label written into the artifact manifest",
    )
    parser.add_argument(
        "--meds-data",
        type=Path,
        required=True,
        help="Patient-bucketed MEDS directory (bucket*.parquet) or a single parquet file",
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        required=True,
        help="Root directory for ehr_encoder_artifacts (artifact written under code_embeddings/v1/)",
    )
    parser.add_argument(
        "--encoder",
        default=DEFAULT_ENCODER_KIND,
        help=(
            "Encoder backend key "
            f"(available: {', '.join(available_encoders())}; default: {DEFAULT_ENCODER_KIND})"
        ),
    )
    parser.add_argument(
        "--model",
        default=None,
        help=(
            "Embedding model id for the backend "
            "(default: backend-specific, e.g. FremyCompany/BioLORD-2023)"
        ),
    )
    parser.add_argument("--batch-size", type=int, default=64)
    parser.add_argument(
        "--revision",
        default=None,
        help="Model revision or commit passed to the encoder loader",
    )
    parser.add_argument(
        "--normalize-embeddings",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="L2-normalize output vectors (enabled by default)",
    )
    parser.add_argument(
        "--event-types",
        default=",".join(TARGET_EVENT_TYPES),
        help=(
            "Comma-separated MEDS event_type values to embed "
            f"(default: {','.join(TARGET_EVENT_TYPES)}; use lab for lab-only artifacts)"
        ),
    )
    parser.add_argument(
        "--device",
        default=None,
        help="Torch device for the encoder (e.g. cuda, cpu). Default: library auto-select",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Build and print vocab stats only; skip encoding and publish",
    )
    parser.add_argument(
        "--mock-encoder",
        action="store_true",
        help="DEPRECATED alias: overrides --encoder to use the deterministic mock encoder",
    )
    parser.add_argument(
        "--mock-embed-dim",
        type=int,
        default=8,
        help="Embedding dim when the mock encoder is used",
    )
    parser.add_argument(
        "--artifact-name",
        default=DEFAULT_ARTIFACT_NAME,
        help=(
            "Stable subdirectory under code_embeddings/v1/ "
            f"(default: {DEFAULT_ARTIFACT_NAME}). Use e.g. dx_proc_clinicalbert "
            "for other encoders. vocab_hash is stored in manifest.json."
        ),
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Replace an existing artifact directory (otherwise abort to avoid clobbering)",
    )
    return parser.parse_args(argv)


def parse_event_types(raw: str) -> tuple[str, ...]:
    event_types = tuple(part.strip() for part in str(raw).split(",") if part.strip())
    if not event_types:
        raise ValueError("--event-types must include at least one event_type")
    return event_types


def _optional_versions(encoder_kind: str) -> tuple[str | None, str | None]:
    st_version = None
    torch_version = None
    if encoder_kind == "sentence_transformers":
        try:
            import sentence_transformers

            st_version = getattr(sentence_transformers, "__version__", None)
        except Exception:
            st_version = None
    try:
        import torch

        torch_version = getattr(torch, "__version__", None)
    except Exception:
        torch_version = None
    return st_version, torch_version


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    meds_data = args.meds_data.expanduser().resolve()
    output_root = args.output_root.expanduser().resolve()

    # --mock-encoder is a backward-compatible alias that overrides --encoder.
    encoder_kind = "mock" if args.mock_encoder else args.encoder
    event_types = parse_event_types(args.event_types)

    if not args.dry_run:
        target = artifact_directory(output_root, args.artifact_name)
        if target.exists() and not args.overwrite:
            raise SystemExit(
                f"Artifact already exists at {target}. Pass --overwrite to replace "
                "it, or choose a different --artifact-name."
            )

    print(f"Scanning MEDS data: {meds_data}")
    result = extract_code_vocab(meds_data, event_types=event_types)
    print(
        f"Extracted n_codes={result.n_codes:,} vocab_size={result.vocab_size:,} "
        f"vocab_hash={result.vocab_hash[:12]}..."
    )
    print(f"event_types={','.join(event_types)}")
    print(f"meds_data_fingerprint={result.meds_data_fingerprint[:12]}...")

    if args.dry_run:
        missing = int((~result.vocab["has_description"]).sum() - 2)  # exclude PAD/UNK
        print(
            f"Dry run complete. Real codes missing description: " f"{max(missing, 0):,}"
        )
        return 0

    model = args.model or default_model_for(encoder_kind)
    kwargs = {}
    if encoder_kind == "mock":
        kwargs["embed_dim"] = args.mock_embed_dim
        print("Using mock encoder (not for production artifacts)")
    else:
        print(f"Loading encoder backend={encoder_kind} model={model}")
    encoder = create_encoder(
        encoder_kind,
        model=model,
        device=args.device,
        revision=args.revision,
        normalize=args.normalize_embeddings,
        **kwargs,
    )
    spec = encoder.spec
    print(
        f"Encoder ready: kind={spec.encoder_kind} embed_dim={spec.embed_dim} "
        f"revision={spec.revision}"
    )

    embed_texts = result.vocab["embed_text"].fillna("").astype(str).tolist()
    embeddings = encode_vocab_embeddings(
        embed_texts,
        encoder,
        pad_id=PAD_ID,
        batch_size=args.batch_size,
        show_progress_bar=True,
    )

    text_hash = embed_text_hash(embed_texts)
    emb_hash = embedding_hash(embeddings)
    st_version, torch_version = _optional_versions(spec.encoder_kind)

    repo_root = Path(__file__).resolve().parents[1]
    manifest = build_manifest(
        source=args.source,
        meds_data_path=meds_data,
        meds_data_fingerprint=result.meds_data_fingerprint,
        vocab_hash=result.vocab_hash,
        n_codes=result.n_codes,
        vocab_size=result.vocab_size,
        encoder_kind=spec.encoder_kind,
        encoder_model=spec.model_name,
        encoder_revision=spec.revision,
        embed_dim=int(spec.embed_dim),
        embed_text_hash=text_hash,
        embedding_hash=emb_hash,
        normalize_embeddings=bool(spec.normalize_embeddings),
        pooling=spec.pooling,
        event_types=event_types,
        creation_commit=git_commit_or_unknown(repo_root),
        sentence_transformers_version=st_version,
        torch_version=torch_version,
        device=args.device,
    )
    validate_artifact(result.vocab, embeddings, manifest)
    artifact_dir = publish_artifact(
        output_root,
        vocab=result.vocab,
        embeddings=embeddings,
        manifest=manifest,
        artifact_name=args.artifact_name,
        overwrite=args.overwrite,
    )
    print(f"Published artifact to {artifact_dir}")
    print(f"vocab_hash={manifest['vocab_hash']}")
    print(f"embed_text_hash={manifest['embed_text_hash']}")
    print(f"embedding_hash={manifest['embedding_hash']}")
    print(
        f"embeddings shape={embeddings.shape} dtype={embeddings.dtype} "
        f"pad_norm={float(np.linalg.norm(embeddings[0])):.1f}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
