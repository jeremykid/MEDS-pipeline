"""Tests for pluggable code-embedding artifact generation (mock encoder only)."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from meds_pipeline.artifacts.text_encoders import (
    MockEncoder,
    SentenceTransformerEncoder,
    encode_vocab_embeddings,
)
from meds_pipeline.artifacts.code_embedding_artifact import (
    ARTIFACT_SCHEMA_VERSION,
    LEGACY_SCHEMA_VERSION,
    build_manifest,
    embed_text_hash,
    embedding_hash,
    load_artifact,
    normalize_manifest,
    publish_artifact,
    validate_artifact,
)
from meds_pipeline.artifacts.code_vocab_extractor import (
    clean_lab_value_text,
    extract_code_vocab,
    vocab_hash_from_codes,
)
from meds_pipeline.artifacts.embed_text import (
    MISSING_DESCRIPTION_SENTINEL,
    PAD_CODE,
    PAD_ID,
    UNK_CODE,
    UNK_EMBED_TEXT,
    UNK_ID,
    build_embed_text,
    parse_code_system,
    resolve_code_system,
)


def _build_manifest_for(result, encoder, embeddings, *, source="ahs"):
    """Helper to build a schema-v1 manifest from a vocab result + encoder."""
    embed_texts = result.vocab["embed_text"].fillna("").astype(str).tolist()
    return build_manifest(
        source=source,
        meds_data_path="/tmp/meds",
        meds_data_fingerprint=result.meds_data_fingerprint,
        vocab_hash=result.vocab_hash,
        n_codes=result.n_codes,
        vocab_size=result.vocab_size,
        encoder_kind=encoder.spec.encoder_kind,
        encoder_model=encoder.spec.model_name,
        encoder_revision=encoder.spec.revision,
        embed_dim=encoder.spec.embed_dim,
        embed_text_hash=embed_text_hash(embed_texts),
        embedding_hash=embedding_hash(embeddings),
        normalize_embeddings=encoder.spec.normalize_embeddings,
        pooling=encoder.spec.pooling,
    )


def test_parse_code_system_patterns():
    assert parse_code_system("DIAGNOSIS//ICD//10//F332") == "ICD//10"
    assert parse_code_system("DIAGNOSIS//ICD//9//7802") == "ICD//9"
    assert parse_code_system("DIAGNOSIS//ICD10CA//E1122") == "ICD10CA"
    assert parse_code_system("PROCEDURE//ICD//10//02RF08Z") == "ICD//10"
    assert parse_code_system("PROCEDURE//CCI//1GZ31CAND") == "CCI"
    assert parse_code_system("LAB//LOINC//718-7") == "LOINC"
    assert parse_code_system("plain") == "unknown"


def test_resolve_code_system_prefers_column():
    assert resolve_code_system("DIAGNOSIS//ICD//10//F332", "CUSTOM") == "CUSTOM"
    assert resolve_code_system("DIAGNOSIS//ICD//10//F332", None) == "ICD//10"
    assert resolve_code_system("DIAGNOSIS//ICD//10//F332", pd.NA) == "ICD//10"


def test_build_embed_text_with_and_without_description():
    with_desc = build_embed_text(
        event_type="diagnosis",
        code_system="ICD//10",
        code="DIAGNOSIS//ICD//10//F332",
        description="Major depressive disorder",
    )
    assert with_desc == (
        "diagnosis | ICD//10 | DIAGNOSIS//ICD//10//F332 | Major depressive disorder"
    )

    without = build_embed_text(
        event_type="diagnosis",
        code_system="ICD10CA",
        code="DIAGNOSIS//ICD10CA//E1122",
        description=None,
    )
    assert without.endswith(f"| {MISSING_DESCRIPTION_SENTINEL}")
    assert without == (
        f"diagnosis | ICD10CA | DIAGNOSIS//ICD10CA//E1122 | {MISSING_DESCRIPTION_SENTINEL}"
    )


def _write_bucket(path: Path, rows: list[dict]) -> None:
    pd.DataFrame(rows).to_parquet(path, index=False)


def test_extract_code_vocab_assigns_pad_unk_and_sorted_ids(tmp_path: Path):
    meds_dir = tmp_path / "meds"
    meds_dir.mkdir()
    _write_bucket(
        meds_dir / "bucket000.parquet",
        [
            {
                "event_type": "diagnosis",
                "code": "DIAGNOSIS//ICD//10//Z99",
                "code_system": None,
                "source_table": "hosp.diagnoses_icd",
                "value_text": "Dependence on enabling machines",
            },
            {
                "event_type": "lab",
                "code": "LAB//MIMIC//1",
                "code_system": "MIMIC_LAB_ITEMID",
                "source_table": "hosp.labevents",
                "value_text": "label=Ignore",
            },
            {
                "event_type": "procedures",
                "code": "PROCEDURE//ICD//10//02RF08Z",
                "code_system": None,
                "source_table": "hosp.procedures_icd",
                "value_text": "Aortic valve replacement",
            },
            {
                "event_type": "diagnosis",
                "code": "DIAGNOSIS//ICD//10//A01",
                "code_system": None,
                "source_table": "ed.diagnosis",
                "value_text": "Typhoid fever",
            },
        ],
    )
    _write_bucket(
        meds_dir / "bucket001.parquet",
        [
            {
                "event_type": "diagnosis",
                "code": "DIAGNOSIS//ICD//10//A01",
                "code_system": None,
                "source_table": "hosp.diagnoses_icd",
                "value_text": "typhoid fever",  # different casing -> lex min wins
            },
        ],
    )

    result = extract_code_vocab(meds_dir)
    vocab = result.vocab

    assert result.n_codes == 3
    assert result.vocab_size == 5
    assert list(vocab["code_id"]) == [0, 1, 2, 3, 4]
    assert vocab.iloc[0]["code"] == PAD_CODE
    assert vocab.iloc[1]["code"] == UNK_CODE
    assert vocab.iloc[1]["embed_text"] == UNK_EMBED_TEXT

    real_codes = vocab.loc[vocab["code_id"] >= 2, "code"].tolist()
    assert real_codes == sorted(real_codes)
    assert "LAB//MIMIC//1" not in real_codes

    a01 = vocab.loc[vocab["code"] == "DIAGNOSIS//ICD//10//A01"].iloc[0]
    # Lexicographic min of {"Typhoid fever", "typhoid fever"} is "Typhoid fever"
    assert a01["description"] == "Typhoid fever"
    assert int(a01["n_description_variants"]) == 2
    assert int(a01["n_events"]) == 2
    assert "ed.diagnosis" in str(a01["source_tables"])
    assert a01["code_system"] == "ICD//10"
    assert result.vocab_hash == vocab_hash_from_codes(real_codes)


def test_lab_vocab_uses_test_names_without_raw_results(tmp_path: Path):
    meds_dir = tmp_path / "meds"
    meds_dir.mkdir()
    _write_bucket(
        meds_dir / "bucket000.parquet",
        [
            {
                "event_type": "lab",
                "code": "LAB//MIMIC//50931",
                "code_system": "MIMIC_LAB_ITEMID",
                "source_table": "hosp.labevents",
                "value_text": "label=Glucose | raw=95",
            },
            {
                "event_type": "lab",
                "code": "LAB//MIMIC//50931",
                "code_system": "MIMIC_LAB_ITEMID",
                "source_table": "hosp.labevents",
                "value_text": "label=Glucose | raw=<0.10",
            },
            {
                "event_type": "lab",
                "code": "LAB//LOINC//718-7",
                "code_system": "LOINC",
                "source_table": "rmt22884_lab",
                "value_text": "test=Hemoglobin | raw=128",
            },
            {
                "event_type": "lab",
                "code": "LAB//MIMIC//99999",
                "code_system": "MIMIC_LAB_ITEMID",
                "source_table": "hosp.labevents",
                "value_text": "negative",
            },
            {
                "event_type": "diagnosis",
                "code": "DIAGNOSIS//ICD//10//A01",
                "code_system": None,
                "source_table": "ed.diagnosis",
                "value_text": "Typhoid fever",
            },
        ],
    )

    result = extract_code_vocab(meds_dir, event_types=("lab",))
    vocab = result.vocab

    assert result.n_codes == 3
    assert set(vocab.loc[vocab["code_id"] >= 2, "event_type"]) == {"lab"}

    glucose = vocab.loc[vocab["code"] == "LAB//MIMIC//50931"].iloc[0]
    assert glucose["description"] == "Glucose"
    assert glucose["embed_text"] == "lab | MIMIC_LAB_ITEMID | LAB//MIMIC//50931 | Glucose"
    assert int(glucose["n_description_variants"]) == 1

    hemoglobin = vocab.loc[vocab["code"] == "LAB//LOINC//718-7"].iloc[0]
    assert hemoglobin["description"] == "Hemoglobin"

    raw_only = vocab.loc[vocab["code"] == "LAB//MIMIC//99999"].iloc[0]
    assert pd.isna(raw_only["description"])
    assert "negative" not in raw_only["embed_text"]
    assert "raw=" not in " ".join(vocab["embed_text"].fillna("").astype(str).tolist())


def test_clean_lab_value_text_handles_old_and_new_formats():
    assert clean_lab_value_text("label=Glucose | raw=95") == "Glucose"
    assert clean_lab_value_text("test=Hemoglobin | raw=>60") == "Hemoglobin"
    assert clean_lab_value_text("Potassium") == "Potassium"
    assert clean_lab_value_text("Unknown") is None
    assert clean_lab_value_text("<0.10") is None
    assert clean_lab_value_text("negative") is None


def test_mock_encoder_is_deterministic_and_pad_zero():
    encoder = MockEncoder(embed_dim=8)
    texts = [UNK_EMBED_TEXT, "diagnosis | ICD//10 | X | foo", ""]
    a = encode_vocab_embeddings(["", *texts], encoder, pad_id=PAD_ID, show_progress_bar=False)
    b = encode_vocab_embeddings(["", *texts], encoder, pad_id=PAD_ID, show_progress_bar=False)
    assert a.shape == (4, 8)
    assert np.array_equal(a, b)
    assert np.allclose(a[PAD_ID], 0.0)
    assert np.isfinite(a).all()


def test_publish_load_validate_roundtrip(tmp_path: Path):
    meds_dir = tmp_path / "meds"
    meds_dir.mkdir()
    _write_bucket(
        meds_dir / "bucket000.parquet",
        [
            {
                "event_type": "diagnosis",
                "code": "DIAGNOSIS//ICD10CA//E1122",
                "code_system": None,
                "source_table": "dad",
                "value_text": None,
            },
            {
                "event_type": "procedures",
                "code": "PROCEDURE//CCI//1GZ31CAND",
                "code_system": "CCI",
                "source_table": "dad",
                "value_text": "Ventilation",
            },
        ],
    )

    result = extract_code_vocab(meds_dir)
    encoder = MockEncoder(embed_dim=16)
    embeddings = encode_vocab_embeddings(
        result.vocab["embed_text"].fillna("").astype(str).tolist(),
        encoder,
        pad_id=PAD_ID,
        show_progress_bar=False,
    )
    manifest = _build_manifest_for(result, encoder, embeddings)
    validate_artifact(result.vocab, embeddings, manifest)

    output_root = tmp_path / "artifacts"
    artifact_dir = publish_artifact(
        output_root,
        vocab=result.vocab,
        embeddings=embeddings,
        manifest=manifest,
    )
    assert artifact_dir.exists()
    assert (artifact_dir / "manifest.json").is_file()
    assert (artifact_dir / "code_vocab.parquet").is_file()
    assert (artifact_dir / "embeddings.npy").is_file()

    loaded = load_artifact(artifact_dir)
    assert loaded.manifest["vocab_hash"] == result.vocab_hash
    assert loaded.manifest["artifact_schema_version"] == ARTIFACT_SCHEMA_VERSION
    assert loaded.embeddings.shape == (result.vocab_size, 16)
    assert np.array_equal(loaded.embeddings, embeddings)

    # Deterministic re-publish
    embeddings2 = encode_vocab_embeddings(
        result.vocab["embed_text"].fillna("").astype(str).tolist(),
        encoder,
        pad_id=PAD_ID,
        show_progress_bar=False,
    )
    assert np.array_equal(embeddings, embeddings2)


def test_validate_rejects_non_zero_pad():
    vocab = pd.DataFrame(
        [
            {
                "code_id": PAD_ID,
                "code": PAD_CODE,
                "event_type": "pad",
                "code_system": "pad",
                "description": pd.NA,
                "embed_text": "",
                "has_description": False,
            },
            {
                "code_id": UNK_ID,
                "code": UNK_CODE,
                "event_type": "unknown",
                "code_system": "unknown",
                "description": pd.NA,
                "embed_text": UNK_EMBED_TEXT,
                "has_description": False,
            },
        ]
    )
    embeddings = np.ones((2, 4), dtype=np.float32)
    manifest = build_manifest(
        source="mimic",
        meds_data_path="/tmp",
        meds_data_fingerprint="abc",
        vocab_hash="def",
        n_codes=0,
        vocab_size=2,
        encoder_kind="mock",
        encoder_model="mock",
        encoder_revision="mock",
        embed_dim=4,
        embed_text_hash="",
        embedding_hash="",
    )
    with pytest.raises(ValueError, match="PAD embedding"):
        validate_artifact(vocab, embeddings, manifest)


def test_cli_dry_run_and_mock_publish(tmp_path: Path):
    from build_code_embeddings import main

    meds_dir = tmp_path / "meds"
    meds_dir.mkdir()
    _write_bucket(
        meds_dir / "bucket000.parquet",
        [
            {
                "event_type": "diagnosis",
                "code": "DIAGNOSIS//ICD//9//7802",
                "code_system": None,
                "source_table": "ed.diagnosis",
                "value_text": "SYNCOPE AND COLLAPSE",
            },
        ],
    )
    output_root = tmp_path / "out"
    assert (
        main(
            [
                "--source",
                "mimic",
                "--meds-data",
                str(meds_dir),
                "--output-root",
                str(output_root),
                "--dry-run",
            ]
        )
        == 0
    )
    assert not output_root.exists()

    assert (
        main(
            [
                "--source",
                "mimic",
                "--meds-data",
                str(meds_dir),
                "--output-root",
                str(output_root),
                "--mock-encoder",
                "--mock-embed-dim",
                "8",
            ]
        )
        == 0
    )
    published = output_root / "code_embeddings" / "v1" / "dx_proc"
    assert published.is_dir()
    loaded = load_artifact(published)
    assert loaded.manifest["source"] == "mimic"
    assert loaded.manifest["n_codes"] == 1
    assert loaded.manifest["encoder_kind"] == "mock"
    assert loaded.manifest["artifact_schema_version"] == ARTIFACT_SCHEMA_VERSION
    assert loaded.embeddings.shape == (3, 8)


def test_cli_can_publish_lab_only_artifact(tmp_path: Path):
    from build_code_embeddings import main

    meds_dir = tmp_path / "meds"
    meds_dir.mkdir()
    _write_bucket(
        meds_dir / "bucket000.parquet",
        [
            {
                "event_type": "lab",
                "code": "LAB//MIMIC//50931",
                "code_system": "MIMIC_LAB_ITEMID",
                "source_table": "hosp.labevents",
                "value_text": "label=Glucose | raw=95",
            },
            {
                "event_type": "diagnosis",
                "code": "DIAGNOSIS//ICD//9//7802",
                "code_system": None,
                "source_table": "ed.diagnosis",
                "value_text": "SYNCOPE AND COLLAPSE",
            },
        ],
    )
    output_root = tmp_path / "out"

    assert (
        main(
            [
                "--source",
                "mimic",
                "--meds-data",
                str(meds_dir),
                "--output-root",
                str(output_root),
                "--event-types",
                "lab",
                "--artifact-name",
                "lab",
                "--mock-encoder",
                "--mock-embed-dim",
                "8",
            ]
        )
        == 0
    )

    published = output_root / "code_embeddings" / "v1" / "lab"
    loaded = load_artifact(published)
    assert loaded.manifest["event_types"] == ["lab"]
    assert loaded.manifest["n_codes"] == 1
    assert loaded.vocab.loc[2, "code"] == "LAB//MIMIC//50931"
    assert loaded.vocab.loc[2, "description"] == "Glucose"
    assert loaded.embeddings.shape == (3, 8)


def _write_meds_single_dx(meds_dir: Path) -> None:
    meds_dir.mkdir()
    _write_bucket(
        meds_dir / "bucket000.parquet",
        [
            {
                "event_type": "diagnosis",
                "code": "DIAGNOSIS//ICD//9//7802",
                "code_system": None,
                "source_table": "ed.diagnosis",
                "value_text": "SYNCOPE AND COLLAPSE",
            },
        ],
    )


def test_cli_publish_requires_overwrite(tmp_path: Path):
    from build_code_embeddings import main

    meds_dir = tmp_path / "meds"
    _write_meds_single_dx(meds_dir)
    output_root = tmp_path / "out"
    args = [
        "--source", "mimic",
        "--meds-data", str(meds_dir),
        "--output-root", str(output_root),
        "--mock-encoder",
    ]
    assert main(args) == 0

    # Second publish to the same artifact-name must abort without --overwrite.
    with pytest.raises(SystemExit):
        main(args)

    # With --overwrite it succeeds.
    assert main([*args, "--overwrite"]) == 0


def test_mock_encoder_cli_resolution_equivalence(tmp_path: Path):
    from build_code_embeddings import main

    meds_dir = tmp_path / "meds"
    _write_meds_single_dx(meds_dir)

    out_flag = tmp_path / "flag"
    out_kind = tmp_path / "kind"
    assert main([
        "--source", "mimic", "--meds-data", str(meds_dir),
        "--output-root", str(out_flag), "--mock-encoder", "--mock-embed-dim", "8",
    ]) == 0
    assert main([
        "--source", "mimic", "--meds-data", str(meds_dir),
        "--output-root", str(out_kind), "--encoder", "mock", "--mock-embed-dim", "8",
    ]) == 0

    a = load_artifact(out_flag / "code_embeddings" / "v1" / "dx_proc")
    b = load_artifact(out_kind / "code_embeddings" / "v1" / "dx_proc")
    assert a.manifest["encoder_kind"] == "mock"
    assert b.manifest["encoder_kind"] == "mock"
    assert np.array_equal(a.embeddings, b.embeddings)


def test_embed_text_hash_and_embedding_hash_deterministic(tmp_path: Path):
    meds_dir = tmp_path / "meds"
    _write_meds_single_dx(meds_dir)
    result = extract_code_vocab(meds_dir)
    encoder = MockEncoder(embed_dim=8)
    texts = result.vocab["embed_text"].fillna("").astype(str).tolist()
    emb1 = encode_vocab_embeddings(texts, encoder, pad_id=PAD_ID, show_progress_bar=False)
    emb2 = encode_vocab_embeddings(texts, encoder, pad_id=PAD_ID, show_progress_bar=False)
    assert embed_text_hash(texts) == embed_text_hash(texts)
    assert embedding_hash(emb1) == embedding_hash(emb2)
    texts_changed = list(texts)
    texts_changed[-1] = texts_changed[-1] + " changed"
    assert embed_text_hash(texts_changed) != embed_text_hash(texts)


def test_build_manifest_schema_v1_fields(tmp_path: Path):
    meds_dir = tmp_path / "meds"
    _write_meds_single_dx(meds_dir)
    result = extract_code_vocab(meds_dir)
    encoder = MockEncoder(embed_dim=8)
    texts = result.vocab["embed_text"].fillna("").astype(str).tolist()
    embeddings = encode_vocab_embeddings(texts, encoder, pad_id=PAD_ID, show_progress_bar=False)
    manifest = _build_manifest_for(result, encoder, embeddings, source="mimic")

    assert manifest["artifact_schema_version"] == ARTIFACT_SCHEMA_VERSION
    assert manifest["encoder_kind"] == "mock"
    assert manifest["encoder_model"] == encoder.spec.model_name
    assert manifest["embed_text_hash"] == embed_text_hash(texts)
    assert manifest["embedding_hash"] == embedding_hash(embeddings)
    assert manifest["pooling"] is None


def test_load_legacy_biolord_manifest(tmp_path: Path):
    """A pre-refactor artifact (biolord_* keys, no schema) still loads."""
    meds_dir = tmp_path / "meds"
    _write_meds_single_dx(meds_dir)
    result = extract_code_vocab(meds_dir)
    encoder = MockEncoder(embed_dim=8)
    texts = result.vocab["embed_text"].fillna("").astype(str).tolist()
    embeddings = encode_vocab_embeddings(texts, encoder, pad_id=PAD_ID, show_progress_bar=False)

    # Construct a legacy-style manifest by hand (old build_manifest shape).
    legacy_manifest = {
        "artifact_version": 1,
        "artifact_kind": "code_embeddings",
        "source": "mimic",
        "event_types": ["diagnosis", "procedures"],
        "meds_data_path": str(meds_dir),
        "meds_data_fingerprint": result.meds_data_fingerprint,
        "vocab_hash": result.vocab_hash,
        "n_codes": result.n_codes,
        "vocab_size": result.vocab_size,
        "pad_id": PAD_ID,
        "unk_id": UNK_ID,
        "biolord_model": "FremyCompany/BioLORD-2023",
        "biolord_revision": "167aab52",
        "embed_dim": 8,
        "normalize_embeddings": True,
        "text_template_version": "v1",
        "text_template": "t",
        "missing_description_sentinel": MISSING_DESCRIPTION_SENTINEL,
        "unk_embed_text": UNK_EMBED_TEXT,
        "pad_embedding_policy": "zeros",
        "generated_at": "2026-01-01T00:00:00Z",
        "creation_commit": "abc",
    }

    normalized = normalize_manifest(legacy_manifest)
    assert normalized["artifact_schema_version"] == LEGACY_SCHEMA_VERSION
    assert normalized["encoder_kind"] == "sentence_transformers"
    assert normalized["encoder_model"] == "FremyCompany/BioLORD-2023"
    assert normalized["encoder_revision"] == "167aab52"

    # Legacy manifest (no embed_text_hash/embedding_hash) should still validate.
    validate_artifact(result.vocab, embeddings, legacy_manifest)


def test_biolord_encoder_alias_is_sentence_transformer():
    import meds_pipeline.artifacts.biolord_encoder as legacy_mod

    assert legacy_mod.BioLORDEncoder is SentenceTransformerEncoder
