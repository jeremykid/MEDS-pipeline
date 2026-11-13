# Code Embeddings

Offline preprocessing that caches text-encoder embeddings for MEDS
`diagnosis` and `procedures` codes by default, with opt-in `lab` support.
Downstream model repos load the artifact at
training time; **the encoder is never called during training**.

The pipeline is encoder-agnostic:

- **Encoder backend** (`encoder_kind`) is the implementation, e.g.
  `sentence_transformers`.
- **Model** (`encoder_model`) is the actual embedding model loaded by that
  backend, e.g. `FremyCompany/BioLORD-2023`.

BioLORD is the current default model, not the only option. See
[Adding another encoder](#adding-another-encoder-eg-clinicalbert) below.

Detailed design: [CODE_EMBEDDINGS_PLAN.md](CODE_EMBEDDINGS_PLAN.md).

## Install (encoding machine only)

Core MEDS ETL does **not** need these packages. On the conda `pytorch` env:

```bash
pip install -r requirements-biolord.txt
# If import errors mention huggingface-hub, run with:
#   PYTHONNOUSERSITE=1 pip install -r requirements-biolord.txt
```

## Build an artifact

```bash
cd MEDS-pipeline
conda activate pytorch

# MIMIC (default backend=sentence_transformers, model=BioLORD-2023)
PYTHONPATH=src python src/build_code_embeddings.py \
  --source mimic \
  --meds-data /data/padmalab_external/special_project/meds_pipeline_output/mimic/mimic_meds_core_by_patient \
  --output-root /data/padmalab_external/special_project/meds_pipeline_output/mimic/ehr_encoder_artifacts

# AHS
PYTHONPATH=src python src/build_code_embeddings.py \
  --source ahs \
  --meds-data /data/padmalab_external/special_project/meds_pipeline_output/ahs/ahs_meds_core_by_patient \
  --output-root /data/padmalab_external/special_project/meds_pipeline_output/ahs/ehr_encoder_artifacts
```

Useful flags:

| Flag | Meaning |
|------|---------|
| `--encoder` | Encoder backend key (default: `sentence_transformers`; also `mock`) |
| `--model` | Model id for the backend (default: `FremyCompany/BioLORD-2023`) |
| `--artifact-name` | Stable subdirectory name (default: `dx_proc`) |
| `--event-types` | Comma-separated event types; use `diagnosis,procedures,lab` for combined coverage |
| `--overwrite` | Replace an existing artifact directory (otherwise abort) |
| `--dry-run` | Extract vocab only; skip encoding and publish |
| `--batch-size 64` | Encoding batch size |
| `--device cuda` | Force device (default: library auto-select) |
| `--mock-encoder` | DEPRECATED alias for `--encoder mock` (overrides `--encoder`) |

## New MEDS data and shared BioLORD sidecars

To build a combined diagnosis/procedure/lab artifact from a new MIMIC MEDS
export, use a new output root or artifact name:

```bash
python src/build_code_embeddings.py \
  --source mimic \
  --meds-data /path/to/new_meds_buckets \
  --output-root /path/to/new_ehr_encoder_artifacts \
  --event-types diagnosis,procedures,lab \
  --artifact-name dx_proc_lab \
  --device cuda --batch-size 128
```

Use `--source ahs` for AHS. The current CLI accepts only those two source
labels. Input can be a single parquet file or a directory of `bucket*.parquet`
files. Add `--dry-run` to inspect vocabulary statistics without loading the
encoder or publishing files. Lab embedding text contains the test identity,
not the numeric result; numeric values remain separate downstream inputs.

The artifact contains one vector per unique code, not one vector per patient
event. It is separate from the patient-event cache and its vocabulary.
In the `cardio-context` repository, connect the two using:

```bash
python scripts/data_pipeline/build_shared_biolord_patient_cache_sidecar.py \
  --patient-event-cache-dir /path/to/new_patient_event_cache \
  --semantic-code-embedding-artifact-dir /path/to/new_ehr_encoder_artifacts/code_embeddings/v1/dx_proc_lab \
  --output-root /path/to/sidecar_artifacts
```

That builder maps patient-cache code IDs to semantic embedding rows and checks
coverage; it does not run BioLORD. Cross-dataset compatibility also depends on
matching model revision, text template, embedding dimension, and normalization.
This CLI records the model revision on a best-effort basis; it does not expose
a revision-pinning flag. Check the resulting manifest before reusing it with
an existing shared-BioLORD contract.

## Artifact layout

```
{output_root}/code_embeddings/v1/{artifact_name}/
├── manifest.json          # schema version, encoder_*, vocab_hash, embed_text_hash, embedding_hash
├── code_vocab.parquet
└── embeddings.npy         # float32 [vocab_size, embed_dim]
```

The publish directory name is fixed (`dx_proc` by default). The content
fingerprints live in `manifest.json`.

- `code_id=0` (`<PAD>`): zero vector
- `code_id=1` (`UNKNOWN`): encoder embedding of
  `unknown | unknown | UNKNOWN | [no description]`
- `code_id >= 2`: real MEDS codes, sorted lexicographically

### Manifest fields

| Field | Meaning |
|-------|---------|
| `artifact_schema_version` | `code_embeddings.v1` (or `legacy_biolord_v1` for pre-refactor artifacts) |
| `encoder_kind` | Backend implementation (`sentence_transformers`, `mock`) |
| `encoder_model` | Actual model id (e.g. `FremyCompany/BioLORD-2023`) |
| `encoder_revision` | HF commit sha; may be `null` if not resolvable (pin for production) |
| `embed_dim` | Embedding dimension |
| `normalize_embeddings` | Whether vectors are L2-normalized |
| `pooling` | `null` for sentence-transformers; `mean`/`cls` for future BERT backends |
| `vocab_hash` | SHA256 of the sorted MEDS code set |
| `embed_text_hash` | SHA256 of the joined `embed_text` inputs (in `code_id` order) |
| `embedding_hash` | SHA256 of the raw `embeddings.npy` bytes |
| `sentence_transformers_version`, `torch_version`, `device` | Audit only |

The three hashes answer three separate questions:

- `vocab_hash` — did the MEDS code set change?
- `embed_text_hash` — did the encoder input text change?
- `embedding_hash` — did the final embedding matrix change?

### Embed text template (v1)

```
{event_type} | {code_system} | {code} | {description_or_no_description}
```

Description comes from cleaned MEDS `value_text`. Missing descriptions use the
sentinel `[no description]`.

## Multi-encoder naming convention

Different encoders must use different `--artifact-name` values so they do not
clobber each other. Publishing to an existing directory requires `--overwrite`.

```
code_embeddings/v1/
├── dx_proc/                 # legacy / current default BioLORD artifact
├── dx_proc_biolord/         # explicit BioLORD (recommended for future publishes)
├── dx_proc_clinicalbert/    # future
└── dx_proc_sapbert/         # future
```

Recommended pattern for a new encoder: `{scope}_{model_short}`, e.g.
`dx_proc_clinicalbert`.

## Downstream loading (PyTorch)

```python
import json
from pathlib import Path

import numpy as np
import pandas as pd
import torch
import torch.nn as nn

artifact_dir = Path(
    "/data/padmalab_external/special_project/meds_pipeline_output/"
    "mimic/ehr_encoder_artifacts/code_embeddings/v1/dx_proc"
)

manifest = json.loads((artifact_dir / "manifest.json").read_text())
vocab = pd.read_parquet(artifact_dir / "code_vocab.parquet")
emb = np.load(artifact_dir / "embeddings.npy")  # float32 [V, embed_dim]

code_to_id = {
    str(row.code): int(row.code_id)
    for row in vocab.itertuples(index=False)
    if int(row.code_id) >= 2
}
unk_id = int(manifest["unk_id"])  # 1


def encode_code(code: str) -> int:
    return code_to_id.get(str(code), unk_id)


# Frozen pretrained code embedding table
embed_code = nn.Embedding.from_pretrained(
    torch.from_numpy(emb),
    freeze=True,
    padding_idx=int(manifest["pad_id"]),
)
```

Read `manifest["encoder_kind"]` / `encoder_model` / `embedding_hash` to select
and pin the right artifact. Validation helpers live in
`meds_pipeline.artifacts.code_embedding_artifact.load_artifact`.

## Adding another encoder (e.g. ClinicalBERT)

Not implemented in this repo yet. Three steps when needed:

1. Implement a `TransformersBertEncoder` in
   [text_encoders.py](../src/meds_pipeline/artifacts/text_encoders.py) that
   exposes `spec: EncoderSpec` and `encode(...) -> float32 [N, D]`, with mean/cls
   pooling and 512-token truncation. Encode the UNK row with the same pooling.
2. Register it in
   [encoder_registry.py](../src/meds_pipeline/artifacts/encoder_registry.py)
   (e.g. `"clinicalbert": ...`) and add its default model.
3. Publish with a distinct artifact name:

   ```bash
   PYTHONPATH=src python src/build_code_embeddings.py \
     --source mimic --encoder clinicalbert \
     --model emilyalsentzer/Bio_ClinicalBERT \
     --artifact-name dx_proc_clinicalbert \
     --meds-data ... --output-root ...
   ```

Different encoders can share the same `vocab_hash` while producing different
embedding matrices; downstream selects via `manifest.encoder_kind` /
`encoder_model` and pins `embedding_hash`.

## Generated artifacts (production)

| Source | n_codes | vocab_size | shape | missing descriptions |
|--------|---------|------------|-------|----------------------|
| MIMIC | 44,872 | 44,874 | `(44874, 768)` | 0 |
| AHS | 24,091 | 24,093 | `(24093, 768)` | 1,755 |

Paths (stable directory; check `manifest.json` for hashes):

```
/data/padmalab_external/special_project/meds_pipeline_output/mimic/ehr_encoder_artifacts/code_embeddings/v1/dx_proc/
/data/padmalab_external/special_project/meds_pipeline_output/ahs/ehr_encoder_artifacts/code_embeddings/v1/dx_proc/
```

| Source | vocab_hash (in manifest) |
|--------|--------------------------|
| MIMIC | `c02f33a8fec5ec6cf93e317888981c54a0e8653500b2c8dcfe13696e4efb7485` |
| AHS | `c6369f5aabb86736cfcd266a69acdff1a6da9635b097f5afa0e43551a3c23c8c` |

Backend `sentence_transformers`, model `FremyCompany/BioLORD-2023` revision
`167aab527b238a50ca65224e6319215d2ff4fc9f`. These artifacts predate the schema
change and load as `legacy_biolord_v1` (auto-normalized to `encoder_*` fields).

## Scope (v1)

- Included: `diagnosis`, `procedures`
- Separate artifacts for MIMIC and AHS
- Full vocabulary from processed MEDS (not train-split subset)
- Registered backends: `sentence_transformers`, `mock`
- Not included: ClinicalBERT/SapBERT implementations, labs, medicines,
  demographics, cardio-context training changes
