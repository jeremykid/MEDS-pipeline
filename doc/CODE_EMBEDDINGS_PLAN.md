# BioLORD Code Embedding Artifact — Implementation Plan (v1)

Offline preprocessing step for MEDS-pipeline. Generates a cached **code → 768-d BioLORD embedding** artifact from processed MEDS-CORE data. Downstream model repos load the artifact at training time; **BioLORD is never called during training**.

## Decisions (locked for v1)

| Topic | Decision |
|-------|----------|
| Scope | **diagnosis + procedures only** (labs, medicines, demographics deferred) |
| Datasets | **Separate artifacts** for MIMIC and AHS |
| Vocabulary | **All unique codes** appearing in the processed MEDS output (full cache, not train-split subset) |
| BioLORD model | `FremyCompany/BioLORD-2023` via `sentence-transformers` (conda `pytorch` env; optional install extra) |
| Embed text template (v1) | `{event_type} \| {code_system} \| {code} \| {description_or_no_description}` |
| Description source | `value_text` already written by ETL into `*_meds_core_by_patient` parquet |
| PAD / UNK | `code_id=0` PAD (zero vector), `code_id=1` UNK (BioLORD on fixed UNK template; see below) |

### Input data paths (production)

```
/data/padmalab_external/special_project/meds_pipeline_output/mimic/mimic_meds_core_by_patient/
/data/padmalab_external/special_project/meds_pipeline_output/ahs/ahs_meds_core_by_patient/
```

Each directory contains `bucket000.parquet` … `bucket255.parquet`.

---

## 1. Goal

```
MEDS parquet (dx + proc)
    → extract unique `code` + metadata
    → build deterministic `embed_text` per code
    → BioLORD encode (offline, once)
    → artifact: manifest.json + code_vocab.parquet + embeddings.npy
```

Downstream: `code_id → embeddings[code_id]` lookup only.

---

## 2. Text template (v1)

### Format

```
{event_type} | {code_system} | {code} | {description_or_no_description}
```

**Example (MIMIC diagnosis with description):**

```
diagnosis | ICD//10 | DIAGNOSIS//ICD//10//F332 | Major depressive disorder, recurrent severe without psychotic features
```

**Example (AHS diagnosis, no description in data):**

```
diagnosis | ICD10CA | DIAGNOSIS//ICD10CA//E1122 | [no description]
```

**Example (procedure):**

```
procedures | ICD//10 | PROCEDURE//ICD//10//02RF08Z | Replacement of Aortic Valve with Zooplastic Tissue, Open Approach
```

### Field rules

| Field | Source | Rule |
|-------|--------|------|
| `event_type` | parquet `event_type` | Lowercase as stored (`diagnosis`, `procedures`) |
| `code_system` | parquet `code_system`, else parse from `code` | See §2.1 |
| `code` | parquet `code` | Full MEDS code string, unchanged |
| `description_or_no_description` | parquet `value_text` | Cleaned description, or sentinel `[no description]` |

Description cleaning reuses `clean_description()` from `meds_pipeline.etl.code_descriptions` (strip, collapse whitespace, drop unknown sentinels).

### 2.1 `code_system` resolution

Parquet often has `code_system=<NA>` for dx/proc because the system is embedded in `code`. Parse deterministically:

| Code pattern | `code_system` |
|--------------|---------------|
| `DIAGNOSIS//ICD//{ver}//{raw}` | `ICD//{ver}` |
| `DIAGNOSIS//ICD10CA//{raw}` | `ICD10CA` |
| `PROCEDURE//ICD//{ver}//{raw}` | `ICD//{ver}` |
| `PROCEDURE//CCI//{raw}` | `CCI` |
| Non-empty parquet `code_system` | Use column value |
| Otherwise | `unknown` |

### 2.2 Multiple descriptions per code

Same `code` can appear with different `value_text` (e.g. MIMIC ICD-9 title casing). **Deterministic pick:**

1. Collect all non-null cleaned descriptions per `code` while scanning.
2. If multiple: take **lexicographically smallest** description (stable across runs).
3. Record `n_description_variants` in metadata for audit.

### 2.3 UNK template (reserved row `code_id=1`)

```
unknown | unknown | UNKNOWN | [no description]
```

Encoded once with BioLORD offline. Used when downstream encounters a code not in the vocabulary table.

### 2.4 PAD row (`code_id=0`)

Zero vector `(embed_dim,)`. No BioLORD call.

---

## 3. Code vocabulary extraction

### 3.1 Scan

- Input: patient-bucketed directory (`bucket*.parquet`).
- Filter: `event_type in ("diagnosis", "procedures")`.
- Columns: `code`, `code_system`, `event_type`, `source_table`, `value_text`.
- Stream bucket files (no full-dataset load into memory).

### 3.2 Aggregate per `code`

| Column | Aggregation |
|--------|-------------|
| `code` | group key |
| `event_type` | mode; tie-break lexicographic min |
| `code_system` | resolved per §2.1 |
| `description` | per §2.2 |
| `source_tables` | sorted unique, `;`-joined |
| `n_events` | row count (metadata only) |
| `has_description` | bool |

### 3.3 Assign `code_id`

| `code_id` | Row |
|-----------|-----|
| 0 | PAD (synthetic, not in parquet) |
| 1 | UNK (synthetic) |
| 2 … N+1 | Real codes, sorted by `code` lexicographically |

`vocab_size = n_codes + 2`

`vocab_hash = sha256("\n".join(sorted(real_codes)))`

---

## 4. Artifact layout

```
{output_root}/code_embeddings/v1/dx_proc/
├── manifest.json
├── code_vocab.parquet
└── embeddings.npy
```

Default `output_root`:

```
/data/padmalab_external/special_project/meds_pipeline_output/{source}/ehr_encoder_artifacts/
```

### 4.1 `code_vocab.parquet`

| Column | Type | Notes |
|--------|------|-------|
| `code_id` | int64 | Row index into `embeddings.npy` |
| `code` | string | MEDS code; PAD/UNK use sentinel strings |
| `event_type` | string | |
| `code_system` | string | |
| `description` | string | nullable; cleaned text |
| `embed_text` | string | Exact BioLORD input |
| `has_description` | bool | |
| `source_tables` | string | optional aggregate |
| `n_events` | int64 | optional |
| `n_description_variants` | int64 | optional |

Rows ordered by `code_id` ascending.

### 4.2 `embeddings.npy`

- dtype: `float32`
- shape: `(vocab_size, embed_dim)` where `embed_dim=768` for BioLORD-2023
- `embeddings[i]` corresponds to `code_id == i`

### 4.3 `manifest.json`

Note: the manifest schema was later generalized for pluggable encoders. Current
artifacts use `artifact_schema_version: "code_embeddings.v1"` with
`encoder_kind` / `encoder_model` / `encoder_revision` (plus `embed_text_hash`
and `embedding_hash`) instead of the `biolord_*` fields shown below. Legacy
artifacts load as `legacy_biolord_v1`. See
[CODE_EMBEDDINGS.md](CODE_EMBEDDINGS.md#manifest-fields) for the authoritative
field list.

```json
{
  "artifact_version": 1,
  "artifact_kind": "code_embeddings",
  "source": "mimic",
  "event_types": ["diagnosis", "procedures"],
  "meds_data_path": "/data/.../mimic_meds_core_by_patient",
  "meds_data_fingerprint": "<sha256 of bucket file paths + sizes + mtimes>",
  "vocab_hash": "<sha256>",
  "n_codes": 25000,
  "vocab_size": 25002,
  "pad_id": 0,
  "unk_id": 1,
  "biolord_model": "FremyCompany/BioLORD-2023",
  "biolord_revision": "<hf revision id>",
  "embed_dim": 768,
  "normalize_embeddings": true,
  "text_template_version": "v1",
  "text_template": "{event_type} | {code_system} | {code} | {description_or_no_description}",
  "missing_description_sentinel": "[no description]",
  "unk_embed_text": "unknown | unknown | UNKNOWN | [no description]",
  "pad_embedding_policy": "zeros",
  "generated_at": "2026-07-09T19:00:00Z",
  "creation_commit": "<git sha>"
}
```

---

## 5. Repository layout (new code)

```
MEDS-pipeline/
├── doc/
│   └── CODE_EMBEDDINGS_PLAN.md          # this file
├── src/
│   ├── build_code_embeddings.py         # CLI entry (like split_patients.py)
│   └── meds_pipeline/
│       └── artifacts/
│           ├── __init__.py
│           ├── code_vocab_extractor.py  # scan parquet → vocab table
│           ├── embed_text.py            # template v1 + code_system parse
│           ├── biolord_encoder.py       # sentence-transformers wrapper
│           └── code_embedding_artifact.py  # save / load / validate / publish
└── tests/
    └── test_code_embedding_artifact.py  # mock encoder; no GPU / HF download
```

**Not in scope for v1:** changes to `cardio-context` training code or `EventSequenceEncoder`.

---

## 6. CLI

```bash
# MIMIC
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

Optional flags: `--model`, `--batch-size`, `--dry-run` (build vocab only, skip BioLORD), `--device cuda|cpu`.

---

## 7. Validation checks

Run automatically after generation:

1. `embeddings.shape[0] == vocab_size == len(code_vocab)`
2. `code_id` is contiguous `0 … vocab_size-1`
3. No duplicate `code` among real rows (`code_id >= 2`)
4. `embeddings.shape[1] == manifest.embed_dim` constant
5. `embeddings[0]` is all zeros (PAD)
6. No NaN / Inf in embedding matrix
7. `code_vocab.embed_text.iloc[i]` matches manifest rules for every real code
8. Row order: `code_vocab.code_id` equals row index in `embeddings.npy`
9. **Determinism:** same `meds_data_fingerprint` + same model revision → identical `vocab_hash` and `embeddings` bytes (tested with mock encoder in CI)

---

## 8. Dependencies

**Core pipeline** (`requirements.txt`): unchanged.

**Optional extra** (document in README / `pyproject` or `requirements-biolord.txt`):

```
sentence-transformers>=2.2.0
torch  # already in conda pytorch env
```

Install only on machines that run `build_code_embeddings.py`.

---

## 9. Downstream consumption (documentation only)

```python
import json
import numpy as np
import pandas as pd
from pathlib import Path

artifact_dir = Path(".../code_embeddings/v1/{vocab_hash}")
manifest = json.loads((artifact_dir / "manifest.json").read_text())
vocab = pd.read_parquet(artifact_dir / "code_vocab.parquet")
emb = np.load(artifact_dir / "embeddings.npy")  # float32 [vocab_size, 768]

code_to_id = vocab.set_index("code")["code_id"].to_dict()

def lookup(code: str) -> np.ndarray:
    code_id = code_to_id.get(code, manifest["unk_id"])
    return emb[code_id]
```

Future `cardio-context` integration (out of scope v1):

- Load artifact path from config or `${meds_root.parent}/ehr_encoder_artifacts/{dataset}/...`
- Replace or augment `nn.Embedding` for codes with `nn.Embedding.from_pretrained(torch.from_numpy(emb), freeze=True)`
- Keep existing `HashedCodeVocab` / hash path as fallback behind a feature flag

---

## 10. Implementation order

| Step | Task | Est. |
|------|------|------|
| 1 | `embed_text.py` — template + `code_system` parse + tests | small |
| 2 | `code_vocab_extractor.py` — bucket scan + aggregation | medium |
| 3 | `code_embedding_artifact.py` — save/load/validate | medium |
| 4 | `biolord_encoder.py` — batch encode wrapper | small |
| 5 | `build_code_embeddings.py` — CLI wiring | small |
| 6 | `test_code_embedding_artifact.py` — mock encoder determinism | small |
| 7 | Run on MIMIC + AHS; record `vocab_hash` and row counts in PR | ops |
| 8 | `doc/CODE_EMBEDDINGS.md` — usage + downstream load guide | small |

---

## 11. Out of scope (v1)

- Labs, medicines, demographics event types
- Merged cross-dataset vocabulary
- Training-time BioLORD inference
- Replacing hash / frequency code vocab in `cardio-context`
- Ablation studies (hash vs BioLORD vs frequency)
- Re-running ICD codebook mappers (descriptions already in MEDS `value_text`)

---

## 12. Open items (minor; safe defaults chosen)

| Item | v1 default |
|------|------------|
| UNK embedding | BioLORD on `unknown \| unknown \| UNKNOWN \| [no description]` |
| BioLORD `normalize_embeddings` | `True` (L2 norm) |
| Multiple descriptions | Lexicographic min |
| `event_type` for procedures | Use parquet value (`procedures`); do not singularize |

---

## 13. Success criteria

- [ ] MIMIC artifact: all dx/proc codes in `mimic_meds_core_by_patient` have exactly one embedding row
- [ ] AHS artifact: same for AHS data
- [ ] Validation passes on both artifacts
- [ ] Re-run produces bit-identical `embeddings.npy` given unchanged input and model revision
- [ ] No BioLORD import required to run core MEDS ETL or unit tests
