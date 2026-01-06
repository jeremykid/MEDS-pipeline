
# MIMIC Code Mapper

A code-to-description mapping system for MIMIC-IV medical coding standards.

## Data Sources

MIMIC ICD dx code:
`/data/padmalab_external/special_project/physionet.org/files/mimiciv/3.1/hosp/d_icd_diagnoses.csv.gz`

Procedure code:
`/data/padmalab_external/special_project/physionet.org/files/mimiciv/3.1/hosp/d_icd_procedures.csv.gz`

## Composite Code Format

The mapper supports composite code format: `PREFIX//ICD//VERSION//CODE`

### Example ICD Diagnosis Codes
- `DIAGNOSIS//ICD//9//5723` (ICD-9)
- `DIAGNOSIS//ICD//10//R531` (ICD-10)

### Example Procedure Codes
- `PROCEDURE//ICD//9//8938` (ICD-9)
- `PROCEDURE//ICD//10//0QS734Z` (ICD-10)

## Usage

```python
from mimic_codde_mapper.registry import init_mimic_mappers

# Initialize all mappers (ICD-9/10 for both diagnosis and procedure)
registry = init_mimic_mappers(
    diagnosis_path="/data/padmalab_external/special_project/physionet.org/files/mimiciv/3.1/hosp/d_icd_diagnoses.csv.gz",
    procedure_path="/data/padmalab_external/special_project/physionet.org/files/mimiciv/3.1/hosp/d_icd_procedures.csv.gz"
)

# Auto-routing: composite codes are automatically routed to correct mapper
diagnosis_desc = registry.get_description("diagnosis_10", "DIAGNOSIS//ICD//10//R531")
# Returns: "Weakness"

procedure_desc = registry.get_description("procedure_10", "PROCEDURE//ICD//10//0QS734Z")
# Returns: "Reposition Left Upper Femur with Internal Fixation Device, Percutaneous Approach"

# Hierarchical fallback: if exact code not found, tries shorter versions
# e.g., R5319999 -> R531999 -> R53199 -> ... -> R531 (found!)
desc = registry.get_description("diagnosis_10", "DIAGNOSIS//ICD//10//R5319999")
# Returns: "Weakness" (matched R531)
```

## Registered Mappers

After initialization, the following mappers are available:
- `diagnosis_9`: ICD-9 diagnosis codes
- `diagnosis_10`: ICD-10 diagnosis codes
- `procedure_9`: ICD-9 procedure codes
- `procedure_10`: ICD-10 procedure codes

## Running Tests

```bash
conda activate pytorch
python tests/test_mimic_codde_mapper.py
```
