# AHS Lab Results → MEDS

## Overview

Converts AHS laboratory test results to MEDS format with LOINC code mapping.

**Data**: 40M+ lab records across 2012-2021

---

## Data Source

| File | Period | Records |
|------|--------|---------|
| `rmt22884_lab_2012_2016_20211105.sas7bdat` | 2012-2016 | ~20M |
| `rmt22884_lab_2017_2021_20211105.sas7bdat` | 2017-2021 | ~20M |

**Location**: `/data/padmalab_external/special_project/AHS_Data_Release_2/`

---

## LOINC Mappings (Verified)

| LOINC | Test | AHS Codes | Records |
|-------|------|-----------|---------|
| 718-7 | Hemoglobin | HGB, HGB1 | 8.3M |
| 2160-0 | Creatinine | CREA, CCREA1, FCREA | 7.8M |
| 2823-3 | Potassium | K, IK, UAK, KR, WBK | 7.6M |
| 62238-1 | eGFR | GFR, GFRE1, GFR2, GFRF1, GFRF2, CREGF | 7.6M |
| 2951-2 | Sodium | NA | 7.5M |
| 10839-9 | Troponin I | TROP, TROPI, TROPI1, TROPI3, + numeric codes | 1.1M |
| 67151-1 | Troponin T (HS) | TROPTHS, TROPTN, TROPTSQ, TROPTSQN, + numeric codes | 21K |
| 30934-4 | BNP | BNP, BNP1 | 198K |
| 33762-6 | NT-proBNP | NTBNP, BNPNT, PBNP, + numeric codes | 9K |

**Note**: Mappings verified against `TEST_NM` column in actual data.

---

## Output Format

| Column | Example |
|--------|---------|
| subject_id | "2" |
| time | 2012-04-26 08:57:00 |
| code | `LAB//LOINC//718-7` |
| numeric_value | 110.0 |
| comparator | `<`, `>`, or None |
| unit | g/L |
| event_type | lab |
| code_system | LOINC |
| source_table | rmt22884_lab |

**Comparator**: Preserves clinical meaning (e.g., `<0.10` for troponin = below detection limit)

---

## Not Available in AHS Data

These tests from supervisor's list have **0 records** in the data:
- Troponin T, CK-MB, CK, Myoglobin, LDH, D-dimer, CRP
- Chloride, Calcium, Magnesium, Phosphate, Urea/BUN
- Hematocrit, WBC, Platelets
- Albumin, AST, ALT, Bilirubin, Total Protein
- Cholesterol, HDL, LDL, Triglycerides, Glucose

---

## Config

In `src/meds_pipeline/configs/ahs.yaml`:
```yaml
raw_paths:
  labs: "/data/padmalab_external/special_project/AHS_Data_Release_2/rmt22884_lab_2012_2016_20211105.sas7bdat"
```

---

## Key Design Decisions

1. **LOINC mappings hardcoded in Python** (not YAML config) - follows ECG pattern
2. **Comparator column** preserves `<` and `>` prefixes for clinical meaning
3. **Fallback**: Unmapped codes use `LAB//AHS//{code}` format
4. **100% mapping rate** on verified codes (9 LOINC codes cover all mapped tests)
