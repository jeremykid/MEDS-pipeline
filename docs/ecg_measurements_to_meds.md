# AHS ECG Measurements → MEDS Integration Plan

## 1. What is MEDS? (Simple Explanation)

MEDS (Medical Event Data Standard) is a **simple, flat table format** for health data. Instead of having complex tables with many columns, MEDS stores everything as **events** (one row per thing that happened).

Think of it like a timeline of everything that happened to a patient:
- "Patient 123 had heart rate of 72 at 10:30am"
- "Patient 123 had QRS duration of 90ms at 10:30am"
- "Patient 123 got diagnosed with diabetes at 2:00pm"

All stored in the same simple format!

---

## 2. What Will The Output Look Like in this particular case?

### BEFORE: Raw AHS Data (Wide Format - Many Columns)

**ECG Records Table** (`rmt22884_ecg_20211105_df.pickle`):
```
| PATID  | ecgId                                      | dateAcquired        |
|--------|-------------------------------------------|---------------------|
| 219126 | N'407be100-3b36-11dc-4823-000206d60029'   | 2007-07-25 23:02:25 |
| 219126 | N'b44d0080-71ed-11dc-4823-00102de50029'   | 2007-10-03 14:11:31 |
| 54321  | N'ee6b9780-6f8c-11dc-4823-003316050029'   | 2007-09-30 13:33:47 |
```

**Measurements Table** (`Globalmeasurements.pickle`):
```
| ecgId                                  | heartrate | qrsdur | qtint | qtcb | qrsfrontaxis | tfrontaxis |
|----------------------------------------|-----------|--------|-------|------|--------------|------------|
| 407be100-3b36-11dc-4823-000206d60029   | 63        | 74     | 392   | 402  | 45           | 32         |
| b44d0080-71ed-11dc-4823-00102de50029   | 88        | 92     | 368   | 446  | -12          | 55         |
| ee6b9780-6f8c-11dc-4823-003316050029   | 72        | 86     | 410   | 428  | 60           | 48         |
```

### AFTER: MEDS Output (Long Format - One Event Per Row)

From **ONE ECG** (patient 219126 at 2007-07-25 23:02:25), we produce **multiple rows**:

```
| subject_id | time                | code           | numeric_value | unit      |
|------------|---------------------|----------------|---------------|-----------|
| 219126     | 2007-07-25 23:02:25 | ECG//HR        | 63.0          | beats/min |
| 219126     | 2007-07-25 23:02:25 | ECG//QRS       | 74.0          | ms        |
| 219126     | 2007-07-25 23:02:25 | ECG//QT        | 392.0         | ms        |
| 219126     | 2007-07-25 23:02:25 | ECG//QTC       | 402.0         | ms        |
| 219126     | 2007-07-25 23:02:25 | ECG//QRS_AXIS  | 45.0          | °         |
| 219126     | 2007-07-25 23:02:25 | ECG//T_AXIS    | 32.0          | °         |
| ...        | ...                 | ...            | ...           | ...       |
```

---

## 3. data fiels to be used in our case

Convert AHS ECG global measurements to MEDS format using standardized ECG code vocabulary.

**Data Sources**:
- `Globalmeasurements.pickle` - ECG measurements (2.27M records × 25 columns)
- `rmt22884_ecg_20211105_df.pickle` - Patient ID ↔ ECG ID mapping (2.26M records)

---

## 4. Code Format that we will have in meds data for the abvoe pandas files

Following existing pipeline convention, codes use **DOUBLE SLASHES**:
- `ECG//HR` (not `ECG/HR`)
- `ECG//QRS`
- `ECG//WAVEFORM`

This matches existing patterns:
- `DIAGNOSIS//ICD10CA//{code}`
- `MEDICINE//ATC//{code}`
- `ADMIT//HOSP//{code}`

---

## 5. Code Mapping (provided by dr sunil)

| Code | Data Column | Unit | Observed Range (5th-95th%) | Status |
|------|-------------|------|---------------------------|--------|
| ECG//HR | `heartrate` | beats/min | 52–128 | ✅ Available |
| ECG//QRS | `qrsdur` | ms | 74–156 | ✅ Available |
| ECG//QT | `qtint` | ms | 310–492 | ✅ Available |
| ECG//QTC | `qtcb` | ms | 398–524 | ✅ Available (Bazett) |
| ECG//PR | `print` | ms | 117–232 | ✅ Available |
| ECG//P_DURATION | `pdur` | ms | 0–500 | ✅ Available |
| ECG//T_AXIS | `tfrontaxis` | ° | -42–194 | ✅ Available |
| ECG//QRS_AXIS | `qrsfrontaxis` | ° | -64–106 | ✅ Available |
| ECG//RR | `rrint` | ms | 469–1154 | ✅ Available |
| ECG//WAVEFORM | `ecgId` | - | - | ⚠️ Handled by `ecgs.py` (not this component) |
| ECG//ST_ELEV | - | mV | - | ⚠️ Not in data (only axis available) |
| ECG//ST_DEP | - | mV | - | ⚠️ Not in data (only axis available) |

**Note**: ST elevation/depression amplitude (mV) is NOT in data. ST axis (degrees) is available instead.

---

## 6. mappings that i added after observing the data

| Proposed Code | Data Column | Unit | Observed Range (5th-95th%) | Notes |
|---------------|-------------|------|---------------------------|-------|
| ECG//ATRIAL_RATE | `atrialrate` | beats/min | 0–169 | Atrial rate |
| ECG//QTC_FRIDERICIA | `qtcf` | ms | 381–502 | QTc by Fridericia |
| ECG//P_AXIS | `pfrontaxis` | ° | -7–83 | P-wave frontal axis |
| ECG//ST_AXIS_FRONTAL | `stfrontaxis` | ° | -57–243 | ST frontal axis |
| ECG//ST_AXIS_HORIZONTAL | `sthorizaxis` | ° | -24–213 | ST horizontal axis |
| ECG//T_AXIS_HORIZONTAL | `thorizaxis` | ° | -34–177 | T-wave horizontal axis |
| ECG//QRS_AXIS_HORIZONTAL | `qrshorizaxis` | ° | -75–228 | QRS horizontal axis |
| ECG//I40_AXIS | `i40frontaxis` | ° | -45–99 | Initial 40ms axis |
| ECG//T40_AXIS | `t40frontaxis` | ° | -73–229 | Terminal 40ms axis |
| ECG//Q_ONSET | `qonset` | ms | 499–516 | Q wave onset |
| ECG//T_ONSET | `tonset` | ms | 655–778 | T onset (**sparse: 6%**) |

**Excluded**: `qtco` is 100% NULL.

---

## 7. Usage

```bash
PYTHONPATH=src python3 -m meds_pipeline.cli run \
  --source ahs \
  --components ecg_measurements \
  --cfg src/meds_pipeline/configs/ahs.yaml \
  --max-patients 100 \
  --progress
```

---

## 8. Output Schema

| Column | Type | Example |
|--------|------|---------|
| subject_id | string | "219126" |
| time | timestamp | 2007-07-25 23:02:25 |
| code | string | "ECG//HR" |
| numeric_value | float | 63.0 |
| unit | string | "beats/min" |
| event_type | string | "ECG" |
| code_system | string | "AHS_ECG" |

---
