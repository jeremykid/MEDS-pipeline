"""
MIMIC ECG EHR 1-Year Lookback Flag Assignment

For each ECG record in ecg_df, set `has_EHR_1yr_lookback = True` if there exists
any EHR record (from MEDS parquet files, excluding source_table == 'ECG_RECORDS') where:
  - subject_id matches (ecg_df subject_id == MEDS subject_id)
  - time >= 1_yr_lookback  (within 1 year before ECG)
  - time < ecg_time         (strictly before ECG date)

Once a row is marked True, it is never overwritten to False.

Optimization strategy:
  - Use vectorized merge instead of iterrows()
  - Only process pending (still False) rows each iteration
  - Pre-filter MEDS rows with isin() to reduce merge size
  - Early exit when all rows are True
"""

import pandas as pd
from glob import glob
from tqdm import tqdm

# ============================================================
# 1. Load MIMIC ECG data
# ============================================================
print("Loading MIMIC ecg_df...")
ecg_df = pd.read_csv(
    "/data/padmalab_external/special_project/physionet.org/files/mimic-iv-ecg/1.0/record_list.csv"
)
ecg_df['ecg_time'] = pd.to_datetime(ecg_df['ecg_time'])
# ecg_df['subject_id'] = ecg_df['subject_id'].astype(str)
# ecg_df['subject_id'] = ecg_df['subject_id'].astype(int)

ecg_df['1_yr_lookback'] = ecg_df['ecg_time'] - pd.Timedelta(days=365)
ecg_df['has_EHR_1yr_lookback'] = False

print(f"ecg_df loaded: {len(ecg_df):,} rows")

# ============================================================
# 2. Load list of MEDS parquet files
# ============================================================
meds_files = sorted(
    glob('/data/padmalab_external/special_project/meds_pipeline_output/mimic/mimic_meds_core_part_*.parquet')
)
print(f"Found {len(meds_files)} MEDS files")

# ============================================================
# 3. Iterate through MEDS files with vectorized matching
# ============================================================
for meds_file in tqdm(meds_files, desc="Processing MEDS files"):
    # Early exit: all rows already flagged
    if ecg_df['has_EHR_1yr_lookback'].all():
        print("All ECG rows already have EHR lookback — stopping early.")
        break

    # Read one MEDS file
    temp = pd.read_parquet(meds_file)

    # Exclude ECG source table (we only want non-ECG EHR records)
    temp = temp[temp['source_table'] != 'ECG_RECORDS']

    if temp.empty:
        continue

    # Ensure subject_id type consistency
    temp['subject_id'] = temp['subject_id'].astype(str)

    # Get pending ECG rows (still False)
    pending_mask = ~ecg_df['has_EHR_1yr_lookback']
    ecg_pending = ecg_df.loc[pending_mask, ['subject_id', 'ecg_time', '1_yr_lookback']]

    # Pre-filter: only keep MEDS rows matching pending patients
    pending_ids = ecg_pending['subject_id'].unique()
    temp = temp[temp['subject_id'].isin(pending_ids)][['subject_id', 'time']]

    if temp.empty:
        continue

    # Vectorized merge: join on subject_id
    merged = ecg_pending.merge(
        temp,
        on='subject_id',
        how='inner'
    )

    if merged.empty:
        continue

    # Vectorized time-range filter
    matched = merged[
        (merged['time'] < merged['ecg_time']) &
        (merged['time'] >= merged['1_yr_lookback'])
    ]

    if matched.empty:
        continue

    # Update flag for matched subject_ids (only False -> True, never True -> False)
    matched_ids = matched['subject_id'].unique()
    ecg_df.loc[ecg_df['subject_id'].isin(matched_ids), 'has_EHR_1yr_lookback'] = True

    # Progress info
    n_true = ecg_df['has_EHR_1yr_lookback'].sum()
    tqdm.write(f"  -> {len(matched_ids):,} new matches | Total True: {n_true:,} / {len(ecg_df):,}")

# ============================================================
# 4. Summary & Save
# ============================================================
n_true = ecg_df['has_EHR_1yr_lookback'].sum()
n_total = len(ecg_df)
print(f"\nDone! {n_true:,} / {n_total:,} ECG records have EHR data within 1-year lookback ({n_true/n_total*100:.2f}%)")
print(ecg_df['has_EHR_1yr_lookback'].value_counts())

output_path = "/data/padmalab_external/special_project/Weijie_Code/MEDs_label/mimic_ecg_with_ehr_lookback_flag.parquet"
ecg_df.to_parquet(output_path, index=False)
print(f"Saved to: {output_path}")
