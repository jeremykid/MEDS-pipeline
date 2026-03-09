"""
ECG EHR 1-Year Lookback Flag Assignment

For each ECG record in ecg_df, set `has_EHR_1yr_lookback = True` if there exists
any EHR record (from MEDS parquet files, excluding source_table == 'AHS_ECG') where:
  - PATID == subject_id (patient match)
  - time >= 1_yr_lookback  (within 1 year before ECG)
  - time < dateAcquired     (strictly before ECG date)

Once a row is marked True, it is never overwritten to False.

Optimization strategy:
  - Use vectorized merge instead of iterrows()
  - Only process pending (still False) rows each iteration
  - Early exit when all rows are True
"""

import pandas as pd
from glob import glob
from tqdm import tqdm

# ============================================================
# 1. Load ECG data
# ============================================================
print("Loading ecg_df...")
ecg_df = pd.read_parquet(
    "/data/padmalab_external/special_project/AHS_Data_Release_2/rmt22884_ecg_20211105_df.parquet"
)

# Clean ecgId (strip first 2 and last 1 characters)
ecg_df['ecgId'] = ecg_df['ecgId'].str[2:-1]

# Ensure PATID is string (used for matching with MEDS subject_id)
ecg_df['PATID'] = ecg_df['PATID'].astype(str)

# Compute 1-year lookback window
ecg_df['1_yr_lookback'] = ecg_df['dateAcquired'] - pd.Timedelta(days=365)

# Initialize flag
ecg_df['has_EHR_1yr_lookback'] = False

print(f"ecg_df loaded: {len(ecg_df):,} rows")

# ============================================================
# 2. Load list of MEDS parquet files
# ============================================================
meds_files = sorted(
    glob('/data/padmalab_external/special_project/meds_pipeline_output/ahs/ahs_meds_core_part_*.parquet')
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
    temp = temp[temp['source_table'] != 'AHS_ECG']

    if temp.empty:
        continue

    # Only keep MEDS rows whose subject_id appears in ecg_df PATID (reduces merge size)
    temp = temp[temp['subject_id'].isin(ecg_df['PATID'])]

    if temp.empty:
        continue

    # Get pending ECG rows (still False)
    pending_mask = ~ecg_df['has_EHR_1yr_lookback']
    ecg_pending = ecg_df.loc[pending_mask, ['PATID', 'dateAcquired', '1_yr_lookback']]

    # Further filter: only keep MEDS rows matching pending patients
    temp = temp[temp['subject_id'].isin(ecg_pending['PATID'])]

    if temp.empty:
        continue

    # Vectorized merge: join on patient ID (PATID ↔ subject_id)
    merged = ecg_pending.merge(
        temp[['subject_id', 'time']],
        left_on='PATID',
        right_on='subject_id',
        how='inner'
    )

    if merged.empty:
        continue

    # Vectorized time-range filter
    matched = merged[
        (merged['time'] < merged['dateAcquired']) &
        (merged['time'] >= merged['1_yr_lookback'])
    ]

    if matched.empty:
        continue

    # Update flag for matched PATIDs (only False -> True, never True -> False)
    matched_patids = matched['PATID'].unique()
    ecg_df.loc[ecg_df['PATID'].isin(matched_patids), 'has_EHR_1yr_lookback'] = True

    # Progress info
    n_true = ecg_df['has_EHR_1yr_lookback'].sum()
    tqdm.write(f"  -> {len(matched_patids):,} new matches | Total True: {n_true:,} / {len(ecg_df):,}")

# ============================================================
# 4. Summary & Save
# ============================================================
n_true = ecg_df['has_EHR_1yr_lookback'].sum()
n_total = len(ecg_df)
print(f"\nDone! {n_true:,} / {n_total:,} ECG records have EHR data within 1-year lookback ({n_true/n_total*100:.2f}%)")

output_path = "/data/padmalab_external/special_project/Weijie_Code/MEDs_label/ahs_ecg_with_ehr_lookback_flag.parquet"

ecg_df.rename(columns={'PATID': 'subject_id', 'dateAcquired': 'ecg_time'}, inplace=True)
ecg_df['path'] = ecg_df.apply(lambda row: '%s.xml.npy.gz' % row['ecgId'], axis=1)

ecg_df.to_parquet(output_path, index=False)
print(f"Saved to: {output_path}")
