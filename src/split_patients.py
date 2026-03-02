# AHS split
import pandas as pd
splits_path = '/data/padmalab_external/special_project/AHS_Data_Release_2/AHS_Data_release_2_ECG_study_data_splits.pickle'
splits_df = pd.read_pickle(splits_path)
ecg_path = '/data/padmalab_external/special_project/AHS_Data_Release_2/rmt22884_ecg_20211105_df.parquet'
ecg_df = pd.read_parquet(ecg_path)
patients_split_df = ecg_df[['PATID', 'ecgId']]
patients_split_df['split'] = 'development'
patients_split_df.loc[patients_split_df['ecgId'].isin(splits_df['Holdout Set - all ECGs']), 'split'] = 'holdout'
# only keep one patient id if there is more ECGs for the same patient, to check if there is any patient overlap between development and holdout set
patients_split_df = patients_split_df.drop_duplicates(subset=['PATID'])
print (patients_split_df['split'].value_counts())
patients_split_df[['PATID', 'split']].to_parquet('/data/padmalab_external/special_project/meds_pipeline_output/ahs_patients_split.parquet', index=False)

# MIMIC split
import pandas as pd
import random
random.seed(42)
temp = pd.read_csv('/data/padmalab_external/special_project/physionet.org/files/mimic-iv-ecg-ext-icd-labels/1.0.1/records_w_diag_icd10.csv')
temp = temp[~temp['age'].isna()]

ecg_record_df = pd.read_csv('/data/padmalab_external/special_project/physionet.org/files/mimic-iv-ecg/1.0/record_list.csv')

data_df = temp[['study_id', 'subject_id', 'strat_fold']]
data_df = data_df.merge(ecg_record_df[['study_id', 'path', 'ecg_time']], on='study_id')
folds = data_df['strat_fold'].unique().tolist()
random.shuffle(folds)
train, val, test = folds[:10], folds[10:12], folds[12:]

train_df = data_df[data_df['strat_fold'].isin(train)]
val_df = data_df[data_df['strat_fold'].isin(val)]
test_df = data_df[data_df['strat_fold'].isin(test)]

print ('train', train_df.shape[0], 'val', val_df.shape[0], 'test', test_df.shape[0])
data_df['split'] = 'development'
data_df.loc[data_df['strat_fold'].isin(['test']), 'split'] = 'holdout'
data_df = data_df.drop_duplicates(subset=['subject_id'])
data_df.to_parquet('/data/padmalab_external/special_project/meds_pipeline_output/mimic_patients_split.parquet', index=False)