import pandas as pd

def get_binary_discharged_label(dataset, num_days=30):
    if dataset == 'AHS':
        discharge_path = '/data/padmalab_external/special_project/Weijie_Code/MEDs_label/ahs_discharged_death.parquet'
    elif dataset == 'MIMIC':
        discharge_path = '/data/padmalab_external/special_project/Weijie_Code/MEDs_label/mimic_discharged_death.parquet'
    discharge_df = pd.read_parquet(discharge_path)
    discharge_df['time'] = (discharge_df['event_time'] - discharge_df['discharge_date']).dt.days
    discharge_df['event'] = discharge_df['death_event']
    discharge_df[f'{num_days}_days_death'] = -1
    discharge_df.loc[(discharge_df['time'] <= num_days) & (discharge_df['event'] == True), f'{num_days}_days_death'] = 1
    discharge_df.loc[(discharge_df['time'] > num_days), f'{num_days}_days_death'] = 0
    print (discharge_df[f'{num_days}_days_death'].value_counts())
    discharge_df = discharge_df[discharge_df[f'{num_days}_days_death'] != -1]
    return discharge_df[['patient_id', 'discharge_date', f'{num_days}_days_death']]

if __name__ == '__main__':
    print (get_binary_discharged_label('AHS', num_days=30))
    print (get_binary_discharged_label('MIMIC', num_days=365))