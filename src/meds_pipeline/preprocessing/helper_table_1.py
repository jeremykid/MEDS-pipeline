import yaml
from typing import Dict, List
import pickle
import pandas as pd
import re

def get_icd_codes_map():
    with open("icd_codes_map.yaml", "r", encoding="utf-8") as f:
        icd_codes_map: Dict[str, List[str]] = yaml.safe_load(f)
    return icd_codes_map

def get_ecg_ids_by_set_name(set_name='Full Data sets'):
    with open('/data/padmalab/ecg/data/processed/ECG_all_dx_study_data_splits_with_pacemaker_051021.pickle',"rb") as f:
        ecg_splits = pickle.load(f)    
    if set_name == 'Full Data sets':
        ecg_ids_set = list(ecg_splits['Holdout Set - all ECGs']) +\
                    list(ecg_splits['CV Train Sets - 5 folds']['Fold-1']) +\
                    list(ecg_splits['CV Test Sets - 5 folds']['Fold-1'])
        ecg_ids_set = [ecgid[2:-1] for ecgid in ecg_ids_set]                
    elif set_name == 'Development set':
        ecg_ids_set = list(ecg_splits['CV Train Sets - 5 folds']['Fold-1']) +\
                    list(ecg_splits['CV Test Sets - 5 folds']['Fold-1'])
        ecg_ids_set = [ecgid[2:-1] for ecgid in ecg_ids_set]        
    elif set_name == 'Holdout set':
        ecg_ids_set = list(ecg_splits['Holdout Set - all ECGs'])
        ecg_ids_set = [ecgid[2:-1] for ecgid in ecg_ids_set]                
    elif set_name == 'First ecg per patient':
        ecg_ids_set = list(ecg_splits['Holdout Set - all ECGs'])
        ecg_ids_set = [ecgid[2:-1] for ecgid in ecg_ids_set]     
        ecg_df = pd.read_pickle('/data/padmalab/ecg/data/raw/rmt22884_ecg.pickle')
        ecg_df.ecgId = ecg_df.ecgId.str[2:-1]
        ecg_df = ecg_df[ecg_df['ecgId'].isin(ecg_ids_set)]
        ecg_df = ecg_df.sort_values(by='dateAcquired').drop_duplicates(subset='PATID', keep='first')
        ecg_ids_set = ecg_df['ecgId'].tolist()
    return ecg_ids_set

def add_history_flag(
    ecg_df: pd.DataFrame,
    episode_df: pd.DataFrame,
    claim_df: pd.DataFrame,
    dad_df: pd.DataFrame,
    ed_df: pd.DataFrame,
    ICD_9_list_str: str,
    ICD_10_list_str: str,
    icd9_cols=('HLTH_DX_ICD9X_CODE_1', 'HLTH_DX_ICD9X_CODE_2', 'HLTH_DX_ICD9X_CODE_3'),
    dad_dx_regex=r'DXCODE',
    ed_dx_regex=r'DXCODE',
    lookback_years: int = 5,
):
    """
    Build 'has_history' for ECG rows using the SAME semantics as the original script.
    Optimized version using vectorized operations and reduced memory copies.
    """

    # ---------- 1) Filter claims (ICD-9) - vectorized approach ----------
    columns_to_check = [c for c in icd9_cols if c in claim_df.columns]
    if not columns_to_check:
        raise ValueError("None of the icd9_cols were found in claim_df.")
    if not ICD_9_list_str:
        ICD_9_list_str = 'impossible_to_match'

    # Compile regex once for reuse
    icd9_pattern = re.compile(ICD_9_list_str)
    
    # Build combined mask instead of concat multiple dataframes
    claim_mask = pd.Series(False, index=claim_df.index)
    for dx_column in columns_to_check:
        claim_mask |= claim_df[dx_column].astype(str).str.contains(icd9_pattern, na=False)
    
    keep_cols = ['PATID', 'SE_END_DATE'] + columns_to_check
    filtered_claim_df = claim_df.loc[claim_mask, keep_cols].drop_duplicates()

    # ---------- 2) Filter DAD (ICD-10) - vectorized approach ----------
    if not ICD_10_list_str:
        ICD_10_list_str = 'impossible_to_match'
    icd10_pattern = re.compile(ICD_10_list_str)
    dad_dx_cols = dad_df.columns[dad_df.columns.str.contains(dad_dx_regex, na=False)]
    
    dad_mask = pd.Series(False, index=dad_df.index)
    for dx_column in dad_dx_cols:
        dad_mask |= dad_df[dx_column].astype(str).str.contains(icd10_pattern, na=False)
    
    filtered_dad_df = dad_df.loc[dad_mask].drop_duplicates()

    # ---------- 3) Filter ED (ICD-10) - vectorized approach ----------
    ed_dx_cols = ed_df.columns[ed_df.columns.str.contains(ed_dx_regex, na=False)]
    
    ed_mask = pd.Series(False, index=ed_df.index)
    for dx_column in ed_dx_cols:
        ed_mask |= ed_df[dx_column].astype(str).str.contains(icd10_pattern, na=False)
    
    filtered_ed_df = ed_df.loc[ed_mask].drop_duplicates()

    # ---------- 4) Mark episodes with_history ----------
    episode_df = episode_df.copy()
    
    if 'episode_order' not in episode_df.columns:
        raise ValueError("episode_df must contain 'episode_order'.")
    
    # Use set for O(1) lookup instead of isin
    history_episodes = set()
    if 'episode_order' in filtered_dad_df.columns:
        history_episodes.update(filtered_dad_df['episode_order'].unique())
    if 'episode_order' in filtered_ed_df.columns:
        history_episodes.update(filtered_ed_df['episode_order'].unique())
    
    episode_df['with_history'] = episode_df['episode_order'].isin(history_episodes)
    filtered_episode_df = episode_df.loc[episode_df['with_history'], ['PATID', 'start_date', 'end_date']].copy()

    # ---------- 5) Build ECG window and compute hits - optimized ----------
    ecg = ecg_df.copy()
    ecg['dateAcquired'] = pd.to_datetime(ecg['dateAcquired'])
    ecg['end_date'] = pd.to_datetime(ecg['end_date'])
    ecg['hist_start'] = ecg['dateAcquired'] - pd.DateOffset(years=lookback_years)

    # Pre-filter claims and episodes by global window
    global_start = ecg['hist_start'].min()
    global_end = ecg['end_date'].max()

    # Claims processing
    claims = filtered_claim_df[['PATID', 'SE_END_DATE']].copy()
    claims['SE_END_DATE'] = pd.to_datetime(claims['SE_END_DATE'], errors='coerce')
    claims = claims[(claims['SE_END_DATE'] >= global_start) & (claims['SE_END_DATE'] <= global_end)]

    # Episodes processing
    episodes = filtered_episode_df.copy()
    episodes['start_date'] = pd.to_datetime(episodes['start_date'], errors='coerce')
    episodes['ep_end_date'] = pd.to_datetime(episodes['end_date'], errors='coerce')
    episodes = episodes[(episodes['ep_end_date'] >= global_start) & (episodes['start_date'] <= global_end)]
    episodes = episodes[['PATID', 'start_date', 'ep_end_date']]

    # Get unique patients with claims/episodes for fast filtering
    patients_with_claims = set(claims['PATID'].unique())
    patients_with_episodes = set(episodes['PATID'].unique())

    # Initialize flags
    ecg['has_claim'] = False
    ecg['has_episode'] = False

    # Process only patients that have potential claims
    ecg_with_claims = ecg.loc[ecg['PATID'].isin(patients_with_claims), ['PATID', 'hist_start', 'end_date']].copy()
    if len(ecg_with_claims) > 0:
        ecg_with_claims['ecg_idx'] = ecg_with_claims.index
        m_claims = ecg_with_claims.merge(claims, on='PATID', how='inner')
        m_claims['claim_hit'] = (m_claims['SE_END_DATE'] >= m_claims['hist_start']) & (m_claims['SE_END_DATE'] <= m_claims['end_date'])
        claim_hits = m_claims.groupby('ecg_idx')['claim_hit'].any()
        ecg.loc[claim_hits[claim_hits].index, 'has_claim'] = True

    # Process only patients that have potential episodes
    ecg_with_episodes = ecg.loc[ecg['PATID'].isin(patients_with_episodes), ['PATID', 'hist_start', 'end_date']].copy()
    if len(ecg_with_episodes) > 0:
        ecg_with_episodes['ecg_idx'] = ecg_with_episodes.index
        m_eps = ecg_with_episodes.merge(episodes, on='PATID', how='inner')
        m_eps['ep_hit'] = (m_eps['start_date'] <= m_eps['end_date']) & (m_eps['ep_end_date'] >= m_eps['hist_start'])
        ep_hits = m_eps.groupby('ecg_idx')['ep_hit'].any()
        ecg.loc[ep_hits[ep_hits].index, 'has_episode'] = True

    ecg['has_history'] = ecg['has_claim'] | ecg['has_episode']
    temp_ecg_df = ecg.drop(columns=['hist_start', 'has_claim', 'has_episode'])

    return temp_ecg_df

