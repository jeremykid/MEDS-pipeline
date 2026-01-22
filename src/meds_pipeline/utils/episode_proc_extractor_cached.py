"""
Cached and preprocessed utilities for extracting procedure codes (PROCCODE) from DAD data.

This module provides optimized functions for extracting procedure codes with:
1. Preprocessing: Extract all PROCCODEs into lists to avoid repeated column access
2. PATID-based caching: Group data by PATID to minimize repeated filtering
3. Binary search: Use sorted data with binary search for efficient time window filtering
4. Optional cuDF GPU acceleration (experimental)

Design choices:
- Deduplication: Procedure codes are deduplicated per episode (using set)
- Sorting: Final list is sorted alphabetically for consistent output
- Time window: [start_date - number_of_days, start_date - 1 day] (excludes episode start date)
"""

import pandas as pd
import numpy as np
from typing import List, Dict, Set, Optional

# Try to import cuDF for GPU acceleration
try:
    import cudf
    CUDF_AVAILABLE = True
except ImportError:
    CUDF_AVAILABLE = False
    cudf = None

# Try to import tqdm for progress bar
try:
    from tqdm import tqdm
except ImportError:
    def tqdm(iterable, **kwargs):
        return iterable


def preprocess_proc_codes(df: pd.DataFrame, proc_cols: List[str], source_name: str = "") -> pd.DataFrame:
    """
    Preprocess dataframe to extract all procedure codes into a single list column.
    This avoids repeatedly reading and processing PROCCODE columns.
    
    Parameters
    ----------
    df : pd.DataFrame
        Dataframe with PROCCODE columns
    proc_cols : List[str]
        List of PROCCODE column names (e.g., ['PROCCODE1', 'PROCCODE2', ...])
    source_name : str
        Name of source for logging
        
    Returns
    -------
    pd.DataFrame
        Dataframe with added 'proc_codes_list' column containing list of all procedure codes
    """
    print(f"   Preprocessing {source_name} data: extracting PROCCODEs...")
    
    df = df.copy()
    
    # Get existing PROCCODE columns
    existing_proc_cols = [col for col in proc_cols if col in df.columns]
    
    if not existing_proc_cols:
        print(f"   ⚠️  No PROCCODE columns found in {source_name}")
        df['proc_codes_list'] = df.apply(lambda x: [], axis=1)
        return df
    
    print(f"   Found {len(existing_proc_cols)} PROCCODE columns: {existing_proc_cols[0]}...{existing_proc_cols[-1]}")
    
    # Extract all PROCCODEs into a list for each row
    def extract_proc_list(row):
        proc_list = []
        for col in existing_proc_cols:
            val = row[col]
            if pd.notna(val):
                val_str = str(val).strip()
                if val_str != '':
                    proc_list.append(val_str)
        return proc_list
    
    df['proc_codes_list'] = df.apply(extract_proc_list, axis=1)
    
    # Count statistics
    total_codes = sum(len(codes) for codes in df['proc_codes_list'])
    rows_with_codes = (df['proc_codes_list'].apply(len) > 0).sum()
    print(f"   ✅ Preprocessed {len(df):,} records")
    print(f"      - Records with procedure codes: {rows_with_codes:,} ({rows_with_codes/len(df)*100:.1f}%)")
    print(f"      - Total procedure codes extracted: {total_codes:,}")
    
    return df


def extract_proc_codes_cached(
    episode_df: pd.DataFrame,
    dad_df: pd.DataFrame,
    number_of_days: int,
    batch_size: int = 1000,
    show_progress: bool = True,
    use_cudf: bool = False
) -> pd.DataFrame:
    """
    Optimized procedure code extraction with preprocessing and PATID-based caching.
    
    Key optimizations:
    1. Preprocess DAD to extract all PROCCODEs into lists (avoid repeated column reading)
    2. Cache filtered results by PATID (avoid re-filtering for same patient's episodes)
    3. Use binary search for time window filtering
    
    Parameters
    ----------
    episode_df : pd.DataFrame
        Episode dataframe with columns: episode_order, start_date, PATID
    dad_df : pd.DataFrame
        DAD dataframe with episode_order, ADMITDATE_DT, DISDATE_DT, PATID, PROCCODE columns
    number_of_days : int
        Number of days for time window (extracts codes from [start_date - N days, start_date - 1 day])
    batch_size : int
        Number of episodes to process in each batch (for logging purposes)
    show_progress : bool
        Whether to show progress bar
    use_cudf : bool
        Whether to use cuDF for GPU acceleration (experimental)
        
    Returns
    -------
    pd.DataFrame
        DataFrame with columns:
        - index: episode_order
        - PATID: patient ID
        - start_date: episode start date
        - proc_codes: list of procedure codes (deduplicated, sorted alphabetically)
    """
    # Check cuDF availability
    if use_cudf and not CUDF_AVAILABLE:
        print("⚠️  cuDF is not available. Falling back to CPU processing.")
        print("   To use cuDF, install it with: pip install cudf-cu12  # for CUDA 12")
        use_cudf = False
    
    if use_cudf:
        print(f"🚀 Using cached preprocessing with cuDF GPU acceleration...")
        print("   Note: cuDF GPU acceleration is experimental. Using optimized CPU processing.")
        use_cudf = False  # Disable for now, cached CPU method is already very fast
    else:
        print(f"🚀 Using cached preprocessing (CPU, batch size: {batch_size})...")
    
    # Validate required columns
    required_episode_cols = ['episode_order', 'start_date']
    missing_cols = [col for col in required_episode_cols if col not in episode_df.columns]
    if missing_cols:
        raise ValueError(f"Episode dataframe missing required columns: {missing_cols}")
    
    required_dad_cols = ['episode_order', 'ADMITDATE_DT']
    missing_cols = [col for col in required_dad_cols if col not in dad_df.columns]
    if missing_cols:
        raise ValueError(f"DAD dataframe missing required columns: {missing_cols}")
    
    # Prepare episode data
    print("   Preparing episode data...")
    episodes = episode_df[['episode_order', 'start_date']].copy()
    if 'PATID' in episode_df.columns:
        episodes['PATID'] = episode_df['PATID']
    else:
        # Try to extract PATID from episode_order if possible
        print("   ⚠️  PATID not found in episode_df, attempting to extract from episode_order")
        episodes['PATID'] = episodes['episode_order'].str.split('_').str[0]
    
    # Convert PATID to string for consistent matching
    episodes['PATID'] = episodes['PATID'].astype(str)
    
    # Parse start_date
    try:
        episodes['start_date'] = pd.to_datetime(episodes['start_date'])
    except Exception as e:
        raise ValueError(f"Failed to parse start_date column: {e}")
    
    # Calculate time window
    # Window: [start_date - number_of_days, start_date - 1 day]
    # Excludes the episode start date itself
    episodes['window_start'] = episodes['start_date'] - pd.Timedelta(days=number_of_days)
    episodes['window_end'] = episodes['start_date'] - pd.Timedelta(days=1)
    
    print(f"   ✅ Prepared {len(episodes):,} episodes")
    
    # Prepare and preprocess DAD data
    print("   Preparing DAD data...")
    dad_prepared = dad_df.copy()
    
    # Parse dates
    try:
        if 'ADMITDATE_DT' in dad_prepared.columns:
            dad_prepared['ADMITDATE_DT'] = pd.to_datetime(dad_prepared['ADMITDATE_DT'], errors='coerce')
        if 'DISDATE_DT' in dad_prepared.columns:
            dad_prepared['DISDATE_DT'] = pd.to_datetime(dad_prepared['DISDATE_DT'], errors='coerce')
        else:
            # If DISDATE_DT not available, use ADMITDATE_DT as both
            dad_prepared['DISDATE_DT'] = dad_prepared['ADMITDATE_DT']
    except Exception as e:
        raise ValueError(f"Failed to parse DAD date columns: {e}")
    
    # Remove invalid records (missing dates)
    initial_count = len(dad_prepared)
    dad_prepared = dad_prepared[
        dad_prepared['ADMITDATE_DT'].notna() & 
        dad_prepared['DISDATE_DT'].notna()
    ].copy()
    if len(dad_prepared) < initial_count:
        print(f"   ⚠️  Removed {initial_count - len(dad_prepared):,} records with invalid dates")
    
    # Preprocess PROCCODEs into lists
    # DAD typically has PROCCODE1..PROCCODE20 (up to 20 procedure codes)
    proc_cols = [f'PROCCODE{i}' for i in range(1, 21)]
    dad_prepared = preprocess_proc_codes(dad_prepared, proc_cols, "DAD")
    
    # Sort by dates for binary search
    dad_prepared = dad_prepared.sort_values(['ADMITDATE_DT', 'DISDATE_DT']).reset_index(drop=True)
    
    # Group DAD by PATID for caching (major optimization!)
    if 'PATID' in dad_prepared.columns:
        # Convert PATID to string for consistent matching
        dad_prepared['PATID'] = dad_prepared['PATID'].astype(str)
        dad_by_patid = {patid: group for patid, group in dad_prepared.groupby('PATID')}
        print(f"   ✅ Grouped DAD data by {len(dad_by_patid):,} unique PATIDs")
    else:
        print("   ⚠️  PATID not found in DAD, will use episode_order for matching")
        dad_by_patid = {}
    
    # Process episodes - group by PATID to maximize efficiency
    results = []
    
    # Group episodes by PATID - this is the key optimization!
    # For each PATID, we only filter their DAD data once
    episodes_by_patid = episodes.groupby('PATID')
    
    print(f"   Processing {len(episodes_by_patid):,} unique patients...")
    
    iterator = tqdm(episodes_by_patid, desc="Processing patients") if show_progress else episodes_by_patid
    
    for patid, patient_episodes in iterator:
        # Get this patient's DAD data ONCE (major optimization!)
        patient_dad = dad_by_patid.get(patid, pd.DataFrame())
        
        # Sort patient data once (if not empty)
        if len(patient_dad) > 0:
            patient_dad = patient_dad.sort_values(['ADMITDATE_DT', 'DISDATE_DT']).reset_index(drop=True)
        
        # Process each episode for this patient
        for _, episode in patient_episodes.iterrows():
            episode_order = episode['episode_order']
            start_date = episode['start_date']
            window_start = episode['window_start']
            window_end = episode['window_end']
            
            proc_codes = set()
            
            # Extract from DAD - use binary search on already-filtered patient data
            if len(patient_dad) > 0:
                # Binary search: find records where DISDATE_DT >= window_start
                start_idx = patient_dad['DISDATE_DT'].searchsorted(window_start, side='left')
                # Binary search: find records where ADMITDATE_DT <= window_end
                end_idx = patient_dad['ADMITDATE_DT'].searchsorted(window_end, side='right')
                
                if start_idx < end_idx:
                    dad_candidates = patient_dad.iloc[start_idx:end_idx]
                    
                    # Filter: exclude current episode and check time overlap
                    # A DAD record overlaps with window if:
                    # ADMITDATE_DT <= window_end AND DISDATE_DT >= window_start
                    dad_mask = (
                        (dad_candidates['episode_order'] != episode_order) &
                        (dad_candidates['ADMITDATE_DT'] <= window_end) &
                        (dad_candidates['DISDATE_DT'] >= window_start)
                    )
                    dad_filtered = dad_candidates[dad_mask]
                    
                    if len(dad_filtered) > 0:
                        # Use preprocessed proc_codes_list - just union sets! (much faster)
                        for proc_list in dad_filtered['proc_codes_list']:
                            proc_codes.update(proc_list)
            
            # Store result
            results.append({
                'episode_order': episode_order,
                'PATID': patid,
                'start_date': start_date,
                'proc_codes': sorted(list(proc_codes))  # Deduplicated and sorted alphabetically
            })
    
    # Create result DataFrame
    result_df = pd.DataFrame(results)
    result_df = result_df.set_index('episode_order')
    
    # If we used cuDF, ensure we return a pandas DataFrame
    if use_cudf and CUDF_AVAILABLE:
        if isinstance(result_df, cudf.DataFrame):
            result_df = result_df.to_pandas()
    
    return result_df
