#!/usr/bin/env python3
"""
Unit tests for episode_proc_extractor_cached module.

Tests cover:
1. Time window boundary logic
2. Empty/null value handling
3. Deduplication and sorting
4. PATID matching
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from meds_pipeline.utils.episode_proc_extractor_cached import (
    extract_proc_codes_cached,
    preprocess_proc_codes
)


class TestPreprocessProcCodes:
    """Tests for preprocess_proc_codes function."""
    
    def test_basic_extraction(self):
        """Test basic procedure code extraction."""
        df = pd.DataFrame({
            'PROCCODE1': ['A001', 'B002', None],
            'PROCCODE2': ['A002', None, 'C003'],
            'PROCCODE3': [None, 'B003', ''],
        })
        
        result = preprocess_proc_codes(df, ['PROCCODE1', 'PROCCODE2', 'PROCCODE3'], "test")
        
        assert 'proc_codes_list' in result.columns
        assert result.iloc[0]['proc_codes_list'] == ['A001', 'A002']
        assert result.iloc[1]['proc_codes_list'] == ['B002', 'B003']
        assert result.iloc[2]['proc_codes_list'] == ['C003']
    
    def test_empty_columns(self):
        """Test when no PROCCODE columns exist."""
        df = pd.DataFrame({
            'other_col': [1, 2, 3]
        })
        
        result = preprocess_proc_codes(df, ['PROCCODE1', 'PROCCODE2'], "test")
        
        assert 'proc_codes_list' in result.columns
        assert all(result['proc_codes_list'].apply(len) == 0)
    
    def test_all_null_values(self):
        """Test when all values are null or empty."""
        df = pd.DataFrame({
            'PROCCODE1': [None, '', np.nan],
            'PROCCODE2': ['', None, '  '],
        })
        
        result = preprocess_proc_codes(df, ['PROCCODE1', 'PROCCODE2'], "test")
        
        assert all(result['proc_codes_list'].apply(len) == 0)


class TestExtractProcCodesCached:
    """Tests for extract_proc_codes_cached function."""
    
    def setup_method(self):
        """Set up test data."""
        # Create episode data
        self.episode_df = pd.DataFrame({
            'episode_order': ['P001_1', 'P001_2', 'P002_1'],
            'start_date': pd.to_datetime(['2024-06-01', '2024-07-01', '2024-06-15']),
            'PATID': ['P001', 'P001', 'P002']
        })
        
        # Create DAD data
        self.dad_df = pd.DataFrame({
            'episode_order': ['P001_0', 'P001_1', 'P002_0', 'P002_1'],
            'ADMITDATE_DT': pd.to_datetime(['2024-05-01', '2024-06-01', '2024-05-10', '2024-06-15']),
            'DISDATE_DT': pd.to_datetime(['2024-05-05', '2024-06-05', '2024-05-15', '2024-06-20']),
            'PATID': ['P001', 'P001', 'P002', 'P002'],
            'PROCCODE1': ['PROC_A', 'PROC_B', 'PROC_C', 'PROC_D'],
            'PROCCODE2': ['PROC_E', None, 'PROC_F', None]
        })
    
    def test_basic_extraction(self):
        """Test basic procedure code extraction."""
        result = extract_proc_codes_cached(
            self.episode_df,
            self.dad_df,
            number_of_days=60,
            show_progress=False
        )
        
        assert len(result) == 3
        assert 'proc_codes' in result.columns
        
        # Episode P001_1 (2024-06-01): should find P001_0 (2024-05-01 to 2024-05-05)
        # Window: 2024-04-02 to 2024-05-31
        p001_1_codes = result.loc['P001_1', 'proc_codes']
        assert 'PROC_A' in p001_1_codes
        assert 'PROC_E' in p001_1_codes
    
    def test_time_window_boundary(self):
        """Test that time window boundaries are correct."""
        # Create data with specific dates to test boundary
        episode_df = pd.DataFrame({
            'episode_order': ['E1'],
            'start_date': pd.to_datetime(['2024-06-10']),
            'PATID': ['P001']
        })
        
        dad_df = pd.DataFrame({
            'episode_order': ['D1', 'D2', 'D3', 'D4'],
            # D1: ends exactly on window_start - should be included (overlap)
            # D2: within window - should be included
            # D3: starts exactly on start_date - should be excluded (window_end = start_date - 1)
            # D4: different patient - should be excluded
            'ADMITDATE_DT': pd.to_datetime(['2024-06-01', '2024-06-05', '2024-06-10', '2024-06-05']),
            'DISDATE_DT': pd.to_datetime(['2024-06-03', '2024-06-07', '2024-06-12', '2024-06-07']),
            'PATID': ['P001', 'P001', 'P001', 'P002'],
            'PROCCODE1': ['BOUNDARY', 'WITHIN', 'ON_START', 'OTHER_PAT']
        })
        
        # 10-day window: 2024-06-01 to 2024-06-09
        result = extract_proc_codes_cached(
            episode_df,
            dad_df,
            number_of_days=10,
            show_progress=False
        )
        
        codes = result.loc['E1', 'proc_codes']
        assert 'BOUNDARY' in codes  # D1 overlaps with window
        assert 'WITHIN' in codes    # D2 within window
        assert 'ON_START' not in codes  # D3 starts on episode start date
        assert 'OTHER_PAT' not in codes  # D4 different patient
    
    def test_exclude_current_episode(self):
        """Test that current episode's DAD record is excluded."""
        episode_df = pd.DataFrame({
            'episode_order': ['E1'],
            'start_date': pd.to_datetime(['2024-06-01']),
            'PATID': ['P001']
        })
        
        dad_df = pd.DataFrame({
            'episode_order': ['E1', 'E0'],  # E1 is current episode
            'ADMITDATE_DT': pd.to_datetime(['2024-05-25', '2024-05-20']),
            'DISDATE_DT': pd.to_datetime(['2024-05-30', '2024-05-25']),
            'PATID': ['P001', 'P001'],
            'PROCCODE1': ['CURRENT', 'PREVIOUS']
        })
        
        result = extract_proc_codes_cached(
            episode_df,
            dad_df,
            number_of_days=30,
            show_progress=False
        )
        
        codes = result.loc['E1', 'proc_codes']
        assert 'CURRENT' not in codes  # Should exclude current episode
        assert 'PREVIOUS' in codes     # Should include previous episode
    
    def test_deduplication_and_sorting(self):
        """Test that proc_codes are deduplicated and sorted."""
        episode_df = pd.DataFrame({
            'episode_order': ['E1'],
            'start_date': pd.to_datetime(['2024-06-01']),
            'PATID': ['P001']
        })
        
        dad_df = pd.DataFrame({
            'episode_order': ['D1', 'D2'],
            'ADMITDATE_DT': pd.to_datetime(['2024-05-10', '2024-05-15']),
            'DISDATE_DT': pd.to_datetime(['2024-05-15', '2024-05-20']),
            'PATID': ['P001', 'P001'],
            'PROCCODE1': ['ZZZ', 'AAA'],  # Same code in both records
            'PROCCODE2': ['AAA', 'BBB']   # AAA appears twice
        })
        
        result = extract_proc_codes_cached(
            episode_df,
            dad_df,
            number_of_days=30,
            show_progress=False
        )
        
        codes = result.loc['E1', 'proc_codes']
        assert codes == ['AAA', 'BBB', 'ZZZ']  # Deduplicated and sorted
    
    def test_empty_result(self):
        """Test when no matching records found."""
        episode_df = pd.DataFrame({
            'episode_order': ['E1'],
            'start_date': pd.to_datetime(['2024-06-01']),
            'PATID': ['P001']
        })
        
        dad_df = pd.DataFrame({
            'episode_order': ['D1'],
            'ADMITDATE_DT': pd.to_datetime(['2024-01-01']),  # Way before window
            'DISDATE_DT': pd.to_datetime(['2024-01-05']),
            'PATID': ['P001'],
            'PROCCODE1': ['PROC_A']
        })
        
        result = extract_proc_codes_cached(
            episode_df,
            dad_df,
            number_of_days=30,
            show_progress=False
        )
        
        codes = result.loc['E1', 'proc_codes']
        assert codes == []
    
    def test_output_columns(self):
        """Test that output contains expected columns."""
        result = extract_proc_codes_cached(
            self.episode_df,
            self.dad_df,
            number_of_days=60,
            show_progress=False
        )
        
        assert result.index.name == 'episode_order'
        assert 'PATID' in result.columns
        assert 'start_date' in result.columns
        assert 'proc_codes' in result.columns
    
    def test_missing_required_columns(self):
        """Test error handling for missing columns."""
        bad_episode_df = pd.DataFrame({
            'episode_order': ['E1'],
            # Missing start_date
            'PATID': ['P001']
        })
        
        with pytest.raises(ValueError) as excinfo:
            extract_proc_codes_cached(
                bad_episode_df,
                self.dad_df,
                number_of_days=30,
                show_progress=False
            )
        assert 'start_date' in str(excinfo.value)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
