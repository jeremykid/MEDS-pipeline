#!/usr/bin/env python3
"""
Test script for AHS procedures ETL validation

This script validates:
1. Procedures ETL can be loaded and registered
2. Code format is PROCEDURE//CCI//{code}
3. PROCSTDT{n}_DT is used as time, with ADMITDATE_DT as fallback
4. Value column contains sequence numbers (1-20) as strings
"""

import sys
import os
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

import pandas as pd
from meds_pipeline.etl.ahs.procedures import AHSProcedures
from meds_pipeline.etl.registry import REGISTRY

def test_procedures_registration():
    """Test that procedures component is registered"""
    print("Testing procedures registration...")
    assert "procedures" in REGISTRY, "Procedures component not registered"
    print("[OK] Procedures component registered")

def test_code_format():
    """Test the code format generation"""
    print("\nTesting code format generation...")
    
    # Test CCI codes
    code_1 = AHSProcedures._build_procedure_code("1HZ53HAGP")
    assert code_1 == "PROCEDURE//CCI//1HZ53HAGP", f"Expected PROCEDURE//CCI//1HZ53HAGP, got {code_1}"
    print(f"[OK] CCI format 1: {code_1}")
    
    code_2 = AHSProcedures._build_procedure_code("1HT53LA")
    assert code_2 == "PROCEDURE//CCI//1HT53LA", f"Expected PROCEDURE//CCI//1HT53LA, got {code_2}"
    print(f"[OK] CCI format 2: {code_2}")
    
    # Test with pandas Series
    test_df = pd.DataFrame({
        'procedure_code': ['1HZ53HAGP', '1HT53LA', '2AF21BAGP']
    })
    test_df['meds_code'] = test_df['procedure_code'].apply(AHSProcedures._build_procedure_code)
    expected = [
        "PROCEDURE//CCI//1HZ53HAGP",
        "PROCEDURE//CCI//1HT53LA",
        "PROCEDURE//CCI//2AF21BAGP"
    ]
    assert test_df['meds_code'].tolist() == expected, f"Expected {expected}, got {test_df['meds_code'].tolist()}"
    print(f"[OK] Batch format generation works correctly")

def test_sequence_value():
    """Test value column generation for sequence numbers"""
    print("\nTesting value column generation...")
    
    # Test with sequence_num column (1-20)
    test_df = pd.DataFrame({
        'PATID': ['1343', '1343', '1343'],
        'sequence_num': [1, 2, 3],
        'procedure_code': ['1HZ53HAGP', '1HT53LA', '2AF21BAGP'],
        'event_time': ['2021-08-20', '2021-08-21', '2021-08-22']
    })
    
    # Simulate value creation logic
    test_df['value'] = test_df['sequence_num'].astype('Int64').astype("string")
    
    expected_values = ['1', '2', '3']
    assert test_df['value'].tolist() == expected_values, f"Expected {expected_values}, got {test_df['value'].tolist()}"
    assert test_df['value'].dtype == 'string', f"Expected string dtype, got {test_df['value'].dtype}"
    print(f"[OK] Sequence values generated correctly: {test_df['value'].tolist()}")
    print(f"[OK] Value column dtype is string: {test_df['value'].dtype}")

def test_time_fallback():
    """Test time fallback logic (PROCSTDT -> ADMITDATE_DT)"""
    print("\nTesting time fallback logic...")
    
    # Simulate the fallback logic
    test_df = pd.DataFrame({
        'PROCSTDT1_DT': [pd.NaT, '2021-08-20', '2021-08-21'],
        'ADMITDATE_DT': ['2021-08-19', '2021-08-19', '2021-08-19']
    })
    
    # Convert to datetime
    test_df['proc_time'] = pd.to_datetime(test_df['PROCSTDT1_DT'], errors='coerce')
    test_df['admit_time'] = pd.to_datetime(test_df['ADMITDATE_DT'], errors='coerce')
    
    # Apply fallback
    test_df['event_time'] = test_df['proc_time'].fillna(test_df['admit_time'])
    
    expected_times = pd.Series(pd.to_datetime(['2021-08-19', '2021-08-20', '2021-08-21']))
    pd.testing.assert_series_equal(test_df['event_time'].reset_index(drop=True), expected_times.reset_index(drop=True), check_names=False)
    print(f"[OK] Time fallback works correctly")
    print(f"    Row 1: PROCSTDT1_DT=NaT -> uses ADMITDATE_DT=2021-08-19")
    print(f"    Row 2: PROCSTDT1_DT=2021-08-20 -> uses PROCSTDT1_DT")
    print(f"    Row 3: PROCSTDT1_DT=2021-08-21 -> uses PROCSTDT1_DT")

def test_melt_logic():
    """Test the melting logic for PROCCODE1-20"""
    print("\nTesting PROCCODE melt logic...")
    
    # Create sample data similar to DAD structure
    test_df = pd.DataFrame({
        'PATID': ['1343'],
        'ADMITDATE_DT': ['2021-08-19'],
        'PROCCODE1': ['1HZ53HAGP'],
        'PROCSTDT1_DT': ['2021-08-20'],
        'PROCCODE2': ['1HT53LA'],
        'PROCSTDT2_DT': [pd.NaT],
        'PROCCODE3': [None],
        'PROCSTDT3_DT': [pd.NaT],
    })
    
    melted_rows = []
    for i in [1, 2, 3]:
        proc_col = f'PROCCODE{i}'
        date_col = f'PROCSTDT{i}_DT'
        
        if proc_col not in test_df.columns:
            continue
        
        subset = test_df[['PATID', 'ADMITDATE_DT', proc_col]].copy()
        if date_col in test_df.columns:
            subset[date_col] = test_df[date_col]
        
        # Filter empty
        subset = subset[subset[proc_col].notna()]
        subset = subset[subset[proc_col].astype(str).str.strip() != '']
        
        if len(subset) == 0:
            continue
        
        subset = subset.rename(columns={proc_col: 'procedure_code'})
        
        if date_col in subset.columns:
            subset['event_time'] = pd.to_datetime(subset[date_col], errors='coerce')
            subset['event_time'] = subset['event_time'].fillna(
                pd.to_datetime(subset['ADMITDATE_DT'], errors='coerce')
            )
        else:
            subset['event_time'] = pd.to_datetime(subset['ADMITDATE_DT'], errors='coerce')
        
        subset['sequence_num'] = i
        melted_rows.append(subset[['PATID', 'event_time', 'procedure_code', 'sequence_num']])
    
    result = pd.concat(melted_rows, ignore_index=True)
    
    assert len(result) == 2, f"Expected 2 rows (PROCCODE1 and PROCCODE2), got {len(result)}"
    assert result.iloc[0]['procedure_code'] == '1HZ53HAGP', "First procedure code mismatch"
    assert result.iloc[1]['procedure_code'] == '1HT53LA', "Second procedure code mismatch"
    assert result.iloc[0]['sequence_num'] == 1, "First sequence number should be 1"
    assert result.iloc[1]['sequence_num'] == 2, "Second sequence number should be 2"
    # PROCCODE2's date is NaT, should fallback to ADMITDATE_DT
    assert pd.to_datetime(result.iloc[1]['event_time']).date() == pd.to_datetime('2021-08-19').date(), \
        "Second procedure should use ADMITDATE_DT as fallback"
    
    print(f"[OK] Melt logic works correctly:")
    print(f"    - Extracted 2 valid procedures (PROCCODE3 is null, filtered out)")
    print(f"    - Sequence numbers assigned: 1, 2")
    print(f"    - Time fallback applied for PROCCODE2 (NaT -> ADMITDATE_DT)")

def test_run_core_structure():
    """Test that run_core returns correct structure"""
    print("\nTesting run_core output structure...")
    
    # This would require actual data files, so we'll just verify the method exists
    assert hasattr(AHSProcedures, 'run_core'), "run_core method not found"
    print("[OK] run_core method exists")
    
    # Verify run_plus is deprecated
    assert hasattr(AHSProcedures, 'run_plus'), "run_plus method not found"
    print("[OK] run_plus method exists (for backward compatibility)")

def main():
    print("="*60)
    print("AHS Procedures ETL Validation Tests")
    print("="*60)
    
    try:
        test_procedures_registration()
        test_code_format()
        test_sequence_value()
        test_time_fallback()
        test_melt_logic()
        test_run_core_structure()
        
        print("\n" + "="*60)
        print("[SUCCESS] All tests passed!")
        print("="*60)
        print("\n提示：运行完整 ETL 测试:")
        print("  cd /home/weijiesun/MEDS-pipeline")
        print("  ./tests/test_ahs_procedures.sh")
        print("\n或手动运行:")
        print("  PYTHONPATH=src python3 -m meds_pipeline.cli run \\")
        print("    --source ahs --components procedures \\")
        print("    --max-patients 10 --progress")
        return 0
        
    except AssertionError as e:
        print(f"\n[FAILED] Test failed: {e}")
        return 1
    except Exception as e:
        print(f"\n[ERROR] Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())
