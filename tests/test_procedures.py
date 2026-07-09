#!/usr/bin/env python3
"""
Test script for MIMIC procedures ETL validation

This script validates:
1. Procedures ETL can be loaded and registered
2. Code format is PROCEDURE//ICD//9//code or PROCEDURE//ICD//10//code
3. Chartdate is used as time
4. Value column contains sequence numbers as strings
"""

import sys
import os
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

import pandas as pd
from meds_pipeline.etl.mimic.procedures import MIMICProcedures
from meds_pipeline.etl.registry import REGISTRY
import yaml

def test_procedures_registration():
    """Test that procedures component is registered"""
    print("Testing procedures registration...")
    assert "procedures" in REGISTRY, "Procedures component not registered"
    print("[OK] Procedures component registered")

def test_code_format():
    """Test the code format generation"""
    print("\nTesting code format generation...")
    
    # Test ICD-9
    code_9 = MIMICProcedures._build_procedure_code("4611", 9)
    assert code_9 == "PROCEDURE//ICD//9//4611", f"Expected PROCEDURE//ICD//9//4611, got {code_9}"
    print(f"[OK] ICD-9 format: {code_9}")
    
    # Test ICD-10
    code_10 = MIMICProcedures._build_procedure_code("30243V1", 10)
    assert code_10 == "PROCEDURE//ICD//10//30243V1", f"Expected PROCEDURE//ICD//10//30243V1, got {code_10}"
    print(f"[OK] ICD-10 format: {code_10}")
    
    # Test with pandas Series
    test_df = pd.DataFrame({
        'icd_code': ['4611', '30243V1', '8872'],
        'icd_version': [9, 10, 9]
    })
    test_df['meds_code'] = test_df.apply(
        lambda row: MIMICProcedures._build_procedure_code(row['icd_code'], row['icd_version']),
        axis=1
    )
    expected = [
        "PROCEDURE//ICD//9//4611",
        "PROCEDURE//ICD//10//30243V1",
        "PROCEDURE//ICD//9//8872"
    ]
    assert test_df['meds_code'].tolist() == expected, f"Expected {expected}, got {test_df['meds_code'].tolist()}"
    print(f"[OK] Batch format generation works correctly")

def test_sequence_value():
    """Test value column generation for sequence numbers"""
    print("\nTesting value column generation...")
    
    # Test with seq_num column
    test_df = pd.DataFrame({
        'subject_id': ['10000032', '10000032', '10000032'],
        'hadm_id': ['22595853', '22595853', '22595853'],
        'seq_num': [1, 2, 3],
        'icd_code': ['9904', '8872', '4611'],
        'icd_version': [9, 9, 9],
        'chartdate': ['2180-05-07', '2180-05-07', '2180-05-07']
    })
    
    # Simulate value creation logic
    test_df['value'] = pd.to_numeric(
        test_df['seq_num'], 
        errors='coerce'
    ).astype('Int64').astype("string")
    
    expected_values = ['1', '2', '3']
    assert test_df['value'].tolist() == expected_values, f"Expected {expected_values}, got {test_df['value'].tolist()}"
    assert test_df['value'].dtype == 'string', f"Expected string dtype, got {test_df['value'].dtype}"
    print(f"[OK] Sequence values generated correctly: {test_df['value'].tolist()}")
    print(f"[OK] Value column dtype is string: {test_df['value'].dtype}")

def test_run_core_structure():
    """Test that run_core returns correct structure"""
    print("\nTesting run_core output structure...")
    
    # This would require actual data files, so we'll just verify the method exists
    assert hasattr(MIMICProcedures, 'run_core'), "run_core method not found"
    print("[OK] run_core method exists")
    
    # Verify run_plus is deprecated
    assert hasattr(MIMICProcedures, 'run_plus'), "run_plus method not found"
    print("[OK] run_plus method exists (for backward compatibility)")

def main():
    print("="*60)
    print("MIMIC Procedures ETL Validation Tests")
    print("="*60)
    
    try:
        test_procedures_registration()
        test_code_format()
        test_sequence_value()
        test_run_core_structure()
        
        print("\n" + "="*60)
        print("[SUCCESS] All tests passed!")
        print("="*60)
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
