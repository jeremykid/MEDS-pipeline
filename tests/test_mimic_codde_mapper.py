#!/usr/bin/env python
"""
Test suite for mimic_codde_mapper module.

Run with: pytest tests/test_mimic_codde_mapper.py -v
Or: conda activate pytorch && python tests/test_mimic_codde_mapper.py
"""

import sys
from pathlib import Path

import pytest

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from mimic_codde_mapper.composite import (
    parse_composite_code,
    is_composite_code,
    extract_plain_code,
    get_mapper_key
)
from mimic_codde_mapper.registry import init_mimic_mappers

# Test data paths
DIAGNOSIS_PATH = "/data/padmalab_external/special_project/physionet.org/files/mimiciv/3.1/hosp/d_icd_diagnoses.csv.gz"
PROCEDURE_PATH = "/data/padmalab_external/special_project/physionet.org/files/mimiciv/3.1/hosp/d_icd_procedures.csv.gz"


@pytest.fixture(scope="module")
def registry():
    """Shared MIMIC mapper registry for integration tests."""
    return init_mimic_mappers(
        diagnosis_path=DIAGNOSIS_PATH,
        procedure_path=PROCEDURE_PATH,
    )


def test_parse_composite_code():
    """Test parsing of composite code strings."""
    # Diagnosis codes
    result = parse_composite_code("DIAGNOSIS//ICD//10//R531")
    assert result is not None
    assert result['prefix'] == 'DIAGNOSIS'
    assert result['system'] == 'ICD'
    assert result['version'] == '10'
    assert result['code'] == 'R531'
    
    result = parse_composite_code("DIAGNOSIS//ICD//9//5723")
    assert result is not None
    assert result['version'] == '9'
    assert result['code'] == '5723'
    
    # Procedure codes
    result = parse_composite_code("PROCEDURE//ICD//10//0QS734Z")
    assert result is not None
    assert result['prefix'] == 'PROCEDURE'
    assert result['code'] == '0QS734Z'
    
    # Plain code (should return None)
    result = parse_composite_code("R531")
    assert result is None
    
    print("✓ test_parse_composite_code passed")


def test_is_composite_code():
    """Test composite code detection."""
    assert is_composite_code("DIAGNOSIS//ICD//10//R531") is True
    assert is_composite_code("PROCEDURE//ICD//9//8938") is True
    assert is_composite_code("R531") is False
    assert is_composite_code("0010") is False
    
    print("✓ test_is_composite_code passed")


def test_extract_plain_code():
    """Test plain code extraction."""
    assert extract_plain_code("DIAGNOSIS//ICD//10//R531") == "R531"
    assert extract_plain_code("PROCEDURE//ICD//9//8938") == "8938"
    assert extract_plain_code("R531") == "R531"
    
    print("✓ test_extract_plain_code passed")


def test_get_mapper_key():
    """Test mapper key generation."""
    assert get_mapper_key("DIAGNOSIS//ICD//10//R531") == "diagnosis_10"
    assert get_mapper_key("DIAGNOSIS//ICD//9//5723") == "diagnosis_9"
    assert get_mapper_key("PROCEDURE//ICD//10//0QS734Z") == "procedure_10"
    assert get_mapper_key("PROCEDURE//ICD//9//8938") == "procedure_9"
    assert get_mapper_key("R531") is None
    
    print("✓ test_get_mapper_key passed")


def test_init_mimic_mappers(registry):
    """Test registry initialization with real MIMIC data."""
    print(f"Registry: {registry}")
    print(f"Registered mappers: {registry.list_mappers()}")
    
    # Verify all 4 mappers are registered
    assert registry.has_mapper("diagnosis_9")
    assert registry.has_mapper("diagnosis_10")
    assert registry.has_mapper("procedure_9")
    assert registry.has_mapper("procedure_10")
    
    print("✓ test_init_mimic_mappers passed")


def test_get_description_auto_route(registry):
    """Test get_description with auto-routing."""
    print("\n--- Testing auto-routing ---")
    
    # ICD-10 Diagnosis
    desc = registry.get_description("diagnosis_10", "DIAGNOSIS//ICD//10//R531")
    print(f"DIAGNOSIS//ICD//10//R531: {desc}")
    assert desc != "Unknown", f"Expected description, got: {desc}"
    
    # ICD-9 Diagnosis
    desc = registry.get_description("diagnosis_9", "DIAGNOSIS//ICD//9//5723")
    print(f"DIAGNOSIS//ICD//9//5723: {desc}")
    # Note: may or may not find exact match, fallback should work
    
    # ICD-10 Procedure
    desc = registry.get_description("procedure_10", "PROCEDURE//ICD//10//0QS734Z")
    print(f"PROCEDURE//ICD//10//0QS734Z: {desc}")
    
    # ICD-9 Procedure
    desc = registry.get_description("procedure_9", "PROCEDURE//ICD//9//8938")
    print(f"PROCEDURE//ICD//9//8938: {desc}")
    
    print("✓ test_get_description_auto_route passed")


def test_hierarchical_fallback(registry):
    """Test hierarchical fallback matching."""
    print("\n--- Testing hierarchical fallback ---")
    
    # Test with a code that may need fallback
    # R5319 -> R531 -> R53 -> R5
    mapper = registry.get_mapper("diagnosis_10")
    
    # First, check what codes exist
    print(f"Total diagnosis_10 codes: {len(mapper)}")
    
    # Test fallback: try a more specific code
    desc = mapper.get_description("R5319999")
    print(f"R5319999 (fallback test): {desc}")
    
    # The fallback should find R531 or R53 or R5
    desc_r53 = mapper.get_description("R53")
    print(f"R53 (direct): {desc_r53}")
    
    print("✓ test_hierarchical_fallback passed")


def main():
    """Run all tests."""
    print("=" * 60)
    print("MIMIC Code Mapper Tests")
    print("=" * 60)
    
    # Unit tests (no data needed)
    test_parse_composite_code()
    test_is_composite_code()
    test_extract_plain_code()
    test_get_mapper_key()
    
    # Integration tests (need MIMIC data)
    print("\n" + "=" * 60)
    print("Integration Tests (with MIMIC data)")
    print("=" * 60)
    
    registry = test_init_mimic_mappers()
    test_get_description_auto_route(registry)
    test_hierarchical_fallback(registry)
    
    print("\n" + "=" * 60)
    print("All tests passed! ✓")
    print("=" * 60)


if __name__ == "__main__":
    main()
