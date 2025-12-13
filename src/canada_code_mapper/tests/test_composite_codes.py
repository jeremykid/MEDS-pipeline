"""
Unit tests for composite code parsing functionality.

Tests the ability to parse and lookup codes in composite format:
- DIAGNOSIS//ICD10CA//M1000
- PROCEDURE//CCI//1VG52HA
"""

import pytest
import pandas as pd
from pathlib import Path
import sys

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from canada_code_mapper import CodeMapper, MapperRegistry
from canada_code_mapper.composite import (
    parse_composite_code,
    is_composite_code,
    extract_plain_code,
    extract_system
)


class TestCompositeCodeParsing:
    """Test cases for composite code parsing"""
    
    def test_parse_diagnosis_code(self):
        """Test parsing diagnosis composite code"""
        result = parse_composite_code("DIAGNOSIS//ICD10CA//M1000")
        assert result is not None
        assert result['prefix'] == 'DIAGNOSIS'
        assert result['system'] == 'icd10ca'
        assert result['code'] == 'M1000'
    
    def test_parse_procedure_code(self):
        """Test parsing procedure composite code"""
        result = parse_composite_code("PROCEDURE//CCI//1VG52HA")
        assert result is not None
        assert result['prefix'] == 'PROCEDURE'
        assert result['system'] == 'cci'
        assert result['code'] == '1VG52HA'
    
    def test_parse_with_whitespace(self):
        """Test parsing with extra whitespace"""
        result = parse_composite_code("  DIAGNOSIS // ICD10CA // M1000  ")
        assert result is not None
        assert result['code'] == 'M1000'
    
    def test_parse_plain_code_returns_none(self):
        """Test that plain codes return None"""
        assert parse_composite_code("M1000") is None
        assert parse_composite_code("A00.0") is None
        assert parse_composite_code("1VG52HA") is None
    
    def test_parse_invalid_format(self):
        """Test invalid formats return None"""
        assert parse_composite_code("DIAGNOSIS/ICD10CA/M1000") is None  # Single slash
        assert parse_composite_code("DIAGNOSIS//M1000") is None  # Missing system
        assert parse_composite_code("//ICD10CA//M1000") is None  # Missing prefix
        assert parse_composite_code("") is None
        assert parse_composite_code(None) is None
    
    def test_is_composite_code(self):
        """Test is_composite_code helper"""
        assert is_composite_code("DIAGNOSIS//ICD10CA//M1000") is True
        assert is_composite_code("PROCEDURE//CCI//1VG52HA") is True
        assert is_composite_code("M1000") is False
        assert is_composite_code("A00.0") is False
    
    def test_extract_plain_code(self):
        """Test extracting plain code from composite"""
        assert extract_plain_code("DIAGNOSIS//ICD10CA//M1000") == "M1000"
        assert extract_plain_code("PROCEDURE//CCI//1VG52HA") == "1VG52HA"
        assert extract_plain_code("A00.0") == "A00.0"  # Plain code unchanged
    
    def test_extract_system(self):
        """Test extracting system from composite code"""
        assert extract_system("DIAGNOSIS//ICD10CA//M1000") == "icd10ca"
        assert extract_system("PROCEDURE//CCI//1VG52HA") == "cci"
        assert extract_system("A00.0") is None  # Plain code has no system
    
    def test_system_aliases(self):
        """Test various system name formats are normalized"""
        result1 = parse_composite_code("DIAGNOSIS//ICD10CA//M1000")
        result2 = parse_composite_code("DIAGNOSIS//ICD-10-CA//M1000")
        result3 = parse_composite_code("DIAGNOSIS//icd10ca//M1000")
        
        assert result1['system'] == result2['system'] == result3['system'] == 'icd10ca'


class TestCodeMapperWithComposite:
    """Test CodeMapper with composite codes"""
    
    @pytest.fixture
    def sample_icd_mapper(self):
        """Create sample ICD-10-CA mapper"""
        df = pd.DataFrame({
            'code': ['M1000', 'M1001', 'A00.0', 'A00.1'],
            'description': [
                'Idiopathic gout, unspecified site',
                'Lead-induced gout, shoulder region',
                'Cholera due to Vibrio cholerae 01, biovar cholerae',
                'Cholera due to Vibrio cholerae 01, biovar eltor'
            ]
        })
        return CodeMapper.from_dataframe(df, name='ICD-10-CA', code_type='diagnosis')
    
    @pytest.fixture
    def sample_cci_mapper(self):
        """Create sample CCI mapper"""
        df = pd.DataFrame({
            'code': ['1VG52HA', '1JE50GQOA', '1AA.50'],
            'description': [
                'Sample CCI procedure 1',
                'Sample CCI procedure 2',
                'Transplantation of heart'
            ]
        })
        return CodeMapper.from_dataframe(df, name='CCI', code_type='procedure')
    
    def test_plain_code_lookup(self, sample_icd_mapper):
        """Test that plain code lookup still works"""
        desc = sample_icd_mapper.get_description('M1000')
        assert desc == 'Idiopathic gout, unspecified site'
    
    def test_composite_code_lookup(self, sample_icd_mapper):
        """Test composite code lookup"""
        desc = sample_icd_mapper.get_description('DIAGNOSIS//ICD10CA//M1000')
        assert desc == 'Idiopathic gout, unspecified site'
    
    def test_composite_with_tab(self, sample_icd_mapper):
        """Test composite code with tab character (as in real data)"""
        desc = sample_icd_mapper.get_description('DIAGNOSIS//ICD10CA//M1000\t')
        assert desc == 'Idiopathic gout, unspecified site'
    
    def test_code_exists_plain(self, sample_icd_mapper):
        """Test code_exists with plain code"""
        assert sample_icd_mapper.code_exists('M1000') is True
        assert sample_icd_mapper.code_exists('INVALID') is False
    
    def test_code_exists_composite(self, sample_icd_mapper):
        """Test code_exists with composite code"""
        assert sample_icd_mapper.code_exists('DIAGNOSIS//ICD10CA//M1000') is True
        assert sample_icd_mapper.code_exists('DIAGNOSIS//ICD10CA//INVALID') is False
    
    def test_batch_lookup_mixed(self, sample_icd_mapper):
        """Test batch lookup with mixed plain and composite codes"""
        codes = [
            'M1000',  # Plain
            'DIAGNOSIS//ICD10CA//M1001',  # Composite
            'A00.0',  # Plain
            'DIAGNOSIS//ICD10CA//INVALID'  # Composite invalid
        ]
        descriptions = sample_icd_mapper.get_descriptions(codes, default='Unknown')
        
        assert len(descriptions) == 4
        assert descriptions[0] == 'Idiopathic gout, unspecified site'
        assert descriptions[1] == 'Lead-induced gout, shoulder region'
        assert descriptions[2] == 'Cholera due to Vibrio cholerae 01, biovar cholerae'
        assert descriptions[3] == 'Unknown'
    
    def test_statistics_with_composite(self, sample_icd_mapper):
        """Test that statistics work correctly with composite codes"""
        sample_icd_mapper.reset_stats()
        
        sample_icd_mapper.get_description('DIAGNOSIS//ICD10CA//M1000')
        sample_icd_mapper.get_description('DIAGNOSIS//ICD10CA//INVALID')
        
        stats = sample_icd_mapper.get_stats()
        assert stats['lookups'] == 2
        assert stats['hits'] == 1
        assert stats['misses'] == 1


class TestRegistryWithComposite:
    """Test MapperRegistry with composite codes and auto-routing"""
    
    @pytest.fixture
    def sample_registry(self):
        """Create registry with ICD-10-CA and CCI mappers"""
        registry = MapperRegistry()
        
        # ICD-10-CA mapper
        icd_df = pd.DataFrame({
            'code': ['M1000', 'A00.0', 'A099'],
            'description': [
                'Idiopathic gout',
                'Cholera biovar cholerae',
                'Gastroenteritis, unspecified'
            ]
        })
        icd_mapper = CodeMapper.from_dataframe(icd_df, name='ICD-10-CA')
        registry.register('icd10ca', icd_mapper)
        
        # CCI mapper
        cci_df = pd.DataFrame({
            'code': ['1VG52HA', '1JE50GQOA'],
            'description': [
                'CCI procedure 1',
                'CCI procedure 2'
            ]
        })
        cci_mapper = CodeMapper.from_dataframe(cci_df, name='CCI')
        registry.register('cci', cci_mapper)
        
        return registry
    
    def test_get_mapper_by_system(self, sample_registry):
        """Test getting mapper by system name"""
        icd_mapper = sample_registry.get_mapper_by_system('icd10ca')
        assert icd_mapper is not None
        assert icd_mapper.name == 'ICD-10-CA'
        
        cci_mapper = sample_registry.get_mapper_by_system('cci')
        assert cci_mapper is not None
        assert cci_mapper.name == 'CCI'
    
    def test_auto_route_diagnosis(self, sample_registry):
        """Test auto-routing for diagnosis composite code"""
        desc = sample_registry.get_description(
            'icd10ca',  # Fallback mapper name
            'DIAGNOSIS//ICD10CA//M1000',
            auto_route=True
        )
        assert desc == 'Idiopathic gout'
    
    def test_auto_route_procedure(self, sample_registry):
        """Test auto-routing for procedure composite code"""
        desc = sample_registry.get_description(
            'cci',  # Fallback mapper name
            'PROCEDURE//CCI//1VG52HA',
            auto_route=True
        )
        assert desc == 'CCI procedure 1'
    
    def test_auto_route_plain_code(self, sample_registry):
        """Test that plain codes use specified mapper"""
        desc = sample_registry.get_description(
            'icd10ca',
            'M1000',
            auto_route=True
        )
        assert desc == 'Idiopathic gout'
    
    def test_no_auto_route(self, sample_registry):
        """Test with auto_route disabled"""
        desc = sample_registry.get_description(
            'icd10ca',
            'DIAGNOSIS//ICD10CA//M1000',
            auto_route=False
        )
        # Should still work because mapper extracts the code portion
        assert desc == 'Idiopathic gout'
    
    def test_real_world_format(self, sample_registry):
        """Test with real-world format from AHS data"""
        codes = [
            'DIAGNOSIS//ICD10CA//M1000\t',
            'DIAGNOSIS//ICD10CA//A099\t',
            'PROCEDURE//CCI//1VG52HA\t',
            'PROCEDURE//CCI//1JE50GQOA\t'
        ]
        
        for code in codes:
            if 'DIAGNOSIS' in code:
                desc = sample_registry.get_description('icd10ca', code, auto_route=True)
            else:
                desc = sample_registry.get_description('cci', code, auto_route=True)
            
            assert desc != 'Unknown', f"Failed to find description for: {code}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
