"""
Unit tests for code_mapper module.

Run with: python -m pytest test_code_mapper.py
"""

import pytest
import pandas as pd
from pathlib import Path
import sys

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from canada_code_mapper import CodeMapper, MapperRegistry
from canada_code_mapper.utils import (
    enrich_dataframe,
    find_missing_codes,
    merge_mappers
)


@pytest.fixture
def sample_mapping_data():
    """Create sample mapping data for testing"""
    return pd.DataFrame({
        'code': ['A00.0', 'A00.1', 'A00.9', 'A01.0'],
        'description': [
            'Cholera due to Vibrio cholerae 01, biovar cholerae',
            'Cholera due to Vibrio cholerae 01, biovar eltor',
            'Cholera, unspecified',
            'Typhoid fever'
        ]
    })


@pytest.fixture
def sample_mapper(sample_mapping_data):
    """Create a sample CodeMapper"""
    return CodeMapper.from_dataframe(
        sample_mapping_data,
        code_column='code',
        description_column='description',
        name='TestMapper'
    )


class TestCodeMapper:
    """Test cases for CodeMapper class"""
    
    def test_initialization(self, sample_mapper):
        """Test mapper initialization"""
        assert len(sample_mapper) == 4
        assert sample_mapper.name == 'TestMapper'
    
    def test_get_description_exists(self, sample_mapper):
        """Test getting description for existing code"""
        desc = sample_mapper.get_description('A00.0')
        assert desc == 'Cholera due to Vibrio cholerae 01, biovar cholerae'
    
    def test_get_description_not_exists(self, sample_mapper):
        """Test getting description for non-existing code"""
        desc = sample_mapper.get_description('INVALID', default='Not Found')
        assert desc == 'Not Found'
    
    def test_get_descriptions_batch(self, sample_mapper):
        """Test batch lookup"""
        codes = ['A00.0', 'A00.1', 'INVALID']
        descriptions = sample_mapper.get_descriptions(codes, default='Unknown')
        
        assert len(descriptions) == 3
        assert descriptions[0] == 'Cholera due to Vibrio cholerae 01, biovar cholerae'
        assert descriptions[1] == 'Cholera due to Vibrio cholerae 01, biovar eltor'
        assert descriptions[2] == 'Unknown'
    
    def test_code_exists(self, sample_mapper):
        """Test code existence check"""
        assert sample_mapper.code_exists('A00.0') == True
        assert sample_mapper.code_exists('INVALID') == False
    
    def test_get_codes(self, sample_mapper):
        """Test getting all codes"""
        codes = sample_mapper.get_codes()
        assert len(codes) == 4
        assert 'A00.0' in codes
    
    def test_search(self, sample_mapper):
        """Test search functionality"""
        results = sample_mapper.search('cholera')
        assert len(results) > 0
        assert 'cholera' in results['description'].iloc[0].lower()
    
    def test_statistics(self, sample_mapper):
        """Test statistics tracking"""
        # Reset stats
        sample_mapper.reset_stats()
        
        # Perform lookups
        sample_mapper.get_description('A00.0')
        sample_mapper.get_description('A00.1')
        sample_mapper.get_description('INVALID')
        
        stats = sample_mapper.get_stats()
        assert stats['lookups'] == 3
        assert stats['hits'] == 2
        assert stats['misses'] == 1
        assert stats['hit_rate'] == pytest.approx(2/3)
    
    def test_dict_like_access(self, sample_mapper):
        """Test dictionary-like access"""
        desc = sample_mapper['A00.0']
        assert desc == 'Cholera due to Vibrio cholerae 01, biovar cholerae'


class TestMapperRegistry:
    """Test cases for MapperRegistry"""
    
    def test_registry_initialization(self):
        """Test registry initialization"""
        registry = MapperRegistry()
        assert len(registry) == 0
    
    def test_register_mapper(self, sample_mapper):
        """Test registering a mapper"""
        registry = MapperRegistry()
        registry.register('test', sample_mapper)
        
        assert registry.has_mapper('test')
        assert len(registry) == 1
    
    def test_get_mapper(self, sample_mapper):
        """Test getting registered mapper"""
        registry = MapperRegistry()
        registry.register('test', sample_mapper)
        
        retrieved = registry.get_mapper('test')
        assert retrieved == sample_mapper
    
    def test_get_mapper_not_found(self):
        """Test getting non-existent mapper"""
        registry = MapperRegistry()
        
        with pytest.raises(KeyError):
            registry.get_mapper('nonexistent')
    
    def test_list_mappers(self, sample_mapper):
        """Test listing mappers"""
        registry = MapperRegistry()
        registry.register('mapper1', sample_mapper)
        registry.register('mapper2', sample_mapper)
        
        mappers = registry.list_mappers()
        assert len(mappers) == 2
        assert 'mapper1' in mappers
        assert 'mapper2' in mappers
    
    def test_remove_mapper(self, sample_mapper):
        """Test removing a mapper"""
        registry = MapperRegistry()
        registry.register('test', sample_mapper)
        
        assert registry.has_mapper('test')
        registry.remove_mapper('test')
        assert not registry.has_mapper('test')
    
    def test_get_description_via_registry(self, sample_mapper):
        """Test getting description through registry"""
        registry = MapperRegistry()
        registry.register('test', sample_mapper)
        
        desc = registry.get_description('test', 'A00.0')
        assert desc == 'Cholera due to Vibrio cholerae 01, biovar cholerae'


class TestUtils:
    """Test cases for utility functions"""
    
    def test_enrich_dataframe(self, sample_mapper):
        """Test DataFrame enrichment"""
        df = pd.DataFrame({
            'patient_id': [1, 2, 3],
            'diagnosis_code': ['A00.0', 'A00.1', 'INVALID']
        })
        
        enriched = enrich_dataframe(
            df,
            code_column='diagnosis_code',
            mapper=sample_mapper,
            description_column='description'
        )
        
        assert 'description' in enriched.columns
        assert enriched['description'].iloc[0] == 'Cholera due to Vibrio cholerae 01, biovar cholerae'
        assert enriched['description'].iloc[2] == 'Unknown'
    
    def test_find_missing_codes(self, sample_mapper):
        """Test finding missing codes"""
        df = pd.DataFrame({
            'code': ['A00.0', 'A00.1', 'INVALID1', 'INVALID2']
        })
        
        missing = find_missing_codes(
            df,
            code_column='code',
            mapper=sample_mapper,
            return_dataframe=False
        )
        
        assert len(missing) == 2
        assert 'INVALID1' in missing
        assert 'INVALID2' in missing
    
    def test_merge_mappers(self, sample_mapping_data):
        """Test merging mappers"""
        mapper1 = CodeMapper.from_dataframe(
            sample_mapping_data.iloc[:2],
            name='Mapper1'
        )
        mapper2 = CodeMapper.from_dataframe(
            sample_mapping_data.iloc[2:],
            name='Mapper2'
        )
        
        merged = merge_mappers(mapper1, mapper2, name='Merged')
        
        assert len(merged) == 4
        assert merged.name == 'Merged'


def test_from_dataframe(sample_mapping_data):
    """Test creating mapper from DataFrame"""
    mapper = CodeMapper.from_dataframe(
        sample_mapping_data,
        code_column='code',
        description_column='description'
    )
    
    assert len(mapper) == 4
    assert mapper.code_exists('A00.0')


def test_invalid_columns():
    """Test with invalid column names"""
    df = pd.DataFrame({
        'wrong_column': ['A00.0'],
        'description': ['Test']
    })
    
    with pytest.raises(ValueError):
        CodeMapper.from_dataframe(
            df,
            code_column='code',  # This doesn't exist
            description_column='description'
        )


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
