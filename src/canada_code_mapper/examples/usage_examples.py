"""
Example usage of the code_mapper module.

This script demonstrates how to use CodeMapper and MapperRegistry
for Canadian ICD-10-CA and CCI codes.
"""

import sys
from pathlib import Path

# Add src to path if running directly
sys.path.insert(0, str(Path(__file__).parent.parent))

from canada_code_mapper import CodeMapper, MapperRegistry
from canada_code_mapper.registry import init_canadian_mappers
from canada_code_mapper.utils import (
    validate_mapping_file,
    enrich_dataframe,
    find_missing_codes
)
import pandas as pd


def example_basic_usage():
    """Example 1: Basic CodeMapper usage"""
    print("\n" + "="*60)
    print("Example 1: Basic CodeMapper Usage")
    print("="*60)
    
    # Create sample data for demonstration
    sample_data = {
        'code': ['A00.0', 'A00.1', 'A00.9', 'A01.0'],
        'description': [
            'Cholera due to Vibrio cholerae 01, biovar cholerae',
            'Cholera due to Vibrio cholerae 01, biovar eltor',
            'Cholera, unspecified',
            'Typhoid fever'
        ]
    }
    df = pd.DataFrame(sample_data)
    
    # Create mapper from DataFrame
    mapper = CodeMapper.from_dataframe(
        df,
        code_column='code',
        description_column='description',
        name='ICD-10-CA Sample',
        code_type='diagnosis'
    )
    
    print(f"\nCreated mapper with {len(mapper)} codes")
    
    # Single lookup
    code = 'A00.0'
    desc = mapper.get_description(code)
    print(f"\nCode: {code}")
    print(f"Description: {desc}")
    
    # Batch lookup
    codes = ['A00.0', 'A00.1', 'A00.9', 'INVALID']
    descriptions = mapper.get_descriptions(codes)
    print(f"\nBatch lookup results:")
    for c, d in zip(codes, descriptions):
        print(f"  {c}: {d}")
    
    # Search functionality
    print(f"\nSearching for 'cholera':")
    results = mapper.search('cholera', max_results=5)
    print(results)
    
    # Statistics
    stats = mapper.get_stats()
    print(f"\nMapper statistics:")
    print(f"  Total codes: {stats['total_codes']}")
    print(f"  Lookups: {stats['lookups']}")
    print(f"  Hits: {stats['hits']}")
    print(f"  Misses: {stats['misses']}")
    print(f"  Hit rate: {stats['hit_rate']:.2%}")


def example_registry_usage():
    """Example 2: Using MapperRegistry"""
    print("\n" + "="*60)
    print("Example 2: MapperRegistry Usage")
    print("="*60)
    
    # Create registry
    registry = MapperRegistry()
    
    # Create sample ICD data
    icd_data = pd.DataFrame({
        'code': ['A00.0', 'A00.1', 'A00.9'],
        'description': [
            'Cholera due to Vibrio cholerae 01, biovar cholerae',
            'Cholera due to Vibrio cholerae 01, biovar eltor',
            'Cholera, unspecified'
        ]
    })
    
    # Create sample CCI data
    cci_data = pd.DataFrame({
        'code': ['1.AA.50', '1.AA.51', '1.AA.52'],
        'description': [
            'Transplantation of heart',
            'Transplantation of heart and lung',
            'Transplantation of lung'
        ]
    })
    
    # Create and register mappers
    icd_mapper = CodeMapper.from_dataframe(
        icd_data, name='ICD-10-CA', code_type='diagnosis'
    )
    cci_mapper = CodeMapper.from_dataframe(
        cci_data, name='CCI', code_type='procedure'
    )
    
    registry.register('icd10ca', icd_mapper)
    registry.register('cci', cci_mapper)
    
    print(f"\nRegistered mappers: {registry.list_mappers()}")
    print(f"Registry: {registry}")
    
    # Use registry for lookups
    icd_desc = registry.get_description('icd10ca', 'A00.0')
    cci_desc = registry.get_description('cci', '1.AA.50')
    
    print(f"\nICD-10-CA lookup (A00.0): {icd_desc}")
    print(f"CCI lookup (1.AA.50): {cci_desc}")
    
    # Get all statistics
    all_stats = registry.get_all_stats()
    print(f"\nAll mapper statistics:")
    for name, stats in all_stats.items():
        print(f"  {name}: {stats['total_codes']} codes, "
              f"{stats['lookups']} lookups, "
              f"{stats['hit_rate']:.2%} hit rate")


def example_dataframe_enrichment():
    """Example 3: Enrich DataFrame with descriptions"""
    print("\n" + "="*60)
    print("Example 3: DataFrame Enrichment")
    print("="*60)
    
    # Create sample mapper
    mapper_data = pd.DataFrame({
        'code': ['A00.0', 'A00.1', 'A00.9', 'A01.0'],
        'description': [
            'Cholera due to Vibrio cholerae 01, biovar cholerae',
            'Cholera due to Vibrio cholerae 01, biovar eltor',
            'Cholera, unspecified',
            'Typhoid fever'
        ]
    })
    
    mapper = CodeMapper.from_dataframe(mapper_data, name='ICD-10-CA')
    
    # Create sample patient data
    patient_data = pd.DataFrame({
        'patient_id': [1, 2, 3, 4, 5],
        'diagnosis_code': ['A00.0', 'A00.1', 'A00.9', 'A01.0', 'INVALID'],
        'date': pd.date_range('2024-01-01', periods=5)
    })
    
    print("\nOriginal DataFrame:")
    print(patient_data)
    
    # Enrich with descriptions
    enriched_df = enrich_dataframe(
        patient_data,
        code_column='diagnosis_code',
        mapper=mapper,
        description_column='diagnosis_description'
    )
    
    print("\nEnriched DataFrame:")
    print(enriched_df)
    
    # Find missing codes
    missing_df = find_missing_codes(
        patient_data,
        code_column='diagnosis_code',
        mapper=mapper
    )
    
    print(f"\nMissing codes:")
    print(missing_df)


def example_ahs_integration():
    """Example 4: Integration with AHS pipeline"""
    print("\n" + "="*60)
    print("Example 4: AHS Pipeline Integration")
    print("="*60)
    
    # Simulate AHS component using code mappers
    class AHSDiagnosisComponent:
        def __init__(self, icd_mapper):
            self.icd_mapper = icd_mapper
        
        def process(self, df):
            """Add diagnosis descriptions to dataframe"""
            df = df.copy()
            df['diagnosis_description'] = df['diagnosis_code'].apply(
                lambda code: self.icd_mapper.get_description(code)
            )
            return df
    
    class AHSProcedureComponent:
        def __init__(self, cci_mapper):
            self.cci_mapper = cci_mapper
        
        def process(self, df):
            """Add procedure descriptions to dataframe"""
            df = df.copy()
            df['procedure_description'] = df['procedure_code'].apply(
                lambda code: self.cci_mapper.get_description(code)
            )
            return df
    
    # Create sample mappers
    icd_data = pd.DataFrame({
        'code': ['A00.0', 'A00.1', 'A00.9'],
        'description': [
            'Cholera due to Vibrio cholerae 01, biovar cholerae',
            'Cholera due to Vibrio cholerae 01, biovar eltor',
            'Cholera, unspecified'
        ]
    })
    
    cci_data = pd.DataFrame({
        'code': ['1.AA.50', '1.AA.51'],
        'description': [
            'Transplantation of heart',
            'Transplantation of heart and lung'
        ]
    })
    
    icd_mapper = CodeMapper.from_dataframe(icd_data, name='ICD-10-CA')
    cci_mapper = CodeMapper.from_dataframe(cci_data, name='CCI')
    
    # Initialize components
    diagnosis_component = AHSDiagnosisComponent(icd_mapper)
    procedure_component = AHSProcedureComponent(cci_mapper)
    
    # Process diagnosis data
    diagnosis_df = pd.DataFrame({
        'patient_id': [1, 2, 3],
        'diagnosis_code': ['A00.0', 'A00.1', 'A00.9']
    })
    
    print("\nProcessing diagnosis data:")
    print("Input:")
    print(diagnosis_df)
    
    diagnosis_result = diagnosis_component.process(diagnosis_df)
    print("\nOutput:")
    print(diagnosis_result)
    
    # Process procedure data
    procedure_df = pd.DataFrame({
        'patient_id': [1, 2],
        'procedure_code': ['1.AA.50', '1.AA.51']
    })
    
    print("\nProcessing procedure data:")
    print("Input:")
    print(procedure_df)
    
    procedure_result = procedure_component.process(procedure_df)
    print("\nOutput:")
    print(procedure_result)


def main():
    """Run all examples"""
    print("\n" + "="*60)
    print("CODE MAPPER EXAMPLES")
    print("="*60)
    
    example_basic_usage()
    example_registry_usage()
    example_dataframe_enrichment()
    example_ahs_integration()
    
    print("\n" + "="*60)
    print("All examples completed!")
    print("="*60 + "\n")


if __name__ == "__main__":
    main()
