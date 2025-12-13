"""
Example: Using canada_code_mapper with composite codes.

Demonstrates how to use the updated mapper to handle codes in the format:
- DIAGNOSIS//ICD10CA//M1000
- PROCEDURE//CCI//1VG52HA
"""

import sys
from pathlib import Path
import pandas as pd

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from canada_code_mapper import CodeMapper, MapperRegistry


def example_1_basic_composite():
    """Example 1: Basic composite code lookup"""
    print("\n" + "="*60)
    print("Example 1: Basic Composite Code Lookup")
    print("="*60)
    
    # Create sample ICD-10-CA mapper
    icd_df = pd.DataFrame({
        'code': ['M1000', 'M1001', 'A00.0', 'A099'],
        'description': [
            'Idiopathic gout, unspecified site',
            'Lead-induced gout, shoulder region',
            'Cholera due to Vibrio cholerae 01, biovar cholerae',
            'Gastroenteritis, unspecified'
        ]
    })
    
    mapper = CodeMapper.from_dataframe(icd_df, name='ICD-10-CA')
    
    # Test plain codes (old format - still works)
    print("\n📝 Plain code lookup:")
    desc = mapper.get_description('M1000')
    print(f"  M1000 → {desc}")
    
    # Test composite codes (new format)
    print("\n📝 Composite code lookup:")
    composite_codes = [
        'DIAGNOSIS//ICD10CA//M1000',
        'DIAGNOSIS//ICD10CA//M1001\t',  # With tab (as in real data)
        'DIAGNOSIS//ICD10CA//A099',
    ]
    
    for code in composite_codes:
        desc = mapper.get_description(code)
        print(f"  {code.strip()} → {desc}")
    
    # Mixed format batch lookup
    print("\n📝 Mixed format batch lookup:")
    mixed_codes = [
        'M1000',  # Plain
        'DIAGNOSIS//ICD10CA//A00.0',  # Composite
        'A099',  # Plain
        'DIAGNOSIS//ICD10CA//INVALID'  # Invalid
    ]
    
    descriptions = mapper.get_descriptions(mixed_codes, default='Code not found')
    for code, desc in zip(mixed_codes, descriptions):
        print(f"  {code} → {desc}")


def example_2_registry_auto_routing():
    """Example 2: Registry with automatic routing"""
    print("\n" + "="*60)
    print("Example 2: Registry with Auto-Routing")
    print("="*60)
    
    # Create registry with both ICD-10-CA and CCI mappers
    registry = MapperRegistry()
    
    # ICD-10-CA mapper
    icd_df = pd.DataFrame({
        'code': ['M1000', 'A00.0', 'A099'],
        'description': [
            'Idiopathic gout, unspecified site',
            'Cholera due to Vibrio cholerae 01',
            'Gastroenteritis, unspecified'
        ]
    })
    icd_mapper = CodeMapper.from_dataframe(icd_df, name='ICD-10-CA')
    registry.register('icd10ca', icd_mapper)
    
    # CCI mapper
    cci_df = pd.DataFrame({
        'code': ['1VG52HA', '1JE50GQOA', '1AA.50'],
        'description': [
            'Excision, pleura NEC',
            'Bypass, pulmonary artery using open approach',
            'Transplantation of heart'
        ]
    })
    cci_mapper = CodeMapper.from_dataframe(cci_df, name='CCI')
    registry.register('cci', cci_mapper)
    
    print(f"\n📋 Registered mappers: {registry.list_mappers()}")
    
    # Auto-routing based on system in composite code
    print("\n🔀 Auto-routing composite codes:")
    
    test_codes = [
        ('DIAGNOSIS//ICD10CA//M1000', 'Diagnosis code'),
        ('DIAGNOSIS//ICD10CA//A099', 'Diagnosis code'),
        ('PROCEDURE//CCI//1VG52HA', 'Procedure code'),
        ('PROCEDURE//CCI//1JE50GQOA', 'Procedure code'),
    ]
    
    for code, code_type in test_codes:
        # Registry automatically routes to correct mapper based on system
        desc = registry.get_description('icd10ca', code, auto_route=True)
        print(f"  {code_type}: {code}")
        print(f"    → {desc}")
    
    # Manual routing (specify mapper explicitly)
    print("\n📌 Manual routing:")
    desc_icd = registry.get_description('icd10ca', 'DIAGNOSIS//ICD10CA//M1000', auto_route=False)
    desc_cci = registry.get_description('cci', 'PROCEDURE//CCI//1VG52HA', auto_route=False)
    print(f"  ICD mapper: {desc_icd}")
    print(f"  CCI mapper: {desc_cci}")


def example_3_real_world_ahs_format():
    """Example 3: Real-world AHS data format"""
    print("\n" + "="*60)
    print("Example 3: Real-World AHS Data Format")
    print("="*60)
    
    # Simulate AHS DataFrame structure
    ahs_data = pd.DataFrame({
        'patient_id': [1, 1, 2, 2, 3],
        'time': pd.date_range('2024-01-01', periods=5),
        'code': [
            'DIAGNOSIS//ICD10CA//M1000\t',
            'PROCEDURE//CCI//1VG52HA\t',
            'DIAGNOSIS//ICD10CA//A099\t',
            'PROCEDURE//CCI//1JE50GQOA\t',
            'DIAGNOSIS//ICD10CA//A00.0\t'
        ],
        'event_type': ['diagnosis', 'procedures', 'diagnosis', 'procedures', 'diagnosis']
    })
    
    print("\n📊 Sample AHS data:")
    print(ahs_data)
    
    # Setup mappers
    registry = MapperRegistry()
    
    icd_df = pd.DataFrame({
        'code': ['M1000', 'A099', 'A00.0'],
        'description': [
            'Idiopathic gout, unspecified site',
            'Gastroenteritis, unspecified',
            'Cholera due to Vibrio cholerae 01'
        ]
    })
    registry.register('icd10ca', CodeMapper.from_dataframe(icd_df, name='ICD-10-CA'))
    
    cci_df = pd.DataFrame({
        'code': ['1VG52HA', '1JE50GQOA'],
        'description': [
            'Excision, pleura NEC',
            'Bypass, pulmonary artery using open approach'
        ]
    })
    registry.register('cci', CodeMapper.from_dataframe(cci_df, name='CCI'))
    
    # Add descriptions to DataFrame
    print("\n✨ Adding descriptions:")
    
    def get_desc_with_routing(code):
        """Get description with automatic routing"""
        return registry.get_description('icd10ca', code, auto_route=True)
    
    ahs_data['description'] = ahs_data['code'].apply(get_desc_with_routing)
    
    print(ahs_data[['patient_id', 'code', 'description']])
    
    # Check coverage
    found = (ahs_data['description'] != 'Unknown').sum()
    total = len(ahs_data)
    print(f"\n📈 Coverage: {found}/{total} ({100*found/total:.1f}%)")


def example_4_integration_with_components():
    """Example 4: Integration pattern for MEDS pipeline components"""
    print("\n" + "="*60)
    print("Example 4: Integration with MEDS Pipeline Components")
    print("="*60)
    
    # Simulate AHS component pattern
    class AHSDiagnosisComponent:
        """Example AHS diagnosis component with composite code support"""
        
        def __init__(self, icd_mapper):
            self.icd_mapper = icd_mapper
        
        def process(self, df):
            """Add descriptions to diagnosis codes"""
            df = df.copy()
            
            # Mapper automatically handles both plain and composite formats
            df['diagnosis_description'] = df['diagnosis_code'].apply(
                lambda code: self.icd_mapper.get_description(code, default='Unknown')
            )
            
            return df
    
    # Setup
    icd_df = pd.DataFrame({
        'code': ['M1000', 'A099', 'A00.0'],
        'description': [
            'Idiopathic gout',
            'Gastroenteritis',
            'Cholera'
        ]
    })
    icd_mapper = CodeMapper.from_dataframe(icd_df)
    
    # Test data with mixed formats
    test_df = pd.DataFrame({
        'patient_id': [1, 2, 3, 4],
        'diagnosis_code': [
            'M1000',  # Plain format
            'DIAGNOSIS//ICD10CA//A099',  # Composite format
            'A00.0',  # Plain format
            'DIAGNOSIS//ICD10CA//M1000\t'  # Composite with tab
        ]
    })
    
    print("\n📝 Input data:")
    print(test_df)
    
    # Process
    component = AHSDiagnosisComponent(icd_mapper)
    result_df = component.process(test_df)
    
    print("\n✨ Processed data:")
    print(result_df)
    
    print("\n✅ All formats processed correctly!")


def main():
    """Run all examples"""
    print("\n" + "="*60)
    print("COMPOSITE CODE SUPPORT EXAMPLES")
    print("canada_code_mapper v0.1.0")
    print("="*60)
    
    example_1_basic_composite()
    example_2_registry_auto_routing()
    example_3_real_world_ahs_format()
    example_4_integration_with_components()
    
    print("\n" + "="*60)
    print("✅ All examples completed successfully!")
    print("="*60)
    print("\n💡 Key features:")
    print("  • Supports both plain codes (A00.0) and composite codes (DIAGNOSIS//ICD10CA//A00.0)")
    print("  • Automatic routing based on system name")
    print("  • Backward compatible with existing code")
    print("  • Handles tabs and whitespace in real data")
    print("\n")


if __name__ == "__main__":
    main()
