"""
Quick start script for testing code_mapper with your ICD-10-CA and CCI files.

Usage:
    python quickstart.py --icd path/to/icd10ca.txt --cci path/to/cci.txt
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from canada_code_mapper import CodeMapper, MapperRegistry
from canada_code_mapper.utils import validate_mapping_file
import pandas as pd


def validate_files(icd_path, cci_path):
    """Validate the mapping files"""
    print("\n" + "="*60)
    print("📋 VALIDATING MAPPING FILES")
    print("="*60)
    
    if icd_path:
        print(f"\n🔍 Validating ICD-10-CA file: {icd_path}")
        icd_result = validate_mapping_file(
            icd_path,
            code_column="code",
            description_column="description",
            delimiter="|"
        )
        
        if icd_result['valid']:
            print(f"   ✅ Valid! Found {icd_result['unique_codes']} unique codes")
            print(f"   📊 Total rows: {icd_result['total_rows']}")
            print(f"   🔄 Duplicates: {icd_result['duplicate_codes']}")
            print(f"   ❌ Null codes: {icd_result['null_codes']}")
            print(f"   ❌ Null descriptions: {icd_result['null_descriptions']}")
            print(f"\n   Sample entries:")
            for entry in icd_result['sample'][:3]:
                print(f"      {entry['code']}: {entry['description'][:60]}...")
        else:
            print(f"   ❌ Invalid: {icd_result.get('error', 'Unknown error')}")
            return False
    
    if cci_path:
        print(f"\n🔍 Validating CCI file: {cci_path}")
        cci_result = validate_mapping_file(
            cci_path,
            code_column="code",
            description_column="description",
            delimiter=","
        )
        
        if cci_result['valid']:
            print(f"   ✅ Valid! Found {cci_result['unique_codes']} unique codes")
            print(f"   📊 Total rows: {cci_result['total_rows']}")
            print(f"   🔄 Duplicates: {cci_result['duplicate_codes']}")
            print(f"   ❌ Null codes: {cci_result['null_codes']}")
            print(f"   ❌ Null descriptions: {cci_result['null_descriptions']}")
            print(f"\n   Sample entries:")
            for entry in cci_result['sample'][:3]:
                print(f"      {entry['code']}: {entry['description'][:60]}...")
        else:
            print(f"   ❌ Invalid: {cci_result.get('error', 'Unknown error')}")
            return False
    
    return True


def test_mappers(icd_path, cci_path):
    """Test the mappers with sample queries"""
    print("\n" + "="*60)
    print("🧪 TESTING MAPPERS")
    print("="*60)
    
    registry = MapperRegistry()
    
    # Load ICD-10-CA
    if icd_path:
        print(f"\n📖 Loading ICD-10-CA mapper from {icd_path}")
        registry.register_from_file(
            name="icd10ca",
            file_path=icd_path,
            code_column="code",
            description_column="description",
            delimiter="|",
            code_type="diagnosis"
        )
        icd_mapper = registry.get_mapper("icd10ca")
        print(f"   ✅ Loaded {len(icd_mapper)} ICD-10-CA codes")
        
        # Test some common codes
        test_codes = ["A00.0", "A00.1", "I21.0", "I21.1", "E11.9"]
        print(f"\n   🔍 Testing {len(test_codes)} sample codes:")
        for code in test_codes:
            desc = icd_mapper.get_description(code, default="Not found")
            status = "✅" if desc != "Not found" else "❌"
            print(f"      {status} {code}: {desc[:70]}")
    
    # Load CCI
    if cci_path:
        print(f"\n📖 Loading CCI mapper from {cci_path}")
        registry.register_from_file(
            name="cci",
            file_path=cci_path,
            code_column="code",
            description_column="description",
            delimiter=",",
            code_type="procedure"
        )
        cci_mapper = registry.get_mapper("cci")
        print(f"   ✅ Loaded {len(cci_mapper)} CCI codes")
        
        # Test some common codes
        test_codes = ["1.AA.50", "1.AA.51", "1.HZ.53", "1.VA.55"]
        print(f"\n   🔍 Testing {len(test_codes)} sample codes:")
        for code in test_codes:
            desc = cci_mapper.get_description(code, default="Not found")
            status = "✅" if desc != "Not found" else "❌"
            print(f"      {status} {code}: {desc[:70]}")
    
    # Show statistics
    print("\n📊 Mapper Statistics:")
    all_stats = registry.get_all_stats()
    for name, stats in all_stats.items():
        print(f"   {name}:")
        print(f"      Total codes: {stats['total_codes']:,}")
        print(f"      Lookups: {stats['lookups']}")
        print(f"      Hit rate: {stats['hit_rate']:.2%}")
    
    return registry


def demo_integration(registry):
    """Demonstrate integration with sample data"""
    print("\n" + "="*60)
    print("🎯 DEMO: INTEGRATION WITH SAMPLE DATA")
    print("="*60)
    
    if not registry.has_mapper("icd10ca"):
        print("   ⚠️  ICD-10-CA mapper not available, skipping demo")
        return
    
    # Create sample patient data
    sample_data = pd.DataFrame({
        'patient_id': [1, 2, 3, 4, 5],
        'diagnosis_code': ['A00.0', 'A00.1', 'I21.0', 'E11.9', 'UNKNOWN'],
        'visit_date': pd.date_range('2024-01-01', periods=5)
    })
    
    print("\n📝 Sample patient data (before enrichment):")
    print(sample_data.to_string(index=False))
    
    # Enrich with descriptions
    icd_mapper = registry.get_mapper("icd10ca")
    sample_data['diagnosis_description'] = sample_data['diagnosis_code'].apply(
        lambda code: icd_mapper.get_description(code, default="Unknown code")
    )
    
    print("\n✨ Enriched patient data (with descriptions):")
    print(sample_data.to_string(index=False))
    
    # Find missing codes
    missing = [
        code for code in sample_data['diagnosis_code'].unique()
        if not icd_mapper.code_exists(code)
    ]
    
    if missing:
        print(f"\n⚠️  Missing codes found: {missing}")
        print(f"   Coverage: {(1 - len(missing)/len(sample_data))*100:.1f}%")
    else:
        print(f"\n✅ All codes found! 100% coverage")


def main():
    parser = argparse.ArgumentParser(
        description="Quick start script for testing code_mapper"
    )
    parser.add_argument(
        '--icd',
        type=str,
        help='Path to ICD-10-CA mapping file (pipe-delimited)'
    )
    parser.add_argument(
        '--cci',
        type=str,
        help='Path to CCI mapping file (comma-delimited)'
    )
    parser.add_argument(
        '--skip-validation',
        action='store_true',
        help='Skip file validation step'
    )
    parser.add_argument(
        '--skip-demo',
        action='store_true',
        help='Skip integration demo'
    )
    
    args = parser.parse_args()
    
    if not args.icd and not args.cci:
        parser.print_help()
        print("\n⚠️  Please provide at least one file path (--icd or --cci)")
        return
    
    print("\n" + "="*60)
    print("🚀 CODE MAPPER QUICK START")
    print("="*60)
    
    # Validate files
    if not args.skip_validation:
        if not validate_files(args.icd, args.cci):
            print("\n❌ Validation failed. Please check your files.")
            return
    
    # Test mappers
    registry = test_mappers(args.icd, args.cci)
    
    # Demo integration
    if not args.skip_demo:
        demo_integration(registry)
    
    print("\n" + "="*60)
    print("✅ QUICK START COMPLETED!")
    print("="*60)
    print("\n📚 Next steps:")
    print("   1. Review the output above")
    print("   2. Check src/code_mapper/README.md for full documentation")
    print("   3. See src/code_mapper/examples/usage_examples.py for more examples")
    print("   4. Integrate into your MEDS pipeline components")
    print("\n")


if __name__ == "__main__":
    main()
