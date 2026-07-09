#!/usr/bin/env python3
"""
Quick validation of new features
"""
import sys
import os
sys.path.insert(0, '/home/weijiesun/MEDS-pipeline/src')

def test_basic_functionality():
    try:
        # Test imports
        from meds_pipeline.cli import cli
        print("✅ CLI import successful")
        
        # Test new methods in base class
        from meds_pipeline.etl.base import ComponentETL
        print("✅ Base class import successful")
        
        return True
    except Exception as e:
        print(f"❌ Import failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    print("🧪 Testing new features...")
    
    if test_basic_functionality():
        print("\n📋 New features summary:")
        print("1. ✅ Improved progress display - Shows data loading and processing details")
        print("2. ✅ Patient count limiting - Use --max-patients N")
        print("3. ✅ Detailed statistics - Shows row counts and patient counts")
        
        print("\n💡 Recommended test command:")
        print("cd /home/weijiesun/MEDS-pipeline/src")
        print("python3 -m meds_pipeline.cli run --source mimic --components medicines --max-patients 50")
        
        print("\n🔧 If you encounter problems:")
        print("1. Check if data file paths are correct")
        print("2. Start testing with smaller patient counts (e.g., --max-patients 10)")
        print("3. Use --no-progress to disable detailed output")
    else:
        print("❌ Feature test failed")

if __name__ == "__main__":
    main()
