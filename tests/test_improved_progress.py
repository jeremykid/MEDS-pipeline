#!/usr/bin/env python3
"""
Test improved progress display functionality

New progress display includes:
1. 📖 Detailed information during data loading phase
2. 📊 Row and patient counts for each component
3. 🔗 Information during component merging phase
4. ✅ Final result statistics

Usage:
python test_improved_progress.py
"""

import subprocess
import sys
import os

def run_test():
    """Run test"""
    
    print("🧪 Testing improved progress display functionality")
    print("="*60)
    
    # Test command
    cmd = [
        "python3", "-m", "meds_pipeline.cli", "run",
        "--source", "mimic",
        "--components", "medicines",
        "--cfg", "mimic.yaml",
        "--max-patients", "100",
        "--progress"
    ]
    
    print(f"Running command: {' '.join(cmd)}")
    print("="*60)
    
    # Set working directory
    cwd = "/home/weijiesun/MEDS-pipeline/src"
    
    try:
        # Run command and display output in real time
        process = subprocess.Popen(
            cmd, 
            cwd=cwd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True,
            bufsize=1
        )
        
        # Print output in real time
        for line in process.stdout:
            print(line, end='')
        
        # Wait for completion
        process.wait()
        
        if process.returncode == 0:
            print("\n✅ Test completed successfully!")
        else:
            print(f"\n❌ Test failed, return code: {process.returncode}")
            
    except Exception as e:
        print(f"❌ Execution failed: {e}")

if __name__ == "__main__":
    run_test()
