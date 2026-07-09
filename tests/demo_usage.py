#!/usr/bin/env python3
"""
Demo script: Shows how to use MEDS pipeline with progress bars and patient limiting

New features:
1. --max-patients: Limit the number of patients to process (for testing)
2. --progress/--no-progress: Control whether to show progress bars
3. Display processing time and data statistics

Usage examples:
1. Test mode - process only 100 patients:
   python demo_usage.py --test

2. Full run but no progress bars:
   python demo_usage.py --no-progress

3. Custom patient count:
   python demo_usage.py --patients 500
"""

import subprocess
import sys
import os
from pathlib import Path

def run_meds_pipeline(source="mimic", components="admissions", max_patients=None, progress=True, plus=True):
    """Run MEDS pipeline"""
    
    # Build command
    cmd = [
        sys.executable, "src/meds_pipeline/cli.py", "run",
        "--source", source,
        "--components", components,
    ]
    
    if max_patients:
        cmd.extend(["--max-patients", str(max_patients)])
    
    if progress:
        cmd.append("--progress")
    else:
        cmd.append("--no-progress")
        
    if plus:
        cmd.append("--plus")
    else:
        cmd.append("--core")
    
    print("="*60)
    print(f"Running command: {' '.join(cmd)}")
    print("="*60)
    
    # Run command
    try:
        result = subprocess.run(cmd, cwd="/home/weijiesun/MEDS-pipeline", 
                              capture_output=True, text=True)
        print("STDOUT:")
        print(result.stdout)
        if result.stderr:
            print("STDERR:")
            print(result.stderr)
        return result.returncode == 0
    except Exception as e:
        print(f"Error: {e}")
        return False

def main():
    import argparse
    parser = argparse.ArgumentParser(description="MEDS pipeline demo script")
    parser.add_argument("--test", action="store_true", help="Test mode: process only 100 patients")
    parser.add_argument("--patients", type=int, help="Specify number of patients to process")
    parser.add_argument("--no-progress", action="store_true", help="Don't show progress bars")
    parser.add_argument("--source", default="mimic", choices=["mimic", "ahs"], help="Data source")
    parser.add_argument("--components", default="admissions", help="Components to process")
    parser.add_argument("--core", action="store_true", help="Output MEDS core format (default is PLUS)")
    
    args = parser.parse_args()
    
    # Determine patient count
    max_patients = None
    if args.test:
        max_patients = 100
        print("🧪 Test mode: limiting to 100 patients")
    elif args.patients:
        max_patients = args.patients
        print(f"👥 Custom mode: limiting to {max_patients} patients")
    else:
        print("🚀 Full mode: processing all patients")
    
    # Determine progress bar settings
    show_progress = not args.no_progress
    if show_progress:
        print("📊 Progress bars enabled")
    else:
        print("🔇 Progress bars disabled")
    
    # Determine output format
    plus_format = not args.core
    format_str = "MEDS-PLUS" if plus_format else "MEDS-CORE"
    print(f"📋 Output format: {format_str}")
    
    print(f"🔧 Components: {args.components}")
    print(f"💾 Data source: {args.source}")
    print()
    
    # Run pipeline
    success = run_meds_pipeline(
        source=args.source,
        components=args.components,
        max_patients=max_patients,
        progress=show_progress,
        plus=plus_format
    )
    
    if success:
        print("✅ Processing completed!")
    else:
        print("❌ Processing failed")
        sys.exit(1)

if __name__ == "__main__":
    main()
