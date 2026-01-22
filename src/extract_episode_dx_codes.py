#!/usr/bin/env python3
"""
Extract diagnosis codes from AHS episodes within a specified time window.

This script processes AHS episode data and extracts diagnosis codes from DAD and/or ED
records that fall within a time window before each episode start date.

Usage:
    PYTHONPATH=src python src/extract_episode_dx_codes.py \
    --episode-file /data/padmalab_external/special_project/AHS_Data_Release_2/rmt22884_episode_df_all.parquet \
    --dad-file /data/padmalab_external/special_project/AHS_Data_Release_2/rmt22884_dad_20211105_w_episode_order.parquet \
    --ed-file /data/padmalab_external/special_project/AHS_Data_Release_2/rmt22884_ed_20211105_w_episode_order.parquet \
    --number-of-days 1825 \
    --feature "inp ignore ed" \
    --output /data/padmalab_external/special_project/AHS_Data_Release_2/episode_5yr_history_dx_codes.parquet \
    --use-cached  # Default: enabled (fastest method) \
    --use-cudf  # Optional: for GPU acceleration (experimental)

"""

import argparse
import sys
import json
from pathlib import Path
import pandas as pd

# Try to import tqdm (optional)
try:
    from tqdm import tqdm
except ImportError:
    # Fallback if tqdm not available
    def tqdm(iterable, **kwargs):
        return iterable

# Add src to path to import meds_pipeline modules
sys.path.insert(0, str(Path(__file__).parent))

# Import cached version (main implementation with optional cuDF support)
from meds_pipeline.utils.episode_dx_extractor_cached import extract_dx_codes_cached, CUDF_AVAILABLE


def load_data(episode_file: str, dad_file: str, ed_file: str, load_only_required_cols: bool = True) -> tuple:
    """
    Load all required data files.
    
    Parameters
    ----------
    episode_file : str
        Path to episode parquet file
    dad_file : str
        Path to DAD parquet file
    ed_file : str
        Path to ED parquet file
        
    Returns
    -------
    tuple
        (episode_df, dad_df, ed_df)
    """
    print("📖 Loading data files...")
    
    # Load episode file
    print(f"   Loading episodes from: {episode_file}")
    if load_only_required_cols:
        episode_cols = ['episode_order', 'start_date', 'type', 'PATID']
        # Check if PATID exists in file
        try:
            import pyarrow.parquet as pq
            parquet_file = pq.ParquetFile(episode_file)
            available_cols = parquet_file.schema_arrow.names
            episode_cols = [col for col in episode_cols if col in available_cols]
        except ImportError:
            pass
        episode_df = pd.read_parquet(episode_file, columns=episode_cols)
    else:
        episode_df = pd.read_parquet(episode_file)
    print(f"   ✅ Loaded {len(episode_df):,} episodes")
    
    # Load DAD file - only load required columns
    print(f"   Loading DAD data from: {dad_file}")
    if load_only_required_cols:
        # Only load episode_order, dates, PATID, and DXCODE columns
        dad_cols = ['episode_order', 'ADMITDATE_DT', 'DISDATE_DT', 'PATID']
        dad_cols.extend([f'DXCODE{i}' for i in range(1, 26)])
        # Check which columns actually exist
        try:
            import pyarrow.parquet as pq
            parquet_file = pq.ParquetFile(dad_file)
            available_cols = parquet_file.schema_arrow.names
            dad_cols = [col for col in dad_cols if col in available_cols]
            print(f"   Loading {len(dad_cols)} columns from DAD (instead of all columns)")
        except ImportError:
            # If pyarrow not available, just try to load with specified columns
            print(f"   Attempting to load {len(dad_cols)} columns from DAD")
        dad_df = pd.read_parquet(dad_file, columns=dad_cols)
    else:
        dad_df = pd.read_parquet(dad_file)
    print(f"   ✅ Loaded {len(dad_df):,} DAD records")
    
    # Load ED file - only load required columns
    print(f"   Loading ED data from: {ed_file}")
    if load_only_required_cols:
        # Only load episode_order, date, PATID, and DXCODE columns
        ed_cols = ['episode_order', 'VISIT_DATE_DT', 'PATID']
        ed_cols.extend([f'DXCODE{i}' for i in range(1, 11)])
        # Check which columns actually exist
        try:
            import pyarrow.parquet as pq
            parquet_file = pq.ParquetFile(ed_file)
            available_cols = parquet_file.schema_arrow.names
            ed_cols = [col for col in ed_cols if col in available_cols]
            print(f"   Loading {len(ed_cols)} columns from ED (instead of all columns)")
        except ImportError:
            # If pyarrow not available, just try to load with specified columns
            print(f"   Attempting to load {len(ed_cols)} columns from ED")
        ed_df = pd.read_parquet(ed_file, columns=ed_cols)
    else:
        ed_df = pd.read_parquet(ed_file)
    print(f"   ✅ Loaded {len(ed_df):,} ED records")
    
    # Validate required columns
    required_episode_cols = ['episode_order', 'start_date']
    missing_cols = [col for col in required_episode_cols if col not in episode_df.columns]
    if missing_cols:
        raise ValueError(f"Episode file missing required columns: {missing_cols}")
    
    return episode_df, dad_df, ed_df


def process_episodes(
    episode_df: pd.DataFrame,
    dad_df: pd.DataFrame,
    ed_df: pd.DataFrame,
    number_of_days: int,
    feature: str,
    show_progress: bool = True,
    use_cached: bool = True,
    use_cudf: bool = False,
    batch_size: int = 1000
) -> pd.DataFrame:
    """
    Process all episodes and extract diagnosis codes.
    
    Parameters
    ----------
    episode_df : pd.DataFrame
        Episode dataframe
    dad_df : pd.DataFrame
        DAD dataframe
    ed_df : pd.DataFrame
        ED dataframe
    number_of_days : int
        Number of days for time window
    feature : str
        Feature mode: "inp only", "both", or "inp ignore ed"
    show_progress : bool
        Whether to show progress bar
        
    Returns
    -------
    pd.DataFrame
        DataFrame with episode_order as index and dx_codes as column (list)
    """
    print(f"\n🔍 Processing {len(episode_df):,} episodes...")
    print(f"   Time window: {number_of_days} days before episode start")
    print(f"   Feature mode: {feature}")
    print("=" * 60)
    
    # Use cached preprocessing (default and only method)
    result_df = extract_dx_codes_cached(
        episode_df, dad_df, ed_df, number_of_days, feature,
        batch_size=batch_size, show_progress=show_progress, use_cudf=use_cudf
    )
    
    # Print statistics
    total_codes = sum(len(codes) for codes in result_df['dx_codes'])
    episodes_with_codes = (result_df['dx_codes'].apply(len) > 0).sum()
    
    print("\n" + "=" * 60)
    print(f"✅ Processing complete!")
    print(f"   Total episodes processed: {len(result_df):,}")
    print(f"   Episodes with diagnosis codes: {episodes_with_codes:,} ({episodes_with_codes/len(result_df)*100:.1f}%)")
    print(f"   Total unique diagnosis codes collected: {total_codes:,}")
    print(f"   Average codes per episode: {total_codes/len(result_df):.1f}")
    
    return result_df


def main():
    parser = argparse.ArgumentParser(
        description="Extract diagnosis codes from AHS episodes within time window",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    parser.add_argument(
        "--episode-file",
        type=str,
        required=True,
        help="Path to episode parquet file"
    )
    
    parser.add_argument(
        "--dad-file",
        type=str,
        required=True,
        help="Path to DAD parquet file with episode_order"
    )
    
    parser.add_argument(
        "--ed-file",
        type=str,
        required=True,
        help="Path to ED parquet file with episode_order"
    )
    
    parser.add_argument(
        "--number-of-days",
        type=int,
        required=True,
        help="Number of days for time window (e.g., 365)"
    )
    
    parser.add_argument(
        "--feature",
        type=str,
        choices=["inp only", "both", "inp ignore ed"],
        required=True,
        help="Feature mode: 'inp only' (DAD only), 'both' (ED+DAD), or 'inp ignore ed' (DAD for inp episodes, both otherwise)"
    )
    
    parser.add_argument(
        "--output",
        type=str,
        required=True,
        help="Output path for parquet file"
    )
    
    parser.add_argument(
        "--no-progress",
        action="store_true",
        help="Disable progress bar"
    )
    
    parser.add_argument(
        "--use-cudf",
        action="store_true",
        help="Use cuDF for GPU acceleration (requires GPU and cuDF installation). Works with --use-cached."
    )
    
    parser.add_argument(
        "--use-cached",
        action="store_true",
        default=True,
        help="Use cached preprocessing with PATID grouping (default: enabled, fastest method)"
    )
    
    parser.add_argument(
        "--no-cached",
        action="store_true",
        help="Disable cached preprocessing (not recommended)"
    )
    
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Batch size for processing (default: 1000, only used with cached method)"
    )
    
    parser.add_argument(
        "--load-all-cols",
        action="store_true",
        help="Load all columns from data files (default: only load required columns for faster loading)"
    )
    
    args = parser.parse_args()
    
    # Validate files exist
    for file_path, name in [
        (args.episode_file, "Episode file"),
        (args.dad_file, "DAD file"),
        (args.ed_file, "ED file")
    ]:
        if not Path(file_path).exists():
            raise FileNotFoundError(f"{name} not found: {file_path}")
    
    print("=" * 60)
    print("🚀 AHS Episode Diagnosis Code Extractor")
    print("=" * 60)
    
    # Load data
    episode_df, dad_df, ed_df = load_data(
        args.episode_file,
        args.dad_file,
        args.ed_file,
        load_only_required_cols=not args.load_all_cols
    )
    
    # Check cuDF availability
    if args.use_cudf and not CUDF_AVAILABLE:
        print("⚠️  cuDF is not available. Falling back to CPU processing.")
        print("   To use cuDF, install it with: pip install cudf-cu12  # for CUDA 12")
        args.use_cudf = False
    
    # Determine if cached should be used
    use_cached = args.use_cached and not args.no_cached
    
    if not use_cached:
        print("⚠️  Warning: --no-cached is not recommended. Cached method is much faster.")
    
    # Process episodes
    result_df = process_episodes(
        episode_df,
        dad_df,
        ed_df,
        args.number_of_days,
        args.feature,
        show_progress=not args.no_progress,
        use_cached=use_cached,
        use_cudf=args.use_cudf,
        batch_size=args.batch_size
    )
    
    # Save results
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    print(f"\n💾 Saving results to: {output_path}")
    
    # PyArrow supports list types natively, but we need to ensure the column is properly typed
    result_df_copy = result_df.copy()
    
    # Ensure dx_codes is a list type (not object type)
    # Convert to list of strings explicitly
    result_df_copy['dx_codes'] = result_df_copy['dx_codes'].apply(lambda x: x if isinstance(x, list) else [])
    
    # Save with pyarrow engine which supports list types
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
        
        # Create pyarrow table with explicit schema for list type
        table = pa.Table.from_pandas(result_df_copy, preserve_index=True)
        pq.write_table(table, output_path)
        print(f"✅ Saved {len(result_df_copy):,} episodes to {output_path}")
    except Exception as e:
        print(f"⚠️  Error saving with explicit schema: {e}")
        print("   Trying standard pandas to_parquet...")
        try:
            result_df_copy.to_parquet(output_path, index=True, engine='pyarrow')
            print(f"✅ Saved {len(result_df_copy):,} episodes to {output_path}")
        except Exception as e2:
            print(f"⚠️  Error with standard method: {e2}")
            print("   Fallback: converting lists to JSON strings...")
            # Fallback: convert lists to JSON strings
            result_df_copy['dx_codes'] = result_df_copy['dx_codes'].apply(lambda x: json.dumps(x) if isinstance(x, list) else json.dumps([]))
            result_df_copy.to_parquet(output_path, index=True, engine='pyarrow')
            print(f"✅ Saved {len(result_df_copy):,} episodes to {output_path} (lists as JSON strings)")
    
    print("\n" + "=" * 60)
    print("✅ All done!")


if __name__ == "__main__":
    main()
