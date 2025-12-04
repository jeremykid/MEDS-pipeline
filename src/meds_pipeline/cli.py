# src/meds_pipeline/cli.py
import click
from meds_pipeline.etl.orchestrators.mimic_source import MIMICSourceETL
from meds_pipeline.etl.orchestrators.ahs_source import AHSSourceETL
from meds_pipeline.etl.registry import build_components, REGISTRY
import yaml, pandas as pd
from tqdm import tqdm
import time
# from meds_pipeline.meds.schema import build_schema  # TODO
# from meds_pipeline.meds.writer import write_df      # TODO
print("BEFORE import, REGISTRY keys:", list(REGISTRY.keys()))

@click.group()
def cli():
    """MEDS pipeline CLI"""

from pathlib import Path
import yaml
from importlib.resources import files
import os

def _load(path_or_pkg_rel):
    p = Path(path_or_pkg_rel)
    if p.exists():  
        return yaml.safe_load(p.read_text(encoding="utf-8"))

    pkg_root = files("meds_pipeline.configs")
    return yaml.safe_load((pkg_root / path_or_pkg_rel).read_text(encoding="utf-8"))

@cli.command()
@click.option("--source", type=click.Choice(["mimic","ahs"]), required=True)
@click.option("--components", help="Comma-separated, e.g. admissions,diagnoses_icd", required=True)
@click.option("--cfg", default="mimic.yaml")
@click.option("--base", default="base.yaml")
@click.option("--max-patients", type=int, default=None, help="Limit number of patients to process (for testing)")
@click.option("--progress/--no-progress", default=True, help="Show progress bar")
def run(source, components, cfg, base, max_patients, progress):
    cfg_d  = _load(cfg)
    base_d = _load(base)    
    
    # Create output directory if it doesn't exist
    os.makedirs(base_d["output_dir"], exist_ok=True)
    comp_list = [x.strip() for x in components.split(",") if x.strip()]
    
    # Add patient limit and progress options to config
    base_d["max_patients"] = max_patients
    base_d["show_progress"] = progress
    
    # choose source
    if source == "mimic":
        etl = MIMICSourceETL(comp_list, cfg_d, base_d)
    elif source == "ahs":
        etl = AHSSourceETL(comp_list, cfg_d, base_d)
    
    click.echo(f"Processing {len(comp_list)} components: {', '.join(comp_list)}")
    if max_patients:
        click.echo(f"Limiting to first {max_patients} patients")
    
    start_time = time.time()
    df = etl.to_meds_core()
    end_time = time.time()
    
    click.echo(f"Data processing completed in {end_time - start_time:.2f} seconds")
        
    # Create output directory with source name
    output_dir = Path(base_d["output_dir"]) / source
    os.makedirs(output_dir, exist_ok=True)
    
    # Display summary statistics
    total_rows = len(df)
    unique_patients = df['subject_id'].nunique() if 'subject_id' in df.columns else 0
    click.echo(f"Generated {total_rows:,} rows for {unique_patients:,} unique patients")
    
    # Split large DataFrame into smaller chunks
    chunk_size = 100000  # Adjust this based on your needs
    
    if total_rows <= chunk_size:
        # Single file if small enough
        output_path = output_dir / f"{source}_meds_core.parquet"
        df.to_parquet(output_path, index=False)
        click.echo(f"Saved to {output_path}: {total_rows:,} rows")
    else:
        # Split into multiple files with progress bar
        num_chunks = (total_rows + chunk_size - 1) // chunk_size
        if progress:
            chunk_iter = tqdm(range(num_chunks), desc="Saving chunks", unit="chunk")
        else:
            chunk_iter = range(num_chunks)
            
        for i in chunk_iter:
            start_idx = i * chunk_size
            end_idx = min((i + 1) * chunk_size, total_rows)
            chunk_df = df.iloc[start_idx:end_idx]
            
            output_path = output_dir / f"{source}_meds_core_part_{i+1:03d}.parquet"
            chunk_df.to_parquet(output_path, index=False)
            if not progress:  # Only show individual chunk messages if no progress bar
                click.echo(f"Saved chunk {i+1}/{num_chunks} to {output_path}: {len(chunk_df):,} rows")
        
        click.echo(f"Done: {total_rows:,} total rows split into {num_chunks} files")
    
    # schema = build_schema("configs/meds_schema_core.yaml",
    #                       "configs/meds_schema_plus.yaml" if plus else None)
    # df = schema.validate(df)

if __name__ == "__main__":
    cli()
