# src/meds_pipeline/cli.py
import click
from meds_pipeline.etl.orchestrators.mimic_source import MIMICSourceETL
from meds_pipeline.etl.orchestrators.ahs_source import AHSSourceETL
from meds_pipeline.etl.registry import build_components, REGISTRY
from meds_pipeline.meds.writer import (
    finalize_patient_bucketed_parquet,
    load_staging_manifest,
    normalize_meds_core_schema,
    reset_component_bucketed_staging,
    reset_bucketed_staging,
    staged_component_names,
    update_staging_manifest,
    write_component_bucketed_staging,
    write_component_bucketed_staging_chunk,
)
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
@click.option(
    "--layout",
    type=click.Choice(["flat", "patient-bucketed"]),
    default="flat",
    show_default=True,
    help="Parquet output layout",
)
@click.option(
    "--patient-buckets",
    type=int,
    default=256,
    show_default=True,
    help="Number of subject_id hash buckets for patient-bucketed layout",
)
@click.option(
    "--incremental",
    is_flag=True,
    help="For patient-bucketed layout, refresh only the requested component staging before finalizing",
)
def run(source, components, cfg, base, max_patients, progress, layout, patient_buckets, incremental):
    cfg_d  = _load(cfg)
    base_d = _load(base)    
    if patient_buckets <= 0:
        raise click.ClickException("--patient-buckets must be greater than 0")
    if incremental and layout != "patient-bucketed":
        raise click.ClickException("--incremental is only supported with --layout patient-bucketed")
    
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

    # Create output directory with source name
    output_dir = Path(base_d["output_dir"]) / source
    os.makedirs(output_dir, exist_ok=True)

    if layout == "patient-bucketed":
        start_time = time.time()
        result = _run_patient_bucketed(
            etl=etl,
            source=source,
            output_dir=output_dir,
            max_patients=max_patients,
            patient_buckets=patient_buckets,
            compression=base_d.get("compression", "snappy"),
            progress=progress,
            incremental=incremental,
        )
        end_time = time.time()

        click.echo(f"Data processing completed in {end_time - start_time:.2f} seconds")
        click.echo(
            f"Saved patient-bucketed dataset to {result['dataset_dir']}: "
            f"{result['row_count']:,} rows across {len(result['buckets']):,} non-empty buckets"
        )
        click.echo(f"Finalized components: {', '.join(result['components'])}")
        if result.get("patient_count") is not None:
            click.echo(f"Patient cohort: {result['patient_count']:,} patients")
        return
    
    start_time = time.time()
    df = etl.to_meds_core()
    df = normalize_meds_core_schema(df)
    end_time = time.time()
    
    click.echo(f"Data processing completed in {end_time - start_time:.2f} seconds")
        
    # Display summary statistics
    total_rows = len(df)
    unique_patients = df['subject_id'].nunique() if 'subject_id' in df.columns else 0
    click.echo(f"Generated {total_rows:,} rows for {unique_patients:,} unique patients")
    
    # Split large DataFrame into smaller chunks
    chunk_size = 100000  # Adjust this based on your needs
    # If only a single component was requested, always write a single file named {source}_{component}_meds_core.parquet
    if len(comp_list) == 1:
        output_path = output_dir / f"{source}_{comp_list[0]}_meds_core.parquet"
        df.to_parquet(output_path, index=False)
        click.echo(f"Saved to {output_path}: {total_rows:,} rows")
    elif total_rows <= chunk_size:
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
            
            pad = max(3, len(str(num_chunks)))
            output_path = output_dir / f"{source}_meds_core_part_{i+1:0{pad}d}.parquet"
            chunk_df.to_parquet(output_path, index=False)
            if not progress:  # Only show individual chunk messages if no progress bar
                click.echo(f"Saved chunk {i+1}/{num_chunks} to {output_path}: {len(chunk_df):,} rows")
        
        click.echo(f"Done: {total_rows:,} total rows split into {num_chunks} files")
    
    # schema = build_schema("configs/meds_schema_core.yaml",
    #                       "configs/meds_schema_plus.yaml" if plus else None)
    # df = schema.validate(df)

def _run_patient_bucketed(
    etl,
    source,
    output_dir,
    max_patients,
    patient_buckets,
    compression,
    progress,
    incremental,
):
    if not incremental:
        reset_bucketed_staging(output_dir)

    manifest = load_staging_manifest(output_dir)
    existing_patient_ids = manifest.get("patient_ids") if incremental else None
    patient_ids = [str(patient_id) for patient_id in existing_patient_ids] if existing_patient_ids else None
    patient_order = list(patient_ids or [])
    seen_patients = set(patient_order)
    component_stats = []

    if progress:
        mode = "incremental refresh" if incremental else "two-stage build"
        click.echo(f"\n🧱 Patient-bucketed {mode} with {patient_buckets:,} buckets")
        click.echo("=" * 60)

    for i, component in enumerate(etl.components):
        if progress:
            click.echo(f"\n📋 Component {i+1}/{len(etl.components)}: {component.name}")

        if patient_ids is not None:
            component.base_cfg = {
                **component.base_cfg,
                "patient_ids": patient_ids,
                "max_patients": None,
            }

        if callable(getattr(component, "iter_core", None)):
            stats, patient_ids = _stage_streaming_component(
                component=component,
                output_dir=output_dir,
                max_patients=max_patients,
                patient_ids=patient_ids,
                patient_order=patient_order,
                seen_patients=seen_patients,
                patient_buckets=patient_buckets,
                compression=compression,
            )
        else:
            df = component.run_core()
            df = normalize_meds_core_schema(df)

            if patient_ids is None and max_patients:
                _extend_patient_order(df, max_patients, patient_order, seen_patients)
                patient_ids = patient_order[:max_patients] if len(seen_patients) >= max_patients else None

            active_patient_ids = patient_ids
            if active_patient_ids is None and max_patients:
                active_patient_ids = patient_order

            stats = write_component_bucketed_staging(
                df,
                output_dir=output_dir,
                component=component.name,
                num_buckets=patient_buckets,
                compression=compression,
                patient_ids=active_patient_ids,
            )
        component_stats.append(stats)

        if progress:
            click.echo(
                f"   ✅ Staged {stats['row_count']:,} rows "
                f"across {len(stats['buckets']):,} buckets"
            )

    if max_patients and patient_ids is None:
        patient_ids = patient_order

    update_staging_manifest(
        output_dir=output_dir,
        source=source,
        num_buckets=patient_buckets,
        compression=compression,
        component_stats=component_stats,
        patient_ids=patient_ids,
        reset=not incremental,
    )

    components_to_finalize = staged_component_names(output_dir)
    result = finalize_patient_bucketed_parquet(
        output_dir=output_dir,
        source=source,
        num_buckets=patient_buckets,
        compression=compression,
        component_names=components_to_finalize,
        patient_ids=patient_ids,
    )
    result["patient_count"] = len(patient_ids) if patient_ids is not None else None
    return result


def _stage_streaming_component(
    component,
    output_dir,
    max_patients,
    patient_ids,
    patient_order,
    seen_patients,
    patient_buckets,
    compression,
):
    reset_component_bucketed_staging(output_dir, component.name)
    component_stats = {"name": component.name, "row_count": 0, "buckets": {}}
    wrote_parts = 0

    for part_number, df in enumerate(component.iter_core()):
        df = normalize_meds_core_schema(df)

        if patient_ids is None and max_patients:
            _extend_patient_order(df, max_patients, patient_order, seen_patients)
            patient_ids = patient_order[:max_patients] if len(seen_patients) >= max_patients else None

        active_patient_ids = patient_ids
        if active_patient_ids is None and max_patients:
            active_patient_ids = patient_order

        stats = write_component_bucketed_staging_chunk(
            df,
            output_dir=output_dir,
            component=component.name,
            num_buckets=patient_buckets,
            compression=compression,
            patient_ids=active_patient_ids,
            part_number=part_number,
        )
        wrote_parts += 1
        component_stats["row_count"] += stats["row_count"]
        _merge_bucket_counts(component_stats["buckets"], stats["buckets"])

    component_stats["parts"] = wrote_parts
    return component_stats, patient_ids


def _extend_patient_order(df, max_patients, patient_order, seen_patients):
    for subject_id in df["subject_id"].dropna().astype(str):
        if subject_id not in seen_patients:
            seen_patients.add(subject_id)
            patient_order.append(subject_id)
            if len(seen_patients) >= max_patients:
                break


def _merge_bucket_counts(target, source):
    for bucket, count in source.items():
        target[bucket] = int(target.get(bucket, 0)) + int(count)


if __name__ == "__main__":
    cli()
