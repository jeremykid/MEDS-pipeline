# src/meds_pipeline/cli.py
import click
from meds_pipeline.etl.orchestrators.mimic_source import MIMICSourceETL
# from meds_pipeline.etl.orchestrators.ahs_source import AHSSourceETL
from meds_pipeline.etl.registry import build_components, REGISTRY
import yaml, pandas as pd
# from meds_pipeline.meds.schema import build_schema  # TODO
# from meds_pipeline.meds.writer import write_df      # TODO
print("BEFORE import, REGISTRY keys:", list(REGISTRY.keys()))

@click.group()
def cli():
    """MEDS pipeline CLI"""

# def _load(cfg_path):
#     with open(cfg_path, "r") as f:
#         return yaml.safe_load(f)

from pathlib import Path
import yaml
from importlib.resources import files

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
@click.option("--plus/--core", default=True, help="Export MEDS-PLUS (default) or CORE")
def run(source, components, cfg, base, plus):
    cfg_d  = _load(cfg)
    base_d = _load(base)
    comp_list = [x.strip() for x in components.split(",") if x.strip()]
    # choose source
    if source == "mimic":
        etl = MIMICSourceETL(comp_list, cfg_d, base_d)
    # else:
        # etl = AHSSourceETL(comp_list, cfg_d, base_d)
    df = etl.to_meds_plus() if plus else etl.to_meds_core()
    print(df)
    # schema = build_schema("configs/meds_schema_core.yaml",
    #                       "configs/meds_schema_plus.yaml" if plus else None)
    # df = schema.validate(df)
    # write_df(df, base_d)  # write parquet/csv to base_d["output_dir"]
    # click.echo(f"Done: {len(df):,} rows")

if __name__ == "__main__":
    cli()
