# MEDS-pipeline
MEDS pipeline for both MIMIC and AHS datasets

Demo example

python3 -m meds_pipeline.cli run --source mimic --components admissions --cfg mimic.yaml

python3 -m meds_pipeline.cli run --source ahs --components admissions --cfg ahs.yaml

TODO:

write build_schema
write write_df
Write Test

## structure

## 项目结构

```
MEDS-pipeline/
├── .gitignore
├── LICENSE
├── README.md
├── src/
│   ├── args.py
│   └── meds_pipeline/
│       ├── __init__.py
│       ├── cli.py
│       ├── configs/
│       │   ├── ahs.yaml
│       │   ├── base.yaml
│       │   └── mimic.yaml
│       └── etl/
│           ├── base.py
│           ├── registry.py
│           ├── ahs/
│           │   ├── __init__.py
│           │   └── admissions.py
│           │   └── ...
│           └── mimic/
│           │   ├── __init__.py
│           │   └── admissions.py
│           │   └── ...
│           └── orchestrators/
│           │   ├── __init__.py
│           │   └── ahs_source.py
│           │   └── mimic_source.py
└── tests/
```