# MEDS-pipeline
MEDS pipeline for both MIMIC and AHS datasets

Demo example

python3 -m meds_pipeline.cli run --source mimic --components admissions --cfg mimic.yaml

python3 -m meds_pipeline.cli run --source ahs --components admissions --cfg ahs.yaml

TODO:

write build_schema
write write_df
Write Test