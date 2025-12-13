# Canada Code Mapper

A focused code-to-description mapping system for Canadian medical coding standards:
- **ICD-10-CA**: International Classification of Diseases, 10th Revision, Canada (diagnosis codes)
- **CCI**: Canadian Classification of Health Interventions (procedure codes)

**Data Source**: [CIHI (Canadian Institute for Health Information)](https://secure.cihi.ca/estore/productSeries.htm?pc=PCC84)

## Features

- 🏥 **Medical Code Support**: ICD-10-CA (diagnosis) and CCI (procedure) codes
- 🔍 **Fast Lookup**: Efficient dictionary-based lookups
- 📊 **Batch Processing**: Process multiple codes at once
- 📈 **Statistics**: Track lookup performance and hit rates
- 🔧 **Flexible**: Load from CSV, TXT, or DataFrame
- 🎯 **Registry Pattern**: Manage multiple mappers centrally
- 🔀 **Composite Code Support**: Handle codes in format `PREFIX//SYSTEM//CODE`

## Composite Code Format

The mapper now supports **composite code format** commonly found in medical databases:

```
DIAGNOSIS//ICD10CA//M1000
PROCEDURE//CCI//1VG52HA
```

This format includes:
1. **Prefix**: Event type (e.g., DIAGNOSIS, PROCEDURE)
2. **System**: Coding system (e.g., ICD10CA, CCI)
3. **Code**: The actual medical code

The mapper automatically extracts the code portion for lookup, and the registry can auto-route to the correct mapper based on the system.

## Quick Start

### Basic Usage

```python
from canada_code_mapper import CodeMapper

# Load ICD-10-CA mapper from file
icd_mapper = CodeMapper.from_file(
    "data/icd10ca_descriptions.txt",
    code_column="code",
    description_column="description",
    delimiter="|",
    code_type="diagnosis"
)

# Get description for a single code
desc = icd_mapper.get_description("A00.0")
print(desc)  # Output: "Cholera due to Vibrio cholerae 01, biovar cholerae"

# Batch lookup
codes = ["A00.0", "A00.1", "A00.9"]
descriptions = icd_mapper.get_descriptions(codes)

# Check if code exists
if icd_mapper.code_exists("A00.0"):
    print("Code exists!")

# Search for codes/descriptions
results = icd_mapper.search("cholera", max_results=10)
print(results)
```

### Composite Code Format

The mapper supports composite codes commonly found in AHS and other medical databases:

```python
from canada_code_mapper import CodeMapper

# Create mapper
icd_mapper = CodeMapper.from_file("data/icd10ca_descriptions.txt", delimiter="|")

# Plain code format (traditional)
desc = icd_mapper.get_description("M1000")
print(desc)  # Works as before

# Composite code format (new)
desc = icd_mapper.get_description("DIAGNOSIS//ICD10CA//M1000")
print(desc)  # Automatically extracts "M1000" and looks it up

# With tabs (as in real data)
desc = icd_mapper.get_description("DIAGNOSIS//ICD10CA//M1000\t")
print(desc)  # Handles whitespace automatically

# Batch lookup with mixed formats
codes = [
    "M1000",                        # Plain
    "DIAGNOSIS//ICD10CA//A00.0",    # Composite
    "A099",                          # Plain
]
descriptions = icd_mapper.get_descriptions(codes)
```

### Using the Registry

```python
from canada_code_mapper import MapperRegistry

# Create registry
registry = MapperRegistry()

# Register ICD-10-CA mapper
registry.register_from_file(
    name="icd10ca",
    file_path="data/icd10ca_descriptions.txt",
    code_column="code",
    description_column="description",
    delimiter="|",
    code_type="diagnosis"
)

# Register CCI mapper
registry.register_from_file(
    name="cci",
    file_path="data/cci_descriptions.txt",
    code_column="code",
    description_column="description",
    delimiter=",",
    code_type="procedure"
)

# Use registered mappers
icd_desc = registry.get_description("icd10ca", "A00.0")
cci_desc = registry.get_description("cci", "1.AA.50")

# Get mapper directly
icd_mapper = registry.get_mapper("icd10ca")

# List all registered mappers
print(registry.list_mappers())  # ['icd10ca', 'cci']
```

### Auto-Routing with Composite Codes

The registry can automatically route composite codes to the correct mapper:

```python
from canada_code_mapper import MapperRegistry

# Setup registry with mappers
registry = MapperRegistry()
registry.register_from_file("icd10ca", "data/icd10ca.txt", delimiter="|")
registry.register_from_file("cci", "data/cci.txt", delimiter=",")

# Auto-routing based on system in composite code
desc = registry.get_description(
    "icd10ca",  # Fallback mapper if routing fails
    "DIAGNOSIS//ICD10CA//M1000",  # Composite code
    auto_route=True  # Enable auto-routing
)
# Automatically routes to 'icd10ca' mapper

desc = registry.get_description(
    "icd10ca",  # Fallback mapper
    "PROCEDURE//CCI//1VG52HA",  # Composite code with CCI system
    auto_route=True
)
# Automatically routes to 'cci' mapper

# Process DataFrame with mixed codes
import pandas as pd

df = pd.DataFrame({
    'code': [
        'DIAGNOSIS//ICD10CA//M1000',
        'PROCEDURE//CCI//1VG52HA',
        'DIAGNOSIS//ICD10CA//A099'
    ]
})

df['description'] = df['code'].apply(
    lambda c: registry.get_description('icd10ca', c, auto_route=True)
)
```

### Convenience Function for Canadian Codes

```python
from canada_code_mapper.registry import init_canadian_mappers

# Initialize both ICD-10-CA and CCI mappers at once
registry = init_canadian_mappers(
    icd10ca_path="data/icd10ca_descriptions.txt",
    cci_path="data/cci_descriptions.txt"
)

# Use the mappers
icd_desc = registry.get_description("icd10ca", "A00.0")
cci_desc = registry.get_description("cci", "1.AA.50")
```

## Usage with MEDS Pipeline

### In AHS Diagnosis Component

```python
from canada_code_mapper import CodeMapper

class AHSDiagnosis:
    def __init__(self, config):
        # Initialize ICD-10-CA mapper
        self.icd_mapper = CodeMapper.from_file(
            config.icd10ca_file,
            code_column="code",
            description_column="description",
            code_type="diagnosis"
        )
    
    def process(self, df):
        # Add descriptions to diagnosis codes
        df['code_description'] = df['diagnosis_code'].apply(
            lambda code: self.icd_mapper.get_description(code, default="Unknown")
        )
        return df
```

### In AHS Procedures Component

```python
from canada_code_mapper import CodeMapper

class AHSProcedures:
    def __init__(self, config):
        # Initialize CCI mapper
        self.cci_mapper = CodeMapper.from_file(
            config.cci_file,
            code_column="code",
            description_column="description",
            code_type="procedure"
        )
    
    def process(self, df):
        # Add descriptions to procedure codes
        df['code_description'] = df['procedure_code'].apply(
            lambda code: self.cci_mapper.get_description(code, default="Unknown")
        )
        return df
```

## Utility Functions

### Validate Mapping File

```python
from canada_code_mapper.utils import validate_mapping_file

results = validate_mapping_file(
    "data/icd10ca_descriptions.txt",
    code_column="code",
    description_column="description",
    delimiter="|"
)

print(f"Valid: {results['valid']}")
print(f"Total rows: {results['total_rows']}")
print(f"Unique codes: {results['unique_codes']}")
print(f"Duplicate codes: {results['duplicate_codes']}")
```

### Find Missing Codes

```python
from canada_code_mapper.utils import find_missing_codes
import pandas as pd

# Your data with diagnosis codes
df = pd.read_parquet("ahs_diagnosis.parquet")

# Find codes not in mapper
missing_df = find_missing_codes(
    df,
    code_column="diagnosis_code",
    mapper=icd_mapper
)

print(f"Missing codes: {len(missing_df)}")
```

### Enrich DataFrame with Descriptions

```python
from canada_code_mapper.utils import enrich_dataframe
import pandas as pd

# Your data
df = pd.read_parquet("ahs_diagnosis.parquet")

# Add description column
df_enriched = enrich_dataframe(
    df,
    code_column="diagnosis_code",
    mapper=icd_mapper,
    description_column="diagnosis_description"
)
```

## Statistics and Monitoring

```python
# Get lookup statistics
stats = icd_mapper.get_stats()
print(f"Total codes: {stats['total_codes']}")
print(f"Lookups performed: {stats['lookups']}")
print(f"Hit rate: {stats['hit_rate']:.2%}")

# Reset statistics
icd_mapper.reset_stats()

# Get stats for all mappers in registry
all_stats = registry.get_all_stats()
for mapper_name, stats in all_stats.items():
    print(f"{mapper_name}: {stats['hit_rate']:.2%} hit rate")
```

## File Format Requirements

### Expected Format

Your mapping files should be CSV or TXT files with at least two columns:
- One column for the code
- One column for the description

Example:

```
code|description
A00.0|Cholera due to Vibrio cholerae 01, biovar cholerae
A00.1|Cholera due to Vibrio cholerae 01, biovar eltor
A00.9|Cholera, unspecified
```

### Supported Delimiters
- `,` (CSV)
- `|` (pipe)
- `\t` (tab)
- Any custom delimiter

## Advanced Usage

### Loading from DataFrame

```python
import pandas as pd
from canada_code_mapper import CodeMapper

# Create mapper from existing DataFrame
df = pd.DataFrame({
    'code': ['A00.0', 'A00.1'],
    'description': ['Description 1', 'Description 2']
})

mapper = CodeMapper.from_dataframe(
    df,
    code_column='code',
    description_column='description',
    name='CustomMapper'
)
```

### Merging Mappers

```python
from canada_code_mapper.utils import merge_mappers

# Merge two mappers
combined_mapper = merge_mappers(
    mapper1=icd_mapper,
    mapper2=custom_mapper,
    name="CombinedMapper"
)
```

### Export Mapper

```python
from canada_code_mapper.utils import export_mapper_to_csv

# Export mapper to CSV
export_mapper_to_csv(
    icd_mapper,
    output_path="exported_icd_mappings.csv"
)
```

## Integration with MEDS Pipeline Configuration

Add to your `ahs.yaml`:

```yaml
# Code mapping files
code_mappings:
  icd10ca_file: "data/icd10ca_descriptions.txt"
  icd10ca_delimiter: "|"
  cci_file: "data/cci_descriptions.txt"
  cci_delimiter: ","
```

## Future: Standalone Package

This module is designed to be easily extracted into a standalone package:

```bash
# Future installation (when published)
pip install canada-code-mapper

# Usage will remain the same
from canada_code_mapper import CodeMapper, MapperRegistry
```

## Backward Compatibility

⚠️ **Note**: This module was previously named `code_mapper`. For backward compatibility, you can still import from `code_mapper`, but a deprecation warning will be shown. Please update your imports:

```python
# OLD (deprecated, will show warning)
from code_mapper import CodeMapper

# NEW (recommended)
from canada_code_mapper import CodeMapper
```

The `code_mapper` compatibility shim will be removed in version 0.3.0.

## Data Sources

This library is designed to work with official Canadian medical coding data from:

- **CIHI (Canadian Institute for Health Information)**
  - ICD-10-CA codes and descriptions
  - CCI codes and descriptions
  - Available at: https://secure.cihi.ca/estore/productSeries.htm?pc=PCC84

### Expected File Formats

**ICD-10-CA** (diagnosis codes):
- File format: Pipe-delimited text file
- Columns: `code | description`
- Example: `A00.0|Cholera due to Vibrio cholerae 01, biovar cholerae`

**CCI** (procedure codes):
- File format: Comma-delimited text file
- Columns: `code, description`
- Example: `1.AA.50,Transplantation of heart`

## API Reference

### CodeMapper

- `from_file()`: Load mapper from file
- `from_dataframe()`: Load mapper from DataFrame
- `get_description(code)`: Get description for single code
- `get_descriptions(codes)`: Batch lookup
- `code_exists(code)`: Check if code exists
- `search(query)`: Search codes and descriptions
- `get_stats()`: Get lookup statistics
- `reset_stats()`: Reset statistics

### MapperRegistry

- `register()`: Register a mapper
- `register_from_file()`: Create and register from file
- `get_mapper(name)`: Get mapper by name
- `get_description(mapper_name, code)`: Lookup using named mapper
- `list_mappers()`: List all registered mappers
- `has_mapper(name)`: Check if mapper exists
- `remove_mapper(name)`: Remove a mapper

## License

Part of the MEDS-pipeline project.

## Contributing

Contributions welcome! This module is designed to be flexible and extensible for various medical coding systems.
