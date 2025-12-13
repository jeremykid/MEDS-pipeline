"""
Utility functions for code mapping.
"""

import pandas as pd
from typing import List, Dict, Optional
import logging

logger = logging.getLogger(__name__)


def validate_mapping_file(
    file_path: str,
    code_column: str,
    description_column: str,
    delimiter: str = ",",
    encoding: str = "utf-8",
    sample_size: int = 5
) -> Dict:
    """
    Validate a mapping file and return statistics.
    
    Args:
        file_path: Path to the mapping file
        code_column: Name of code column
        description_column: Name of description column
        delimiter: File delimiter
        encoding: File encoding
        sample_size: Number of sample rows to return
        
    Returns:
        Dictionary with validation results and statistics
    """
    try:
        df = pd.read_csv(file_path, delimiter=delimiter, encoding=encoding)
        
        results = {
            "valid": True,
            "total_rows": len(df),
            "columns": df.columns.tolist(),
            "has_code_column": code_column in df.columns,
            "has_description_column": description_column in df.columns,
        }
        
        if results["has_code_column"] and results["has_description_column"]:
            subset = df[[code_column, description_column]]
            results["null_codes"] = subset[code_column].isnull().sum()
            results["null_descriptions"] = subset[description_column].isnull().sum()
            results["unique_codes"] = subset[code_column].nunique()
            results["duplicate_codes"] = len(subset) - results["unique_codes"]
            results["sample"] = subset.head(sample_size).to_dict(orient="records")
        else:
            results["valid"] = False
            results["error"] = "Required columns not found"
        
        return results
        
    except Exception as e:
        logger.error(f"Error validating file {file_path}: {e}")
        return {
            "valid": False,
            "error": str(e)
        }


def merge_mappers(mapper1, mapper2, name: str = "MergedMapper"):
    """
    Merge two code mappers into one.
    
    Args:
        mapper1: First CodeMapper
        mapper2: Second CodeMapper
        name: Name for merged mapper
        
    Returns:
        New CodeMapper with combined mappings
    """
    from .mapper import CodeMapper
    
    merged_mapping = {**mapper1.mapping, **mapper2.mapping}
    
    logger.info(
        f"Merged {len(mapper1)} and {len(mapper2)} codes "
        f"into {len(merged_mapping)} total codes"
    )
    
    return CodeMapper(merged_mapping, name=name)


def find_missing_codes(
    df: pd.DataFrame,
    code_column: str,
    mapper,
    return_dataframe: bool = True
) -> Optional[pd.DataFrame]:
    """
    Find codes in a DataFrame that are missing from a mapper.
    
    Args:
        df: DataFrame containing codes
        code_column: Name of column with codes
        mapper: CodeMapper instance
        return_dataframe: If True, return DataFrame of missing codes
        
    Returns:
        DataFrame with missing codes or None
    """
    if code_column not in df.columns:
        raise ValueError(f"Column '{code_column}' not found in DataFrame")
    
    codes = df[code_column].dropna().unique()
    missing = [code for code in codes if not mapper.code_exists(code)]
    
    logger.info(
        f"Found {len(missing)} missing codes out of "
        f"{len(codes)} unique codes ({len(missing)/len(codes)*100:.1f}%)"
    )
    
    if return_dataframe:
        return pd.DataFrame({"missing_code": missing})
    
    return missing


def enrich_dataframe(
    df: pd.DataFrame,
    code_column: str,
    mapper,
    description_column: str = "description",
    inplace: bool = False
) -> pd.DataFrame:
    """
    Add description column to DataFrame based on code column.
    
    Args:
        df: DataFrame with code column
        code_column: Name of column containing codes
        mapper: CodeMapper instance
        description_column: Name for new description column
        inplace: Modify DataFrame inplace
        
    Returns:
        DataFrame with added description column
    """
    if not inplace:
        df = df.copy()
    
    if code_column not in df.columns:
        raise ValueError(f"Column '{code_column}' not found in DataFrame")
    
    df[description_column] = df[code_column].apply(
        lambda code: mapper.get_description(code, default="Unknown")
    )
    
    logger.info(f"Added '{description_column}' column to DataFrame")
    
    return df


def export_mapper_to_csv(
    mapper,
    output_path: str,
    encoding: str = "utf-8"
):
    """
    Export a mapper to CSV file.
    
    Args:
        mapper: CodeMapper instance
        output_path: Path for output CSV file
        encoding: File encoding
    """
    df = pd.DataFrame([
        {"code": code, "description": desc}
        for code, desc in mapper.mapping.items()
    ])
    
    df.to_csv(output_path, index=False, encoding=encoding)
    logger.info(f"Exported {len(df)} mappings to {output_path}")
