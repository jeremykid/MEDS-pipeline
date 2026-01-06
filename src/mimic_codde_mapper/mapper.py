"""
Core MIMICCodeMapper class for mapping MIMIC medical codes to descriptions.

Supports:
- ICD-9/ICD-10 diagnosis codes
- ICD-9/ICD-10 procedure codes
- Composite code format (e.g., DIAGNOSIS//ICD//10//R531)
- Hierarchical fallback matching (code truncation from right)
"""

import pandas as pd
from pathlib import Path
from typing import Dict, Optional, Union, List
import logging

from .composite import parse_composite_code, extract_plain_code

logger = logging.getLogger(__name__)


class MIMICCodeMapper:
    """
    A flexible mapper for MIMIC medical coding systems.
    
    Supports ICD-9 and ICD-10 codes for both diagnoses and procedures,
    with hierarchical fallback matching.
    
    Usage:
        # Initialize with MIMIC diagnosis mapping
        dx_mapper = MIMICCodeMapper.from_mimic_file(
            "d_icd_diagnoses.csv.gz",
            code_type="diagnosis"
        )
        
        # Get description for a code (with hierarchical fallback)
        desc = dx_mapper.get_description("DIAGNOSIS//ICD//10//R531")
        
        # Batch lookup
        descriptions = dx_mapper.get_descriptions(["R531", "R53"])
    """
    
    def __init__(
        self,
        mapping_dict: Dict[str, str],
        name: str = "MIMICCodeMapper",
        code_type: str = "generic"
    ):
        """
        Initialize MIMICCodeMapper with a mapping dictionary.
        
        Args:
            mapping_dict: Dictionary mapping codes to descriptions
            name: Name of this mapper (e.g., "diagnosis_10", "procedure_9")
            code_type: Type of codes (e.g., "diagnosis", "procedure")
        """
        self.mapping = mapping_dict
        self.name = name
        self.code_type = code_type
        self._stats = {
            "total_codes": len(mapping_dict),
            "lookups": 0,
            "hits": 0,
            "misses": 0
        }
        logger.info(f"Initialized {name} mapper with {len(mapping_dict)} codes")
    
    @classmethod
    def from_mimic_file(
        cls,
        file_path: Union[str, Path],
        code_type: str = "diagnosis",
        icd_version: Optional[int] = None,
        name: Optional[str] = None
    ) -> "MIMICCodeMapper":
        """
        Load mapper from a MIMIC CSV file (d_icd_diagnoses.csv.gz or d_icd_procedures.csv.gz).
        
        Args:
            file_path: Path to the MIMIC mapping file
            code_type: Type of codes ("diagnosis" or "procedure")
            icd_version: ICD version to filter (9 or 10). If None, loads all versions.
            name: Name for this mapper (defaults to generated name)
            
        Returns:
            MIMICCodeMapper instance
        """
        file_path = Path(file_path)
        
        if not file_path.exists():
            raise FileNotFoundError(f"Mapping file not found: {file_path}")
        
        # Read the MIMIC file (CSV format, possibly gzipped)
        compression = 'gzip' if str(file_path).endswith('.gz') else None
        df = pd.read_csv(file_path, compression=compression, dtype=str)
        
        # MIMIC format: icd_code, icd_version, long_title
        required_cols = ['icd_code', 'icd_version', 'long_title']
        for col in required_cols:
            if col not in df.columns:
                raise ValueError(f"Required column '{col}' not found. Available: {df.columns.tolist()}")
        
        # Filter by ICD version if specified
        if icd_version is not None:
            df = df[df['icd_version'].astype(str) == str(icd_version)]
        
        # Create mapping dictionary
        df = df[['icd_code', 'long_title']].dropna()
        df['icd_code'] = df['icd_code'].astype(str).str.strip()
        df['long_title'] = df['long_title'].astype(str).str.strip()
        
        mapping_dict = dict(zip(df['icd_code'], df['long_title']))
        
        # Generate mapper name
        if name is None:
            version_str = f"_{icd_version}" if icd_version else ""
            name = f"{code_type}{version_str}"
        
        logger.info(f"Loaded {len(mapping_dict)} mappings from {file_path.name}")
        
        return cls(mapping_dict, name=name, code_type=code_type)
    
    @classmethod
    def from_dataframe(
        cls,
        df: pd.DataFrame,
        code_column: str = "icd_code",
        description_column: str = "long_title",
        name: str = "DataFrameMapper",
        code_type: str = "generic"
    ) -> "MIMICCodeMapper":
        """
        Create mapper from a pandas DataFrame.
        
        Args:
            df: DataFrame containing code-description mappings
            code_column: Name of code column
            description_column: Name of description column
            name: Name for this mapper
            code_type: Type of codes
            
        Returns:
            MIMICCodeMapper instance
        """
        if code_column not in df.columns or description_column not in df.columns:
            raise ValueError(
                f"Columns {code_column} and {description_column} must exist in DataFrame"
            )
        
        df = df[[code_column, description_column]].dropna()
        df[code_column] = df[code_column].astype(str).str.strip()
        df[description_column] = df[description_column].astype(str).str.strip()
        
        mapping_dict = dict(zip(df[code_column], df[description_column]))
        
        return cls(mapping_dict, name=name, code_type=code_type)
    
    def get_description(
        self,
        code: str,
        default: str = "Unknown",
        update_stats: bool = True,
        min_leaf_len: int = 1
    ) -> str:
        """
        Get description for a single code with hierarchical fallback.
        
        Supports both plain codes and composite format:
        - Plain: "R531" or "0010"
        - Composite: "DIAGNOSIS//ICD//10//R531" or "PROCEDURE//ICD//9//8938"
        
        For composite codes, only the code portion is used for lookup.
        
        Hierarchical fallback:
        - If exact match not found, progressively truncate from right
        - e.g., R531 -> R53 -> R5 until match found or min_leaf_len reached
        
        Args:
            code: Medical code to lookup (plain or composite format)
            default: Default value if code not found
            update_stats: Whether to update lookup statistics
            min_leaf_len: Minimum code length to try during fallback
            
        Returns:
            Description string
        """
        code_str = str(code).strip()
        
        # Parse composite format (e.g., "DIAGNOSIS//ICD//10//R531")
        parsed = parse_composite_code(code_str)
        if parsed:
            lookup_code = parsed['code']
            logger.debug(f"Parsed composite code: {code_str} -> {lookup_code}")
        else:
            lookup_code = code_str
        
        if update_stats:
            self._stats["lookups"] += 1
        
        # Try exact match first
        if lookup_code in self.mapping:
            if update_stats:
                self._stats["hits"] += 1
            return self.mapping[lookup_code]
        
        # Hierarchical fallback: progressively truncate from right
        found = None
        for L in range(len(lookup_code) - 1, min_leaf_len - 1, -1):
            candidate = lookup_code[:L]
            if candidate in self.mapping:
                found = self.mapping[candidate]
                logger.debug(f"Fallback match: {lookup_code} -> {candidate}")
                break
        
        if found:
            if update_stats:
                self._stats["hits"] += 1
            return found
        
        # Not found
        if update_stats:
            self._stats["misses"] += 1
        logger.debug(f"Code not found after fallback: {code_str}")
        return default
    
    def get_descriptions(
        self,
        codes: List[str],
        default: str = "Unknown",
        return_dataframe: bool = False
    ) -> Union[List[str], pd.DataFrame]:
        """
        Get descriptions for multiple codes (batch lookup).
        
        Args:
            codes: List of medical codes
            default: Default value for codes not found
            return_dataframe: If True, return DataFrame instead of list
            
        Returns:
            List of descriptions or DataFrame with code-description pairs
        """
        descriptions = [
            self.get_description(code, default=default, update_stats=True)
            for code in codes
        ]
        
        if return_dataframe:
            return pd.DataFrame({
                "code": codes,
                "description": descriptions
            })
        
        return descriptions
    
    def code_exists(self, code: str) -> bool:
        """
        Check if a code exists in the mapping (exact match only).
        
        Args:
            code: Medical code (plain or composite format)
            
        Returns:
            True if code exists in mapping, False otherwise
        """
        lookup_code = extract_plain_code(code)
        return lookup_code in self.mapping
    
    def get_stats(self) -> Dict:
        """Get lookup statistics."""
        stats = self._stats.copy()
        if stats["lookups"] > 0:
            stats["hit_rate"] = stats["hits"] / stats["lookups"]
        else:
            stats["hit_rate"] = 0.0
        return stats
    
    def __len__(self) -> int:
        """Return number of codes in mapper."""
        return len(self.mapping)
    
    def __repr__(self) -> str:
        return f"MIMICCodeMapper(name={self.name}, codes={len(self.mapping)})"
