"""
Core CodeMapper class for mapping medical codes to descriptions.

Supports:
- ICD-10-CA (diagnosis codes)
- CCI (procedure codes)
- Custom mapping files
- Composite code format (e.g., DIAGNOSIS//ICD10CA//M1000)
"""

import pandas as pd
from pathlib import Path
from typing import Dict, Optional, Union, List
import logging

from .composite import parse_composite_code, extract_plain_code

logger = logging.getLogger(__name__)


class CodeMapper:
    """
    A flexible mapper for medical coding systems.
    
    Usage:
        # Initialize with ICD-10-CA mapping
        icd_mapper = CodeMapper.from_file(
            "path/to/icd10ca.txt",
            code_column="code",
            description_column="description",
            delimiter="|"
        )
        
        # Get description for a code
        desc = icd_mapper.get_description("A00.0")
        
        # Batch lookup
        descriptions = icd_mapper.get_descriptions(["A00.0", "A00.1"])
    """
    
    def __init__(
        self,
        mapping_dict: Dict[str, str],
        name: str = "CodeMapper",
        code_type: str = "generic"
    ):
        """
        Initialize CodeMapper with a mapping dictionary.
        
        Args:
            mapping_dict: Dictionary mapping codes to descriptions
            name: Name of this mapper (e.g., "ICD-10-CA", "CCI")
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
    def from_file(
        cls,
        file_path: Union[str, Path],
        code_column: str = "code",
        description_column: str = "description",
        delimiter: str = ",",
        encoding: str = "utf-8",
        name: Optional[str] = None,
        code_type: str = "generic",
        **read_csv_kwargs
    ) -> "CodeMapper":
        """
        Load mapper from a CSV/TXT file.
        
        Args:
            file_path: Path to the mapping file
            code_column: Name of the column containing codes
            description_column: Name of the column containing descriptions
            delimiter: File delimiter (default: ',')
            encoding: File encoding (default: 'utf-8')
            name: Name for this mapper (defaults to filename)
            code_type: Type of codes
            **read_csv_kwargs: Additional arguments for pd.read_csv
            
        Returns:
            CodeMapper instance
        """
        file_path = Path(file_path)
        
        if not file_path.exists():
            raise FileNotFoundError(f"Mapping file not found: {file_path}")
        
        # Read the file
        df = pd.read_csv(
            file_path,
            delimiter=delimiter,
            encoding=encoding,
            **read_csv_kwargs
        )
        
        # Validate columns
        if code_column not in df.columns:
            raise ValueError(
                f"Code column '{code_column}' not found. "
                f"Available columns: {df.columns.tolist()}"
            )
        if description_column not in df.columns:
            raise ValueError(
                f"Description column '{description_column}' not found. "
                f"Available columns: {df.columns.tolist()}"
            )
        
        # Create mapping dictionary, handling NaN values
        df = df[[code_column, description_column]].dropna()
        df[code_column] = df[code_column].astype(str).str.strip()
        df[description_column] = df[description_column].astype(str).str.strip()
        
        mapping_dict = dict(zip(df[code_column], df[description_column]))
        
        mapper_name = name or file_path.stem
        
        logger.info(
            f"Loaded {len(mapping_dict)} mappings from {file_path.name}"
        )
        
        return cls(mapping_dict, name=mapper_name, code_type=code_type)
    
    @classmethod
    def from_dataframe(
        cls,
        df: pd.DataFrame,
        code_column: str = "code",
        description_column: str = "description",
        name: str = "DataFrameMapper",
        code_type: str = "generic"
    ) -> "CodeMapper":
        """
        Create mapper from a pandas DataFrame.
        
        Args:
            df: DataFrame containing code-description mappings
            code_column: Name of code column
            description_column: Name of description column
            name: Name for this mapper
            code_type: Type of codes
            
        Returns:
            CodeMapper instance
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
        update_stats: bool = True
    ) -> str:
        """
        Get description for a single code.
        
        Supports both plain codes and composite format:
        - Plain: "A00.0" or "M1000"
        - Composite: "DIAGNOSIS//ICD10CA//M1000" or "PROCEDURE//CCI//1VG52HA"
        
        For composite codes, only the code portion is used for lookup.
        
        Args:
            code: Medical code to lookup (plain or composite format)
            default: Default value if code not found
            update_stats: Whether to update lookup statistics
            
        Returns:
            Description string
        """
        code_str = str(code).strip()
        
        # Try to parse composite format (e.g., "DIAGNOSIS//ICD10CA//M1000")
        parsed = parse_composite_code(code_str)
        if parsed:
            # Extract just the code portion for lookup
            lookup_code = parsed['code']
            logger.debug(f"Parsed composite code: {code_str} -> {lookup_code}")
        else:
            # Plain code format
            lookup_code = code_str
        
        if update_stats:
            self._stats["lookups"] += 1
        
        if lookup_code in self.mapping:
            if update_stats:
                self._stats["hits"] += 1
            return self.mapping[lookup_code]
        else:
            if update_stats:
                self._stats["misses"] += 1
            logger.debug(f"Code not found: {lookup_code}")
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
        Check if a code exists in the mapping.
        
        Supports both plain and composite code formats.
        
        Args:
            code: Medical code (plain or composite format)
            
        Returns:
            True if code exists in mapping, False otherwise
        """
        code_str = str(code).strip()
        
        # Extract plain code from composite format if needed
        lookup_code = extract_plain_code(code_str)
        
        return lookup_code in self.mapping
    
    def get_codes(self) -> List[str]:
        """Get all available codes."""
        return list(self.mapping.keys())
    
    def get_stats(self) -> Dict:
        """Get lookup statistics."""
        stats = self._stats.copy()
        if stats["lookups"] > 0:
            stats["hit_rate"] = stats["hits"] / stats["lookups"]
        else:
            stats["hit_rate"] = 0.0
        return stats
    
    def reset_stats(self):
        """Reset lookup statistics."""
        self._stats = {
            "total_codes": len(self.mapping),
            "lookups": 0,
            "hits": 0,
            "misses": 0
        }
    
    def search(self, query: str, max_results: int = 10) -> pd.DataFrame:
        """
        Search for codes or descriptions containing the query string.
        
        Args:
            query: Search query string
            max_results: Maximum number of results to return
            
        Returns:
            DataFrame with matching code-description pairs
        """
        query = query.lower()
        matches = []
        
        for code, description in self.mapping.items():
            if query in code.lower() or query in description.lower():
                matches.append({"code": code, "description": description})
                if len(matches) >= max_results:
                    break
        
        return pd.DataFrame(matches)
    
    def __len__(self) -> int:
        """Return number of mappings."""
        return len(self.mapping)
    
    def __repr__(self) -> str:
        return (
            f"CodeMapper(name='{self.name}', "
            f"code_type='{self.code_type}', "
            f"total_codes={len(self.mapping)})"
        )
    
    def __getitem__(self, code: str) -> str:
        """Allow dict-like access: mapper[code]"""
        return self.get_description(code)
