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
import csv
import io
import re

from .composite import parse_composite_code, extract_plain_code

logger = logging.getLogger(__name__)


def _detect_encoding(file_path: Path, sample_size: int = 8192) -> str:
    """
    Auto-detect file encoding by trying common encodings.
    
    Args:
        file_path: Path to file
        sample_size: Number of bytes to sample
        
    Returns:
        Detected encoding string
    """
    candidates = ["utf-8", "iso-8859-1", "cp1252", "latin-1"]
    
    with open(file_path, "rb") as f:
        sample_bytes = f.read(sample_size)
    
    # Try chardet if available
    try:
        import chardet
        result = chardet.detect(sample_bytes)
        enc = result.get("encoding")
        if enc:
            try:
                sample_bytes.decode(enc)
                return enc
            except Exception:
                pass
    except ImportError:
        pass
    
    # Fallback: try common encodings
    for enc in candidates:
        try:
            sample_bytes.decode(enc)
            return enc
        except Exception:
            continue
    
    return "latin-1"  # Safe fallback


def _detect_delimiter(sample_text: str) -> Optional[str]:
    """
    Auto-detect CSV delimiter from sample text.
    
    Args:
        sample_text: Sample of file content
        
    Returns:
        Detected delimiter or None if fixed-width format
    """
    try:
        dialect = csv.Sniffer().sniff(sample_text, delimiters=",\t|;")
        return dialect.delimiter
    except Exception:
        return None


def _parse_fixed_width_line(line: str) -> tuple:
    """
    Parse a fixed-width format line into (code, short_desc, long_desc).
    
    The format appears to be:
    - First column: code (variable length, ends when multiple spaces appear)
    - Second column: short description
    - Third column: long description (padded with spaces)
    
    Args:
        line: A single line from the file
        
    Returns:
        Tuple of (code, description) where description is the long description
    """
    # Remove trailing whitespace and carriage return
    line = line.rstrip()
    
    if not line:
        return None, None
    
    # Split by 2+ consecutive spaces to separate columns
    parts = re.split(r'\s{2,}', line)
    
    if len(parts) >= 3:
        # Format: code, short_desc, long_desc
        code = parts[0].strip()
        # Use long description (last part), fall back to short if needed
        description = parts[-1].strip() if parts[-1].strip() else parts[1].strip()
        return code, description
    elif len(parts) == 2:
        # Format: code, description
        return parts[0].strip(), parts[1].strip()
    elif len(parts) == 1:
        # Only code, no description
        return parts[0].strip(), ""
    
    return None, None


def _read_file_robust(
    file_path: Path,
    code_column: str = "code",
    description_column: str = "description",
    delimiter: Optional[str] = None,
    encoding: Optional[str] = None,
    **read_csv_kwargs
) -> pd.DataFrame:
    """
    Robustly read a mapping file, handling various formats and encodings.
    
    Args:
        file_path: Path to the file
        code_column: Expected code column name
        description_column: Expected description column name
        delimiter: File delimiter (auto-detected if None)
        encoding: File encoding (auto-detected if None)
        **read_csv_kwargs: Additional pandas read_csv arguments
        
    Returns:
        DataFrame with 'code' and 'description' columns
    """
    # Auto-detect encoding if not provided
    enc_to_use = encoding if encoding else _detect_encoding(file_path)
    logger.debug(f"Using encoding: {enc_to_use}")
    
    # Read sample for format detection
    with open(file_path, "r", encoding=enc_to_use, errors="replace") as f:
        sample_lines = [f.readline() for _ in range(10)]
        sample_text = "".join(sample_lines)
    
    # Check if file has header row
    first_line = sample_lines[0].strip() if sample_lines else ""
    has_header = (
        code_column.lower() in first_line.lower() or
        description_column.lower() in first_line.lower() or
        "code" in first_line.lower()
    )
    
    # Auto-detect delimiter
    sep_to_use = delimiter
    if sep_to_use is None:
        sep_to_use = _detect_delimiter(sample_text)
    
    # Check if this looks like fixed-width format (multiple consecutive spaces)
    is_fixed_width = sep_to_use is None or re.search(r'\s{3,}', first_line)
    
    if is_fixed_width and not has_header:
        # Parse as fixed-width format
        logger.info(f"Detected fixed-width format for {file_path.name}")
        
        records = []
        with open(file_path, "r", encoding=enc_to_use, errors="replace") as f:
            for line in f:
                code, desc = _parse_fixed_width_line(line)
                if code:
                    records.append({"code": code, "description": desc})
        
        df = pd.DataFrame(records)
        logger.info(f"Parsed {len(df)} records from fixed-width file")
        return df
    
    # Try standard CSV/TSV parsing
    try:
        df = pd.read_csv(
            file_path,
            sep=sep_to_use if sep_to_use else ",",
            encoding=enc_to_use,
            engine="python",
            dtype=str,
            header=0 if has_header else None,
            on_bad_lines="warn",
            **read_csv_kwargs
        )
        
        # If no header, assign default column names
        if not has_header:
            if len(df.columns) >= 2:
                df.columns = ["code", "description"] + [f"col_{i}" for i in range(2, len(df.columns))]
            elif len(df.columns) == 1:
                df.columns = ["code"]
                df["description"] = ""
        
        # Rename columns if needed
        col_mapping = {}
        for col in df.columns:
            col_lower = str(col).lower()
            if col_lower == code_column.lower() and col != "code":
                col_mapping[col] = "code"
            elif col_lower == description_column.lower() and col != "description":
                col_mapping[col] = "description"
        
        if col_mapping:
            df = df.rename(columns=col_mapping)
        
        return df
        
    except Exception as e:
        logger.warning(f"Standard CSV parsing failed: {e}, trying fixed-width fallback")
        
        # Fallback: parse as fixed-width
        records = []
        with open(file_path, "r", encoding=enc_to_use, errors="replace") as f:
            for line in f:
                code, desc = _parse_fixed_width_line(line)
                if code:
                    records.append({"code": code, "description": desc})
        
        return pd.DataFrame(records)


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
        delimiter: Optional[str] = None,
        encoding: Optional[str] = None,
        name: Optional[str] = None,
        code_type: str = "generic",
        **read_csv_kwargs
    ) -> "CodeMapper":
        """
        Load mapper from a CSV/TXT file.
        
        Supports various file formats including:
        - Standard CSV/TSV files with headers
        - Fixed-width format files (common in Canadian medical code files)
        - Various encodings (auto-detected if not specified)
        
        Args:
            file_path: Path to the mapping file
            code_column: Name of the column containing codes (default: 'code')
            description_column: Name of the column containing descriptions (default: 'description')
            delimiter: File delimiter (auto-detected if None)
            encoding: File encoding (auto-detected if None)
            name: Name for this mapper (defaults to filename)
            code_type: Type of codes
            **read_csv_kwargs: Additional arguments for pd.read_csv
            
        Returns:
            CodeMapper instance
        """
        file_path = Path(file_path)
        
        if not file_path.exists():
            raise FileNotFoundError(f"Mapping file not found: {file_path}")
        
        # Read the file using robust parser (auto-detects encoding and format)
        df = _read_file_robust(
            file_path,
            code_column=code_column,
            description_column=description_column,
            delimiter=delimiter,
            encoding=encoding,
            **read_csv_kwargs
        )
        
        # For fixed-width files, columns are already named 'code' and 'description'
        # Check if requested columns exist, otherwise use defaults
        actual_code_col = "code" if "code" in df.columns else code_column
        actual_desc_col = "description" if "description" in df.columns else description_column
        
        if actual_code_col not in df.columns:
            raise ValueError(
                f"Code column '{actual_code_col}' not found. "
                f"Available columns: {df.columns.tolist()}"
            )
        if actual_desc_col not in df.columns:
            raise ValueError(
                f"Description column '{actual_desc_col}' not found. "
                f"Available columns: {df.columns.tolist()}"
            )
        
        # Create mapping dictionary, handling NaN values
        df = df[[actual_code_col, actual_desc_col]].dropna()
        df[actual_code_col] = df[actual_code_col].astype(str).str.strip()
        df[actual_desc_col] = df[actual_desc_col].astype(str).str.strip()
        
        mapping_dict = dict(zip(df[actual_code_col], df[actual_desc_col]))
        
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
        # else:
        #     if update_stats:
        #         self._stats["misses"] += 1
        #     logger.debug(f"Code not found: {lookup_code}")
        #     return default
        min_leaf_len = 1  # keep flexible; change if you want a stricter lower bound
        leaf = lookup_code
        found = None
        for L in range(len(leaf) - 1, min_leaf_len - 1, -1):
            candidate_leaf = leaf[:L]
            # try plain candidate
            if candidate_leaf in self.mapping:
                found = self.mapping[candidate_leaf]
                break

        if found:
            if update_stats:
                self._stats["hits"] += 1
            return found

        # Not found: count a miss and return default
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
