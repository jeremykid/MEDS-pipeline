"""
Utility functions for parsing MIMIC composite medical codes.

Supports parsing composite code strings like:
- DIAGNOSIS//ICD//9//5723
- DIAGNOSIS//ICD//10//R531
- PROCEDURE//ICD//9//8938
- PROCEDURE//ICD//10//0QS734Z

These composite codes contain:
1. Prefix (e.g., DIAGNOSIS, PROCEDURE)
2. System (ICD)
3. Version (9 or 10)
4. Code (the actual medical code)
"""

import re
from typing import Optional, Dict

# Regular expression to match MIMIC composite code format: PREFIX//ICD//VERSION//CODE
# Example: "DIAGNOSIS//ICD//10//R531"
_MIMIC_COMPOSITE_RE = re.compile(
    r'^\s*(?P<prefix>[A-Za-z_]+)\s*//\s*(?P<system>ICD)\s*//\s*(?P<version>9|10)\s*//\s*(?P<code>.+?)\s*$',
    flags=re.IGNORECASE
)


def parse_composite_code(code_string: str) -> Optional[Dict[str, str]]:
    """
    Parse MIMIC composite medical code strings.
    
    Recognizes format: PREFIX//ICD//VERSION//CODE
    
    Examples:
        >>> parse_composite_code("DIAGNOSIS//ICD//10//R531")
        {'prefix': 'DIAGNOSIS', 'system': 'ICD', 'version': '10', 'code': 'R531'}
        
        >>> parse_composite_code("PROCEDURE//ICD//9//8938")
        {'prefix': 'PROCEDURE', 'system': 'ICD', 'version': '9', 'code': '8938'}
        
        >>> parse_composite_code("R531")  # Plain code
        None
    
    Args:
        code_string: Input code string (may be plain or composite format)
        
    Returns:
        Dictionary with keys 'prefix', 'system', 'version', 'code' if composite format detected,
        None if input is a plain code
    """
    if not isinstance(code_string, str):
        return None
    
    match = _MIMIC_COMPOSITE_RE.match(code_string)
    if not match:
        return None
    
    # Extract components
    prefix = match.group('prefix').upper()
    system = match.group('system').upper()
    version = match.group('version')
    code = match.group('code').strip()
    
    return {
        "prefix": prefix,
        "system": system,
        "version": version,
        "code": code
    }


def is_composite_code(code_string: str) -> bool:
    """
    Check if a code string is in MIMIC composite format.
    
    Args:
        code_string: Input code string
        
    Returns:
        True if composite format detected, False otherwise
    """
    return parse_composite_code(code_string) is not None


def extract_plain_code(code_string: str) -> str:
    """
    Extract the plain code from either composite or plain format.
    
    Examples:
        >>> extract_plain_code("DIAGNOSIS//ICD//10//R531")
        'R531'
        
        >>> extract_plain_code("R531")
        'R531'
    
    Args:
        code_string: Input code string (composite or plain)
        
    Returns:
        Plain code portion
    """
    parsed = parse_composite_code(code_string)
    if parsed:
        return parsed['code']
    return str(code_string).strip()


def get_mapper_key(code_string: str) -> Optional[str]:
    """
    Get the mapper key (e.g., 'diagnosis_9', 'diagnosis_10', 'procedure_9', 'procedure_10')
    from a composite code string.
    
    Examples:
        >>> get_mapper_key("DIAGNOSIS//ICD//10//R531")
        'diagnosis_10'
        
        >>> get_mapper_key("PROCEDURE//ICD//9//8938")
        'procedure_9'
    
    Args:
        code_string: Input composite code string
        
    Returns:
        Mapper key string or None if not a composite code
    """
    parsed = parse_composite_code(code_string)
    if parsed:
        prefix = parsed['prefix'].lower()
        version = parsed['version']
        return f"{prefix}_{version}"
    return None
