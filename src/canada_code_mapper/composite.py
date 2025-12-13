"""
Utility functions for parsing composite medical codes.

Supports parsing composite code strings like:
- DIAGNOSIS//ICD10CA//M1000
- PROCEDURE//CCI//1VG52HA

These composite codes contain:
1. Prefix (e.g., DIAGNOSIS, PROCEDURE)
2. System (e.g., ICD10CA, CCI)
3. Code (the actual medical code)
"""

import re
from typing import Optional, Dict

# Regular expression to match composite code format: PREFIX//SYSTEM//CODE
# Example: "DIAGNOSIS//ICD10CA//M1000"
_COMPOSITE_RE = re.compile(
    r'^\s*(?P<prefix>[A-Za-z_]+)\s*//\s*(?P<system>[^/]+)\s*//\s*(?P<code>.+?)\s*$',
    flags=re.IGNORECASE
)

# Map various system name variations to canonical internal names
SYSTEM_ALIASES = {
    'ICD10CA': 'icd10ca',
    'ICD-10-CA': 'icd10ca',
    'ICD10-CA': 'icd10ca',
    'ICD_10_CA': 'icd10ca',
    'CCI': 'cci',
    # Add more aliases as needed
}


def parse_composite_code(code_string: str) -> Optional[Dict[str, str]]:
    """
    Parse composite medical code strings.
    
    Recognizes format: PREFIX//SYSTEM//CODE
    
    Examples:
        >>> parse_composite_code("DIAGNOSIS//ICD10CA//M1000")
        {'prefix': 'DIAGNOSIS', 'system': 'icd10ca', 'code': 'M1000'}
        
        >>> parse_composite_code("PROCEDURE//CCI//1VG52HA")
        {'prefix': 'PROCEDURE', 'system': 'cci', 'code': '1VG52HA'}
        
        >>> parse_composite_code("A00.0")  # Plain code
        None
    
    Args:
        code_string: Input code string (may be plain or composite format)
        
    Returns:
        Dictionary with keys 'prefix', 'system', 'code' if composite format detected,
        None if input is a plain code
    """
    if not isinstance(code_string, str):
        return None
    
    match = _COMPOSITE_RE.match(code_string)
    if not match:
        return None
    
    # Extract components
    prefix = match.group('prefix').upper()
    system_raw = match.group('system').upper().strip()
    code = match.group('code').strip()
    
    # Normalize system name using aliases
    system = SYSTEM_ALIASES.get(system_raw, system_raw.lower())
    
    return {
        "prefix": prefix,
        "system": system,
        "code": code
    }


def is_composite_code(code_string: str) -> bool:
    """
    Check if a code string is in composite format.
    
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
        >>> extract_plain_code("DIAGNOSIS//ICD10CA//M1000")
        'M1000'
        
        >>> extract_plain_code("A00.0")
        'A00.0'
    
    Args:
        code_string: Input code string (plain or composite)
        
    Returns:
        The plain code portion
    """
    parsed = parse_composite_code(code_string)
    if parsed:
        return parsed['code']
    return code_string


def extract_system(code_string: str) -> Optional[str]:
    """
    Extract the coding system from a composite code.
    
    Args:
        code_string: Input code string
        
    Returns:
        Normalized system name (e.g., 'icd10ca', 'cci') or None if plain code
    """
    parsed = parse_composite_code(code_string)
    if parsed:
        return parsed['system']
    return None
