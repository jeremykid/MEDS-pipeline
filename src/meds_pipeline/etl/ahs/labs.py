# src/meds_pipeline/etl/ahs/labs.py
"""
AHS Lab Results ETL Component

Data sources:
- rmt22884_lab_2012_2016_20211105.sas7bdat: Lab results 2012-2016
- rmt22884_lab_2017_2021_20211105.sas7bdat: Lab results 2017-2021

Expected columns:
- PATID: Patient identifier
- TEST_VRFY_DTTM: Test verification datetime
- TEST_CD: Local test code (e.g., HGB, CREA, K)
- TEST_NM: Test name (e.g., Hemoglobin)
- TEST_RSLT: Result value (may have < or > prefixes)
- TEST_UOFM: Unit of measure

Example data:
PATID  TEST_VRFY_DTTM       TEST_CD  TEST_NM     TEST_RSLT  TEST_UOFM
2.0    2012-04-26 08:57:00  HGB      Hemoglobin  110        g/L
"""
from __future__ import annotations

import pandas as pd
import pyreadstat
from ..base import ComponentETL
from ..registry import register


# LOINC mappings for codes that EXIST in the AHS data
# Format: TEST_CD -> (LOINC code, unit)
# VERIFIED against TEST_NM column in lab_combined.parquet
LOINC_MAPPINGS = {
    # =========================================================================
    # Hematology - LOINC 718-7 (Hemoglobin)
    # =========================================================================
    'HGB':   ('718-7', 'g/L'),   # TEST_NM: "Hemoglobin" (8.3M)
    'HGB1':  ('718-7', 'g/L'),   # TEST_NM: "HEMOGLOBIN" (108)
    
    # =========================================================================
    # Electrolytes - Potassium LOINC 2823-3
    # =========================================================================
    'K':   ('2823-3', 'mmol/L'),  # TEST_NM: "Potassium" (7.6M)
    'IK':  ('2823-3', 'mmol/L'),  # TEST_NM: "Potassium" (439)
    'UAK': ('2823-3', 'mmol/L'),  # TEST_NM: "Potassium" (27)
    'KR':  ('2823-3', 'mmol/L'),  # TEST_NM: "Potassium" (11)
    'WBK': ('2823-3', 'mmol/L'),  # TEST_NM: "Potassium" (169)
    
    # =========================================================================
    # Electrolytes - Sodium LOINC 2951-2
    # =========================================================================
    'NA': ('2951-2', 'mmol/L'),  # TEST_NM: "Sodium" (7.5M)

    # =========================================================================
    # Kidney Function - Creatinine LOINC 2160-0
    # =========================================================================
    'CREA':   ('2160-0', 'µmol/L'),  # TEST_NM: "Creatinine" (7.8M)
    'CCREA1': ('2160-0', 'µmol/L'),  # TEST_NM: "CREATININE" (673)
    'FCREA':  ('2160-0', 'µmol/L'),  # TEST_NM: "CREATININE" (426)
    
    # =========================================================================
    # Kidney Function - eGFR LOINC 62238-1
    # =========================================================================
    'GFR':   ('62238-1', 'mL/min/1.73m²'),  # TEST_NM: "Glomerular Filtration Rate Estimate"
    'GFRE1': ('62238-1', 'mL/min/1.73m²'),  # TEST_NM: "GFR ESTIMATED"
    'GFR2':  ('62238-1', 'mL/min/1.73m²'),  # TEST_NM: "ESTIMATED GFR"
    'GFRF2': ('62238-1', 'mL/min/1.73m²'),  # TEST_NM: "ESTIMATED GFR"
    'GFRF1': ('62238-1', 'mL/min/1.73m²'),  # TEST_NM: "ESTIMATED GFR"
    'CREGF': ('62238-1', 'mL/min/1.73m²'),  # TEST_NM: "Calculated GFR" (was wrongly creatinine!)
    
    # =========================================================================
    # Cardiac - Troponin I LOINC 10839-9
    # =========================================================================
    'TROP':       ('10839-9', 'ng/L'),  # TEST_NM: "Troponin I" (895K)
    'TROPI':      ('10839-9', 'ng/L'),  # TEST_NM: "TROPONIN I" (94K)
    'TROPI1':     ('10839-9', 'ng/L'),  # TEST_NM: "TROPONIN I" (28K)
    'TROPI3':     ('10839-9', 'ng/L'),  # TEST_NM: "TROPONIN I" (65K)
    '684792.00':  ('10839-9', 'ng/L'),  # TEST_NM: "Troponin-I" (1K)
    '6703961.00': ('10839-9', 'ng/L'),  # TEST_NM: "Troponin I Vidas" (950)
    
    # =========================================================================
    # Cardiac - Troponin T LOINC 67151-1 (high-sensitivity)
    # =========================================================================
    'TROPTHS':     ('67151-1', 'ng/L'),  # TEST_NM: "TROPONIN T,HIGH SENSITIVITY" (1.7K)
    'TROPTN':      ('67151-1', 'ng/L'),  # TEST_NM: "TROPONIN T,SEMI QUANTITATIVE" (9)
    'TROPTSQ':     ('67151-1', 'ng/L'),  # TEST_NM: "TROPONIN T,SEMI QUANTITATIVE" (227)
    'TROPTSQN':    ('67151-1', 'ng/L'),  # TEST_NM: "TROP T,SEMI QUANTITATIVE NUMER" (2.6K)
    '29919347.00': ('67151-1', 'ng/L'),  # TEST_NM: "Troponin T-HS" (16K)
    '29919350.00': ('67151-1', 'ng/L'),  # TEST_NM: "Troponin T-Calc" (890)
    
    # =========================================================================
    # Cardiac - BNP LOINC 30934-4
    # =========================================================================
    'BNP':  ('30934-4', 'ng/L'),  # TEST_NM: "BNP" (178K)
    'BNP1': ('30934-4', 'ng/L'),  # TEST_NM: "B-TYPE NATRIURETIC PEPTIDE" (19K)
    
    # =========================================================================
    # Cardiac - NT-proBNP LOINC 33762-6
    # =========================================================================
    'NTBNP':       ('33762-6', 'ng/L'),  # TEST_NM: "NT proBNP, Research UAH" (36)
    'BNPNT':       ('33762-6', 'ng/L'),  # TEST_NM: "NT-pro BRAIN NATRIURET PEPTIDE" (2K)
    'PBNP':        ('33762-6', 'ng/L'),  # TEST_NM: "NT proBNP" (2.5K)
    '31470156.00': ('33762-6', 'ng/L'),  # TEST_NM: "NT-proBNP" (4.6K)
}


@register("labs")
class AHSLabs(ComponentETL):
    """
    Component: AHS lab results → MEDS event stream
    
    Produces lab measurement events with LOINC codes where mappable,
    falling back to local AHS codes for unmapped tests.
    
    Expected raw paths in config:
      - labs: Path to lab SAS or parquet file
    
    Output columns:
      - subject_id: Patient ID (from PATID)
      - time: Test verification timestamp (from TEST_VRFY_DTTM)
      - code: LAB//LOINC//{loinc_code} or LAB//AHS//{local_code}
      - numeric_value: Parsed result value
      - comparator: "<", "<=", ">", ">=" or None (preserves clinical meaning)
      - unit: Unit of measure
      - event_type: "lab"
      - code_system: "LOINC" or "AHS_LAB"
      - source_table: "rmt22884_lab"
    
    Example MEDS output:
    
    | subject_id | time                | code                | numeric_value | comparator | unit   |
    |------------|---------------------|---------------------|---------------|------------|--------|
    | 2          | 2012-04-26 08:57:00 | LAB//LOINC//718-7   | 110.0         | None       | g/L    |
    | 2          | 2012-04-26 12:47:00 | LAB//LOINC//2823-3  | 4.7           | None       | mmol/L |
    | 5          | 2013-01-15 09:30:00 | LAB//LOINC//10839-9 | 0.01          | <          | ng/L   |
    | 5          | 2013-01-15 09:30:00 | LAB//LOINC//30934-4 | 100.0         | >          | ng/L   |
    
    Note: comparator preserves clinical meaning:
      - "<0.01" for troponin means "below detection limit" (normal)
      - ">100" means "above measurable range" (very abnormal)
    
    """
    
    @staticmethod
    def _clean_bytes(val):
        """
        Clean byte-encoded values from SAS files.
        
        SAS7BDAT files may store string values as bytes (b'value').
        This converts them to clean strings.
        
        Args:
            val: Value that may be bytes, string, or None
            
        Returns:
            Cleaned string or None
        """
        if pd.isna(val):
            return None
        if isinstance(val, bytes):
            return val.decode('utf-8', errors='ignore').strip()
        return str(val).strip()
    
    @staticmethod
    def _parse_numeric_with_comparator(result_str):
        """
        Parse numeric value and comparator from lab result string.
        
        Preserves inequality information which is clinically important:
        - "<0.5" means below detection limit (e.g., normal troponin)
        - ">60" means above measurable range (e.g., GFR > 60)
        
        Handles formats like:
        - "110" -> (110.0, None)
        - "<0.10" -> (0.1, "<")
        - ">60  (MDRD equation)..." -> (60.0, ">")
        - "negative" -> (None, None)
        
        Args:
            result_str: Raw result string
            
        Returns:
            Tuple of (numeric_value, comparator)
            comparator is one of: "<", "<=", ">", ">=", or None
        """
        import re
        
        if pd.isna(result_str) or result_str is None:
            return None, None
        
        s = str(result_str).strip()
        if not s:
            return None, None
        
        # Check for inequality prefixes (order matters: check longer ones first)
        comparator = None
        for prefix in ['<=', '>=', '<', '>']:
            if s.startswith(prefix):
                comparator = prefix
                s = s[len(prefix):].strip()
                break
        
        # Try to extract the first number from the remaining string
        # This handles cases like ">60  (MDRD equation)..."
        match = re.match(r'^([+-]?\d*\.?\d+)', s)
        if match:
            try:
                return float(match.group(1)), comparator
            except (ValueError, TypeError):
                pass
        
        # Fallback: try to parse the whole string
        try:
            return float(s), comparator
        except (ValueError, TypeError):
            return None, None
    
    def _map_code(self, test_cd):
        """
        Map local TEST_CD to MEDS code.
        
        Args:
            test_cd: Local AHS test code (e.g., 'HGB')
            
        Returns:
            Tuple of (meds_code, code_system)
        """
        if pd.isna(test_cd) or not test_cd:
            return None, None
        
        test_cd_upper = str(test_cd).upper().strip()
        
        # Check LOINC mapping
        if test_cd_upper in LOINC_MAPPINGS:
            loinc, _ = LOINC_MAPPINGS[test_cd_upper]
            return f"LAB//LOINC//{loinc}", "LOINC"
        
        # Fallback to local code
        return f"LAB//AHS//{test_cd_upper}", "AHS_LAB"
    
    def run_core(self) -> pd.DataFrame:
        """
        Convert AHS lab results to MEDS core format.
        
        Returns:
            DataFrame with MEDS-compliant lab events
        """
        path = self.cfg["raw_paths"]["labs"]
        
        # Load SAS file
        df, _ = pyreadstat.read_sas7bdat(path, output_format="pandas")
        print(f"📊 Loaded {len(df):,} lab records")
        
        # Clean byte-encoded columns
        byte_cols = ['TEST_CD', 'TEST_NM', 'TEST_RSLT', 'TEST_UOFM']
        for col in byte_cols:
            if col in df.columns:
                df[col] = df[col].apply(self._clean_bytes)
        
        # Map codes
        mapped = df['TEST_CD'].apply(self._map_code)
        df['meds_code'] = [m[0] for m in mapped]
        df['meds_code_system'] = [m[1] for m in mapped]
        
        # Parse numeric results with comparator (preserves <, >, etc.)
        parsed = df['TEST_RSLT'].apply(self._parse_numeric_with_comparator)
        df['value_num'] = [p[0] for p in parsed]
        df['comparator'] = [p[1] for p in parsed]
        
        # Get unit - prefer data unit, fallback to LOINC mapping unit
        def get_unit(row):
            if pd.notna(row.get('TEST_UOFM')) and row['TEST_UOFM']:
                return row['TEST_UOFM']
            test_cd = str(row.get('TEST_CD', '')).upper().strip()
            if test_cd in LOINC_MAPPINGS:
                return LOINC_MAPPINGS[test_cd][1]
            return ''
        
        df['unit'] = df.apply(get_unit, axis=1)
        
        # Build output
        out = pd.DataFrame({
            'subject_id': df['PATID'].astype('Int64').astype(str),
            'time': pd.to_datetime(df['TEST_VRFY_DTTM'], errors='coerce'),
            'code': df['meds_code'],
            'numeric_value': df['value_num'],
            'comparator': df['comparator'],  # <, <=, >, >= or None
            'unit': df['unit'],
            'event_type': 'lab',
            'code_system': df['meds_code_system'],
            'source_table': 'rmt22884_lab',
        })
        
        # Filter invalid rows
        out = out.dropna(subset=['subject_id', 'time', 'code'])
        out = out[out['subject_id'].str.strip() != '']
        out = out[out['code'].notna() & (out['code'] != '')]
        out = out.reset_index(drop=True)
        
        # Stats
        loinc_count = (out['code_system'] == 'LOINC').sum()
        local_count = (out['code_system'] == 'AHS_LAB').sum()
        print(f"✅ Generated {len(out):,} lab events ({loinc_count:,} LOINC, {local_count:,} local)")
        
        return out
