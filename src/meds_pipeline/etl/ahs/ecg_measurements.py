# src/meds_pipeline/etl/ahs/ecg_measurements.py
"""
AHS ECG Global Measurements ETL Component

Data sources:
- rmt22884_ecg_20211105_df.pickle: ECG records with patient IDs. columns ->
  * PATID: Patient identifier
  * ecgId: ECG identifier (GUID in N'...' format)
  * dateAcquired: Timestamp of ECG recording
  * dateConfirmed: Confirmation timestamp

- Globalmeasurements.pickle: ECG measurement values. columns ->
  * ecgId: ECG identifier (clean GUID)
  * heartrate, qrsdur, qtint, qtcb, etc.: Measurement values

Example ECG record:
PATID    ecgId                                        dateAcquired
219126   N'407be100-3b36-11dc-4823-000206d60029'     2007-07-25 23:02:25

Example measurements:
ecgId                                   heartrate  qrsdur  qtint  qtcb
407be100-3b36-11dc-4823-000206d60029   63         74      392    402

Output codes follow format: ECG//{MEASUREMENT}
- ECG//HR (heart rate)
- ECG//QRS (QRS duration)
- ECG//QT (QT interval)
- etc.
"""
from __future__ import annotations

import pandas as pd
import re
from ..base import ComponentETL
from ..registry import register


# ECG measurement mappings: column_name -> (code, unit)
ECG_MEASUREMENTS = {
    'heartrate':     ('ECG//HR',                  'beats/min'),
    'rrint':         ('ECG//RR',                  'ms'),
    'pdur':          ('ECG//P_DURATION',          'ms'),
    'print':         ('ECG//PR',                  'ms'),
    'qrsdur':        ('ECG//QRS',                 'ms'),
    'qtint':         ('ECG//QT',                  'ms'),
    'qtcb':          ('ECG//QTC',                 'ms'),
    'qrsfrontaxis':  ('ECG//QRS_AXIS',            '°'),
    'tfrontaxis':    ('ECG//T_AXIS',              '°'),
    'atrialrate':    ('ECG//ATRIAL_RATE',         'beats/min'),
    'qtcf':          ('ECG//QTC_FRIDERICIA',      'ms'),
    'pfrontaxis':    ('ECG//P_AXIS',              '°'),
    'stfrontaxis':   ('ECG//ST_AXIS_FRONTAL',     '°'),
    'sthorizaxis':   ('ECG//ST_AXIS_HORIZONTAL',  '°'),
    'thorizaxis':    ('ECG//T_AXIS_HORIZONTAL',   '°'),
    'qrshorizaxis':  ('ECG//QRS_AXIS_HORIZONTAL', '°'),
    'i40frontaxis':  ('ECG//I40_AXIS',            '°'),
    't40frontaxis':  ('ECG//T40_AXIS',            '°'),
    'qonset':        ('ECG//Q_ONSET',             'ms'),
    'tonset':        ('ECG//T_ONSET',             'ms'),
    # Excluded: qtco (100% NULL)
}


@register("ecg_measurements")
class AHSECGMeasurements(ComponentETL):
    """
    Component: AHS ECG Global Measurements → MEDS event stream
    
    Joins ECG records (PATID ↔ ecgId) with global measurements (ecgId ↔ values)
    to produce individual measurement events per ECG.
    
    Expected raw paths in config:
      - ecg: Path to ECG records pickle (rmt22884_ecg_20211105_df.pickle)
      - ecg_measurements: Path to measurements pickle (Globalmeasurements.pickle)
    
    Output format:
      - subject_id: Patient ID (from PATID)
      - time: ECG acquisition timestamp (from dateAcquired)
      - code: ECG//HR, ECG//QRS, ECG//QT, etc. (double slashes per convention)
      - numeric_value: The measurement value
      - unit: beats/min, ms, or ° depending on measurement type
      - event_type: ECG (matches ecgs.py convention)
      - code_system: AHS_ECG (matches ecgs.py convention)
      - source_table: Globalmeasurements 
    
    Data quality notes:
      - qtco column is 100% NULL, excluded
      - tonset is sparse (~6% populated)
      - Units are standard ECG conventions (not stored in source data)
    """
    
    @staticmethod
    def _clean_ecg_id(ecg_id):
        """
        Extract GUID from N'...' format ECG IDs.
        
        The ECG records file stores ecgId in format: N'407be100-3b36-11dc-...'
        The measurements file stores ecgId as clean GUID: 407be100-3b36-11dc-...
        
        This function normalizes the format for joining.
        
        Args:
            ecg_id: Raw ECG ID string, e.g., "N'407be100-3b36-11dc-4823-000206d60029'"
            
        Returns:
            Clean GUID string, e.g., "407be100-3b36-11dc-4823-000206d60029"
            or None if input is null
        """
        if pd.isna(ecg_id):
            return None
        ecg_str = str(ecg_id)
        # Try regex match first
        match = re.search(r"N'([^']+)'", ecg_str)
        if match:
            return match.group(1)
        # Fallback: remove N' prefix and ' suffix manually
        if ecg_str.startswith("N'"):
            ecg_str = ecg_str[2:]
        if ecg_str.endswith("'"):
            ecg_str = ecg_str[:-1]
        return ecg_str.strip()
    
    def _load_pickle(self, path: str, name: str) -> pd.DataFrame:
        """
        Load pickle file with pandas version compatibility handling.
        
        Args:
            path: Path to the pickle file
            name: Human-readable name for error messages
            
        Returns:
            DataFrame loaded from pickle
        """
        try:
            return pd.read_pickle(path)
        except (ModuleNotFoundError, AttributeError) as e:
            print(f"⚠️  {name} pickle compatibility issue: {e}")
            try:
                import pandas.compat.pickle_compat as pc
                with open(path, 'rb') as f:
                    return pc.load(f)
            except Exception as final_error:
                raise RuntimeError(f"Failed to load {name}: {final_error}")
    
    def run_core(self) -> pd.DataFrame:
        """
        Convert AHS ECG measurements to MEDS core format.
        
        Process:
        1. Load ECG records (PATID, ecgId, dateAcquired)
        2. Load measurements (ecgId, heartrate, qrsdur, ...)
        3. Clean ecgId format in records for joining
        4. Join on ecgId
        5. Melt each measurement column into separate MEDS events
        
        Returns:
            DataFrame with columns: subject_id, time, event_type, code, 
            code_system, numeric_value, unit, source_table
        """
        # Load data
        ecg_records = self._load_pickle(self.cfg["raw_paths"]["ecg"], "ECG records")
        measurements = self._load_pickle(self.cfg["raw_paths"]["ecg_measurements"], "ECG measurements")
        
        # Validate required columns
        required_cols = ["PATID", "ecgId", "dateAcquired"]
        missing = [c for c in required_cols if c not in ecg_records.columns]
        if missing:
            raise KeyError(f"ECG records missing columns: {missing}")
        
        # Clean ECG IDs for joining (remove N'...' wrapper)
        ecg_records['ecgId_clean'] = ecg_records['ecgId'].apply(self._clean_ecg_id)
        
        # Join on ecgId
        merged = ecg_records.merge(
            measurements,
            left_on='ecgId_clean',
            right_on='ecgId',
            how='inner',
            suffixes=('_rec', '_meas')
        )
        
        print(f"📊 Merged {len(merged):,} ECG records with measurements")
        
        if len(merged) == 0:
            print("⚠️  No records matched between ECG records and measurements!")
            return pd.DataFrame(columns=[
                'subject_id', 'time', 'event_type', 'code', 
                'code_system', 'numeric_value', 'unit', 'source_table'
            ])
        
        # Melt measurements into long format (one row per measurement)
        all_events = []
        
        for col_name, (code, unit) in ECG_MEASUREMENTS.items():
            if col_name not in merged.columns:
                continue
            
            # Extract this measurement
            df_meas = pd.DataFrame({
                'subject_id': merged['PATID'].astype('Int64').astype(str),
                'time': pd.to_datetime(merged['dateAcquired'], errors='coerce'),
                'code': code,
                'numeric_value': pd.to_numeric(merged[col_name], errors='coerce'),
                'unit': unit,
            })
            
            # Drop rows with missing required values
            df_meas = df_meas.dropna(subset=['subject_id', 'time', 'numeric_value'])
            
            if len(df_meas) > 0:
                all_events.append(df_meas)
        
        # Combine all events
        out = pd.concat(all_events, ignore_index=True)
        
        # Add metadata columns (following ecgs.py convention)
        out['event_type'] = 'ECG'
        out['code_system'] = 'AHS_ECG'
        out['source_table'] = 'Globalmeasurements'  # Actual source file name
        
        print(f"✅ Generated {len(out):,} ECG measurement events")
        
        return out
