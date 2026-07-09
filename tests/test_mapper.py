#!/usr/bin/env python
"""Test script for canada_code_mapper."""

from canada_code_mapper.registry import init_canadian_mappers

print("Initializing mappers...")
registry = init_canadian_mappers(
    icd10ca_path='./resource/ICD_Code_Eng_Desc_10CA2026_V1_0.txt',
    cci_path='./resource/CCI_Code_Eng_Desc_CCI2026_V1_0.txt'
)

print('Registry created successfully!')
print('Registered mappers:', registry.list_mappers())
print('Registry:', registry)

# Test get_description
print("\n--- Testing ICD-10-CA ---")
icd_desc = registry.get_description('icd10ca', 'A000')
print(f'A000: {icd_desc}')

icd_desc2 = registry.get_description('icd10ca', 'A001')
print(f'A001: {icd_desc2}')

print("\n--- Testing CCI ---")
cci_desc = registry.get_description('cci', '1AA13HAC2')
print(f'1AA13HAC2: {cci_desc}')

cci_desc2 = registry.get_description('cci', '1AA35HA1C')
print(f'1AA35HA1C: {cci_desc2}')

print("\n--- Test complete ---")
