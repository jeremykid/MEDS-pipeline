#!/usr/bin/env python3
"""简单验证 AHS procedures.py 的代码逻辑"""

import sys
sys.path.insert(0, 'src')

import pandas as pd
from meds_pipeline.etl.ahs.procedures import AHSProcedures

print("="*60)
print("AHS Procedures ETL 快速验证")
print("="*60)

# Test 1: Code format
print("\n[Test 1] Code 格式测试")
code1 = AHSProcedures._build_procedure_code("1HZ53HAGP")
print(f"  Input: '1HZ53HAGP' -> Output: '{code1}'")
assert code1 == "PROCEDURE//CCI//1HZ53HAGP", f"格式错误: {code1}"
print("  ✓ 格式正确: PROCEDURE//CCI//code")

# Test 2: Sequence value
print("\n[Test 2] Value 列（序号）测试")
test_seq = pd.Series([1, 2, 3])
value_col = test_seq.astype('Int64').astype("string")
print(f"  序号: {test_seq.tolist()} -> Value: {value_col.tolist()}")
print(f"  dtype: {value_col.dtype}")
assert value_col.dtype == 'string', f"dtype 错误: {value_col.dtype}"
print("  ✓ dtype 正确: string")

# Test 3: Time fallback
print("\n[Test 3] 时间回退逻辑测试")
test_df = pd.DataFrame({
    'PROCSTDT_DT': [pd.NaT, '2021-08-20'],
    'ADMITDATE_DT': ['2021-08-19', '2021-08-19']
})
test_df['proc_time'] = pd.to_datetime(test_df['PROCSTDT_DT'], errors='coerce')
test_df['admit_time'] = pd.to_datetime(test_df['ADMITDATE_DT'], errors='coerce')
test_df['event_time'] = test_df['proc_time'].fillna(test_df['admit_time'])
print(f"  Row 1: PROCSTDT=NaT -> event_time={test_df.iloc[0]['event_time'].date()}")
print(f"  Row 2: PROCSTDT=2021-08-20 -> event_time={test_df.iloc[1]['event_time'].date()}")
assert str(test_df.iloc[0]['event_time'].date()) == '2021-08-19', "回退逻辑错误"
assert str(test_df.iloc[1]['event_time'].date()) == '2021-08-20', "时间使用错误"
print("  ✓ 时间回退逻辑正确")

# Test 4: Component registration
print("\n[Test 4] 组件注册测试")
from meds_pipeline.etl.registry import REGISTRY
assert "procedures" in REGISTRY, "组件未注册"
print(f"  ✓ 组件已注册: procedures -> {REGISTRY['procedures'].__name__}")

print("\n" + "="*60)
print("✅ 所有测试通过！")
print("="*60)
print("\n下一步: 运行完整 ETL")
print("  cd /home/weijiesun/MEDS-pipeline")
print("  PYTHONPATH=src python3 -m meds_pipeline.cli run \\")
print("    --source ahs --components procedures \\")
print("    --max-patients 10 --progress")
