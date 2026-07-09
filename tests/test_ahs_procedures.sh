#!/bin/bash
# AHS Procedures ETL 测试命令
# 
# 功能验证：
# 1. 读取 DAD SAS 文件中的 PROCCODE1..PROCCODE20
# 2. 使用对应的 PROCSTDT{n}_DT 作为时间，若为 NaT 则用 ADMITDATE_DT
# 3. Code 格式为 PROCEDURE//CCI//{code}
# 4. Value 列为序号（string dtype）

echo "=========================================="
echo "AHS Procedures ETL 测试"
echo "=========================================="

# 1. 运行 procedures 组件（AHS，limit 10 patients）
echo -e "\n[1/4] 运行 AHS procedures ETL (限制10个患者)..."
PYTHONPATH=src python3 -m meds_pipeline.cli run \
  --source ahs \
  --components procedures \
  --cfg src/meds_pipeline/configs/ahs.yaml \
  --max-patients 10 \
  --progress

# 2. 获取输出目录
echo -e "\n[2/4] 获取输出目录..."
OUTPUT_DIR=$(python3 -c "import yaml; cfg = yaml.safe_load(open('src/meds_pipeline/configs/base.yaml')); print(cfg.get('output_dir', '/tmp'))")
echo "输出目录: $OUTPUT_DIR/ahs"

# 3. 列出生成的文件
echo -e "\n[3/4] 列出生成的文件..."
ls -lh "$OUTPUT_DIR"/ahs/*.parquet 2>/dev/null || echo "未找到 parquet 文件"

# 4. 验证输出数据
echo -e "\n[4/4] 验证输出数据结构和内容..."
python3 <<'PYCODE'
import pandas as pd
import glob
import os

# 获取输出目录
import yaml
cfg = yaml.safe_load(open('src/meds_pipeline/configs/base.yaml'))
output_dir = cfg.get('output_dir', '/tmp')
ahs_dir = os.path.join(output_dir, 'ahs')

# 查找 parquet 文件
parts = sorted(glob.glob(f"{ahs_dir}/*.parquet"))
if not parts:
    print("❌ 未找到 parquet 文件")
    exit(1)

print(f"✓ 找到 {len(parts)} 个文件")
print(f"✓ 读取第一个文件: {parts[0]}")

# 读取第一个文件
df = pd.read_parquet(parts[0])

print(f"\n【基本信息】")
print(f"  总行数: {len(df):,}")
print(f"  唯一患者数: {df['subject_id'].nunique():,}")
print(f"  列名: {df.columns.tolist()}")

print(f"\n【数据类型】")
for col in ['subject_id', 'time', 'event_type', 'code', 'value']:
    if col in df.columns:
        print(f"  {col}: {df[col].dtype}")

print(f"\n【Code 格式验证】")
if 'code' in df.columns:
    sample_codes = df['code'].dropna().head(10).tolist()
    print(f"  示例 codes: {sample_codes[:5]}")
    cci_count = df['code'].astype(str).str.startswith('PROCEDURE//CCI//').sum()
    print(f"  ✓ 以 'PROCEDURE//CCI//' 开头的记录数: {cci_count:,} / {len(df):,}")
    if cci_count < len(df):
        print(f"  ⚠️  警告: 有 {len(df) - cci_count} 条记录不符合格式")

print(f"\n【Value 列验证】")
if 'value' in df.columns:
    print(f"  dtype: {df['value'].dtype}")
    print(f"  示例值: {df['value'].dropna().unique()[:10].tolist()}")
    print(f"  值范围: {df['value'].dropna().astype(int).min()} ~ {df['value'].dropna().astype(int).max()}")

print(f"\n【时间列验证】")
if 'time' in df.columns:
    print(f"  缺失值: {df['time'].isna().sum()}")
    print(f"  最早时间: {df['time'].min()}")
    print(f"  最晚时间: {df['time'].max()}")

print(f"\n【样本数据】")
print(df[['subject_id', 'time', 'code', 'value']].head(20).to_string())

print(f"\n{'='*60}")
print("✅ 验证完成")
print(f"{'='*60}")
PYCODE

echo -e "\n=========================================="
echo "测试完成"
echo "=========================================="
