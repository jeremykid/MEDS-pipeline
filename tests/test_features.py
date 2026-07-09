#!/usr/bin/env python3
"""
简单测试：验证新功能是否正常工作
"""

import sys
import os
sys.path.insert(0, '/home/weijiesun/MEDS-pipeline/src')

def test_imports():
    """测试导入是否正常"""
    try:
        from meds_pipeline.cli import cli
        print("✅ CLI导入成功")
        
        from tqdm import tqdm
        print("✅ tqdm导入成功")
        
        return True
    except ImportError as e:
        print(f"❌ 导入失败: {e}")
        return False

def show_help():
    """显示CLI帮助"""
    try:
        from meds_pipeline.cli import cli
        from click.testing import CliRunner
        
        runner = CliRunner()
        result = runner.invoke(cli, ['--help'])
        print("CLI 主帮助:")
        print(result.output)
        
        result = runner.invoke(cli, ['run', '--help'])
        print("\nRUN 命令帮助:")
        print(result.output)
        
    except Exception as e:
        print(f"❌ 显示帮助失败: {e}")

def main():
    print("🧪 测试MEDS管道新功能")
    print("="*50)
    
    # 测试导入
    if not test_imports():
        return
    
    # 显示帮助
    show_help()
    
    print("\n📋 新功能说明:")
    print("1. --max-patients N     : 限制处理N个患者（测试用）")
    print("2. --progress          : 显示进度条（默认）")
    print("3. --no-progress       : 不显示进度条")
    print("\n💡 使用示例:")
    print("python src/meds_pipeline/cli.py run --source mimic --components admissions --max-patients 100")
    print("python src/meds_pipeline/cli.py run --source mimic --components admissions --no-progress")

if __name__ == "__main__":
    main()
