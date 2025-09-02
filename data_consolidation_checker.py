#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据整合检查器
=============

检查重复数据文件，识别需要合并的数据集
"""

import pandas as pd
from pathlib import Path
from datetime import datetime
import hashlib

def check_data_consolidation():
    """检查数据整合情况"""
    
    print("🔍 QuantTrade数据整合检查器")
    print("=" * 60)
    print(f"⏰ 检查时间: {datetime.now()}")
    print()
    
    data_path = Path("/Users/jackstudio/QuantTrade/data")
    
    # 重点检查的目录
    key_dirs = {
        "daily": "日行情数据",
        "adjustment": "复权因子",
        "dividend": "股票分红", 
        "basic_info": "股票基本信息",
        "capital_flow": "资金流向",
        "financial": "财务数据"
    }
    
    consolidation_issues = []
    
    print("📊 各目录数据重复检查:")
    print("-" * 50)
    
    for dir_name, desc in key_dirs.items():
        dir_path = data_path / dir_name
        
        print(f"\n🔍 检查 {desc} ({dir_name}):")
        
        if not dir_path.exists():
            print(f"   ❌ 目录不存在")
            continue
            
        csv_files = list(dir_path.glob("*.csv"))
        
        if not csv_files:
            print(f"   📂 目录为空")
            continue
            
        # 按类型分组文件
        modern_files = [f for f in csv_files if "2000_2009" not in f.name]
        historical_files = [f for f in csv_files if "2000_2009" in f.name]
        
        print(f"   📈 现代数据文件: {len(modern_files)} 个")
        print(f"   📜 历史数据文件: {len(historical_files)} 个")
        
        # 检查现代数据重复
        if len(modern_files) > 1:
            print(f"   🔍 检查现代数据重复...")
            
            # 对于日行情数据，检查年度文件是否有重叠
            if dir_name == "daily":
                year_files = [f for f in modern_files if f.name.startswith("daily_")]
                core_files = [f for f in modern_files if "core" in f.name]
                
                if year_files and core_files:
                    print(f"   ⚠️ 发现可能重复: {len(year_files)}个年度文件 + {len(core_files)}个核心文件")
                    consolidation_issues.append({
                        'dir': dir_name,
                        'type': 'daily_overlap',
                        'files': [f.name for f in year_files + core_files]
                    })
                    
                    # 检查时间重叠
                    for year_file in year_files:
                        try:
                            df_year = pd.read_csv(year_file)
                            if 'tradeDate' in df_year.columns:
                                year_range = f"{df_year['tradeDate'].min()} ~ {df_year['tradeDate'].max()}"
                                print(f"      📄 {year_file.name}: {year_range}")
                        except:
                            pass
            
            # 检查文件内容相似性
            for i, file1 in enumerate(modern_files):
                for file2 in modern_files[i+1:]:
                    try:
                        # 比较文件大小
                        size1 = file1.stat().st_size
                        size2 = file2.stat().st_size
                        
                        if abs(size1 - size2) < size1 * 0.1:  # 大小相差不到10%
                            print(f"   ⚠️ 疑似重复文件: {file1.name} vs {file2.name} (大小相近)")
                            
                    except Exception as e:
                        pass
        
        # 检查是否缺少历史数据
        if modern_files and not historical_files:
            print(f"   ⏳ 缺少历史数据 (2000-2009)")
            consolidation_issues.append({
                'dir': dir_name,
                'type': 'missing_historical',
                'files': []
            })
        
        # 计算总记录数和大小
        total_records = 0
        total_size = 0
        
        for file in csv_files:
            try:
                size = file.stat().st_size / (1024*1024)
                total_size += size
                
                df = pd.read_csv(file)
                total_records += len(df)
                
            except Exception as e:
                pass
                
        print(f"   📊 总计: {total_records:,} 条记录, {total_size:.1f}MB")
    
    # 检查跨目录的潜在问题
    print(f"\n🔍 跨目录数据检查:")
    print("-" * 30)
    
    # 检查是否有分散的相同数据
    test_dir = data_path / "test_api_download"
    if test_dir.exists():
        test_files = list(test_dir.glob("*.csv"))
        if test_files:
            print(f"   ⚠️ 发现测试数据目录: {len(test_files)} 个测试文件")
            consolidation_issues.append({
                'dir': 'test_api_download',
                'type': 'test_data_cleanup',
                'files': [f.name for f in test_files]
            })
    
    # 汇总整合建议
    print(f"\n📋 数据整合建议:")
    print("=" * 40)
    
    if consolidation_issues:
        print(f"🔧 发现 {len(consolidation_issues)} 个需要处理的问题:")
        
        for issue in consolidation_issues:
            if issue['type'] == 'daily_overlap':
                print(f"   ⚠️ 日行情数据重叠: 建议合并年度文件，移除core文件")
            elif issue['type'] == 'missing_historical':
                print(f"   ⏳ {issue['dir']}: 等待历史数据下载完成")
            elif issue['type'] == 'test_data_cleanup':
                print(f"   🧹 测试数据清理: 建议移除或移动到备份目录")
        
        print(f"\n🎯 优化后预期效果:")
        print(f"   📁 减少重复文件")
        print(f"   🔄 统一数据格式") 
        print(f"   📊 提高查询效率")
        print(f"   💾 优化存储空间")
        
    else:
        print(f"✅ 未发现明显的数据重复问题")
        print(f"📊 当前数据组织良好")
    
    # 等待状态检查
    print(f"\n⏳ 等待中的下载任务:")
    print("-" * 30)
    
    # 检查2000-2009历史数据完成情况
    historical_dirs = ['calendar', 'financial', 'capital_flow', 'limit_info', 'rank_list']
    pending_historical = []
    
    for dir_name in historical_dirs:
        dir_path = data_path / dir_name
        if dir_path.exists():
            historical_files = list(dir_path.glob("*2000_2009.csv"))
            if not historical_files:
                pending_historical.append(dir_name)
    
    if pending_historical:
        print(f"   📜 等待历史数据: {', '.join(pending_historical)}")
    else:
        print(f"   ✅ 历史数据已完成")
    
    return consolidation_issues

if __name__ == "__main__":
    check_data_consolidation()