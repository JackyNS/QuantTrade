#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
2000-2009年历史数据下载监控器
===========================

监控2000-2009年历史数据补齐进度
"""

import pandas as pd
from pathlib import Path
from datetime import datetime
import time

def monitor_historical_download():
    """监控历史数据下载进度"""
    
    print("📜 2000-2009年历史数据下载监控")
    print("=" * 50)
    print(f"⏰ 监控时间: {datetime.now()}")
    print()
    
    data_path = Path("/Users/jackstudio/QuantTrade/data")
    
    # 监控的历史文件模式
    historical_patterns = {
        "basic_info": "*2000_2009.csv",
        "calendar": "*2000_2009.csv", 
        "daily": "*2000_2009.csv",
        "adjustment": "*2000_2009.csv",
        "dividend": "*2000_2009.csv",
        "financial": "*2000_2009.csv",
        "capital_flow": "*2000_2009.csv",
        "limit_info": "*2000_2009.csv",
        "rank_list": "*2000_2009.csv"
    }
    
    print("📊 2000-2009年历史文件检查:")
    total_historical_files = 0
    total_historical_size = 0
    total_historical_records = 0
    
    for dir_name, pattern in historical_patterns.items():
        dir_path = data_path / dir_name
        if dir_path.exists():
            historical_files = list(dir_path.glob(pattern))
            
            if historical_files:
                dir_size = 0
                dir_records = 0
                
                for file in historical_files:
                    file_size_mb = file.stat().st_size / (1024*1024)
                    dir_size += file_size_mb
                    
                    try:
                        df = pd.read_csv(file)
                        records = len(df)
                        dir_records += records
                        print(f"   ✅ {file.name}: {records:,} 条记录, {file_size_mb:.1f}MB")
                    except Exception as e:
                        print(f"   ❌ {file.name}: 读取失败 - {e}")
                
                print(f"   📊 {dir_name}: {len(historical_files)} 文件, {dir_records:,} 记录, {dir_size:.1f}MB")
                total_historical_files += len(historical_files)
                total_historical_size += dir_size
                total_historical_records += dir_records
            else:
                print(f"   ⏳ {dir_name}: 暂无历史文件")
        else:
            print(f"   📂 {dir_name}: 目录不存在")
        print()
    
    # 总计统计
    print("📈 2000-2009年历史数据总计:")
    print(f"   📄 文件数: {total_historical_files}")
    print(f"   📋 记录数: {total_historical_records:,}")
    print(f"   💾 数据量: {total_historical_size:.1f}MB")
    
    expected_files = len(historical_patterns)  # 每个目录至少1个文件
    completion_rate = (total_historical_files / expected_files) * 100
    print(f"   📈 完成度: {completion_rate:.1f}% ({total_historical_files}/{expected_files})")
    
    print()
    
    # 组合统计 (包含2010-2025 + 2000-2009)
    print("🎯 完整25年数据覆盖统计:")
    
    all_dirs = ["basic_info", "calendar", "daily", "adjustment", "dividend", "financial", "capital_flow", "limit_info", "rank_list"]
    total_all_files = 0
    total_all_size = 0
    
    for dir_name in all_dirs:
        dir_path = data_path / dir_name
        if dir_path.exists():
            all_files = list(dir_path.glob("*.csv"))
            dir_size = sum(f.stat().st_size for f in all_files) / (1024*1024)
            total_all_files += len(all_files)
            total_all_size += dir_size
            
            # 检查是否有2000-2009文件
            has_historical = any(dir_path.glob("*2000_2009.csv"))
            has_modern = len(all_files) > (1 if has_historical else 0)
            
            status = "🎊" if (has_historical and has_modern) else "⏳" if has_modern else "❌"
            print(f"   {status} {dir_name}: {len(all_files)} 文件, {dir_size:.1f}MB")
    
    print(f"\n📊 全部数据统计:")
    print(f"   📄 总文件: {total_all_files}")
    print(f"   💾 总大小: {total_all_size / 1024:.2f}GB")
    
    if total_historical_files >= 8:  # 80%以上完成
        print(f"\n🎊 25年完整数据覆盖: 接近完成!")
        print(f"📈 历史补齐进度: {completion_rate:.1f}%")
    else:
        print(f"\n⏳ 历史数据补齐: 进行中...")

if __name__ == "__main__":
    monitor_historical_download()