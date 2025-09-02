#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据补齐进度实时监控器
====================

监控两个补齐器的运行状态和进度
"""

import pandas as pd
from pathlib import Path
from datetime import datetime
import time

def monitor_supplement_progress():
    """监控补齐进度"""
    
    print("🔄 数据补齐进度实时监控")
    print("=" * 60)
    print(f"⏰ 监控时间: {datetime.now()}")
    print()
    
    data_path = Path("/Users/jackstudio/QuantTrade/data")
    
    # 监控重点目录和预期文件
    supplement_targets = {
        "calendar": {
            "desc": "交易日历",
            "files": ["trading_calendar_2000_2009.csv", "trading_calendar.csv"],
            "target_years": 25
        },
        "capital_flow": {
            "desc": "资金流向", 
            "files": [f"capital_flow_{year}.csv" for year in range(2015, 2024)],
            "target_years": 9
        },
        "limit_info": {
            "desc": "涨跌停数据",
            "files": [f"limit_data_{year}.csv" for year in range(2000, 2025)],
            "target_years": 25
        },
        "rank_list": {
            "desc": "龙虎榜数据",
            "files": [f"rank_list_{year}.csv" for year in range(2000, 2025)], 
            "target_years": 25
        }
    }
    
    print("📊 各数据集补齐状态:")
    print("-" * 50)
    
    total_progress = 0
    total_targets = len(supplement_targets)
    
    for dir_name, info in supplement_targets.items():
        dir_path = data_path / dir_name
        
        print(f"\n🔍 {info['desc']} ({dir_name}):")
        
        if not dir_path.exists():
            print(f"   📂 目录未创建")
            continue
            
        # 检查实际存在的文件
        existing_files = list(dir_path.glob("*.csv"))
        
        # 特定文件检查
        found_files = []
        total_records = 0
        total_size = 0
        
        for file in existing_files:
            try:
                file_size = file.stat().st_size / (1024*1024)
                df = pd.read_csv(file)
                records = len(df)
                
                found_files.append({
                    'name': file.name,
                    'records': records,
                    'size': file_size
                })
                
                total_records += records
                total_size += file_size
                
            except Exception as e:
                continue
        
        if found_files:
            print(f"   ✅ 已有文件: {len(found_files)} 个")
            print(f"   📊 总记录: {total_records:,} 条")
            print(f"   💾 总大小: {total_size:.1f}MB")
            
            # 显示最新文件
            latest_files = sorted(found_files, key=lambda x: x['records'], reverse=True)[:3]
            for file_info in latest_files:
                print(f"      📄 {file_info['name']}: {file_info['records']:,} 条, {file_info['size']:.1f}MB")
            
            # 计算完成度
            if dir_name == "calendar":
                progress = len(found_files) / 2 * 100  # 期望2个文件
            elif dir_name == "capital_flow":
                progress = len(found_files) / 9 * 100  # 期望9年
            else:
                progress = len(found_files) / 25 * 100  # 期望25年
            
            print(f"   📈 进度: {progress:.1f}%")
            total_progress += progress
        else:
            print(f"   ⏳ 暂无数据")
    
    # 整体进度
    avg_progress = total_progress / total_targets if total_targets > 0 else 0
    
    print(f"\n🎯 整体补齐进度:")
    print(f"   📊 平均完成度: {avg_progress:.1f}%")
    
    if avg_progress >= 80:
        print(f"   🎊 补齐接近完成!")
    elif avg_progress >= 50:
        print(f"   📈 补齐进展良好")
    elif avg_progress >= 25:
        print(f"   🔄 补齐正在进行")
    else:
        print(f"   ⏳ 补齐刚刚开始")
    
    print(f"\n⏱️ 下次检查建议: 10分钟后")

if __name__ == "__main__":
    monitor_supplement_progress()