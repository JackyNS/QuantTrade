#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
QuantTrade框架数据目录结构查看器
==============================

展示完整的数据目录组织结构和文件统计
"""

import pandas as pd
from pathlib import Path
from datetime import datetime
import os

def show_data_structure():
    """展示数据目录结构"""
    
    print("📁 QuantTrade框架数据目录结构")
    print("=" * 70)
    print(f"⏰ 查看时间: {datetime.now()}")
    print(f"📍 数据路径: /Users/jackstudio/QuantTrade/data")
    print()
    
    data_path = Path("/Users/jackstudio/QuantTrade/data")
    
    if not data_path.exists():
        print("❌ 数据目录不存在")
        return
    
    # 获取所有子目录
    subdirs = sorted([d for d in data_path.iterdir() if d.is_dir()])
    
    # API映射
    api_mapping = {
        "basic_info": {"api": "EquGet", "desc": "股票基本信息", "icon": "🏢"},
        "calendar": {"api": "TradeCalGet", "desc": "交易日历", "icon": "📅"}, 
        "daily": {"api": "MktEqudGet", "desc": "股票日行情", "icon": "📈"},
        "adjustment": {"api": "MktAdjfGet", "desc": "复权因子", "icon": "🔧"},
        "dividend": {"api": "EquDivGet", "desc": "股票分红", "icon": "💎"},
        "financial": {"api": "FdmtBs2018Get等", "desc": "财务数据", "icon": "💰"},
        "capital_flow": {"api": "MktEquFlowGet", "desc": "资金流向", "icon": "💸"},
        "limit_info": {"api": "MktLimitGet", "desc": "涨跌停信息", "icon": "⚠️"},
        "rank_list": {"api": "MktRankListStocksGet", "desc": "龙虎榜数据", "icon": "🔥"}
    }
    
    total_files = 0
    total_size_mb = 0
    total_records = 0
    
    print("📊 数据目录详细结构:")
    print("=" * 70)
    
    for subdir in subdirs:
        dir_name = subdir.name
        mapping = api_mapping.get(dir_name, {"api": "Unknown", "desc": "未知数据", "icon": "📂"})
        
        print(f"\n{mapping['icon']} {dir_name}/ - {mapping['desc']}")
        print(f"   📡 API: {mapping['api']}")
        
        # 获取目录中的所有CSV文件
        csv_files = list(subdir.glob("*.csv"))
        
        if csv_files:
            dir_size = 0
            dir_records = 0
            
            print(f"   📄 文件列表:")
            
            # 按文件名排序，现代数据在前，历史数据在后
            modern_files = [f for f in csv_files if "2000_2009" not in f.name]
            historical_files = [f for f in csv_files if "2000_2009" in f.name]
            
            # 显示现代数据文件
            if modern_files:
                print(f"      📈 2010-2025年数据:")
                for file in sorted(modern_files):
                    file_size = file.stat().st_size / (1024*1024)
                    dir_size += file_size
                    
                    try:
                        df = pd.read_csv(file)
                        records = len(df)
                        dir_records += records
                        
                        # 显示时间范围
                        time_info = ""
                        if 'tradeDate' in df.columns:
                            time_range = f"{df['tradeDate'].min()} ~ {df['tradeDate'].max()}"
                            time_info = f" ({time_range})"
                        elif 'endDate' in df.columns:
                            time_range = f"{df['endDate'].min()} ~ {df['endDate'].max()}"
                            time_info = f" ({time_range})"
                        elif 'calendarDate' in df.columns:
                            time_range = f"{df['calendarDate'].min()} ~ {df['calendarDate'].max()}"
                            time_info = f" ({time_range})"
                        
                        print(f"         📄 {file.name}: {records:,}条 | {file_size:.1f}MB{time_info}")
                        
                    except Exception as e:
                        print(f"         ❌ {file.name}: {file_size:.1f}MB (读取失败)")
                        dir_size += file_size
            
            # 显示历史数据文件
            if historical_files:
                print(f"      📜 2000-2009年历史数据:")
                for file in sorted(historical_files):
                    file_size = file.stat().st_size / (1024*1024)
                    dir_size += file_size
                    
                    try:
                        df = pd.read_csv(file)
                        records = len(df)
                        dir_records += records
                        
                        # 显示时间范围
                        time_info = ""
                        if 'tradeDate' in df.columns:
                            time_range = f"{df['tradeDate'].min()} ~ {df['tradeDate'].max()}"
                            time_info = f" ({time_range})"
                        elif 'endDate' in df.columns:
                            time_range = f"{df['endDate'].min()} ~ {df['endDate'].max()}"
                            time_info = f" ({time_range})"
                            
                        print(f"         📄 {file.name}: {records:,}条 | {file_size:.1f}MB{time_info}")
                        
                    except Exception as e:
                        print(f"         ❌ {file.name}: {file_size:.1f}MB (读取失败)")
                        dir_size += file_size
            
            # 目录统计
            coverage_status = "🎊 25年" if modern_files and historical_files else "⏳ 部分"
            print(f"   📊 目录统计: {len(csv_files)}文件 | {dir_records:,}条记录 | {dir_size:.1f}MB | {coverage_status}")
            
            total_files += len(csv_files)
            total_size_mb += dir_size  
            total_records += dir_records
            
        else:
            print(f"   📂 目录为空")
    
    # 显示总体统计
    print(f"\n📊 数据总体统计:")
    print("=" * 50)
    print(f"📁 数据目录: {len(subdirs)} 个")
    print(f"📄 CSV文件: {total_files} 个")
    print(f"📋 总记录数: {total_records:,} 条")
    print(f"💾 总大小: {total_size_mb:.1f}MB ({total_size_mb/1024:.2f}GB)")
    
    # 显示目录树结构
    print(f"\n🌳 目录树结构:")
    print("=" * 50)
    print("data/")
    
    for subdir in subdirs:
        mapping = api_mapping.get(subdir.name, {"icon": "📂"})
        csv_count = len(list(subdir.glob("*.csv")))
        
        if subdir == subdirs[-1]:
            print(f"└── {mapping['icon']} {subdir.name}/ ({csv_count} files)")
        else:
            print(f"├── {mapping['icon']} {subdir.name}/ ({csv_count} files)")
            
        # 显示文件
        csv_files = sorted(list(subdir.glob("*.csv")))
        for i, file in enumerate(csv_files):
            if subdir == subdirs[-1]:
                prefix = "    "
            else:
                prefix = "│   "
                
            if i == len(csv_files) - 1:
                print(f"{prefix}└── 📄 {file.name}")
            else:
                print(f"{prefix}├── 📄 {file.name}")
    
    # 数据质量评估
    print(f"\n🎯 数据质量评估:")
    print("=" * 50)
    
    if total_size_mb > 1000:
        quality = "🚀 大型数据集"
        capability = "支持高频策略、长周期回测、多因子模型"
    elif total_size_mb > 100:
        quality = "📈 中型数据集" 
        capability = "支持中频策略、技术分析、基础回测"
    else:
        quality = "📊 小型数据集"
        capability = "支持基础策略、短期分析"
    
    print(f"{quality} ({total_size_mb/1024:.2f}GB)")
    print(f"💡 {capability}")
    
    # 框架就绪状态
    core_dirs = ['basic_info', 'calendar', 'daily']
    tech_dirs = ['adjustment', 'dividend', 'financial'] 
    emotion_dirs = ['capital_flow', 'limit_info', 'rank_list']
    
    core_ready = sum(1 for d in core_dirs if (data_path / d).exists() and list((data_path / d).glob("*.csv")))
    tech_ready = sum(1 for d in tech_dirs if (data_path / d).exists() and list((data_path / d).glob("*.csv")))
    emotion_ready = sum(1 for d in emotion_dirs if (data_path / d).exists() and list((data_path / d).glob("*.csv")))
    
    print(f"\n🚀 框架功能就绪状态:")
    print(f"🔥 核心交易: {core_ready}/{len(core_dirs)} ({'✅' if core_ready >= 2 else '⏳'})")
    print(f"💰 技术分析: {tech_ready}/{len(tech_dirs)} ({'✅' if tech_ready >= 2 else '⏳'})")
    print(f"🧠 情绪分析: {emotion_ready}/{len(emotion_dirs)} ({'✅' if emotion_ready >= 1 else '⏳'})")

if __name__ == "__main__":
    show_data_structure()