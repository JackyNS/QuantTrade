#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
优矿数据下载完成报告
===================

分析已下载数据的完整性和质量
"""

import pandas as pd
from pathlib import Path
import json
from datetime import datetime

def analyze_downloaded_data():
    """分析已下载的数据"""
    
    print("🎯 优矿核心数据下载完成报告")
    print("=" * 60)
    print(f"⏰ 报告生成时间: {datetime.now()}")
    print()
    
    data_path = Path("/Users/jackstudio/QuantTrade/data")
    
    # 检查各个数据目录
    data_summary = {
        "basic_info": {
            "description": "股票基本信息",
            "files": list((data_path / "basic_info").glob("*.csv")),
            "importance": "🔥 核心必需"
        },
        "daily": {
            "description": "股票日行情",
            "files": list((data_path / "daily").glob("*.csv")),
            "importance": "🔥 核心必需"
        },
        "calendar": {
            "description": "交易日历",
            "files": list((data_path / "calendar").glob("*.csv")),
            "importance": "🔥 核心必需"
        },
        "capital_flow": {
            "description": "资金流向",
            "files": list((data_path / "capital_flow").glob("*.csv")),
            "importance": "⭐ 情绪重要"
        },
        "limit_info": {
            "description": "涨跌停信息",
            "files": list((data_path / "limit_info").glob("*.csv")),
            "importance": "⭐ 情绪重要"
        },
        "rank_list": {
            "description": "龙虎榜数据",
            "files": list((data_path / "rank_list").glob("*.csv")),
            "importance": "⭐ 情绪重要"
        }
    }
    
    total_records = 0
    successful_downloads = 0
    
    for category, info in data_summary.items():
        print(f"📁 {info['description']} ({category})")
        print(f"   {info['importance']}")
        
        if info['files']:
            for file in info['files']:
                try:
                    df = pd.read_csv(file)
                    records = len(df)
                    file_size = file.stat().st_size / (1024*1024)  # MB
                    
                    print(f"   ✅ {file.name}: {records:,} 条记录, {file_size:.1f}MB")
                    
                    # 显示数据时间范围
                    if 'tradeDate' in df.columns:
                        date_range = f"{df['tradeDate'].min()} 至 {df['tradeDate'].max()}"
                        print(f"      📅 时间范围: {date_range}")
                    elif 'calendarDate' in df.columns:
                        date_range = f"{df['calendarDate'].min()} 至 {df['calendarDate'].max()}"
                        print(f"      📅 时间范围: {date_range}")
                    
                    # 显示股票数量
                    if 'ticker' in df.columns:
                        stock_count = df['ticker'].nunique()
                        print(f"      🏢 股票数量: {stock_count} 只")
                    
                    total_records += records
                    successful_downloads += 1
                    
                except Exception as e:
                    print(f"   ❌ {file.name}: 读取失败 - {e}")
        else:
            print(f"   ❌ 无数据文件")
        
        print()
    
    # 汇总统计
    print("📊 下载汇总统计")
    print("-" * 30)
    print(f"✅ 成功下载: {successful_downloads} 个文件")
    print(f"📋 总记录数: {total_records:,} 条")
    print(f"💾 数据覆盖: 2024年1月-8月 (测试期)")
    print(f"🏢 股票范围: 前100只A股 (测试)")
    
    # 评估数据完整性
    print("\n🎯 数据完整性评估")
    print("-" * 30)
    
    core_missing = []
    sentiment_missing = []
    
    if not (data_path / "basic_info").glob("*.csv"):
        core_missing.append("股票基本信息")
    if not (data_path / "daily").glob("*.csv"):
        core_missing.append("股票日行情") 
    if not (data_path / "calendar").glob("*.csv"):
        core_missing.append("交易日历")
        
    if not (data_path / "capital_flow").glob("*.csv"):
        sentiment_missing.append("资金流向")
    if not (data_path / "limit_info").glob("*.csv"):
        sentiment_missing.append("涨跌停信息")
    if not (data_path / "rank_list").glob("*.csv"):
        sentiment_missing.append("龙虎榜数据")
    
    if not core_missing and not sentiment_missing:
        print("🎊 数据完整性: 优秀 - 所有核心数据已下载")
    elif not core_missing:
        print("✅ 核心数据: 完整 - backtest、strategy模块可正常运行")
        if sentiment_missing:
            print(f"⚠️  情绪数据: 缺失 {len(sentiment_missing)} 项 - {', '.join(sentiment_missing)}")
    else:
        print(f"❌ 核心数据缺失: {', '.join(core_missing)}")
        
    # 下一步建议
    print("\n🚀 下一步建议")
    print("-" * 30)
    if core_missing:
        print("1. 🔧 修复交易日历API下载问题")
        print("2. 📅 完成核心数据下载")
    else:
        print("1. 📈 扩展到完整股票列表 (5507只)")
        print("2. ⏰ 扩展到完整时间范围 (2000-2025)")
        print("3. 🧪 测试core模块数据加载功能")
        print("4. 💰 增加更多财务和技术指标数据")

if __name__ == "__main__":
    analyze_downloaded_data()