#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据下载进度报告
===============

实时监控数据下载进度和完成情况
"""

import pandas as pd
from pathlib import Path
from datetime import datetime
import os

def check_download_progress():
    """检查数据下载进度"""
    
    print("📊 优矿数据下载进度报告")
    print("=" * 60)
    print(f"⏰ 检查时间: {datetime.now()}")
    print()
    
    data_path = Path("/Users/jackstudio/QuantTrade/data")
    
    # 检查已完成的数据
    data_status = {
        "basic_info": {
            "name": "股票基本信息",
            "api": "EquGet", 
            "status": "✅ 已完成",
            "files": list((data_path / "basic_info").glob("*.csv"))
        },
        "calendar": {
            "name": "交易日历",
            "api": "TradeCalGet",
            "status": "✅ 已完成", 
            "files": list((data_path / "calendar").glob("*.csv"))
        },
        "daily": {
            "name": "股票日行情",
            "api": "MktEqudGet",
            "status": "🔄 下载中",
            "files": list((data_path / "daily").glob("*.csv"))
        },
        "capital_flow": {
            "name": "资金流向",
            "api": "MktEquFlowGet", 
            "status": "🔄 下载中",
            "files": list((data_path / "capital_flow").glob("*.csv"))
        },
        "adjustment": {
            "name": "复权因子",
            "api": "MktAdjfGet",
            "status": "⏳ 待下载",
            "files": list((data_path / "adjustment").glob("*.csv"))
        },
        "dividend": {
            "name": "股票分红",
            "api": "EquDivGet",
            "status": "⏳ 待下载",
            "files": list((data_path / "dividend").glob("*.csv"))
        },
        "financial": {
            "name": "财务数据",
            "api": "FdmtBSGet",
            "status": "❌ 需要权限",
            "files": list((data_path / "financial").glob("*.csv"))
        },
        "limit_info": {
            "name": "涨跌停信息",
            "api": "MktLimitGet", 
            "status": "⏳ 待下载",
            "files": list((data_path / "limit_info").glob("*.csv"))
        },
        "rank_list": {
            "name": "龙虎榜数据",
            "api": "MktRankListStocksGet",
            "status": "⏳ 待下载", 
            "files": list((data_path / "rank_list").glob("*.csv"))
        }
    }
    
    # 显示详细状态
    completed = 0
    downloading = 0
    pending = 0
    permission_needed = 0
    
    for category, info in data_status.items():
        print(f"📁 {info['name']} ({info['api']})")
        print(f"   状态: {info['status']}")
        
        if info['files']:
            for file in info['files']:
                try:
                    size_mb = file.stat().st_size / (1024*1024)
                    df = pd.read_csv(file)
                    records = len(df)
                    
                    print(f"   📄 {file.name}: {records:,} 条记录, {size_mb:.1f}MB")
                    
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
                        
                except Exception as e:
                    print(f"   ❌ 文件读取失败: {e}")
        else:
            print(f"   📂 目录: {category}/ (空)")
        
        # 统计状态
        if "已完成" in info['status']:
            completed += 1
        elif "下载中" in info['status']:
            downloading += 1  
        elif "待下载" in info['status']:
            pending += 1
        elif "需要权限" in info['status']:
            permission_needed += 1
        
        print()
    
    # 进度总结
    total = len(data_status)
    progress = (completed / total) * 100
    
    print("📈 下载进度总结")
    print("-" * 40)
    print(f"✅ 已完成: {completed} 个数据集")
    print(f"🔄 下载中: {downloading} 个数据集")  
    print(f"⏳ 待下载: {pending} 个数据集")
    print(f"❌ 需要权限: {permission_needed} 个数据集")
    print(f"📊 总体进度: {progress:.1f}% ({completed}/{total})")
    
    print()
    
    # 下一步建议
    print("🚀 下一步建议")
    print("-" * 30)
    
    if downloading > 0:
        print("1. ⏰ 等待当前下载完成")
        print("2. 📊 监控下载进度")
    
    if pending > 0:
        print("3. 🔄 继续下载待下载数据")
        print("4. 🎯 优先完成核心数据")
    
    if permission_needed > 0:
        print("5. 💰 考虑购买优矿专业版获取财务数据")
        print("6. 📋 或使用其他数据源补充")
    
    print("7. 🧪 测试已下载数据的完整性")
    print("8. 🔧 优化分批下载策略")

if __name__ == "__main__":
    check_download_progress()