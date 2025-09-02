#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
大规模数据下载完成报告
===================

统计所有已下载的数据情况
"""

import pandas as pd
from pathlib import Path
from datetime import datetime

def generate_massive_download_report():
    """生成大规模下载报告"""
    
    print("🎊 优矿大规模数据下载完成报告")
    print("=" * 70)
    print(f"⏰ 报告时间: {datetime.now()}")
    print()
    
    data_path = Path("/Users/jackstudio/QuantTrade/data")
    
    # 所有数据目录
    data_categories = {
        "basic_info": {"name": "股票基本信息", "api": "EquGet", "priority": 1},
        "calendar": {"name": "交易日历", "api": "TradeCalGet", "priority": 1}, 
        "daily": {"name": "股票日行情", "api": "MktEqudGet", "priority": 1},
        "adjustment": {"name": "复权因子", "api": "MktAdjfGet", "priority": 2},
        "dividend": {"name": "股票分红", "api": "EquDivGet", "priority": 2},
        "financial": {"name": "财务数据", "api": "FdmtBs2018Get等", "priority": 2},
        "capital_flow": {"name": "资金流向", "api": "MktEquFlowGet", "priority": 3},
        "limit_info": {"name": "涨跌停信息", "api": "MktLimitGet", "priority": 3},
        "rank_list": {"name": "龙虎榜数据", "api": "MktRankListStocksGet", "priority": 3}
    }
    
    # 分优先级统计
    priority_stats = {1: [], 2: [], 3: []}
    total_size_gb = 0
    total_files = 0
    total_records = 0
    
    for category, info in data_categories.items():
        dir_path = data_path / category
        
        print(f"📁 {info['name']} ({info['api']})")
        
        if dir_path.exists():
            csv_files = list(dir_path.glob("*.csv"))
            
            if csv_files:
                cat_size = 0
                cat_records = 0
                
                for file in csv_files:
                    try:
                        file_size_mb = file.stat().st_size / (1024*1024)
                        cat_size += file_size_mb
                        
                        # 读取记录数
                        df = pd.read_csv(file)
                        records = len(df)
                        cat_records += records
                        
                        print(f"   📄 {file.name}: {records:,} 条记录, {file_size_mb:.1f}MB")
                        
                        # 显示时间范围和股票数量
                        if 'tradeDate' in df.columns:
                            date_range = f"{df['tradeDate'].min()} 至 {df['tradeDate'].max()}"
                            print(f"      📅 时间: {date_range}")
                            if 'ticker' in df.columns:
                                stock_count = df['ticker'].nunique()
                                print(f"      🏢 股票: {stock_count} 只")
                        elif 'endDate' in df.columns:
                            date_range = f"{df['endDate'].min()} 至 {df['endDate'].max()}"
                            print(f"      📅 时间: {date_range}")
                            if 'ticker' in df.columns:
                                stock_count = df['ticker'].nunique()
                                print(f"      🏢 公司: {stock_count} 家")
                        elif 'calendarDate' in df.columns:
                            date_range = f"{df['calendarDate'].min()} 至 {df['calendarDate'].max()}"
                            print(f"      📅 时间: {date_range}")
                        elif 'exDivDate' in df.columns:
                            date_range = f"{df['exDivDate'].min()} 至 {df['exDivDate'].max()}"
                            print(f"      📅 时间: {date_range}")
                            if 'ticker' in df.columns:
                                stock_count = df['ticker'].nunique()
                                print(f"      🏢 股票: {stock_count} 只")
                        
                    except Exception as e:
                        print(f"   ❌ {file.name}: 读取失败 - {e}")
                        cat_size += file.stat().st_size / (1024*1024)
                
                print(f"   📊 子目录总计: {len(csv_files)} 文件, {cat_records:,} 记录, {cat_size:.1f}MB")
                priority_stats[info['priority']].append({
                    'name': info['name'], 
                    'files': len(csv_files),
                    'records': cat_records,
                    'size_mb': cat_size
                })
                
                total_size_gb += cat_size / 1024
                total_files += len(csv_files)
                total_records += cat_records
                
            else:
                print(f"   📂 目录存在但无CSV文件")
        else:
            print(f"   ❌ 目录不存在")
        
        print()
    
    # 按优先级汇总
    print("📊 按优先级汇总统计")
    print("-" * 50)
    
    priority_names = {
        1: "🔥 核心交易数据",
        2: "💰 技术分析数据", 
        3: "🧠 情绪分析数据"
    }
    
    for priority in [1, 2, 3]:
        print(f"{priority_names[priority]}")
        
        if priority_stats[priority]:
            p_files = sum(item['files'] for item in priority_stats[priority])
            p_records = sum(item['records'] for item in priority_stats[priority])
            p_size = sum(item['size_mb'] for item in priority_stats[priority])
            
            print(f"   📁 数据集: {len(priority_stats[priority])} 个")
            print(f"   📄 文件数: {p_files} 个")
            print(f"   📋 记录数: {p_records:,} 条")
            print(f"   💾 数据量: {p_size:.1f}MB")
            
            for item in priority_stats[priority]:
                status = "✅" if item['records'] > 0 else "⚠️"
                print(f"     {status} {item['name']}: {item['records']:,} 条记录")
        else:
            print(f"   ⏳ 该优先级暂无数据")
        
        print()
    
    # 总体统计
    print("🎯 总体下载统计")
    print("-" * 40)
    print(f"📁 数据集: {len([s for stats in priority_stats.values() for s in stats])} 个")
    print(f"📄 文件总数: {total_files} 个")
    print(f"📋 记录总数: {total_records:,} 条")  
    print(f"💾 数据总量: {total_size_gb:.2f} GB")
    
    # 下载完成度
    expected_datasets = 10  # 预期的数据集数量
    completed_datasets = len([s for stats in priority_stats.values() for s in stats if s['records'] > 0])
    completion_rate = (completed_datasets / expected_datasets) * 100
    
    print(f"📈 完成度: {completion_rate:.1f}% ({completed_datasets}/{expected_datasets})")
    
    print()
    
    # Framework支持评估
    print("🎯 QuantTrade Framework 支持评估")  
    print("-" * 50)
    
    core_complete = len(priority_stats[1]) >= 2  # 至少需要日行情+交易日历
    technical_complete = len(priority_stats[2]) >= 2  # 需要复权+财务
    sentiment_complete = len(priority_stats[3]) >= 1  # 至少需要资金流向
    
    print(f"🔥 核心交易功能: {'✅ 支持' if core_complete else '⏳ 部分支持'}")
    print(f"💰 技术分析功能: {'✅ 支持' if technical_complete else '⏳ 部分支持'}")  
    print(f"🧠 情绪分析功能: {'✅ 支持' if sentiment_complete else '⏳ 部分支持'}")
    print(f"📊 基本面分析功能: {'✅ 支持' if len([s for s in priority_stats[2] if '财务' in s['name']]) > 0 else '⏳ 部分支持'}")
    
    if core_complete and technical_complete and sentiment_complete:
        print(f"\n🎊 Framework完整功能支持: ✅ 已就绪!")
        print(f"🚀 可开始量化交易策略开发和回测!")
    else:
        print(f"\n⏳ Framework功能支持: 基础功能可用，等待更多数据补充")

if __name__ == "__main__":
    generate_massive_download_report()