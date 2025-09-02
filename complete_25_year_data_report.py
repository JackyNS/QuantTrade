#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
QuantTrade框架25年完整数据覆盖报告
==============================

综合统计2000年1月1日至2025年8月31日
全市场A股数据完整性和覆盖情况
"""

import pandas as pd
from pathlib import Path
from datetime import datetime
import numpy as np

def generate_complete_25_year_report():
    """生成25年完整数据覆盖报告"""
    
    print("🎊 QuantTrade框架25年完整数据覆盖报告")
    print("=" * 80)
    print(f"⏰ 报告时间: {datetime.now()}")
    print(f"📅 数据覆盖: 2000年1月1日 - 2025年8月31日 (25年)")
    print(f"🎯 目标: 全市场A股量化交易数据完整性评估")
    print()
    
    data_path = Path("/Users/jackstudio/QuantTrade/data")
    
    # 核心10个API数据统计
    api_categories = {
        "basic_info": {
            "name": "股票基本信息", 
            "api": "EquGet",
            "priority": "🔥 核心",
            "patterns": ["equget.csv", "*股票基本*2000_2009.csv"],
            "required": True
        },
        "calendar": {
            "name": "交易日历", 
            "api": "TradeCalGet", 
            "priority": "🔥 核心",
            "patterns": ["trading_calendar.csv", "*交易日历*2000_2009.csv"],
            "required": True
        },
        "daily": {
            "name": "股票日行情", 
            "api": "MktEqudGet",
            "priority": "🔥 核心", 
            "patterns": ["daily_*.csv", "*股票日行情*2000_2009.csv"],
            "required": True
        },
        "adjustment": {
            "name": "复权因子", 
            "api": "MktAdjfGet",
            "priority": "💰 技术",
            "patterns": ["adjustment_*.csv", "*复权因子*2000_2009.csv"],
            "required": True
        },
        "dividend": {
            "name": "股票分红", 
            "api": "EquDivGet",
            "priority": "💰 技术",
            "patterns": ["dividend_*.csv", "*股票分红*2000_2009.csv"],
            "required": True
        },
        "financial": {
            "name": "财务数据", 
            "api": "FdmtBs2018Get等",
            "priority": "💰 技术",
            "patterns": ["*.csv", "*财务*2000_2009.csv"],
            "required": True
        },
        "capital_flow": {
            "name": "资金流向", 
            "api": "MktEquFlowGet",
            "priority": "🧠 情绪",
            "patterns": ["capital_flow_*.csv", "*资金流向*2000_2009.csv"],
            "required": False
        },
        "limit_info": {
            "name": "涨跌停信息", 
            "api": "MktLimitGet",
            "priority": "🧠 情绪",
            "patterns": ["limit_*.csv", "*涨跌停*2000_2009.csv"],
            "required": False
        },
        "rank_list": {
            "name": "龙虎榜数据", 
            "api": "MktRankListStocksGet",
            "priority": "🧠 情绪",
            "patterns": ["rank_*.csv", "*龙虎榜*2000_2009.csv"],
            "required": False
        }
    }
    
    # 统计各数据集情况
    total_files = 0
    total_size_gb = 0
    total_records = 0
    coverage_summary = {}
    
    print("📊 各数据集详细统计:")
    print("-" * 80)
    
    for category, info in api_categories.items():
        dir_path = data_path / category
        
        print(f"\n{info['priority']} {info['name']} ({info['api']})")
        print("-" * 50)
        
        if dir_path.exists():
            all_files = []
            for pattern in info['patterns']:
                all_files.extend(list(dir_path.glob(pattern)))
            
            # 去重
            all_files = list(set(all_files))
            
            if all_files:
                cat_size = 0
                cat_records = 0
                modern_files = []
                historical_files = []
                
                for file in all_files:
                    file_size_mb = file.stat().st_size / (1024*1024)
                    cat_size += file_size_mb
                    
                    try:
                        df = pd.read_csv(file)
                        records = len(df)
                        cat_records += records
                        
                        # 分类现代和历史文件
                        if "2000_2009" in file.name:
                            historical_files.append((file, records, file_size_mb))
                        else:
                            modern_files.append((file, records, file_size_mb))
                            
                    except Exception as e:
                        print(f"   ❌ {file.name}: 读取失败 - {e}")
                        cat_size += file_size_mb
                
                # 显示现代数据 (2010-2025)
                if modern_files:
                    print("   📈 2010-2025年数据:")
                    for file, records, size in modern_files[:3]:  # 显示前3个文件
                        print(f"      📄 {file.name}: {records:,} 条记录, {size:.1f}MB")
                    if len(modern_files) > 3:
                        remaining = len(modern_files) - 3
                        remaining_records = sum(r for _, r, _ in modern_files[3:])
                        print(f"      ... 还有{remaining}个文件, {remaining_records:,} 条记录")
                
                # 显示历史数据 (2000-2009)
                if historical_files:
                    print("   📜 2000-2009年历史数据:")
                    for file, records, size in historical_files:
                        print(f"      📄 {file.name}: {records:,} 条记录, {size:.1f}MB")
                
                # 数据完整性评估
                has_modern = len(modern_files) > 0
                has_historical = len(historical_files) > 0
                
                if has_modern and has_historical:
                    status = "🎊 完整覆盖"
                    coverage = "25年"
                elif has_modern:
                    status = "⏳ 部分覆盖" 
                    coverage = "15年 (2010-2025)"
                elif has_historical:
                    status = "📜 历史覆盖"
                    coverage = "10年 (2000-2009)"
                else:
                    status = "❌ 无数据"
                    coverage = "0年"
                
                print(f"   📊 {info['name']}统计:")
                print(f"      📁 文件数: {len(all_files)} 个")
                print(f"      📋 记录数: {cat_records:,} 条")
                print(f"      💾 数据量: {cat_size:.1f}MB")
                print(f"      {status} ({coverage})")
                
                coverage_summary[category] = {
                    'name': info['name'],
                    'priority': info['priority'],
                    'files': len(all_files),
                    'records': cat_records,
                    'size_mb': cat_size,
                    'coverage': coverage,
                    'status': status,
                    'required': info['required'],
                    'has_modern': has_modern,
                    'has_historical': has_historical
                }
                
                total_files += len(all_files)
                total_size_gb += cat_size / 1024
                total_records += cat_records
                
            else:
                print(f"   📂 目录存在但无匹配文件")
                coverage_summary[category] = {
                    'name': info['name'], 
                    'priority': info['priority'],
                    'files': 0,
                    'records': 0,
                    'size_mb': 0,
                    'coverage': "0年",
                    'status': "❌ 无数据",
                    'required': info['required'],
                    'has_modern': False,
                    'has_historical': False
                }
        else:
            print(f"   ❌ 目录不存在")
            coverage_summary[category] = {
                'name': info['name'],
                'priority': info['priority'], 
                'files': 0,
                'records': 0,
                'size_mb': 0,
                'coverage': "0年",
                'status': "❌ 无数据",
                'required': info['required'],
                'has_modern': False,
                'has_historical': False
            }
    
    # 按优先级汇总
    print(f"\n📈 按功能模块汇总:")
    print("=" * 60)
    
    priority_groups = {
        "🔥 核心": [],
        "💰 技术": [], 
        "🧠 情绪": []
    }
    
    for cat, summary in coverage_summary.items():
        priority = summary['priority']
        priority_groups[priority].append(summary)
    
    for priority, items in priority_groups.items():
        if items:
            group_files = sum(item['files'] for item in items)
            group_records = sum(item['records'] for item in items)  
            group_size = sum(item['size_mb'] for item in items)
            complete_count = sum(1 for item in items if item['has_modern'] and item['has_historical'])
            
            print(f"\n{priority} 数据 ({len(items)}个数据集)")
            print(f"   📁 总文件: {group_files} 个")
            print(f"   📋 总记录: {group_records:,} 条") 
            print(f"   💾 总大小: {group_size:.1f}MB")
            print(f"   🎯 完整覆盖: {complete_count}/{len(items)} 个")
            
            for item in items:
                required_mark = "⭐" if item['required'] else "  "
                print(f"   {required_mark} {item['status']} {item['name']}: {item['coverage']}")
    
    # 总体评估
    print(f"\n🎯 QuantTrade框架数据完整性评估:")
    print("=" * 60)
    
    # 计算各种完成度
    total_datasets = len(coverage_summary)
    complete_datasets = sum(1 for s in coverage_summary.values() if s['has_modern'] and s['has_historical'])
    partial_datasets = sum(1 for s in coverage_summary.values() if s['has_modern'] or s['has_historical'])
    required_datasets = sum(1 for s in coverage_summary.values() if s['required'])
    required_complete = sum(1 for s in coverage_summary.values() if s['required'] and s['has_modern'] and s['has_historical'])
    
    print(f"📊 数据总量统计:")
    print(f"   📁 总文件数: {total_files} 个")
    print(f"   📋 总记录数: {total_records:,} 条")
    print(f"   💾 总数据量: {total_size_gb:.2f} GB")
    print(f"   📅 时间跨度: 25年 (2000-2025)")
    
    print(f"\n🎯 完整性评估:")
    complete_rate = (complete_datasets / total_datasets) * 100
    partial_rate = (partial_datasets / total_datasets) * 100
    required_rate = (required_complete / required_datasets) * 100 if required_datasets > 0 else 0
    
    print(f"   📈 25年完整覆盖: {complete_datasets}/{total_datasets} ({complete_rate:.1f}%)")
    print(f"   📊 部分数据覆盖: {partial_datasets}/{total_datasets} ({partial_rate:.1f}%)")
    print(f"   ⭐ 必需数据完整: {required_complete}/{required_datasets} ({required_rate:.1f}%)")
    
    # 框架功能支持评估
    print(f"\n🚀 QuantTrade框架功能支持评估:")
    print("=" * 60)
    
    # 核心功能评估 (基本信息+日历+日行情)
    core_complete = all(coverage_summary[api]['has_modern'] or coverage_summary[api]['has_historical'] 
                       for api in ['basic_info', 'calendar', 'daily'])
    
    # 技术分析功能 (复权+分红)  
    technical_complete = all(coverage_summary[api]['has_modern'] or coverage_summary[api]['has_historical']
                           for api in ['adjustment', 'dividend'])
    
    # 基本面分析功能 (财务数据)
    fundamental_complete = coverage_summary['financial']['has_modern'] or coverage_summary['financial']['has_historical']
    
    # 情绪分析功能 (资金流向)
    sentiment_complete = coverage_summary['capital_flow']['has_modern'] or coverage_summary['capital_flow']['has_historical']
    
    print(f"🔥 核心交易功能: {'✅ 完全支持' if core_complete else '⚠️ 部分支持'}")
    print(f"💰 技术分析功能: {'✅ 完全支持' if technical_complete else '⚠️ 部分支持'}")
    print(f"📊 基本面分析功能: {'✅ 完全支持' if fundamental_complete else '⚠️ 部分支持'}")  
    print(f"🧠 情绪分析功能: {'✅ 完全支持' if sentiment_complete else '⚠️ 部分支持'}")
    
    # 最终评估
    if complete_rate >= 80 and required_rate >= 90:
        final_status = "🎊 框架数据就绪"
        recommendation = "✅ 可开始全功能量化交易策略开发!"
    elif complete_rate >= 60 or required_rate >= 80:
        final_status = "⏳ 框架基本就绪"
        recommendation = "🔧 建议补齐历史数据以获得最佳回测效果"
    else:
        final_status = "🔧 框架数据不完整"
        recommendation = "⚠️ 需要下载更多数据才能正常使用"
    
    print(f"\n{final_status}")
    print(f"📋 综合完成度: {complete_rate:.1f}% (完整) + {partial_rate-complete_rate:.1f}% (部分)")
    print(f"💡 {recommendation}")
    
    # 数据质量提示
    if total_size_gb > 1.0:
        print(f"\n💾 数据规模: 大型数据集 ({total_size_gb:.1f}GB)")
        print(f"🚀 支持: 高频策略、长周期回测、多因子模型")
    elif total_size_gb > 0.5:
        print(f"\n💾 数据规模: 中型数据集 ({total_size_gb:.1f}GB)")
        print(f"📈 支持: 中频策略、中期回测、技术分析")
    else:
        print(f"\n💾 数据规模: 小型数据集 ({total_size_gb:.1f}GB)")
        print(f"📊 支持: 基础策略、短期分析")

if __name__ == "__main__":
    generate_complete_25_year_report()