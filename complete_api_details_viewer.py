#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
完整API详情查看器 - 展示所有69个API的完整明细
"""

import pandas as pd
from pathlib import Path

def display_complete_api_details():
    """展示所有API的完整详细信息"""
    
    # 读取详细分析报告
    overview_file = Path("API详细分析报告_概览.csv")
    quality_file = Path("API详细分析报告_质量.csv")
    
    if not overview_file.exists():
        print("❌ 未找到API详细分析报告，请先运行 detailed_api_analyzer.py")
        return
    
    # 读取数据
    df_overview = pd.read_csv(overview_file)
    
    print("="*120)
    print("🎯 **QuantTrade 全部69个API详细明细** 🎯")
    print("="*120)
    print(f"📊 数据统计时间: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"📋 API总数: {len(df_overview)} 个")
    
    # 按分类展示每个API
    categories = df_overview['category'].unique()
    
    for category in categories:
        category_apis = df_overview[df_overview['category'] == category].sort_values('total_size_mb', ascending=False)
        
        print(f"\n" + "="*100)
        print(f"📁 **{category.upper()}** - {len(category_apis)} 个API")
        print("="*100)
        
        for idx, api in category_apis.iterrows():
            # API基本信息
            print(f"\n🔹 **{api['api_name']}**")
            print(f"   📝 中文描述: {api['chinese_description']}")
            
            # 数据规模
            if api['file_count'] > 0:
                print(f"   📊 数据规模: {api['file_count']} 个文件, {api['total_size_mb']:.1f}MB")
                print(f"   📈 记录统计: 约 {api['estimated_total_records']:,} 条记录")
                print(f"   📏 数据结构: 平均 {api['avg_rows_per_file']:,} 行 × {api['avg_columns_per_file']} 列")
            else:
                print(f"   ⚠️  数据状态: 暂无数据文件")
                continue
            
            # 时间覆盖
            time_desc = api['date_range_description'] if pd.notna(api['date_range_description']) else "时间范围未知"
            print(f"   📅 时间覆盖: {time_desc}")
            
            # 数据质量
            missing_rate = api['avg_missing_rate']
            if missing_rate == 0:
                quality_status = "🟢 优秀"
            elif missing_rate < 10:
                quality_status = "🟡 良好"
            elif missing_rate < 30:
                quality_status = "🟠 一般"
            else:
                quality_status = "🔴 待优化"
            
            print(f"   🎯 数据质量: {quality_status} (缺失率: {missing_rate:.2f}%)")
            
            # 数据更新模式
            pattern_desc = {
                'yearly': '按年更新',
                'quarterly': '按季度更新', 
                'monthly': '按月更新',
                'daily': '按日更新',
                'snapshot': '快照数据',
                'data_driven': '数据驱动',
                'unknown': '更新模式未知'
            }.get(api['time_pattern'], api['time_pattern'])
            
            print(f"   🔄 更新模式: {pattern_desc}")
            
            # 相对重要性（基于数据量）
            size_mb = api['total_size_mb']
            if size_mb > 1000:
                importance = "🔴 核心数据源"
            elif size_mb > 100:
                importance = "🟠 重要数据源"
            elif size_mb > 10:
                importance = "🟡 常用数据源"
            else:
                importance = "🟢 辅助数据源"
            
            print(f"   ⭐ 重要程度: {importance}")
    
    # 生成汇总统计
    print(f"\n" + "="*100)
    print("📊 **全体API汇总统计**")
    print("="*100)
    
    # 按分类统计
    print("\n🏷️ **按分类统计**:")
    category_stats = df_overview.groupby('category').agg({
        'api_name': 'count',
        'file_count': 'sum',
        'total_size_mb': 'sum',
        'estimated_total_records': 'sum'
    }).round(1)
    
    for category, stats in category_stats.iterrows():
        print(f"  📁 {category}: {int(stats['api_name'])} APIs, "
              f"{int(stats['file_count'])} 文件, "
              f"{stats['total_size_mb']:.0f}MB, "
              f"{int(stats['estimated_total_records']):,} 记录")
    
    # 按数据量排序的Top 10
    print(f"\n🔝 **数据量Top 10 API**:")
    top_apis = df_overview.nlargest(10, 'total_size_mb')
    for i, (_, api) in enumerate(top_apis.iterrows(), 1):
        print(f"  {i:2d}. {api['api_name']}: {api['total_size_mb']:.0f}MB "
              f"({api['estimated_total_records']:,} 条记录)")
    
    # 按记录数排序的Top 10
    print(f"\n📈 **记录数量Top 10 API**:")
    top_records = df_overview.nlargest(10, 'estimated_total_records')
    for i, (_, api) in enumerate(top_records.iterrows(), 1):
        print(f"  {i:2d}. {api['api_name']}: {api['estimated_total_records']:,} 条记录 "
              f"({api['total_size_mb']:.0f}MB)")
    
    # 数据质量分布
    print(f"\n🎯 **数据质量分布**:")
    quality_ranges = [
        (0, 5, "🟢 优秀"),
        (5, 15, "🟡 良好"), 
        (15, 35, "🟠 一般"),
        (35, 100, "🔴 待优化")
    ]
    
    for min_rate, max_rate, status in quality_ranges:
        count = len(df_overview[
            (df_overview['avg_missing_rate'] >= min_rate) & 
            (df_overview['avg_missing_rate'] < max_rate) &
            (df_overview['file_count'] > 0)
        ])
        print(f"  {status} (缺失率{min_rate}-{max_rate}%): {count} 个API")
    
    # 时间模式分布
    print(f"\n📅 **数据更新模式分布**:")
    pattern_counts = df_overview[df_overview['file_count'] > 0]['time_pattern'].value_counts()
    pattern_names = {
        'yearly': '年度更新',
        'quarterly': '季度更新',
        'monthly': '月度更新', 
        'daily': '日度更新',
        'snapshot': '快照数据',
        'data_driven': '数据驱动',
        'unknown': '未知模式'
    }
    
    for pattern, count in pattern_counts.items():
        pattern_name = pattern_names.get(pattern, pattern)
        print(f"  📊 {pattern_name}: {count} 个API")
    
    # 无数据的API
    no_data_apis = df_overview[df_overview['file_count'] == 0]
    if len(no_data_apis) > 0:
        print(f"\n⚠️  **暂无数据的API ({len(no_data_apis)} 个)**:")
        for _, api in no_data_apis.iterrows():
            print(f"  🔴 {api['api_name']} ({api['category']})")
    
    print(f"\n" + "="*100)
    print("✅ **完整API明细展示完毕**")
    print(f"💾 详细数据已保存至: API详细分析报告_概览.csv")
    print(f"📊 质量详情已保存至: API详细分析报告_质量.csv") 
    print(f"📁 分类汇总已保存至: API详细分析报告_分类汇总.csv")
    print("="*100)

if __name__ == "__main__":
    display_complete_api_details()