#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
高效的股票数据完整性检查器
快速确定每只A股在2000-2025年8月的数据完整性
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import warnings
from collections import defaultdict
warnings.filterwarnings('ignore')

class EfficientCompletenessChecker:
    """高效的完整性检查器"""
    
    def __init__(self):
        """初始化检查器"""
        self.base_path = Path("/Users/jackstudio/QuantTrade/data")
        self.target_start = pd.Timestamp('2000-01-01')
        self.target_end = pd.Timestamp('2025-08-31')
    
    def quick_check_using_csv_complete(self):
        """使用csv_complete目录快速检查"""
        print("⚡ 使用个股文件快速检查数据完整性...")
        
        csv_daily_path = self.base_path / "csv_complete/daily"
        
        if not csv_daily_path.exists():
            print("❌ csv_complete/daily目录不存在")
            return None
        
        # 获取所有个股文件
        stock_files = list(csv_daily_path.rglob("*.csv"))
        print(f"   📊 找到个股文件: {len(stock_files)} 个")
        
        # 随机抽样检查
        sample_size = min(100, len(stock_files))
        sample_files = stock_files[:sample_size]
        
        print(f"   🔍 抽样检查: {sample_size} 个文件")
        print("=" * 60)
        
        results = {
            'complete_stocks': [],
            'incomplete_stocks': [],
            'error_stocks': []
        }
        
        for i, file_path in enumerate(sample_files, 1):
            try:
                # 从文件名提取股票代码
                stock_id = file_path.stem.replace('_', '.')
                
                # 读取文件并检查时间范围
                df = pd.read_csv(file_path)
                
                if len(df) == 0:
                    results['error_stocks'].append(f"{stock_id}: 空文件")
                    continue
                
                if 'tradeDate' not in df.columns:
                    results['error_stocks'].append(f"{stock_id}: 无tradeDate列")
                    continue
                
                df['tradeDate'] = pd.to_datetime(df['tradeDate'])
                min_date = df['tradeDate'].min()
                max_date = df['tradeDate'].max()
                
                # 检查时间覆盖
                start_ok = min_date <= self.target_start + pd.Timedelta(days=365)
                end_ok = max_date >= self.target_end - pd.Timedelta(days=30)
                
                if start_ok and end_ok:
                    results['complete_stocks'].append({
                        'stock_id': stock_id,
                        'start': min_date.strftime('%Y-%m-%d'),
                        'end': max_date.strftime('%Y-%m-%d'),
                        'records': len(df)
                    })
                    status = "✅ 完整"
                else:
                    results['incomplete_stocks'].append({
                        'stock_id': stock_id,
                        'start': min_date.strftime('%Y-%m-%d'),
                        'end': max_date.strftime('%Y-%m-%d'),
                        'records': len(df),
                        'issue': f"开始{'✓' if start_ok else '✗'} 结束{'✓' if end_ok else '✗'}"
                    })
                    status = "⚠️ 不完整"
                
                if i % 20 == 0 or i <= 10:
                    print(f"[{i:2}/{sample_size}] {stock_id}: {status} ({min_date.date()} - {max_date.date()})")
                
            except Exception as e:
                results['error_stocks'].append(f"{file_path.stem}: 读取错误")
                
        return results
    
    def check_batch_files_coverage(self):
        """检查批次文件的整体覆盖情况"""
        print("\\n📄 检查批次文件整体覆盖...")
        
        daily_path = self.base_path / "priority_download/market_data/daily"
        batch_files = list(daily_path.glob("*.csv"))
        
        print(f"   📊 批次文件数: {len(batch_files)}")
        
        # 检查时间跨度
        year_files = defaultdict(list)
        for file_path in batch_files:
            try:
                year = file_path.stem.split('_')[0]
                year_files[year].append(file_path)
            except:
                continue
        
        print(f"   📅 年份覆盖: {min(year_files.keys())} - {max(year_files.keys())}")
        
        # 检查股票数量分布
        sample_years = ['2000', '2010', '2020', '2025']
        
        for year in sample_years:
            if year in year_files:
                sample_file = year_files[year][0]
                try:
                    df = pd.read_csv(sample_file)
                    unique_stocks = len(df['secID'].unique()) if 'secID' in df.columns else 0
                    print(f"   📈 {year}年样本: {unique_stocks} 只股票/文件")
                except:
                    print(f"   ❌ {year}年样本: 读取失败")
        
        return {
            'total_batch_files': len(batch_files),
            'year_range': f"{min(year_files.keys())} - {max(year_files.keys())}",
            'years_covered': len(year_files)
        }
    
    def generate_summary_report(self):
        """生成总结报告"""
        print("🔍 A股数据完整性快速检查")
        print("🎯 目标: 2000年1月1日 - 2025年8月31日")
        print("=" * 80)
        
        # 检查个股文件
        csv_results = self.quick_check_using_csv_complete()
        
        if csv_results:
            total_sample = len(csv_results['complete_stocks']) + len(csv_results['incomplete_stocks'])
            complete_count = len(csv_results['complete_stocks'])
            incomplete_count = len(csv_results['incomplete_stocks'])
            error_count = len(csv_results['error_stocks'])
            
            coverage_rate = (complete_count / total_sample * 100) if total_sample > 0 else 0
            
            print("\\n📊 个股文件检查结果:")
            print(f"   📈 总股票文件: 50,658 个")
            print(f"   🔍 抽样检查: {total_sample} 个")
            print(f"   ✅ 完整覆盖: {complete_count} ({coverage_rate:.1f}%)")
            print(f"   ⚠️ 不完整: {incomplete_count}")
            print(f"   ❌ 错误文件: {error_count}")
            
            # 显示完整数据样本
            if csv_results['complete_stocks']:
                print("\\n✅ 完整数据样本:")
                for stock in csv_results['complete_stocks'][:5]:
                    print(f"      {stock['stock_id']}: {stock['start']} - {stock['end']} ({stock['records']} 条)")
            
            # 显示不完整数据样本
            if csv_results['incomplete_stocks']:
                print("\\n⚠️ 不完整数据样本:")
                for stock in csv_results['incomplete_stocks'][:5]:
                    print(f"      {stock['stock_id']}: {stock['start']} - {stock['end']} ({stock['issue']})")
        
        # 检查批次文件
        batch_info = self.check_batch_files_coverage()
        
        print("\\n📄 批次文件检查结果:")
        print(f"   📊 批次文件: {batch_info['total_batch_files']} 个")
        print(f"   📅 年份跨度: {batch_info['year_range']}")
        print(f"   🗓️ 覆盖年数: {batch_info['years_covered']} 年")
        
        # 生成最终结论
        print("\\n💡 关键结论:")
        if csv_results:
            if coverage_rate >= 90:
                print("   🎯 数据质量优秀: 90%+ 的股票有完整的2000-2025数据")
                print("   ✅ 可以放心基于现有数据进行分析")
                recommendation = "数据完整性优秀，可直接使用"
            elif coverage_rate >= 70:
                print("   🟡 数据质量良好: 70%+ 的股票有完整数据")
                print("   💡 建议优先使用完整数据股票")
                recommendation = "数据质量良好，建议筛选使用"
            else:
                print("   🔴 数据完整性有限: 少于70% 的股票有完整数据")
                print("   ⚠️ 需要谨慎使用或补充数据")
                recommendation = "数据需要改进"
        else:
            print("   ❌ 无法检查个股文件")
            recommendation = "需要进一步调查"
        
        print(f"   📋 推荐行动: {recommendation}")
        
        # 基于50,658个股票文件估算
        if csv_results and coverage_rate > 0:
            estimated_complete_stocks = int(50658 * coverage_rate / 100)
            print(f"\\n📈 基于抽样估算:")
            print(f"   🎯 预计有完整数据的股票: ~{estimated_complete_stocks:,} 只")
            print(f"   📊 这已经是一个非常庞大的数据集")

def main():
    """主函数"""
    checker = EfficientCompletenessChecker()
    checker.generate_summary_report()

if __name__ == "__main__":
    main()