#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
详细筛查统计工具
展示具体的数据筛查统计情况，不做结论
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import warnings
from collections import defaultdict, Counter
warnings.filterwarnings('ignore')

class DetailedScreeningStatistics:
    """详细筛查统计"""
    
    def __init__(self):
        """初始化"""
        self.base_path = Path("/Users/jackstudio/QuantTrade/data")
        self.screening_data = {}
    
    def screen_individual_stock_files(self, sample_size=500):
        """筛查个股文件的具体情况"""
        print("🔍 个股文件详细筛查")
        print(f"📊 筛查样本数: {sample_size}")
        print("=" * 120)
        
        csv_daily_path = self.base_path / "csv_complete/daily"
        if not csv_daily_path.exists():
            print("❌ 目录不存在")
            return
        
        stock_files = list(csv_daily_path.rglob("*.csv"))
        print(f"📈 总文件数: {len(stock_files):,}")
        
        # 选择筛查样本
        step = max(1, len(stock_files) // sample_size)
        sample_files = stock_files[::step][:sample_size]
        
        screening_results = []
        
        print("\\n📋 逐个筛查结果:")
        print("-" * 120)
        print(f"{'序号':<4} {'股票代码':<12} {'开始日期':<12} {'结束日期':<12} {'记录数':<8} {'年数':<6} {'状态'}")
        print("-" * 120)
        
        for i, file_path in enumerate(sample_files, 1):
            try:
                stock_id = file_path.stem.replace('_', '.')
                df = pd.read_csv(file_path)
                
                if len(df) == 0 or 'tradeDate' not in df.columns:
                    result = {
                        'index': i,
                        'stock_id': stock_id,
                        'start_date': 'N/A',
                        'end_date': 'N/A', 
                        'records': 0,
                        'years': 0,
                        'status': '无有效数据'
                    }
                else:
                    df['tradeDate'] = pd.to_datetime(df['tradeDate'])
                    start_date = df['tradeDate'].min()
                    end_date = df['tradeDate'].max()
                    records = len(df)
                    years = round((end_date - start_date).days / 365.25, 1)
                    
                    # 简单状态判断
                    if start_date.year <= 2001 and end_date.year >= 2024:
                        status = '长期数据'
                    elif start_date.year <= 2010 and end_date.year >= 2020:
                        status = '中期数据'
                    elif years >= 3:
                        status = '短期数据'
                    else:
                        status = '数据较少'
                    
                    result = {
                        'index': i,
                        'stock_id': stock_id,
                        'start_date': start_date.strftime('%Y-%m-%d'),
                        'end_date': end_date.strftime('%Y-%m-%d'),
                        'records': records,
                        'years': years,
                        'status': status
                    }
                
                screening_results.append(result)
                
                # 显示筛查结果
                print(f"{result['index']:<4} {result['stock_id']:<12} {result['start_date']:<12} {result['end_date']:<12} {result['records']:<8} {result['years']:<6} {result['status']}")
                
            except Exception as e:
                result = {
                    'index': i,
                    'stock_id': file_path.stem,
                    'start_date': 'ERROR',
                    'end_date': 'ERROR',
                    'records': 0,
                    'years': 0,
                    'status': '读取失败'
                }
                screening_results.append(result)
                print(f"{result['index']:<4} {result['stock_id']:<12} {'ERROR':<12} {'ERROR':<12} {0:<8} {0:<6} 读取失败")
        
        self.screening_data['individual_files'] = screening_results
        return screening_results
    
    def analyze_screening_distribution(self, screening_results):
        """分析筛查结果分布"""
        print("\\n📊 筛查结果分布统计")
        print("=" * 80)
        
        # 按状态分类统计
        status_counts = Counter([r['status'] for r in screening_results])
        print("\\n📋 按状态分类:")
        for status, count in status_counts.items():
            percentage = count / len(screening_results) * 100
            print(f"   {status}: {count} 只 ({percentage:.1f}%)")
        
        # 按开始年份分类统计  
        start_years = []
        end_years = []
        valid_results = [r for r in screening_results if r['start_date'] != 'N/A' and r['start_date'] != 'ERROR']
        
        for result in valid_results:
            try:
                start_years.append(int(result['start_date'][:4]))
                end_years.append(int(result['end_date'][:4]))
            except:
                continue
        
        if start_years:
            start_year_counts = Counter(start_years)
            end_year_counts = Counter(end_years)
            
            print("\\n📅 按开始年份分布:")
            for year in sorted(start_year_counts.keys()):
                count = start_year_counts[year]
                percentage = count / len(valid_results) * 100
                print(f"   {year}: {count} 只 ({percentage:.1f}%)")
            
            print("\\n📅 按结束年份分布:")
            for year in sorted(end_year_counts.keys()):
                count = end_year_counts[year]
                percentage = count / len(valid_results) * 100
                print(f"   {year}: {count} 只 ({percentage:.1f}%)")
        
        # 按数据年数分类统计
        years_ranges = {
            '0-1年': 0,
            '1-3年': 0, 
            '3-5年': 0,
            '5-10年': 0,
            '10-15年': 0,
            '15-20年': 0,
            '20年以上': 0
        }
        
        for result in valid_results:
            years = result['years']
            if years <= 1:
                years_ranges['0-1年'] += 1
            elif years <= 3:
                years_ranges['1-3年'] += 1
            elif years <= 5:
                years_ranges['3-5年'] += 1
            elif years <= 10:
                years_ranges['5-10年'] += 1
            elif years <= 15:
                years_ranges['10-15年'] += 1
            elif years <= 20:
                years_ranges['15-20年'] += 1
            else:
                years_ranges['20年以上'] += 1
        
        print("\\n📊 按数据年数分布:")
        for range_name, count in years_ranges.items():
            percentage = count / len(valid_results) * 100 if valid_results else 0
            print(f"   {range_name}: {count} 只 ({percentage:.1f}%)")
        
        # 记录数分布统计
        record_ranges = {
            '0-100': 0,
            '100-500': 0,
            '500-1000': 0,
            '1000-3000': 0,
            '3000-5000': 0,
            '5000以上': 0
        }
        
        for result in valid_results:
            records = result['records']
            if records <= 100:
                record_ranges['0-100'] += 1
            elif records <= 500:
                record_ranges['100-500'] += 1
            elif records <= 1000:
                record_ranges['500-1000'] += 1
            elif records <= 3000:
                record_ranges['1000-3000'] += 1
            elif records <= 5000:
                record_ranges['3000-5000'] += 1
            else:
                record_ranges['5000以上'] += 1
        
        print("\\n📈 按记录数分布:")
        for range_name, count in record_ranges.items():
            percentage = count / len(valid_results) * 100 if valid_results else 0
            print(f"   {range_name}条: {count} 只 ({percentage:.1f}%)")
    
    def screen_batch_files_detail(self):
        """筛查批次文件的详细情况"""
        print("\\n\\n📄 批次文件详细筛查")
        print("=" * 100)
        
        daily_path = self.base_path / "priority_download/market_data/daily"
        if not daily_path.exists():
            print("❌ 批次文件目录不存在")
            return
        
        batch_files = list(daily_path.glob("*.csv"))
        print(f"📊 批次文件总数: {len(batch_files)}")
        
        # 按年份详细统计
        year_details = defaultdict(list)
        
        print("\\n📋 批次文件详细信息:")
        print("-" * 100)
        print(f"{'文件名':<20} {'年份':<6} {'股票数':<8} {'记录数':<10} {'开始日期':<12} {'结束日期':<12}")
        print("-" * 100)
        
        # 抽样检查每年的代表文件
        year_files = defaultdict(list)
        for file_path in batch_files:
            try:
                year = file_path.stem.split('_')[0]
                year_files[year].append(file_path)
            except:
                continue
        
        for year in sorted(year_files.keys()):
            files = year_files[year]
            # 检查该年份的前3个文件
            for file_path in files[:3]:
                try:
                    df = pd.read_csv(file_path)
                    
                    if 'secID' in df.columns and 'tradeDate' in df.columns:
                        stock_count = len(df['secID'].unique())
                        record_count = len(df)
                        
                        df['tradeDate'] = pd.to_datetime(df['tradeDate'])
                        start_date = df['tradeDate'].min().strftime('%Y-%m-%d')
                        end_date = df['tradeDate'].max().strftime('%Y-%m-%d')
                        
                        print(f"{file_path.name:<20} {year:<6} {stock_count:<8} {record_count:<10} {start_date:<12} {end_date:<12}")
                        
                        year_details[year].append({
                            'file': file_path.name,
                            'stocks': stock_count,
                            'records': record_count,
                            'start': start_date,
                            'end': end_date
                        })
                    else:
                        print(f"{file_path.name:<20} {year:<6} {'ERROR':<8} {'ERROR':<10} {'ERROR':<12} {'ERROR':<12}")
                        
                except Exception as e:
                    print(f"{file_path.name:<20} {year:<6} {'ERROR':<8} {'ERROR':<10} {'ERROR':<12} {'ERROR':<12}")
        
        self.screening_data['batch_files'] = dict(year_details)
    
    def generate_screening_report(self, sample_size=500):
        """生成筛查报告"""
        print("🔍 A股数据详细筛查统计")
        print("📅 筛查时间:", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        print("=" * 120)
        
        # 筛查个股文件
        screening_results = self.screen_individual_stock_files(sample_size)
        
        # 分析筛查分布
        self.analyze_screening_distribution(screening_results)
        
        # 筛查批次文件
        self.screen_batch_files_detail()
        
        # 输出原始数据摘要
        print("\\n\\n📋 筛查数据摘要")
        print("=" * 60)
        print(f"个股文件筛查样本: {len(screening_results)}")
        print(f"批次文件年份覆盖: {len(self.screening_data.get('batch_files', {}))}")
        print(f"总数据文件数: 50,658 (个股) + 713 (批次)")

def main():
    """主函数"""
    screener = DetailedScreeningStatistics()
    screener.generate_screening_report(sample_size=500)

if __name__ == "__main__":
    main()