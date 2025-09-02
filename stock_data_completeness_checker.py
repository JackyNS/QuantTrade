#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
股票数据完整性检查器
确定每只A股在2000年1月1日-2025年8月31日期间的数据完整性
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import warnings
from collections import defaultdict
warnings.filterwarnings('ignore')

class StockDataCompletenessChecker:
    """股票数据完整性检查器"""
    
    def __init__(self):
        """初始化检查器"""
        self.base_path = Path("/Users/jackstudio/QuantTrade/data")
        self.target_start = pd.Timestamp('2000-01-01')
        self.target_end = pd.Timestamp('2025-08-31')
        
        self.stock_coverage = {}
        self.results_summary = {}
    
    def get_all_stocks_from_batches(self):
        """从批次文件中获取所有股票代码"""
        print("🔍 获取所有A股股票代码...")
        
        daily_path = self.base_path / "priority_download/market_data/daily"
        batch_files = list(daily_path.glob("*.csv"))
        
        all_stocks = set()
        
        # 扫描所有批次文件收集股票代码
        for i, file_path in enumerate(batch_files, 1):
            try:
                df = pd.read_csv(file_path)
                if 'secID' in df.columns:
                    stocks = df['secID'].unique()
                    all_stocks.update(stocks)
                
                if i % 100 == 0:
                    print(f"   已处理: {i}/{len(batch_files)} 文件，累计股票: {len(all_stocks)}")
                    
            except Exception as e:
                continue
        
        print(f"   ✅ 总共发现: {len(all_stocks)} 只A股")
        return sorted(list(all_stocks))
    
    def check_stock_time_coverage(self, stock_id):
        """检查单只股票的时间覆盖情况"""
        daily_path = self.base_path / "priority_download/market_data/daily"
        batch_files = list(daily_path.glob("*.csv"))
        
        stock_dates = []
        
        # 在所有批次文件中查找该股票的数据
        for file_path in batch_files:
            try:
                df = pd.read_csv(file_path)
                
                if 'secID' in df.columns:
                    stock_data = df[df['secID'] == stock_id]
                    
                    if len(stock_data) > 0 and 'tradeDate' in stock_data.columns:
                        dates = pd.to_datetime(stock_data['tradeDate'])
                        stock_dates.extend(dates.tolist())
                        
            except Exception as e:
                continue
        
        if not stock_dates:
            return {
                'has_data': False,
                'date_range': None,
                'coverage_status': '无数据'
            }
        
        # 分析时间范围
        stock_dates = pd.to_datetime(stock_dates)
        min_date = stock_dates.min()
        max_date = stock_dates.max()
        
        # 判断覆盖情况
        start_coverage = min_date <= self.target_start + pd.Timedelta(days=365)  # 允许1年误差
        end_coverage = max_date >= self.target_end - pd.Timedelta(days=30)      # 允许1月误差
        
        if start_coverage and end_coverage:
            status = '✅ 完整覆盖'
        elif start_coverage:
            status = '⚠️ 缺少近期数据'
        elif end_coverage:
            status = '⚠️ 缺少早期数据'
        else:
            status = '❌ 覆盖不足'
        
        return {
            'has_data': True,
            'date_range': f"{min_date.strftime('%Y-%m-%d')} - {max_date.strftime('%Y-%m-%d')}",
            'start_date': min_date,
            'end_date': max_date,
            'coverage_status': status,
            'start_coverage': start_coverage,
            'end_coverage': end_coverage,
            'data_points': len(stock_dates)
        }
    
    def batch_check_all_stocks(self, all_stocks, sample_size=None):
        """批量检查所有股票的数据完整性"""
        
        if sample_size:
            check_stocks = all_stocks[:sample_size]
            print(f"🔍 检查前 {sample_size} 只股票的数据完整性...")
        else:
            check_stocks = all_stocks
            print(f"🔍 检查全部 {len(all_stocks)} 只股票的数据完整性...")
        
        print(f"📅 目标时间范围: {self.target_start.date()} - {self.target_end.date()}")
        print("=" * 80)
        
        results = {
            'complete_coverage': [],      # 完整覆盖
            'missing_recent': [],         # 缺少近期数据
            'missing_early': [],          # 缺少早期数据
            'insufficient_coverage': [],  # 覆盖不足
            'no_data': []                # 无数据
        }
        
        for i, stock_id in enumerate(check_stocks, 1):
            coverage = self.check_stock_time_coverage(stock_id)
            
            # 分类统计
            if not coverage['has_data']:
                results['no_data'].append(stock_id)
                status_symbol = '❌'
            elif coverage['coverage_status'] == '✅ 完整覆盖':
                results['complete_coverage'].append({
                    'stock_id': stock_id,
                    'date_range': coverage['date_range'],
                    'data_points': coverage['data_points']
                })
                status_symbol = '✅'
            elif coverage['coverage_status'] == '⚠️ 缺少近期数据':
                results['missing_recent'].append({
                    'stock_id': stock_id,
                    'date_range': coverage['date_range'],
                    'end_date': coverage['end_date']
                })
                status_symbol = '⚠️'
            elif coverage['coverage_status'] == '⚠️ 缺少早期数据':
                results['missing_early'].append({
                    'stock_id': stock_id,
                    'date_range': coverage['date_range'],
                    'start_date': coverage['start_date']
                })
                status_symbol = '⚠️'
            else:
                results['insufficient_coverage'].append({
                    'stock_id': stock_id,
                    'date_range': coverage['date_range']
                })
                status_symbol = '❌'
            
            # 显示进度
            if i % 50 == 0 or i <= 20:
                print(f"[{i:4}/{len(check_stocks)}] {stock_id}: {status_symbol} {coverage['coverage_status']}")
            elif i % 100 == 0:
                complete = len(results['complete_coverage'])
                progress = (i / len(check_stocks)) * 100
                print(f"📊 进度: {i}/{len(check_stocks)} ({progress:.1f}%) | 完整: {complete}")
        
        self.results_summary = results
        return results
    
    def generate_completeness_report(self, sample_size=200):
        """生成数据完整性报告"""
        print("🔍 A股数据完整性检查")
        print("🎯 目标: 确定每只股票在2000-2025年8月的数据完整性")
        print("=" * 80)
        
        # 获取所有股票
        all_stocks = self.get_all_stocks_from_batches()
        
        if not all_stocks:
            print("❌ 未找到任何股票数据")
            return
        
        # 检查数据完整性
        results = self.batch_check_all_stocks(all_stocks, sample_size)
        
        # 生成统计报告
        total_checked = sum(len(v) if isinstance(v, list) else len([item for item in v]) 
                           for v in results.values())
        
        complete_count = len(results['complete_coverage'])
        missing_recent_count = len(results['missing_recent'])
        missing_early_count = len(results['missing_early'])
        insufficient_count = len(results['insufficient_coverage'])
        no_data_count = len(results['no_data'])
        
        print("\\n🎊 数据完整性检查结果:")
        print("=" * 80)
        print(f"📊 检查股票总数: {total_checked}")
        print(f"✅ 完整覆盖(2000-2025): {complete_count} ({complete_count/total_checked*100:.1f}%)")
        print(f"⚠️ 缺少近期数据: {missing_recent_count} ({missing_recent_count/total_checked*100:.1f}%)")
        print(f"⚠️ 缺少早期数据: {missing_early_count} ({missing_early_count/total_checked*100:.1f}%)")
        print(f"❌ 覆盖不足: {insufficient_count} ({insufficient_count/total_checked*100:.1f}%)")
        print(f"❌ 无数据: {no_data_count} ({no_data_count/total_checked*100:.1f}%)")
        
        # 显示完整覆盖的股票样本
        if results['complete_coverage']:
            print(f"\\n✅ 完整覆盖股票样本 (前10只):")
            for stock_info in results['complete_coverage'][:10]:
                print(f"   {stock_info['stock_id']}: {stock_info['date_range']} ({stock_info['data_points']} 个交易日)")
        
        # 显示有问题的股票样本
        if results['missing_recent']:
            print(f"\\n⚠️ 缺少近期数据样本 (前5只):")
            for stock_info in results['missing_recent'][:5]:
                print(f"   {stock_info['stock_id']}: 数据到 {stock_info['end_date'].date()}")
        
        if results['missing_early']:
            print(f"\\n⚠️ 缺少早期数据样本 (前5只):")
            for stock_info in results['missing_early'][:5]:
                print(f"   {stock_info['stock_id']}: 数据从 {stock_info['start_date'].date()}")
        
        # 生成结论
        coverage_rate = complete_count / total_checked * 100
        
        print(f"\\n💡 关键结论:")
        if coverage_rate >= 80:
            print(f"   🎯 数据质量优秀: {coverage_rate:.1f}% 的股票有完整的2000-2025数据")
            print(f"   ✅ 可以基于现有数据进行全面分析")
        elif coverage_rate >= 60:
            print(f"   🟡 数据质量良好: {coverage_rate:.1f}% 的股票有完整数据")
            print(f"   💡 建议重点使用完整数据股票进行分析")
        else:
            print(f"   🔴 数据完整性不足: 仅 {coverage_rate:.1f}% 的股票有完整数据")
            print(f"   🚨 需要补充数据或调整分析策略")

def main():
    """主函数"""
    checker = StockDataCompletenessChecker()
    checker.generate_completeness_report(sample_size=200)  # 先检查200只样本

if __name__ == "__main__":
    main()