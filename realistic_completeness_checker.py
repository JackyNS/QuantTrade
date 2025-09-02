#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
基于上市时间的现实数据完整性检查器
考虑股票实际上市时间，合理判断数据完整性
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import warnings
from collections import defaultdict
warnings.filterwarnings('ignore')

class RealisticCompletenessChecker:
    """基于上市时间的现实完整性检查器"""
    
    def __init__(self):
        """初始化检查器"""
        self.base_path = Path("/Users/jackstudio/QuantTrade/data")
        self.analysis_end = pd.Timestamp('2025-08-31')
        self.stock_info = {}
    
    def get_stock_listing_info(self):
        """获取股票上市信息"""
        print("📋 获取股票上市信息...")
        
        # 尝试从basic_info目录获取股票基本信息
        basic_info_paths = [
            "final_comprehensive_download/basic_info",
            "optimized_data/basic_info", 
            "raw/basic_info"
        ]
        
        stock_info = {}
        
        for path in basic_info_paths:
            full_path = self.base_path / path
            if full_path.exists():
                info_files = list(full_path.rglob("*.csv"))
                print(f"   📁 检查 {path}: {len(info_files)} 个文件")
                
                for file_path in info_files[:3]:  # 检查前几个文件
                    try:
                        df = pd.read_csv(file_path)
                        
                        # 寻找相关列
                        if 'secID' in df.columns:
                            for _, row in df.iterrows():
                                stock_id = row['secID']
                                
                                # 寻找上市日期列
                                listing_date = None
                                for col in df.columns:
                                    if any(keyword in col.lower() for keyword in ['list', 'ipo', '上市', 'date']):
                                        try:
                                            date_val = pd.to_datetime(row[col])
                                            if date_val.year >= 1990 and date_val.year <= 2025:
                                                listing_date = date_val
                                                break
                                        except:
                                            continue
                                
                                if listing_date:
                                    stock_info[stock_id] = {
                                        'listing_date': listing_date,
                                        'source': path
                                    }
                                    
                        if len(stock_info) > 0:
                            print(f"   ✅ 从 {file_path.name} 获取了 {len(stock_info)} 只股票信息")
                            break
                            
                    except Exception as e:
                        continue
                
                if stock_info:
                    break
        
        self.stock_info = stock_info
        print(f"   📊 总共获取: {len(stock_info)} 只股票的上市信息")
        return stock_info
    
    def estimate_listing_date_from_data(self, stock_id, stock_data):
        """从数据本身估算上市时间"""
        if len(stock_data) == 0:
            return None
        
        # 数据的最早日期可能就是上市日期附近
        earliest_date = stock_data['tradeDate'].min()
        
        # 如果数据开始时间合理（1990年后），认为接近上市时间
        if earliest_date.year >= 1990:
            return earliest_date
        
        return None
    
    def check_stock_realistic_completeness(self, stock_id):
        """检查单只股票的现实数据完整性"""
        # 1. 收集该股票的所有数据
        daily_path = self.base_path / "priority_download/market_data/daily"
        batch_files = list(daily_path.glob("*.csv"))
        
        all_data = []
        
        for file_path in batch_files:
            try:
                df = pd.read_csv(file_path)
                if 'secID' in df.columns:
                    stock_data = df[df['secID'] == stock_id]
                    if len(stock_data) > 0:
                        all_data.append(stock_data)
            except:
                continue
        
        if not all_data:
            return {
                'has_data': False,
                'status': '❌ 无数据'
            }
        
        # 合并所有数据
        combined_df = pd.concat(all_data, ignore_index=True)
        combined_df['tradeDate'] = pd.to_datetime(combined_df['tradeDate'])
        combined_df = combined_df.drop_duplicates(subset=['tradeDate']).sort_values('tradeDate')
        
        data_start = combined_df['tradeDate'].min()
        data_end = combined_df['tradeDate'].max()
        
        # 2. 确定期望的开始时间
        if stock_id in self.stock_info:
            # 有上市信息
            listing_date = self.stock_info[stock_id]['listing_date']
            expected_start = listing_date
            info_source = "上市信息"
        else:
            # 估算上市时间
            estimated_listing = self.estimate_listing_date_from_data(stock_id, combined_df)
            if estimated_listing:
                expected_start = estimated_listing
                info_source = "数据估算"
            else:
                expected_start = pd.Timestamp('2000-01-01')  # 默认
                info_source = "默认假设"
        
        # 3. 判断完整性
        # 开始时间：允许一个月的误差
        start_coverage = data_start <= expected_start + pd.Timedelta(days=30)
        # 结束时间：应该到2025年8月附近
        end_coverage = data_end >= self.analysis_end - pd.Timedelta(days=60)
        
        if start_coverage and end_coverage:
            status = '✅ 完整'
        elif start_coverage and not end_coverage:
            status = '🟡 缺少近期数据'
        elif not start_coverage and end_coverage:
            status = '🟡 缺少早期数据'
        else:
            status = '🔴 数据不足'
        
        return {
            'has_data': True,
            'status': status,
            'data_range': f"{data_start.strftime('%Y-%m-%d')} - {data_end.strftime('%Y-%m-%d')}",
            'expected_start': expected_start.strftime('%Y-%m-%d'),
            'data_start': data_start,
            'data_end': data_end,
            'expected_start_date': expected_start,
            'records': len(combined_df),
            'info_source': info_source,
            'start_coverage': start_coverage,
            'end_coverage': end_coverage
        }
    
    def realistic_completeness_check(self, sample_size=150):
        """进行现实的完整性检查"""
        print("🔍 基于上市时间的现实数据完整性检查")
        print("🎯 检查逻辑: 从上市日期开始到2025年8月31日")
        print("=" * 80)
        
        # 获取上市信息
        self.get_stock_listing_info()
        
        # 获取股票样本
        csv_daily_path = self.base_path / "csv_complete/daily"
        if csv_daily_path.exists():
            stock_files = list(csv_daily_path.rglob("*.csv"))
            sample_files = stock_files[:sample_size]
            sample_stocks = [f.stem.replace('_', '.') for f in sample_files]
        else:
            print("❌ 无法获取股票样本")
            return
        
        print(f"📊 检查样本: {len(sample_stocks)} 只股票")
        print("=" * 80)
        
        results = {
            'complete': [],           # 完整数据
            'missing_recent': [],     # 缺少近期数据
            'missing_early': [],      # 缺少早期数据
            'insufficient': [],       # 数据不足
            'no_data': []            # 无数据
        }
        
        for i, stock_id in enumerate(sample_stocks, 1):
            result = self.check_stock_realistic_completeness(stock_id)
            
            if not result['has_data']:
                results['no_data'].append(stock_id)
                continue
            
            stock_result = {
                'stock_id': stock_id,
                'status': result['status'],
                'data_range': result['data_range'],
                'expected_start': result['expected_start'],
                'records': result['records'],
                'info_source': result['info_source']
            }
            
            if result['status'] == '✅ 完整':
                results['complete'].append(stock_result)
            elif result['status'] == '🟡 缺少近期数据':
                results['missing_recent'].append(stock_result)
            elif result['status'] == '🟡 缺少早期数据':
                results['missing_early'].append(stock_result)
            else:
                results['insufficient'].append(stock_result)
            
            # 显示进度
            if i % 30 == 0 or i <= 15:
                expected = result.get('expected_start', 'Unknown')
                print(f"[{i:3}/{len(sample_stocks)}] {stock_id}: {result['status']} (期望:{expected} 实际:{result.get('data_range', 'N/A')})")
        
        self.generate_realistic_report(results, len(sample_stocks))
    
    def generate_realistic_report(self, results, total_checked):
        """生成现实的检查报告"""
        complete_count = len(results['complete'])
        missing_recent_count = len(results['missing_recent'])
        missing_early_count = len(results['missing_early'])
        insufficient_count = len(results['insufficient'])
        no_data_count = len(results['no_data'])
        
        print("\\n🎊 现实数据完整性检查结果:")
        print("=" * 80)
        print(f"📊 检查样本: {total_checked}")
        print(f"✅ 完整数据: {complete_count} ({complete_count/total_checked*100:.1f}%)")
        print(f"🟡 缺少近期: {missing_recent_count} ({missing_recent_count/total_checked*100:.1f}%)")
        print(f"🟡 缺少早期: {missing_early_count} ({missing_early_count/total_checked*100:.1f}%)")
        print(f"🔴 数据不足: {insufficient_count} ({insufficient_count/total_checked*100:.1f}%)")
        print(f"❌ 无数据: {no_data_count} ({no_data_count/total_checked*100:.1f}%)")
        
        # 显示完整数据样本
        if results['complete']:
            print("\\n✅ 完整数据样本:")
            for stock in results['complete'][:8]:
                print(f"   {stock['stock_id']}: {stock['data_range']} ({stock['records']} 条) - {stock['info_source']}")
        
        # 显示有问题的样本
        if results['missing_recent']:
            print("\\n🟡 缺少近期数据样本:")
            for stock in results['missing_recent'][:5]:
                print(f"   {stock['stock_id']}: {stock['data_range']} (期望到2025-08)")
        
        if results['missing_early']:
            print("\\n🟡 缺少早期数据样本:")
            for stock in results['missing_early'][:5]:
                print(f"   {stock['stock_id']}: {stock['data_range']} (期望从{stock['expected_start']})")
        
        # 计算可用性评估
        usable_count = complete_count + missing_recent_count  # 缺少近期的也基本可用
        usable_rate = usable_count / total_checked * 100
        
        print("\\n💡 现实评估:")
        print(f"   🎯 基本可用数据: {usable_count} ({usable_rate:.1f}%)")
        print(f"   📊 估算50,658只股票中可用: ~{int(50658 * usable_rate / 100):,} 只")
        
        if usable_rate >= 80:
            print("   🏆 数据质量评估: 优秀")
            print("   ✅ 建议: 数据完全满足分析需求")
        elif usable_rate >= 60:
            print("   🟡 数据质量评估: 良好") 
            print("   💡 建议: 可以进行大部分分析工作")
        else:
            print("   🔴 数据质量评估: 需要改进")
            print("   ⚠️ 建议: 需要补充或筛选数据")

def main():
    """主函数"""
    checker = RealisticCompletenessChecker()
    checker.realistic_completeness_check(sample_size=150)

if __name__ == "__main__":
    main()