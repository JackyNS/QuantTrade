#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
全股票覆盖分析器
检查所有A股从2000年1月1日到2025年8月31日的数据完整性
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
import json
import warnings
from collections import defaultdict
import os
warnings.filterwarnings('ignore')

class CompleteStockCoverageAnalyzer:
    """全股票覆盖分析器"""
    
    def __init__(self):
        """初始化分析器"""
        self.base_path = Path("/Users/jackstudio/QuantTrade/data")
        self.target_start = pd.Timestamp('2000-01-01')
        self.target_end = pd.Timestamp('2025-08-31')
        
        self.stats = {
            'analysis_start': datetime.now(),
            'all_stocks_found': set(),
            'stock_coverage': {},
            'invalid_stocks': [],
            'incomplete_stocks': [],
            'complete_stocks': []
        }
    
    def get_all_stocks_from_batches(self):
        """从所有批次文件中获取完整的股票列表"""
        print("🔍 扫描所有批次文件获取完整股票列表...")
        
        daily_path = self.base_path / "priority_download/market_data/daily"
        batch_files = list(daily_path.glob("*.csv"))
        
        print(f"   📄 找到批次文件: {len(batch_files)} 个")
        
        all_stocks = set()
        file_count = 0
        
        for file_path in batch_files:
            try:
                df = pd.read_csv(file_path)
                stocks_in_file = set(df['secID'].unique())
                all_stocks.update(stocks_in_file)
                file_count += 1
                
                if file_count % 100 == 0:
                    print(f"   📊 已处理: {file_count}/{len(batch_files)} 文件, 累计股票: {len(all_stocks)}")
                    
            except Exception as e:
                print(f"   ❌ 文件读取失败: {file_path.name}")
                continue
        
        print(f"   ✅ 扫描完成: {len(all_stocks)} 只股票")
        self.stats['all_stocks_found'] = all_stocks
        return all_stocks
    
    def analyze_stock_coverage(self, stock_id):
        """分析单只股票的数据覆盖情况"""
        daily_path = self.base_path / "priority_download/market_data/daily"
        batch_files = list(daily_path.glob("*.csv"))
        
        stock_data = []
        
        # 遍历所有批次文件寻找该股票数据
        for file_path in batch_files:
            try:
                df = pd.read_csv(file_path)
                stock_df = df[df['secID'] == stock_id]
                
                if len(stock_df) > 0:
                    stock_data.append(stock_df)
                    
            except Exception as e:
                continue
        
        if not stock_data:
            return {
                'has_data': False,
                'date_range': None,
                'completeness': 'no_data'
            }
        
        # 合并所有数据
        combined_df = pd.concat(stock_data, ignore_index=True)
        combined_df['tradeDate'] = pd.to_datetime(combined_df['tradeDate'])
        combined_df = combined_df.drop_duplicates(subset=['tradeDate']).sort_values('tradeDate')
        
        # 分析覆盖范围
        min_date = combined_df['tradeDate'].min()
        max_date = combined_df['tradeDate'].max()
        total_records = len(combined_df)
        
        # 判断完整性
        start_coverage = min_date <= self.target_start + timedelta(days=365)  # 允许1年内开始
        end_coverage = max_date >= self.target_end - timedelta(days=30)       # 允许1月内结束
        
        if start_coverage and end_coverage:
            completeness = 'complete'
        elif start_coverage:
            completeness = 'partial_end_missing'
        elif end_coverage:
            completeness = 'partial_start_missing'
        else:
            completeness = 'insufficient'
        
        return {
            'has_data': True,
            'date_range': (min_date, max_date),
            'total_records': total_records,
            'completeness': completeness,
            'start_coverage': start_coverage,
            'end_coverage': end_coverage
        }
    
    def batch_analyze_all_stocks(self, all_stocks):
        """批量分析所有股票的覆盖情况"""
        print(f"\\n📊 开始分析 {len(all_stocks)} 只股票的数据完整性...")
        print("🎯 目标范围: 2000年1月1日 - 2025年8月31日")
        print("=" * 80)
        
        complete_stocks = []
        incomplete_stocks = []
        invalid_stocks = []
        
        stock_list = list(all_stocks)
        
        # 分析每只股票（显示进度）
        for i, stock_id in enumerate(stock_list, 1):
            coverage = self.analyze_stock_coverage(stock_id)
            
            if not coverage['has_data']:
                invalid_stocks.append(stock_id)
                status = "❌ 无数据"
            elif coverage['completeness'] == 'complete':
                complete_stocks.append({
                    'stock_id': stock_id,
                    'date_range': coverage['date_range'],
                    'records': coverage['total_records']
                })
                status = "✅ 完整"
            else:
                incomplete_stocks.append({
                    'stock_id': stock_id,
                    'date_range': coverage['date_range'],
                    'completeness': coverage['completeness'],
                    'records': coverage['total_records']
                })
                status = f"⚠️ {coverage['completeness']}"
            
            # 显示进度
            if i % 50 == 0 or i <= 20:
                print(f"[{i:4}/{len(stock_list)}] {stock_id}: {status}")
            elif i % 100 == 0:
                progress = (i / len(stock_list)) * 100
                print(f"📈 进度: {i}/{len(stock_list)} ({progress:.1f}%) | 完整:{len(complete_stocks)} 不完整:{len(incomplete_stocks)} 无效:{len(invalid_stocks)}")
        
        # 保存结果
        self.stats['complete_stocks'] = complete_stocks
        self.stats['incomplete_stocks'] = incomplete_stocks
        self.stats['invalid_stocks'] = invalid_stocks
        
        return complete_stocks, incomplete_stocks, invalid_stocks
    
    def generate_coverage_report(self):
        """生成覆盖情况报告"""
        print("🔍 开始全A股数据覆盖分析...")
        print("=" * 80)
        
        # 获取所有股票
        all_stocks = self.get_all_stocks_from_batches()
        
        if not all_stocks:
            print("❌ 未找到任何股票数据")
            return
        
        # 分析所有股票覆盖情况
        complete_stocks, incomplete_stocks, invalid_stocks = self.batch_analyze_all_stocks(all_stocks)
        
        # 生成统计报告
        total_stocks = len(all_stocks)
        complete_count = len(complete_stocks)
        incomplete_count = len(incomplete_stocks)
        invalid_count = len(invalid_stocks)
        
        coverage_rate = (complete_count / total_stocks) * 100 if total_stocks > 0 else 0
        
        report = {
            'analysis_info': {
                'analysis_time': self.stats['analysis_start'].isoformat(),
                'target_period': '2000年1月1日 - 2025年8月31日',
                'analysis_scope': '全A股数据完整性分析'
            },
            'coverage_summary': {
                'total_stocks_found': total_stocks,
                'complete_stocks': complete_count,
                'incomplete_stocks': incomplete_count,
                'invalid_stocks': invalid_count,
                'coverage_rate': f"{coverage_rate:.1f}%"
            },
            'detailed_results': {
                'complete_stocks_list': [stock['stock_id'] for stock in complete_stocks],
                'incomplete_stocks_detail': [
                    {
                        'stock_id': stock['stock_id'],
                        'issue': stock['completeness'],
                        'date_range': f"{stock['date_range'][0].date()} - {stock['date_range'][1].date()}" if stock['date_range'] else 'N/A',
                        'records': stock.get('records', 0)
                    } for stock in incomplete_stocks
                ],
                'invalid_stocks_list': invalid_stocks
            },
            'data_quality_assessment': {
                'overall_quality': 'excellent' if coverage_rate >= 90 else 'good' if coverage_rate >= 70 else 'needs_improvement',
                'recommendation': self._get_recommendation(coverage_rate, complete_count, total_stocks)
            }
        }
        
        # 保存详细报告
        report_file = self.base_path / 'complete_stock_coverage_report.json'
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2, default=str)
        
        # 输出摘要
        print("\\n🎊 全A股数据覆盖分析完成!")
        print("=" * 80)
        print("📊 覆盖统计:")
        print(f"   📈 股票总数: {total_stocks}")
        print(f"   ✅ 完整覆盖: {complete_count} ({coverage_rate:.1f}%)")
        print(f"   ⚠️ 不完整: {incomplete_count}")
        print(f"   ❌ 无效数据: {invalid_count}")
        
        if complete_count > 0:
            print(f"\\n✅ 完整数据样本 (前10只):")
            for stock in complete_stocks[:10]:
                date_range = f"{stock['date_range'][0].date()} - {stock['date_range'][1].date()}"
                print(f"      {stock['stock_id']}: {stock['records']} 条记录 ({date_range})")
        
        if incomplete_stocks:
            print(f"\\n⚠️ 不完整数据样本 (前10只):")
            for stock in incomplete_stocks[:10]:
                date_range = f"{stock['date_range'][0].date()} - {stock['date_range'][1].date()}" if stock['date_range'] else 'N/A'
                print(f"      {stock['stock_id']}: {stock['completeness']} ({date_range})")
        
        print(f"\\n📄 详细报告: {report_file}")
        print(f"💡 评估: {report['data_quality_assessment']['overall_quality']}")
        print(f"🔧 建议: {report['data_quality_assessment']['recommendation']}")
    
    def _get_recommendation(self, coverage_rate, complete_count, total_stocks):
        """根据覆盖率生成建议"""
        if coverage_rate >= 90:
            return f"数据质量优秀，{complete_count}只股票数据完整，可直接用于分析"
        elif coverage_rate >= 70:
            return f"数据质量良好，建议重点使用{complete_count}只完整股票进行分析"
        else:
            return f"数据完整性需要改进，建议补充缺失数据或使用现有{complete_count}只完整股票"

def main():
    """主函数"""
    analyzer = CompleteStockCoverageAnalyzer()
    analyzer.generate_coverage_report()

if __name__ == "__main__":
    main()