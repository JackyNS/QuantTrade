#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
基于API分类的数据分析器
按优矿API类型分类，表格形式显示数据明细、时间范围和缺失情况
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import json
import warnings
from collections import defaultdict
import os
warnings.filterwarnings('ignore')

class APIBasedDataClassifier:
    """基于API分类的数据分析器"""
    
    def __init__(self):
        """初始化分析器"""
        self.base_path = Path("/Users/jackstudio/QuantTrade/data")
        self.target_start = "2000-01-01"
        self.target_end = "2025-08-31"
        
        # 优矿API映射表
        self.api_mapping = {
            # 股票行情数据
            'MktEqudGet': {'name': '日线行情', 'category': '股票行情', 'expected_path': 'priority_download/market_data/daily'},
            'MktEquwGet': {'name': '周线行情', 'category': '股票行情', 'expected_path': 'priority_download/market_data/weekly'},  
            'MktEqumGet': {'name': '月线行情', 'category': '股票行情', 'expected_path': 'priority_download/market_data/monthly'},
            'MktEqudAdjGet': {'name': '日线行情(前复权)', 'category': '股票行情', 'expected_path': 'priority_download/market_data/adj_daily'},
            'MktEquwAdjGet': {'name': '周线行情(前复权)', 'category': '股票行情', 'expected_path': 'priority_download/market_data/adj_weekly'},
            'MktEqumAdjGet': {'name': '月线行情(前复权)', 'category': '股票行情', 'expected_path': 'priority_download/market_data/adj_monthly'},
            
            # 财务数据
            'FdmtIncomestatementsGet': {'name': '利润表', 'category': '财务数据', 'expected_path': 'final_comprehensive_download/financial'},
            'FdmtBalancesheetsGet': {'name': '资产负债表', 'category': '财务数据', 'expected_path': 'final_comprehensive_download/financial'},
            'FdmtCashflowstatementsGet': {'name': '现金流量表', 'category': '财务数据', 'expected_path': 'final_comprehensive_download/financial'},
            'FdmtEfinancialindicatorGet': {'name': '财务指标', 'category': '财务数据', 'expected_path': 'final_comprehensive_download/financial'},
            
            # 基础信息
            'EquGet': {'name': '股票基本信息', 'category': '基础信息', 'expected_path': 'final_comprehensive_download/basic_info'},
            'SecIDGet': {'name': '证券代码表', 'category': '基础信息', 'expected_path': 'final_comprehensive_download/basic_info'},
            
            # 公司治理
            'EquDivGet': {'name': '分红配股', 'category': '公司治理', 'expected_path': 'final_comprehensive_download/governance'},
            'EquRetGet': {'name': '回报率', 'category': '公司治理', 'expected_path': 'final_comprehensive_download/governance'},
            'EquIndustryGet': {'name': '行业分类', 'category': '公司治理', 'expected_path': 'final_comprehensive_download/governance'},
            
            # 特殊交易
            'MktLimitGet': {'name': '涨跌停统计', 'category': '特殊交易', 'expected_path': 'final_comprehensive_download/special_trading'},
            'MktEqudStatsGet': {'name': '股票统计', 'category': '特殊交易', 'expected_path': 'final_comprehensive_download/special_trading'},
            'ResconSeccodesGet': {'name': '限售股份', 'category': '特殊交易', 'expected_path': 'final_comprehensive_download/special_trading'},
        }
        
        self.data_inventory = {}
    
    def scan_directory_for_api_data(self, dir_path, api_name):
        """扫描目录寻找特定API的数据"""
        full_path = self.base_path / dir_path
        
        if not full_path.exists():
            return {
                'exists': False,
                'file_count': 0,
                'time_range': None,
                'sample_files': [],
                'status': '目录不存在'
            }
        
        # 查找可能的数据文件
        patterns_to_check = [
            f"*{api_name.lower()}*",
            f"*{api_name}*", 
            "*.csv",
            "*.parquet"
        ]
        
        found_files = []
        for pattern in patterns_to_check:
            found_files.extend(list(full_path.rglob(pattern)))
        
        if not found_files:
            # 检查是否有其他CSV文件
            csv_files = list(full_path.rglob("*.csv"))
            return {
                'exists': len(csv_files) > 0,
                'file_count': len(csv_files),
                'time_range': self._analyze_time_range(csv_files[:3]) if csv_files else None,
                'sample_files': [f.name for f in csv_files[:3]],
                'status': '可能包含相关数据' if csv_files else '无相关文件'
            }
        
        # 分析找到的文件
        time_range = self._analyze_time_range(found_files[:5])
        
        return {
            'exists': True,
            'file_count': len(found_files),
            'time_range': time_range,
            'sample_files': [f.name for f in found_files[:3]],
            'status': '数据完整' if time_range and self._check_completeness(time_range) else '数据部分缺失'
        }
    
    def _analyze_time_range(self, file_list):
        """分析文件的时间范围"""
        if not file_list:
            return None
            
        all_dates = []
        
        for file_path in file_list:
            try:
                df = pd.read_csv(file_path)
                
                # 寻找可能的日期列
                date_columns = []
                for col in df.columns:
                    if any(keyword in col.lower() for keyword in ['date', 'time', '日期', '时间']):
                        date_columns.append(col)
                
                # 分析日期范围
                for date_col in date_columns:
                    try:
                        dates = pd.to_datetime(df[date_col])
                        all_dates.extend([dates.min(), dates.max()])
                        break  # 找到一个有效的日期列就够了
                    except:
                        continue
                        
            except Exception as e:
                continue
        
        if all_dates:
            return {
                'start': min(all_dates).strftime('%Y-%m-%d'),
                'end': max(all_dates).strftime('%Y-%m-%d'),
                'span_years': (max(all_dates) - min(all_dates)).days // 365
            }
        
        return None
    
    def _check_completeness(self, time_range):
        """检查时间完整性"""
        if not time_range:
            return False
        
        start_date = pd.to_datetime(time_range['start'])
        end_date = pd.to_datetime(time_range['end'])
        target_start = pd.to_datetime(self.target_start)
        target_end = pd.to_datetime(self.target_end)
        
        # 允许一些容差
        start_ok = start_date <= target_start + pd.Timedelta(days=365)
        end_ok = end_date >= target_end - pd.Timedelta(days=90)
        
        return start_ok and end_ok
    
    def classify_all_data(self):
        """按API分类所有数据"""
        print("🔍 按API类型分类数据...")
        print("🎯 目标时间范围: 2000-01-01 至 2025-08-31")
        print("=" * 100)
        
        results = []
        
        for api_name, api_info in self.api_mapping.items():
            print(f"📊 检查 {api_info['name']} ({api_name})...")
            
            # 扫描预期路径
            data_info = self.scan_directory_for_api_data(api_info['expected_path'], api_name)
            
            # 如果预期路径没找到，尝试其他可能路径
            if not data_info['exists']:
                alternative_paths = [
                    'optimized_data',
                    'raw',
                    'csv_complete',
                    'smart_download',
                    'historical_download'
                ]
                
                for alt_path in alternative_paths:
                    alt_info = self.scan_directory_for_api_data(alt_path, api_name)
                    if alt_info['exists']:
                        data_info = alt_info
                        data_info['actual_path'] = alt_path
                        break
            
            # 分析缺失情况
            missing_info = self._analyze_missing_data(data_info)
            
            result = {
                'API名称': api_name,
                '数据类型': api_info['name'],
                '数据分类': api_info['category'],
                '预期路径': api_info['expected_path'],
                '实际路径': data_info.get('actual_path', api_info['expected_path']),
                '是否存在': '✅' if data_info['exists'] else '❌',
                '文件数量': data_info['file_count'],
                '时间范围': f"{data_info['time_range']['start']} - {data_info['time_range']['end']}" if data_info['time_range'] else '无数据',
                '时间跨度': f"{data_info['time_range']['span_years']}年" if data_info['time_range'] else '0年',
                '完整性': data_info['status'],
                '缺失分析': missing_info,
                '样本文件': '; '.join(data_info['sample_files'][:2]) if data_info['sample_files'] else '无'
            }
            
            results.append(result)
            print(f"   结果: {result['是否存在']} {result['完整性']}")
        
        self.data_inventory = results
        return results
    
    def _analyze_missing_data(self, data_info):
        """分析缺失数据"""
        if not data_info['exists']:
            return "完全缺失"
        
        if not data_info['time_range']:
            return "无法确定时间范围"
        
        start_date = pd.to_datetime(data_info['time_range']['start'])
        end_date = pd.to_datetime(data_info['time_range']['end'])
        target_start = pd.to_datetime(self.target_start)
        target_end = pd.to_datetime(self.target_end)
        
        missing_parts = []
        
        if start_date > target_start + pd.Timedelta(days=365):
            missing_parts.append(f"缺失早期数据({self.target_start} - {start_date.strftime('%Y-%m-%d')})")
        
        if end_date < target_end - pd.Timedelta(days=90):
            missing_parts.append(f"缺失近期数据({end_date.strftime('%Y-%m-%d')} - {self.target_end})")
        
        return '; '.join(missing_parts) if missing_parts else "时间范围完整"
    
    def generate_summary_table(self):
        """生成汇总表格"""
        if not self.data_inventory:
            return
        
        df = pd.DataFrame(self.data_inventory)
        
        print("\\n📋 数据汇总表格:")
        print("=" * 120)
        
        # 按分类整理
        categories = df['数据分类'].unique()
        
        for category in categories:
            print(f"\\n📊 {category}:")
            print("-" * 80)
            
            cat_df = df[df['数据分类'] == category]
            
            # 创建简化表格
            display_columns = ['数据类型', '是否存在', '文件数量', '时间范围', '完整性', '缺失分析']
            display_df = cat_df[display_columns].copy()
            
            # 格式化显示
            for _, row in display_df.iterrows():
                print(f"  {row['数据类型']:<15} {row['是否存在']:<4} {row['文件数量']:<8} {row['时间范围']:<25} {row['完整性']:<15} {row['缺失分析']}")
        
        return df
    
    def generate_detailed_report(self):
        """生成详细报告"""
        results = self.classify_all_data()
        summary_df = self.generate_summary_table()
        
        # 统计汇总
        total_apis = len(results)
        existing_apis = sum(1 for r in results if r['是否存在'] == '✅')
        missing_apis = total_apis - existing_apis
        
        # 按分类统计
        category_stats = {}
        for result in results:
            category = result['数据分类']
            if category not in category_stats:
                category_stats[category] = {'total': 0, 'existing': 0}
            category_stats[category]['total'] += 1
            if result['是否存在'] == '✅':
                category_stats[category]['existing'] += 1
        
        print("\\n🎊 数据分析汇总:")
        print("=" * 60)
        print(f"📊 API总数: {total_apis}")
        print(f"✅ 有数据: {existing_apis} ({existing_apis/total_apis*100:.1f}%)")
        print(f"❌ 缺失: {missing_apis} ({missing_apis/total_apis*100:.1f}%)")
        
        print(f"\\n📋 分类统计:")
        for category, stats in category_stats.items():
            coverage = stats['existing'] / stats['total'] * 100
            print(f"  {category}: {stats['existing']}/{stats['total']} ({coverage:.1f}%)")
        
        # 保存详细报告
        report = {
            'analysis_time': datetime.now().isoformat(),
            'target_time_range': f"{self.target_start} - {self.target_end}",
            'summary_stats': {
                'total_apis': total_apis,
                'existing_apis': existing_apis,
                'missing_apis': missing_apis,
                'coverage_rate': f"{existing_apis/total_apis*100:.1f}%"
            },
            'category_stats': category_stats,
            'detailed_inventory': results
        }
        
        report_file = self.base_path / 'api_based_data_classification_report.json'
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2, default=str)
        
        # 保存CSV格式
        if summary_df is not None:
            csv_file = self.base_path / 'api_data_inventory.csv'
            summary_df.to_csv(csv_file, index=False, encoding='utf-8-sig')
            print(f"\\n📄 详细报告: {report_file}")
            print(f"📊 CSV表格: {csv_file}")

def main():
    """主函数"""
    classifier = APIBasedDataClassifier()
    classifier.generate_detailed_report()

if __name__ == "__main__":
    main()