#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
准确的数据表格分析器
基于实际文件内容准确分析API数据分布
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import json
import warnings
from collections import defaultdict
warnings.filterwarnings('ignore')

class AccurateDataTableAnalyzer:
    """准确的数据表格分析器"""
    
    def __init__(self):
        """初始化分析器"""
        self.base_path = Path("/Users/jackstudio/QuantTrade/data")
        self.results = []
    
    def analyze_batch_data(self, data_type, path_suffix):
        """分析批次数据"""
        data_path = self.base_path / f"priority_download/market_data/{path_suffix}"
        
        if not data_path.exists():
            return {
                'data_type': data_type,
                'status': '❌ 不存在',
                'file_count': 0,
                'time_range': '无数据',
                'stock_sample': 0,
                'completeness': '无数据',
                'path': str(data_path)
            }
        
        batch_files = list(data_path.glob("*.csv"))
        
        if not batch_files:
            return {
                'data_type': data_type,
                'status': '❌ 无文件',
                'file_count': 0,
                'time_range': '无数据',
                'stock_sample': 0,
                'completeness': '无数据',
                'path': str(data_path)
            }
        
        # 分析第一个和最后一个文件获取时间范围
        first_file = min(batch_files, key=lambda x: x.name)
        last_file = max(batch_files, key=lambda x: x.name)
        
        time_range = "无法确定"
        stock_sample = 0
        
        try:
            # 分析第一个文件
            df_first = pd.read_csv(first_file)
            if 'tradeDate' in df_first.columns:
                df_first['tradeDate'] = pd.to_datetime(df_first['tradeDate'])
                start_date = df_first['tradeDate'].min()
                stock_sample = len(df_first['secID'].unique()) if 'secID' in df_first.columns else 0
            else:
                start_date = None
                
            # 分析最后一个文件
            df_last = pd.read_csv(last_file)
            if 'tradeDate' in df_last.columns:
                df_last['tradeDate'] = pd.to_datetime(df_last['tradeDate'])
                end_date = df_last['tradeDate'].max()
            else:
                end_date = None
            
            if start_date and end_date:
                time_range = f"{start_date.strftime('%Y-%m-%d')} - {end_date.strftime('%Y-%m-%d')}"
                
                # 判断完整性
                target_start = pd.to_datetime('2000-01-01')
                target_end = pd.to_datetime('2025-08-31')
                
                if start_date <= target_start + pd.Timedelta(days=10) and end_date >= target_end - pd.Timedelta(days=10):
                    completeness = "✅ 完整"
                elif start_date <= target_start + pd.Timedelta(days=365) and end_date >= target_end - pd.Timedelta(days=90):
                    completeness = "🟡 基本完整"
                else:
                    completeness = "🔴 部分缺失"
            else:
                completeness = "❓ 无法确定"
                
        except Exception as e:
            time_range = f"读取错误: {str(e)}"
            completeness = "❌ 数据损坏"
        
        return {
            'data_type': data_type,
            'status': '✅ 存在',
            'file_count': len(batch_files),
            'time_range': time_range,
            'stock_sample': stock_sample,
            'completeness': completeness,
            'path': str(data_path)
        }
    
    def analyze_csv_complete_data(self, data_type, path_suffix):
        """分析csv_complete数据"""
        data_path = self.base_path / f"csv_complete/{path_suffix}"
        
        if not data_path.exists():
            return {
                'data_type': f"{data_type}(个股文件)",
                'status': '❌ 不存在',
                'file_count': 0,
                'time_range': '无数据',
                'stock_sample': 0,
                'completeness': '无数据',
                'path': str(data_path)
            }
        
        csv_files = list(data_path.rglob("*.csv"))
        
        if not csv_files:
            return {
                'data_type': f"{data_type}(个股文件)",
                'status': '❌ 无文件',
                'file_count': 0,
                'time_range': '无数据',
                'stock_sample': 0,
                'completeness': '无数据',
                'path': str(data_path)
            }
        
        # 随机抽样分析几个文件获取时间范围
        sample_files = csv_files[:3] if len(csv_files) >= 3 else csv_files
        
        all_start_dates = []
        all_end_dates = []
        
        for sample_file in sample_files:
            try:
                df = pd.read_csv(sample_file)
                if 'tradeDate' in df.columns:
                    df['tradeDate'] = pd.to_datetime(df['tradeDate'])
                    all_start_dates.append(df['tradeDate'].min())
                    all_end_dates.append(df['tradeDate'].max())
            except:
                continue
        
        if all_start_dates and all_end_dates:
            overall_start = min(all_start_dates)
            overall_end = max(all_end_dates)
            time_range = f"{overall_start.strftime('%Y-%m-%d')} - {overall_end.strftime('%Y-%m-%d')}"
            
            # 判断完整性
            target_start = pd.to_datetime('2000-01-01')
            target_end = pd.to_datetime('2025-08-31')
            
            if overall_start <= target_start + pd.Timedelta(days=10) and overall_end >= target_end - pd.Timedelta(days=10):
                completeness = "✅ 完整"
            elif overall_start <= target_start + pd.Timedelta(days=365) and overall_end >= target_end - pd.Timedelta(days=90):
                completeness = "🟡 基本完整"
            else:
                completeness = "🔴 部分缺失"
        else:
            time_range = "无法确定"
            completeness = "❓ 无法确定"
        
        return {
            'data_type': f"{data_type}(个股文件)",
            'status': '✅ 存在',
            'file_count': len(csv_files),
            'time_range': time_range,
            'stock_sample': len(csv_files),  # 个股文件数量就是股票数量
            'completeness': completeness,
            'path': str(data_path)
        }
    
    def analyze_special_data(self):
        """分析特殊数据类型"""
        special_results = []
        
        # 检查final_comprehensive_download目录
        final_path = self.base_path / "final_comprehensive_download"
        
        if final_path.exists():
            subdirs = [d for d in final_path.iterdir() if d.is_dir()]
            
            for subdir in subdirs:
                files = list(subdir.rglob("*.csv"))
                
                if files:
                    # 尝试分析时间范围
                    sample_file = files[0]
                    time_range = "数据存在"
                    
                    try:
                        df = pd.read_csv(sample_file)
                        date_cols = [col for col in df.columns if any(keyword in col.lower() for keyword in ['date', 'time', '日期'])]
                        
                        if date_cols:
                            df[date_cols[0]] = pd.to_datetime(df[date_cols[0]])
                            start_date = df[date_cols[0]].min()
                            end_date = df[date_cols[0]].max()
                            time_range = f"{start_date.strftime('%Y-%m-%d')} - {end_date.strftime('%Y-%m-%d')}"
                    except:
                        pass
                    
                    special_results.append({
                        'data_type': f"{subdir.name}数据",
                        'status': '✅ 存在',
                        'file_count': len(files),
                        'time_range': time_range,
                        'stock_sample': '不确定',
                        'completeness': '🟡 需要具体分析',
                        'path': str(subdir)
                    })
        
        return special_results
    
    def generate_complete_table(self):
        """生成完整的数据表格"""
        print("📊 生成准确的数据分析表格...")
        print("🎯 目标: 2000-01-01 至 2025-08-31")
        print("=" * 100)
        
        # 分析核心行情数据
        core_data = [
            ('日线行情数据', 'daily'),
            ('周线行情数据', 'weekly'), 
            ('月线行情数据', 'monthly'),
            ('前复权日线', 'adj_daily'),
            ('前复权周线', 'adj_weekly'),
            ('前复权月线', 'adj_monthly')
        ]
        
        print("\\n📈 核心行情数据分析:")
        print("-" * 100)
        print(f"{'数据类型':<20} {'状态':<10} {'文件数':<8} {'时间范围':<30} {'股票样本':<10} {'完整性':<15}")
        print("-" * 100)
        
        for data_name, path_suffix in core_data:
            result = self.analyze_batch_data(data_name, path_suffix)
            self.results.append(result)
            
            print(f"{result['data_type']:<20} {result['status']:<10} {result['file_count']:<8} {result['time_range']:<30} {result['stock_sample']:<10} {result['completeness']:<15}")
        
        # 分析个股文件数据
        print("\\n📊 个股文件数据分析:")
        print("-" * 100)
        print(f"{'数据类型':<20} {'状态':<10} {'文件数':<8} {'时间范围':<30} {'股票数量':<10} {'完整性':<15}")
        print("-" * 100)
        
        for data_name, path_suffix in [('日线', 'daily'), ('周线', 'weekly'), ('月线', 'monthly')]:
            result = self.analyze_csv_complete_data(data_name, path_suffix)
            self.results.append(result)
            
            print(f"{result['data_type']:<20} {result['status']:<10} {result['file_count']:<8} {result['time_range']:<30} {result['stock_sample']:<10} {result['completeness']:<15}")
        
        # 分析其他数据
        special_results = self.analyze_special_data()
        
        if special_results:
            print("\\n🔍 其他数据类型:")
            print("-" * 100)
            print(f"{'数据类型':<20} {'状态':<10} {'文件数':<8} {'时间范围':<30} {'说明':<10} {'完整性':<15}")
            print("-" * 100)
            
            for result in special_results:
                self.results.append(result)
                print(f"{result['data_type']:<20} {result['status']:<10} {result['file_count']:<8} {result['time_range']:<30} {result['stock_sample']:<10} {result['completeness']:<15}")
        
        self.generate_summary()
    
    def generate_summary(self):
        """生成总结"""
        print("\\n🎊 数据分析总结:")
        print("=" * 80)
        
        # 统计各状态数量
        existing_count = sum(1 for r in self.results if r['status'] == '✅ 存在')
        missing_count = sum(1 for r in self.results if r['status'].startswith('❌'))
        complete_count = sum(1 for r in self.results if r['completeness'] == '✅ 完整')
        
        print(f"📊 数据类型总数: {len(self.results)}")
        print(f"✅ 存在数据: {existing_count}")
        print(f"❌ 缺失数据: {missing_count}")
        print(f"🏆 完整数据: {complete_count}")
        
        # 重点关注的核心数据
        core_types = ['日线行情数据', '周线行情数据', '月线行情数据']
        print(f"\\n🎯 核心行情数据状态:")
        for result in self.results:
            if result['data_type'] in core_types:
                print(f"   {result['data_type']}: {result['completeness']} ({result['time_range']})")
        
        # 保存详细结果
        report_file = self.base_path / 'accurate_data_analysis_table.json'
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump({
                'analysis_time': datetime.now().isoformat(),
                'target_range': '2000-01-01 to 2025-08-31',
                'summary': {
                    'total_types': len(self.results),
                    'existing_data': existing_count,
                    'missing_data': missing_count,
                    'complete_data': complete_count
                },
                'detailed_results': self.results
            }, f, ensure_ascii=False, indent=2, default=str)
        
        print(f"\\n📄 详细报告已保存: {report_file}")

def main():
    """主函数"""
    analyzer = AccurateDataTableAnalyzer()
    analyzer.generate_complete_table()

if __name__ == "__main__":
    main()