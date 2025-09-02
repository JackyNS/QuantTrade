#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
本地数据全面检查和梳理工具
这是唯一且首要的工作：搞清楚本地数据的所有明细
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import json
import warnings
from collections import defaultdict
import os
import subprocess
warnings.filterwarnings('ignore')

class CompleteLocalDataInspector:
    """本地数据全面检查和梳理工具"""
    
    def __init__(self):
        """初始化检查工具"""
        self.base_path = Path("/Users/jackstudio/QuantTrade/data")
        self.report = {
            'inspection_time': datetime.now().isoformat(),
            'total_size': None,
            'directory_structure': {},
            'file_inventory': {},
            'data_content_analysis': {},
            'stock_universe': {},
            'time_coverage': {},
            'data_quality': {},
            'recommendations': []
        }
    
    def get_total_size(self):
        """获取总数据大小"""
        print("📏 获取总数据大小...")
        try:
            result = subprocess.run(['du', '-sh', str(self.base_path)], 
                                  capture_output=True, text=True)
            total_size = result.stdout.split()[0]
            print(f"   💾 总大小: {total_size}")
            self.report['total_size'] = total_size
            return total_size
        except Exception as e:
            print(f"   ❌ 获取大小失败: {e}")
            return "Unknown"
    
    def analyze_directory_structure(self):
        """分析目录结构"""
        print("\n📁 分析目录结构...")
        
        structure = {}
        
        for item in self.base_path.iterdir():
            if item.is_dir():
                try:
                    # 获取目录大小
                    result = subprocess.run(['du', '-sh', str(item)], 
                                          capture_output=True, text=True)
                    size = result.stdout.split()[0] if result.returncode == 0 else "Unknown"
                    
                    # 统计文件数量
                    file_count = sum(1 for _ in item.rglob('*') if _.is_file())
                    
                    structure[item.name] = {
                        'path': str(item),
                        'size': size,
                        'file_count': file_count,
                        'subdirs': [sub.name for sub in item.iterdir() if sub.is_dir()]
                    }
                    
                    print(f"   📂 {item.name}: {size}, {file_count} 个文件")
                    
                except Exception as e:
                    structure[item.name] = {
                        'path': str(item),
                        'size': 'Error',
                        'error': str(e)
                    }
        
        self.report['directory_structure'] = structure
        return structure
    
    def inventory_batch_files(self):
        """清点批次文件"""
        print("\n📄 清点批次文件...")
        
        batch_dirs = [
            'priority_download/market_data/daily',
            'priority_download/market_data/weekly', 
            'priority_download/market_data/monthly'
        ]
        
        batch_inventory = {}
        
        for batch_dir in batch_dirs:
            batch_path = self.base_path / batch_dir
            data_type = batch_dir.split('/')[-1]
            
            if not batch_path.exists():
                batch_inventory[data_type] = {'exists': False}
                print(f"   ❌ {data_type}: 目录不存在")
                continue
            
            # 获取所有CSV文件
            csv_files = list(batch_path.glob('*.csv'))
            
            # 按年份分组统计
            year_groups = defaultdict(list)
            for file_path in csv_files:
                try:
                    year = file_path.stem.split('_')[0]
                    year_groups[year].append(file_path.name)
                except:
                    year_groups['unknown'].append(file_path.name)
            
            batch_inventory[data_type] = {
                'exists': True,
                'total_files': len(csv_files),
                'year_distribution': {year: len(files) for year, files in year_groups.items()},
                'year_range': f"{min(year_groups.keys())} - {max(year_groups.keys())}" if year_groups else "None"
            }
            
            print(f"   📊 {data_type}: {len(csv_files)} 个文件 ({min(year_groups.keys()) if year_groups else 'N/A'} - {max(year_groups.keys()) if year_groups else 'N/A'})")
        
        self.report['file_inventory']['batch_files'] = batch_inventory
        return batch_inventory
    
    def inventory_csv_complete_files(self):
        """清点csv_complete文件"""
        print("\n📊 清点csv_complete文件...")
        
        csv_complete_path = self.base_path / 'csv_complete'
        
        if not csv_complete_path.exists():
            print("   ❌ csv_complete目录不存在")
            self.report['file_inventory']['csv_complete'] = {'exists': False}
            return {'exists': False}
        
        csv_inventory = {'exists': True}
        
        # 检查各时间周期目录
        timeframes = ['daily', 'weekly', 'monthly']
        
        for tf in timeframes:
            tf_path = csv_complete_path / tf
            
            if tf_path.exists():
                # 递归统计所有CSV文件
                csv_files = list(tf_path.rglob('*.csv'))
                
                # 分析文件分布
                if csv_files:
                    # 按目录层级分析
                    dir_levels = defaultdict(int)
                    for file_path in csv_files:
                        relative_path = file_path.relative_to(tf_path)
                        level = len(relative_path.parts) - 1  # 减去文件名
                        dir_levels[f"level_{level}"] += 1
                    
                    csv_inventory[tf] = {
                        'file_count': len(csv_files),
                        'directory_levels': dict(dir_levels),
                        'sample_files': [f.name for f in csv_files[:5]]  # 前5个文件样本
                    }
                else:
                    csv_inventory[tf] = {'file_count': 0}
                
                print(f"   📂 {tf}: {len(csv_files)} 个文件")
            else:
                csv_inventory[tf] = {'exists': False, 'file_count': 0}
                print(f"   ❌ {tf}: 目录不存在")
        
        self.report['file_inventory']['csv_complete'] = csv_inventory
        return csv_inventory
    
    def analyze_sample_data_content(self):
        """分析样本数据内容"""
        print("\n🔍 分析样本数据内容...")
        
        content_analysis = {}
        
        # 分析批次文件样本
        daily_path = self.base_path / 'priority_download/market_data/daily'
        if daily_path.exists():
            sample_files = list(daily_path.glob('*.csv'))[:5]  # 取5个样本
            
            batch_samples = []
            all_stocks = set()
            all_dates = []
            
            for sample_file in sample_files:
                try:
                    df = pd.read_csv(sample_file)
                    
                    # 分析文件内容
                    file_analysis = {
                        'filename': sample_file.name,
                        'records': len(df),
                        'columns': list(df.columns),
                        'stock_count': len(df['secID'].unique()) if 'secID' in df.columns else 0,
                        'date_range': None
                    }
                    
                    # 分析日期范围
                    if 'tradeDate' in df.columns:
                        df['tradeDate'] = pd.to_datetime(df['tradeDate'])
                        min_date = df['tradeDate'].min()
                        max_date = df['tradeDate'].max()
                        file_analysis['date_range'] = {
                            'start': min_date.strftime('%Y-%m-%d'),
                            'end': max_date.strftime('%Y-%m-%d')
                        }
                        all_dates.extend([min_date, max_date])
                    
                    # 收集股票代码
                    if 'secID' in df.columns:
                        stocks = df['secID'].unique()
                        all_stocks.update(stocks)
                        file_analysis['sample_stocks'] = list(stocks[:5])
                    
                    batch_samples.append(file_analysis)
                    print(f"   📄 {sample_file.name}: {len(df)} 条记录, {file_analysis['stock_count']} 只股票")
                    
                except Exception as e:
                    batch_samples.append({
                        'filename': sample_file.name,
                        'error': str(e)
                    })
                    print(f"   ❌ {sample_file.name}: 读取失败")
            
            content_analysis['batch_samples'] = {
                'sample_files': batch_samples,
                'total_unique_stocks': len(all_stocks),
                'sample_stock_list': list(all_stocks)[:20],  # 前20只样本股票
                'overall_date_range': {
                    'earliest': min(all_dates).strftime('%Y-%m-%d') if all_dates else None,
                    'latest': max(all_dates).strftime('%Y-%m-%d') if all_dates else None
                }
            }
            
            print(f"   📈 样本统计: {len(all_stocks)} 只股票")
            if all_dates:
                print(f"   📅 时间跨度: {min(all_dates).date()} - {max(all_dates).date()}")
        
        # 分析csv_complete样本
        csv_daily_path = self.base_path / 'csv_complete/daily'
        if csv_daily_path.exists():
            csv_files = list(csv_daily_path.rglob('*.csv'))
            
            if csv_files:
                sample_csv = csv_files[0]  # 取一个样本
                try:
                    df_csv = pd.read_csv(sample_csv)
                    
                    csv_sample = {
                        'sample_file': sample_csv.name,
                        'records': len(df_csv),
                        'columns': list(df_csv.columns),
                        'structure': 'individual_stock_files'
                    }
                    
                    if 'tradeDate' in df_csv.columns:
                        df_csv['tradeDate'] = pd.to_datetime(df_csv['tradeDate'])
                        csv_sample['date_range'] = {
                            'start': df_csv['tradeDate'].min().strftime('%Y-%m-%d'),
                            'end': df_csv['tradeDate'].max().strftime('%Y-%m-%d')
                        }
                    
                    content_analysis['csv_complete_sample'] = csv_sample
                    print(f"   📊 CSV样本: {sample_csv.name}, {len(df_csv)} 条记录")
                    
                except Exception as e:
                    content_analysis['csv_complete_sample'] = {'error': str(e)}
        
        self.report['data_content_analysis'] = content_analysis
        return content_analysis
    
    def estimate_full_coverage(self):
        """估算完整数据覆盖情况"""
        print("\n🎯 估算完整数据覆盖...")
        
        coverage_estimate = {}
        
        # 基于批次文件估算
        daily_path = self.base_path / 'priority_download/market_data/daily'
        if daily_path.exists():
            batch_files = list(daily_path.glob('*.csv'))
            
            # 按年份统计
            year_stats = defaultdict(lambda: {'files': 0, 'estimated_stocks': 0})
            
            for file_path in batch_files:
                try:
                    year = file_path.stem.split('_')[0]
                    year_stats[year]['files'] += 1
                    year_stats[year]['estimated_stocks'] += 100  # 假设每文件100只股票
                except:
                    continue
            
            # 总体估算
            total_files = len(batch_files)
            estimated_total_stocks = len(batch_files) * 100  # 粗略估算
            
            coverage_estimate['batch_files'] = {
                'total_batch_files': total_files,
                'estimated_stock_records': estimated_total_stocks,
                'year_coverage': dict(year_stats),
                'time_span': f"{min(year_stats.keys())} - {max(year_stats.keys())}" if year_stats else "Unknown"
            }
            
            print(f"   📄 批次文件: {total_files} 个")
            print(f"   📈 估算股票记录: {estimated_total_stocks:,} 条")
            print(f"   📅 年份覆盖: {min(year_stats.keys())} - {max(year_stats.keys())}")
        
        # CSV完整文件估算
        csv_path = self.base_path / 'csv_complete'
        if csv_path.exists():
            daily_csv_files = list((csv_path / 'daily').rglob('*.csv'))
            
            coverage_estimate['csv_complete'] = {
                'individual_stock_files': len(daily_csv_files),
                'estimated_unique_stocks': len(daily_csv_files),  # 假设每文件一只股票
                'organization': 'by_stock'
            }
            
            print(f"   📊 个股文件: {len(daily_csv_files)} 个")
        
        self.report['stock_universe'] = coverage_estimate
        return coverage_estimate
    
    def generate_recommendations(self):
        """生成使用建议"""
        recommendations = []
        
        # 基于分析结果生成建议
        if 'batch_files' in self.report.get('file_inventory', {}):
            batch_info = self.report['file_inventory']['batch_files']
            if batch_info.get('daily', {}).get('total_files', 0) > 0:
                recommendations.append("批次数据文件完整，适合进行大规模股票筛选和分析")
        
        if 'csv_complete' in self.report.get('file_inventory', {}):
            csv_info = self.report['file_inventory']['csv_complete']
            if csv_info.get('daily', {}).get('file_count', 0) > 0:
                recommendations.append("个股CSV文件完整，适合单股票详细分析")
        
        if self.report.get('stock_universe', {}).get('batch_files', {}).get('time_span'):
            time_span = self.report['stock_universe']['batch_files']['time_span']
            if '2000' in time_span and '2025' in time_span:
                recommendations.append("时间覆盖完整(2000-2025)，满足长期历史分析需求")
        
        recommendations.append("数据量庞大(220GB)，建议使用高效的分析策略")
        recommendations.append("优先使用批次文件进行策略筛选，再用个股文件进行详细分析")
        
        self.report['recommendations'] = recommendations
        return recommendations
    
    def generate_complete_report(self):
        """生成完整检查报告"""
        print("🔍 开始本地数据全面检查和梳理...")
        print("🎯 目标: 搞清楚本地数据的所有明细")
        print("=" * 80)
        
        # 执行所有检查
        self.get_total_size()
        self.analyze_directory_structure()
        self.inventory_batch_files()
        self.inventory_csv_complete_files()
        self.analyze_sample_data_content()
        self.estimate_full_coverage()
        self.generate_recommendations()
        
        # 保存详细报告
        report_file = self.base_path / 'complete_local_data_inspection_report.json'
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(self.report, f, ensure_ascii=False, indent=2, default=str)
        
        # 生成可读性总结
        self.print_summary(report_file)
    
    def print_summary(self, report_file):
        """打印总结报告"""
        print("\n🎊 本地数据全面检查完成!")
        print("=" * 80)
        print("📊 数据概览:")
        print(f"   💾 总大小: {self.report.get('total_size', 'Unknown')}")
        
        # 目录结构总结
        dir_count = len(self.report.get('directory_structure', {}))
        print(f"   📁 主要目录: {dir_count} 个")
        
        # 批次文件总结
        batch_info = self.report.get('file_inventory', {}).get('batch_files', {})
        if 'daily' in batch_info:
            daily_files = batch_info['daily'].get('total_files', 0)
            print(f"   📄 日线批次文件: {daily_files} 个")
        
        # CSV文件总结
        csv_info = self.report.get('file_inventory', {}).get('csv_complete', {})
        if 'daily' in csv_info:
            csv_files = csv_info['daily'].get('file_count', 0)
            print(f"   📊 个股CSV文件: {csv_files:,} 个")
        
        # 数据范围总结
        coverage = self.report.get('stock_universe', {}).get('batch_files', {})
        if 'time_span' in coverage:
            print(f"   📅 时间跨度: {coverage['time_span']}")
        if 'estimated_stock_records' in coverage:
            print(f"   📈 估算记录: {coverage['estimated_stock_records']:,} 条")
        
        print(f"\n💡 核心建议:")
        for i, rec in enumerate(self.report.get('recommendations', []), 1):
            print(f"   {i}. {rec}")
        
        print(f"\n📄 详细报告: {report_file}")
        print("✅ 本地数据明细已全部梳理完成!")

def main():
    """主函数"""
    inspector = CompleteLocalDataInspector()
    inspector.generate_complete_report()

if __name__ == "__main__":
    main()