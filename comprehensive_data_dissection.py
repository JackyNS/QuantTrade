#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
全面拆解本地数据结构分析器
============================

目标：
1. 全面扫描所有数据目录和文件
2. 按API来源分类统计数据
3. 分析数据完整性和时间覆盖
4. 识别重复、缺失和不一致的数据
5. 为数据重组提供基础信息

"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import json
import warnings
from collections import defaultdict, Counter
import os
import re

warnings.filterwarnings('ignore')

class ComprehensiveDataDissection:
    """全面数据拆解分析器"""
    
    def __init__(self):
        """初始化"""
        self.base_path = Path("/Users/jackstudio/QuantTrade/data")
        self.analysis_results = {}
        
    def scan_all_data_directories(self):
        """扫描所有数据目录"""
        print("🔍 全面扫描本地数据目录结构...")
        print("=" * 80)
        
        directory_structure = {}
        
        # 递归扫描所有目录
        for root, dirs, files in os.walk(self.base_path):
            rel_path = Path(root).relative_to(self.base_path)
            
            # 统计文件信息
            csv_files = [f for f in files if f.endswith('.csv')]
            json_files = [f for f in files if f.endswith('.json')]
            other_files = [f for f in files if not f.endswith(('.csv', '.json'))]
            
            if csv_files or json_files or other_files:
                directory_structure[str(rel_path)] = {
                    'csv_count': len(csv_files),
                    'json_count': len(json_files),
                    'other_count': len(other_files),
                    'total_files': len(files),
                    'csv_files': csv_files[:10],  # 只显示前10个文件名
                    'directory_size_mb': self._calculate_directory_size(Path(root))
                }
        
        self.analysis_results['directory_structure'] = directory_structure
        return directory_structure
    
    def _calculate_directory_size(self, directory):
        """计算目录大小（MB）"""
        total_size = 0
        try:
            for file_path in directory.rglob('*'):
                if file_path.is_file():
                    total_size += file_path.stat().st_size
        except:
            pass
        return round(total_size / (1024 * 1024), 2)
    
    def analyze_batch_files(self):
        """分析批次文件（priority_download）"""
        print("\n📄 分析批次文件...")
        print("-" * 60)
        
        batch_path = self.base_path / "priority_download/market_data/daily"
        batch_analysis = {}
        
        if not batch_path.exists():
            print("❌ 批次文件目录不存在")
            return {}
        
        batch_files = list(batch_path.glob("*.csv"))
        print(f"📊 批次文件总数: {len(batch_files)}")
        
        # 按年份分组分析
        year_groups = defaultdict(list)
        for file_path in batch_files:
            try:
                year = file_path.stem.split('_')[0]
                year_groups[year].append(file_path)
            except:
                continue
        
        print(f"📅 覆盖年份: {min(year_groups.keys())} - {max(year_groups.keys())}")
        
        # 详细分析每年的数据
        year_details = {}
        for year in sorted(year_groups.keys()):
            files = year_groups[year]
            print(f"\n📋 {year}年批次文件: {len(files)} 个")
            
            # 抽样分析文件内容
            sample_stats = []
            for sample_file in files[:3]:  # 分析前3个文件
                try:
                    df = pd.read_csv(sample_file)
                    if 'tradeDate' in df.columns and 'secID' in df.columns:
                        df['tradeDate'] = pd.to_datetime(df['tradeDate'])
                        stats = {
                            'file': sample_file.name,
                            'stocks': len(df['secID'].unique()),
                            'records': len(df),
                            'date_range': f"{df['tradeDate'].min().date()} - {df['tradeDate'].max().date()}",
                            'columns': list(df.columns)
                        }
                        sample_stats.append(stats)
                        print(f"   📈 {sample_file.name}: {stats['stocks']} 只股票, {stats['records']} 条记录")
                except Exception as e:
                    print(f"   ❌ {sample_file.name}: 读取失败")
            
            year_details[year] = {
                'file_count': len(files),
                'sample_stats': sample_stats,
                'total_size_mb': sum(self._calculate_directory_size(f.parent) for f in files[:1])
            }
        
        batch_analysis = {
            'total_files': len(batch_files),
            'year_range': f"{min(year_groups.keys())} - {max(year_groups.keys())}",
            'years_covered': len(year_groups),
            'year_details': year_details
        }
        
        self.analysis_results['batch_files'] = batch_analysis
        return batch_analysis
    
    def analyze_individual_stock_files(self):
        """分析个股文件（csv_complete）"""
        print("\n📊 分析个股文件...")
        print("-" * 60)
        
        individual_analysis = {}
        
        # 1. 分析主要个股文件目录
        stock_dirs = {
            'daily_stocks': self.base_path / "csv_complete/daily/stocks",
            'weekly_stocks': self.base_path / "csv_complete/weekly/stocks", 
            'monthly_stocks': self.base_path / "csv_complete/monthly/stocks"
        }
        
        for dir_name, dir_path in stock_dirs.items():
            if not dir_path.exists():
                print(f"❌ {dir_name} 目录不存在")
                continue
                
            stock_files = list(dir_path.glob("*.csv"))
            print(f"📈 {dir_name}: {len(stock_files)} 个文件")
            
            # 抽样分析
            sample_files = stock_files[:20]  # 分析前20个文件
            sample_analysis = []
            
            for file_path in sample_files:
                try:
                    df = pd.read_csv(file_path)
                    stock_id = file_path.stem.replace('_', '.')
                    
                    if 'tradeDate' in df.columns and len(df) > 0:
                        df['tradeDate'] = pd.to_datetime(df['tradeDate'])
                        analysis = {
                            'stock_id': stock_id,
                            'records': len(df),
                            'date_range': f"{df['tradeDate'].min().date()} - {df['tradeDate'].max().date()}",
                            'years': round((df['tradeDate'].max() - df['tradeDate'].min()).days / 365.25, 1)
                        }
                        sample_analysis.append(analysis)
                except Exception as e:
                    continue
            
            individual_analysis[dir_name] = {
                'total_files': len(stock_files),
                'sample_analysis': sample_analysis[:10]  # 只保存前10个样本
            }
        
        # 2. 分析按年分类的文件
        yearly_path = self.base_path / "csv_complete/daily/yearly"
        if yearly_path.exists():
            year_dirs = [d for d in yearly_path.iterdir() if d.is_dir() and d.name.startswith('year_')]
            year_analysis = {}
            
            print(f"\n📅 按年分类目录: {len(year_dirs)} 个年份")
            
            for year_dir in sorted(year_dirs):
                year = year_dir.name.replace('year_', '')
                year_files = list(year_dir.glob("*.csv"))
                
                year_analysis[year] = {
                    'file_count': len(year_files),
                    'size_mb': self._calculate_directory_size(year_dir)
                }
                
                print(f"   📋 {year}: {len(year_files)} 个文件")
            
            individual_analysis['yearly_classification'] = year_analysis
        
        self.analysis_results['individual_files'] = individual_analysis
        return individual_analysis
    
    def analyze_data_consistency(self):
        """分析数据一致性"""
        print("\n🔍 分析数据一致性...")
        print("-" * 60)
        
        consistency_issues = []
        
        # 检查同一股票在不同位置的数据是否一致
        test_stocks = ['000001_XSHE', '000002_XSHE', '600000_XSHG']
        
        for stock in test_stocks:
            stock_data_locations = {}
            
            # 1. 检查主要个股文件
            main_file = self.base_path / f"csv_complete/daily/stocks/{stock}.csv"
            if main_file.exists():
                try:
                    df = pd.read_csv(main_file)
                    stock_data_locations['main_file'] = {
                        'records': len(df),
                        'date_range': f"{df['tradeDate'].min()} - {df['tradeDate'].max()}" if 'tradeDate' in df.columns else 'No dates'
                    }
                except:
                    stock_data_locations['main_file'] = {'error': 'Failed to read'}
            
            # 2. 检查年度分类文件（2024年）
            year_file = self.base_path / f"csv_complete/daily/yearly/year_2024/{stock}.csv"
            if year_file.exists():
                try:
                    df = pd.read_csv(year_file)
                    stock_data_locations['year_2024'] = {
                        'records': len(df),
                        'date_range': f"{df['tradeDate'].min()} - {df['tradeDate'].max()}" if 'tradeDate' in df.columns else 'No dates'
                    }
                except:
                    stock_data_locations['year_2024'] = {'error': 'Failed to read'}
            
            if len(stock_data_locations) > 1:
                consistency_issues.append({
                    'stock': stock,
                    'locations': stock_data_locations
                })
        
        print(f"🔍 一致性检查样本: {len(test_stocks)} 只股票")
        for issue in consistency_issues:
            print(f"   📊 {issue['stock']}:")
            for location, data in issue['locations'].items():
                if 'error' not in data:
                    print(f"      {location}: {data['records']} 条记录, {data['date_range']}")
                else:
                    print(f"      {location}: {data['error']}")
        
        self.analysis_results['consistency_issues'] = consistency_issues
        return consistency_issues
    
    def identify_data_gaps(self):
        """识别数据缺口"""
        print("\n⚠️ 识别数据缺口...")
        print("-" * 60)
        
        gaps_analysis = {}
        
        # 1. 检查批次文件的时间连续性
        batch_path = self.base_path / "priority_download/market_data/daily"
        if batch_path.exists():
            batch_files = sorted(batch_path.glob("*.csv"))
            
            # 分析批次文件的年份覆盖
            batch_years = set()
            for file_path in batch_files:
                try:
                    year = file_path.stem.split('_')[0]
                    batch_years.add(int(year))
                except:
                    continue
            
            if batch_years:
                year_range = list(range(min(batch_years), max(batch_years) + 1))
                missing_years = set(year_range) - batch_years
                
                gaps_analysis['batch_files'] = {
                    'covered_years': sorted(batch_years),
                    'missing_years': sorted(missing_years),
                    'year_range': f"{min(batch_years)}-{max(batch_years)}"
                }
                
                print(f"📅 批次文件年份覆盖: {min(batch_years)}-{max(batch_years)}")
                if missing_years:
                    print(f"⚠️ 缺失年份: {sorted(missing_years)}")
                else:
                    print("✅ 年份覆盖连续")
        
        # 2. 检查个股文件与批次文件的差异
        individual_path = self.base_path / "csv_complete/daily/stocks"
        if individual_path.exists() and batch_path.exists():
            # 从批次文件中统计总股票数
            all_stocks_in_batch = set()
            for file_path in batch_files[:10]:  # 检查前10个批次文件
                try:
                    df = pd.read_csv(file_path)
                    if 'secID' in df.columns:
                        all_stocks_in_batch.update(df['secID'].unique())
                except:
                    continue
            
            # 统计个股文件数
            individual_files = list(individual_path.glob("*.csv"))
            individual_stocks = {f.stem.replace('_', '.') for f in individual_files}
            
            gaps_analysis['stock_coverage'] = {
                'batch_stocks_sample': len(all_stocks_in_batch),
                'individual_files': len(individual_files),
                'missing_individual_files': len(all_stocks_in_batch - individual_stocks) if all_stocks_in_batch else 0
            }
            
            print(f"📊 批次文件股票数（抽样）: {len(all_stocks_in_batch)}")
            print(f"📈 个股文件数: {len(individual_files)}")
            if all_stocks_in_batch:
                missing_count = len(all_stocks_in_batch - individual_stocks)
                print(f"⚠️ 可能缺失的个股文件: {missing_count}")
        
        self.analysis_results['data_gaps'] = gaps_analysis
        return gaps_analysis
    
    def generate_comprehensive_report(self):
        """生成全面的数据拆解报告"""
        print("\n🎊 生成全面数据拆解报告")
        print("=" * 80)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # 1. 扫描目录结构
        directory_structure = self.scan_all_data_directories()
        
        # 2. 分析批次文件
        batch_analysis = self.analyze_batch_files()
        
        # 3. 分析个股文件
        individual_analysis = self.analyze_individual_stock_files()
        
        # 4. 分析数据一致性
        consistency_analysis = self.analyze_data_consistency()
        
        # 5. 识别数据缺口
        gaps_analysis = self.identify_data_gaps()
        
        # 6. 生成总结报告
        summary_report = {
            'analysis_time': datetime.now().isoformat(),
            'total_directories': len(directory_structure),
            'total_data_size_gb': sum(d['directory_size_mb'] for d in directory_structure.values()) / 1024,
            'key_findings': self._generate_key_findings(),
            'data_organization_status': self._assess_data_organization(),
            'recommendations': self._generate_recommendations()
        }
        
        # 保存完整报告
        complete_report = {
            'summary': summary_report,
            'directory_structure': directory_structure,
            'batch_files_analysis': batch_analysis,
            'individual_files_analysis': individual_analysis,
            'consistency_analysis': consistency_analysis,
            'data_gaps_analysis': gaps_analysis
        }
        
        # 保存报告文件
        report_file = Path(f"data_dissection_report_{timestamp}.json")
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(complete_report, f, indent=2, ensure_ascii=False, default=str)
        
        print(f"\n📄 完整报告已保存: {report_file}")
        
        # 显示关键发现
        self._display_key_findings(summary_report)
        
        return complete_report
    
    def _generate_key_findings(self):
        """生成关键发现"""
        findings = []
        
        # 基于分析结果生成发现
        if 'batch_files' in self.analysis_results:
            batch_info = self.analysis_results['batch_files']
            findings.append(f"批次文件覆盖 {batch_info['years_covered']} 个年份")
        
        if 'individual_files' in self.analysis_results:
            individual_info = self.analysis_results['individual_files']
            if 'daily_stocks' in individual_info:
                findings.append(f"个股日线文件 {individual_info['daily_stocks']['total_files']} 个")
        
        if 'consistency_issues' in self.analysis_results:
            consistency_info = self.analysis_results['consistency_issues']
            findings.append(f"发现 {len(consistency_info)} 个一致性检查样本")
        
        return findings
    
    def _assess_data_organization(self):
        """评估数据组织状况"""
        return {
            'batch_files_status': 'Complete' if 'batch_files' in self.analysis_results else 'Missing',
            'individual_files_status': 'Partial' if 'individual_files' in self.analysis_results else 'Missing',
            'yearly_classification_status': 'Exists' if 'yearly_classification' in self.analysis_results.get('individual_files', {}) else 'Missing'
        }
    
    def _generate_recommendations(self):
        """生成建议"""
        recommendations = [
            "1. 统一数据存储结构，避免重复存储",
            "2. 建立数据同步机制，确保批次文件与个股文件一致",
            "3. 实施数据完整性检查，定期验证数据质量",
            "4. 优化存储空间，删除冗余和过时数据"
        ]
        return recommendations
    
    def _display_key_findings(self, summary_report):
        """显示关键发现"""
        print("\n💡 关键发现:")
        for finding in summary_report['key_findings']:
            print(f"   ✓ {finding}")
        
        print(f"\n📊 数据总量: {summary_report['total_data_size_gb']:.2f} GB")
        print(f"📁 目录数量: {summary_report['total_directories']}")
        
        print("\n🎯 数据组织状况:")
        for status_key, status_value in summary_report['data_organization_status'].items():
            print(f"   • {status_key}: {status_value}")
        
        print("\n💡 建议:")
        for rec in summary_report['recommendations']:
            print(f"   {rec}")

def main():
    """主函数"""
    print("🔍 全面拆解本地数据结构")
    print("🎯 目标：为数据重组提供详细基础信息")
    print("=" * 80)
    
    dissector = ComprehensiveDataDissection()
    report = dissector.generate_comprehensive_report()
    
    print("\n✅ 数据拆解分析完成！")
    print("接下来可以基于此报告进行数据重组工作。")

if __name__ == "__main__":
    main()