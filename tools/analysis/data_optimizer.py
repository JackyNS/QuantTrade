#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据优化整合工具
"""

import pandas as pd
import numpy as np
from pathlib import Path
import json
from datetime import datetime
import shutil
import os
from tqdm import tqdm
import warnings
warnings.filterwarnings('ignore')

class DataOptimizer:
    """数据优化器"""
    
    def __init__(self):
        self.data_root = Path("data")
        self.optimized_root = self.data_root / "optimized"
        self.backup_root = self.data_root / "backup"
        
    def analyze_redundancy(self):
        """分析数据冗余"""
        print("🔍 分析数据冗余...")
        
        redundancy_report = {
            'overlapping_years': [],
            'duplicate_data': {},
            'size_comparison': {}
        }
        
        # 1. 分析年份重叠
        overlap_analysis = self._analyze_year_overlap()
        redundancy_report['overlapping_years'] = overlap_analysis
        
        # 2. 分析重复数据
        duplicate_analysis = self._analyze_duplicate_content()
        redundancy_report['duplicate_data'] = duplicate_analysis
        
        # 3. 分析存储效率
        size_analysis = self._analyze_storage_efficiency()
        redundancy_report['size_comparison'] = size_analysis
        
        return redundancy_report
    
    def _analyze_year_overlap(self):
        """分析年份重叠"""
        overlap_data = []
        
        # 检查2003-2025年的重叠情况
        for year in range(2003, 2026):
            year_data = {
                'year': year,
                'sources': [],
                'file_counts': {},
                'total_size_mb': 0
            }
            
            # 历史下载器
            hist_path = self.data_root / f"historical_download/market_data/year_{year}"
            if hist_path.exists():
                files = list(hist_path.glob("*.csv"))
                if files:
                    size_mb = sum(f.stat().st_size for f in files) / (1024*1024)
                    year_data['sources'].append('historical')
                    year_data['file_counts']['historical'] = len(files)
                    year_data['total_size_mb'] += size_mb
            
            # 智能下载器
            smart_path = self.data_root / f"smart_download/year_{year}"
            if smart_path.exists():
                files = list(smart_path.glob("*.csv"))
                if files:
                    size_mb = sum(f.stat().st_size for f in files) / (1024*1024)
                    year_data['sources'].append('smart')
                    year_data['file_counts']['smart'] = len(files)
                    year_data['total_size_mb'] += size_mb
            
            if len(year_data['sources']) > 1:
                overlap_data.append(year_data)
        
        return overlap_data
    
    def _analyze_duplicate_content(self):
        """分析重复内容"""
        duplicate_info = {}
        
        # 抽样检查2024年数据的重复情况
        year = 2024
        
        sample_data = {}
        
        # 读取各下载器的样本数据
        sources = {
            'historical': self.data_root / f"historical_download/market_data/year_{year}/batch_001.csv",
            'smart': self.data_root / f"smart_download/year_{year}/batch_001.csv"
        }
        
        for source, file_path in sources.items():
            if file_path.exists():
                try:
                    df = pd.read_csv(file_path)
                    if 'ticker' in df.columns and 'tradeDate' in df.columns:
                        # 创建唯一键
                        df['key'] = df['ticker'].astype(str) + '_' + df['tradeDate'].astype(str)
                        sample_data[source] = set(df['key'].tolist())
                except:
                    continue
        
        # 计算重叠
        if len(sample_data) >= 2:
            sources = list(sample_data.keys())
            for i, s1 in enumerate(sources[:-1]):
                for s2 in sources[i+1:]:
                    overlap = sample_data[s1] & sample_data[s2]
                    total = sample_data[s1] | sample_data[s2]
                    overlap_ratio = len(overlap) / len(total) if len(total) > 0 else 0
                    
                    duplicate_info[f"{s1}_vs_{s2}"] = {
                        'overlap_records': len(overlap),
                        'total_unique': len(total),
                        'overlap_ratio': overlap_ratio
                    }
        
        return duplicate_info
    
    def _analyze_storage_efficiency(self):
        """分析存储效率"""
        size_info = {}
        
        # 分析各目录大小
        directories = [
            'historical_download',
            'smart_download',
            'priority_download'
        ]
        
        for dir_name in directories:
            dir_path = self.data_root / dir_name
            if dir_path.exists():
                total_size = 0
                file_count = 0
                
                for file_path in dir_path.rglob("*.csv"):
                    total_size += file_path.stat().st_size
                    file_count += 1
                
                size_info[dir_name] = {
                    'size_mb': total_size / (1024*1024),
                    'file_count': file_count,
                    'avg_file_size_mb': (total_size / file_count / (1024*1024)) if file_count > 0 else 0
                }
        
        return size_info
    
    def design_optimized_structure(self):
        """设计优化后的存储结构"""
        print("🎨 设计优化存储结构...")
        
        structure = {
            'unified': {
                'daily': {  # 日行情数据
                    'description': '统一的日行情数据，去重合并',
                    'structure': 'year_YYYY/YYYY_MM.parquet',
                    'source': 'merge historical + smart + priority daily'
                },
                'weekly': {  # 周行情数据
                    'description': '周行情数据',
                    'structure': 'weekly/YYYY.parquet',
                    'source': 'priority_download weekly'
                },
                'monthly': {  # 月行情数据
                    'description': '月行情数据',
                    'structure': 'monthly/YYYY.parquet',
                    'source': 'priority_download monthly'
                },
                'adjusted': {  # 复权数据
                    'description': '前复权行情数据',
                    'structure': 'adjusted/year_YYYY/YYYY_MM.parquet',
                    'source': 'priority_download adj_daily'
                },
                'factors': {  # 复权因子
                    'description': '复权因子数据',
                    'structure': 'factors/YYYY.parquet',
                    'source': 'priority_download adj_factor'
                },
                'flow': {  # 资金流向
                    'description': '个股和行业资金流向',
                    'structure': 'flow/stock/YYYY.parquet, flow/industry/YYYY.parquet',
                    'source': 'priority_download flow_data'
                }
            },
            'metadata': {
                'stocks': 'stock_info.parquet',
                'calendars': 'trading_calendar.parquet',
                'data_quality': 'quality_metrics.json'
            }
        }
        
        return structure
    
    def create_unified_daily_data(self, year_start=2000, year_end=2025):
        """创建统一的日行情数据"""
        print(f"🔄 创建统一日行情数据 ({year_start}-{year_end})...")
        
        # 创建目录
        unified_daily_dir = self.optimized_root / "daily"
        unified_daily_dir.mkdir(parents=True, exist_ok=True)
        
        success_count = 0
        
        for year in range(year_start, year_end + 1):
            print(f"\n📅 处理 {year} 年数据...")
            
            year_dir = unified_daily_dir / f"year_{year}"
            year_dir.mkdir(exist_ok=True)
            
            # 收集该年份所有数据源
            all_data = []
            
            # 1. 历史下载器数据
            hist_dir = self.data_root / f"historical_download/market_data/year_{year}"
            if hist_dir.exists():
                hist_files = list(hist_dir.glob("*.csv"))
                print(f"   📁 历史数据: {len(hist_files)} 文件")
                for file_path in hist_files:
                    try:
                        df = pd.read_csv(file_path)
                        if not df.empty:
                            df['source'] = 'historical'
                            all_data.append(df)
                    except:
                        continue
            
            # 2. 智能下载器数据
            smart_dir = self.data_root / f"smart_download/year_{year}"
            if smart_dir.exists():
                smart_files = list(smart_dir.glob("*.csv"))
                print(f"   📁 智能数据: {len(smart_files)} 文件")
                for file_path in smart_files:
                    try:
                        df = pd.read_csv(file_path)
                        if not df.empty:
                            df['source'] = 'smart'
                            all_data.append(df)
                    except:
                        continue
            
            # 3. 优先级下载器数据
            priority_files = list((self.data_root / "priority_download/market_data/daily").glob(f"{year}_batch_*.csv"))
            if priority_files:
                print(f"   📁 优先数据: {len(priority_files)} 文件")
                for file_path in priority_files:
                    try:
                        df = pd.read_csv(file_path)
                        if not df.empty:
                            df['source'] = 'priority'
                            all_data.append(df)
                    except:
                        continue
            
            if not all_data:
                print(f"   ⚠️ {year} 年无数据")
                continue
            
            # 合并数据
            combined_df = pd.concat(all_data, ignore_index=True)
            print(f"   📊 合并前: {len(combined_df):,} 条记录")
            
            # 数据清理和去重
            cleaned_df = self._clean_and_dedupe_data(combined_df)
            print(f"   ✅ 清理后: {len(cleaned_df):,} 条记录")
            
            if len(cleaned_df) > 0:
                # 按月分割存储
                self._save_monthly_data(cleaned_df, year_dir, year)
                success_count += 1
            
        print(f"\n🎉 统一日行情数据完成: {success_count}/{year_end-year_start+1} 年")
        return success_count > 0
    
    def _clean_and_dedupe_data(self, df):
        """清理和去重数据"""
        # 1. 基础清理
        if 'ticker' not in df.columns or 'tradeDate' not in df.columns:
            return df
        
        # 2. 删除异常价格数据
        price_cols = ['openPrice', 'highestPrice', 'lowestPrice', 'closePrice']
        for col in price_cols:
            if col in df.columns:
                df = df[df[col] > 0]  # 删除负价格和零价格
        
        # 3. 去重 - 优先级: priority > smart > historical
        df['priority'] = df['source'].map({'priority': 3, 'smart': 2, 'historical': 1})
        df = df.sort_values(['ticker', 'tradeDate', 'priority'], ascending=[True, True, False])
        df = df.drop_duplicates(['ticker', 'tradeDate'], keep='first')
        df = df.drop(['source', 'priority'], axis=1)
        
        return df
    
    def _save_monthly_data(self, df, year_dir, year):
        """按月保存数据"""
        if 'tradeDate' not in df.columns:
            # 如果没有日期列，直接保存整年数据
            df.to_parquet(year_dir / f"{year}_all.parquet", compression='snappy')
            return
        
        try:
            df['tradeDate'] = pd.to_datetime(df['tradeDate'])
            df['month'] = df['tradeDate'].dt.month
            
            for month in range(1, 13):
                monthly_data = df[df['month'] == month].drop('month', axis=1)
                if len(monthly_data) > 0:
                    file_path = year_dir / f"{year}_{month:02d}.parquet"
                    monthly_data.to_parquet(file_path, compression='snappy')
                    
        except:
            # 如果日期处理失败，保存整年数据
            df.to_parquet(year_dir / f"{year}_all.parquet", compression='snappy')
    
    def create_optimized_structure(self):
        """创建优化后的目录结构"""
        print("🏗️ 创建优化存储结构...")
        
        # 创建主要目录
        directories = [
            'optimized/daily',
            'optimized/weekly', 
            'optimized/monthly',
            'optimized/adjusted',
            'optimized/factors',
            'optimized/flow/stock',
            'optimized/flow/industry',
            'optimized/metadata',
            'backup/original'
        ]
        
        for dir_path in directories:
            (self.data_root / dir_path).mkdir(parents=True, exist_ok=True)
        
        # 复制其他类型数据
        self._migrate_other_data()
        
        return True
    
    def _migrate_other_data(self):
        """迁移其他类型数据"""
        print("   📦 迁移其他数据类型...")
        
        # 1. 迁移周行情数据
        weekly_source = self.data_root / "priority_download/market_data/weekly"
        weekly_target = self.optimized_root / "weekly"
        if weekly_source.exists():
            self._migrate_and_compress(weekly_source, weekly_target, "weekly")
        
        # 2. 迁移月行情数据
        monthly_source = self.data_root / "priority_download/market_data/monthly"
        monthly_target = self.optimized_root / "monthly"
        if monthly_source.exists():
            self._migrate_and_compress(monthly_source, monthly_target, "monthly")
        
        # 3. 迁移复权数据
        adj_source = self.data_root / "priority_download/market_data/adj_daily"
        adj_target = self.optimized_root / "adjusted"
        if adj_source.exists():
            self._migrate_and_compress(adj_source, adj_target, "adjusted")
        
        # 4. 迁移复权因子
        factor_source = self.data_root / "priority_download/market_data/adj_factor"
        factor_target = self.optimized_root / "factors"
        if factor_source.exists():
            self._migrate_and_compress(factor_source, factor_target, "factors")
        
        # 5. 迁移资金流向数据
        flow_source = self.data_root / "priority_download/flow_data"
        flow_target = self.optimized_root / "flow"
        if flow_source.exists():
            self._migrate_flow_data(flow_source, flow_target)
    
    def _migrate_and_compress(self, source_dir, target_dir, data_type):
        """迁移并压缩数据"""
        csv_files = list(source_dir.rglob("*.csv"))
        if not csv_files:
            return
        
        print(f"      {data_type}: {len(csv_files)} 文件")
        
        # 按年份归类
        year_data = {}
        for file_path in csv_files:
            year = None
            for y in range(2000, 2026):
                if str(y) in file_path.name:
                    year = y
                    break
            
            if year:
                if year not in year_data:
                    year_data[year] = []
                year_data[year].append(file_path)
        
        # 合并并压缩
        for year, files in year_data.items():
            all_data = []
            for file_path in files:
                try:
                    df = pd.read_csv(file_path)
                    if not df.empty:
                        all_data.append(df)
                except:
                    continue
            
            if all_data:
                combined_df = pd.concat(all_data, ignore_index=True)
                combined_df = combined_df.drop_duplicates()
                
                output_file = target_dir / f"{year}.parquet"
                combined_df.to_parquet(output_file, compression='snappy')
    
    def _migrate_flow_data(self, source_dir, target_dir):
        """迁移资金流向数据"""
        # 个股流向
        stock_flow_dir = source_dir / "stock_flow"
        if stock_flow_dir.exists():
            target_stock_dir = target_dir / "stock"
            self._migrate_and_compress(stock_flow_dir, target_stock_dir, "stock_flow")
        
        # 行业流向
        industry_flow_dir = source_dir / "industry_flow"
        if industry_flow_dir.exists():
            target_industry_dir = target_dir / "industry"
            self._migrate_and_compress(industry_flow_dir, target_industry_dir, "industry_flow")
    
    def generate_optimization_report(self):
        """生成优化报告"""
        print("📋 生成优化报告...")
        
        # 分析冗余
        redundancy = self.analyze_redundancy()
        
        # 统计原始大小
        original_size = 0
        original_files = 0
        for file_path in self.data_root.rglob("*.csv"):
            if "optimized" not in str(file_path) and "backup" not in str(file_path):
                original_size += file_path.stat().st_size
                original_files += 1
        
        # 统计优化后大小
        optimized_size = 0
        optimized_files = 0
        if self.optimized_root.exists():
            for file_path in self.optimized_root.rglob("*"):
                if file_path.is_file():
                    optimized_size += file_path.stat().st_size
                    optimized_files += 1
        
        report = {
            'analysis_time': datetime.now().isoformat(),
            'redundancy_analysis': redundancy,
            'size_comparison': {
                'original': {
                    'size_mb': original_size / (1024*1024),
                    'files': original_files
                },
                'optimized': {
                    'size_mb': optimized_size / (1024*1024),
                    'files': optimized_files
                },
                'savings': {
                    'size_mb': (original_size - optimized_size) / (1024*1024),
                    'percentage': ((original_size - optimized_size) / original_size * 100) if original_size > 0 else 0
                }
            }
        }
        
        # 保存报告
        report_file = self.data_root / 'optimization_report.json'
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        return report
    
    def print_optimization_summary(self, report):
        """打印优化摘要"""
        print("\n" + "="*60)
        print("📊 数据优化摘要")
        print("="*60)
        
        # 冗余分析
        overlap_years = len(report['redundancy_analysis']['overlapping_years'])
        print(f"🔍 数据重叠: {overlap_years} 个年份存在冗余")
        
        # 存储效率
        size_comp = report['size_comparison']
        original_gb = size_comp['original']['size_mb'] / 1024
        optimized_gb = size_comp['optimized']['size_mb'] / 1024
        savings_gb = size_comp['savings']['size_mb'] / 1024
        savings_pct = size_comp['savings']['percentage']
        
        print(f"💾 存储优化:")
        print(f"   原始大小: {original_gb:.2f} GB ({size_comp['original']['files']} 文件)")
        print(f"   优化大小: {optimized_gb:.2f} GB ({size_comp['optimized']['files']} 文件)")
        print(f"   节省空间: {savings_gb:.2f} GB ({savings_pct:.1f}%)")
        
        # 重复数据分析
        duplicate_info = report['redundancy_analysis']['duplicate_data']
        if duplicate_info:
            print(f"\n🔄 数据重复分析:")
            for comparison, info in duplicate_info.items():
                overlap_pct = info['overlap_ratio'] * 100
                print(f"   {comparison}: {overlap_pct:.1f}% 重复")

def main():
    """主函数"""
    optimizer = DataOptimizer()
    
    # 1. 分析现状
    print("🚀 开始数据优化整合...\n")
    
    # 2. 创建优化结构
    optimizer.create_optimized_structure()
    
    # 3. 创建统一日行情数据
    optimizer.create_unified_daily_data()
    
    # 4. 生成报告
    report = optimizer.generate_optimization_report()
    optimizer.print_optimization_summary(report)

if __name__ == "__main__":
    main()