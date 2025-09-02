#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据清单分析器 - 统计现有API接口数量和数据分布
"""

import pandas as pd
from pathlib import Path
import logging
from datetime import datetime
import os

class DataInventoryAnalyzer:
    """数据清单分析器"""
    
    def __init__(self):
        self.base_dir = Path("data/final_comprehensive_download")
        self.setup_logging()
        
    def setup_logging(self):
        """设置日志"""
        log_file = Path("data_inventory_analysis.log")
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file, encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
    
    def scan_directory_structure(self):
        """扫描目录结构"""
        logging.info("🔍 开始扫描数据目录结构...")
        
        structure = {}
        total_apis = 0
        total_files = 0
        total_size = 0
        
        if not self.base_dir.exists():
            logging.error(f"❌ 数据目录不存在: {self.base_dir}")
            return structure
        
        # 扫描各个分类目录
        for category_dir in self.base_dir.iterdir():
            if category_dir.is_dir():
                category_name = category_dir.name
                structure[category_name] = {
                    'apis': {},
                    'api_count': 0,
                    'file_count': 0,
                    'total_size': 0
                }
                
                logging.info(f"📂 扫描分类: {category_name}")
                
                # 扫描API目录
                for api_dir in category_dir.iterdir():
                    if api_dir.is_dir():
                        api_name = api_dir.name
                        csv_files = list(api_dir.glob("*.csv"))
                        
                        # 计算文件大小
                        api_size = sum(f.stat().st_size for f in csv_files)
                        
                        structure[category_name]['apis'][api_name] = {
                            'file_count': len(csv_files),
                            'size_mb': api_size / (1024 * 1024),
                            'files': [f.name for f in csv_files]
                        }
                        
                        structure[category_name]['api_count'] += 1
                        structure[category_name]['file_count'] += len(csv_files)
                        structure[category_name]['total_size'] += api_size
                        
                        total_apis += 1
                        total_files += len(csv_files)
                        total_size += api_size
                        
                        logging.info(f"  ✅ {api_name}: {len(csv_files)} 文件, {api_size/(1024*1024):.1f}MB")
        
        # 总计信息
        structure['_summary'] = {
            'total_categories': len([k for k in structure.keys() if not k.startswith('_')]),
            'total_apis': total_apis,
            'total_files': total_files,
            'total_size_mb': total_size / (1024 * 1024),
            'total_size_gb': total_size / (1024 * 1024 * 1024)
        }
        
        return structure
    
    def analyze_data_coverage(self):
        """分析数据覆盖情况"""
        logging.info("📊 分析数据覆盖情况...")
        
        coverage_analysis = {
            'by_category': {},
            'by_time_range': {},
            'by_data_type': {}
        }
        
        # 按分类分析
        for category_dir in self.base_dir.iterdir():
            if category_dir.is_dir():
                category_name = category_dir.name
                api_list = []
                
                for api_dir in category_dir.iterdir():
                    if api_dir.is_dir():
                        csv_files = list(api_dir.glob("*.csv"))
                        if csv_files:
                            api_list.append(api_dir.name)
                
                coverage_analysis['by_category'][category_name] = {
                    'api_count': len(api_list),
                    'api_list': sorted(api_list)
                }
        
        return coverage_analysis
    
    def generate_inventory_report(self):
        """生成数据清单报告"""
        logging.info("📝 生成数据清单报告...")
        
        structure = self.scan_directory_structure()
        coverage = self.analyze_data_coverage()
        
        # 输出控制台报告
        self.print_console_report(structure, coverage)
        
        # 生成详细CSV报告
        self.generate_csv_reports(structure, coverage)
        
        return structure, coverage
    
    def print_console_report(self, structure, coverage):
        """打印控制台报告"""
        print("\n" + "="*80)
        print("🎯 **QuantTrade 数据清单统计报告**")
        print("="*80)
        
        # 总体统计
        summary = structure.get('_summary', {})
        print(f"\n📊 **总体统计**:")
        print(f"  📁 数据分类: {summary.get('total_categories', 0)} 个")
        print(f"  🔌 API接口: {summary.get('total_apis', 0)} 个")
        print(f"  📄 数据文件: {summary.get('total_files', 0)} 个")
        print(f"  💾 数据大小: {summary.get('total_size_gb', 0):.2f} GB ({summary.get('total_size_mb', 0):.1f} MB)")
        
        # 按分类统计
        print(f"\n📂 **按分类统计**:")
        for category, info in structure.items():
            if not category.startswith('_'):
                print(f"  🏷️  {category}:")
                print(f"     - API数量: {info['api_count']} 个")
                print(f"     - 文件数量: {info['file_count']} 个")
                print(f"     - 数据大小: {info['total_size']/(1024*1024):.1f} MB")
        
        # API详细信息
        print(f"\n🔍 **API详细信息**:")
        for category, info in structure.items():
            if not category.startswith('_') and info['apis']:
                print(f"\n  📁 {category} ({info['api_count']} APIs):")
                for api_name, api_info in info['apis'].items():
                    print(f"    ✅ {api_name}: {api_info['file_count']} 文件, {api_info['size_mb']:.1f}MB")
    
    def generate_csv_reports(self, structure, coverage):
        """生成CSV详细报告"""
        
        # 1. API清单报告
        api_inventory = []
        for category, info in structure.items():
            if not category.startswith('_'):
                for api_name, api_info in info['apis'].items():
                    api_inventory.append({
                        'category': category,
                        'api_name': api_name,
                        'file_count': api_info['file_count'],
                        'size_mb': api_info['size_mb'],
                        'files': '; '.join(api_info['files'][:5]) + ('...' if len(api_info['files']) > 5 else '')
                    })
        
        df_inventory = pd.DataFrame(api_inventory)
        df_inventory.to_csv('data_api_inventory.csv', index=False, encoding='utf-8')
        logging.info("✅ 生成API清单报告: data_api_inventory.csv")
        
        # 2. 分类汇总报告
        category_summary = []
        for category, info in structure.items():
            if not category.startswith('_'):
                category_summary.append({
                    'category': category,
                    'api_count': info['api_count'],
                    'file_count': info['file_count'],
                    'size_mb': info['total_size'] / (1024 * 1024),
                    'completeness': f"{info['api_count']}/{info['api_count']}" if info['api_count'] > 0 else "0/0"
                })
        
        df_summary = pd.DataFrame(category_summary)
        df_summary.to_csv('data_category_summary.csv', index=False, encoding='utf-8')
        logging.info("✅ 生成分类汇总报告: data_category_summary.csv")

if __name__ == "__main__":
    analyzer = DataInventoryAnalyzer()
    structure, coverage = analyzer.generate_inventory_report()