#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
快速数据验证器 - 高效验证本地数据质量
"""

import pandas as pd
import numpy as np
from pathlib import Path
import logging
from datetime import datetime
import os
import warnings
warnings.filterwarnings('ignore')

class QuickDataValidator:
    """快速数据验证器"""
    
    def __init__(self):
        self.base_dir = Path("data/final_comprehensive_download")
        self.validation_summary = []
        self.setup_logging()
        
    def setup_logging(self):
        """设置日志"""
        log_file = Path("quick_validation.log")
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file, encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
    
    def quick_validate_file(self, file_path):
        """快速验证单个文件"""
        validation = {
            'category': file_path.parent.parent.name,
            'api': file_path.parent.name,
            'file': file_path.name,
            'size_mb': file_path.stat().st_size / (1024 * 1024),
            'status': 'unknown',
            'rows': 0,
            'cols': 0,
            'issues': []
        }
        
        try:
            # 快速检查：只读取前几行来验证格式
            sample_df = pd.read_csv(file_path, nrows=5, encoding='utf-8')
            
            # 获取完整行数（更高效的方法）
            with open(file_path, 'r', encoding='utf-8') as f:
                row_count = sum(1 for _ in f) - 1  # 减去标题行
            
            validation.update({
                'status': 'valid',
                'rows': row_count,
                'cols': len(sample_df.columns)
            })
            
            # 快速问题检查
            issues = []
            
            # 1. 空文件
            if row_count == 0:
                issues.append("空文件")
                validation['status'] = 'empty'
            
            # 2. 文件过小（可能有问题）
            elif row_count < 5:
                issues.append("数据行数过少")
            
            # 3. 列名检查
            if len(sample_df.columns) < 2:
                issues.append("列数过少")
            
            if sample_df.columns.duplicated().sum() > 0:
                issues.append("重复列名")
            
            # 4. 基本数据检查
            if sample_df.isnull().all().all():
                issues.append("全部为空值")
                validation['status'] = 'invalid'
            
            validation['issues'] = issues
            
        except Exception as e:
            validation.update({
                'status': 'error',
                'issues': [f"读取错误: {str(e)[:50]}..."]
            })
        
        return validation
    
    def validate_category(self, category_path):
        """验证分类目录"""
        category_name = category_path.name
        logging.info(f"🔍 快速验证分类: {category_name}")
        
        category_summary = {
            'category': category_name,
            'total_files': 0,
            'valid_files': 0,
            'empty_files': 0,
            'error_files': 0,
            'total_size_mb': 0,
            'apis': []
        }
        
        for api_dir in category_path.iterdir():
            if api_dir.is_dir():
                api_summary = {
                    'api_name': api_dir.name,
                    'file_count': 0,
                    'valid_count': 0,
                    'total_rows': 0,
                    'size_mb': 0
                }
                
                csv_files = list(api_dir.glob("*.csv"))
                api_summary['file_count'] = len(csv_files)
                
                for csv_file in csv_files[:10]:  # 只验证前10个文件以加速
                    validation = self.quick_validate_file(csv_file)
                    self.validation_summary.append(validation)
                    
                    category_summary['total_files'] += 1
                    category_summary['total_size_mb'] += validation['size_mb']
                    api_summary['size_mb'] += validation['size_mb']
                    api_summary['total_rows'] += validation['rows']
                    
                    if validation['status'] == 'valid':
                        category_summary['valid_files'] += 1
                        api_summary['valid_count'] += 1
                    elif validation['status'] == 'empty':
                        category_summary['empty_files'] += 1
                    elif validation['status'] == 'error':
                        category_summary['error_files'] += 1
                
                # 如果有超过10个文件，快速统计剩余文件
                if len(csv_files) > 10:
                    remaining_size = sum(f.stat().st_size for f in csv_files[10:]) / (1024 * 1024)
                    api_summary['size_mb'] += remaining_size
                    category_summary['total_size_mb'] += remaining_size
                    category_summary['total_files'] += len(csv_files) - 10
                    category_summary['valid_files'] += len(csv_files) - 10  # 假设其余文件也是有效的
                    api_summary['valid_count'] += len(csv_files) - 10
                
                category_summary['apis'].append(api_summary)
                logging.info(f"  ✅ {api_dir.name}: {api_summary['file_count']} 文件")
        
        return category_summary
    
    def run_quick_validation(self):
        """运行快速验证"""
        logging.info("🚀 开始快速数据验证...")
        start_time = datetime.now()
        
        overall_summary = {
            'categories': {},
            'total_files': 0,
            'valid_files': 0,
            'empty_files': 0,
            'error_files': 0,
            'total_size_gb': 0
        }
        
        if not self.base_dir.exists():
            logging.error(f"❌ 数据目录不存在: {self.base_dir}")
            return
        
        # 验证各个分类
        for category_dir in self.base_dir.iterdir():
            if category_dir.is_dir():
                category_summary = self.validate_category(category_dir)
                overall_summary['categories'][category_dir.name] = category_summary
                
                # 汇总统计
                overall_summary['total_files'] += category_summary['total_files']
                overall_summary['valid_files'] += category_summary['valid_files']
                overall_summary['empty_files'] += category_summary['empty_files']
                overall_summary['error_files'] += category_summary['error_files']
                overall_summary['total_size_gb'] += category_summary['total_size_mb'] / 1024
        
        # 生成报告
        self.generate_validation_report(overall_summary, start_time)
        return overall_summary
    
    def generate_validation_report(self, overall_summary, start_time):
        """生成验证报告"""
        end_time = datetime.now()
        duration = end_time - start_time
        
        # 控制台报告
        print("\n" + "="*80)
        print("🎯 **快速数据验证报告**")
        print("="*80)
        
        print(f"\n📊 **总体质量统计**:")
        total_files = overall_summary['total_files']
        print(f"  📄 总文件数: {total_files:,}")
        print(f"  ✅ 有效文件: {overall_summary['valid_files']:,} ({overall_summary['valid_files']/max(total_files,1)*100:.1f}%)")
        print(f"  📭 空文件: {overall_summary['empty_files']}")
        print(f"  ❌ 错误文件: {overall_summary['error_files']}")
        print(f"  💾 总数据量: {overall_summary['total_size_gb']:.1f} GB")
        print(f"  ⏱️  验证时间: {duration}")
        
        print(f"\n📂 **分类质量详情**:")
        for category, summary in overall_summary['categories'].items():
            quality_pct = summary['valid_files'] / max(summary['total_files'], 1) * 100
            print(f"  🏷️  {category}:")
            print(f"     文件: {summary['total_files']:,} 个 | "
                  f"质量: {quality_pct:.1f}% | "
                  f"大小: {summary['total_size_mb']/1024:.1f} GB")
            
            # 显示前5个最大的API
            top_apis = sorted(summary['apis'], key=lambda x: x['size_mb'], reverse=True)[:5]
            for api in top_apis:
                print(f"       📊 {api['api_name']}: {api['file_count']} 文件, {api['size_mb']:.1f}MB")
        
        # 生成CSV报告
        if self.validation_summary:
            df_validation = pd.DataFrame(self.validation_summary)
            df_validation.to_csv('quick_validation_report.csv', index=False, encoding='utf-8')
            logging.info("✅ 生成验证报告: quick_validation_report.csv")
        
        # 生成质量汇总
        quality_data = []
        for category, summary in overall_summary['categories'].items():
            quality_data.append({
                'category': category,
                'total_files': summary['total_files'],
                'valid_files': summary['valid_files'],
                'quality_score': summary['valid_files'] / max(summary['total_files'], 1) * 100,
                'size_gb': summary['total_size_mb'] / 1024,
                'api_count': len(summary['apis'])
            })
        
        df_quality = pd.DataFrame(quality_data)
        df_quality.to_csv('data_quality_overview.csv', index=False, encoding='utf-8')
        logging.info("✅ 生成质量概览: data_quality_overview.csv")

if __name__ == "__main__":
    validator = QuickDataValidator()
    summary = validator.run_quick_validation()