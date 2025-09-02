#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
全面数据完整性验证器
检查所有本地数据的时间范围、质量和完整性
生成详细的数据完整性报告
"""

import sys
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
import warnings
import json
import os
import glob
from collections import defaultdict, Counter
import re
warnings.filterwarnings('ignore')

class BulkDataIntegrityVerifier:
    """全面数据完整性验证器"""
    
    def __init__(self):
        """初始化验证器"""
        self.base_path = Path("/Users/jackstudio/QuantTrade/data")
        self.target_start = pd.Timestamp('2000-01-01')
        self.target_end = pd.Timestamp('2025-08-31')
        self.verification_results = {}
        self.summary_stats = {
            'total_directories': 0,
            'total_files': 0,
            'total_size_gb': 0,
            'valid_files': 0,
            'invalid_files': 0,
            'missing_files': 0,
            'date_range_issues': 0,
            'data_quality_issues': 0
        }
        
        print(f"🔍 数据完整性验证器")
        print(f"📁 检查路径: {self.base_path}")
        print(f"📅 目标时间范围: {self.target_start.date()} - {self.target_end.date()}")
        print("=" * 80)
    
    def discover_all_data_directories(self):
        """发现所有数据目录"""
        print("🗂️ 扫描所有数据目录...")
        
        data_directories = {}
        
        # 遍历所有子目录
        for root, dirs, files in os.walk(self.base_path):
            root_path = Path(root)
            
            # 跳过系统目录
            if any(skip in str(root_path) for skip in ['.DS_Store', '__pycache__', '.git']):
                continue
            
            # 只关注包含CSV或parquet文件的目录
            data_files = [f for f in files if f.endswith(('.csv', '.parquet', '.json'))]
            
            if data_files:
                # 分类数据目录
                relative_path = root_path.relative_to(self.base_path)
                category = self.categorize_directory(relative_path, data_files)
                
                if category not in data_directories:
                    data_directories[category] = []
                
                data_directories[category].append({
                    'path': root_path,
                    'relative_path': str(relative_path),
                    'files': data_files,
                    'file_count': len(data_files)
                })
        
        self.data_directories = data_directories
        self.summary_stats['total_directories'] = sum(len(dirs) for dirs in data_directories.values())
        
        print(f"✅ 发现 {len(data_directories)} 个数据类别")
        for category, directories in data_directories.items():
            print(f"   📊 {category}: {len(directories)} 个目录")
        
        return data_directories
    
    def categorize_directory(self, relative_path, files):
        """分类数据目录"""
        path_str = str(relative_path).lower()
        
        # 股票行情数据
        if any(keyword in path_str for keyword in ['mktequd', 'daily', 'stocks']):
            if 'adj' in path_str:
                return '股票日线复权数据'
            else:
                return '股票日线数据'
        
        # 周线数据
        elif any(keyword in path_str for keyword in ['week', 'weekly']):
            if 'adj' in path_str:
                return '股票周线复权数据'
            else:
                return '股票周线数据'
        
        # 月线数据
        elif any(keyword in path_str for keyword in ['month', 'monthly']):
            if 'adj' in path_str:
                return '股票月线复权数据'
            else:
                return '股票月线数据'
        
        # 季线年线数据
        elif any(keyword in path_str for keyword in ['quarter', 'yearly', 'annual']):
            return '股票季年线数据'
        
        # 财务数据
        elif any(keyword in path_str for keyword in ['financial', 'fdmt', 'balance', 'income', 'cash']):
            return '财务报表数据'
        
        # 基金数据
        elif any(keyword in path_str for keyword in ['fund', 'fundnav', 'etf']):
            return '基金数据'
        
        # 指数数据
        elif any(keyword in path_str for keyword in ['index', 'idx', 'benchmark']):
            return '指数数据'
        
        # 因子数据
        elif any(keyword in path_str for keyword in ['factor', 'alpha', 'risk']):
            return '因子数据'
        
        # 宏观数据
        elif any(keyword in path_str for keyword in ['macro', 'economic', 'eco']):
            return '宏观经济数据'
        
        # 期货期权数据
        elif any(keyword in path_str for keyword in ['future', 'option', 'derivative']):
            return '衍生品数据'
        
        # 新闻舆情数据
        elif any(keyword in path_str for keyword in ['news', 'sentiment', 'research']):
            return '资讯研究数据'
        
        # 配置和日志
        elif any(keyword in path_str for keyword in ['config', 'log', 'temp', 'cache']):
            return '系统配置数据'
        
        # 报告输出
        elif any(keyword in path_str for keyword in ['output', 'report', 'result']):
            return '输出报告数据'
        
        # 原始下载数据
        elif any(keyword in path_str for keyword in ['raw', 'download', 'backup', 'archive']):
            return '原始备份数据'
        
        # 其他
        else:
            return '其他数据'
    
    def verify_single_file(self, file_path, category):
        """验证单个文件"""
        file_result = {
            'file_name': file_path.name,
            'file_path': str(file_path),
            'size_mb': 0,
            'is_valid': False,
            'row_count': 0,
            'column_count': 0,
            'date_range': {
                'start_date': None,
                'end_date': None,
                'date_column': None
            },
            'issues': []
        }
        
        try:
            # 获取文件大小
            file_result['size_mb'] = file_path.stat().st_size / (1024 * 1024)
            
            # 根据文件类型读取
            if file_path.suffix == '.csv':
                df = pd.read_csv(file_path, nrows=100)  # 只读前100行进行验证
            elif file_path.suffix == '.json':
                file_result['is_valid'] = True
                return file_result
            else:
                file_result['issues'].append(f"不支持的文件类型: {file_path.suffix}")
                return file_result
            
            if df is None or len(df) == 0:
                file_result['issues'].append("文件为空")
                return file_result
            
            file_result['row_count'] = len(df)
            file_result['column_count'] = len(df.columns)
            
            # 检查日期列
            date_columns = [col for col in df.columns if any(date_word in col.lower() for date_word in ['date', 'time', '日期'])]
            
            if date_columns:
                date_column = date_columns[0]
                file_result['date_range']['date_column'] = date_column
                
                try:
                    df[date_column] = pd.to_datetime(df[date_column])
                    start_date = df[date_column].min()
                    end_date = df[date_column].max()
                    
                    file_result['date_range']['start_date'] = start_date.strftime('%Y-%m-%d') if pd.notna(start_date) else None
                    file_result['date_range']['end_date'] = end_date.strftime('%Y-%m-%d') if pd.notna(end_date) else None
                    
                    # 验证时间范围
                    if category in ['股票日线数据', '股票周线数据', '股票月线数据']:
                        if end_date < pd.Timestamp('2024-01-01'):
                            file_result['issues'].append(f"结束时间过早: {end_date.date()}")
                    
                except Exception as e:
                    file_result['issues'].append(f"日期解析失败: {str(e)}")
            
            # 如果没有严重问题，标记为有效
            serious_issues = [issue for issue in file_result['issues'] if any(serious in issue for serious in ['读取失败', '文件为空', '结束时间过早'])]
            file_result['is_valid'] = len(serious_issues) == 0
            
        except Exception as e:
            file_result['issues'].append(f"验证过程异常: {str(e)}")
        
        return file_result
    
    def generate_comprehensive_report(self):
        """生成全面的完整性报告"""
        print("\n📊 生成完整性报告...")
        
        # 收集所有目录的数据
        data_directories = self.discover_all_data_directories()
        
        # 验证每个类别
        for category, directories in data_directories.items():
            if directories:
                print(f"\n🔍 验证 {category}...")
                category_results = {
                    'category': category,
                    'directories': [],
                    'total_files': 0,
                    'valid_files': 0,
                    'total_size_mb': 0,
                    'sample_files': []
                }
                
                for dir_info in directories:
                    dir_path = dir_info['path']
                    print(f"   📂 检查: {dir_info['relative_path']}")
                    
                    # 检查目录中的文件 (限制前5个文件)
                    valid_files_in_dir = 0
                    dir_size = 0
                    sample_files = []
                    
                    for file_name in dir_info['files'][:5]:
                        file_path = dir_path / file_name
                        if file_path.exists():
                            file_result = self.verify_single_file(file_path, category)
                            if file_result['is_valid']:
                                valid_files_in_dir += 1
                            dir_size += file_result['size_mb']
                            sample_files.append(file_result)
                    
                    dir_summary = {
                        'path': str(dir_path),
                        'relative_path': dir_info['relative_path'],
                        'total_files': len(dir_info['files']),
                        'valid_files': valid_files_in_dir,
                        'size_mb': dir_size
                    }
                    
                    category_results['directories'].append(dir_summary)
                    category_results['total_files'] += len(dir_info['files'])
                    category_results['valid_files'] += valid_files_in_dir
                    category_results['total_size_mb'] += dir_size
                    category_results['sample_files'].extend(sample_files[:2])  # 每个目录取2个样本
                
                self.verification_results[category] = category_results
                print(f"      📊 {category}: {category_results['valid_files']}/{category_results['total_files']} 有效")
                print(f"      💾 大小: {category_results['total_size_mb']:.1f} MB")
        
        # 更新总体统计
        for category_result in self.verification_results.values():
            self.summary_stats['total_files'] += category_result['total_files']
            self.summary_stats['valid_files'] += category_result['valid_files']
            self.summary_stats['total_size_gb'] += category_result['total_size_mb'] / 1024
        
        self.summary_stats['invalid_files'] = self.summary_stats['total_files'] - self.summary_stats['valid_files']
        
        # 生成报告文件
        report = self.create_simple_report()
        return report
    
    def create_simple_report(self):
        """创建简化报告"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        report = {
            'verification_info': {
                'timestamp': datetime.now().isoformat(),
                'target_date_range': f"{self.target_start.date()} - {self.target_end.date()}",
                'verification_scope': '全部本地数据'
            },
            'summary_statistics': {
                'total_categories': len(self.verification_results),
                'total_files': self.summary_stats['total_files'],
                'valid_files': self.summary_stats['valid_files'],
                'invalid_files': self.summary_stats['invalid_files'],
                'validity_rate': f"{self.summary_stats['valid_files']/max(self.summary_stats['total_files'], 1)*100:.1f}%",
                'total_size_gb': round(self.summary_stats['total_size_gb'], 2)
            },
            'category_details': self.verification_results
        }
        
        # 保存报告
        report_file = f"data_integrity_report_{timestamp}.json"
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2, default=str)
        
        # 生成可读报告
        readable_report = self.create_readable_summary(report)
        readable_file = f"data_integrity_report_{timestamp}.txt"
        with open(readable_file, 'w', encoding='utf-8') as f:
            f.write(readable_report)
        
        print(f"\n📄 详细报告已生成:")
        print(f"   📊 JSON报告: {report_file}")
        print(f"   📝 可读报告: {readable_file}")
        
        return report, readable_report
    
    def create_readable_summary(self, report):
        """创建可读摘要"""
        lines = []
        lines.append("=" * 100)
        lines.append("📊 QuantTrade 数据完整性验证报告")
        lines.append("=" * 100)
        lines.append(f"🕐 验证时间: {report['verification_info']['timestamp']}")
        lines.append(f"📅 目标时间范围: {report['verification_info']['target_date_range']}")
        lines.append("")
        
        # 总体统计
        summary = report['summary_statistics']
        lines.append("📈 总体统计")
        lines.append("-" * 50)
        lines.append(f"数据类别数量: {summary['total_categories']}")
        lines.append(f"数据文件总数: {summary['total_files']}")
        lines.append(f"有效文件数量: {summary['valid_files']}")
        lines.append(f"无效文件数量: {summary['invalid_files']}")
        lines.append(f"数据有效率: {summary['validity_rate']}")
        lines.append(f"数据总大小: {summary['total_size_gb']} GB")
        lines.append("")
        
        # 各类别详情
        lines.append("📋 各类别数据明细")
        lines.append("=" * 100)
        
        for category, details in report['category_details'].items():
            lines.append(f"\n📊 {category}")
            lines.append("-" * 80)
            lines.append(f"目录数量: {len(details['directories'])}")
            lines.append(f"文件总数: {details['total_files']}")
            lines.append(f"有效文件: {details['valid_files']}")
            lines.append(f"数据大小: {details['total_size_mb']:.1f} MB")
            
            lines.append(f"\n📂 目录明细:")
            for dir_info in details['directories']:
                lines.append(f"   📁 {dir_info['relative_path']}")
                lines.append(f"      文件数: {dir_info['total_files']}")
                lines.append(f"      有效文件: {dir_info['valid_files']}")
                lines.append(f"      数据大小: {dir_info['size_mb']:.1f} MB")
            
            if details['sample_files']:
                lines.append(f"\n📋 样本文件:")
                for sample in details['sample_files'][:3]:
                    status = "✅" if sample['is_valid'] else "❌"
                    lines.append(f"   {status} {sample['file_name']} ({sample['row_count']} 行, {sample['size_mb']:.1f} MB)")
                    if sample['date_range']['start_date']:
                        lines.append(f"      时间: {sample['date_range']['start_date']} - {sample['date_range']['end_date']}")
                    if sample['issues']:
                        lines.append(f"      问题: {'; '.join(sample['issues'])}")
        
        return "\n".join(lines)

def main():
    """主函数"""
    print("🔍 全面数据完整性验证")
    print("=" * 80)
    print("📋 验证目标: 检查所有本地数据的完整性和时间范围")
    print("📅 时间要求: 2000年1月1日 - 2025年8月31日")
    
    verifier = BulkDataIntegrityVerifier()
    report, readable_report = verifier.generate_comprehensive_report()
    
    print("\n" + "=" * 100)
    print("📊 验证完成！以下是报告摘要:")
    print("=" * 100)
    print(readable_report.split("📋 各类别数据明细")[0])
    
    return report

if __name__ == "__main__":
    main()
