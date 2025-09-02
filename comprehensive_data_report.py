#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
综合数据质量报告生成器 - 生成完整的数据现状报告
"""

import pandas as pd
import numpy as np
from pathlib import Path
import logging
from datetime import datetime
import json

class ComprehensiveDataReporter:
    """综合数据质量报告生成器"""
    
    def __init__(self):
        self.base_dir = Path("data/final_comprehensive_download")
        self.setup_logging()
        
    def setup_logging(self):
        """设置日志"""
        log_file = Path("comprehensive_report.log")
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file, encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
    
    def load_existing_reports(self):
        """加载已有的报告数据"""
        reports = {}
        
        # 加载API清单
        if Path('data_api_inventory.csv').exists():
            reports['inventory'] = pd.read_csv('data_api_inventory.csv')
        
        # 加载质量概览
        if Path('data_quality_overview.csv').exists():
            reports['quality'] = pd.read_csv('data_quality_overview.csv')
            
        # 加载验证报告
        if Path('quick_validation_report.csv').exists():
            reports['validation'] = pd.read_csv('quick_validation_report.csv')
        
        return reports
    
    def analyze_data_coverage(self):
        """分析数据覆盖范围"""
        coverage_analysis = {
            'time_coverage': {},
            'stock_coverage': {},
            'data_completeness': {}
        }
        
        # 分析时间覆盖
        for category_dir in self.base_dir.iterdir():
            if category_dir.is_dir():
                category_name = category_dir.name
                category_files = []
                
                for api_dir in category_dir.iterdir():
                    if api_dir.is_dir():
                        csv_files = list(api_dir.glob("*.csv"))
                        for csv_file in csv_files[:3]:  # 采样分析
                            try:
                                df_sample = pd.read_csv(csv_file, nrows=100)
                                
                                # 寻找日期列
                                date_columns = []
                                for col in df_sample.columns:
                                    if any(date_word in col.lower() for date_word in ['date', 'time', 'year']):
                                        date_columns.append(col)
                                
                                if date_columns:
                                    category_files.append({
                                        'api': api_dir.name,
                                        'file': csv_file.name,
                                        'date_columns': date_columns,
                                        'row_count': len(df_sample)
                                    })
                            except:
                                continue
                
                coverage_analysis['time_coverage'][category_name] = category_files
        
        return coverage_analysis
    
    def calculate_api_completeness(self):
        """计算API完整性"""
        
        # 定义期望的API分类和数量（基于业务需求）
        expected_apis = {
            'basic_info': {
                'expected': 10,  # 基础信息类API
                'critical': ['secidget', 'equget', 'equindustryget']
            },
            'financial': {
                'expected': 15,  # 财务数据API  
                'critical': ['fdmtisalllatestget', 'fdmtbsalllatestget', 'fdmtcfalllatestget']
            },
            'special_trading': {
                'expected': 20,  # 特殊交易数据API
                'critical': ['mktlimitget', 'fstdetailget', 'mktblockdget']
            },
            'governance': {
                'expected': 25,  # 公司治理API
                'critical': ['equshtenget', 'equfloatshtenget']
            },
            'additional_apis': {
                'expected': 10,  # 额外API
                'critical': ['equ_fancy_factors_lite', 'fst_total']
            }
        }
        
        completeness_report = {}
        
        for category_dir in self.base_dir.iterdir():
            if category_dir.is_dir():
                category_name = category_dir.name
                
                if category_name in expected_apis:
                    # 统计实际API数量
                    actual_apis = []
                    for api_dir in category_dir.iterdir():
                        if api_dir.is_dir():
                            csv_files = list(api_dir.glob("*.csv"))
                            if csv_files:  # 只计算有数据的API
                                actual_apis.append(api_dir.name)
                    
                    expected_count = expected_apis[category_name]['expected']
                    actual_count = len(actual_apis)
                    critical_apis = expected_apis[category_name]['critical']
                    
                    # 检查关键API的可用性
                    critical_available = []
                    for critical_api in critical_apis:
                        if any(critical_api in api for api in actual_apis):
                            critical_available.append(critical_api)
                    
                    completeness_report[category_name] = {
                        'actual_count': actual_count,
                        'expected_count': expected_count,
                        'completeness_pct': min((actual_count / expected_count) * 100, 100),
                        'critical_apis_available': len(critical_available),
                        'critical_apis_total': len(critical_apis),
                        'critical_completeness_pct': (len(critical_available) / len(critical_apis)) * 100,
                        'api_list': actual_apis[:10]  # 只显示前10个
                    }
        
        return completeness_report
    
    def generate_executive_summary(self, reports, coverage, completeness):
        """生成执行摘要"""
        
        total_files = sum(cat['total_files'] for cat in reports.get('quality', pd.DataFrame()).to_dict('records'))
        total_size = sum(cat['size_gb'] for cat in reports.get('quality', pd.DataFrame()).to_dict('records'))
        total_apis = sum(cat['api_count'] for cat in reports.get('quality', pd.DataFrame()).to_dict('records'))
        
        avg_quality = np.mean([cat['quality_score'] for cat in reports.get('quality', pd.DataFrame()).to_dict('records')])
        
        summary = {
            'report_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'overall_stats': {
                'total_api_interfaces': total_apis,
                'total_data_files': total_files, 
                'total_data_size_gb': total_size,
                'average_data_quality': avg_quality,
                'data_categories': len(reports.get('quality', pd.DataFrame())),
            },
            'quality_assessment': {
                'excellent': len([cat for cat in reports.get('quality', pd.DataFrame()).to_dict('records') if cat.get('quality_score', 0) >= 95]),
                'good': len([cat for cat in reports.get('quality', pd.DataFrame()).to_dict('records') if 85 <= cat.get('quality_score', 0) < 95]),
                'fair': len([cat for cat in reports.get('quality', pd.DataFrame()).to_dict('records') if 70 <= cat.get('quality_score', 0) < 85]),
                'poor': len([cat for cat in reports.get('quality', pd.DataFrame()).to_dict('records') if cat.get('quality_score', 0) < 70])
            },
            'completeness_summary': {
                category: data['completeness_pct'] 
                for category, data in completeness.items()
            },
            'top_data_sources': []
        }
        
        # 找出最大的数据源
        if 'quality' in reports:
            top_sources = reports['quality'].nlargest(5, 'size_gb').to_dict('records')
            summary['top_data_sources'] = [
                {'category': src['category'], 'size_gb': src['size_gb']} 
                for src in top_sources
            ]
        
        return summary
    
    def generate_comprehensive_report(self):
        """生成综合报告"""
        logging.info("📊 生成综合数据质量报告...")
        
        # 加载已有报告
        reports = self.load_existing_reports()
        
        # 分析数据覆盖
        coverage = self.analyze_data_coverage()
        
        # 计算完整性
        completeness = self.calculate_api_completeness()
        
        # 生成执行摘要
        summary = self.generate_executive_summary(reports, coverage, completeness)
        
        # 输出控制台报告
        self.print_comprehensive_report(summary, completeness, reports)
        
        # 保存JSON报告
        with open('comprehensive_data_report.json', 'w', encoding='utf-8') as f:
            json.dump({
                'executive_summary': summary,
                'api_completeness': completeness,
                'data_coverage': coverage
            }, f, ensure_ascii=False, indent=2)
        
        # 生成Excel报告
        self.create_excel_report(reports, summary, completeness)
        
        return summary, completeness, coverage
    
    def print_comprehensive_report(self, summary, completeness, reports):
        """打印综合报告"""
        print("\n" + "="*100)
        print("🎯 **QuantTrade 数据资产综合质量报告**")
        print("="*100)
        print(f"📅 报告生成时间: {summary['report_date']}")
        
        # 执行摘要
        stats = summary['overall_stats']
        print(f"\n📊 **数据资产概览**:")
        print(f"  🔌 API接口总数: {stats['total_api_interfaces']} 个")
        print(f"  📄 数据文件总数: {stats['total_data_files']:,} 个")
        print(f"  💾 数据总容量: {stats['total_data_size_gb']:.1f} GB")
        print(f"  📁 数据分类: {stats['data_categories']} 个")
        print(f"  🎯 平均数据质量: {stats['average_data_quality']:.1f}%")
        
        # 质量评估
        quality = summary['quality_assessment']
        print(f"\n🏆 **数据质量分布**:")
        print(f"  🟢 优秀 (≥95%): {quality['excellent']} 个分类")
        print(f"  🟡 良好 (85-95%): {quality['good']} 个分类")
        print(f"  🟠 一般 (70-85%): {quality['fair']} 个分类") 
        print(f"  🔴 较差 (<70%): {quality['poor']} 个分类")
        
        # API完整性
        print(f"\n📋 **API完整性分析**:")
        for category, comp_data in completeness.items():
            status = "🟢" if comp_data['completeness_pct'] >= 90 else "🟡" if comp_data['completeness_pct'] >= 70 else "🔴"
            print(f"  {status} {category}: {comp_data['actual_count']}/{comp_data['expected_count']} APIs ({comp_data['completeness_pct']:.1f}%)")
            print(f"     关键API: {comp_data['critical_apis_available']}/{comp_data['critical_apis_total']} ({comp_data['critical_completeness_pct']:.1f}%)")
        
        # 最大数据源
        print(f"\n🔝 **主要数据源 (按容量排序)**:")
        for source in summary['top_data_sources'][:5]:
            print(f"  📊 {source['category']}: {source['size_gb']:.1f} GB")
        
        # 推荐建议
        print(f"\n💡 **优化建议**:")
        
        # 基于完整性给出建议
        for category, comp_data in completeness.items():
            if comp_data['completeness_pct'] < 90:
                missing = comp_data['expected_count'] - comp_data['actual_count']
                print(f"  🔧 {category}: 建议补充 {missing} 个API接口以达到完整覆盖")
            
            if comp_data['critical_completeness_pct'] < 100:
                print(f"  ⚠️ {category}: 关键API尚未完全覆盖，建议优先补充")
        
        # 数据质量建议
        if stats['average_data_quality'] < 95:
            print(f"  🧹 数据清洗: 平均质量为 {stats['average_data_quality']:.1f}%，建议进行数据清洗优化")
        
        if stats['total_data_size_gb'] > 200:
            print(f"  📦 存储优化: 数据量已达 {stats['total_data_size_gb']:.1f}GB，建议考虑数据压缩或归档策略")
    
    def create_excel_report(self, reports, summary, completeness):
        """创建Excel报告"""
        try:
            with pd.ExcelWriter('QuantTrade_数据质量综合报告.xlsx', engine='openpyxl') as writer:
                
                # 执行摘要
                summary_df = pd.DataFrame([summary['overall_stats']])
                summary_df.to_excel(writer, sheet_name='执行摘要', index=False)
                
                # API清单
                if 'inventory' in reports:
                    reports['inventory'].to_excel(writer, sheet_name='API清单', index=False)
                
                # 质量概览
                if 'quality' in reports:
                    reports['quality'].to_excel(writer, sheet_name='质量概览', index=False)
                
                # 完整性分析
                completeness_df = pd.DataFrame.from_dict(completeness, orient='index')
                completeness_df.to_excel(writer, sheet_name='完整性分析')
                
            logging.info("✅ 生成Excel报告: QuantTrade_数据质量综合报告.xlsx")
            
        except Exception as e:
            logging.warning(f"⚠️ Excel报告生成失败: {e}")

if __name__ == "__main__":
    reporter = ComprehensiveDataReporter()
    summary, completeness, coverage = reporter.generate_comprehensive_report()