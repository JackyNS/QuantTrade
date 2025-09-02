#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
详细API分析器 - 提供所有API的具体描述、数据时间范围和质量分析
"""

import pandas as pd
import numpy as np
from pathlib import Path
import logging
from datetime import datetime
import re
import warnings
warnings.filterwarnings('ignore')

class DetailedAPIAnalyzer:
    """详细API分析器"""
    
    def __init__(self):
        self.base_dir = Path("data/final_comprehensive_download")
        self.api_descriptions = self.load_api_descriptions()
        self.setup_logging()
        
    def setup_logging(self):
        """设置日志"""
        log_file = Path("detailed_api_analysis.log")
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file, encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
    
    def load_api_descriptions(self):
        """加载API中文描述映射"""
        return {
            # Basic Info APIs
            'secidget': '证券基础信息表 - 包含股票代码、名称、上市日期等基础信息',
            'equget': '股票基本信息 - A股、B股、H股等股票基本属性信息', 
            'equipoget': 'IPO基本信息 - 首次公开发行股票的基本信息',
            'equindustryget': '股票行业分类 - 各种行业分类标准下的股票归属',
            'equdivget': '分红送股信息 - 历史分红、送股、转股等权益分派数据',
            'equsplitsget': '股票拆细信息 - 股票拆细、合并等资本结构调整',
            'mktidxdget': '指数日行情数据 - 各类指数的历史价格和成交数据',
            
            # Financial APIs  
            'fdmtisalllatestget': '利润表最新数据 - 上市公司利润表科目最新财报数据',
            'fdmtbsalllatestget': '资产负债表最新数据 - 上市公司资产负债表最新财报',
            'fdmtcfalllatestget': '现金流量表最新数据 - 上市公司现金流量表最新财报',
            'fdmtisindualllatestget': '行业利润表数据 - 按行业汇总的利润表数据',
            'fdmtbsindualllatestget': '行业资产负债表数据 - 按行业汇总的资产负债表',
            'fdmtcfindualllatestget': '行业现金流量表数据 - 按行业汇总的现金流量表',
            'fdmtisbankalllatestget': '银行利润表数据 - 银行业专用利润表格式数据',
            'fdmtbsbankalllatestget': '银行资产负债表数据 - 银行业专用资产负债表',
            'fdmtcfbankalllatestget': '银行现金流量表数据 - 银行业专用现金流量表',
            'fdmtderget': '衍生工具公允价值 - 金融衍生工具的公允价值数据',
            'fdmtindipsget': '行业指标数据 - 各行业的关键财务指标统计',
            'fdmtindigrowthget': '行业成长性指标 - 行业增长率等成长性指标',
            
            # Special Trading APIs
            'mktlimitget': '涨跌停数据 - 每日涨停、跌停股票信息及触板时间',
            'mktblockdget': '大宗交易数据 - 大宗交易成交明细和统计信息',
            'fstdetailget': '融资融券明细 - 个股融资融券每日交易明细数据',
            'fsttotalget': '融资融券汇总 - 市场融资融券整体统计数据',
            'fstdetailget': '融资融券每日明细 - 个股融资融券详细交易记录',
            'sechaltget': '停牌复牌信息 - 股票停牌、复牌公告及原因',
            'vfsttargetget': '融资融券标的 - 可进行融资融券交易的证券名单',
            'equisactivityget': '增发配股活动 - 股票增发、配股等再融资活动',
            'equisparticipantqaget': '网上发行中签信息 - IPO网上申购中签结果',
            'mktconsbondpremiumget': '可转债转股价值 - 可转换债券转股价值及溢价率',
            'mktrankdivyieldget': '股息率排行 - 按股息率排名的股票列表',
            'mktranklistsalesget': '营业收入排行 - 按营业收入排名的上市公司',
            'mktrankliststocksget': '股票排行榜 - 各类股票排行榜数据',
            'mktrankinsttrget': '机构席位交易排名 - 机构专用席位交易统计',
            'mktequdstatsget': '股票交易统计 - 股票市场交易量价统计数据',
            'mktipocontraddaysget': 'IPO连续涨停天数 - 新股上市后连续涨停统计',
            'fdmtee': '环境保护投资 - 上市公司环保投资支出数据',
            'mktrankinstr': '机构投资者排行 - 机构投资者持仓排行数据',
            'equmarginsecget': '融资融券标的证券 - 融资融券业务标的证券清单',
            'mktequperfget': '股票业绩表现 - 股票各类业绩表现指标',
            
            # Governance APIs
            'equshtenget': '十大股东信息 - 上市公司前十大股东持股明细',
            'equfloatshtenget': '十大流通股东 - 前十大流通股东持股信息',
            'equshareholdernumget': '股东户数变化 - 历史股东户数变化趋势',
            'equactualcontrollerget': '实际控制人信息 - 上市公司实际控制人变更',
            'equexecsholdingsget': '高管持股变动 - 董监高人员持股变动明细',
            'equpledgeget': '股权质押信息 - 控股股东及高管股权质押情况',
            'equstockpledgeget': '股票质押回购 - 股票质押式回购交易信息',
            'equshareholdersmeetingget': '股东大会信息 - 股东大会召开及决议情况',
            'equrelatedtransactionget': '关联交易信息 - 上市公司关联交易明细',
            'equsharesexcitget': '限售股解禁 - 限售股份解禁上市计划和实施',
            'equchangeplanget': '股本变动计划 - 送股、转股、增发等股本变动计划',
            'equiposharefloatget': 'IPO限售股上市 - IPO限售股份上市流通信息',
            'equreformsharefloatget': '股改限售股上市 - 股权分置改革限售股上市',
            'equpartynatureget': '控股股东性质 - 控股股东的性质分类信息',
            'equallotget': '配股实施 - 配股方案实施情况及结果',
            'equspopubresultget': '增发结果公告 - 增发股票发行结果公告',
            'equallotmentsubscriptionresultsget': '配股缴款结果 - 配股缴款结果统计',
            'equspoget': '增发股票信息 - 增发股票方案及实施情况',
            'equoldshofferget': '老股转让 - IPO老股东售股转让信息',
            'equmschangesget': '股本结构变动 - 股本结构历史变动记录',
            'equmanagersget': '管理层信息 - 董事、监事、高管人员基本信息',
            'equsharesfloatget': '解禁股份明细 - 各类限售股份解禁明细',
            
            # Additional APIs
            'equ_fancy_factors_lite': '精选量化因子 - 优矿精选的量化投资因子数据',
            'sec_type_region': '地域分类信息 - 上市公司按地域的分类标准',
            'sec_type_rel': '板块成分关系 - 证券与各类板块的归属关系',
            'sec_type': '板块分类信息 - 概念板块、行业板块等分类标准',
            'industry': '行业分类标准 - 各类行业分类体系及标准定义',
            'fst_total': '融资融券汇总 - 全市场融资融券业务汇总数据',
            'fst_detail': '融资融券明细 - 个股融资融券交易明细记录',
            'eco_data_china_lite': '中国宏观经济数据 - 重要宏观经济指标数据'
        }
    
    def analyze_file_time_range(self, file_path):
        """分析文件的时间范围"""
        time_info = {
            'start_date': None,
            'end_date': None,
            'date_pattern': 'unknown',
            'sample_dates': []
        }
        
        try:
            # 从文件名推断时间信息
            filename = file_path.stem
            
            # 检查季度模式：2023_Q1, 2024_Q4等
            quarter_match = re.search(r'(\d{4})_Q(\d)', filename)
            if quarter_match:
                year, quarter = quarter_match.groups()
                time_info['date_pattern'] = 'quarterly'
                time_info['sample_dates'].append(f"{year}Q{quarter}")
                return time_info
            
            # 检查年度模式：year_2023, 2024等
            year_match = re.search(r'(year_)?(\d{4})', filename)
            if year_match:
                year = year_match.group(2)
                time_info['date_pattern'] = 'yearly'
                time_info['sample_dates'].append(year)
                return time_info
            
            # 检查快照模式
            if 'snapshot' in filename.lower():
                time_info['date_pattern'] = 'snapshot'
                time_info['sample_dates'].append('当前快照')
                return time_info
            
            # 尝试从数据内容分析
            df_sample = pd.read_csv(file_path, nrows=100)
            date_columns = []
            
            for col in df_sample.columns:
                col_lower = col.lower()
                if any(date_word in col_lower for date_word in 
                      ['date', 'time', 'year', 'month', 'day', 'enddate', 'tradedate']):
                    date_columns.append(col)
            
            if date_columns and len(df_sample) > 0:
                # 分析第一个日期列
                date_col = date_columns[0]
                sample_values = df_sample[date_col].dropna().head(5).tolist()
                
                if sample_values:
                    time_info['sample_dates'] = [str(val)[:10] for val in sample_values]
                    time_info['date_pattern'] = 'data_driven'
                    
                    # 尝试解析最早和最晚日期
                    try:
                        all_dates = pd.to_datetime(df_sample[date_col].dropna())
                        time_info['start_date'] = all_dates.min().strftime('%Y-%m-%d')
                        time_info['end_date'] = all_dates.max().strftime('%Y-%m-%d')
                    except:
                        pass
        
        except Exception as e:
            time_info['date_pattern'] = 'error'
            time_info['sample_dates'] = [f"解析错误: {str(e)[:30]}"]
        
        return time_info
    
    def analyze_data_quality_details(self, file_path):
        """分析数据质量详情"""
        quality_info = {
            'row_count': 0,
            'column_count': 0,
            'missing_rate': 0.0,
            'duplicate_rate': 0.0,
            'data_types': {},
            'key_columns': [],
            'data_sample': {}
        }
        
        try:
            # 读取样本数据
            df_sample = pd.read_csv(file_path, nrows=1000)
            
            quality_info.update({
                'row_count': len(df_sample),
                'column_count': len(df_sample.columns),
                'missing_rate': (df_sample.isnull().sum().sum() / (len(df_sample) * len(df_sample.columns)) * 100),
                'duplicate_rate': (df_sample.duplicated().sum() / len(df_sample) * 100) if len(df_sample) > 0 else 0
            })
            
            # 数据类型分析
            type_counts = df_sample.dtypes.value_counts()
            quality_info['data_types'] = {str(dtype): int(count) for dtype, count in type_counts.items()}
            
            # 关键列识别
            key_columns = []
            for col in df_sample.columns[:10]:  # 只分析前10列
                col_lower = col.lower()
                if any(key_word in col_lower for key_word in 
                      ['code', 'symbol', 'id', 'name', 'date', 'price', 'volume']):
                    key_columns.append(col)
            quality_info['key_columns'] = key_columns
            
            # 数据样本
            if len(df_sample) > 0:
                sample_dict = {}
                for col in df_sample.columns[:5]:  # 前5列的样本
                    sample_values = df_sample[col].dropna().head(3).tolist()
                    sample_dict[col] = [str(val)[:20] for val in sample_values]
                quality_info['data_sample'] = sample_dict
        
        except Exception as e:
            quality_info['error'] = str(e)
        
        return quality_info
    
    def analyze_single_api(self, api_dir):
        """分析单个API的详细信息"""
        api_name = api_dir.name
        csv_files = list(api_dir.glob("*.csv"))
        
        api_info = {
            'api_name': api_name,
            'chinese_description': self.api_descriptions.get(api_name, '暂无描述'),
            'file_count': len(csv_files),
            'total_size_mb': 0,
            'time_coverage': {},
            'quality_metrics': {},
            'sample_files': []
        }
        
        if not csv_files:
            api_info['status'] = 'no_data'
            return api_info
        
        # 计算总大小
        total_size = sum(f.stat().st_size for f in csv_files)
        api_info['total_size_mb'] = total_size / (1024 * 1024)
        
        # 分析时间覆盖（采样分析）
        time_patterns = {}
        sample_files_info = []
        
        for i, csv_file in enumerate(csv_files[:5]):  # 分析前5个文件
            time_info = self.analyze_file_time_range(csv_file)
            quality_info = self.analyze_data_quality_details(csv_file)
            
            sample_files_info.append({
                'filename': csv_file.name,
                'size_mb': csv_file.stat().st_size / (1024 * 1024),
                'time_info': time_info,
                'quality_info': quality_info
            })
            
            pattern = time_info['date_pattern']
            time_patterns[pattern] = time_patterns.get(pattern, 0) + 1
        
        # 推断总体时间覆盖模式
        most_common_pattern = max(time_patterns, key=time_patterns.get) if time_patterns else 'unknown'
        api_info['time_coverage'] = {
            'pattern': most_common_pattern,
            'pattern_distribution': time_patterns,
            'estimated_date_range': self.estimate_date_range(csv_files, most_common_pattern)
        }
        
        # 质量指标汇总
        if sample_files_info:
            avg_rows = np.mean([f['quality_info'].get('row_count', 0) for f in sample_files_info])
            avg_cols = np.mean([f['quality_info'].get('column_count', 0) for f in sample_files_info])
            avg_missing = np.mean([f['quality_info'].get('missing_rate', 0) for f in sample_files_info])
            
            api_info['quality_metrics'] = {
                'avg_rows_per_file': int(avg_rows),
                'avg_columns_per_file': int(avg_cols),
                'avg_missing_rate': round(avg_missing, 2),
                'estimated_total_records': int(avg_rows * len(csv_files))
            }
        
        api_info['sample_files'] = sample_files_info
        api_info['status'] = 'analyzed'
        
        return api_info
    
    def estimate_date_range(self, csv_files, pattern):
        """根据文件名估算日期范围"""
        if pattern == 'snapshot':
            return {'type': 'snapshot', 'description': '当前时点快照数据'}
        
        years = []
        for file in csv_files:
            filename = file.stem
            year_match = re.search(r'(\d{4})', filename)
            if year_match:
                years.append(int(year_match.group(1)))
        
        if years:
            min_year, max_year = min(years), max(years)
            if pattern == 'quarterly':
                return {
                    'type': 'quarterly',
                    'start_year': min_year,
                    'end_year': max_year,
                    'description': f'{min_year}年-{max_year}年按季度数据'
                }
            elif pattern == 'yearly':
                return {
                    'type': 'yearly', 
                    'start_year': min_year,
                    'end_year': max_year,
                    'description': f'{min_year}年-{max_year}年按年度数据'
                }
        
        return {'type': 'unknown', 'description': '日期范围未知'}
    
    def generate_detailed_report(self):
        """生成详细API分析报告"""
        logging.info("📊 开始详细API分析...")
        
        detailed_report = {
            'report_metadata': {
                'generation_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'total_apis': 0,
                'total_categories': 0
            },
            'categories': {}
        }
        
        all_api_details = []
        
        for category_dir in self.base_dir.iterdir():
            if category_dir.is_dir():
                category_name = category_dir.name
                category_apis = []
                
                logging.info(f"📂 分析分类: {category_name}")
                
                for api_dir in category_dir.iterdir():
                    if api_dir.is_dir():
                        api_info = self.analyze_single_api(api_dir)
                        category_apis.append(api_info)
                        all_api_details.append({
                            'category': category_name,
                            **api_info
                        })
                        logging.info(f"  ✅ {api_info['api_name']}: {api_info['file_count']} 文件")
                
                detailed_report['categories'][category_name] = {
                    'api_count': len(category_apis),
                    'apis': category_apis
                }
        
        detailed_report['report_metadata']['total_apis'] = len(all_api_details)
        detailed_report['report_metadata']['total_categories'] = len(detailed_report['categories'])
        
        # 生成控制台报告
        self.print_detailed_report(detailed_report)
        
        # 生成CSV详细报告
        self.create_detailed_csv_reports(all_api_details)
        
        return detailed_report
    
    def print_detailed_report(self, report):
        """打印详细报告"""
        print("\n" + "="*120)
        print("🔍 **QuantTrade API详细分析报告** 🔍")
        print("="*120)
        print(f"📅 生成时间: {report['report_metadata']['generation_time']}")
        print(f"📊 分析范围: {report['report_metadata']['total_categories']} 个分类, {report['report_metadata']['total_apis']} 个API")
        
        for category_name, category_data in report['categories'].items():
            print(f"\n" + "─"*100)
            print(f"📁 **{category_name.upper()}** ({category_data['api_count']} APIs)")
            print("─"*100)
            
            for api in category_data['apis']:
                if api['status'] == 'no_data':
                    print(f"\n🔴 {api['api_name']}")
                    print(f"   📝 描述: {api['chinese_description']}")
                    print(f"   ⚠️ 状态: 无数据文件")
                    continue
                
                print(f"\n🟢 **{api['api_name']}**")
                print(f"   📝 描述: {api['chinese_description']}")
                print(f"   📊 数据规模: {api['file_count']} 文件, {api['total_size_mb']:.1f}MB")
                
                # 时间覆盖信息
                time_info = api['time_coverage']
                print(f"   📅 时间覆盖: {time_info['estimated_date_range'].get('description', '未知')}")
                
                # 质量指标
                if api['quality_metrics']:
                    metrics = api['quality_metrics']
                    print(f"   🎯 数据质量: 平均{metrics['avg_rows_per_file']:,}行/文件, "
                          f"{metrics['avg_columns_per_file']}列, "
                          f"缺失率{metrics['avg_missing_rate']}%")
                    print(f"   📈 总记录数: 约{metrics['estimated_total_records']:,}条")
                
                # 关键列信息
                if api['sample_files']:
                    sample_file = api['sample_files'][0]
                    key_columns = sample_file['quality_info'].get('key_columns', [])
                    if key_columns:
                        print(f"   🔑 关键字段: {', '.join(key_columns[:5])}")
                    
                    # 数据样本
                    data_sample = sample_file['quality_info'].get('data_sample', {})
                    if data_sample:
                        print(f"   📋 数据样本:")
                        for col, values in list(data_sample.items())[:3]:
                            print(f"      • {col}: {', '.join(values)}")
    
    def create_detailed_csv_reports(self, all_api_details):
        """创建详细的CSV报告"""
        
        # 1. API概览报告
        api_overview = []
        for api in all_api_details:
            time_info = api.get('time_coverage', {})
            quality_info = api.get('quality_metrics', {})
            
            api_overview.append({
                'category': api['category'],
                'api_name': api['api_name'],
                'chinese_description': api['chinese_description'],
                'file_count': api['file_count'],
                'total_size_mb': api['total_size_mb'],
                'time_pattern': time_info.get('pattern', 'unknown'),
                'date_range_description': time_info.get('estimated_date_range', {}).get('description', ''),
                'avg_rows_per_file': quality_info.get('avg_rows_per_file', 0),
                'avg_columns_per_file': quality_info.get('avg_columns_per_file', 0),
                'avg_missing_rate': quality_info.get('avg_missing_rate', 0),
                'estimated_total_records': quality_info.get('estimated_total_records', 0),
                'status': api.get('status', 'unknown')
            })
        
        df_overview = pd.DataFrame(api_overview)
        df_overview.to_csv('API详细分析报告_概览.csv', index=False, encoding='utf-8-sig')
        logging.info("✅ 生成API概览报告: API详细分析报告_概览.csv")
        
        # 2. 数据质量详细报告
        quality_details = []
        for api in all_api_details:
            if api.get('sample_files'):
                for sample_file in api['sample_files']:
                    quality_info = sample_file['quality_info']
                    quality_details.append({
                        'category': api['category'],
                        'api_name': api['api_name'],
                        'filename': sample_file['filename'],
                        'file_size_mb': sample_file['size_mb'],
                        'row_count': quality_info.get('row_count', 0),
                        'column_count': quality_info.get('column_count', 0),
                        'missing_rate': quality_info.get('missing_rate', 0),
                        'duplicate_rate': quality_info.get('duplicate_rate', 0),
                        'key_columns': ', '.join(quality_info.get('key_columns', [])),
                        'data_types': str(quality_info.get('data_types', {}))
                    })
        
        if quality_details:
            df_quality = pd.DataFrame(quality_details)
            df_quality.to_csv('API详细分析报告_质量.csv', index=False, encoding='utf-8-sig')
            logging.info("✅ 生成质量详细报告: API详细分析报告_质量.csv")
        
        # 3. 按分类汇总报告
        category_summary = []
        categories = {}
        
        for api in all_api_details:
            cat = api['category']
            if cat not in categories:
                categories[cat] = {
                    'api_count': 0,
                    'total_files': 0,
                    'total_size_mb': 0,
                    'total_records': 0,
                    'apis_with_data': 0
                }
            
            categories[cat]['api_count'] += 1
            categories[cat]['total_files'] += api['file_count']
            categories[cat]['total_size_mb'] += api['total_size_mb']
            
            if api.get('quality_metrics'):
                categories[cat]['total_records'] += api['quality_metrics'].get('estimated_total_records', 0)
            
            if api['file_count'] > 0:
                categories[cat]['apis_with_data'] += 1
        
        for cat_name, cat_data in categories.items():
            category_summary.append({
                'category': cat_name,
                'api_count': cat_data['api_count'],
                'apis_with_data': cat_data['apis_with_data'],
                'data_coverage_rate': (cat_data['apis_with_data'] / cat_data['api_count'] * 100) if cat_data['api_count'] > 0 else 0,
                'total_files': cat_data['total_files'],
                'total_size_mb': cat_data['total_size_mb'],
                'estimated_total_records': cat_data['total_records']
            })
        
        df_category = pd.DataFrame(category_summary)
        df_category.to_csv('API详细分析报告_分类汇总.csv', index=False, encoding='utf-8-sig')
        logging.info("✅ 生成分类汇总报告: API详细分析报告_分类汇总.csv")

if __name__ == "__main__":
    analyzer = DetailedAPIAnalyzer()
    detailed_report = analyzer.generate_detailed_report()