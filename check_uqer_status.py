#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
优矿权限和数据状态检查工具
=======================

功能：
1. 检查优矿API权限和接口清单
2. 分析已下载的数据情况
3. 生成详细的状态报告

Author: QuantTrader Team
Date: 2025-08-31
"""

import os
import sys
import json
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

# 添加项目路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

class UqerStatusChecker:
    """优矿状态检查器"""
    
    def __init__(self):
        self.token = self.get_token()
        self.client = None
        self.data_dir = Path('./data')
        self.report = {
            'check_time': datetime.now(),
            'token_status': None,
            'permissions': {},
            'available_apis': [],
            'data_status': {},
            'recommendations': []
        }
    
    def get_token(self):
        """获取优矿Token"""
        # 从环境变量获取
        token = os.environ.get('UQER_TOKEN')
        if token:
            return token
        
        # 从配置文件获取
        config_files = ['config/uqer_config.json', 'uqer_config.json']
        for config_file in config_files:
            if Path(config_file).exists():
                try:
                    with open(config_file, 'r', encoding='utf-8') as f:
                        config = json.load(f)
                        token = config.get('token')
                        if token:
                            return token
                except:
                    continue
        
        return None
    
    def check_uqer_permissions(self):
        """检查优矿权限和接口"""
        print("🔍 检查优矿API权限...")
        
        if not self.token:
            print("❌ 未找到优矿Token")
            self.report['token_status'] = 'missing'
            return False
        
        try:
            import uqer
            self.client = uqer.Client(token=self.token)
            print("✅ 优矿客户端初始化成功")
            self.report['token_status'] = 'valid'
            
            # 检查基础权限 - 尝试获取股票列表
            print("📊 检查基础权限...")
            try:
                # 获取最近一个交易日的股票列表
                yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
                stock_data = self.client.getMktEqud(
                    tradeDate=yesterday,
                    field='ticker,secShortName,tradeDate',
                    pandas="1"
                )
                
                if not stock_data.empty:
                    print(f"✅ 股票数据权限正常，获取到 {len(stock_data)} 条记录")
                    self.report['permissions']['stock_basic'] = {
                        'status': 'available',
                        'sample_count': len(stock_data),
                        'latest_date': yesterday
                    }
                else:
                    print("⚠️ 股票数据为空，可能是非交易日")
                    self.report['permissions']['stock_basic'] = {
                        'status': 'empty_result',
                        'note': 'possible_non_trading_day'
                    }
                    
            except Exception as e:
                print(f"❌ 股票数据权限检查失败: {e}")
                self.report['permissions']['stock_basic'] = {
                    'status': 'error',
                    'error': str(e)
                }
            
            return True
            
        except ImportError:
            print("❌ uqer包未安装")
            self.report['token_status'] = 'package_missing'
            return False
        except Exception as e:
            print(f"❌ 优矿连接失败: {e}")
            self.report['token_status'] = 'connection_failed'
            self.report['permissions']['error'] = str(e)
            return False
    
    def check_available_apis(self):
        """检查可用的API接口"""
        print("\n📋 检查可用的API接口...")
        
        if not self.client:
            print("❌ 优矿客户端未初始化")
            return
        
        # 定义要测试的API列表
        api_tests = [
            {
                'name': 'getMktEqud',
                'description': '股票行情数据',
                'test_params': {
                    'beginDate': '20241201',
                    'endDate': '20241201',
                    'ticker': '000001.XSHE',
                    'field': 'ticker,closePrice,turnoverVol'
                },
                'category': 'market_data'
            },
            {
                'name': 'getEquIndustry', 
                'description': '行业分类数据',
                'test_params': {
                    'ticker': '000001.XSHE',
                    'industryVersionCD': '010303'
                },
                'category': 'reference_data'
            },
            {
                'name': 'getFundamental',
                'description': '财务基础数据',
                'test_params': {
                    'ticker': '000001.XSHE',
                    'beginDate': '20241001',
                    'endDate': '20241201'
                },
                'category': 'fundamental_data'
            },
            {
                'name': 'getSecIndustry',
                'description': '证券行业分类',
                'test_params': {
                    'ticker': '000001.XSHE'
                },
                'category': 'reference_data'
            },
            {
                'name': 'getMktIdxd',
                'description': '指数行情数据',
                'test_params': {
                    'ticker': '000001.XSHG',
                    'beginDate': '20241201',
                    'endDate': '20241201'
                },
                'category': 'index_data'
            }
        ]
        
        available_apis = []
        permission_summary = {}
        
        for api_info in api_tests:
            api_name = api_info['name']
            category = api_info['category']
            
            print(f"   测试 {api_name} - {api_info['description']}...")
            
            try:
                # 获取API方法
                api_method = getattr(self.client, api_name, None)
                if not api_method:
                    print(f"   ❌ API方法不存在")
                    continue
                
                # 尝试调用API
                result = api_method(**api_info['test_params'], pandas="1")
                
                if result is not None and not result.empty:
                    status = 'available'
                    sample_count = len(result)
                    print(f"   ✅ 可用 - {sample_count} 条记录")
                else:
                    status = 'available_but_empty'
                    sample_count = 0
                    print(f"   ⚠️ 可用但无数据")
                
                api_status = {
                    'name': api_name,
                    'description': api_info['description'],
                    'category': category,
                    'status': status,
                    'sample_count': sample_count
                }
                
                available_apis.append(api_status)
                
                # 按类别统计
                if category not in permission_summary:
                    permission_summary[category] = {'available': 0, 'total': 0}
                permission_summary[category]['available'] += 1
                permission_summary[category]['total'] += 1
                
            except Exception as e:
                print(f"   ❌ 不可用: {e}")
                
                api_status = {
                    'name': api_name,
                    'description': api_info['description'],
                    'category': category,
                    'status': 'error',
                    'error': str(e)
                }
                
                available_apis.append(api_status)
                
                if category not in permission_summary:
                    permission_summary[category] = {'available': 0, 'total': 0}
                permission_summary[category]['total'] += 1
        
        self.report['available_apis'] = available_apis
        self.report['permissions']['summary'] = permission_summary
        
        # 显示权限总结
        print(f"\n📊 权限总结:")
        for category, stats in permission_summary.items():
            print(f"   {category}: {stats['available']}/{stats['total']} 可用")
    
    def check_downloaded_data(self):
        """检查已下载的数据"""
        print("\n📂 检查已下载的数据...")
        
        if not self.data_dir.exists():
            print("❌ 数据目录不存在")
            self.report['data_status'] = {
                'directory_exists': False,
                'total_files': 0,
                'total_size': 0
            }
            return
        
        # 扫描数据文件
        csv_files = list(self.data_dir.glob('*.csv'))
        parquet_files = list(self.data_dir.glob('*.parquet'))
        all_data_files = csv_files + parquet_files
        
        if not all_data_files:
            print("❌ 未找到数据文件")
            self.report['data_status'] = {
                'directory_exists': True,
                'total_files': 0,
                'total_size': 0,
                'file_types': {'csv': 0, 'parquet': 0}
            }
            return
        
        print(f"✅ 找到 {len(all_data_files)} 个数据文件")
        
        # 分析文件信息
        file_analysis = {
            'total_files': len(all_data_files),
            'csv_files': len(csv_files),
            'parquet_files': len(parquet_files),
            'total_size': 0,
            'files_by_size': [],
            'date_range': {},
            'symbols': set()
        }
        
        print("📊 分析文件详情...")
        
        # 分析每个文件
        for file_path in all_data_files[:20]:  # 限制分析前20个文件，避免太慢
            try:
                file_size = file_path.stat().st_size
                file_analysis['total_size'] += file_size
                
                # 从文件名提取股票代码
                symbol = file_path.stem
                file_analysis['symbols'].add(symbol)
                
                # 读取文件获取日期范围（限制行数避免太慢）
                if file_path.suffix == '.csv':
                    try:
                        df = pd.read_csv(file_path, nrows=1000)
                        if 'date' in df.columns:
                            dates = pd.to_datetime(df['date'], errors='coerce')
                            valid_dates = dates.dropna()
                            if not valid_dates.empty:
                                min_date = valid_dates.min()
                                max_date = valid_dates.max()
                                
                                if symbol not in file_analysis['date_range']:
                                    file_analysis['date_range'][symbol] = {
                                        'start': min_date,
                                        'end': max_date,
                                        'days': (max_date - min_date).days + 1
                                    }
                    except Exception as e:
                        pass  # 跳过读取失败的文件
                
                file_analysis['files_by_size'].append({
                    'name': file_path.name,
                    'size': file_size,
                    'size_mb': round(file_size / 1024 / 1024, 2)
                })
                
            except Exception as e:
                print(f"   ⚠️ 分析文件失败 {file_path.name}: {e}")
        
        # 按大小排序
        file_analysis['files_by_size'].sort(key=lambda x: x['size'], reverse=True)
        
        # 统计信息
        total_size_mb = file_analysis['total_size'] / 1024 / 1024
        total_size_gb = total_size_mb / 1024
        
        print(f"📊 数据统计:")
        print(f"   总文件数: {file_analysis['total_files']}")
        print(f"   CSV文件: {file_analysis['csv_files']}")
        print(f"   Parquet文件: {file_analysis['parquet_files']}")
        print(f"   总大小: {total_size_mb:.1f} MB ({total_size_gb:.2f} GB)")
        print(f"   股票数量: {len(file_analysis['symbols'])}")
        
        if file_analysis['files_by_size']:
            print(f"\n📋 最大的5个文件:")
            for file_info in file_analysis['files_by_size'][:5]:
                print(f"   {file_info['name']}: {file_info['size_mb']} MB")
        
        if file_analysis['date_range']:
            print(f"\n📅 数据日期范围（部分样本）:")
            for symbol, date_info in list(file_analysis['date_range'].items())[:5]:
                start_str = date_info['start'].strftime('%Y-%m-%d')
                end_str = date_info['end'].strftime('%Y-%m-%d')
                print(f"   {symbol}: {start_str} 到 {end_str} ({date_info['days']}天)")
        
        # 更新报告
        self.report['data_status'] = {
            'directory_exists': True,
            'total_files': file_analysis['total_files'],
            'csv_files': file_analysis['csv_files'],
            'parquet_files': file_analysis['parquet_files'],
            'total_size_mb': round(total_size_mb, 2),
            'total_size_gb': round(total_size_gb, 3),
            'symbols_count': len(file_analysis['symbols']),
            'sample_symbols': list(file_analysis['symbols'])[:10],
            'largest_files': file_analysis['files_by_size'][:5],
            'date_range_samples': {
                symbol: {
                    'start': date_info['start'].strftime('%Y-%m-%d'),
                    'end': date_info['end'].strftime('%Y-%m-%d'),
                    'days': date_info['days']
                } 
                for symbol, date_info in list(file_analysis['date_range'].items())[:5]
            }
        }
    
    def generate_recommendations(self):
        """生成推荐建议"""
        print("\n💡 生成推荐建议...")
        
        recommendations = []
        
        # Token状态建议
        if self.report['token_status'] == 'missing':
            recommendations.append({
                'type': 'critical',
                'title': '配置优矿Token',
                'description': '未找到优矿API Token，请配置后才能使用数据下载功能',
                'action': '设置环境变量 UQER_TOKEN 或创建配置文件'
            })
        elif self.report['token_status'] == 'connection_failed':
            recommendations.append({
                'type': 'warning',
                'title': '检查网络连接',
                'description': '优矿API连接失败，请检查网络和Token有效性',
                'action': '确认网络畅通且Token未过期'
            })
        
        # 权限建议
        if 'summary' in self.report['permissions']:
            for category, stats in self.report['permissions']['summary'].items():
                if stats['available'] < stats['total']:
                    recommendations.append({
                        'type': 'info',
                        'title': f'扩展{category}权限',
                        'description': f'{category}类API只有{stats["available"]}/{stats["total"]}可用',
                        'action': '联系优矿客服申请更多权限或升级账户等级'
                    })
        
        # 数据状态建议
        data_status = self.report['data_status']
        if data_status.get('total_files', 0) == 0:
            recommendations.append({
                'type': 'info',
                'title': '开始数据下载',
                'description': '未发现历史数据，建议开始首次全量下载',
                'action': '运行 python download_uqer_data.py'
            })
        elif data_status.get('total_files', 0) > 0:
            recommendations.append({
                'type': 'success',
                'title': '设置定期更新',
                'description': f'已有{data_status["total_files"]}个数据文件，建议配置自动更新',
                'action': '运行 python setup_scheduler.py 配置定时任务'
            })
        
        # 存储空间建议
        if data_status.get('total_size_gb', 0) > 5:
            recommendations.append({
                'type': 'warning',
                'title': '监控存储空间',
                'description': f'数据已占用{data_status["total_size_gb"]:.1f}GB空间',
                'action': '定期清理旧数据或扩展存储空间'
            })
        
        self.report['recommendations'] = recommendations
        
        # 显示建议
        if recommendations:
            print("📝 建议事项:")
            for rec in recommendations:
                icon = {'critical': '🔴', 'warning': '🟡', 'info': '🔵', 'success': '🟢'}.get(rec['type'], '💡')
                print(f"   {icon} {rec['title']}")
                print(f"      {rec['description']}")
                print(f"      操作: {rec['action']}")
        else:
            print("✅ 系统状态良好，无特殊建议")
    
    def save_report(self):
        """保存检查报告"""
        print("\n📝 保存检查报告...")
        
        # 创建报告目录
        reports_dir = Path('reports')
        reports_dir.mkdir(exist_ok=True)
        
        # 生成报告文件名
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        report_file = reports_dir / f'uqer_status_check_{timestamp}.json'
        
        # 保存JSON报告
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(self.report, f, ensure_ascii=False, indent=2, default=str)
        
        # 生成文本报告
        text_report = self.generate_text_report()
        text_report_file = reports_dir / f'uqer_status_check_{timestamp}.txt'
        
        with open(text_report_file, 'w', encoding='utf-8') as f:
            f.write(text_report)
        
        print(f"✅ 报告已保存:")
        print(f"   JSON: {report_file}")
        print(f"   文本: {text_report_file}")
        
        return report_file, text_report_file
    
    def generate_text_report(self):
        """生成文本格式报告"""
        report_lines = []
        report_lines.append("=" * 60)
        report_lines.append("🔍 优矿权限和数据状态检查报告")
        report_lines.append("=" * 60)
        report_lines.append(f"📅 检查时间: {self.report['check_time'].strftime('%Y-%m-%d %H:%M:%S')}")
        report_lines.append("")
        
        # Token状态
        report_lines.append("🔑 Token状态:")
        status_icon = {'valid': '✅', 'missing': '❌', 'connection_failed': '⚠️'}.get(
            self.report['token_status'], '❓'
        )
        report_lines.append(f"   {status_icon} {self.report['token_status']}")
        report_lines.append("")
        
        # API权限
        report_lines.append("📋 API权限状态:")
        if self.report['available_apis']:
            for api in self.report['available_apis']:
                status_icon = {'available': '✅', 'available_but_empty': '⚠️', 'error': '❌'}.get(
                    api['status'], '❓'
                )
                report_lines.append(f"   {status_icon} {api['name']}: {api['description']}")
        else:
            report_lines.append("   ❌ 无可用API")
        report_lines.append("")
        
        # 数据状态
        report_lines.append("📂 数据状态:")
        data_status = self.report['data_status']
        if data_status.get('directory_exists', False):
            report_lines.append(f"   📁 数据目录: 存在")
            report_lines.append(f"   📊 文件总数: {data_status.get('total_files', 0)}")
            report_lines.append(f"   📈 股票数量: {data_status.get('symbols_count', 0)}")
            report_lines.append(f"   💾 总大小: {data_status.get('total_size_gb', 0):.2f} GB")
        else:
            report_lines.append("   ❌ 数据目录不存在")
        report_lines.append("")
        
        # 建议事项
        report_lines.append("💡 建议事项:")
        if self.report['recommendations']:
            for rec in self.report['recommendations']:
                icon = {'critical': '🔴', 'warning': '🟡', 'info': '🔵', 'success': '🟢'}.get(rec['type'], '💡')
                report_lines.append(f"   {icon} {rec['title']}")
                report_lines.append(f"      {rec['description']}")
                report_lines.append(f"      操作: {rec['action']}")
        else:
            report_lines.append("   ✅ 系统状态良好")
        
        report_lines.append("")
        report_lines.append("=" * 60)
        
        return "\n".join(report_lines)
    
    def run_full_check(self):
        """运行完整检查"""
        print("🚀 开始优矿权限和数据状态检查")
        print("=" * 50)
        
        # 1. 检查权限
        permissions_ok = self.check_uqer_permissions()
        
        # 2. 检查API接口
        if permissions_ok:
            self.check_available_apis()
        
        # 3. 检查已下载数据
        self.check_downloaded_data()
        
        # 4. 生成建议
        self.generate_recommendations()
        
        # 5. 保存报告
        report_files = self.save_report()
        
        print("\n" + "=" * 50)
        print("🎉 检查完成!")
        
        return self.report

def main():
    """主函数"""
    checker = UqerStatusChecker()
    report = checker.run_full_check()
    
    # 显示简要总结
    print(f"\n📊 检查总结:")
    print(f"   Token状态: {report['token_status']}")
    print(f"   可用API: {len([api for api in report.get('available_apis', []) if api['status'] == 'available'])}")
    print(f"   数据文件: {report['data_status'].get('total_files', 0)}")
    print(f"   建议事项: {len(report.get('recommendations', []))}")
    
    return report

if __name__ == "__main__":
    main()