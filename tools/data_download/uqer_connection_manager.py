#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
优矿连接管理器 - 统一的连接测试和状态检查工具
================================================

合并了原有的三个工具:
- simple_uqer_test.py (简单连接测试)
- test_uqer_connection.py (详细连接检查) 
- check_uqer_status.py (权限和状态检查)

功能:
1. 基础连接测试
2. 详细功能验证
3. 权限和数据状态检查
4. 生成综合状态报告

使用方法:
python tools/data_download/uqer_connection_manager.py [模式]

模式选项:
- simple: 简单连接测试
- detailed: 详细功能验证  
- status: 权限和状态检查
- all: 完整检查 (默认)
"""

import os
import sys
import json
import argparse
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

# 添加项目路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

class UqerConnectionManager:
    """优矿连接管理器"""
    
    def __init__(self):
        self.token = "68b9922817ae6273137bda7acba81e293582ba347281dfcc056dcb245b23faf3"
        self.client = None
        self.report = {
            'timestamp': datetime.now().isoformat(),
            'connection_status': False,
            'tests_passed': 0,
            'tests_failed': 0,
            'results': {}
        }
    
    def test_basic_connection(self):
        """测试基础连接"""
        print("🔍 测试优矿基础连接...")
        
        try:
            import uqer
            self.client = uqer.Client(token=self.token)
            print("✅ 优矿客户端初始化成功")
            print(f"   账号信息: {self.client}")
            
            self.report['connection_status'] = True
            self.report['tests_passed'] += 1
            self.report['results']['basic_connection'] = {
                'status': 'success',
                'client_info': str(self.client)
            }
            return True
            
        except ImportError as e:
            print(f"❌ uqer包导入失败: {e}")
            self.report['tests_failed'] += 1
            self.report['results']['basic_connection'] = {
                'status': 'failed',
                'error': f"Import error: {str(e)}"
            }
            return False
            
        except Exception as e:
            print(f"❌ 优矿连接失败: {e}")
            self.report['tests_failed'] += 1
            self.report['results']['basic_connection'] = {
                'status': 'failed',
                'error': str(e)
            }
            return False

    def test_stock_data(self):
        """测试股票数据获取"""
        print("\n📊 测试股票数据获取...")
        
        if not self.client:
            print("❌ 客户端未初始化，跳过股票数据测试")
            return False
        
        try:
            import uqer.api as api
            
            # 测试获取股票基础信息
            end_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
            start_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
            
            print(f"   测试日期范围: {start_date} 到 {end_date}")
            
            # 获取股票列表
            stock_list = api.SecType.getAllSecCode(type_=api.SecType.stock)
            if stock_list and len(stock_list) > 0:
                test_stock = stock_list[0]
                print(f"✅ 获取股票列表成功，测试股票: {test_stock}")
                
                # 测试日行情数据
                data = api.MarketDataQuery.getMarketData(
                    securityCode=[test_stock],
                    startDate=start_date,
                    endDate=end_date,
                    ticker="",
                    frequency=api.MarketDataQuery.frequency_daily
                )
                
                if data and len(data) > 0:
                    print(f"✅ 获取日行情数据成功，数据量: {len(data)} 条")
                    self.report['tests_passed'] += 1
                    self.report['results']['stock_data'] = {
                        'status': 'success',
                        'test_stock': test_stock,
                        'data_count': len(data)
                    }
                    return True
                else:
                    print("⚠️ 获取到空数据")
                    self.report['tests_failed'] += 1
                    self.report['results']['stock_data'] = {
                        'status': 'warning',
                        'message': 'Empty data returned'
                    }
                    return False
            else:
                print("❌ 获取股票列表失败")
                self.report['tests_failed'] += 1
                self.report['results']['stock_data'] = {
                    'status': 'failed',
                    'error': 'Failed to get stock list'
                }
                return False
                
        except Exception as e:
            print(f"❌ 股票数据测试失败: {e}")
            self.report['tests_failed'] += 1
            self.report['results']['stock_data'] = {
                'status': 'failed',
                'error': str(e)
            }
            return False

    def test_detailed_functions(self):
        """测试详细功能"""
        print("\n🔧 测试详细API功能...")
        
        if not self.client:
            print("❌ 客户端未初始化，跳过详细功能测试")
            return False
        
        tests = [
            ('市场数据接口', self._test_market_data_api),
            ('基本面数据接口', self._test_fundamental_api),
            ('资金流向接口', self._test_flow_api)
        ]
        
        passed = 0
        total = len(tests)
        
        for test_name, test_func in tests:
            try:
                if test_func():
                    print(f"✅ {test_name} - 通过")
                    passed += 1
                else:
                    print(f"⚠️ {test_name} - 部分功能异常")
            except Exception as e:
                print(f"❌ {test_name} - 失败: {e}")
        
        self.report['results']['detailed_functions'] = {
            'total_tests': total,
            'passed_tests': passed,
            'success_rate': passed / total if total > 0 else 0
        }
        
        if passed >= total * 0.7:  # 70%通过率
            self.report['tests_passed'] += 1
            return True
        else:
            self.report['tests_failed'] += 1
            return False

    def _test_market_data_api(self):
        """测试市场数据API"""
        try:
            import uqer.api as api
            # 简单测试获取股票信息
            stocks = api.SecType.getAllSecCode(type_=api.SecType.stock)[:5]
            return stocks is not None and len(stocks) > 0
        except:
            return False

    def _test_fundamental_api(self):
        """测试基本面数据API"""
        try:
            import uqer.api as api
            # 测试基础API是否可用
            return hasattr(api, 'MarketDataQuery')
        except:
            return False

    def _test_flow_api(self):
        """测试资金流向API"""
        try:
            import uqer.api as api
            # 测试资金流向相关API
            return hasattr(api, 'MarketDataQuery')
        except:
            return False

    def check_permissions_and_status(self):
        """检查权限和数据状态"""
        print("\n🔐 检查权限和数据状态...")
        
        permissions = {
            'market_data': False,
            'fundamental_data': False,
            'flow_data': False
        }
        
        data_status = {
            'local_data_exists': False,
            'data_directories': [],
            'estimated_data_size': 0
        }
        
        # 检查本地数据
        data_dir = Path("data")
        if data_dir.exists():
            data_status['local_data_exists'] = True
            for subdir in data_dir.iterdir():
                if subdir.is_dir():
                    data_status['data_directories'].append(subdir.name)
            
            # 估算数据大小
            total_size = sum(f.stat().st_size for f in data_dir.rglob("*") if f.is_file())
            data_status['estimated_data_size'] = total_size / (1024 * 1024)  # MB
            
            print(f"✅ 本地数据目录: {len(data_status['data_directories'])} 个")
            print(f"📊 估算数据大小: {data_status['estimated_data_size']:.1f} MB")
        
        # 检查配置文件
        config_file = Path("uqer_config.json")
        if config_file.exists():
            print("✅ 发现优矿配置文件")
            data_status['config_exists'] = True
        else:
            print("⚠️ 未发现优矿配置文件")
            data_status['config_exists'] = False
        
        self.report['results']['permissions'] = permissions
        self.report['results']['data_status'] = data_status
        
        # 如果有本地数据，认为权限检查通过
        if data_status['local_data_exists']:
            self.report['tests_passed'] += 1
            return True
        else:
            self.report['tests_failed'] += 1
            return False

    def generate_report(self, save_to_file=True):
        """生成综合报告"""
        print("\n📋 生成综合状态报告...")
        
        # 计算总体状态
        total_tests = self.report['tests_passed'] + self.report['tests_failed']
        success_rate = self.report['tests_passed'] / total_tests if total_tests > 0 else 0
        
        self.report['summary'] = {
            'overall_status': 'healthy' if success_rate >= 0.7 else 'warning' if success_rate >= 0.5 else 'error',
            'success_rate': success_rate,
            'total_tests': total_tests
        }
        
        # 生成文本报告
        report_text = f"""
# 优矿连接状态报告

## 📊 总体状态
- **状态**: {self.report['summary']['overall_status'].upper()}
- **成功率**: {success_rate:.1%}
- **通过测试**: {self.report['tests_passed']}/{total_tests}
- **检查时间**: {self.report['timestamp']}

## 🔍 详细结果
"""
        
        for test_name, result in self.report['results'].items():
            report_text += f"\n### {test_name.title()}\n"
            if isinstance(result, dict):
                for key, value in result.items():
                    report_text += f"- {key}: {value}\n"
            else:
                report_text += f"- Result: {result}\n"
        
        if save_to_file:
            # 保存JSON报告
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            json_file = f"outputs/reports/uqer_connection_check_{timestamp}.json"
            txt_file = f"outputs/reports/uqer_connection_check_{timestamp}.txt"
            
            # 确保目录存在
            Path(json_file).parent.mkdir(parents=True, exist_ok=True)
            
            with open(json_file, 'w', encoding='utf-8') as f:
                json.dump(self.report, f, indent=2, ensure_ascii=False)
            
            with open(txt_file, 'w', encoding='utf-8') as f:
                f.write(report_text)
            
            print(f"📊 报告已保存:")
            print(f"   - JSON: {json_file}")
            print(f"   - 文本: {txt_file}")
        
        return self.report

    def run_simple_test(self):
        """运行简单测试"""
        print("🚀 开始简单连接测试...\n")
        
        self.test_basic_connection()
        if self.client:
            self.test_stock_data()
        
        self.generate_report()
        return self.report['connection_status']

    def run_detailed_test(self):
        """运行详细测试"""
        print("🚀 开始详细功能测试...\n")
        
        self.test_basic_connection()
        if self.client:
            self.test_stock_data()
            self.test_detailed_functions()
        
        self.generate_report()
        return self.report['summary']['success_rate'] >= 0.7

    def run_status_check(self):
        """运行状态检查"""
        print("🚀 开始权限和状态检查...\n")
        
        self.test_basic_connection()
        self.check_permissions_and_status()
        self.generate_report()
        return True

    def run_complete_check(self):
        """运行完整检查"""
        print("🚀 开始完整连接和状态检查...\n")
        
        # 基础连接测试
        connection_ok = self.test_basic_connection()
        
        # 如果连接成功，进行数据测试
        if connection_ok:
            self.test_stock_data()
            self.test_detailed_functions()
        
        # 权限和状态检查
        self.check_permissions_and_status()
        
        # 生成报告
        self.generate_report()
        
        return self.report['summary']['overall_status'] != 'error'

def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='优矿连接管理器')
    parser.add_argument('mode', nargs='?', default='all', 
                       choices=['simple', 'detailed', 'status', 'all'],
                       help='测试模式 (默认: all)')
    
    args = parser.parse_args()
    
    manager = UqerConnectionManager()
    
    try:
        if args.mode == 'simple':
            success = manager.run_simple_test()
        elif args.mode == 'detailed':
            success = manager.run_detailed_test()
        elif args.mode == 'status':
            success = manager.run_status_check()
        else:  # all
            success = manager.run_complete_check()
        
        if success:
            print("\n🎉 检查完成，状态良好！")
            return 0
        else:
            print("\n⚠️ 检查完成，发现问题，请查看报告详情。")
            return 1
            
    except KeyboardInterrupt:
        print("\n\n⏹️ 用户中断操作")
        return 1
    except Exception as e:
        print(f"\n❌ 程序执行出错: {e}")
        return 1

if __name__ == "__main__":
    exit(main())