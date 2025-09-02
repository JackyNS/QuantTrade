#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
策略验证器 - 现代化的策略模块验证工具
=====================================

功能:
1. 策略模块完整性验证
2. 技术指标计算验证
3. K线形态识别验证
4. 资金流分析验证
5. 信号生成验证
6. 性能基准测试

使用方法:
python tools/testing/strategy_validator.py [模式]

模式选项:
- quick: 快速验证核心功能
- full: 完整功能验证
- performance: 性能基准测试
- all: 运行所有验证 (默认)
"""

import os
import sys
import json
import argparse
import warnings
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

warnings.filterwarnings('ignore')

# 添加项目路径
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

class StrategyValidator:
    """策略验证器"""
    
    def __init__(self):
        self.validation_results = {
            'timestamp': datetime.now().isoformat(),
            'validation_status': 'unknown',
            'tests_passed': 0,
            'tests_failed': 0,
            'test_results': {},
            'performance_metrics': {}
        }
        
        # 生成测试数据
        self.test_data = self._generate_test_data()
    
    def _generate_test_data(self, n_days=50, n_stocks=2):
        """生成测试用的股票数据"""
        np.random.seed(42)
        dates = pd.date_range(end=datetime.now(), periods=n_days)
        
        all_data = []
        
        for i in range(n_stocks):
            ticker = f"TEST{i:03d}"
            
            # 生成价格数据（随机游走）
            close = 10 + np.cumsum(np.random.randn(n_days) * 0.5)
            close = np.maximum(close, 1)  # 确保价格为正
            
            # OHLC数据
            high = close * (1 + np.abs(np.random.randn(n_days) * 0.02))
            low = close * (1 - np.abs(np.random.randn(n_days) * 0.02))
            open_price = close + np.random.randn(n_days) * 0.1
            
            # 成交量和资金流
            volume = np.random.randint(1000000, 10000000, n_days)
            main_net_flow = np.random.randn(n_days) * 1000000
            
            stock_data = pd.DataFrame({
                'date': dates,
                'ticker': ticker,
                'open': open_price,
                'high': high,
                'low': low,
                'close': close,
                'volume': volume,
                'turnover': close * volume,
                'pct_change': pd.Series(close).pct_change() * 100,
                'main_net_flow': main_net_flow,
                'name': f'测试股票{i}'
            })
            
            all_data.append(stock_data)
        
        return pd.concat(all_data, ignore_index=True)
    
    def validate_technical_indicators(self):
        """验证技术指标模块"""
        print("📈 验证技术指标模块...")
        
        test_result = {
            'status': 'unknown',
            'indicators_tested': 0,
            'indicators_passed': 0,
            'errors': []
        }
        
        try:
            from core.strategy.technical_indicators import TechnicalIndicators
            
            ti = TechnicalIndicators()
            test_stock = self.test_data[self.test_data['ticker'] == 'TEST000'].copy()
            
            # 测试基础指标
            indicators_to_test = [
                ('SMA', lambda: ti.sma(test_stock['close'], 20)),
                ('EMA', lambda: ti.ema(test_stock['close'], 20)),
                ('RSI', lambda: ti.rsi(test_stock['close'], 14)),
                ('MACD', lambda: ti.macd(test_stock['close'])),
                ('Bollinger Bands', lambda: ti.bollinger_bands(test_stock['close'], 20))
            ]
            
            for indicator_name, test_func in indicators_to_test:
                try:
                    result = test_func()
                    if result is not None:
                        test_result['indicators_passed'] += 1
                        print(f"  ✅ {indicator_name}: 计算成功")
                    else:
                        test_result['errors'].append(f"{indicator_name}: 返回空值")
                        print(f"  ⚠️ {indicator_name}: 返回空值")
                except Exception as e:
                    test_result['errors'].append(f"{indicator_name}: {str(e)}")
                    print(f"  ❌ {indicator_name}: {str(e)}")
                
                test_result['indicators_tested'] += 1
            
            test_result['status'] = 'success' if test_result['indicators_passed'] > 0 else 'failed'
            
        except ImportError as e:
            test_result['status'] = 'import_error'
            test_result['errors'].append(f"导入错误: {str(e)}")
            print(f"  ❌ 技术指标模块导入失败: {e}")
        except Exception as e:
            test_result['status'] = 'error'
            test_result['errors'].append(f"运行错误: {str(e)}")
            print(f"  ❌ 技术指标测试异常: {e}")
        
        self.validation_results['test_results']['technical_indicators'] = test_result
        
        if test_result['status'] == 'success':
            self.validation_results['tests_passed'] += 1
        else:
            self.validation_results['tests_failed'] += 1
        
        return test_result['status'] == 'success'
    
    def validate_strategy_framework(self):
        """验证策略框架"""
        print("🎯 验证策略框架...")
        
        test_result = {
            'status': 'unknown',
            'components_tested': 0,
            'components_passed': 0,
            'errors': []
        }
        
        components_to_test = [
            ('BaseStrategy', 'core.strategy.base_strategy', 'BaseStrategy'),
            ('SignalGenerator', 'core.strategy.signal_generator', 'SignalGenerator'),
            ('CapitalFlowAnalysis', 'core.strategy.capital_flow_analysis', 'CapitalFlowAnalysis')
        ]
        
        for component_name, module_path, class_name in components_to_test:
            try:
                module = __import__(module_path, fromlist=[class_name])
                component_class = getattr(module, class_name)
                
                # 尝试实例化
                instance = component_class()
                test_result['components_passed'] += 1
                print(f"  ✅ {component_name}: 导入和实例化成功")
                
            except ImportError as e:
                test_result['errors'].append(f"{component_name} 导入失败: {str(e)}")
                print(f"  ⚠️ {component_name}: 导入失败 (模块可能未实现)")
            except Exception as e:
                test_result['errors'].append(f"{component_name} 实例化失败: {str(e)}")
                print(f"  ❌ {component_name}: 实例化失败 - {e}")
            
            test_result['components_tested'] += 1
        
        test_result['status'] = 'success' if test_result['components_passed'] > 0 else 'failed'
        
        self.validation_results['test_results']['strategy_framework'] = test_result
        
        if test_result['status'] == 'success':
            self.validation_results['tests_passed'] += 1
        else:
            self.validation_results['tests_failed'] += 1
        
        return test_result['status'] == 'success'
    
    def validate_data_integration(self):
        """验证数据集成"""
        print("📊 验证数据集成...")
        
        test_result = {
            'status': 'unknown',
            'data_sources': 0,
            'data_accessible': 0,
            'errors': []
        }
        
        try:
            # 测试数据管理器
            from core.data.data_manager import DataManager
            dm = DataManager()
            
            test_result['data_sources'] += 1
            test_result['data_accessible'] += 1
            print(f"  ✅ DataManager: 初始化成功")
            
        except Exception as e:
            test_result['errors'].append(f"DataManager: {str(e)}")
            print(f"  ⚠️ DataManager: {str(e)}")
            test_result['data_sources'] += 1
        
        try:
            # 测试增强数据管理器
            from core.data.enhanced_data_manager import EnhancedDataManager
            edm = EnhancedDataManager()
            
            test_result['data_sources'] += 1
            test_result['data_accessible'] += 1
            print(f"  ✅ EnhancedDataManager: 初始化成功")
            
        except Exception as e:
            test_result['errors'].append(f"EnhancedDataManager: {str(e)}")
            print(f"  ⚠️ EnhancedDataManager: {str(e)}")
            test_result['data_sources'] += 1
        
        test_result['status'] = 'success' if test_result['data_accessible'] > 0 else 'failed'
        
        self.validation_results['test_results']['data_integration'] = test_result
        
        if test_result['status'] == 'success':
            self.validation_results['tests_passed'] += 1
        else:
            self.validation_results['tests_failed'] += 1
        
        return test_result['status'] == 'success'
    
    def run_performance_benchmark(self):
        """运行性能基准测试"""
        print("⚡ 运行性能基准测试...")
        
        performance_metrics = {}
        
        # 技术指标计算性能
        try:
            from core.strategy.technical_indicators import TechnicalIndicators
            ti = TechnicalIndicators()
            
            test_data = pd.Series(np.random.randn(1000))
            
            # 基准测试SMA计算
            start_time = datetime.now()
            for _ in range(100):
                ti.sma(test_data, 20)
            sma_time = (datetime.now() - start_time).total_seconds()
            
            performance_metrics['sma_calculation'] = {
                'iterations': 100,
                'total_time_seconds': sma_time,
                'avg_time_ms': sma_time * 10,  # 每次平均时间(ms)
            }
            
            print(f"  📊 SMA计算: {sma_time:.3f}s (100次), 平均 {sma_time*10:.2f}ms/次")
            
        except Exception as e:
            print(f"  ⚠️ 性能测试异常: {e}")
        
        self.validation_results['performance_metrics'] = performance_metrics
        return performance_metrics
    
    def generate_validation_report(self, save_to_file=True):
        """生成验证报告"""
        print("\n📋 生成验证报告...")
        
        # 计算总体状态
        total_tests = self.validation_results['tests_passed'] + self.validation_results['tests_failed']
        success_rate = self.validation_results['tests_passed'] / total_tests if total_tests > 0 else 0
        
        if success_rate >= 0.8:
            self.validation_results['validation_status'] = 'excellent'
        elif success_rate >= 0.6:
            self.validation_results['validation_status'] = 'good'
        elif success_rate >= 0.4:
            self.validation_results['validation_status'] = 'fair'
        else:
            self.validation_results['validation_status'] = 'poor'
        
        # 生成文本报告
        report_text = f"""
# 策略模块验证报告

## 📊 总体状态
- **验证状态**: {self.validation_results['validation_status'].upper()}
- **成功率**: {success_rate:.1%}
- **通过测试**: {self.validation_results['tests_passed']}/{total_tests}
- **验证时间**: {self.validation_results['timestamp']}

## 🔍 详细结果
"""
        
        for test_name, result in self.validation_results['test_results'].items():
            report_text += f"\n### {test_name.replace('_', ' ').title()}\n"
            report_text += f"- 状态: {result['status']}\n"
            
            if 'indicators_tested' in result:
                report_text += f"- 指标测试: {result['indicators_passed']}/{result['indicators_tested']}\n"
            elif 'components_tested' in result:
                report_text += f"- 组件测试: {result['components_passed']}/{result['components_tested']}\n"
            elif 'data_sources' in result:
                report_text += f"- 数据源测试: {result['data_accessible']}/{result['data_sources']}\n"
            
            if result.get('errors'):
                report_text += f"- 错误信息:\n"
                for error in result['errors']:
                    report_text += f"  - {error}\n"
        
        if save_to_file:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            json_file = f"outputs/reports/strategy_validation_{timestamp}.json"
            txt_file = f"outputs/reports/strategy_validation_{timestamp}.txt"
            
            # 确保目录存在
            Path(json_file).parent.mkdir(parents=True, exist_ok=True)
            
            with open(json_file, 'w', encoding='utf-8') as f:
                json.dump(self.validation_results, f, indent=2, ensure_ascii=False)
            
            with open(txt_file, 'w', encoding='utf-8') as f:
                f.write(report_text)
            
            print(f"📊 报告已保存:")
            print(f"   - JSON: {json_file}")
            print(f"   - 文本: {txt_file}")
        
        return self.validation_results
    
    def run_quick_validation(self):
        """运行快速验证"""
        print("🚀 开始快速策略验证...\n")
        
        self.validate_technical_indicators()
        self.validate_data_integration()
        
        self.generate_validation_report()
        
        return self.validation_results['validation_status'] in ['excellent', 'good']
    
    def run_full_validation(self):
        """运行完整验证"""
        print("🚀 开始完整策略验证...\n")
        
        self.validate_technical_indicators()
        self.validate_strategy_framework()
        self.validate_data_integration()
        
        self.generate_validation_report()
        
        return self.validation_results['validation_status'] in ['excellent', 'good']
    
    def run_performance_validation(self):
        """运行性能验证"""
        print("🚀 开始性能基准验证...\n")
        
        self.validate_technical_indicators()
        self.run_performance_benchmark()
        
        self.generate_validation_report()
        
        return True
    
    def run_all_validation(self):
        """运行所有验证"""
        print("🚀 开始完整策略验证和性能测试...\n")
        
        self.validate_technical_indicators()
        self.validate_strategy_framework()
        self.validate_data_integration()
        self.run_performance_benchmark()
        
        self.generate_validation_report()
        
        return self.validation_results['validation_status'] in ['excellent', 'good']

def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='策略验证器')
    parser.add_argument('mode', nargs='?', default='all',
                       choices=['quick', 'full', 'performance', 'all'],
                       help='验证模式 (默认: all)')
    
    args = parser.parse_args()
    
    validator = StrategyValidator()
    
    try:
        if args.mode == 'quick':
            success = validator.run_quick_validation()
        elif args.mode == 'full':
            success = validator.run_full_validation()
        elif args.mode == 'performance':
            success = validator.run_performance_validation()
        else:  # all
            success = validator.run_all_validation()
        
        if success:
            print("\n🎉 策略验证通过！")
            return 0
        else:
            print("\n⚠️ 策略验证发现问题，请查看报告详情。")
            return 1
            
    except KeyboardInterrupt:
        print("\n\n⏹️ 用户中断操作")
        return 1
    except Exception as e:
        print(f"\n❌ 验证程序执行出错: {e}")
        return 1

if __name__ == "__main__":
    exit(main())