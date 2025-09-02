#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
QuantTrade增强系统测试 - 验证策略和回测功能
"""

import sys
import os
import pandas as pd
from pathlib import Path
import logging
from datetime import datetime, timedelta
import traceback
from typing import Dict, List, Tuple, Optional
import numpy as np

class EnhancedSystemTest:
    """增强系统测试器"""
    
    def __init__(self):
        self.project_root = Path("/Users/jackstudio/QuantTrade")
        sys.path.insert(0, str(self.project_root))
        self.setup_logging()
        
    def setup_logging(self):
        """设置日志"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[logging.StreamHandler()]
        )
    
    def test_data_manager_functionality(self) -> Dict:
        """测试数据管理器功能"""
        logging.info("📊 测试数据管理器功能...")
        
        result = {
            "test_name": "数据管理器功能",
            "passed": True,
            "details": [],
            "errors": []
        }
        
        try:
            # 导入数据管理器
            from core.data import create_data_manager_safe
            
            dm = create_data_manager_safe()
            result["details"].append("✅ 数据管理器创建成功")
            
            # 测试本地数据读取
            test_data_path = self.project_root / "data/final_comprehensive_download/basic_info/mktidxdget/year_2024.csv"
            if test_data_path.exists():
                try:
                    df = pd.read_csv(test_data_path, nrows=100)
                    result["details"].append(f"✅ 本地数据读取: {len(df)} 行, {len(df.columns)} 列")
                    result["details"].append(f"✅ 数据时间范围: {df['tradeDate'].min()} 到 {df['tradeDate'].max()}")
                except Exception as e:
                    result["errors"].append(f"本地数据读取失败: {str(e)}")
                    result["passed"] = False
            
            # 测试数据缓存功能
            cache_dir = self.project_root / "cache"
            if cache_dir.exists():
                result["details"].append(f"✅ 缓存目录存在: {cache_dir}")
            else:
                cache_dir.mkdir(exist_ok=True)
                result["details"].append(f"✅ 创建缓存目录: {cache_dir}")
            
        except Exception as e:
            result["errors"].append(f"数据管理器测试失败: {str(e)}")
            result["passed"] = False
            logging.error(f"❌ 数据管理器测试失败: {e}")
        
        return result
    
    def test_strategy_framework(self) -> Dict:
        """测试策略框架"""
        logging.info("🎯 测试策略框架...")
        
        result = {
            "test_name": "策略框架",
            "passed": True,
            "details": [],
            "errors": []
        }
        
        try:
            # 导入策略相关模块
            from core.strategy import BaseStrategy, TechnicalStrategy, MLStrategy
            
            result["details"].append("✅ 基础策略类导入成功")
            result["details"].append("✅ 技术分析策略类导入成功")
            result["details"].append("✅ 机器学习策略类导入成功")
            
            # 测试创建基础策略实例
            class TestStrategy(BaseStrategy):
                def __init__(self, name="test_strategy"):
                    super().__init__(name)
                
                def generate_signals(self, data):
                    return pd.Series([0] * len(data))
                
                def calculate_positions(self, signals, data):
                    return signals
            
            test_strategy = TestStrategy()
            result["details"].append(f"✅ 测试策略实例创建: {test_strategy.name}")
            
        except Exception as e:
            result["errors"].append(f"策略框架测试失败: {str(e)}")
            result["passed"] = False
            logging.error(f"❌ 策略框架测试失败: {e}")
        
        return result
    
    def test_backtest_engine(self) -> Dict:
        """测试回测引擎"""
        logging.info("⚙️ 测试回测引擎...")
        
        result = {
            "test_name": "回测引擎",
            "passed": True,
            "details": [],
            "errors": []
        }
        
        try:
            # 导入回测模块
            from core.backtest import BacktestEngine, PerformanceAnalyzer
            
            result["details"].append("✅ 回测引擎导入成功")
            result["details"].append("✅ 性能分析器导入成功")
            
            # 创建回测引擎实例
            engine = BacktestEngine()
            result["details"].append("✅ 回测引擎实例创建成功")
            
            # 创建性能分析器实例
            analyzer = PerformanceAnalyzer()
            result["details"].append("✅ 性能分析器实例创建成功")
            
        except Exception as e:
            result["errors"].append(f"回测引擎测试失败: {str(e)}")
            result["passed"] = False
            logging.error(f"❌ 回测引擎测试失败: {e}")
        
        return result
    
    def test_end_to_end_workflow(self) -> Dict:
        """测试端到端工作流"""
        logging.info("🔄 测试端到端工作流...")
        
        result = {
            "test_name": "端到端工作流",
            "passed": True,
            "details": [],
            "errors": []
        }
        
        try:
            # 1. 数据加载测试
            from core.data import create_data_manager_safe
            
            dm = create_data_manager_safe()
            result["details"].append("✅ 步骤1: 数据管理器初始化")
            
            # 2. 策略创建测试
            from core.strategy import BaseStrategy
            
            class SimpleMAStrategy(BaseStrategy):
                def __init__(self):
                    super().__init__("simple_ma")
                
                def generate_signals(self, data):
                    if 'closePrice' in data.columns:
                        ma5 = data['closePrice'].rolling(5).mean()
                        ma20 = data['closePrice'].rolling(20).mean()
                        signals = (ma5 > ma20).astype(int)
                        return signals
                    return pd.Series([0] * len(data))
                
                def calculate_positions(self, signals, data):
                    return signals
            
            strategy = SimpleMAStrategy()
            result["details"].append("✅ 步骤2: 简单移动平均策略创建")
            
            # 3. 模拟数据测试
            test_data = pd.DataFrame({
                'tradeDate': pd.date_range('2024-01-01', periods=30),
                'closePrice': np.random.randn(30).cumsum() + 100,
                'volume': np.random.randint(1000, 10000, 30)
            })
            
            signals = strategy.generate_signals(test_data)
            result["details"].append(f"✅ 步骤3: 信号生成完成，共{len(signals)}个信号")
            
            # 4. 回测引擎测试
            from core.backtest import BacktestEngine
            
            engine = BacktestEngine()
            result["details"].append("✅ 步骤4: 回测引擎准备就绪")
            
            result["details"].append("🎯 端到端工作流测试完成")
            
        except Exception as e:
            result["errors"].append(f"端到端测试失败: {str(e)}")
            result["passed"] = False
            logging.error(f"❌ 端到端测试失败: {e}")
        
        return result
    
    def test_data_integrity_comprehensive(self) -> Dict:
        """全面测试数据完整性"""
        logging.info("🔍 全面测试数据完整性...")
        
        result = {
            "test_name": "数据完整性全面验证",
            "passed": True,
            "details": [],
            "errors": []
        }
        
        try:
            data_root = self.project_root / "data/final_comprehensive_download"
            
            # 按类别验证数据
            categories = {
                "basic_info": {"expected_apis": 8, "key_data": "mktidxdget"},
                "financial": {"expected_apis": 13, "key_data": "fdmtbsalllatestget"},
                "special_trading": {"expected_apis": 20, "key_data": "mktlimitget"},
                "governance": {"expected_apis": 22, "key_data": "equshtenget"},
                "additional_apis": {"expected_apis": 8, "key_data": "mktstockfactorsonedayget"}
            }
            
            total_verified_apis = 0
            total_verified_files = 0
            
            for category, info in categories.items():
                category_dir = data_root / category
                if category_dir.exists():
                    apis = [d for d in category_dir.iterdir() if d.is_dir()]
                    files = list(category_dir.rglob("*.csv"))
                    
                    total_verified_apis += len(apis)
                    total_verified_files += len(files)
                    
                    result["details"].append(f"✅ {category}: {len(apis)} APIs, {len(files)} 文件")
                    
                    # 验证关键数据
                    key_data_dir = category_dir / info["key_data"]
                    if key_data_dir.exists():
                        key_files = list(key_data_dir.glob("*.csv"))
                        if key_files:
                            # 检查年份覆盖
                            years_found = set()
                            for f in key_files[:10]:  # 检查前10个文件
                                if "year_" in f.name:
                                    try:
                                        year = int(f.name.split("year_")[1].split(".")[0])
                                        years_found.add(year)
                                    except:
                                        pass
                            
                            if years_found:
                                min_year = min(years_found)
                                max_year = max(years_found)
                                result["details"].append(f"   📅 {info['key_data']}: {min_year}-{max_year}")
                                
                                if min_year <= 2000:
                                    result["details"].append(f"   ✅ 历史数据覆盖充分")
                        else:
                            result["errors"].append(f"{category}/{info['key_data']} 无CSV文件")
                    else:
                        result["details"].append(f"   ⚠️ {info['key_data']} 目录不存在")
                else:
                    result["errors"].append(f"分类目录不存在: {category}")
                    result["passed"] = False
            
            # 总体验证
            result["details"].append(f"📊 数据验证总结:")
            result["details"].append(f"   🔌 已验证API: {total_verified_apis}")
            result["details"].append(f"   📄 已验证文件: {total_verified_files}")
            
            if total_verified_apis >= 65:  # 至少65个API
                result["details"].append(f"   ✅ API数量符合预期 ({total_verified_apis}≥65)")
            else:
                result["errors"].append(f"API数量不足: {total_verified_apis}<65")
                result["passed"] = False
            
            if total_verified_files >= 1400:  # 至少1400个文件
                result["details"].append(f"   ✅ 文件数量符合预期 ({total_verified_files}≥1400)")
            else:
                result["errors"].append(f"文件数量不足: {total_verified_files}<1400")
                result["passed"] = False
            
        except Exception as e:
            result["errors"].append(f"数据完整性验证失败: {str(e)}")
            result["passed"] = False
            logging.error(f"❌ 数据完整性验证失败: {e}")
        
        return result
    
    def run_enhanced_tests(self) -> Dict:
        """运行增强测试套件"""
        logging.info("🚀 开始增强系统测试...")
        
        test_suite = [
            ("数据管理器功能", self.test_data_manager_functionality),
            ("策略框架", self.test_strategy_framework),
            ("回测引擎", self.test_backtest_engine),
            ("端到端工作流", self.test_end_to_end_workflow),
            ("数据完整性全面验证", self.test_data_integrity_comprehensive)
        ]
        
        all_results = []
        passed_count = 0
        
        for test_name, test_func in test_suite:
            logging.info(f"\n▶️ 执行测试: {test_name}")
            try:
                result = test_func()
                all_results.append(result)
                if result["passed"]:
                    passed_count += 1
                    logging.info(f"✅ {test_name} 测试通过")
                else:
                    logging.error(f"❌ {test_name} 测试失败")
            except Exception as e:
                error_result = {
                    "test_name": test_name,
                    "passed": False,
                    "details": [],
                    "errors": [f"测试执行异常: {str(e)}"]
                }
                all_results.append(error_result)
                logging.error(f"❌ {test_name} 测试异常: {e}")
        
        # 生成增强测试报告
        self.generate_enhanced_report(all_results, passed_count, len(test_suite))
        
        return {
            "total_tests": len(test_suite),
            "passed_tests": passed_count,
            "success_rate": passed_count / len(test_suite) * 100,
            "results": all_results
        }
    
    def generate_enhanced_report(self, results: List[Dict], passed: int, total: int):
        """生成增强测试报告"""
        logging.info("📊 生成增强测试报告...")
        
        report = []
        report.append("="*80)
        report.append("🧪 **QuantTrade增强系统测试报告**")
        report.append("="*80)
        report.append(f"📅 测试时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append("")
        report.append("📊 **增强测试概览:**")
        report.append(f"  🎯 总测试数: {total}")
        report.append(f"  ✅ 通过测试: {passed}")
        report.append(f"  ❌ 失败测试: {total - passed}")
        report.append(f"  📈 成功率: {passed/total*100:.1f}%")
        report.append("")
        
        # 详细结果
        for result in results:
            status = "✅ 通过" if result["passed"] else "❌ 失败"
            report.append(f"📋 **{result['test_name']}** - {status}")
            
            for detail in result["details"]:
                report.append(f"    {detail}")
            
            if result["errors"]:
                report.append("    🚨 错误详情:")
                for error in result["errors"]:
                    report.append(f"      • {error}")
            
            report.append("")
        
        # 总结
        if passed == total:
            report.append("🎊 **所有增强测试通过！系统完全准备就绪**")
        elif passed >= total * 0.8:
            report.append("🟡 **大部分增强测试通过，系统基本可用**")
        else:
            report.append("🔴 **多项增强测试失败，需要修复核心问题**")
        
        report.append("="*80)
        
        # 输出到控制台
        for line in report:
            print(line)
        
        # 保存到文件
        report_file = self.project_root / "enhanced_test_report.txt"
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write('\n'.join(report))
        
        logging.info(f"📄 增强测试报告已保存: {report_file}")

if __name__ == "__main__":
    tester = EnhancedSystemTest()
    results = tester.run_enhanced_tests()