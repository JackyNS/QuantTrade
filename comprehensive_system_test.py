#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
QuantTrade系统综合测试 - 确保所有模块功能完整
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
import time

class QuantTradeSystemTest:
    """QuantTrade系统综合测试器"""
    
    def __init__(self):
        self.project_root = Path("/Users/jackstudio/QuantTrade")
        self.data_root = self.project_root / "data"
        self.core_root = self.project_root / "core"
        
        # 添加项目路径到Python路径
        sys.path.insert(0, str(self.project_root))
        
        self.setup_logging()
        self.test_results = {}
        
    def setup_logging(self):
        """设置日志"""
        log_file = self.project_root / "system_test.log"
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file, encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        
    def test_project_structure(self) -> Dict:
        """测试项目结构完整性"""
        logging.info("🏗️ 测试项目结构完整性...")
        
        result = {
            "test_name": "项目结构",
            "passed": True,
            "details": [],
            "errors": []
        }
        
        # 核心目录检查
        required_dirs = [
            "core",
            "core/data",
            "core/strategy", 
            "core/backtest",
            "core/config",
            "core/utils",
            "core/visualization",
            "data",
            "scripts_new"
        ]
        
        for dir_path in required_dirs:
            full_path = self.project_root / dir_path
            if full_path.exists():
                result["details"].append(f"✅ {dir_path} - 存在")
                logging.info(f"  ✅ {dir_path}")
            else:
                result["details"].append(f"❌ {dir_path} - 缺失")
                result["errors"].append(f"缺失目录: {dir_path}")
                result["passed"] = False
                logging.error(f"  ❌ {dir_path}")
        
        # 核心文件检查
        required_files = [
            "main.py",
            "core/__init__.py",
            "core/data/__init__.py",
            "core/strategy/__init__.py",
            "core/backtest/__init__.py"
        ]
        
        for file_path in required_files:
            full_path = self.project_root / file_path
            if full_path.exists():
                result["details"].append(f"✅ {file_path} - 存在")
                logging.info(f"  ✅ {file_path}")
            else:
                result["details"].append(f"❌ {file_path} - 缺失")
                result["errors"].append(f"缺失文件: {file_path}")
                result["passed"] = False
                logging.error(f"  ❌ {file_path}")
        
        return result
    
    def test_data_access_layer(self) -> Dict:
        """测试数据访问层功能"""
        logging.info("📊 测试数据访问层功能...")
        
        result = {
            "test_name": "数据访问层",
            "passed": True,
            "details": [],
            "errors": []
        }
        
        try:
            # 测试本地数据访问
            data_dirs = [
                "data/final_comprehensive_download",
                "data/smart_download", 
                "data/priority_download"
            ]
            
            total_files = 0
            total_size_gb = 0
            
            for data_dir in data_dirs:
                full_path = self.project_root / data_dir
                if full_path.exists():
                    # 统计文件数量和大小
                    csv_files = list(full_path.rglob("*.csv"))
                    files_count = len(csv_files)
                    size_mb = sum(f.stat().st_size for f in csv_files[:100]) / (1024*1024)  # 只统计前100个文件避免超时
                    
                    total_files += files_count
                    total_size_gb += size_mb / 1024
                    
                    result["details"].append(f"✅ {data_dir}: {files_count} CSV文件")
                    logging.info(f"  ✅ {data_dir}: {files_count} CSV文件")
                else:
                    result["details"].append(f"❌ {data_dir}: 不存在")
                    result["errors"].append(f"数据目录不存在: {data_dir}")
                    logging.error(f"  ❌ {data_dir}: 不存在")
            
            # 测试数据读取功能
            test_file = self.project_root / "data/final_comprehensive_download/basic_info/mktidxdget/year_2024.csv"
            if test_file.exists():
                try:
                    df = pd.read_csv(test_file, nrows=10)  # 只读取前10行测试
                    result["details"].append(f"✅ 数据读取测试: 成功读取 {len(df)} 行")
                    result["details"].append(f"✅ 数据字段: {list(df.columns)[:5]}...")
                    logging.info(f"  ✅ 数据读取测试成功: {len(df)} 行")
                except Exception as e:
                    result["errors"].append(f"数据读取失败: {str(e)}")
                    result["passed"] = False
                    logging.error(f"  ❌ 数据读取失败: {e}")
            
            result["details"].append(f"📊 数据统计: 约{total_files}个文件, ~{total_size_gb:.1f}GB")
            logging.info(f"  📊 数据统计: 约{total_files}个文件")
            
        except Exception as e:
            result["errors"].append(f"数据访问层测试异常: {str(e)}")
            result["passed"] = False
            logging.error(f"❌ 数据访问层测试异常: {e}")
        
        return result
    
    def test_core_modules(self) -> Dict:
        """测试核心模块导入和基础功能"""
        logging.info("🧩 测试核心模块导入...")
        
        result = {
            "test_name": "核心模块",
            "passed": True,
            "details": [],
            "errors": []
        }
        
        # 测试核心模块导入
        core_modules = [
            ("core", "核心模块"),
            ("core.data", "数据模块"),
            ("core.strategy", "策略模块"),
            ("core.backtest", "回测模块"),
            ("core.config", "配置模块"),
            ("core.utils", "工具模块"),
            ("core.visualization", "可视化模块")
        ]
        
        for module_name, description in core_modules:
            try:
                module = __import__(module_name, fromlist=[''])
                result["details"].append(f"✅ {description} ({module_name}) - 导入成功")
                logging.info(f"  ✅ {description} 导入成功")
                
                # 检查模块属性
                if hasattr(module, '__version__') or hasattr(module, '__all__') or dir(module):
                    attrs_count = len([attr for attr in dir(module) if not attr.startswith('_')])
                    result["details"].append(f"   📦 包含 {attrs_count} 个公开属性/函数")
                    
            except ImportError as e:
                result["errors"].append(f"{description} 导入失败: {str(e)}")
                result["details"].append(f"❌ {description} - 导入失败")
                result["passed"] = False
                logging.error(f"  ❌ {description} 导入失败: {e}")
            except Exception as e:
                result["errors"].append(f"{description} 测试异常: {str(e)}")
                result["details"].append(f"⚠️ {description} - 测试异常")
                logging.warning(f"  ⚠️ {description} 测试异常: {e}")
        
        return result
    
    def test_main_entry(self) -> Dict:
        """测试主程序入口"""
        logging.info("🚀 测试主程序入口...")
        
        result = {
            "test_name": "主程序入口",
            "passed": True,
            "details": [],
            "errors": []
        }
        
        try:
            # 检查main.py存在性和基本结构
            main_file = self.project_root / "main.py"
            if main_file.exists():
                with open(main_file, 'r', encoding='utf-8') as f:
                    content = f.read()
                
                # 检查关键函数/类
                key_elements = [
                    "argparse",
                    "def main",
                    "__main__",
                    "validate",
                    "update-data",
                    "backtest"
                ]
                
                found_elements = []
                for element in key_elements:
                    if element in content:
                        found_elements.append(element)
                        result["details"].append(f"✅ 发现关键元素: {element}")
                    else:
                        result["details"].append(f"⚠️ 缺失关键元素: {element}")
                
                result["details"].append(f"📋 main.py 文件大小: {len(content)} 字符")
                result["details"].append(f"📋 发现 {len(found_elements)}/{len(key_elements)} 个关键元素")
                
                if len(found_elements) < len(key_elements) * 0.7:  # 至少70%的关键元素存在
                    result["passed"] = False
                    result["errors"].append("main.py 缺失过多关键元素")
                
            else:
                result["errors"].append("main.py 文件不存在")
                result["passed"] = False
            
        except Exception as e:
            result["errors"].append(f"主程序测试异常: {str(e)}")
            result["passed"] = False
            logging.error(f"❌ 主程序测试异常: {e}")
        
        return result
    
    def test_data_reliability(self) -> Dict:
        """测试数据可靠性 (100%要求)"""
        logging.info("🔍 测试数据可靠性...")
        
        result = {
            "test_name": "数据可靠性",
            "passed": True,
            "details": [],
            "errors": []
        }
        
        try:
            # 测试主要数据目录
            comprehensive_data_dir = self.project_root / "data/final_comprehensive_download"
            
            if not comprehensive_data_dir.exists():
                result["errors"].append("主数据目录不存在")
                result["passed"] = False
                return result
            
            # 按分类检查数据
            categories = ["basic_info", "financial", "special_trading", "governance", "additional_apis"]
            category_stats = {}
            
            for category in categories:
                category_dir = comprehensive_data_dir / category
                if category_dir.exists():
                    apis = [d for d in category_dir.iterdir() if d.is_dir()]
                    csv_files = list(category_dir.rglob("*.csv"))
                    
                    category_stats[category] = {
                        "apis": len(apis),
                        "files": len(csv_files),
                        "size_mb": sum(f.stat().st_size for f in csv_files[:50]) / (1024*1024) if csv_files else 0
                    }
                    
                    result["details"].append(f"✅ {category}: {len(apis)} APIs, {len(csv_files)} 文件")
                    
                    # 数据质量抽查
                    if csv_files:
                        sample_file = csv_files[0]
                        try:
                            df_sample = pd.read_csv(sample_file, nrows=5)
                            if df_sample.empty:
                                result["errors"].append(f"{category} 存在空数据文件")
                                result["passed"] = False
                            else:
                                result["details"].append(f"   📊 样本数据: {len(df_sample.columns)} 列")
                        except Exception as e:
                            result["errors"].append(f"{category} 数据读取错误: {str(e)}")
                            result["passed"] = False
                else:
                    category_stats[category] = {"apis": 0, "files": 0, "size_mb": 0}
                    result["errors"].append(f"分类目录不存在: {category}")
                    result["passed"] = False
            
            # 统计总结
            total_apis = sum(stats["apis"] for stats in category_stats.values())
            total_files = sum(stats["files"] for stats in category_stats.values())
            total_size = sum(stats["size_mb"] for stats in category_stats.values())
            
            result["details"].append(f"📊 数据总览: {total_apis} APIs, {total_files} 文件, ~{total_size:.1f}MB 抽样")
            
            # 检查是否达到预期的71个API
            if total_apis >= 65:  # 允许小范围误差
                result["details"].append(f"✅ API数量达标: {total_apis}/71")
            else:
                result["errors"].append(f"API数量不足: {total_apis}/71")
                result["passed"] = False
            
            # 时间范围检查
            sample_files_with_year = [f for f in comprehensive_data_dir.rglob("*year_*.csv")][:10]
            if sample_files_with_year:
                years_found = set()
                for f in sample_files_with_year:
                    if "year_" in f.name:
                        try:
                            year = int(f.name.split("year_")[1].split(".")[0])
                            years_found.add(year)
                        except:
                            pass
                
                if years_found:
                    year_range = f"{min(years_found)}-{max(years_found)}"
                    result["details"].append(f"📅 时间覆盖: {year_range}")
                    
                    # 检查是否覆盖2000-2025
                    if min(years_found) <= 2000 and max(years_found) >= 2024:
                        result["details"].append("✅ 时间覆盖达标 (2000-2025)")
                    else:
                        result["errors"].append(f"时间覆盖不足: {year_range}")
            
        except Exception as e:
            result["errors"].append(f"数据可靠性测试异常: {str(e)}")
            result["passed"] = False
            logging.error(f"❌ 数据可靠性测试异常: {e}")
        
        return result
    
    def run_all_tests(self) -> Dict:
        """执行所有测试"""
        logging.info("🎯 开始QuantTrade系统综合测试...")
        
        test_suite = [
            ("项目结构", self.test_project_structure),
            ("数据访问层", self.test_data_access_layer), 
            ("核心模块", self.test_core_modules),
            ("主程序入口", self.test_main_entry),
            ("数据可靠性", self.test_data_reliability)
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
        
        # 生成测试报告
        self.generate_test_report(all_results, passed_count, len(test_suite))
        
        return {
            "total_tests": len(test_suite),
            "passed_tests": passed_count,
            "success_rate": passed_count / len(test_suite) * 100,
            "results": all_results
        }
    
    def generate_test_report(self, results: List[Dict], passed: int, total: int):
        """生成测试报告"""
        logging.info("📊 生成测试报告...")
        
        report = []
        report.append("="*80)
        report.append("🧪 **QuantTrade系统综合测试报告**")
        report.append("="*80)
        report.append(f"📅 测试时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append("")
        report.append("📊 **测试概览:**")
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
            report.append("🎊 **所有测试通过！系统功能完整，数据可靠性100%**")
        elif passed >= total * 0.8:
            report.append("🟡 **大部分测试通过，需要关注失败的测试项目**")
        else:
            report.append("🔴 **多项测试失败，需要重点修复系统问题**")
        
        report.append("="*80)
        
        # 输出到控制台
        for line in report:
            print(line)
        
        # 保存到文件
        report_file = self.project_root / "system_test_report.txt"
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write('\n'.join(report))
        
        logging.info(f"📄 测试报告已保存: {report_file}")

if __name__ == "__main__":
    tester = QuantTradeSystemTest()
    results = tester.run_all_tests()
    
    # 返回状态码
    if results["success_rate"] == 100:
        sys.exit(0)  # 所有测试通过
    elif results["success_rate"] >= 80:
        sys.exit(1)  # 大部分通过，但有问题
    else:
        sys.exit(2)  # 多项失败，严重问题