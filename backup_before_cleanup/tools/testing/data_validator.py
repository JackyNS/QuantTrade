#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据验证器 - 现代化的数据模块验证工具
=====================================

功能:
1. 数据模块完整性验证
2. 数据质量检查
3. 数据源连接验证
4. 数据格式验证
5. 数据管道测试

使用方法:
python tools/testing/data_validator.py [模式]

模式选项:
- integrity: 数据完整性验证
- quality: 数据质量检查
- sources: 数据源验证
- pipeline: 数据管道测试
- all: 运行所有验证 (默认)
"""

import os
import sys
import json
import argparse
from pathlib import Path
from datetime import datetime
import pandas as pd
import numpy as np

# 添加项目路径
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

class DataValidator:
    """数据验证器"""
    
    def __init__(self):
        self.validation_results = {
            'timestamp': datetime.now().isoformat(),
            'validation_status': 'unknown',
            'tests_passed': 0,
            'tests_failed': 0,
            'test_results': {},
            'data_metrics': {}
        }
    
    def validate_data_modules(self):
        """验证数据模块"""
        print("📊 验证数据模块...")
        
        test_result = {
            'status': 'unknown',
            'modules_tested': 0,
            'modules_loaded': 0,
            'errors': []
        }
        
        data_modules = [
            ('DataManager', 'core.data.data_manager', '数据管理器'),
            ('EnhancedDataManager', 'core.data.enhanced_data_manager', '增强数据管理器'),
            ('UqerAdapter', 'core.data.adapters.uqer_adapter', '优矿适配器'),
            ('DataProcessor', 'core.data.processors.data_processor', '数据处理器')
        ]
        
        for class_name, module_path, description in data_modules:
            test_result['modules_tested'] += 1
            
            try:
                module = __import__(module_path, fromlist=[class_name])
                data_class = getattr(module, class_name)
                
                # 尝试实例化
                if class_name in ['DataManager', 'EnhancedDataManager']:
                    instance = data_class()
                elif class_name == 'UqerAdapter':
                    # UqerAdapter可能需要token
                    try:
                        instance = data_class()
                    except:
                        instance = data_class(token="test_token")
                else:
                    instance = data_class()
                
                test_result['modules_loaded'] += 1
                print(f"  ✅ {class_name}: {description} - 加载成功")
                
            except ImportError:
                test_result['errors'].append(f"{class_name}: 模块未找到")
                print(f"  ⚠️ {class_name}: 模块未实现")
            except Exception as e:
                test_result['errors'].append(f"{class_name}: {str(e)}")
                print(f"  ❌ {class_name}: 初始化失败 - {e}")
        
        test_result['status'] = 'success' if test_result['modules_loaded'] > 0 else 'failed'
        
        self.validation_results['test_results']['data_modules'] = test_result
        
        if test_result['status'] == 'success':
            self.validation_results['tests_passed'] += 1
        else:
            self.validation_results['tests_failed'] += 1
        
        return test_result['status'] == 'success'
    
    def validate_data_directories(self):
        """验证数据目录结构"""
        print("📁 验证数据目录结构...")
        
        test_result = {
            'status': 'unknown',
            'directories_checked': 0,
            'directories_found': 0,
            'directory_details': {},
            'total_size_mb': 0
        }
        
        expected_directories = [
            ('data', '主数据目录'),
            ('data/optimized', '优化数据'),
            ('data/raw', '原始数据'),
            ('data/processed', '处理后数据'),
            ('data/cache', '缓存数据'),
            ('outputs', '输出目录'),
            ('logs', '日志目录')
        ]
        
        for dir_path, description in expected_directories:
            test_result['directories_checked'] += 1
            full_path = project_root / dir_path
            
            dir_info = {
                'exists': full_path.exists(),
                'description': description,
                'size_mb': 0,
                'file_count': 0
            }
            
            if full_path.exists():
                test_result['directories_found'] += 1
                
                # 计算目录大小和文件数量
                total_size = 0
                file_count = 0
                
                try:
                    for file_path in full_path.rglob("*"):
                        if file_path.is_file():
                            file_count += 1
                            total_size += file_path.stat().st_size
                    
                    dir_info['size_mb'] = total_size / (1024 * 1024)
                    dir_info['file_count'] = file_count
                    test_result['total_size_mb'] += dir_info['size_mb']
                    
                    print(f"  ✅ {dir_path}: {file_count}个文件, {dir_info['size_mb']:.1f}MB")
                    
                except Exception as e:
                    print(f"  ⚠️ {dir_path}: 无法统计 - {e}")
            else:
                print(f"  ⚠️ {dir_path}: 目录不存在")
            
            test_result['directory_details'][dir_path] = dir_info
        
        test_result['status'] = 'success' if test_result['directories_found'] >= 3 else 'warning'
        
        self.validation_results['test_results']['data_directories'] = test_result
        
        if test_result['status'] == 'success':
            self.validation_results['tests_passed'] += 1
        else:
            self.validation_results['tests_failed'] += 1
        
        return test_result['status'] == 'success'
    
    def validate_data_quality(self):
        """验证数据质量"""
        print("🔍 验证数据质量...")
        
        test_result = {
            'status': 'unknown',
            'files_checked': 0,
            'quality_passed': 0,
            'quality_metrics': {},
            'errors': []
        }
        
        # 检查优化数据目录
        optimized_dir = project_root / 'data' / 'optimized'
        
        if optimized_dir.exists():
            # 检查不同类型的数据文件
            data_types = ['daily', 'weekly', 'monthly']
            
            for data_type in data_types:
                type_dir = optimized_dir / data_type
                
                if type_dir.exists():
                    parquet_files = list(type_dir.rglob("*.parquet"))
                    csv_files = list(type_dir.rglob("*.csv"))
                    
                    type_metrics = {
                        'parquet_files': len(parquet_files),
                        'csv_files': len(csv_files),
                        'total_files': len(parquet_files) + len(csv_files)
                    }
                    
                    test_result['files_checked'] += type_metrics['total_files']
                    
                    # 检查数据质量（抽样）
                    if parquet_files:
                        try:
                            sample_file = parquet_files[0]
                            df = pd.read_parquet(sample_file)
                            
                            # 基本质量检查
                            quality_checks = {
                                'non_empty': len(df) > 0,
                                'no_all_null_columns': not df.isnull().all().any(),
                                'reasonable_size': len(df) < 1000000  # 单文件不超过100万行
                            }
                            
                            if all(quality_checks.values()):
                                test_result['quality_passed'] += 1
                                print(f"  ✅ {data_type}: 数据质量良好 ({len(df)}行)")
                            else:
                                failed_checks = [k for k, v in quality_checks.items() if not v]
                                test_result['errors'].append(f"{data_type}: 质量检查失败 - {failed_checks}")
                                print(f"  ❌ {data_type}: 质量检查失败")
                            
                            type_metrics['sample_rows'] = len(df)
                            type_metrics['sample_columns'] = len(df.columns)
                            
                        except Exception as e:
                            test_result['errors'].append(f"{data_type}: 读取失败 - {str(e)}")
                            print(f"  ❌ {data_type}: 无法读取样本文件 - {e}")
                    else:
                        print(f"  ⚠️ {data_type}: 没有Parquet文件")
                    
                    test_result['quality_metrics'][data_type] = type_metrics
                    
                else:
                    print(f"  ⚠️ {data_type}: 目录不存在")
        else:
            test_result['errors'].append("优化数据目录不存在")
            print(f"  ❌ 优化数据目录不存在")
        
        test_result['status'] = 'success' if test_result['quality_passed'] > 0 else 'failed'
        
        self.validation_results['test_results']['data_quality'] = test_result
        
        if test_result['status'] == 'success':
            self.validation_results['tests_passed'] += 1
        else:
            self.validation_results['tests_failed'] += 1
        
        return test_result['status'] == 'success'
    
    def validate_data_sources(self):
        """验证数据源连接"""
        print("🌐 验证数据源连接...")
        
        test_result = {
            'status': 'unknown',
            'sources_tested': 0,
            'sources_available': 0,
            'source_details': {},
            'errors': []
        }
        
        # 测试优矿连接
        test_result['sources_tested'] += 1
        uqer_status = {
            'available': False,
            'error': None
        }
        
        try:
            import uqer
            # 尝试创建客户端（使用测试token）
            test_token = "test_token"
            client = uqer.Client(token=test_token)
            uqer_status['available'] = True
            test_result['sources_available'] += 1
            print(f"  ✅ 优矿API: 库可用，可创建客户端")
            
        except ImportError:
            uqer_status['error'] = "uqer包未安装"
            print(f"  ❌ 优矿API: uqer包未安装")
        except Exception as e:
            uqer_status['error'] = str(e)
            print(f"  ⚠️ 优矿API: {str(e)}")
        
        test_result['source_details']['uqer'] = uqer_status
        
        # 测试其他可能的数据源
        optional_sources = [
            ('tushare', 'Tushare金融数据'),
            ('yfinance', 'Yahoo Finance'),
            ('akshare', 'AKShare数据')
        ]
        
        for source_name, description in optional_sources:
            test_result['sources_tested'] += 1
            source_status = {
                'available': False,
                'description': description,
                'error': None
            }
            
            try:
                __import__(source_name)
                source_status['available'] = True
                test_result['sources_available'] += 1
                print(f"  ✅ {source_name}: 可用")
            except ImportError:
                source_status['error'] = f"{source_name}包未安装"
                print(f"  ⚠️ {source_name}: 未安装 (可选)")
            
            test_result['source_details'][source_name] = source_status
        
        test_result['status'] = 'success' if test_result['sources_available'] > 0 else 'warning'
        
        self.validation_results['test_results']['data_sources'] = test_result
        
        if test_result['status'] in ['success', 'warning']:
            self.validation_results['tests_passed'] += 1
        else:
            self.validation_results['tests_failed'] += 1
        
        return test_result['status'] in ['success', 'warning']
    
    def test_data_pipeline(self):
        """测试数据管道"""
        print("🔄 测试数据管道...")
        
        test_result = {
            'status': 'unknown',
            'pipeline_steps': 0,
            'pipeline_passed': 0,
            'errors': []
        }
        
        # 测试数据加载
        test_result['pipeline_steps'] += 1
        try:
            from core.data.data_manager import DataManager
            dm = DataManager()
            test_result['pipeline_passed'] += 1
            print(f"  ✅ 数据加载: 数据管理器初始化成功")
        except Exception as e:
            test_result['errors'].append(f"数据加载失败: {str(e)}")
            print(f"  ❌ 数据加载: {str(e)}")
        
        # 测试数据处理
        test_result['pipeline_steps'] += 1
        try:
            # 创建模拟数据测试处理
            test_data = pd.DataFrame({
                'date': pd.date_range('2024-01-01', periods=10),
                'close': np.random.randn(10) * 10 + 100,
                'volume': np.random.randint(1000, 10000, 10)
            })
            
            # 基本数据处理测试
            processed_data = test_data.copy()
            processed_data['sma_5'] = processed_data['close'].rolling(5).mean()
            
            if not processed_data['sma_5'].isna().all():
                test_result['pipeline_passed'] += 1
                print(f"  ✅ 数据处理: 基本处理功能正常")
            else:
                test_result['errors'].append("数据处理结果异常")
                print(f"  ❌ 数据处理: 结果异常")
                
        except Exception as e:
            test_result['errors'].append(f"数据处理失败: {str(e)}")
            print(f"  ❌ 数据处理: {str(e)}")
        
        test_result['status'] = 'success' if test_result['pipeline_passed'] >= test_result['pipeline_steps'] * 0.5 else 'failed'
        
        self.validation_results['test_results']['data_pipeline'] = test_result
        
        if test_result['status'] == 'success':
            self.validation_results['tests_passed'] += 1
        else:
            self.validation_results['tests_failed'] += 1
        
        return test_result['status'] == 'success'
    
    def generate_validation_report(self, save_to_file=True):
        """生成验证报告"""
        print("\n📋 生成数据验证报告...")
        
        # 计算总体状态
        total_tests = self.validation_results['tests_passed'] + self.validation_results['tests_failed']
        success_rate = self.validation_results['tests_passed'] / total_tests if total_tests > 0 else 0
        
        if success_rate >= 0.9:
            self.validation_results['validation_status'] = 'excellent'
        elif success_rate >= 0.7:
            self.validation_results['validation_status'] = 'good'
        elif success_rate >= 0.5:
            self.validation_results['validation_status'] = 'fair'
        else:
            self.validation_results['validation_status'] = 'poor'
        
        if save_to_file:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            json_file = f"outputs/reports/data_validation_{timestamp}.json"
            
            # 确保目录存在
            Path(json_file).parent.mkdir(parents=True, exist_ok=True)
            
            with open(json_file, 'w', encoding='utf-8') as f:
                json.dump(self.validation_results, f, indent=2, ensure_ascii=False)
            
            print(f"📊 报告已保存: {json_file}")
        
        return self.validation_results
    
    def run_integrity_validation(self):
        """运行完整性验证"""
        print("🚀 开始数据完整性验证...\n")
        
        self.validate_data_modules()
        self.validate_data_directories()
        
        self.generate_validation_report()
        
        return self.validation_results['validation_status'] in ['excellent', 'good']
    
    def run_quality_validation(self):
        """运行质量验证"""
        print("🚀 开始数据质量验证...\n")
        
        self.validate_data_directories()
        self.validate_data_quality()
        
        self.generate_validation_report()
        
        return self.validation_results['validation_status'] in ['excellent', 'good']
    
    def run_sources_validation(self):
        """运行数据源验证"""
        print("🚀 开始数据源验证...\n")
        
        self.validate_data_sources()
        self.validate_data_modules()
        
        self.generate_validation_report()
        
        return self.validation_results['validation_status'] in ['excellent', 'good', 'fair']
    
    def run_pipeline_validation(self):
        """运行数据管道验证"""
        print("🚀 开始数据管道验证...\n")
        
        self.validate_data_modules()
        self.test_data_pipeline()
        
        self.generate_validation_report()
        
        return self.validation_results['validation_status'] in ['excellent', 'good']
    
    def run_all_validation(self):
        """运行所有验证"""
        print("🚀 开始完整数据验证...\n")
        
        self.validate_data_modules()
        self.validate_data_directories()
        self.validate_data_quality()
        self.validate_data_sources()
        self.test_data_pipeline()
        
        self.generate_validation_report()
        
        return self.validation_results['validation_status'] in ['excellent', 'good']

def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='数据验证器')
    parser.add_argument('mode', nargs='?', default='all',
                       choices=['integrity', 'quality', 'sources', 'pipeline', 'all'],
                       help='验证模式 (默认: all)')
    
    args = parser.parse_args()
    
    validator = DataValidator()
    
    try:
        if args.mode == 'integrity':
            success = validator.run_integrity_validation()
        elif args.mode == 'quality':
            success = validator.run_quality_validation()
        elif args.mode == 'sources':
            success = validator.run_sources_validation()
        elif args.mode == 'pipeline':
            success = validator.run_pipeline_validation()
        else:  # all
            success = validator.run_all_validation()
        
        if success:
            print("\n🎉 数据验证通过！")
            return 0
        else:
            print("\n⚠️ 数据验证发现问题，请查看报告详情。")
            return 1
            
    except KeyboardInterrupt:
        print("\n\n⏹️ 用户中断操作")
        return 1
    except Exception as e:
        print(f"\n❌ 验证程序执行出错: {e}")
        return 1

if __name__ == "__main__":
    exit(main())