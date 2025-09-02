#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
配置验证器 - 现代化的配置模块验证工具
=====================================

功能:
1. 配置模块完整性验证
2. 配置文件格式验证
3. 环境变量检查
4. 配置项有效性验证
5. 配置一致性检查

使用方法:
python tools/testing/config_validator.py [模式]

模式选项:
- basic: 基础配置验证
- advanced: 高级配置检查
- environment: 环境配置验证
- all: 运行所有验证 (默认)
"""

import os
import sys
import json
import argparse
from pathlib import Path
from datetime import datetime
import pandas as pd

# 添加项目路径
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

class ConfigValidator:
    """配置验证器"""
    
    def __init__(self):
        self.validation_results = {
            'timestamp': datetime.now().isoformat(),
            'validation_status': 'unknown',
            'tests_passed': 0,
            'tests_failed': 0,
            'test_results': {},
            'config_health': {}
        }
    
    def validate_core_config(self):
        """验证核心配置模块"""
        print("⚙️ 验证核心配置模块...")
        
        test_result = {
            'status': 'unknown',
            'config_classes': 0,
            'config_loaded': 0,
            'errors': []
        }
        
        config_classes = [
            ('Config', 'core.config.settings', '主配置类'),
            ('TradingConfig', 'core.config.trading_config', '交易配置'),
            ('DatabaseConfig', 'core.config.database_config', '数据库配置')
        ]
        
        for class_name, module_path, description in config_classes:
            try:
                module = __import__(module_path, fromlist=[class_name])
                config_class = getattr(module, class_name)
                
                # 尝试实例化
                config_instance = config_class()
                
                test_result['config_loaded'] += 1
                print(f"  ✅ {class_name}: {description} - 加载成功")
                
                # 检查基本属性
                if hasattr(config_instance, '__dict__'):
                    attrs = len([attr for attr in dir(config_instance) 
                               if not attr.startswith('_')])
                    print(f"     └─ 配置项数量: {attrs}")
                
            except ImportError:
                test_result['errors'].append(f"{class_name}: 模块未找到 (可能未实现)")
                print(f"  ⚠️ {class_name}: 模块未实现")
            except Exception as e:
                test_result['errors'].append(f"{class_name}: {str(e)}")
                print(f"  ❌ {class_name}: 初始化失败 - {e}")
            
            test_result['config_classes'] += 1
        
        test_result['status'] = 'success' if test_result['config_loaded'] > 0 else 'failed'
        
        self.validation_results['test_results']['core_config'] = test_result
        
        if test_result['status'] == 'success':
            self.validation_results['tests_passed'] += 1
        else:
            self.validation_results['tests_failed'] += 1
        
        return test_result['status'] == 'success'
    
    def validate_config_files(self):
        """验证配置文件"""
        print("📁 验证配置文件...")
        
        test_result = {
            'status': 'unknown',
            'files_checked': 0,
            'files_valid': 0,
            'file_details': {},
            'errors': []
        }
        
        config_files = [
            ('uqer_config.json', 'JSON', '优矿API配置'),
            ('requirements.txt', 'TEXT', '依赖包列表'),
            ('uqer接口清单.txt', 'TEXT', 'API接口清单'),
            ('.gitignore', 'TEXT', 'Git忽略配置')
        ]
        
        for filename, file_type, description in config_files:
            file_path = project_root / filename
            
            file_info = {
                'exists': file_path.exists(),
                'type': file_type,
                'description': description,
                'size_bytes': 0,
                'valid': False
            }
            
            test_result['files_checked'] += 1
            
            if file_path.exists():
                file_info['size_bytes'] = file_path.stat().st_size
                
                try:
                    if file_type == 'JSON':
                        with open(file_path, 'r', encoding='utf-8') as f:
                            json.load(f)
                        file_info['valid'] = True
                        print(f"  ✅ {filename}: JSON格式有效")
                    else:
                        # 检查文本文件是否可读
                        with open(file_path, 'r', encoding='utf-8') as f:
                            content = f.read()
                            if content.strip():
                                file_info['valid'] = True
                                print(f"  ✅ {filename}: 文件有效 ({len(content)}字符)")
                            else:
                                print(f"  ⚠️ {filename}: 文件为空")
                    
                    if file_info['valid']:
                        test_result['files_valid'] += 1
                        
                except Exception as e:
                    test_result['errors'].append(f"{filename}: {str(e)}")
                    print(f"  ❌ {filename}: 格式错误 - {e}")
            else:
                print(f"  ⚠️ {filename}: 文件不存在")
            
            test_result['file_details'][filename] = file_info
        
        test_result['status'] = 'success' if test_result['files_valid'] > 0 else 'failed'
        
        self.validation_results['test_results']['config_files'] = test_result
        
        if test_result['status'] == 'success':
            self.validation_results['tests_passed'] += 1
        else:
            self.validation_results['tests_failed'] += 1
        
        return test_result['status'] == 'success'
    
    def validate_environment_config(self):
        """验证环境配置"""
        print("🌐 验证环境配置...")
        
        test_result = {
            'status': 'unknown',
            'env_vars_checked': 0,
            'env_vars_found': 0,
            'python_version': sys.version,
            'working_directory': str(Path.cwd()),
            'errors': []
        }
        
        # 检查关键环境变量
        important_env_vars = [
            ('UQER_TOKEN', '优矿API令牌'),
            ('TUSHARE_TOKEN', 'Tushare API令牌'),
            ('PYTHONPATH', 'Python路径')
        ]
        
        env_details = {}
        
        for var_name, description in important_env_vars:
            test_result['env_vars_checked'] += 1
            
            var_value = os.environ.get(var_name)
            var_info = {
                'set': var_value is not None,
                'description': description,
                'value_length': len(var_value) if var_value else 0
            }
            
            if var_value:
                test_result['env_vars_found'] += 1
                print(f"  ✅ {var_name}: 已设置 ({len(var_value)}字符)")
            else:
                print(f"  ⚠️ {var_name}: 未设置")
            
            env_details[var_name] = var_info
        
        test_result['environment_details'] = env_details
        
        # 检查Python版本
        python_version = sys.version_info
        if python_version >= (3, 8):
            print(f"  ✅ Python版本: {python_version.major}.{python_version.minor}.{python_version.micro} (兼容)")
        else:
            test_result['errors'].append("Python版本过低，建议使用3.8+")
            print(f"  ⚠️ Python版本: {python_version.major}.{python_version.minor}.{python_version.micro} (可能不兼容)")
        
        # 检查工作目录
        if 'QuantTrade' in str(Path.cwd()):
            print(f"  ✅ 工作目录: 正确 ({Path.cwd().name})")
        else:
            print(f"  ⚠️ 工作目录: 可能不正确 ({Path.cwd().name})")
        
        test_result['status'] = 'success'  # 环境检查总是标记为成功
        
        self.validation_results['test_results']['environment_config'] = test_result
        self.validation_results['tests_passed'] += 1
        
        return True
    
    def check_config_consistency(self):
        """检查配置一致性"""
        print("🔍 检查配置一致性...")
        
        test_result = {
            'status': 'unknown',
            'consistency_checks': 0,
            'consistency_passed': 0,
            'warnings': []
        }
        
        # 检查目录结构一致性
        required_dirs = ['core', 'data', 'tools', 'outputs', 'logs']
        for dirname in required_dirs:
            test_result['consistency_checks'] += 1
            dir_path = project_root / dirname
            
            if dir_path.exists():
                test_result['consistency_passed'] += 1
                print(f"  ✅ 目录存在: {dirname}/")
            else:
                test_result['warnings'].append(f"缺少目录: {dirname}/")
                print(f"  ⚠️ 目录缺失: {dirname}/")
        
        # 检查关键文件一致性
        required_files = ['main.py', 'data_usage_guide.py', 'requirements.txt']
        for filename in required_files:
            test_result['consistency_checks'] += 1
            file_path = project_root / filename
            
            if file_path.exists():
                test_result['consistency_passed'] += 1
                print(f"  ✅ 文件存在: {filename}")
            else:
                test_result['warnings'].append(f"缺少文件: {filename}")
                print(f"  ⚠️ 文件缺失: {filename}")
        
        consistency_rate = test_result['consistency_passed'] / test_result['consistency_checks']
        test_result['status'] = 'success' if consistency_rate >= 0.8 else 'warning'
        
        self.validation_results['test_results']['config_consistency'] = test_result
        
        if test_result['status'] == 'success':
            self.validation_results['tests_passed'] += 1
        else:
            self.validation_results['tests_failed'] += 1
        
        return test_result['status'] == 'success'
    
    def generate_validation_report(self, save_to_file=True):
        """生成验证报告"""
        print("\n📋 生成配置验证报告...")
        
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
            json_file = f"outputs/reports/config_validation_{timestamp}.json"
            
            # 确保目录存在
            Path(json_file).parent.mkdir(parents=True, exist_ok=True)
            
            with open(json_file, 'w', encoding='utf-8') as f:
                json.dump(self.validation_results, f, indent=2, ensure_ascii=False)
            
            print(f"📊 报告已保存: {json_file}")
        
        return self.validation_results
    
    def run_basic_validation(self):
        """运行基础验证"""
        print("🚀 开始基础配置验证...\n")
        
        self.validate_core_config()
        self.validate_config_files()
        
        self.generate_validation_report()
        
        return self.validation_results['validation_status'] in ['excellent', 'good']
    
    def run_advanced_validation(self):
        """运行高级验证"""
        print("🚀 开始高级配置验证...\n")
        
        self.validate_core_config()
        self.validate_config_files()
        self.check_config_consistency()
        
        self.generate_validation_report()
        
        return self.validation_results['validation_status'] in ['excellent', 'good']
    
    def run_environment_validation(self):
        """运行环境验证"""
        print("🚀 开始环境配置验证...\n")
        
        self.validate_environment_config()
        self.validate_config_files()
        
        self.generate_validation_report()
        
        return True  # 环境验证总是返回成功
    
    def run_all_validation(self):
        """运行所有验证"""
        print("🚀 开始完整配置验证...\n")
        
        self.validate_core_config()
        self.validate_config_files()
        self.validate_environment_config()
        self.check_config_consistency()
        
        self.generate_validation_report()
        
        return self.validation_results['validation_status'] in ['excellent', 'good']

def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='配置验证器')
    parser.add_argument('mode', nargs='?', default='all',
                       choices=['basic', 'advanced', 'environment', 'all'],
                       help='验证模式 (默认: all)')
    
    args = parser.parse_args()
    
    validator = ConfigValidator()
    
    try:
        if args.mode == 'basic':
            success = validator.run_basic_validation()
        elif args.mode == 'advanced':
            success = validator.run_advanced_validation()
        elif args.mode == 'environment':
            success = validator.run_environment_validation()
        else:  # all
            success = validator.run_all_validation()
        
        if success:
            print("\n🎉 配置验证通过！")
            return 0
        else:
            print("\n⚠️ 配置验证发现问题，请查看报告详情。")
            return 1
            
    except KeyboardInterrupt:
        print("\n\n⏹️ 用户中断操作")
        return 1
    except Exception as e:
        print(f"\n❌ 验证程序执行出错: {e}")
        return 1

if __name__ == "__main__":
    exit(main())