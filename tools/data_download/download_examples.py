#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据下载示例工具 - 统一的下载使用指南
=======================================

合并了原有的两个示例工具:
- download_data_example.py (完整示例)
- simple_download_example.py (简单示例)

功能:
1. 简单模式 - 快速开始下载
2. 完整模式 - 详细配置和高级功能
3. 演示模式 - 展示各种下载方式
4. 交互模式 - 引导式下载设置

使用方法:
python tools/data_download/download_examples.py [模式]

模式选项:
- simple: 简单快速下载示例
- complete: 完整功能演示
- demo: 演示各种下载方式
- interactive: 交互式引导
- all: 运行所有示例 (默认)
"""

import os
import sys
import argparse
from pathlib import Path
import pandas as pd
import numpy as np
import logging

# 添加项目路径
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

class DownloadExamples:
    """数据下载示例管理器"""
    
    def __init__(self):
        self.config = {
            'data_dir': './data',
            'cache': {
                'cache_dir': './data/cache',
                'max_memory_size': 100 * 1024 * 1024  # 100MB
            },
            'batch_size': 10,
            'delay': 0.1,
            'max_retry': 2
        }
        
        # 配置日志
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

    def run_simple_example(self):
        """运行简单下载示例"""
        print("🚀 简单数据下载示例")
        print("=" * 40)
        
        try:
            print("📊 模式: 快速开始")
            print("💡 这是最简单的数据下载方式\n")
            
            # 方法1: 直接使用组件
            success1 = self._test_direct_component_usage()
            
            # 方法2: 使用现有下载器
            success2 = self._test_existing_downloaders()
            
            if success1 or success2:
                print("\n✅ 简单示例执行成功!")
                print("💡 建议: 查看生成的数据文件了解结构")
                return True
            else:
                print("\n⚠️ 简单示例执行遇到问题，请检查环境配置")
                return False
                
        except Exception as e:
            print(f"\n❌ 简单示例执行失败: {e}")
            return False

    def _test_direct_component_usage(self):
        """测试直接组件使用"""
        print("\n🔧 方法1: 直接使用核心组件")
        
        try:
            # 尝试导入核心组件
            from core.data.downloaders.a_shares_downloader import ASharesDownloader
            print("✅ A股下载器导入成功")
            
            # 创建简单的下载器实例
            downloader = ASharesDownloader(self.config)
            print("✅ 下载器初始化成功")
            
            # 这里只是测试初始化，不执行实际下载
            print("💡 下载器准备就绪，可以开始数据下载")
            return True
            
        except ImportError as e:
            print(f"⚠️ 核心组件导入失败: {e}")
            print("💡 提示: 确保core模块正确配置")
            return False
        except Exception as e:
            print(f"⚠️ 下载器初始化失败: {e}")
            return False

    def _test_existing_downloaders(self):
        """测试现有下载器"""
        print("\n📥 方法2: 使用现有下载脚本")
        
        # 检查根目录的下载脚本
        downloaders = [
            'priority_market_flow_downloader.py',
            'start_historical_download.py',
            'start_smart_download.py'
        ]
        
        available = []
        for downloader in downloaders:
            if (project_root / downloader).exists():
                available.append(downloader)
        
        if available:
            print(f"✅ 发现可用下载器: {len(available)} 个")
            for dl in available:
                print(f"   - {dl}")
            print("\n💡 运行示例:")
            print(f"   python {available[0]}")
            return True
        else:
            print("⚠️ 未发现可用的下载器")
            return False

    def run_complete_example(self):
        """运行完整下载示例"""
        print("🚀 完整数据下载示例")
        print("=" * 40)
        
        try:
            print("📊 模式: 完整功能演示")
            print("💡 展示统一数据架构的完整功能\n")
            
            # 演示数据管理器使用
            manager_success = self._demonstrate_data_manager()
            
            # 演示不同数据类型下载
            download_success = self._demonstrate_data_types()
            
            # 演示数据处理流程
            processing_success = self._demonstrate_data_processing()
            
            if manager_success and download_success and processing_success:
                print("\n🎉 完整示例执行成功!")
                return True
            else:
                print("\n⚠️ 部分功能执行遇到问题")
                return False
                
        except Exception as e:
            print(f"\n❌ 完整示例执行失败: {e}")
            return False

    def _demonstrate_data_manager(self):
        """演示数据管理器使用"""
        print("🔧 1. 数据管理器演示")
        
        try:
            from core.data.enhanced_data_manager import EnhancedDataManager
            manager = EnhancedDataManager(self.config)
            print("✅ 数据管理器初始化成功")
            
            # 演示配置检查
            print("📋 配置信息:")
            print(f"   - 数据目录: {self.config['data_dir']}")
            print(f"   - 缓存目录: {self.config['cache']['cache_dir']}")
            print(f"   - 批次大小: {self.config['batch_size']}")
            
            return True
            
        except ImportError:
            print("⚠️ 数据管理器模块未找到，创建模拟示例")
            self._create_mock_data_example()
            return True
        except Exception as e:
            print(f"⚠️ 数据管理器演示失败: {e}")
            return False

    def _demonstrate_data_types(self):
        """演示不同数据类型下载"""
        print("\n📊 2. 数据类型下载演示")
        
        data_types = [
            ('daily', '日行情数据'),
            ('weekly', '周行情数据'),
            ('monthly', '月行情数据'),
            ('flow', '资金流向数据'),
            ('adjusted', '前复权数据')
        ]
        
        print("💡 支持的数据类型:")
        for data_type, desc in data_types:
            print(f"   - {data_type}: {desc}")
        
        # 演示下载配置
        print("\n⚙️ 下载配置示例:")
        example_config = {
            'start_date': '2024-01-01',
            'end_date': '2024-12-31',
            'symbols': ['000001.SZ', '000002.SZ'],
            'frequency': 'daily'
        }
        
        for key, value in example_config.items():
            print(f"   {key}: {value}")
        
        return True

    def _demonstrate_data_processing(self):
        """演示数据处理流程"""
        print("\n🔄 3. 数据处理流程演示")
        
        steps = [
            '1. 数据下载 -> 原始数据存储',
            '2. 数据清洗 -> 去重和验证',
            '3. 数据转换 -> 格式标准化',
            '4. 数据优化 -> Parquet压缩存储',
            '5. 数据索引 -> 快速查询优化'
        ]
        
        print("📋 标准处理流程:")
        for step in steps:
            print(f"   {step}")
        
        return True

    def _create_mock_data_example(self):
        """创建模拟数据示例"""
        print("🎯 创建模拟数据示例")
        
        # 创建示例数据目录
        data_dir = Path(self.config['data_dir']) / 'examples'
        data_dir.mkdir(parents=True, exist_ok=True)
        
        # 生成示例股票数据
        dates = pd.date_range('2024-01-01', '2024-01-10', freq='D')
        sample_data = pd.DataFrame({
            'date': dates,
            'symbol': '000001.SZ',
            'open': np.random.uniform(10, 15, len(dates)),
            'high': np.random.uniform(15, 20, len(dates)),
            'low': np.random.uniform(8, 12, len(dates)),
            'close': np.random.uniform(12, 18, len(dates)),
            'volume': np.random.uniform(1000000, 5000000, len(dates))
        })
        
        # 保存示例数据
        sample_file = data_dir / 'sample_stock_data.csv'
        sample_data.to_csv(sample_file, index=False)
        print(f"✅ 示例数据已保存: {sample_file}")

    def run_demo_mode(self):
        """运行演示模式"""
        print("🎭 数据下载演示模式")
        print("=" * 40)
        
        demos = [
            ('连接测试', self._demo_connection_test),
            ('数据下载', self._demo_data_download),
            ('数据查看', self._demo_data_viewing),
            ('最佳实践', self._demo_best_practices)
        ]
        
        for demo_name, demo_func in demos:
            print(f"\n📍 {demo_name}演示:")
            try:
                demo_func()
                print(f"✅ {demo_name}演示完成")
            except Exception as e:
                print(f"⚠️ {demo_name}演示异常: {e}")
        
        return True

    def _demo_connection_test(self):
        """演示连接测试"""
        print("💡 使用连接管理器测试API连接:")
        print("   python tools/data_download/uqer_connection_manager.py simple")

    def _demo_data_download(self):
        """演示数据下载"""
        print("💡 使用下载器获取数据:")
        print("   python priority_market_flow_downloader.py")
        print("   python start_historical_download.py")

    def _demo_data_viewing(self):
        """演示数据查看"""
        print("💡 查看和使用下载的数据:")
        print("   python data_usage_guide.py")

    def _demo_best_practices(self):
        """演示最佳实践"""
        practices = [
            "1. 先测试连接再下载数据",
            "2. 使用批次下载避免API限制",
            "3. 定期备份重要数据",
            "4. 监控下载进度和错误",
            "5. 使用数据质量检查工具"
        ]
        
        print("💡 数据下载最佳实践:")
        for practice in practices:
            print(f"   {practice}")

    def run_interactive_mode(self):
        """运行交互模式"""
        print("🤝 交互式下载引导")
        print("=" * 40)
        
        try:
            # 引导用户选择
            print("请选择您的使用场景:")
            print("1. 我是新用户，想快速开始")
            print("2. 我需要下载特定数据")
            print("3. 我想了解所有功能")
            print("4. 我需要故障排除帮助")
            
            choice = input("\n请输入选择 (1-4): ").strip()
            
            if choice == '1':
                return self.run_simple_example()
            elif choice == '2':
                return self._guide_specific_download()
            elif choice == '3':
                return self.run_complete_example()
            elif choice == '4':
                return self._provide_troubleshooting()
            else:
                print("⚠️ 无效选择，运行默认演示")
                return self.run_demo_mode()
                
        except KeyboardInterrupt:
            print("\n👋 用户取消操作")
            return False
        except EOFError:
            print("\n💡 非交互环境，运行演示模式")
            return self.run_demo_mode()

    def _guide_specific_download(self):
        """引导特定数据下载"""
        print("\n📊 数据类型选择:")
        print("1. 股票日行情数据")
        print("2. 资金流向数据")  
        print("3. 历史数据批量下载")
        
        try:
            data_choice = input("请选择数据类型 (1-3): ").strip()
            
            if data_choice == '1':
                print("\n💡 股票日行情数据下载:")
                print("   推荐使用: python priority_market_flow_downloader.py")
            elif data_choice == '2':
                print("\n💡 资金流向数据下载:")
                print("   推荐使用: python start_smart_download.py")
            elif data_choice == '3':
                print("\n💡 历史数据批量下载:")
                print("   推荐使用: python start_historical_download.py")
            
            return True
            
        except (KeyboardInterrupt, EOFError):
            return False

    def _provide_troubleshooting(self):
        """提供故障排除帮助"""
        print("\n🔧 常见问题和解决方案:")
        
        issues = [
            ("连接失败", "运行: python tools/data_download/uqer_connection_manager.py"),
            ("导入错误", "检查Python路径和依赖包安装"),
            ("数据为空", "确认API权限和日期范围设置"),
            ("下载中断", "检查网络连接和API限制"),
            ("存储错误", "确认磁盘空间和写入权限")
        ]
        
        for issue, solution in issues:
            print(f"   ❓ {issue}: {solution}")
        
        return True

    def run_all_examples(self):
        """运行所有示例"""
        print("🎯 运行所有数据下载示例")
        print("=" * 50)
        
        results = []
        
        print("1️⃣ 简单示例")
        results.append(self.run_simple_example())
        
        print("\n" + "="*50)
        print("2️⃣ 完整示例")
        results.append(self.run_complete_example())
        
        print("\n" + "="*50)
        print("3️⃣ 演示模式")
        results.append(self.run_demo_mode())
        
        success_count = sum(results)
        print(f"\n📊 总结: {success_count}/3 个示例成功执行")
        
        if success_count >= 2:
            print("🎉 示例执行成功！您可以开始使用数据下载功能了。")
            return True
        else:
            print("⚠️ 部分示例执行遇到问题，建议检查环境配置。")
            return False

def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='数据下载示例工具')
    parser.add_argument('mode', nargs='?', default='all', 
                       choices=['simple', 'complete', 'demo', 'interactive', 'all'],
                       help='示例模式 (默认: all)')
    
    args = parser.parse_args()
    
    examples = DownloadExamples()
    
    try:
        if args.mode == 'simple':
            success = examples.run_simple_example()
        elif args.mode == 'complete':
            success = examples.run_complete_example()
        elif args.mode == 'demo':
            success = examples.run_demo_mode()
        elif args.mode == 'interactive':
            success = examples.run_interactive_mode()
        else:  # all
            success = examples.run_all_examples()
        
        if success:
            print("\n🎉 示例执行完成！")
            return 0
        else:
            print("\n⚠️ 示例执行遇到问题。")
            return 1
            
    except KeyboardInterrupt:
        print("\n\n⏹️ 用户中断操作")
        return 1
    except Exception as e:
        print(f"\n❌ 程序执行出错: {e}")
        return 1

if __name__ == "__main__":
    exit(main())