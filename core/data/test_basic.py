#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
基础测试脚本 - 不依赖外部包
==========================

测试核心组件能否正常导入和初始化
"""

import sys
import os
from pathlib import Path

# 添加项目根目录到Python路径
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

def test_basic_imports():
    """测试基础导入"""
    print("🧪 测试基础导入...")
    
    try:
        # 测试核心模块导入
        from core.data.adapters.base_adapter import BaseDataAdapter
        print("✅ BaseDataAdapter 导入成功")
        
        from core.data.adapters.data_source_manager import DataSourceManager
        print("✅ DataSourceManager 导入成功")
        
        print("🎉 基础导入测试通过")
        return True
    except Exception as e:
        print(f"❌ 基础导入测试失败: {str(e)}")
        return False

def test_data_source_manager_init():
    """测试数据源管理器初始化"""
    print("\n🧪 测试数据源管理器初始化...")
    
    try:
        from core.data.adapters.data_source_manager import DataSourceManager
        
        # 创建数据源管理器
        dsm = DataSourceManager()
        print(f"✅ 数据源管理器创建成功")
        print(f"   注册的适配器数量: {len(dsm.adapters)}")
        print(f"   数据源优先级: {dsm.priority_order}")
        
        return True
    except Exception as e:
        print(f"❌ 数据源管理器初始化失败: {str(e)}")
        return False

def test_cache_manager_init():
    """测试缓存管理器初始化"""
    print("\n🧪 测试缓存管理器初始化...")
    
    try:
        from core.data.cache_manager import SmartCacheManager
        
        # 创建缓存管理器
        config = {
            'cache_dir': './test_cache_basic',
            'max_memory_size': 1024 * 1024,  # 1MB
        }
        
        cm = SmartCacheManager(config)
        print("✅ 缓存管理器创建成功")
        print(f"   缓存目录: {cm.cache_dir}")
        
        # 清理测试缓存
        cm.clear_cache()
        print("✅ 缓存清理完成")
        
        return True
    except Exception as e:
        print(f"❌ 缓存管理器初始化失败: {str(e)}")
        return False

def test_quality_checker_init():
    """测试质量检查器初始化"""
    print("\n🧪 测试质量检查器初始化...")
    
    try:
        from core.data.quality_checker import DataQualityChecker
        
        # 创建质量检查器
        qc = DataQualityChecker()
        print("✅ 质量检查器创建成功")
        
        return True
    except Exception as e:
        print(f"❌ 质量检查器初始化失败: {str(e)}")
        return False

def run_basic_tests():
    """运行所有基础测试"""
    print("🚀 开始运行基础测试...")
    print("="*60)
    
    tests = [
        ("基础导入", test_basic_imports),
        ("数据源管理器初始化", test_data_source_manager_init), 
        ("缓存管理器初始化", test_cache_manager_init),
        ("质量检查器初始化", test_quality_checker_init)
    ]
    
    results = []
    
    for test_name, test_func in tests:
        print(f"\n{'='*50}")
        print(f"测试 {test_name}")
        print('='*50)
        
        try:
            result = test_func()
            results.append((test_name, result))
            
            if result:
                print(f"✅ {test_name} 测试通过")
            else:
                print(f"❌ {test_name} 测试失败")
                
        except Exception as e:
            print(f"❌ {test_name} 测试异常: {str(e)}")
            results.append((test_name, False))
    
    # 输出测试总结
    print(f"\n{'='*60}")
    print("测试总结")
    print('='*60)
    
    passed_count = sum(1 for _, result in results if result)
    total_count = len(results)
    
    for test_name, result in results:
        status = "✅ 通过" if result else "❌ 失败"
        print(f"{test_name}: {status}")
    
    print(f"\n总测试数: {total_count}")
    print(f"通过数: {passed_count}")
    print(f"失败数: {total_count - passed_count}")
    print(f"通过率: {passed_count/total_count*100:.1f}%")
    
    if passed_count == total_count:
        print("🎉 所有基础测试通过！")
    else:
        print("💥 部分基础测试失败！")
    
    return passed_count == total_count

if __name__ == "__main__":
    success = run_basic_tests()
    sys.exit(0 if success else 1)