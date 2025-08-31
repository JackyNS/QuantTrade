#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
增强版数据模块测试
=================

测试所有新增的数据功能
"""

import sys
import os
from pathlib import Path
import pandas as pd
from datetime import datetime, timedelta
import logging

# 添加项目根目录到Python路径
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from core.data.enhanced_data_manager import EnhancedDataManager
from core.data.adapters.data_source_manager import DataSourceManager
from core.data.quality_checker import DataQualityChecker
from core.data.cache_manager import SmartCacheManager

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_data_source_manager():
    """测试数据源管理器"""
    logger.info("🧪 测试数据源管理器...")
    
    try:
        # 创建数据源管理器
        dsm = DataSourceManager()
        
        # 测试连接
        connection_results = dsm.test_all_connections()
        logger.info(f"连接测试结果: {connection_results}")
        
        # 获取可用数据源
        available_sources = dsm.get_available_sources()
        logger.info(f"可用数据源: {available_sources}")
        
        # 测试获取股票列表（如果有可用数据源）
        if available_sources:
            stock_list = dsm.get_stock_list()
            if not stock_list.empty:
                logger.info(f"✅ 获取股票列表成功: {len(stock_list)}只股票")
            else:
                logger.warning("⚠️ 股票列表为空")
        
        # 获取状态报告
        status_report = dsm.get_status_report()
        logger.info(f"数据源状态报告: {status_report}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ 数据源管理器测试失败: {str(e)}")
        return False

def test_quality_checker():
    """测试数据质量检查器"""
    logger.info("🧪 测试数据质量检查器...")
    
    try:
        # 创建测试数据
        test_data = pd.DataFrame({
            'date': pd.date_range('2023-01-01', periods=100),
            'symbol': ['TEST'] * 100,
            'open': [100 + i * 0.1 + (i % 10 - 5) * 0.5 for i in range(100)],
            'high': [100 + i * 0.1 + (i % 10 - 5) * 0.5 + 0.5 for i in range(100)],
            'low': [100 + i * 0.1 + (i % 10 - 5) * 0.5 - 0.5 for i in range(100)],
            'close': [100 + i * 0.1 + (i % 10 - 5) * 0.5 for i in range(100)],
            'volume': [1000000 + i * 10000 for i in range(100)]
        })
        
        # 添加一些缺失值和异常值
        test_data.loc[10:15, 'volume'] = None
        test_data.loc[50, 'high'] = 1000  # 异常值
        
        # 创建质量检查器
        qc = DataQualityChecker()
        
        # 测试各种检查功能
        missing_result = qc.check_missing_data(test_data, critical_columns=['date', 'symbol', 'close'])
        logger.info(f"✅ 缺失数据检查完成: {missing_result['overall']['missing_rate']:.2%}")
        
        outlier_result = qc.check_outliers(test_data)
        logger.info(f"✅ 异常值检查完成")
        
        type_result = qc.check_data_types(test_data)
        logger.info(f"✅ 数据类型检查完成")
        
        consistency_result = qc.check_price_data_consistency(test_data)
        logger.info(f"✅ 价格一致性检查完成")
        
        completeness_result = qc.check_completeness(test_data)
        logger.info(f"✅ 数据完整性检查完成")
        
        # 生成综合质量报告
        quality_report = qc.generate_quality_report(test_data, "测试数据质量报告")
        logger.info(f"✅ 质量报告生成完成，整体得分: {quality_report['overall_score']:.2f}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ 数据质量检查器测试失败: {str(e)}")
        return False

def test_cache_manager():
    """测试缓存管理器"""
    logger.info("🧪 测试缓存管理器...")
    
    try:
        # 创建缓存管理器
        cache_config = {
            'cache_dir': './test_cache',
            'max_memory_size': 10 * 1024 * 1024,  # 10MB
            'default_expire_hours': 1
        }
        cm = SmartCacheManager(cache_config)
        
        # 创建测试数据
        test_data = pd.DataFrame({
            'col1': range(1000),
            'col2': [f'value_{i}' for i in range(1000)]
        })
        
        # 测试缓存存储
        data_type = 'test_data'
        params = {'param1': 'value1', 'param2': 123}
        
        cm.put(data_type, params, test_data, expire_hours=2)
        logger.info("✅ 数据缓存存储成功")
        
        # 测试缓存获取
        cached_data = cm.get(data_type, params)
        if cached_data is not None and len(cached_data) == len(test_data):
            logger.info("✅ 数据缓存获取成功")
        else:
            logger.error("❌ 数据缓存获取失败")
        
        # 测试缓存统计
        cache_stats = cm.get_cache_stats()
        logger.info(f"✅ 缓存统计: 命中率 {cache_stats['statistics']['hit_rate']:.2%}")
        
        # 清理测试缓存
        cm.clear_cache()
        logger.info("✅ 测试缓存清理完成")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ 缓存管理器测试失败: {str(e)}")
        return False

def test_enhanced_data_manager():
    """测试增强版数据管理器"""
    logger.info("🧪 测试增强版数据管理器...")
    
    try:
        # 创建增强版数据管理器
        config = {
            'cache': {
                'cache_dir': './test_cache',
                'max_memory_size': 50 * 1024 * 1024,  # 50MB
                'default_expire_hours': 2
            },
            'quality': {
                'thresholds': {
                    'missing_rate': 0.1,
                    'outlier_zscore': 3.0
                }
            }
        }
        
        edm = EnhancedDataManager(config)
        
        # 验证数据流水线
        validation_result = edm.validate_data_pipeline()
        logger.info(f"✅ 数据流水线验证: {validation_result['overall_status']}")
        
        # 测试获取股票列表
        stock_list = edm.get_stock_list()
        if not stock_list.empty:
            logger.info(f"✅ 获取股票列表成功: {len(stock_list)}只股票")
            
            # 选择几只股票测试价格数据获取
            test_symbols = stock_list['symbol'].head(3).tolist()
            start_date = (datetime.now() - timedelta(days=30)).date()
            end_date = datetime.now().date()
            
            price_data = edm.get_price_data(
                symbols=test_symbols,
                start_date=start_date,
                end_date=end_date,
                use_cache=True,
                quality_check=True
            )
            
            if not price_data.empty:
                logger.info(f"✅ 获取价格数据成功: {len(price_data)}条记录")
                
                # 测试特征生成
                features = edm.generate_features(price_data)
                if not features.empty:
                    logger.info(f"✅ 特征生成成功: {len(features.columns)}个特征")
                else:
                    logger.warning("⚠️ 特征生成失败或无可用特征工程器")
            else:
                logger.warning("⚠️ 价格数据为空")
        else:
            logger.warning("⚠️ 股票列表为空，跳过价格数据测试")
        
        # 获取缓存统计
        cache_stats = edm.get_cache_statistics()
        logger.info(f"✅ 缓存统计获取成功")
        
        # 获取数据源状态
        data_source_status = edm.get_data_source_status()
        logger.info(f"✅ 数据源状态获取成功")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ 增强版数据管理器测试失败: {str(e)}")
        return False

def run_all_tests():
    """运行所有测试"""
    logger.info("🚀 开始运行增强版数据模块测试...")
    
    tests = [
        ("数据源管理器", test_data_source_manager),
        ("数据质量检查器", test_quality_checker),
        ("缓存管理器", test_cache_manager),
        ("增强版数据管理器", test_enhanced_data_manager)
    ]
    
    results = []
    
    for test_name, test_func in tests:
        logger.info(f"\n{'='*50}")
        logger.info(f"测试 {test_name}")
        logger.info('='*50)
        
        try:
            result = test_func()
            results.append((test_name, result))
            
            if result:
                logger.info(f"✅ {test_name} 测试通过")
            else:
                logger.error(f"❌ {test_name} 测试失败")
                
        except Exception as e:
            logger.error(f"❌ {test_name} 测试异常: {str(e)}")
            results.append((test_name, False))
    
    # 输出测试总结
    logger.info(f"\n{'='*50}")
    logger.info("测试总结")
    logger.info('='*50)
    
    passed_count = sum(1 for _, result in results if result)
    total_count = len(results)
    
    for test_name, result in results:
        status = "✅ 通过" if result else "❌ 失败"
        logger.info(f"{test_name}: {status}")
    
    logger.info(f"\n总测试数: {total_count}")
    logger.info(f"通过数: {passed_count}")
    logger.info(f"失败数: {total_count - passed_count}")
    logger.info(f"通过率: {passed_count/total_count*100:.1f}%")
    
    if passed_count == total_count:
        logger.info("🎉 所有测试通过！")
    else:
        logger.error("💥 部分测试失败！")
    
    return passed_count == total_count

if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)