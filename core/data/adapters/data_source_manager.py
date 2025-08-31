#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据源管理器
===========

统一管理和调度所有数据源适配器
"""

from typing import Dict, List, Optional, Union, Any, Type
import pandas as pd
from datetime import datetime, date
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

from .base_adapter import BaseDataAdapter
from .uqer_adapter import UqerAdapter
from .tushare_adapter import TushareAdapter
from .yahoo_adapter import YahooFinanceAdapter
from .akshare_adapter import AKShareAdapter

logger = logging.getLogger(__name__)

class DataSourceManager:
    """数据源管理器
    
    统一管理多个数据源适配器，提供：
    - 数据源自动切换
    - 并发数据获取
    - 数据源优先级管理
    - 错误处理和重试
    """
    
    def __init__(self, config: Optional[Dict] = None):
        """初始化数据源管理器
        
        Args:
            config: 配置参数
        """
        self.config = config or {}
        
        # 注册数据源适配器
        self.adapters: Dict[str, BaseDataAdapter] = {}
        self._adapter_classes = {
            'uqer': UqerAdapter,
            'tushare': TushareAdapter, 
            'yahoo': YahooFinanceAdapter,
            'akshare': AKShareAdapter
        }
        
        # 数据源优先级 (优先级越高，数字越小)
        self.priority_order = ['uqer', 'tushare', 'akshare', 'yahoo']
        
        # 数据源状态
        self.source_status: Dict[str, Dict] = {}
        
        # 初始化适配器
        self._initialize_adapters()
    
    def _initialize_adapters(self):
        """初始化所有适配器"""
        for name, adapter_class in self._adapter_classes.items():
            try:
                # 获取适配器特定配置
                adapter_config = self.config.get(name, {})
                
                # 创建适配器实例
                adapter = adapter_class(adapter_config)
                self.adapters[name] = adapter
                
                # 初始化状态
                self.source_status[name] = {
                    'available': False,
                    'connected': False,
                    'last_test': None,
                    'error_count': 0,
                    'last_error': None
                }
                
                logger.info(f"✅ 已注册数据源适配器: {name}")
                
            except Exception as e:
                logger.error(f"❌ 初始化{name}适配器失败: {str(e)}")
                self.source_status[name] = {
                    'available': False,
                    'connected': False,
                    'last_test': datetime.now(),
                    'error_count': 1,
                    'last_error': str(e)
                }
    
    def test_all_connections(self) -> Dict[str, Dict]:
        """测试所有数据源连接"""
        logger.info("🔍 测试所有数据源连接...")
        
        results = {}
        
        for name, adapter in self.adapters.items():
            try:
                test_result = adapter.test_connection()
                
                # 更新状态
                self.source_status[name].update({
                    'available': test_result['success'],
                    'connected': test_result['success'],
                    'last_test': datetime.now(),
                    'error_count': 0 if test_result['success'] else self.source_status[name]['error_count'] + 1,
                    'last_error': None if test_result['success'] else test_result['message']
                })
                
                results[name] = test_result
                
                if test_result['success']:
                    logger.info(f"✅ {name}: 连接成功")
                else:
                    logger.warning(f"⚠️ {name}: {test_result['message']}")
                    
            except Exception as e:
                error_msg = f"连接测试异常: {str(e)}"
                logger.error(f"❌ {name}: {error_msg}")
                
                self.source_status[name].update({
                    'available': False,
                    'connected': False,
                    'last_test': datetime.now(),
                    'error_count': self.source_status[name]['error_count'] + 1,
                    'last_error': error_msg
                })
                
                results[name] = {
                    'success': False,
                    'adapter': name,
                    'message': error_msg
                }
        
        # 输出总结
        available_count = sum(1 for status in self.source_status.values() if status['available'])
        logger.info(f"📊 数据源连接测试完成: {available_count}/{len(self.adapters)} 可用")
        
        return results
    
    def get_available_sources(self) -> List[str]:
        """获取可用的数据源列表"""
        return [name for name, status in self.source_status.items() if status['available']]
    
    def get_best_source(self, data_type: str = 'price') -> Optional[str]:
        """获取最佳数据源
        
        Args:
            data_type: 数据类型 ('price', 'financial', 'index', etc.)
            
        Returns:
            str: 最佳数据源名称
        """
        available_sources = self.get_available_sources()
        
        if not available_sources:
            return None
        
        # 数据类型优先级映射
        type_preferences = {
            'price': ['uqer', 'tushare', 'akshare', 'yahoo'],
            'financial': ['uqer', 'tushare', 'akshare'],
            'index': ['uqer', 'tushare', 'akshare'],
            'market': ['akshare', 'yahoo', 'uqer'],
            'global': ['yahoo', 'akshare']
        }
        
        preferences = type_preferences.get(data_type, self.priority_order)
        
        # 按优先级返回第一个可用的数据源
        for source in preferences:
            if source in available_sources:
                return source
        
        # 如果没有匹配的优先级，返回第一个可用的
        return available_sources[0]
    
    def get_data_with_fallback(self, 
                              method_name: str,
                              *args, 
                              sources: Optional[List[str]] = None,
                              **kwargs) -> pd.DataFrame:
        """使用备用方案获取数据
        
        Args:
            method_name: 方法名称 ('get_price_data', 'get_stock_list', etc.)
            *args: 位置参数
            sources: 指定的数据源列表，如果为None则使用优先级顺序
            **kwargs: 关键字参数
            
        Returns:
            pd.DataFrame: 获取的数据
        """
        if sources is None:
            sources = self.priority_order
        
        available_sources = self.get_available_sources()
        sources_to_try = [s for s in sources if s in available_sources]
        
        if not sources_to_try:
            logger.error("❌ 没有可用的数据源")
            return pd.DataFrame()
        
        for source_name in sources_to_try:
            try:
                adapter = self.adapters[source_name]
                
                # 确保连接
                if not adapter.is_connected:
                    if not adapter.connect():
                        continue
                
                # 调用方法
                method = getattr(adapter, method_name)
                result = method(*args, **kwargs)
                
                if isinstance(result, pd.DataFrame) and not result.empty:
                    logger.info(f"✅ 使用{source_name}成功获取数据，共{len(result)}条记录")
                    
                    # 重置错误计数
                    self.source_status[source_name]['error_count'] = 0
                    return result
                else:
                    logger.warning(f"⚠️ {source_name}返回空数据")
                    continue
                    
            except Exception as e:
                error_msg = f"{source_name}获取数据失败: {str(e)}"
                logger.warning(f"⚠️ {error_msg}")
                
                # 更新错误状态
                self.source_status[source_name]['error_count'] += 1
                self.source_status[source_name]['last_error'] = error_msg
                
                # 如果错误次数过多，标记为不可用
                if self.source_status[source_name]['error_count'] >= 3:
                    self.source_status[source_name]['available'] = False
                    logger.error(f"❌ {source_name}错误次数过多，标记为不可用")
                
                continue
        
        logger.error("❌ 所有数据源都无法获取数据")
        return pd.DataFrame()
    
    def get_stock_list(self, market: Optional[str] = None, **kwargs) -> pd.DataFrame:
        """获取股票列表"""
        return self.get_data_with_fallback('get_stock_list', market=market, **kwargs)
    
    def get_price_data(self,
                      symbols: Union[str, List[str]],
                      start_date: Union[str, date, datetime],
                      end_date: Union[str, date, datetime],
                      **kwargs) -> pd.DataFrame:
        """获取价格数据"""
        return self.get_data_with_fallback(
            'get_price_data', 
            symbols, start_date, end_date, 
            **kwargs
        )
    
    def get_financial_data(self,
                          symbols: Union[str, List[str]],
                          start_date: Union[str, date, datetime],
                          end_date: Union[str, date, datetime],
                          **kwargs) -> pd.DataFrame:
        """获取财务数据"""
        return self.get_data_with_fallback(
            'get_financial_data',
            symbols, start_date, end_date,
            **kwargs
        )
    
    def get_index_components(self, index_code: str, **kwargs) -> pd.DataFrame:
        """获取指数成分股"""
        return self.get_data_with_fallback('get_index_components', index_code, **kwargs)
    
    def get_market_data(self, data_type: str, 
                       start_date: Union[str, date, datetime],
                       end_date: Union[str, date, datetime],
                       **kwargs) -> pd.DataFrame:
        """获取市场数据"""
        return self.get_data_with_fallback(
            'get_market_data', 
            data_type, start_date, end_date,
            **kwargs
        )
    
    def get_parallel_data(self,
                         method_name: str,
                         symbol_groups: List[List[str]],
                         *args,
                         max_workers: int = 3,
                         **kwargs) -> pd.DataFrame:
        """并行获取数据
        
        Args:
            method_name: 方法名称
            symbol_groups: 股票代码分组列表
            *args: 其他位置参数
            max_workers: 最大工作线程数
            **kwargs: 关键字参数
            
        Returns:
            pd.DataFrame: 合并的数据
        """
        if not symbol_groups:
            return pd.DataFrame()
        
        all_results = []
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 提交任务
            futures = []
            for symbols in symbol_groups:
                future = executor.submit(
                    self.get_data_with_fallback,
                    method_name, symbols, *args, **kwargs
                )
                futures.append(future)
            
            # 收集结果
            for future in as_completed(futures):
                try:
                    result = future.result()
                    if isinstance(result, pd.DataFrame) and not result.empty:
                        all_results.append(result)
                except Exception as e:
                    logger.error(f"❌ 并行获取数据失败: {str(e)}")
        
        # 合并结果
        if all_results:
            combined = pd.concat(all_results, ignore_index=True)
            logger.info(f"✅ 并行获取完成，共{len(combined)}条记录")
            return combined
        else:
            logger.warning("⚠️ 并行获取未返回任何数据")
            return pd.DataFrame()
    
    def get_status_report(self) -> Dict[str, Any]:
        """获取数据源状态报告"""
        report = {
            'timestamp': datetime.now().isoformat(),
            'total_sources': len(self.adapters),
            'available_sources': len(self.get_available_sources()),
            'sources_detail': {}
        }
        
        for name, status in self.source_status.items():
            report['sources_detail'][name] = {
                'available': status['available'],
                'connected': status['connected'],
                'error_count': status['error_count'],
                'last_error': status['last_error'],
                'last_test': status['last_test'].isoformat() if status['last_test'] else None
            }
        
        return report
    
    def cleanup(self):
        """清理资源"""
        for name, adapter in self.adapters.items():
            try:
                if adapter.is_connected:
                    adapter.disconnect()
                    logger.info(f"✅ {name}连接已断开")
            except Exception as e:
                logger.error(f"❌ 断开{name}连接失败: {str(e)}")
    
    def __enter__(self):
        """上下文管理器入口"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器出口"""
        self.cleanup()