#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
增强版数据管理器
===============

集成所有数据功能的统一管理器
"""

from typing import Dict, List, Optional, Union, Any, Tuple
import pandas as pd
from datetime import datetime, date, timedelta
import logging
from pathlib import Path

# 导入数据组件
from .adapters.data_source_manager import DataSourceManager
from .quality_checker import DataQualityChecker
from .cache_manager import SmartCacheManager

# 导入下载器
from .downloaders.a_shares_downloader import ASharesDownloader
from .downloaders.strategy_downloader import StrategyDownloader
from .downloaders.indicator_downloader import IndicatorDownloader

# 导入原有组件（如果存在）
try:
    from .data_processor import DataProcessor
    from .feature_engineer import FeatureEngineer
    LEGACY_COMPONENTS_AVAILABLE = True
except ImportError:
    LEGACY_COMPONENTS_AVAILABLE = False

logger = logging.getLogger(__name__)

class EnhancedDataManager:
    """增强版数据管理器
    
    统一管理所有数据相关功能：
    - 多数据源管理
    - 智能缓存系统
    - 数据质量检查
    - 数据处理和特征工程
    - 完整的数据流水线
    """
    
    def __init__(self, config: Optional[Dict] = None):
        """初始化增强版数据管理器
        
        Args:
            config: 配置参数
        """
        self.config = config or {}
        
        # 初始化各个组件
        logger.info("🚀 初始化增强版数据管理器...")
        
        # 数据源管理器
        data_source_config = self.config.get('data_sources', {})
        self.data_source_manager = DataSourceManager(data_source_config)
        
        # 缓存管理器
        cache_config = self.config.get('cache', {})
        self.cache_manager = SmartCacheManager(cache_config)
        
        # 数据质量检查器
        quality_config = self.config.get('quality', {})
        self.quality_checker = DataQualityChecker(quality_config)
        
        # 初始化下载器
        downloader_config = {**self.config, 'data_dir': self.config.get('data_dir', './data')}
        self.a_shares_downloader = ASharesDownloader(downloader_config)
        self.strategy_downloader = StrategyDownloader(downloader_config)
        self.indicator_downloader = IndicatorDownloader(downloader_config)
        
        # 数据处理器（如果可用）
        self.data_processor = None
        self.feature_engineer = None
        
        if LEGACY_COMPONENTS_AVAILABLE:
            try:
                self.data_processor = DataProcessor(self.config.get('processor', {}))
                self.feature_engineer = FeatureEngineer(self.config.get('feature_engineer', {}))
                logger.info("✅ 遗留数据组件加载成功")
            except Exception as e:
                logger.warning(f"⚠️ 遗留数据组件加载失败: {str(e)}")
        
        # 初始化数据源连接
        self._initialize_data_sources()
        
        logger.info("✅ 增强版数据管理器初始化完成")
    
    def _initialize_data_sources(self):
        """初始化数据源连接"""
        logger.info("🔗 测试数据源连接...")
        
        connection_results = self.data_source_manager.test_all_connections()
        available_sources = self.data_source_manager.get_available_sources()
        
        logger.info(f"✅ 数据源初始化完成，可用数据源: {available_sources}")
        
        if not available_sources:
            logger.warning("⚠️ 没有可用的数据源，某些功能可能受限")
    
    def get_stock_list(self, 
                      market: Optional[str] = None,
                      use_cache: bool = True,
                      **kwargs) -> pd.DataFrame:
        """获取股票列表
        
        Args:
            market: 市场代码
            use_cache: 是否使用缓存
            **kwargs: 其他参数
            
        Returns:
            pd.DataFrame: 股票列表
        """
        # 生成缓存键
        cache_params = {'market': market, **kwargs}
        
        # 尝试从缓存获取
        if use_cache:
            cached_data = self.cache_manager.get('stock_list', cache_params)
            if cached_data is not None:
                logger.info(f"✅ 从缓存获取股票列表，共{len(cached_data)}只股票")
                return cached_data
        
        # 从数据源获取
        logger.info(f"📊 获取股票列表 (市场: {market or '全部'})")
        data = self.data_source_manager.get_stock_list(market=market, **kwargs)
        
        if not data.empty:
            # 数据质量检查
            quality_result = self.quality_checker.check_data_types(
                data, 
                expected_types={'symbol': 'object', 'name': 'object'}
            )
            
            if quality_result.get('type_issues'):
                logger.warning(f"⚠️ 股票列表数据类型问题: {quality_result['type_issues']}")
            
            # 缓存数据
            if use_cache:
                self.cache_manager.put('stock_list', cache_params, data, expire_hours=24)
        
        return data
    
    def get_price_data(self,
                      symbols: Union[str, List[str]],
                      start_date: Union[str, date, datetime],
                      end_date: Union[str, date, datetime],
                      use_cache: bool = True,
                      quality_check: bool = True,
                      **kwargs) -> pd.DataFrame:
        """获取价格数据
        
        Args:
            symbols: 股票代码或代码列表
            start_date: 开始日期
            end_date: 结束日期
            use_cache: 是否使用缓存
            quality_check: 是否进行质量检查
            **kwargs: 其他参数
            
        Returns:
            pd.DataFrame: 价格数据
        """
        # 标准化参数
        if isinstance(symbols, str):
            symbols = [symbols]
        
        # 生成缓存键
        cache_params = {
            'symbols': sorted(symbols),
            'start_date': str(start_date),
            'end_date': str(end_date),
            **kwargs
        }
        
        # 尝试从缓存获取
        if use_cache:
            cached_data = self.cache_manager.get('price_data', cache_params)
            if cached_data is not None:
                logger.info(f"✅ 从缓存获取价格数据，共{len(cached_data)}条记录")
                return cached_data
        
        # 从数据源获取
        logger.info(f"📊 获取价格数据: {len(symbols)}只股票, {start_date} 至 {end_date}")
        data = self.data_source_manager.get_price_data(
            symbols=symbols,
            start_date=start_date,
            end_date=end_date,
            **kwargs
        )
        
        if not data.empty:
            # 数据质量检查
            if quality_check:
                self._perform_price_data_quality_check(data)
            
            # 缓存数据
            if use_cache:
                # 根据数据量设置缓存过期时间
                expire_hours = 1 if len(data) > 10000 else 6
                self.cache_manager.put('price_data', cache_params, data, expire_hours=expire_hours)
        
        return data
    
    def get_financial_data(self,
                          symbols: Union[str, List[str]],
                          start_date: Union[str, date, datetime],
                          end_date: Union[str, date, datetime],
                          report_type: str = 'annual',
                          use_cache: bool = True,
                          **kwargs) -> pd.DataFrame:
        """获取财务数据
        
        Args:
            symbols: 股票代码或代码列表
            start_date: 开始日期
            end_date: 结束日期
            report_type: 报告类型
            use_cache: 是否使用缓存
            **kwargs: 其他参数
            
        Returns:
            pd.DataFrame: 财务数据
        """
        # 标准化参数
        if isinstance(symbols, str):
            symbols = [symbols]
        
        # 生成缓存键
        cache_params = {
            'symbols': sorted(symbols),
            'start_date': str(start_date),
            'end_date': str(end_date),
            'report_type': report_type,
            **kwargs
        }
        
        # 尝试从缓存获取
        if use_cache:
            cached_data = self.cache_manager.get('financial_data', cache_params)
            if cached_data is not None:
                logger.info(f"✅ 从缓存获取财务数据，共{len(cached_data)}条记录")
                return cached_data
        
        # 从数据源获取
        logger.info(f"📊 获取财务数据: {len(symbols)}只股票, 报告类型: {report_type}")
        data = self.data_source_manager.get_financial_data(
            symbols=symbols,
            start_date=start_date,
            end_date=end_date,
            report_type=report_type,
            **kwargs
        )
        
        if not data.empty:
            # 缓存数据 (财务数据更新频率较低，可以缓存更长时间)
            if use_cache:
                self.cache_manager.put('financial_data', cache_params, data, expire_hours=48)
        
        return data
    
    def _perform_price_data_quality_check(self, data: pd.DataFrame):
        """执行价格数据质量检查"""
        try:
            # 检查缺失数据
            missing_result = self.quality_checker.check_missing_data(
                data, 
                critical_columns=['date', 'symbol', 'close']
            )
            
            if missing_result.get('critical_issues'):
                logger.warning(f"⚠️ 价格数据关键列缺失: {missing_result['critical_issues']}")
            
            # 检查价格一致性
            consistency_result = self.quality_checker.check_price_data_consistency(data)
            
            if consistency_result.get('consistency_issues'):
                logger.warning(f"⚠️ 价格数据一致性问题: {consistency_result['consistency_issues']}")
            
            # 检查异常值
            outlier_result = self.quality_checker.check_outliers(
                data,
                numeric_columns=['open', 'high', 'low', 'close', 'volume']
            )
            
            high_outlier_cols = []
            for col, result in outlier_result.get('outlier_summary', {}).items():
                if result.get('outlier_rate', 0) > 0.05:  # 超过5%异常值
                    high_outlier_cols.append(col)
            
            if high_outlier_cols:
                logger.warning(f"⚠️ 价格数据异常值较多的列: {high_outlier_cols}")
                
        except Exception as e:
            logger.error(f"❌ 价格数据质量检查失败: {str(e)}")
    
    def generate_features(self, 
                         price_data: pd.DataFrame,
                         feature_types: Optional[List[str]] = None,
                         use_cache: bool = True) -> pd.DataFrame:
        """生成特征数据
        
        Args:
            price_data: 价格数据
            feature_types: 特征类型列表
            use_cache: 是否使用缓存
            
        Returns:
            pd.DataFrame: 特征数据
        """
        if self.feature_engineer is None:
            logger.error("❌ 特征工程器不可用")
            return pd.DataFrame()
        
        # 生成缓存键
        data_hash = pd.util.hash_pandas_object(price_data).sum()
        cache_params = {
            'data_hash': str(data_hash),
            'feature_types': sorted(feature_types or [])
        }
        
        # 尝试从缓存获取
        if use_cache:
            cached_features = self.cache_manager.get('features', cache_params)
            if cached_features is not None:
                logger.info(f"✅ 从缓存获取特征数据，共{len(cached_features)}条记录")
                return cached_features
        
        # 生成特征
        logger.info("🔧 生成特征数据...")
        
        try:
            if hasattr(self.feature_engineer, 'generate_features'):
                features = self.feature_engineer.generate_features(
                    price_data, 
                    feature_types=feature_types
                )
            else:
                # 使用旧版本方法
                features = self.feature_engineer.generate_all_features(price_data)
            
            if not features.empty:
                # 缓存特征数据
                if use_cache:
                    self.cache_manager.put('features', cache_params, features, expire_hours=12)
                
                logger.info(f"✅ 特征生成完成，共{len(features)}条记录，{len(features.columns)}个特征")
            
            return features
            
        except Exception as e:
            logger.error(f"❌ 特征生成失败: {str(e)}")
            return pd.DataFrame()
    
    def run_complete_pipeline(self,
                             symbols: Union[str, List[str]],
                             start_date: Union[str, date, datetime],
                             end_date: Union[str, date, datetime],
                             include_features: bool = True,
                             quality_report: bool = False) -> Dict[str, Any]:
        """运行完整的数据流水线
        
        Args:
            symbols: 股票代码或代码列表
            start_date: 开始日期
            end_date: 结束日期
            include_features: 是否生成特征
            quality_report: 是否生成质量报告
            
        Returns:
            Dict: 完整的数据结果
        """
        logger.info("🚀 启动完整数据流水线...")
        
        result = {
            'symbols': symbols,
            'date_range': (str(start_date), str(end_date)),
            'timestamp': datetime.now().isoformat(),
            'price_data': pd.DataFrame(),
            'features': pd.DataFrame(),
            'quality_report': None,
            'cache_stats': None,
            'data_source_status': None
        }
        
        try:
            # 1. 获取价格数据
            logger.info("📊 步骤1: 获取价格数据")
            price_data = self.get_price_data(
                symbols=symbols,
                start_date=start_date,
                end_date=end_date,
                quality_check=True
            )
            
            if price_data.empty:
                logger.error("❌ 未获取到价格数据，流水线终止")
                return result
            
            result['price_data'] = price_data
            logger.info(f"✅ 价格数据获取完成: {len(price_data)}条记录")
            
            # 2. 生成特征（如果需要）
            if include_features and self.feature_engineer is not None:
                logger.info("🔧 步骤2: 生成特征数据")
                features = self.generate_features(price_data)
                result['features'] = features
                
                if not features.empty:
                    logger.info(f"✅ 特征生成完成: {len(features)}条记录，{len(features.columns)}个特征")
                else:
                    logger.warning("⚠️ 特征生成失败或无特征数据")
            
            # 3. 生成质量报告（如果需要）
            if quality_report:
                logger.info("📊 步骤3: 生成数据质量报告")
                quality_result = self.quality_checker.generate_quality_report(
                    price_data, 
                    "价格数据质量报告"
                )
                result['quality_report'] = quality_result
                
                logger.info(f"✅ 质量报告生成完成，整体得分: {quality_result['overall_score']:.2f}")
            
            # 4. 获取缓存统计
            result['cache_stats'] = self.cache_manager.get_cache_stats()
            
            # 5. 获取数据源状态
            result['data_source_status'] = self.data_source_manager.get_status_report()
            
            logger.info("🎉 完整数据流水线执行成功")
            
        except Exception as e:
            logger.error(f"❌ 数据流水线执行失败: {str(e)}")
            result['error'] = str(e)
        
        return result
    
    def get_cache_statistics(self) -> Dict[str, Any]:
        """获取缓存统计信息"""
        return self.cache_manager.get_cache_stats()
    
    def get_data_source_status(self) -> Dict[str, Any]:
        """获取数据源状态"""
        return self.data_source_manager.get_status_report()
    
    def clear_cache(self, cache_type: Optional[str] = None):
        """清理缓存
        
        Args:
            cache_type: 缓存类型，None为清理全部
        """
        logger.info(f"🧹 清理缓存 (类型: {cache_type or '全部'})")
        self.cache_manager.clear_cache(cache_type)
    
    def cleanup_expired_cache(self):
        """清理过期缓存"""
        logger.info("🧹 清理过期缓存")
        self.cache_manager.cleanup_expired()
    
    def validate_data_pipeline(self) -> Dict[str, Any]:
        """验证数据流水线状态
        
        Returns:
            Dict: 验证结果
        """
        logger.info("🔍 验证数据流水线状态...")
        
        validation_result = {
            'timestamp': datetime.now().isoformat(),
            'overall_status': 'unknown',
            'components': {},
            'recommendations': []
        }
        
        # 检查数据源管理器
        available_sources = self.data_source_manager.get_available_sources()
        validation_result['components']['data_sources'] = {
            'status': 'ok' if available_sources else 'error',
            'available_count': len(available_sources),
            'available_sources': available_sources
        }
        
        if not available_sources:
            validation_result['recommendations'].append("没有可用的数据源，请检查API配置")
        
        # 检查缓存管理器
        cache_stats = self.cache_manager.get_cache_stats()
        validation_result['components']['cache'] = {
            'status': 'ok',
            'hit_rate': cache_stats['statistics']['hit_rate'],
            'memory_usage_mb': cache_stats['memory_cache']['usage_mb']
        }
        
        # 检查质量检查器
        validation_result['components']['quality_checker'] = {
            'status': 'ok' if self.quality_checker else 'error'
        }
        
        # 检查特征工程器
        validation_result['components']['feature_engineer'] = {
            'status': 'ok' if self.feature_engineer else 'warning',
            'message': '特征工程器不可用' if not self.feature_engineer else '正常'
        }
        
        # 整体状态评估
        component_statuses = [comp['status'] for comp in validation_result['components'].values()]
        if 'error' in component_statuses:
            validation_result['overall_status'] = 'error'
        elif 'warning' in component_statuses:
            validation_result['overall_status'] = 'warning'
        else:
            validation_result['overall_status'] = 'ok'
        
        logger.info(f"✅ 数据流水线验证完成，状态: {validation_result['overall_status']}")
        return validation_result
    
    # ===================================
    # 数据下载功能
    # ===================================
    
    def download_a_shares_data(self, 
                              symbols: Optional[List[str]] = None,
                              market: Optional[str] = None,
                              start_date: Union[str, datetime] = None,
                              end_date: Union[str, datetime] = None,
                              resume: bool = True) -> Dict[str, int]:
        """下载A股数据
        
        Args:
            symbols: 股票代码列表，如果为None则下载所有股票
            market: 市场代码
            start_date: 开始日期
            end_date: 结束日期
            resume: 是否断点续传
            
        Returns:
            下载统计结果
        """
        logger.info("🚀 开始下载A股数据...")
        
        if symbols is not None:
            return self.a_shares_downloader.download_batch(symbols, start_date, end_date, resume)
        else:
            return self.a_shares_downloader.download_all(market, start_date, end_date, resume)
    
    def download_strategy_data(self,
                             symbols: Optional[List[str]] = None,
                             data_types: List[str] = None) -> Dict[str, Dict[str, int]]:
        """下载策略数据
        
        Args:
            symbols: 股票代码列表
            data_types: 数据类型列表 ['capital_flow', 'market_sentiment', etc.]
            
        Returns:
            各类型数据的下载统计结果
        """
        logger.info("🚀 开始下载策略数据...")
        return self.strategy_downloader.download_all_strategy_data(symbols, data_types)
    
    def download_indicators_data(self,
                               symbols: List[str],
                               indicators: List[str] = None) -> Dict[str, int]:
        """下载/计算技术指标数据
        
        Args:
            symbols: 股票代码列表
            indicators: 技术指标列表
            
        Returns:
            下载统计结果
        """
        logger.info("🚀 开始计算技术指标数据...")
        return self.indicator_downloader.download_indicators_batch(symbols, indicators)
    
    def get_download_status(self) -> Dict[str, Any]:
        """获取全部下载状态报告"""
        return {
            'a_shares': self.a_shares_downloader.get_download_status(),
            'strategy': self.strategy_downloader.get_download_status(),
            'indicators': self.indicator_downloader.get_supported_indicators_info(),
            'data_sources': self.data_source_manager.get_status_report(),
            'cache': self.cache_manager.get_cache_stats()
        }
    
    def cleanup_all_cache(self):
        """清理所有缓存"""
        logger.info("🧹 清理所有缓存...")
        self.cache_manager.clear_cache()
        self.a_shares_downloader.cleanup_cache()
        self.strategy_downloader.cleanup_cache()
        self.indicator_downloader.cleanup_cache()
        logger.info("✅ 所有缓存清理完成")
    
    def __enter__(self):
        """上下文管理器入口"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器出口"""
        # 清理资源
        if hasattr(self, 'data_source_manager'):
            self.data_source_manager.cleanup()
        
        logger.info("✅ 增强版数据管理器资源清理完成")