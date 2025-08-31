#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
技术指标数据下载器 - 统一版本
==========================

从scripts/data/迁移而来，集成到统一数据架构中
支持各种技术指标数据的获取和计算

Author: QuantTrader Team
Date: 2025-08-31
"""

import os
import json
import time
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Optional, Union, Dict, Any
import logging
from tqdm import tqdm

try:
    import pandas as pd
    import numpy as np
except ImportError:
    pd = None
    np = None

try:
    import talib
except ImportError:
    talib = None

from ..adapters.data_source_manager import DataSourceManager
from ..cache_manager import SmartCacheManager

logger = logging.getLogger(__name__)

class IndicatorDownloader:
    """技术指标数据下载器
    
    功能：
    1. 基础技术指标计算和下载
    2. 自定义指标计算
    3. 批量指标处理
    4. 指标数据缓存
    5. 多时间周期支持
    """
    
    def __init__(self, config: Optional[Dict] = None):
        """初始化下载器
        
        Args:
            config: 配置参数
        """
        self.config = config or {}
        
        # 检查依赖
        self._check_dependencies()
        
        # 初始化数据源管理器
        self.data_source_manager = DataSourceManager(self.config)
        
        # 缓存配置
        cache_config = {
            'cache_dir': self.config.get('cache_dir', './data/cache'),
            'max_memory_size': 50 * 1024 * 1024,  # 50MB
            'default_expire_hours': 12  # 指标数据缓存时间
        }
        self.cache_manager = SmartCacheManager(cache_config)
        
        # 数据目录
        data_dir = self.config.get('data_dir', './data')
        self.indicators_dir = Path(data_dir) / 'processed' / 'indicators'
        self.indicators_dir.mkdir(parents=True, exist_ok=True)
        
        # 支持的指标类型
        self.supported_indicators = self._get_supported_indicators()
        
        # 下载配置
        self.delay = self.config.get('delay', 0.1)
        self.batch_size = self.config.get('batch_size', 50)
    
    def _check_dependencies(self):
        """检查依赖包"""
        missing_deps = []
        
        if pd is None:
            missing_deps.append('pandas')
        if np is None:
            missing_deps.append('numpy')
        
        if missing_deps:
            logger.warning(f"缺少依赖包: {missing_deps}")
        
        if talib is None:
            logger.info("TA-Lib未安装，将使用基础指标计算")
        else:
            logger.info("TA-Lib可用，支持高级技术指标")
    
    def _get_supported_indicators(self) -> Dict[str, Dict]:
        """获取支持的技术指标列表"""
        indicators = {
            # 趋势指标
            'SMA': {'name': '简单移动平均', 'params': ['period'], 'category': 'trend'},
            'EMA': {'name': '指数移动平均', 'params': ['period'], 'category': 'trend'},
            'MACD': {'name': 'MACD', 'params': ['fast', 'slow', 'signal'], 'category': 'trend'},
            'BOLL': {'name': '布林带', 'params': ['period', 'std'], 'category': 'trend'},
            
            # 动量指标
            'RSI': {'name': '相对强弱指数', 'params': ['period'], 'category': 'momentum'},
            'STOCH': {'name': '随机指标', 'params': ['k_period', 'd_period'], 'category': 'momentum'},
            'CCI': {'name': '顺势指标', 'params': ['period'], 'category': 'momentum'},
            
            # 成交量指标
            'OBV': {'name': '成交量平衡指标', 'params': [], 'category': 'volume'},
            'VOL_SMA': {'name': '成交量移动平均', 'params': ['period'], 'category': 'volume'},
            
            # 波动率指标
            'ATR': {'name': '真实波动范围', 'params': ['period'], 'category': 'volatility'},
            'VOLATILITY': {'name': '历史波动率', 'params': ['period'], 'category': 'volatility'},
        }
        
        return indicators
    
    def calculate_sma(self, data: pd.DataFrame, period: int = 20) -> pd.Series:
        """计算简单移动平均"""
        if 'close' not in data.columns:
            raise ValueError("数据必须包含'close'列")
        
        return data['close'].rolling(window=period).mean()
    
    def calculate_ema(self, data: pd.DataFrame, period: int = 20) -> pd.Series:
        """计算指数移动平均"""
        if 'close' not in data.columns:
            raise ValueError("数据必须包含'close'列")
        
        if talib is not None:
            return pd.Series(talib.EMA(data['close'].values, timeperiod=period), index=data.index)
        else:
            return data['close'].ewm(span=period).mean()
    
    def calculate_macd(self, data: pd.DataFrame, 
                      fast: int = 12, slow: int = 26, signal: int = 9) -> pd.DataFrame:
        """计算MACD"""
        if 'close' not in data.columns:
            raise ValueError("数据必须包含'close'列")
        
        if talib is not None:
            macd, macdsignal, macdhist = talib.MACD(
                data['close'].values, 
                fastperiod=fast, 
                slowperiod=slow, 
                signalperiod=signal
            )
            return pd.DataFrame({
                'MACD': macd,
                'MACD_Signal': macdsignal,
                'MACD_Hist': macdhist
            }, index=data.index)
        else:
            # 基础计算
            ema_fast = self.calculate_ema(data, fast)
            ema_slow = self.calculate_ema(data, slow)
            macd_line = ema_fast - ema_slow
            signal_line = macd_line.ewm(span=signal).mean()
            histogram = macd_line - signal_line
            
            return pd.DataFrame({
                'MACD': macd_line,
                'MACD_Signal': signal_line,
                'MACD_Hist': histogram
            })
    
    def calculate_rsi(self, data: pd.DataFrame, period: int = 14) -> pd.Series:
        """计算RSI"""
        if 'close' not in data.columns:
            raise ValueError("数据必须包含'close'列")
        
        if talib is not None:
            return pd.Series(talib.RSI(data['close'].values, timeperiod=period), index=data.index)
        else:
            # 基础计算
            delta = data['close'].diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
            rs = gain / loss
            return 100 - (100 / (1 + rs))
    
    def calculate_bollinger_bands(self, data: pd.DataFrame, 
                                period: int = 20, std: float = 2.0) -> pd.DataFrame:
        """计算布林带"""
        if 'close' not in data.columns:
            raise ValueError("数据必须包含'close'列")
        
        if talib is not None:
            upper, middle, lower = talib.BBANDS(
                data['close'].values,
                timeperiod=period,
                nbdevup=std,
                nbdevdn=std,
                matype=0
            )
            return pd.DataFrame({
                'BB_Upper': upper,
                'BB_Middle': middle,
                'BB_Lower': lower
            }, index=data.index)
        else:
            # 基础计算
            sma = self.calculate_sma(data, period)
            std_dev = data['close'].rolling(window=period).std()
            
            return pd.DataFrame({
                'BB_Upper': sma + (std_dev * std),
                'BB_Middle': sma,
                'BB_Lower': sma - (std_dev * std)
            })
    
    def calculate_atr(self, data: pd.DataFrame, period: int = 14) -> pd.Series:
        """计算平均真实范围"""
        required_cols = ['high', 'low', 'close']
        if not all(col in data.columns for col in required_cols):
            raise ValueError(f"数据必须包含列: {required_cols}")
        
        if talib is not None:
            return pd.Series(talib.ATR(
                data['high'].values,
                data['low'].values,
                data['close'].values,
                timeperiod=period
            ), index=data.index)
        else:
            # 基础计算
            high_low = data['high'] - data['low']
            high_cp = np.abs(data['high'] - data['close'].shift())
            low_cp = np.abs(data['low'] - data['close'].shift())
            
            ranges = pd.concat([high_low, high_cp, low_cp], axis=1).max(axis=1)
            return ranges.rolling(period).mean()
    
    def calculate_indicators_for_symbol(self, 
                                      symbol: str,
                                      indicators: List[str] = None,
                                      params: Dict[str, Dict] = None,
                                      start_date: Union[str, datetime] = None,
                                      end_date: Union[str, datetime] = None) -> pd.DataFrame:
        """为单只股票计算技术指标
        
        Args:
            symbol: 股票代码
            indicators: 要计算的指标列表
            params: 指标参数配置
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            包含所有指标的DataFrame
        """
        if indicators is None:
            indicators = ['SMA', 'EMA', 'RSI', 'MACD']
        
        if params is None:
            params = {}
        
        try:
            # 获取价格数据
            price_data = self.data_source_manager.get_price_data(
                symbols=[symbol],
                start_date=start_date or '2020-01-01',
                end_date=end_date or datetime.now().strftime('%Y-%m-%d')
            )
            
            if price_data is None or price_data.empty:
                logger.warning(f"无法获取 {symbol} 的价格数据")
                return pd.DataFrame()
            
            # 初始化结果DataFrame
            result = price_data.copy()
            
            # 计算各个指标
            for indicator in indicators:
                try:
                    indicator_params = params.get(indicator, {})
                    
                    if indicator == 'SMA':
                        period = indicator_params.get('period', 20)
                        result[f'SMA_{period}'] = self.calculate_sma(price_data, period)
                    
                    elif indicator == 'EMA':
                        period = indicator_params.get('period', 20)
                        result[f'EMA_{period}'] = self.calculate_ema(price_data, period)
                    
                    elif indicator == 'RSI':
                        period = indicator_params.get('period', 14)
                        result[f'RSI_{period}'] = self.calculate_rsi(price_data, period)
                    
                    elif indicator == 'MACD':
                        fast = indicator_params.get('fast', 12)
                        slow = indicator_params.get('slow', 26)
                        signal = indicator_params.get('signal', 9)
                        macd_data = self.calculate_macd(price_data, fast, slow, signal)
                        for col in macd_data.columns:
                            result[col] = macd_data[col]
                    
                    elif indicator == 'BOLL':
                        period = indicator_params.get('period', 20)
                        std = indicator_params.get('std', 2.0)
                        bb_data = self.calculate_bollinger_bands(price_data, period, std)
                        for col in bb_data.columns:
                            result[col] = bb_data[col]
                    
                    elif indicator == 'ATR':
                        period = indicator_params.get('period', 14)
                        result[f'ATR_{period}'] = self.calculate_atr(price_data, period)
                    
                    logger.debug(f"{symbol}: 计算 {indicator} 完成")
                    
                except Exception as e:
                    logger.error(f"{symbol}: 计算 {indicator} 失败 - {e}")
                    continue
            
            return result
            
        except Exception as e:
            logger.error(f"计算 {symbol} 技术指标失败: {e}")
            return pd.DataFrame()
    
    def download_indicators_batch(self, 
                                symbols: List[str],
                                indicators: List[str] = None,
                                save_to_file: bool = True) -> Dict[str, int]:
        """批量下载技术指标数据
        
        Args:
            symbols: 股票代码列表
            indicators: 指标列表
            save_to_file: 是否保存到文件
            
        Returns:
            下载统计结果
        """
        logger.info(f"开始批量计算技术指标，股票数量: {len(symbols)}")
        
        success_count = 0
        failed_count = 0
        
        with tqdm(total=len(symbols), desc="技术指标计算") as pbar:
            for symbol in symbols:
                try:
                    # 检查缓存
                    cache_key = f"indicators_{symbol}_{'-'.join(indicators or [])}"
                    cached_data = self.cache_manager.get('indicators', {'symbol': symbol})
                    
                    if cached_data is not None:
                        indicators_data = cached_data
                        logger.debug(f"{symbol}: 使用缓存的指标数据")
                    else:
                        # 计算指标
                        indicators_data = self.calculate_indicators_for_symbol(symbol, indicators)
                        
                        # 缓存数据
                        if not indicators_data.empty:
                            self.cache_manager.put('indicators', {'symbol': symbol}, indicators_data)
                    
                    if not indicators_data.empty:
                        if save_to_file:
                            # 保存到文件
                            file_path = self.indicators_dir / f"{symbol}_indicators.csv"
                            indicators_data.to_csv(file_path, index=True)
                        
                        success_count += 1
                    else:
                        failed_count += 1
                    
                except Exception as e:
                    logger.error(f"处理 {symbol} 技术指标失败: {e}")
                    failed_count += 1
                
                pbar.update(1)
                pbar.set_postfix({
                    '成功': success_count,
                    '失败': failed_count
                })
                
                time.sleep(self.delay)
        
        logger.info(f"技术指标计算完成: 成功 {success_count}, 失败 {failed_count}")
        return {'success': success_count, 'failed': failed_count}
    
    def get_supported_indicators_info(self) -> Dict[str, Any]:
        """获取支持的技术指标信息"""
        return {
            'total_indicators': len(self.supported_indicators),
            'indicators': self.supported_indicators,
            'categories': {
                'trend': [k for k, v in self.supported_indicators.items() if v['category'] == 'trend'],
                'momentum': [k for k, v in self.supported_indicators.items() if v['category'] == 'momentum'],
                'volume': [k for k, v in self.supported_indicators.items() if v['category'] == 'volume'],
                'volatility': [k for k, v in self.supported_indicators.items() if v['category'] == 'volatility']
            },
            'dependencies': {
                'pandas': pd is not None,
                'numpy': np is not None,
                'talib': talib is not None
            }
        }
    
    def cleanup_cache(self):
        """清理缓存"""
        self.cache_manager.clear_cache()
        logger.info("技术指标缓存已清理")
    
    def __enter__(self):
        """上下文管理器入口"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器出口"""
        if hasattr(self.data_source_manager, 'cleanup'):
            self.data_source_manager.cleanup()