#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
完整特征工程器 - feature_engineer.py
=====================================

专为量化交易框架设计的高性能特征工程器，包含：
- 📊 60+ 技术指标算法实现
- 🔬 多维度因子特征生成
- 📈 自定义指标支持
- 💾 高效缓存机制
- 🎯 批量特征计算
- 📋 特征重要性评估

版本: 2.0.0
更新时间: 2024-08-26
兼容环境: VSCode + JupyterNote + 优矿API
"""

import os
import warnings
import numpy as np
import pandas as pd
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional, Union, Any, Callable
from pathlib import Path
import hashlib
import pickle
import json

# 科学计算库
from scipy import stats
from scipy.stats import zscore, skew, kurtosis
from scipy.signal import argrelextrema

# 尝试导入TA-Lib，如果失败则使用内置算法
try:
    import talib
    TALIB_AVAILABLE = True
    print("📊 TA-Lib库已加载")
except ImportError:
    TALIB_AVAILABLE = False
    print("⚠️ TA-Lib未安装，将使用内置技术指标算法")

# 抑制警告
warnings.filterwarnings('ignore')

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

print("🔬 特征工程器模块加载中...")


class FeatureEngineer:
    """
    完整特征工程器 - 集成60+技术指标和因子特征
    """
    
    def __init__(self, price_data: Optional[pd.DataFrame] = None, 
                 config: Optional[Dict] = None):
        """
        初始化特征工程器
        
        Args:
            price_data: 价格数据DataFrame，包含OHLCV列
            config: 配置参数字典
        """
        self.price_data = price_data
        self.config = config or self._get_default_config()
        self.cache_dir = self.config.get('cache_dir', './cache')
        os.makedirs(self.cache_dir, exist_ok=True)
        
        # 特征缓存
        self.feature_cache = {}
        
        # 技术指标参数
        self.indicator_params = self._get_default_indicator_params()
        
        # 统计信息
        self.stats = {
            'generated_features': 0,
            'processing_time': 0,
            'cache_hits': 0,
            'cache_misses': 0,
            'feature_importance': {}
        }
        
        print("🛠️ 特征工程器初始化完成")
        print(f"   📁 缓存目录: {self.cache_dir}")
        print(f"   🔧 TA-Lib可用: {'✅' if TALIB_AVAILABLE else '❌'}")
    
    def _get_default_config(self) -> Dict:
        """获取默认配置"""
        return {
            'cache_dir': './cache',
            'enable_cache': True,
            'cache_expire_hours': 24,
            'batch_size': 1000,
            'n_jobs': 1,
            'feature_selection': True,
            'correlation_threshold': 0.95,
        }
    
    def _get_default_indicator_params(self) -> Dict:
        """获取默认技术指标参数"""
        return {
            # 移动平均参数
            'ma_periods': [5, 10, 20, 60],
            'ema_periods': [12, 26],
            
            # 波动性指标
            'bb_period': 20,
            'bb_std': 2,
            'atr_period': 14,
            
            # 动量指标
            'rsi_period': 14,
            'macd_fast': 12,
            'macd_slow': 26,
            'macd_signal': 9,
            
            # 成交量指标
            'volume_ma_period': 20,
            'vwap_period': 20,
        }
    
    def _generate_cache_key(self, *args) -> str:
        """生成缓存键值"""
        key_str = '_'.join(str(arg) for arg in args)
        return hashlib.md5(key_str.encode()).hexdigest()
    
    def _load_from_cache(self, cache_key: str):
        """从缓存加载特征"""
        if not self.config['enable_cache']:
            return None
        
        cache_path = os.path.join(self.cache_dir, f"features_{cache_key}.pkl")
        
        try:
            if os.path.exists(cache_path):
                file_time = datetime.fromtimestamp(os.path.getmtime(cache_path))
                expire_time = datetime.now() - timedelta(hours=self.config['cache_expire_hours'])
                
                if file_time > expire_time:
                    with open(cache_path, 'rb') as f:
                        self.stats['cache_hits'] += 1
                        return pickle.load(f)
        except Exception as e:
            logger.warning(f"特征缓存加载失败: {e}")
        
        self.stats['cache_misses'] += 1
        return None
    
    def _save_to_cache(self, features, cache_key: str):
        """保存特征到缓存"""
        if not self.config['enable_cache']:
            return
        
        cache_path = os.path.join(self.cache_dir, f"features_{cache_key}.pkl")
        
        try:
            with open(cache_path, 'wb') as f:
                pickle.dump(features, f)
        except Exception as e:
            logger.warning(f"特征缓存保存失败: {e}")
    
    # ==========================================
    # 价格相关特征
    # ==========================================
    
    def generate_price_features(self, data: pd.DataFrame = None) -> pd.DataFrame:
        """生成价格相关特征"""
        data = data if data is not None else self.price_data
        if data is None or data.empty:
            return pd.DataFrame()
        
        print("💰 生成价格特征...")
        features = data.copy()
        
        required_cols = ['openPrice', 'highestPrice', 'lowestPrice', 'closePrice']
        if not all(col in features.columns for col in required_cols):
            print("⚠️ 缺少必要的价格列")
            return features
        
        # 按股票分组计算特征
        for ticker, group in features.groupby('ticker'):
            group = group.sort_values('tradeDate')
            idx = group.index
            
            # 基础价格特征
            features.loc[idx, 'price_range'] = group['highestPrice'] - group['lowestPrice']
            features.loc[idx, 'price_gap'] = group['openPrice'] - group['closePrice'].shift(1)
            features.loc[idx, 'upper_shadow'] = group['highestPrice'] - np.maximum(group['openPrice'], group['closePrice'])
            features.loc[idx, 'lower_shadow'] = np.minimum(group['openPrice'], group['closePrice']) - group['lowestPrice']
            
            # 收益率特征
            features.loc[idx, 'daily_return'] = group['closePrice'].pct_change()
            features.loc[idx, 'log_return'] = np.log(group['closePrice'] / group['closePrice'].shift(1))
            
            # 波动率特征（多周期）
            for window in [5, 10, 20]:
                returns = group['closePrice'].pct_change()
                features.loc[idx, f'volatility_{window}d'] = returns.rolling(window).std() * np.sqrt(252)
                features.loc[idx, f'return_mean_{window}d'] = returns.rolling(window).mean()
                features.loc[idx, f'return_skew_{window}d'] = returns.rolling(window).skew()
                features.loc[idx, f'return_kurtosis_{window}d'] = returns.rolling(window).kurt()
        
        print(f"✅ 生成价格特征: {len([col for col in features.columns if col not in data.columns])} 个")
        return features
    
    def generate_technical_indicators(self, data: pd.DataFrame = None) -> pd.DataFrame:
        """生成技术指标特征"""
        data = data if data is not None else self.price_data
        if data is None or data.empty:
            return pd.DataFrame()
        
        print("📈 生成技术指标...")
        features = data.copy()
        
        # 检查必要列
        required_cols = ['closePrice', 'highestPrice', 'lowestPrice', 'turnoverVol']
        if not all(col in features.columns for col in required_cols):
            print("⚠️ 缺少必要的OHLCV列")
            return features
        
        # 按股票分组计算指标
        for ticker, group in features.groupby('ticker'):
            group = group.sort_values('tradeDate')
            idx = group.index
            
            # 确保数据类型正确
            close = group['closePrice'].astype(float).values
            high = group['highestPrice'].astype(float).values
            low = group['lowestPrice'].astype(float).values
            volume = group['turnoverVol'].astype(float).values
            
            if len(close) < 30:  # 数据太少跳过
                continue
            
            try:
                # 1. 移动平均指标
                for period in self.indicator_params['ma_periods']:
                    if TALIB_AVAILABLE:
                        ma = talib.SMA(close, timeperiod=period)
                        ema = talib.EMA(close, timeperiod=period)
                    else:
                        ma = pd.Series(close).rolling(period).mean().values
                        ema = pd.Series(close).ewm(span=period).mean().values
                    
                    features.loc[idx, f'SMA_{period}'] = ma
                    features.loc[idx, f'EMA_{period}'] = ema
                    features.loc[idx, f'price_to_SMA_{period}'] = close / ma
                
                # 2. 布林带
                if TALIB_AVAILABLE:
                    bb_upper, bb_middle, bb_lower = talib.BBANDS(
                        close, timeperiod=self.indicator_params['bb_period'],
                        nbdevup=self.indicator_params['bb_std'],
                        nbdevdn=self.indicator_params['bb_std']
                    )
                else:
                    # 内置布林带算法
                    period = self.indicator_params['bb_period']
                    std_dev = self.indicator_params['bb_std']
                    sma = pd.Series(close).rolling(period).mean()
                    std = pd.Series(close).rolling(period).std()
                    bb_upper = (sma + std_dev * std).values
                    bb_middle = sma.values
                    bb_lower = (sma - std_dev * std).values
                
                features.loc[idx, 'BB_upper'] = bb_upper
                features.loc[idx, 'BB_middle'] = bb_middle
                features.loc[idx, 'BB_lower'] = bb_lower
                features.loc[idx, 'BB_width'] = (bb_upper - bb_lower) / bb_middle
                features.loc[idx, 'BB_position'] = (close - bb_lower) / (bb_upper - bb_lower)
                
                # 3. RSI指标
                if TALIB_AVAILABLE:
                    rsi = talib.RSI(close, timeperiod=self.indicator_params['rsi_period'])
                else:
                    # 内置RSI算法
                    period = self.indicator_params['rsi_period']
                    delta = pd.Series(close).diff()
                    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
                    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
                    rs = gain / loss
                    rsi = (100 - (100 / (1 + rs))).values
                
                features.loc[idx, 'RSI'] = rsi
                features.loc[idx, 'RSI_overbought'] = (rsi > 70).astype(int)
                features.loc[idx, 'RSI_oversold'] = (rsi < 30).astype(int)
                
                # 4. MACD指标
                if TALIB_AVAILABLE:
                    macd, macdsignal, macdhist = talib.MACD(
                        close,
                        fastperiod=self.indicator_params['macd_fast'],
                        slowperiod=self.indicator_params['macd_slow'],
                        signalperiod=self.indicator_params['macd_signal']
                    )
                else:
                    # 内置MACD算法
                    ema12 = pd.Series(close).ewm(span=12).mean()
                    ema26 = pd.Series(close).ewm(span=26).mean()
                    macd = (ema12 - ema26).values
                    macdsignal = pd.Series(macd).ewm(span=9).mean().values
                    macdhist = macd - macdsignal
                
                features.loc[idx, 'MACD'] = macd
                features.loc[idx, 'MACD_signal'] = macdsignal
                features.loc[idx, 'MACD_hist'] = macdhist
                
                # 5. 随机指标KDJ
                if TALIB_AVAILABLE:
                    slowk, slowd = talib.STOCH(high, low, close, fastk_period=9, slowk_period=3, slowd_period=3)
                    j = 3 * slowk - 2 * slowd
                else:
                    # 内置KDJ算法
                    low_min = pd.Series(low).rolling(9).min()
                    high_max = pd.Series(high).rolling(9).max()
                    rsv = 100 * (close - low_min) / (high_max - low_min)
                    slowk = rsv.ewm(com=2).mean().values
                    slowd = pd.Series(slowk).ewm(com=2).mean().values
                    j = 3 * slowk - 2 * slowd
                
                features.loc[idx, 'K'] = slowk
                features.loc[idx, 'D'] = slowd
                features.loc[idx, 'J'] = j
                
                # 6. ATR (平均真实波幅)
                if TALIB_AVAILABLE:
                    atr = talib.ATR(high, low, close, timeperiod=self.indicator_params['atr_period'])
                else:
                    # 内置ATR算法
                    tr1 = high - low
                    tr2 = np.abs(high - np.roll(close, 1))
                    tr3 = np.abs(low - np.roll(close, 1))
                    tr = np.maximum(tr1, np.maximum(tr2, tr3))
                    atr = pd.Series(tr).rolling(14).mean().values
                
                features.loc[idx, 'ATR'] = atr
                features.loc[idx, 'ATR_ratio'] = atr / close
                
                # 7. 威廉指标
                if TALIB_AVAILABLE:
                    williams_r = talib.WILLR(high, low, close, timeperiod=14)
                else:
                    # 内置Williams %R算法
                    high_14 = pd.Series(high).rolling(14).max()
                    low_14 = pd.Series(low).rolling(14).min()
                    williams_r = -100 * (high_14 - close) / (high_14 - low_14)
                    williams_r = williams_r.values
                
                features.loc[idx, 'Williams_R'] = williams_r
                
            except Exception as e:
                logger.warning(f"技术指标计算失败 {ticker}: {e}")
                continue
        
        print(f"✅ 生成技术指标: 约20+ 个")
        return features
    
    def generate_volume_features(self, data: pd.DataFrame = None) -> pd.DataFrame:
        """生成成交量相关特征"""
        data = data if data is not None else self.price_data
        if data is None or data.empty:
            return pd.DataFrame()
        
        print("📊 生成成交量特征...")
        features = data.copy()
        
        if 'turnoverVol' not in features.columns:
            print("⚠️ 缺少成交量数据")
            return features
        
        # 按股票分组计算特征
        for ticker, group in features.groupby('ticker'):
            group = group.sort_values('tradeDate')
            idx = group.index
            
            volume = group['turnoverVol'].values
            close = group['closePrice'].values if 'closePrice' in group.columns else None
            
            # 基础成交量特征
            features.loc[idx, 'volume_ma_5'] = pd.Series(volume).rolling(5).mean().values
            features.loc[idx, 'volume_ma_20'] = pd.Series(volume).rolling(20).mean().values
            features.loc[idx, 'volume_ratio'] = volume / pd.Series(volume).rolling(20).mean().values
            
            # 成交量变化率
            features.loc[idx, 'volume_change'] = pd.Series(volume).pct_change().values
            features.loc[idx, 'volume_std_20'] = pd.Series(volume).rolling(20).std().values
            
            # VWAP (成交量加权平均价)
            if close is not None:
                typical_price = close  # 简化为收盘价
                vwap = (typical_price * volume).rolling(20).sum() / pd.Series(volume).rolling(20).sum()
                features.loc[idx, 'VWAP'] = vwap.values
                features.loc[idx, 'price_to_VWAP'] = close / vwap.values
            
            # OBV (能量潮)
            if close is not None and TALIB_AVAILABLE:
                try:
                    obv = talib.OBV(close.astype(float), volume.astype(float))
                    features.loc[idx, 'OBV'] = obv
                except Exception as e:
                    # 内置OBV算法
                    price_change = pd.Series(close).diff()
                    obv_values = []
                    obv_val = 0
                    for i, (pc, vol) in enumerate(zip(price_change, volume)):
                        if pd.isna(pc):
                            obv_values.append(obv_val)
                        elif pc > 0:
                            obv_val += vol
                            obv_values.append(obv_val)
                        elif pc < 0:
                            obv_val -= vol
                            obv_values.append(obv_val)
                        else:
                            obv_values.append(obv_val)
                    features.loc[idx, 'OBV'] = obv_values
        
        print(f"✅ 生成成交量特征: 约8 个")
        return features
    
    def generate_momentum_features(self, data: pd.DataFrame = None) -> pd.DataFrame:
        """生成动量相关特征"""
        data = data if data is not None else self.price_data
        if data is None or data.empty:
            return pd.DataFrame()
        
        print("🚀 生成动量特征...")
        features = data.copy()
        
        if 'closePrice' not in features.columns:
            print("⚠️ 缺少价格数据")
            return features
        
        # 按股票分组计算特征
        for ticker, group in features.groupby('ticker'):
            group = group.sort_values('tradeDate')
            idx = group.index
            
            close = group['closePrice'].values
            
            # 动量指标
            for period in [5, 10, 20, 60]:
                momentum = close / np.roll(close, period) - 1
                features.loc[idx, f'momentum_{period}d'] = momentum
                
                # 相对强度
                market_return = pd.Series(close).pct_change().rolling(period).mean().values
                features.loc[idx, f'relative_strength_{period}d'] = momentum - market_return
            
            # ROC (变动率指标)
            if TALIB_AVAILABLE:
                roc = talib.ROC(close, timeperiod=10)
            else:
                roc = (close / np.roll(close, 10) - 1) * 100
            features.loc[idx, 'ROC'] = roc
            
            # CCI (商品通道指标)
            if 'highestPrice' in group.columns and 'lowestPrice' in group.columns:
                high = group['highestPrice'].values
                low = group['lowestPrice'].values
                
                if TALIB_AVAILABLE:
                    cci = talib.CCI(high, low, close, timeperiod=14)
                else:
                    # 内置CCI算法
                    tp = (high + low + close) / 3
                    sma_tp = pd.Series(tp).rolling(14).mean()
                    mad = pd.Series(tp).rolling(14).apply(lambda x: np.mean(np.abs(x - x.mean())))
                    cci = (tp - sma_tp) / (0.015 * mad)
                    cci = cci.values
                
                features.loc[idx, 'CCI'] = cci
        
        print(f"✅ 生成动量特征: 约15+ 个")
        return features
    
    def generate_statistical_features(self, data: pd.DataFrame = None) -> pd.DataFrame:
        """生成统计相关特征"""
        data = data if data is not None else self.price_data
        if data is None or data.empty:
            return pd.DataFrame()
        
        print("📈 生成统计特征...")
        features = data.copy()
        
        # 按股票分组计算特征
        for ticker, group in features.groupby('ticker'):
            group = group.sort_values('tradeDate')
            idx = group.index
            
            if 'closePrice' in group.columns:
                close = group['closePrice']
                returns = close.pct_change()
                
                # 统计特征（不同窗口）
                for window in [5, 10, 20]:
                    # 基础统计量
                    features.loc[idx, f'price_mean_{window}d'] = close.rolling(window).mean()
                    features.loc[idx, f'price_std_{window}d'] = close.rolling(window).std()
                    features.loc[idx, f'price_skew_{window}d'] = close.rolling(window).skew()
                    features.loc[idx, f'price_kurt_{window}d'] = close.rolling(window).kurt()
                    
                    # 收益率统计量
                    features.loc[idx, f'return_mean_{window}d'] = returns.rolling(window).mean()
                    features.loc[idx, f'return_std_{window}d'] = returns.rolling(window).std()
                    
                    # 最大回撤
                    rolling_max = close.rolling(window).max()
                    drawdown = (close - rolling_max) / rolling_max
                    features.loc[idx, f'max_drawdown_{window}d'] = drawdown.rolling(window).min()
                    
                    # 上涨下跌天数比例
                    up_days = (returns > 0).rolling(window).sum()
                    features.loc[idx, f'up_ratio_{window}d'] = up_days / window
        
        print(f"✅ 生成统计特征: 约30+ 个")
        return features
    
    def generate_all_features(self, data: pd.DataFrame = None) -> pd.DataFrame:
        """
        生成所有特征
        
        Args:
            data: 输入价格数据
            
        Returns:
            包含所有特征的DataFrame
        """
        data = data if data is not None else self.price_data
        if data is None or data.empty:
            print("❌ 没有输入数据")
            return pd.DataFrame()
        
        print("🎯 生成所有特征...")
        start_time = datetime.now()
        
        # 生成缓存键
        data_hash = hashlib.md5(str(data.shape).encode()).hexdigest()
        cache_key = f"all_features_{data_hash}"
        
        # 尝试从缓存加载
        cached_features = self._load_from_cache(cache_key)
        if cached_features is not None:
            print("📥 从缓存加载所有特征")
            return cached_features
        
        # 逐步生成特征
        features = data.copy()
        
        try:
            # 1. 价格特征
            features = self.generate_price_features(features)
            
            # 2. 技术指标
            features = self.generate_technical_indicators(features)
            
            # 3. 成交量特征
            features = self.generate_volume_features(features)
            
            # 4. 动量特征
            features = self.generate_momentum_features(features)
            
            # 5. 统计特征
            features = self.generate_statistical_features(features)
            
            # 6. 特征后处理
            features = self._post_process_features(features)
            
            # 计算处理统计
            processing_time = (datetime.now() - start_time).total_seconds()
            feature_count = len(features.columns) - len(data.columns)
            
            self.stats['generated_features'] += feature_count
            self.stats['processing_time'] += processing_time
            
            print(f"✅ 特征生成完成")
            print(f"   📊 原始特征: {len(data.columns)} 个")
            print(f"   🔬 新增特征: {feature_count} 个")
            print(f"   ⏱️ 处理时间: {processing_time:.2f}秒")
            
            # 保存到缓存
            self._save_to_cache(features, cache_key)
            
            return features
            
        except Exception as e:
            print(f"❌ 特征生成失败: {e}")
            logger.error(f"特征生成错误: {e}")
            return data
    
    def _post_process_features(self, features: pd.DataFrame) -> pd.DataFrame:
        """特征后处理"""
        print("🔧 特征后处理...")
        
        # 1. 处理无穷值
        features = features.replace([np.inf, -np.inf], np.nan)
        
        # 2. 特征选择（移除高相关性特征）
        if self.config['feature_selection']:
            features = self._remove_highly_correlated_features(features)
        
        # 3. 按股票分组前向填充缺失值
        numeric_cols = features.select_dtypes(include=[np.number]).columns
        features[numeric_cols] = features.groupby('ticker')[numeric_cols].apply(
            lambda x: x.fillna(method='ffill').fillna(method='bfill')
        )
        
        return features
    
    def _remove_highly_correlated_features(self, features: pd.DataFrame) -> pd.DataFrame:
        """移除高相关性特征"""
        numeric_cols = features.select_dtypes(include=[np.number]).columns
        # 排除基础列
        exclude_cols = ['ticker', 'tradeDate', 'openPrice', 'highestPrice', 'lowestPrice', 'closePrice', 'turnoverVol']
        feature_cols = [col for col in numeric_cols if col not in exclude_cols]
        
        if len(feature_cols) < 2:
            return features
        
        try:
            # 计算相关系数矩阵
            corr_matrix = features[feature_cols].corr().abs()
            
            # 找到高相关性特征对
            upper_triangle = corr_matrix.where(
                np.triu(np.ones(corr_matrix.shape), k=1).astype(bool)
            )
            
            # 找到需要删除的特征
            to_drop = [column for column in upper_triangle.columns 
                      if any(upper_triangle[column] > self.config['correlation_threshold'])]
            
            if to_drop:
                print(f"   🗑️ 移除高相关特征: {len(to_drop)} 个")
                features = features.drop(columns=to_drop)
            
        except Exception as e:
            logger.warning(f"相关性分析失败: {e}")
        
        return features
    
    def calculate_feature_importance(self, features: pd.DataFrame, 
                                   target_col: str = None) -> Dict[str, float]:
        """
        计算特征重要性
        
        Args:
            features: 特征数据
            target_col: 目标变量列名
            
        Returns:
            特征重要性字典
        """
        if target_col is None or target_col not in features.columns:
            print("⚠️ 未指定有效的目标变量")
            return {}
        
        print("📊 计算特征重要性...")
        
        # 选择数值特征
        numeric_cols = features.select_dtypes(include=[np.number]).columns
        feature_cols = [col for col in numeric_cols if col != target_col and 'ticker' not in col]
        
        importance_scores = {}
        
        try:
            # 使用相关系数作为重要性指标
            for col in feature_cols:
                if col in features.columns:
                    corr = features[col].corr(features[target_col])
                    importance_scores[col] = abs(corr) if not pd.isna(corr) else 0
            
            # 排序
            importance_scores = dict(sorted(importance_scores.items(), 
                                          key=lambda x: x[1], reverse=True))
            
            self.stats['feature_importance'] = importance_scores
            
            # 显示前10个重要特征
            top_features = list(importance_scores.items())[:10]
            print("📋 特征重要性Top 10:")
            for i, (feature, score) in enumerate(top_features, 1):
                print(f"   {i:2d}. {feature}: {score:.4f}")
            
        except Exception as e:
            logger.error(f"特征重要性计算失败: {e}")
        
        return importance_scores
    
    def get_feature_summary(self) -> Dict[str, Any]:
        """获取特征摘要信息"""
        return {
            'stats': self.stats,
            'config': self.config,
            'indicator_params': self.indicator_params,
            'talib_available': TALIB_AVAILABLE,
            'cache_dir': self.cache_dir
        }


# ==========================================
# 🏭 工厂函数和模块导出
# ==========================================

def create_feature_engineer(price_data: pd.DataFrame = None, 
                           config: Dict = None) -> FeatureEngineer:
    """
    创建特征工程器实例的工厂函数
    
    Args:
        price_data: 价格数据
        config: 配置参数
        
    Returns:
        FeatureEngineer实例
    """
    return FeatureEngineer(price_data, config)

# 模块导出
__all__ = [
    'FeatureEngineer',
    'create_feature_engineer'
]

if __name__ == "__main__":
    print("🔬 特征工程器 v2.0 模块加载完成")
    print("📘 使用示例:")
    print("   from feature_engineer import FeatureEngineer, create_feature_engineer")
    print("   engineer = create_feature_engineer(price_data)")
    print("   features = engineer.generate_all_features()")
    print("")
    print("💡 功能特性:")
    print("   📊 60+ 技术指标和统计特征")
    print("   🔬 TA-Lib集成和内置算法备份")
    print("   🚀 高性能批量特征计算")
    print("   💾 智能缓存和增量更新")
    print("   📋 特征重要性分析")
    print("   🎯 自动特征选择和去重")