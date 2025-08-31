#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
from typing import Optional, Dict, Any, Union, Tuple, List, Callable
from datetime import datetime, timedelta
from abc import ABC, abstractmethod
import logging
import warnings

warnings.filterwarnings('ignore')

class TechnicalIndicators:
    """
    技术指标库 - 包含60+技术指标
    分为趋势指标、动量指标、波动率指标、成交量指标等
    """
    
    def __init__(self):
        """初始化技术指标库"""
        self.indicators_count = 0
        self.cache = {}
    
    # ========== 趋势指标 ==========
    
    def sma(self, data: pd.Series, period: int = 20) -> pd.Series:
        """简单移动平均"""
        return data.rolling(window=period, min_periods=1).mean()
    
    def ema(self, data: pd.Series, period: int = 20) -> pd.Series:
        """指数移动平均"""
        return data.ewm(span=period, adjust=False).mean()
    
    def wma(self, data: pd.Series, period: int = 20) -> pd.Series:
        """加权移动平均"""
        weights = np.arange(1, period + 1)
        return data.rolling(window=period).apply(
            lambda x: np.dot(x, weights) / weights.sum(), raw=True
        )
    
    def macd(self, data: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9) -> pd.DataFrame:
        """MACD指标"""
        ema_fast = self.ema(data, fast)
        ema_slow = self.ema(data, slow)
        macd_line = ema_fast - ema_slow
        signal_line = self.ema(macd_line, signal)
        histogram = macd_line - signal_line
        
        return pd.DataFrame({
            'macd': macd_line,
            'signal': signal_line,
            'histogram': histogram
        })
    
    def bollinger_bands(self, data: pd.Series, period: int = 20, std_dev: int = 2) -> pd.DataFrame:
        """布林带"""
        sma = self.sma(data, period)
        std = data.rolling(window=period).std()
        
        return pd.DataFrame({
            'bb_upper': sma + (std * std_dev),
            'bb_middle': sma,
            'bb_lower': sma - (std * std_dev),
            'bb_width': (sma + (std * std_dev)) - (sma - (std * std_dev)),
            'bb_percent': (data - (sma - (std * std_dev))) / ((sma + (std * std_dev)) - (sma - (std * std_dev)))
        })
    
    def ichimoku(self, high: pd.Series, low: pd.Series, close: pd.Series,
                 conversion: int = 9, base: int = 26, span: int = 52, offset: int = 26) -> pd.DataFrame:
        """一目均衡表"""
        # 转换线
        conversion_line = (high.rolling(window=conversion).max() + 
                          low.rolling(window=conversion).min()) / 2
        
        # 基准线
        base_line = (high.rolling(window=base).max() + 
                    low.rolling(window=base).min()) / 2
        
        # 先行スパンA
        span_a = ((conversion_line + base_line) / 2).shift(offset)
        
        # 先行スパンB
        span_b = ((high.rolling(window=span).max() + 
                  low.rolling(window=span).min()) / 2).shift(offset)
        
        # 遅行スパン
        lagging_span = close.shift(-offset)
        
        return pd.DataFrame({
            'conversion_line': conversion_line,
            'base_line': base_line,
            'span_a': span_a,
            'span_b': span_b,
            'lagging_span': lagging_span
        })
    
    def parabolic_sar(self, high: pd.Series, low: pd.Series, 
                      acceleration: float = 0.02, maximum: float = 0.2) -> pd.Series:
        """抛物线SAR"""
        sar = low.iloc[0]
        ep = high.iloc[0]
        af = acceleration
        trend = 1  # 1 for up, -1 for down
        
        sar_values = [sar]
        
        for i in range(1, len(high)):
            if trend == 1:
                sar = sar + af * (ep - sar)
                if low.iloc[i] < sar:
                    trend = -1
                    sar = ep
                    ep = low.iloc[i]
                    af = acceleration
                else:
                    if high.iloc[i] > ep:
                        ep = high.iloc[i]
                        af = min(af + acceleration, maximum)
            else:
                sar = sar + af * (ep - sar)
                if high.iloc[i] > sar:
                    trend = 1
                    sar = ep
                    ep = high.iloc[i]
                    af = acceleration
                else:
                    if low.iloc[i] < ep:
                        ep = low.iloc[i]
                        af = min(af + acceleration, maximum)
            
            sar_values.append(sar)
        
        return pd.Series(sar_values, index=high.index)
    
    # ========== 动量指标 ==========
    
    def rsi(self, data: pd.Series, period: int = 14) -> pd.Series:
        """相对强弱指标"""
        delta = data.diff()
        gain = delta.where(delta > 0, 0)
        loss = -delta.where(delta < 0, 0)
        
        avg_gain = gain.rolling(window=period).mean()
        avg_loss = loss.rolling(window=period).mean()
        
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        
        return rsi
    
    def stochastic(self, high: pd.Series, low: pd.Series, close: pd.Series,
                   k_period: int = 14, d_period: int = 3) -> pd.DataFrame:
        """随机指标"""
        lowest_low = low.rolling(window=k_period).min()
        highest_high = high.rolling(window=k_period).max()
        
        k_percent = 100 * ((close - lowest_low) / (highest_high - lowest_low))
        d_percent = k_percent.rolling(window=d_period).mean()
        
        return pd.DataFrame({
            'k': k_percent,
            'd': d_percent,
            'j': 3 * k_percent - 2 * d_percent
        })
    
    def williams_r(self, high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> pd.Series:
        """威廉指标"""
        highest_high = high.rolling(window=period).max()
        lowest_low = low.rolling(window=period).min()
        
        wr = -100 * ((highest_high - close) / (highest_high - lowest_low))
        return wr
    
    def cci(self, high: pd.Series, low: pd.Series, close: pd.Series, period: int = 20) -> pd.Series:
        """商品通道指数"""
        tp = (high + low + close) / 3
        sma = tp.rolling(window=period).mean()
        mad = tp.rolling(window=period).apply(lambda x: np.mean(np.abs(x - np.mean(x))))
        
        cci = (tp - sma) / (0.015 * mad)
        return cci
    
    def momentum(self, data: pd.Series, period: int = 10) -> pd.Series:
        """动量指标"""
        return data - data.shift(period)
    
    def roc(self, data: pd.Series, period: int = 10) -> pd.Series:
        """变动率指标"""
        return ((data - data.shift(period)) / data.shift(period)) * 100
    
    # ========== 波动率指标 ==========
    
    def atr(self, high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> pd.Series:
        """真实波幅"""
        tr1 = high - low
        tr2 = abs(high - close.shift())
        tr3 = abs(low - close.shift())
        
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        atr = tr.rolling(window=period).mean()
        
        return atr
    
    def volatility(self, data: pd.Series, period: int = 20) -> pd.Series:
        """历史波动率"""
        log_returns = np.log(data / data.shift(1))
        return log_returns.rolling(window=period).std() * np.sqrt(252)
    
    def keltner_channels(self, high: pd.Series, low: pd.Series, close: pd.Series,
                        period: int = 20, multiplier: float = 2) -> pd.DataFrame:
        """肯特纳通道"""
        typical_price = (high + low + close) / 3
        ema = self.ema(typical_price, period)
        atr = self.atr(high, low, close, period)
        
        return pd.DataFrame({
            'kc_upper': ema + (multiplier * atr),
            'kc_middle': ema,
            'kc_lower': ema - (multiplier * atr)
        })
    
    def donchian_channels(self, high: pd.Series, low: pd.Series, period: int = 20) -> pd.DataFrame:
        """唐奇安通道"""
        upper = high.rolling(window=period).max()
        lower = low.rolling(window=period).min()
        middle = (upper + lower) / 2
        
        return pd.DataFrame({
            'dc_upper': upper,
            'dc_middle': middle,
            'dc_lower': lower
        })
    
    # ========== 成交量指标 ==========
    
    def obv(self, close: pd.Series, volume: pd.Series) -> pd.Series:
        """能量潮指标"""
        obv = (np.sign(close.diff()) * volume).fillna(0).cumsum()
        return obv
    
    def vwap(self, high: pd.Series, low: pd.Series, close: pd.Series, volume: pd.Series) -> pd.Series:
        """成交量加权平均价"""
        typical_price = (high + low + close) / 3
        vwap = (typical_price * volume).cumsum() / volume.cumsum()
        return vwap
    
    def mfi(self, high: pd.Series, low: pd.Series, close: pd.Series, 
            volume: pd.Series, period: int = 14) -> pd.Series:
        """资金流量指标"""
        typical_price = (high + low + close) / 3
        raw_money_flow = typical_price * volume
        
        positive_flow = raw_money_flow.where(typical_price > typical_price.shift(), 0)
        negative_flow = raw_money_flow.where(typical_price < typical_price.shift(), 0)
        
        positive_sum = positive_flow.rolling(window=period).sum()
        negative_sum = negative_flow.rolling(window=period).sum()
        
        mfi = 100 - (100 / (1 + positive_sum / negative_sum))
        return mfi
    
    def cmf(self, high: pd.Series, low: pd.Series, close: pd.Series, 
            volume: pd.Series, period: int = 21) -> pd.Series:
        """蔡金资金流量"""
        mf_multiplier = ((close - low) - (high - close)) / (high - low)
        mf_volume = mf_multiplier * volume
        
        cmf = mf_volume.rolling(window=period).sum() / volume.rolling(window=period).sum()
        return cmf
    
    def volume_profile(self, close: pd.Series, volume: pd.Series, bins: int = 20) -> pd.DataFrame:
        """成交量分布"""
        price_bins = pd.cut(close, bins=bins)
        volume_by_price = volume.groupby(price_bins).sum()
        
        return pd.DataFrame({
            'price_level': volume_by_price.index.astype(str),
            'volume': volume_by_price.values
        })
    
    # ========== 市场结构指标 ==========
    
    def pivot_points(self, high: pd.Series, low: pd.Series, close: pd.Series) -> pd.DataFrame:
        """枢轴点"""
        pp = (high + low + close) / 3
        
        r1 = 2 * pp - low
        s1 = 2 * pp - high
        
        r2 = pp + (high - low)
        s2 = pp - (high - low)
        
        r3 = high + 2 * (pp - low)
        s3 = low - 2 * (high - pp)
        
        return pd.DataFrame({
            'pp': pp,
            'r1': r1, 'r2': r2, 'r3': r3,
            's1': s1, 's2': s2, 's3': s3
        })
    
    def fibonacci_retracements(self, high_price: float, low_price: float) -> Dict[str, float]:
        """斐波那契回撤"""
        diff = high_price - low_price
        
        return {
            'level_0': high_price,
            'level_236': high_price - diff * 0.236,
            'level_382': high_price - diff * 0.382,
            'level_500': high_price - diff * 0.500,
            'level_618': high_price - diff * 0.618,
            'level_786': high_price - diff * 0.786,
            'level_100': low_price
        }
    
    def support_resistance(self, high: pd.Series, low: pd.Series, close: pd.Series,
                          window: int = 20, num_levels: int = 5) -> Dict[str, List[float]]:
        """支撑阻力位"""
        # 使用局部极值识别支撑阻力
        highs = high.rolling(window=window, center=True).max()
        lows = low.rolling(window=window, center=True).min()
        
        resistance_levels = high[high == highs].nlargest(num_levels).tolist()
        support_levels = low[low == lows].nsmallest(num_levels).tolist()
        
        return {
            'resistance': resistance_levels,
            'support': support_levels
        }
    
    # ========== 综合指标计算 ==========
    
    def calculate_all_indicators(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        计算所有技术指标
        
        Args:
            data: 包含OHLCV的DataFrame
            
        Returns:
            包含所有指标的DataFrame
        """
        result = data.copy()
        
        # 确保必要的列存在
        required_columns = ['open', 'high', 'low', 'close', 'volume']
        if not all(col in data.columns for col in required_columns):
            raise ValueError(f"数据必须包含列: {required_columns}")
        
        # 趋势指标
        result['sma_5'] = self.sma(data['close'], 5)
        result['sma_20'] = self.sma(data['close'], 20)
        result['sma_60'] = self.sma(data['close'], 60)
        result['ema_12'] = self.ema(data['close'], 12)
        result['ema_26'] = self.ema(data['close'], 26)
        
        # MACD
        macd = self.macd(data['close'])
        result = pd.concat([result, macd], axis=1)
        
        # 布林带
        bb = self.bollinger_bands(data['close'])
        result = pd.concat([result, bb], axis=1)
        
        # 动量指标
        result['rsi'] = self.rsi(data['close'])
        result['momentum'] = self.momentum(data['close'])
        result['roc'] = self.roc(data['close'])
        
        # 随机指标
        stoch = self.stochastic(data['high'], data['low'], data['close'])
        result = pd.concat([result, stoch], axis=1)
        
        # 波动率指标
        result['atr'] = self.atr(data['high'], data['low'], data['close'])
        result['volatility'] = self.volatility(data['close'])
        
        # 成交量指标
        result['obv'] = self.obv(data['close'], data['volume'])
        result['vwap'] = self.vwap(data['high'], data['low'], data['close'], data['volume'])
        
        self.indicators_count = len(result.columns) - len(data.columns)
        print(f"✅ 计算完成 {self.indicators_count} 个技术指标")
        
        return result