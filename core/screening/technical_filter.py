#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
技术面筛选器 - Technical Filter
================================

专注于技术指标和价格行为的筛选

作者: QuantTrader Team
版本: 1.0.0
更新: 2025-08-30
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
import logging

class TechnicalFilter:
    """
    技术面筛选器
    
    筛选维度：
    1. 价格趋势：均线、通道、趋势强度
    2. 动量指标：RSI、MACD、KDJ、动量
    3. 成交量：量价关系、量能指标
    4. 波动性：ATR、波动率、布林带
    5. 形态识别：K线形态、图表形态
    6. 市场强度：相对强度、板块比较
    """
    
    def __init__(self, config: Optional[Dict] = None):
        """初始化技术面筛选器"""
        self.config = config or self._get_default_config()
        self.logger = self._setup_logger()
        self.filter_results = {}
        
    def _get_default_config(self) -> Dict:
        """默认配置"""
        return {
            # 价格趋势
            'trend': {
                'ma_alignment': 'bullish',  # bullish/bearish/neutral
                'price_vs_ma20': 'above',   # above/below
                'trend_strength': {'min': 0.3, 'max': None},
                'trend_days': {'min': 5, 'max': None},
            },
            
            # 动量指标
            'momentum': {
                'rsi': {'min': 30, 'max': 70},
                'rsi_trend': 'rising',  # rising/falling/neutral
                'macd_signal': 'bullish',  # bullish/bearish/neutral
                'kdj_signal': 'golden_cross',  # golden_cross/death_cross/neutral
                'momentum': {'min': 0, 'max': None},
            },
            
            # 成交量
            'volume': {
                'volume_ma_ratio': {'min': 1.0, 'max': 3.0},
                'volume_trend': 'increasing',  # increasing/decreasing/stable
                'obv_trend': 'rising',  # rising/falling/neutral
                'volume_price_confirm': True,  # 量价配合
            },
            
            # 波动性
            'volatility': {
                'atr': {'min': None, 'max': 2.0},
                'volatility': {'min': 0.10, 'max': 0.40},
                'bollinger_position': 'middle',  # upper/middle/lower
                'vix_level': {'min': None, 'max': 30},
            },
            
            # 形态识别
            'pattern': {
                'candlestick_patterns': ['hammer', 'doji', 'engulfing'],
                'chart_patterns': ['triangle', 'flag', 'wedge'],
                'support_resistance': True,
            },
            
            # 市场强度
            'strength': {
                'relative_strength': {'min': 50, 'max': None},
                'sector_rank': {'min': None, 'max': 10},
                'market_cap_rank': {'min': None, 'max': 100},
            }
        }
    
    def filter_trend(self, data: pd.DataFrame) -> pd.DataFrame:
        """趋势筛选"""
        self.logger.info("📈 趋势筛选...")
        filtered = data.copy()
        criteria = self.config['trend']
        
        # 均线排列
        if criteria['ma_alignment'] == 'bullish':
            if all(col in filtered.columns for col in ['ma5', 'ma20', 'ma60']):
                filtered = filtered[
                    (filtered['ma5'] > filtered['ma20']) &
                    (filtered['ma20'] > filtered['ma60'])
                ]
        elif criteria['ma_alignment'] == 'bearish':
            if all(col in filtered.columns for col in ['ma5', 'ma20', 'ma60']):
                filtered = filtered[
                    (filtered['ma5'] < filtered['ma20']) &
                    (filtered['ma20'] < filtered['ma60'])
                ]
        
        # 价格vs均线
        if criteria['price_vs_ma20'] == 'above' and 'ma20' in filtered.columns:
            filtered = filtered[filtered['close'] > filtered['ma20']]
        elif criteria['price_vs_ma20'] == 'below' and 'ma20' in filtered.columns:
            filtered = filtered[filtered['close'] < filtered['ma20']]
        
        # 趋势强度
        if 'trend_strength' in filtered.columns:
            bounds = criteria['trend_strength']
            if bounds['min'] is not None:
                filtered = filtered[filtered['trend_strength'] >= bounds['min']]
            if bounds['max'] is not None:
                filtered = filtered[filtered['trend_strength'] <= bounds['max']]
        
        self.filter_results['trend'] = len(filtered)
        return filtered
    
    def filter_momentum(self, data: pd.DataFrame) -> pd.DataFrame:
        """动量筛选"""
        self.logger.info("⚡ 动量筛选...")
        filtered = data.copy()
        criteria = self.config['momentum']
        
        # RSI筛选
        if 'rsi' in filtered.columns:
            bounds = criteria['rsi']
            if bounds['min'] is not None:
                filtered = filtered[filtered['rsi'] >= bounds['min']]
            if bounds['max'] is not None:
                filtered = filtered[filtered['rsi'] <= bounds['max']]
        
        # RSI趋势
        if criteria['rsi_trend'] == 'rising' and 'rsi_change' in filtered.columns:
            filtered = filtered[filtered['rsi_change'] > 0]
        elif criteria['rsi_trend'] == 'falling' and 'rsi_change' in filtered.columns:
            filtered = filtered[filtered['rsi_change'] < 0]
        
        # MACD信号
        if criteria['macd_signal'] == 'bullish':
            if all(col in filtered.columns for col in ['dif', 'dea']):
                filtered = filtered[filtered['dif'] > filtered['dea']]
        elif criteria['macd_signal'] == 'bearish':
            if all(col in filtered.columns for col in ['dif', 'dea']):
                filtered = filtered[filtered['dif'] < filtered['dea']]
        
        self.filter_results['momentum'] = len(filtered)
        return filtered
    
    def filter_volume(self, data: pd.DataFrame) -> pd.DataFrame:
        """成交量筛选"""
        self.logger.info("📊 成交量筛选...")
        filtered = data.copy()
        criteria = self.config['volume']
        
        # 量比筛选
        if 'volume_ratio' in filtered.columns:
            bounds = criteria['volume_ma_ratio']
            if bounds['min'] is not None:
                filtered = filtered[filtered['volume_ratio'] >= bounds['min']]
            if bounds['max'] is not None:
                filtered = filtered[filtered['volume_ratio'] <= bounds['max']]
        
        # 成交量趋势
        if criteria['volume_trend'] == 'increasing':
            if 'volume_ma5' in filtered.columns and 'volume_ma20' in filtered.columns:
                filtered = filtered[filtered['volume_ma5'] > filtered['volume_ma20']]
        
        # 量价配合
        if criteria['volume_price_confirm']:
            if all(col in filtered.columns for col in ['price_change', 'volume_change']):
                # 价涨量增或价跌量缩
                filtered = filtered[
                    ((filtered['price_change'] > 0) & (filtered['volume_change'] > 0)) |
                    ((filtered['price_change'] < 0) & (filtered['volume_change'] < 0))
                ]
        
        self.filter_results['volume'] = len(filtered)
        return filtered
    
    def calculate_technical_score(self, data: pd.DataFrame) -> pd.Series:
        """计算技术面分数"""
        score = pd.Series(0, index=data.index)
        
        # 趋势分数（30%）
        if all(col in data.columns for col in ['ma5', 'ma20', 'ma60']):
            trend_score = (
                (data['ma5'] > data['ma20']).astype(float) * 0.5 +
                (data['ma20'] > data['ma60']).astype(float) * 0.5
            )
            score += trend_score * 0.30
        
        # 动量分数（30%）
        if 'rsi' in data.columns:
            # RSI在40-60之间最佳
            rsi_score = 1 - abs(data['rsi'] - 50) / 50
            score += rsi_score * 0.30
        
        # 成交量分数（20%）
        if 'volume_ratio' in data.columns:
            # 量比在1-2之间最佳
            vol_score = np.clip(2 - abs(data['volume_ratio'] - 1.5), 0, 1)
            score += vol_score * 0.20
        
        # 波动性分数（20%）
        if 'volatility' in data.columns:
            # 适度波动最佳（15%-25%）
            vol_score = 1 - abs(data['volatility'] - 0.20) / 0.20
            vol_score = np.clip(vol_score, 0, 1)
            score += vol_score * 0.20
        
        return score
    
    def identify_patterns(self, data: pd.DataFrame) -> pd.DataFrame:
        """识别技术形态"""
        data = data.copy()
        
        # K线形态识别
        if all(col in data.columns for col in ['open', 'high', 'low', 'close']):
            # 锤子线
            body = abs(data['close'] - data['open'])
            lower_shadow = data[['open', 'close']].min(axis=1) - data['low']
            data['hammer'] = (lower_shadow > 2 * body) & (body > 0)
            
            # 十字星
            data['doji'] = body / (data['high'] - data['low']) < 0.1
            
            # 吞没形态
            prev_body = body.shift(1)
            data['bullish_engulfing'] = (
                (data['close'] > data['open']) &  # 当前阳线
                (data['close'].shift(1) < data['open'].shift(1)) &  # 前一天阴线
                (body > prev_body)  # 实体更大
            )
        
        return data
    
    def filter(self, data: pd.DataFrame,
              dimensions: List[str] = None) -> pd.DataFrame:
        """
        执行技术面筛选
        
        Args:
            data: 股票数据
            dimensions: 筛选维度
            
        Returns:
            筛选后的数据
        """
        if dimensions is None:
            dimensions = ['trend', 'momentum', 'volume']
        
        result = data.copy()
        
        # 先识别形态
        result = self.identify_patterns(result)
        
        for dim in dimensions:
            if dim == 'trend':
                result = self.filter_trend(result)
            elif dim == 'momentum':
                result = self.filter_momentum(result)
            elif dim == 'volume':
                result = self.filter_volume(result)
            
            if result.empty:
                break
        
        # 计算技术面分数
        if not result.empty:
            result['technical_score'] = self.calculate_technical_score(result)
        
        return result
    
    def _setup_logger(self) -> logging.Logger:
        """设置日志"""
        logger = logging.getLogger('TechnicalFilter')
        logger.setLevel(logging.INFO)
        return logger