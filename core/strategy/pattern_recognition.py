#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
K线形态识别模块
================

识别各种经典的K线形态和图表模式
"""

import pandas as pd
import numpy as np
from typing import Optional, Dict, Any, Union, Tuple, List
from datetime import datetime, timedelta
import warnings

warnings.filterwarnings('ignore')

class PatternRecognizer:
    """
    K线形态识别器
    识别各种经典的K线形态
    """
    
    def __init__(self):
        """初始化形态识别器"""
        self.patterns = {}
        
    def detect_all_patterns(self, ohlc_data: pd.DataFrame) -> Dict[str, pd.Series]:
        """
        检测所有K线形态
        
        Args:
            ohlc_data: OHLC数据，需要包含open, high, low, close列
            
        Returns:
            形态识别结果字典
        """
        results = {}
        
        # 单K线形态
        results['doji'] = self.detect_doji(ohlc_data)
        results['hammer'] = self.detect_hammer(ohlc_data)
        results['shooting_star'] = self.detect_shooting_star(ohlc_data)
        
        # 双K线形态
        results['engulfing'] = self.detect_engulfing(ohlc_data)
        results['harami'] = self.detect_harami(ohlc_data)
        
        # 三K线形态
        results['morning_star'] = self.detect_morning_star(ohlc_data)
        results['evening_star'] = self.detect_evening_star(ohlc_data)
        
        return results
    
    def detect_doji(self, ohlc_data: pd.DataFrame, threshold: float = 0.001) -> pd.Series:
        """
        十字星形态
        开盘价和收盘价几乎相等
        """
        body = abs(ohlc_data['close'] - ohlc_data['open'])
        high_low_range = ohlc_data['high'] - ohlc_data['low']
        
        doji = body <= (high_low_range * threshold)
        return doji.astype(int)
    
    def detect_hammer(self, ohlc_data: pd.DataFrame) -> pd.Series:
        """
        锤子线形态
        下影线长，实体小，上影线短或无
        """
        body = abs(ohlc_data['close'] - ohlc_data['open'])
        lower_shadow = np.minimum(ohlc_data['open'], ohlc_data['close']) - ohlc_data['low']
        upper_shadow = ohlc_data['high'] - np.maximum(ohlc_data['open'], ohlc_data['close'])
        
        hammer = (lower_shadow > 2 * body) & (upper_shadow < 0.3 * body)
        return hammer.astype(int)
    
    def detect_shooting_star(self, ohlc_data: pd.DataFrame) -> pd.Series:
        """
        流星线形态
        上影线长，实体小，下影线短或无
        """
        body = abs(ohlc_data['close'] - ohlc_data['open'])
        upper_shadow = ohlc_data['high'] - np.maximum(ohlc_data['open'], ohlc_data['close'])
        lower_shadow = np.minimum(ohlc_data['open'], ohlc_data['close']) - ohlc_data['low']
        
        shooting_star = (upper_shadow > 2 * body) & (lower_shadow < 0.3 * body)
        return shooting_star.astype(int)
    
    def detect_engulfing(self, ohlc_data: pd.DataFrame) -> pd.Series:
        """
        吞没形态
        第二根K线完全吞没第一根K线
        """
        prev_body = ohlc_data['close'].shift(1) - ohlc_data['open'].shift(1)
        curr_body = ohlc_data['close'] - ohlc_data['open']
        
        bullish_engulfing = (prev_body < 0) & (curr_body > 0) &                            (ohlc_data['open'] < ohlc_data['close'].shift(1)) &                            (ohlc_data['close'] > ohlc_data['open'].shift(1))
        
        bearish_engulfing = (prev_body > 0) & (curr_body < 0) &                            (ohlc_data['open'] > ohlc_data['close'].shift(1)) &                            (ohlc_data['close'] < ohlc_data['open'].shift(1))
        
        return (bullish_engulfing | bearish_engulfing).astype(int)
    
    def detect_harami(self, ohlc_data: pd.DataFrame) -> pd.Series:
        """
        孕线形态
        第二根K线被第一根K线包含
        """
        prev_body = abs(ohlc_data['close'].shift(1) - ohlc_data['open'].shift(1))
        curr_body = abs(ohlc_data['close'] - ohlc_data['open'])
        
        harami = (curr_body < prev_body) &                  (ohlc_data['open'] > np.minimum(ohlc_data['open'].shift(1), ohlc_data['close'].shift(1))) &                  (ohlc_data['close'] < np.maximum(ohlc_data['open'].shift(1), ohlc_data['close'].shift(1)))
        
        return harami.astype(int)
    
    def detect_morning_star(self, ohlc_data: pd.DataFrame) -> pd.Series:
        """
        启明星形态
        三根K线组成的底部反转形态
        """
        # 第一根：长阴线
        first_bearish = (ohlc_data['close'].shift(2) < ohlc_data['open'].shift(2)) &                        (abs(ohlc_data['close'].shift(2) - ohlc_data['open'].shift(2)) >                         ohlc_data['close'].shift(2) * 0.01)
        
        # 第二根：小实体
        second_small = abs(ohlc_data['close'].shift(1) - ohlc_data['open'].shift(1)) <                       abs(ohlc_data['close'].shift(2) - ohlc_data['open'].shift(2)) * 0.3
        
        # 第三根：长阳线
        third_bullish = (ohlc_data['close'] > ohlc_data['open']) &                        (abs(ohlc_data['close'] - ohlc_data['open']) >                         ohlc_data['close'] * 0.01)
        
        morning_star = first_bearish & second_small & third_bullish
        return morning_star.astype(int)
    
    def detect_evening_star(self, ohlc_data: pd.DataFrame) -> pd.Series:
        """
        黄昏星形态
        三根K线组成的顶部反转形态
        """
        # 第一根：长阳线
        first_bullish = (ohlc_data['close'].shift(2) > ohlc_data['open'].shift(2)) &                        (abs(ohlc_data['close'].shift(2) - ohlc_data['open'].shift(2)) >                         ohlc_data['close'].shift(2) * 0.01)
        
        # 第二根：小实体
        second_small = abs(ohlc_data['close'].shift(1) - ohlc_data['open'].shift(1)) <                       abs(ohlc_data['close'].shift(2) - ohlc_data['open'].shift(2)) * 0.3
        
        # 第三根：长阴线
        third_bearish = (ohlc_data['close'] < ohlc_data['open']) &                        (abs(ohlc_data['close'] - ohlc_data['open']) >                         ohlc_data['close'] * 0.01)
        
        evening_star = first_bullish & second_small & third_bearish
        return evening_star.astype(int)
