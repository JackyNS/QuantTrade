#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
市场分析脚本
============

分析整体市场状况

Author: QuantTrader Team
Date: 2025-08-31
"""

from typing import Dict

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MarketAnalyzer:
    """市场分析器"""
    
    def __init__(self, data_manager):
        self.data_manager = data_manager
        
    def analyze_market_breadth(self, date: str = None) -> Dict:
        """分析市场广度"""
        if not date:
            date = datetime.now().strftime('%Y-%m-%d')
            
        # 获取所有股票当日数据
        market_data = self.data_manager.get_market_snapshot(date)
        
        if market_data.empty:
            return {}
        
        # 计算市场广度指标
        total_stocks = len(market_data)
        advancing = len(market_data[market_data['change'] > 0])
        declining = len(market_data[market_data['change'] < 0])
        unchanged = len(market_data[market_data['change'] == 0])
        
        # 涨停跌停
        limit_up = len(market_data[market_data['change'] >= 0.095])
        limit_down = len(market_data[market_data['change'] <= -0.095])
        
        return {
            'date': date,
            'total': total_stocks,
            'advancing': advancing,
            'declining': declining,
            'unchanged': unchanged,
            'advance_decline_ratio': advancing / declining if declining > 0 else np.inf,
            'limit_up': limit_up,
            'limit_down': limit_down,
            'market_sentiment': self._calculate_sentiment(advancing, declining)
        }
    
    def _calculate_sentiment(self, advancing: int, declining: int) -> str:
        """计算市场情绪"""
        ratio = advancing / (advancing + declining) if (advancing + declining) > 0 else 0.5
        
        if ratio > 0.7:
            return "强势"
        elif ratio > 0.55:
            return "偏强"
        elif ratio > 0.45:
            return "平衡"
        elif ratio > 0.3:
            return "偏弱"
        else:
            return "弱势"
    
    def analyze_sector_rotation(self, period: int = 20) -> pd.DataFrame:
        """分析板块轮动"""
        end_date = datetime.now()
        start_date = end_date - timedelta(days=period)
        
        # 获取各板块数据
        sectors = ['金融', '科技', '消费', '医药', '工业', '材料', '能源', '公用事业']
        
        sector_performance = []
        
        for sector in sectors:
            # 这里应该从实际数据源获取板块指数
            # 现在用示例数据
            performance = {
                'sector': sector,
                'return': np.random.randn() * 0.1,
                'volume_change': np.random.randn() * 0.2,
                'strength': np.random.rand()
            }
            sector_performance.append(performance)
        
        return pd.DataFrame(sector_performance).sort_values('return', ascending=False)
    
    def calculate_market_indicators(self) -> Dict:
        """计算市场技术指标"""
        # 获取指数数据（以沪深300为例）
        index_data = self.data_manager.get_index_data('000300')
        
        if index_data.empty:
            return {}
        
        # 计算技术指标
        indicators = {
            'ma20': index_data['close'].rolling(20).mean().iloc[-1],
            'ma60': index_data['close'].rolling(60).mean().iloc[-1],
            'rsi': self._calculate_rsi(index_data['close']),
            'volume_ratio': index_data['volume'].iloc[-1] / index_data['volume'].rolling(20).mean().iloc[-1]
        }
        
        # 判断趋势
        current_price = index_data['close'].iloc[-1]
        if current_price > indicators['ma20'] > indicators['ma60']:
            indicators['trend'] = "上升趋势"
        elif current_price < indicators['ma20'] < indicators['ma60']:
            indicators['trend'] = "下降趋势"
        else:
            indicators['trend'] = "震荡整理"
        
        return indicators
    
    def _calculate_rsi(self, prices: pd.Series, period: int = 14) -> float:
        """计算RSI"""
        delta = prices.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        
        return rsi.iloc[-1]

def main():
    logger.info("市场分析工具已准备就绪")

if __name__ == "__main__":
    main()