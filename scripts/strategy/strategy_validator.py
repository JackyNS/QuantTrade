#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
策略验证脚本
============

验证策略的有效性和稳定性

Author: QuantTrader Team
Date: 2025-08-31
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import pandas as pd
import numpy as np
from typing import Dict, List
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class StrategyValidator:
    """策略验证器"""
    
    def __init__(self):
        self.validation_results = {}
        
    def validate_signals(self, signals: pd.DataFrame) -> Dict:
        """验证交易信号"""
        results = {
            'total_signals': len(signals),
            'buy_signals': len(signals[signals['signal'] == 1]),
            'sell_signals': len(signals[signals['signal'] == -1]),
            'signal_frequency': len(signals) / signals['date'].nunique() if 'date' in signals.columns else 0,
            'valid': True,
            'issues': []
        }
        
        # 检查信号平衡
        if results['buy_signals'] == 0:
            results['issues'].append("无买入信号")
            results['valid'] = False
            
        if results['sell_signals'] == 0:
            results['issues'].append("无卖出信号")
            results['valid'] = False
            
        # 检查信号频率
        if results['signal_frequency'] > 10:
            results['issues'].append("信号过于频繁")
            
        return results
    
    def validate_performance(self, returns: pd.Series) -> Dict:
        """验证策略表现"""
        results = {
            'total_return': returns.sum(),
            'annual_return': returns.mean() * 252,
            'volatility': returns.std() * np.sqrt(252),
            'sharpe_ratio': (returns.mean() * 252) / (returns.std() * np.sqrt(252)),
            'max_drawdown': self._calculate_max_drawdown(returns),
            'win_rate': len(returns[returns > 0]) / len(returns),
            'valid': True
        }
        
        # 检查异常表现
        if results['sharpe_ratio'] > 3:
            results['warning'] = "夏普比率异常高，可能过拟合"
            
        if results['max_drawdown'] > 0.5:
            results['warning'] = "最大回撤过大"
            
        return results
    
    def _calculate_max_drawdown(self, returns: pd.Series) -> float:
        """计算最大回撤"""
        cumulative = (1 + returns).cumprod()
        running_max = cumulative.cummax()
        drawdown = (cumulative - running_max) / running_max
        return drawdown.min()

def main():
    validator = StrategyValidator()
    
    # 这里可以加载实际的信号数据进行验证
    logger.info("策略验证工具已准备就绪")

if __name__ == "__main__":
    main()