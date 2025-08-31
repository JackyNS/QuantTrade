#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
仓位优化脚本
============

优化资金分配和仓位管理

Author: QuantTrader Team
Date: 2025-08-31
"""

from typing import Dict, List

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import pandas as pd
import numpy as np
from scipy.optimize import minimize_scalar
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AllocationOptimizer:
    """仓位分配优化器"""
    
    def __init__(self, capital: float = 1000000):
        self.capital = capital
        self.max_position_size = 0.1  # 单个股票最大仓位
        self.min_position_size = 0.01  # 最小仓位
        
    def kelly_criterion(self, win_prob: float, win_return: float, loss_return: float) -> float:
        """凯利公式计算最优仓位"""
        if loss_return >= 0:
            return 0
        
        b = win_return / abs(loss_return)
        p = win_prob
        q = 1 - p
        
        kelly = (b * p - q) / b
        
        # 限制最大仓位
        kelly = max(0, min(kelly, self.max_position_size))
        
        return kelly
    
    def optimize_position_sizes(self, signals: pd.DataFrame) -> pd.DataFrame:
        """优化每个信号的仓位大小"""
        
        signals['optimal_position'] = 0.0
        
        for idx, row in signals.iterrows():
            # 基于信号强度和历史胜率计算仓位
            signal_strength = row.get('signal_strength', 1.0)
            expected_return = row.get('expected_return', 0.05)
            risk = row.get('risk', 0.02)
            
            # 使用凯利公式的保守版本（1/4 Kelly）
            win_prob = row.get('win_probability', 0.55)
            kelly_fraction = self.kelly_criterion(win_prob, expected_return, -risk)
            
            # 调整仓位
            position = kelly_fraction * signal_strength * 0.25  # 1/4 Kelly
            position = max(self.min_position_size, min(position, self.max_position_size))
            
            signals.loc[idx, 'optimal_position'] = position
        
        # 确保总仓位不超过100%
        total_position = signals['optimal_position'].sum()
        if total_position > 1.0:
            signals['optimal_position'] = signals['optimal_position'] / total_position
        
        # 计算实际股数
        signals['shares'] = (signals['optimal_position'] * self.capital / signals['price']).astype(int)
        signals['shares'] = (signals['shares'] // 100) * 100  # 调整为100的整数倍
        
        return signals
    
    def risk_budget_allocation(self, assets: List, risk_budget: Dict = None) -> Dict:
        """风险预算分配"""
        
        if risk_budget is None:
            # 默认等风险分配
            risk_budget = {asset: 1/len(assets) for asset in assets}
        
        allocations = {}
        total_risk = sum(risk_budget.values())
        
        for asset in assets:
            allocations[asset] = risk_budget.get(asset, 0) / total_risk
        
        return allocations
    
    def dynamic_rebalancing(self, current_positions: Dict, target_weights: Dict, threshold: float = 0.05):
        """动态再平衡"""
        
        rebalance_orders = []
        
        # 计算当前总值
        total_value = sum(current_positions.values())
        
        for asset, target_weight in target_weights.items():
            current_value = current_positions.get(asset, 0)
            current_weight = current_value / total_value if total_value > 0 else 0
            
            # 检查是否需要再平衡
            weight_diff = target_weight - current_weight
            
            if abs(weight_diff) > threshold:
                # 计算交易金额
                trade_value = weight_diff * total_value
                
                rebalance_orders.append({
                    'asset': asset,
                    'action': 'buy' if trade_value > 0 else 'sell',
                    'amount': abs(trade_value),
                    'current_weight': current_weight,
                    'target_weight': target_weight
                })
        
        return rebalance_orders

def main():
    optimizer = AllocationOptimizer()
    
    # 示例：优化仓位
    sample_signals = pd.DataFrame({
        'symbol': ['000001', '000002', '000858'],
        'signal_strength': [0.8, 0.6, 0.9],
        'expected_return': [0.08, 0.06, 0.10],
        'risk': [0.02, 0.03, 0.025],
        'win_probability': [0.60, 0.55, 0.65],
        'price': [10.5, 25.3, 8.8]
    })
    
    optimized = optimizer.optimize_position_sizes(sample_signals)
    print("优化后的仓位分配:")
    print(optimized[['symbol', 'optimal_position', 'shares']])

if __name__ == "__main__":
    main()