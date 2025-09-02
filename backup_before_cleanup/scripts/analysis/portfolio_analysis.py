#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
组合分析脚本
============

分析投资组合的风险收益特征

Author: QuantTrader Team
Date: 2025-08-31
"""

from typing import Dict

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import pandas as pd
import numpy as np
from scipy.optimize import minimize
import matplotlib.pyplot as plt
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PortfolioAnalyzer:
    """组合分析器"""
    
    def __init__(self, returns_data: pd.DataFrame):
        """
        Args:
            returns_data: 各资产收益率数据，columns为资产代码
        """
        self.returns = returns_data
        self.n_assets = len(returns_data.columns)
        self.assets = returns_data.columns.tolist()
        
    def calculate_portfolio_metrics(self, weights: np.ndarray) -> Dict:
        """计算组合指标"""
        # 组合收益
        portfolio_return = np.sum(self.returns.mean() * weights) * 252
        
        # 组合风险
        cov_matrix = self.returns.cov() * 252
        portfolio_std = np.sqrt(np.dot(weights.T, np.dot(cov_matrix, weights)))
        
        # 夏普比率（假设无风险收益率为3%）
        risk_free_rate = 0.03
        sharpe_ratio = (portfolio_return - risk_free_rate) / portfolio_std
        
        return {
            'return': portfolio_return,
            'risk': portfolio_std,
            'sharpe': sharpe_ratio
        }
    
    def optimize_portfolio(self, target: str = 'sharpe') -> Dict:
        """优化组合权重"""
        
        if target == 'sharpe':
            # 最大化夏普比率
            def neg_sharpe(weights):
                metrics = self.calculate_portfolio_metrics(weights)
                return -metrics['sharpe']
            
            objective = neg_sharpe
            
        elif target == 'min_risk':
            # 最小化风险
            def portfolio_risk(weights):
                metrics = self.calculate_portfolio_metrics(weights)
                return metrics['risk']
            
            objective = portfolio_risk
        
        # 约束条件
        constraints = ({'type': 'eq', 'fun': lambda x: np.sum(x) - 1})
        bounds = tuple((0, 1) for _ in range(self.n_assets))
        
        # 初始权重（等权）
        init_weights = np.array([1/self.n_assets] * self.n_assets)
        
        # 优化
        result = minimize(
            objective,
            init_weights,
            method='SLSQP',
            bounds=bounds,
            constraints=constraints
        )
        
        if result.success:
            optimal_weights = result.x
            metrics = self.calculate_portfolio_metrics(optimal_weights)
            
            return {
                'weights': dict(zip(self.assets, optimal_weights)),
                'metrics': metrics,
                'success': True
            }
        else:
            logger.error("优化失败")
            return {'success': False}
    
    def calculate_efficient_frontier(self, n_points: int = 100):
        """计算有效前沿"""
        target_returns = np.linspace(
            self.returns.mean().min() * 252,
            self.returns.mean().max() * 252,
            n_points
        )
        
        frontier_risk = []
        frontier_return = []
        
        for target_return in target_returns:
            # 对于每个目标收益，最小化风险
            def portfolio_risk(weights):
                cov_matrix = self.returns.cov() * 252
                return np.sqrt(np.dot(weights.T, np.dot(cov_matrix, weights)))
            
            def return_constraint(weights):
                return np.sum(self.returns.mean() * weights) * 252 - target_return
            
            constraints = [
                {'type': 'eq', 'fun': lambda x: np.sum(x) - 1},
                {'type': 'eq', 'fun': return_constraint}
            ]
            
            bounds = tuple((0, 1) for _ in range(self.n_assets))
            init_weights = np.array([1/self.n_assets] * self.n_assets)
            
            result = minimize(
                portfolio_risk,
                init_weights,
                method='SLSQP',
                bounds=bounds,
                constraints=constraints
            )
            
            if result.success:
                frontier_risk.append(result.fun)
                frontier_return.append(target_return)
        
        return pd.DataFrame({
            'return': frontier_return,
            'risk': frontier_risk
        })
    
    def calculate_correlation_matrix(self):
        """计算相关性矩阵"""
        return self.returns.corr()
    
    def calculate_var(self, weights: np.ndarray, confidence: float = 0.95):
        """计算VaR"""
        portfolio_returns = self.returns.dot(weights)
        var = portfolio_returns.quantile(1 - confidence)
        return var

def main():
    # 示例：加载数据并分析
    logger.info("组合分析工具已准备就绪")
    
    # 可以在这里添加实际的数据加载和分析代码

if __name__ == "__main__":
    main()