#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
组合优化脚本
============

寻找最优投资组合配置

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
try:
    import cvxpy as cp
    HAS_CVXPY = True
except ImportError:
    HAS_CVXPY = False
    import warnings
    warnings.warn("cvxpy未安装，部分优化功能将不可用")
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PortfolioOptimizer:
    """组合优化器"""
    
    def __init__(self, returns: pd.DataFrame):
        self.returns = returns
        self.mean_returns = returns.mean()
        self.cov_matrix = returns.cov()
        self.n_assets = len(returns.columns)
        
    def mean_variance_optimization(self, target_return: float = None) -> Dict:
        """均值-方差优化"""
        
        # 定义变量
        weights = cp.Variable(self.n_assets)
        
        # 预期收益
        portfolio_return = self.mean_returns.values @ weights
        
        # 组合风险
        portfolio_risk = cp.quad_form(weights, self.cov_matrix.values)
        
        # 约束条件
        constraints = [
            cp.sum(weights) == 1,
            weights >= 0
        ]
        
        if target_return:
            constraints.append(portfolio_return >= target_return)
            # 最小化风险
            objective = cp.Minimize(portfolio_risk)
        else:
            # 最大化夏普比率（这里简化为最大化收益/风险）
            objective = cp.Maximize(portfolio_return - 0.5 * portfolio_risk)
        
        # 求解
        problem = cp.Problem(objective, constraints)
        problem.solve()
        
        if weights.value is not None:
            return {
                'weights': weights.value,
                'expected_return': self.mean_returns.values @ weights.value,
                'risk': np.sqrt(weights.value @ self.cov_matrix.values @ weights.value),
                'status': 'optimal'
            }
        else:
            return {'status': 'failed'}
    
    def risk_parity_optimization(self) -> Dict:
        """风险平价优化"""
        
        def risk_contribution(weights):
            """计算风险贡献"""
            portfolio_vol = np.sqrt(weights @ self.cov_matrix.values @ weights)
            marginal_contrib = self.cov_matrix.values @ weights
            contrib = weights * marginal_contrib / portfolio_vol
            return contrib
        
        def objective(weights):
            """目标函数：最小化风险贡献的差异"""
            contrib = risk_contribution(weights)
            target = np.ones(self.n_assets) / self.n_assets
            return np.sum((contrib - target) ** 2)
        
        # 约束
        constraints = ({'type': 'eq', 'fun': lambda x: np.sum(x) - 1})
        bounds = tuple((0.01, 1) for _ in range(self.n_assets))
        
        # 初始权重
        x0 = np.ones(self.n_assets) / self.n_assets
        
        # 优化
        result = minimize(objective, x0, method='SLSQP', 
                         bounds=bounds, constraints=constraints)
        
        if result.success:
            weights = result.x
            return {
                'weights': weights,
                'risk_contributions': risk_contribution(weights),
                'total_risk': np.sqrt(weights @ self.cov_matrix.values @ weights),
                'status': 'optimal'
            }
        else:
            return {'status': 'failed'}
    
    def black_litterman_optimization(self, views: Dict, view_confidence: float = 0.25):
        """Black-Litterman模型优化"""
        
        # 市场权重（这里用等权代替）
        market_weights = np.ones(self.n_assets) / self.n_assets
        
        # 风险厌恶系数
        delta = 2.5
        
        # 先验收益
        pi = delta * self.cov_matrix.values @ market_weights
        
        # 观点矩阵
        P = np.zeros((len(views), self.n_assets))
        Q = np.zeros(len(views))
        
        for i, (assets, view_return) in enumerate(views.items()):
            # 简化：假设views是{asset_index: expected_return}的形式
            P[i, assets] = 1
            Q[i] = view_return
        
        # 观点不确定性
        omega = np.diag([view_confidence] * len(views))
        
        # 后验期望收益
        tau = 0.05
        M = np.linalg.inv(np.linalg.inv(tau * self.cov_matrix.values) + P.T @ np.linalg.inv(omega) @ P)
        posterior_returns = M @ (np.linalg.inv(tau * self.cov_matrix.values) @ pi + P.T @ np.linalg.inv(omega) @ Q)
        
        # 后验协方差
        posterior_cov = M + self.cov_matrix.values
        
        # 优化权重
        optimal_weights = np.linalg.inv(delta * posterior_cov) @ posterior_returns
        optimal_weights = optimal_weights / np.sum(optimal_weights)  # 标准化
        
        return {
            'weights': optimal_weights,
            'posterior_returns': posterior_returns,
            'status': 'optimal'
        }

def main():
    logger.info("组合优化工具已准备就绪")

if __name__ == "__main__":
    main()