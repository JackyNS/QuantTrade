#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
回测分析脚本
============

深度分析回测结果

Author: QuantTrader Team
Date: 2025-08-31
"""

from typing import Dict

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import json

class BacktestAnalyzer:
    """回测分析器"""
    
    def __init__(self, backtest_result: Dict):
        self.result = backtest_result
        self.returns = pd.Series(backtest_result.get('returns', []))
        self.equity_curve = pd.Series(backtest_result.get('equity_curve', []))
        
    def calculate_metrics(self) -> Dict:
        """计算详细指标"""
        metrics = {
            # 收益指标
            'total_return': self.returns.sum(),
            'annual_return': self.returns.mean() * 252,
            'monthly_return': self.returns.mean() * 21,
            
            # 风险指标
            'volatility': self.returns.std() * np.sqrt(252),
            'downside_volatility': self._calculate_downside_volatility(),
            'max_drawdown': self._calculate_max_drawdown(),
            'var_95': self.returns.quantile(0.05),
            
            # 风险调整收益
            'sharpe_ratio': self._calculate_sharpe_ratio(),
            'sortino_ratio': self._calculate_sortino_ratio(),
            'calmar_ratio': self._calculate_calmar_ratio(),
            
            # 交易统计
            'win_rate': len(self.returns[self.returns > 0]) / len(self.returns),
            'profit_factor': self._calculate_profit_factor(),
            'avg_win': self.returns[self.returns > 0].mean() if len(self.returns[self.returns > 0]) > 0 else 0,
            'avg_loss': self.returns[self.returns < 0].mean() if len(self.returns[self.returns < 0]) > 0 else 0,
        }
        
        return metrics
    
    def _calculate_downside_volatility(self) -> float:
        """计算下行波动率"""
        negative_returns = self.returns[self.returns < 0]
        if len(negative_returns) > 0:
            return negative_returns.std() * np.sqrt(252)
        return 0
    
    def _calculate_max_drawdown(self) -> float:
        """计算最大回撤"""
        cumulative = (1 + self.returns).cumprod()
        running_max = cumulative.cummax()
        drawdown = (cumulative - running_max) / running_max
        return abs(drawdown.min())
    
    def _calculate_sharpe_ratio(self) -> float:
        """计算夏普比率"""
        if self.returns.std() > 0:
            return (self.returns.mean() * 252) / (self.returns.std() * np.sqrt(252))
        return 0
    
    def _calculate_sortino_ratio(self) -> float:
        """计算索提诺比率"""
        downside_vol = self._calculate_downside_volatility()
        if downside_vol > 0:
            return (self.returns.mean() * 252) / downside_vol
        return 0
    
    def _calculate_calmar_ratio(self) -> float:
        """计算卡尔马比率"""
        max_dd = self._calculate_max_drawdown()
        if max_dd > 0:
            return (self.returns.mean() * 252) / max_dd
        return 0
    
    def _calculate_profit_factor(self) -> float:
        """计算盈亏比"""
        gains = self.returns[self.returns > 0].sum()
        losses = abs(self.returns[self.returns < 0].sum())
        if losses > 0:
            return gains / losses
        return 0
    
    def generate_report(self, output_path: str = None):
        """生成分析报告"""
        metrics = self.calculate_metrics()
        
        report = {
            'summary': {
                'total_return': f"{metrics['total_return']:.2%}",
                'annual_return': f"{metrics['annual_return']:.2%}",
                'sharpe_ratio': f"{metrics['sharpe_ratio']:.2f}",
                'max_drawdown': f"{metrics['max_drawdown']:.2%}",
                'win_rate': f"{metrics['win_rate']:.2%}"
            },
            'detailed_metrics': metrics,
            'timestamp': datetime.now().isoformat()
        }
        
        if output_path:
            with open(output_path, 'w') as f:
                json.dump(report, f, indent=2)
        
        return report

def main():
    # 示例用法
    print("回测分析工具已准备就绪")

if __name__ == "__main__":
    main()