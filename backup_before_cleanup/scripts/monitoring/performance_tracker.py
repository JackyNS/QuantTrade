#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
性能跟踪脚本
============

跟踪和记录策略性能指标

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
import json
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PerformanceTracker:
    """性能跟踪器"""
    
    def __init__(self, strategy_name: str):
        self.strategy_name = strategy_name
        self.performance_dir = Path("./data/performance")
        self.performance_dir.mkdir(parents=True, exist_ok=True)
        
        self.metrics_history = []
        self.trades_history = []
        self.daily_pnl = []
        
    def record_trade(self, trade: Dict):
        """记录交易"""
        trade['timestamp'] = datetime.now()
        trade['strategy'] = self.strategy_name
        self.trades_history.append(trade)
        
        # 更新日内盈亏
        self._update_daily_pnl(trade)
        
    def _update_daily_pnl(self, trade: Dict):
        """更新日内盈亏"""
        today = datetime.now().date()
        
        # 查找今日记录
        daily_record = None
        for record in self.daily_pnl:
            if record['date'] == today:
                daily_record = record
                break
        
        if daily_record is None:
            daily_record = {
                'date': today,
                'trades': 0,
                'gross_pnl': 0,
                'net_pnl': 0,
                'win_trades': 0,
                'loss_trades': 0
            }
            self.daily_pnl.append(daily_record)
        
        # 更新记录
        daily_record['trades'] += 1
        pnl = trade.get('pnl', 0)
        commission = trade.get('commission', 0)
        
        daily_record['gross_pnl'] += pnl
        daily_record['net_pnl'] += (pnl - commission)
        
        if pnl > 0:
            daily_record['win_trades'] += 1
        elif pnl < 0:
            daily_record['loss_trades'] += 1
    
    def calculate_metrics(self) -> Dict:
        """计算性能指标"""
        if not self.trades_history:
            return {}
        
        trades_df = pd.DataFrame(self.trades_history)
        
        # 计算基础指标
        total_trades = len(trades_df)
        winning_trades = len(trades_df[trades_df['pnl'] > 0])
        losing_trades = len(trades_df[trades_df['pnl'] < 0])
        
        # 盈亏统计
        total_pnl = trades_df['pnl'].sum()
        avg_win = trades_df[trades_df['pnl'] > 0]['pnl'].mean() if winning_trades > 0 else 0
        avg_loss = trades_df[trades_df['pnl'] < 0]['pnl'].mean() if losing_trades > 0 else 0
        
        # 计算比率
        win_rate = winning_trades / total_trades if total_trades > 0 else 0
        profit_factor = abs(trades_df[trades_df['pnl'] > 0]['pnl'].sum() / 
                           trades_df[trades_df['pnl'] < 0]['pnl'].sum()) if losing_trades > 0 else 0
        
        # 最大回撤
        cumulative_pnl = trades_df['pnl'].cumsum()
        running_max = cumulative_pnl.cummax()
        drawdown = cumulative_pnl - running_max
        max_drawdown = drawdown.min()
        
        metrics = {
            'total_trades': total_trades,
            'winning_trades': winning_trades,
            'losing_trades': losing_trades,
            'win_rate': win_rate,
            'total_pnl': total_pnl,
            'avg_win': avg_win,
            'avg_loss': avg_loss,
            'profit_factor': profit_factor,
            'max_drawdown': max_drawdown,
            'sharpe_ratio': self._calculate_sharpe_ratio(trades_df),
            'updated_at': datetime.now()
        }
        
        self.metrics_history.append(metrics)
        return metrics
    
    def _calculate_sharpe_ratio(self, trades_df: pd.DataFrame) -> float:
        """计算夏普比率"""
        if len(trades_df) < 2:
            return 0
        
        # 按日汇总收益
        trades_df['date'] = pd.to_datetime(trades_df['timestamp']).dt.date
        daily_returns = trades_df.groupby('date')['pnl'].sum()
        
        if len(daily_returns) < 2:
            return 0
        
        # 计算夏普比率
        mean_return = daily_returns.mean()
        std_return = daily_returns.std()
        
        if std_return == 0:
            return 0
        
        sharpe = (mean_return * 252) / (std_return * np.sqrt(252))
        return sharpe
    
    def save_performance_report(self):
        """保存性能报告"""
        report = {
            'strategy': self.strategy_name,
            'generated_at': datetime.now().isoformat(),
            'current_metrics': self.calculate_metrics(),
            'daily_pnl': [
                {
                    'date': record['date'].isoformat(),
                    'trades': record['trades'],
                    'gross_pnl': record['gross_pnl'],
                    'net_pnl': record['net_pnl'],
                    'win_rate': record['win_trades'] / record['trades'] if record['trades'] > 0 else 0
                }
                for record in self.daily_pnl
            ],
            'metrics_history': [
                {k: v if not isinstance(v, datetime) else v.isoformat() 
                 for k, v in m.items()}
                for m in self.metrics_history[-100:]  # 保留最近100条
            ]
        }
        
        # 保存到文件
        report_file = self.performance_dir / f"{self.strategy_name}_performance_{datetime.now().strftime('%Y%m%d')}.json"
        
        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        
        logger.info(f"性能报告已保存: {report_file}")
        return report_file

def main():
    tracker = PerformanceTracker("MA_Cross")
    
    # 模拟一些交易
    sample_trades = [
        {'symbol': '000001', 'side': 'buy', 'price': 10.5, 'quantity': 1000, 'pnl': 0},
        {'symbol': '000001', 'side': 'sell', 'price': 11.0, 'quantity': 1000, 'pnl': 500, 'commission': 10},
        {'symbol': '000002', 'side': 'buy', 'price': 25.0, 'quantity': 500, 'pnl': 0},
        {'symbol': '000002', 'side': 'sell', 'price': 24.5, 'quantity': 500, 'pnl': -250, 'commission': 10},
    ]
    
    for trade in sample_trades:
        tracker.record_trade(trade)
    
    # 计算指标
    metrics = tracker.calculate_metrics()
    print("性能指标:")
    for key, value in metrics.items():
        if isinstance(value, float):
            print(f"  {key}: {value:.4f}")
        else:
            print(f"  {key}: {value}")
    
    # 保存报告
    tracker.save_performance_report()

if __name__ == "__main__":
    main()