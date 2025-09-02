#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
性能分析器 - performance_analyzer.py
====================================

计算回测的各种性能指标，包括收益率、风险指标、交易统计等。

主要功能：
1. 收益率计算（总收益、年化收益）
2. 风险指标（夏普比率、最大回撤、索提诺比率）
3. 交易统计（胜率、盈亏比、平均持仓时间）
4. 高级指标（卡尔马比率、信息比率等）

版本: 1.0.0
更新: 2025-08-30
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime
import logging
from dataclasses import dataclass


# ==========================================
# 📊 性能指标数据类
# ==========================================

@dataclass
class PerformanceMetrics:
    """性能指标数据类"""
    total_return: float
    annual_return: float
    sharpe_ratio: float
    max_drawdown: float
    win_rate: float
    profit_factor: float
    avg_win: float
    avg_loss: float
    total_trades: int
    winning_trades: int
    losing_trades: int
    avg_holding_days: float
    calmar_ratio: float
    sortino_ratio: float
    
    def to_dict(self) -> Dict:
        """转换为字典"""
        return {
            'total_return': f"{self.total_return:.2%}",
            'annual_return': f"{self.annual_return:.2%}",
            'sharpe_ratio': f"{self.sharpe_ratio:.2f}",
            'max_drawdown': f"{self.max_drawdown:.2%}",
            'win_rate': f"{self.win_rate:.2%}",
            'profit_factor': f"{self.profit_factor:.2f}",
            'avg_win': f"{self.avg_win:.2%}",
            'avg_loss': f"{self.avg_loss:.2%}",
            'total_trades': self.total_trades,
            'winning_trades': self.winning_trades,
            'losing_trades': self.losing_trades,
            'avg_holding_days': f"{self.avg_holding_days:.1f}",
            'calmar_ratio': f"{self.calmar_ratio:.2f}",
            'sortino_ratio': f"{self.sortino_ratio:.2f}"
        }


# ==========================================
# 📈 性能分析器主类
# ==========================================

class PerformanceAnalyzer:
    """性能分析器"""
    
    def __init__(self, results: Dict = None):
        """
        初始化性能分析器
        
        Args:
            results: 回测结果字典，包含：
                - equity_curve: 权益曲线
                - returns: 收益率序列
                - transactions: 交易记录
        """
        self.results = results or {}
        self.equity_curve = self.results.get('equity_curve', pd.DataFrame())
        self.returns = self.results.get('returns', pd.Series())
        self.transactions = self.results.get('transactions', pd.DataFrame())
        
        # 设置日志
        self.logger = logging.getLogger('PerformanceAnalyzer')
        
        # 如果returns为空，从equity_curve计算
        if self.returns.empty and not self.equity_curve.empty:
            self.returns = self.equity_curve['total_value'].pct_change().fillna(0)
        
        self.logger.info("性能分析器初始化完成")
    
    # ==========================================
    # 📊 基础收益指标
    # ==========================================
    
    def calculate_returns(self) -> float:
        """计算总收益率"""
        if self.equity_curve.empty:
            return 0.0
        
        initial_value = self.equity_curve['total_value'].iloc[0]
        final_value = self.equity_curve['total_value'].iloc[-1]
        total_return = (final_value - initial_value) / initial_value
        
        self.logger.debug(f"总收益率: {total_return:.2%}")
        return total_return
    
    def calculate_annual_return(self) -> float:
        """计算年化收益率"""
        if self.returns.empty:
            return 0.0
        
        total_days = len(self.returns)
        if total_days <= 1:
            return 0.0
        
        total_return = self.calculate_returns()
        years = total_days / 252  # 假设252个交易日
        
        if years <= 0:
            return 0.0
        
        annual_return = (1 + total_return) ** (1 / years) - 1
        self.logger.debug(f"年化收益率: {annual_return:.2%}")
        return annual_return
    
    # ==========================================
    # 📉 风险指标
    # ==========================================
    
    def calculate_sharpe_ratio(self, risk_free_rate: float = 0.03) -> float:
        """
        计算夏普比率
        
        Args:
            risk_free_rate: 无风险利率（年化）
        """
        if self.returns.empty:
            return 0.0
        
        # 日化无风险利率
        daily_rf = risk_free_rate / 252
        excess_returns = self.returns - daily_rf
        
        if excess_returns.std() == 0:
            return 0.0
        
        sharpe = np.sqrt(252) * excess_returns.mean() / excess_returns.std()
        self.logger.debug(f"夏普比率: {sharpe:.2f}")
        return sharpe
    
    def calculate_max_drawdown(self) -> Tuple[float, datetime, datetime]:
        """
        计算最大回撤
        
        Returns:
            (最大回撤值, 开始时间, 结束时间)
        """
        if self.equity_curve.empty:
            return 0.0, None, None
        
        # 计算累积收益
        cumulative = (1 + self.returns).cumprod()
        
        # 计算历史最高点
        running_max = cumulative.expanding().max()
        
        # 计算回撤
        drawdown = (cumulative - running_max) / running_max
        
        # 找到最大回撤
        max_dd = drawdown.min()
        
        if max_dd == 0:
            return 0.0, None, None
        
        # 找到最大回撤的时间段
        end_idx = drawdown.idxmin()
        start_idx = cumulative[:end_idx].idxmax()
        
        self.logger.debug(f"最大回撤: {max_dd:.2%}, 期间: {start_idx} 至 {end_idx}")
        return max_dd, start_idx, end_idx
    
    def calculate_sortino_ratio(self, target_return: float = 0.0) -> float:
        """
        计算索提诺比率（只考虑下行风险）
        
        Args:
            target_return: 目标收益率（年化）
        """
        if self.returns.empty:
            return 0.0
        
        # 日化目标收益率
        daily_target = target_return / 252
        excess_returns = self.returns - daily_target
        
        # 只考虑负收益（下行风险）
        downside_returns = excess_returns[excess_returns < 0]
        
        if len(downside_returns) == 0:
            return float('inf')
        
        # 计算下行标准差
        downside_std = np.sqrt((downside_returns ** 2).mean())
        
        if downside_std == 0:
            return 0.0
        
        sortino = np.sqrt(252) * excess_returns.mean() / downside_std
        self.logger.debug(f"索提诺比率: {sortino:.2f}")
        return sortino
    
    def calculate_calmar_ratio(self) -> float:
        """计算卡尔马比率（年化收益/最大回撤）"""
        annual_return = self.calculate_annual_return()
        max_dd, _, _ = self.calculate_max_drawdown()
        
        if max_dd == 0:
            return float('inf') if annual_return > 0 else 0.0
        
        calmar = annual_return / abs(max_dd)
        self.logger.debug(f"卡尔马比率: {calmar:.2f}")
        return calmar
    
    # ==========================================
    # 📊 交易统计
    # ==========================================
    
    def calculate_win_rate(self) -> float:
        """计算胜率"""
        if self.transactions.empty:
            return 0.0
        
        # 计算每笔交易的盈亏
        trades_pnl = self._calculate_trades_pnl()
        
        if not trades_pnl:
            return 0.0
        
        winning_trades = sum(1 for pnl in trades_pnl if pnl > 0)
        win_rate = winning_trades / len(trades_pnl)
        
        self.logger.debug(f"胜率: {win_rate:.2%}")
        return win_rate
    
    def calculate_profit_factor(self) -> float:
        """计算盈亏比（总盈利/总亏损）"""
        if self.transactions.empty:
            return 0.0
        
        trades_pnl = self._calculate_trades_pnl()
        
        if not trades_pnl:
            return 0.0
        
        gross_profit = sum(pnl for pnl in trades_pnl if pnl > 0)
        gross_loss = abs(sum(pnl for pnl in trades_pnl if pnl < 0))
        
        if gross_loss == 0:
            return float('inf') if gross_profit > 0 else 0.0
        
        profit_factor = gross_profit / gross_loss
        self.logger.debug(f"盈亏比: {profit_factor:.2f}")
        return profit_factor
    
    def _calculate_trades_pnl(self) -> List[float]:
        """计算每笔交易的盈亏"""
        trades_pnl = []
        
        # 简化处理：假设买卖配对
        for i in range(0, len(self.transactions) - 1, 2):
            if i + 1 < len(self.transactions):
                buy_trade = self.transactions.iloc[i]
                sell_trade = self.transactions.iloc[i + 1]
                
                # 确保是买卖配对
                if buy_trade.get('side') == 'BUY' and sell_trade.get('side') == 'SELL':
                    buy_price = buy_trade['price']
                    sell_price = sell_trade['price']
                    
                    # 计算收益率
                    pnl = (sell_price - buy_price) / buy_price
                    trades_pnl.append(pnl)
        
        return trades_pnl
    
    # ==========================================
    # 📈 综合分析
    # ==========================================
    
    def calculate_all_metrics(self) -> PerformanceMetrics:
        """计算所有性能指标"""
        
        self.logger.info("开始计算所有性能指标...")
        
        # 基础指标
        total_return = self.calculate_returns()
        annual_return = self.calculate_annual_return()
        sharpe_ratio = self.calculate_sharpe_ratio()
        max_drawdown, dd_start, dd_end = self.calculate_max_drawdown()
        
        # 交易统计
        win_rate = self.calculate_win_rate()
        profit_factor = self.calculate_profit_factor()
        
        # 计算平均盈亏
        trades_pnl = self._calculate_trades_pnl()
        winning_pnl = [pnl for pnl in trades_pnl if pnl > 0]
        losing_pnl = [pnl for pnl in trades_pnl if pnl < 0]
        
        avg_win = np.mean(winning_pnl) if winning_pnl else 0.0
        avg_loss = np.mean(losing_pnl) if losing_pnl else 0.0
        
        # 交易次数统计
        total_trades = len(trades_pnl)
        winning_trades = len(winning_pnl)
        losing_trades = len(losing_pnl)
        
        # 平均持仓天数（简化计算）
        avg_holding_days = len(self.returns) / max(total_trades, 1)
        
        # 高级指标
        sortino_ratio = self.calculate_sortino_ratio()
        calmar_ratio = self.calculate_calmar_ratio()
        
        metrics = PerformanceMetrics(
            total_return=total_return,
            annual_return=annual_return,
            sharpe_ratio=sharpe_ratio,
            max_drawdown=max_drawdown,
            win_rate=win_rate,
            profit_factor=profit_factor,
            avg_win=avg_win,
            avg_loss=avg_loss,
            total_trades=total_trades,
            winning_trades=winning_trades,
            losing_trades=losing_trades,
            avg_holding_days=avg_holding_days,
            calmar_ratio=calmar_ratio,
            sortino_ratio=sortino_ratio
        )
        
        self.logger.info("性能指标计算完成")
        return metrics
    
    # ==========================================
    # 📊 辅助分析
    # ==========================================
    
    def calculate_rolling_sharpe(self, window: int = 252) -> pd.Series:
        """计算滚动夏普比率"""
        if self.returns.empty or len(self.returns) < window:
            return pd.Series()
        
        rolling_sharpe = self.returns.rolling(window).apply(
            lambda x: np.sqrt(252) * x.mean() / x.std() if x.std() != 0 else 0
        )
        
        return rolling_sharpe
    
    def calculate_monthly_returns(self) -> pd.Series:
        """计算月度收益"""
        if self.returns.empty:
            return pd.Series()
        
        monthly = self.returns.resample('M').apply(
            lambda x: (1 + x).prod() - 1
        )
        
        return monthly
    
    def get_trade_analysis(self) -> Dict:
        """获取详细的交易分析"""
        if self.transactions.empty:
            return {}
        
        trades_pnl = self._calculate_trades_pnl()
        
        if not trades_pnl:
            return {}
        
        return {
            'best_trade': max(trades_pnl),
            'worst_trade': min(trades_pnl),
            'avg_trade': np.mean(trades_pnl),
            'median_trade': np.median(trades_pnl),
            'trade_std': np.std(trades_pnl),
            'consecutive_wins': self._max_consecutive_wins(),
            'consecutive_losses': self._max_consecutive_losses()
        }
    
    def _max_consecutive_wins(self) -> int:
        """计算最大连胜次数"""
        trades_pnl = self._calculate_trades_pnl()
        max_wins = 0
        current_wins = 0
        
        for pnl in trades_pnl:
            if pnl > 0:
                current_wins += 1
                max_wins = max(max_wins, current_wins)
            else:
                current_wins = 0
        
        return max_wins
    
    def _max_consecutive_losses(self) -> int:
        """计算最大连亏次数"""
        trades_pnl = self._calculate_trades_pnl()
        max_losses = 0
        current_losses = 0
        
        for pnl in trades_pnl:
            if pnl < 0:
                current_losses += 1
                max_losses = max(max_losses, current_losses)
            else:
                current_losses = 0
        
        return max_losses


# ==========================================
# 📊 测试代码
# ==========================================

if __name__ == "__main__":
    print("性能分析器模块测试")
    print("=" * 60)
    
    # 创建测试数据
    dates = pd.date_range('2023-01-01', '2023-12-31', freq='D')
    test_results = {
        'equity_curve': pd.DataFrame({
            'total_value': (1 + np.random.randn(len(dates)) * 0.02).cumprod() * 1000000
        }, index=dates),
        'returns': pd.Series(np.random.randn(len(dates)) * 0.02, index=dates),
        'transactions': pd.DataFrame({
            'timestamp': dates[::20],
            'symbol': '000001.SZ',
            'side': ['BUY', 'SELL'] * (len(dates[::20]) // 2),
            'price': np.random.uniform(50, 60, len(dates[::20])),
            'quantity': [1000] * len(dates[::20])
        })
    }
    
    # 创建分析器
    analyzer = PerformanceAnalyzer(test_results)
    
    # 计算指标
    metrics = analyzer.calculate_all_metrics()
    
    print("性能指标:")
    for key, value in metrics.to_dict().items():
        print(f"  {key}: {value}")
    
    print("\n✅ 性能分析器模块测试完成！")