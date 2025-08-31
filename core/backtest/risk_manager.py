#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
风险管理器 - risk_manager.py
============================

提供全面的风险管理功能，包括仓位管理、风险度量、止损控制等。

主要功能：
1. 仓位管理（固定仓位、凯利公式、风险平价）
2. 风险度量（VaR、CVaR、最大回撤控制）
3. 止损止盈策略
4. 组合风险管理

版本: 1.0.0
更新: 2025-08-30
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime
import logging
from dataclasses import dataclass
from enum import Enum


# ==========================================
# 📊 风险管理枚举和数据类
# ==========================================

class PositionSizingMethod(Enum):
    """仓位管理方法枚举"""
    FIXED = "fixed"                    # 固定仓位
    KELLY = "kelly"                    # 凯利公式
    RISK_PARITY = "risk_parity"        # 风险平价
    VOLATILITY_BASED = "volatility"    # 基于波动率
    ATR_BASED = "atr"                  # 基于ATR


@dataclass
class RiskMetrics:
    """风险指标数据类"""
    var_95: float               # 95% VaR
    var_99: float               # 99% VaR
    cvar_95: float              # 95% CVaR
    cvar_99: float              # 99% CVaR
    max_position_risk: float    # 最大仓位风险
    portfolio_volatility: float  # 组合波动率
    beta: float                 # 贝塔系数
    correlation_with_market: float  # 与市场相关性
    
    def to_dict(self) -> Dict:
        """转换为字典"""
        return {
            'VaR_95%': f"{self.var_95:.2%}",
            'VaR_99%': f"{self.var_99:.2%}",
            'CVaR_95%': f"{self.cvar_95:.2%}",
            'CVaR_99%': f"{self.cvar_99:.2%}",
            'max_position_risk': f"{self.max_position_risk:.2%}",
            'portfolio_volatility': f"{self.portfolio_volatility:.2%}",
            'beta': f"{self.beta:.2f}",
            'correlation_with_market': f"{self.correlation_with_market:.2f}"
        }


# ==========================================
# 📈 仓位管理器
# ==========================================

class PositionSizer:
    """仓位管理器"""
    
    def __init__(self, method: str = 'fixed'):
        """
        初始化仓位管理器
        
        Args:
            method: 仓位管理方法
        """
        self.method = PositionSizingMethod(method) if isinstance(method, str) else method
        self.logger = logging.getLogger('PositionSizer')
    
    def calculate_position_size(
        self,
        capital: float,
        risk_per_trade: float,
        stop_loss: float,
        price: float,
        volatility: Optional[float] = None,
        win_rate: Optional[float] = None,
        win_loss_ratio: Optional[float] = None
    ) -> int:
        """
        计算仓位大小
        
        Args:
            capital: 可用资金
            risk_per_trade: 每笔交易风险
            stop_loss: 止损比例
            price: 当前价格
            volatility: 波动率（可选）
            win_rate: 胜率（可选，凯利公式需要）
            win_loss_ratio: 盈亏比（可选，凯利公式需要）
        
        Returns:
            建议持仓股数（已取整到100股）
        """
        if self.method == PositionSizingMethod.FIXED:
            return self._fixed_position_size(capital, risk_per_trade, price)
        
        elif self.method == PositionSizingMethod.KELLY:
            return self._kelly_position_size(
                capital, price, win_rate or 0.55, win_loss_ratio or 2.0
            )
        
        elif self.method == PositionSizingMethod.VOLATILITY_BASED:
            return self._volatility_based_position_size(
                capital, risk_per_trade, price, volatility or 0.02
            )
        
        elif self.method == PositionSizingMethod.ATR_BASED:
            return self._atr_based_position_size(
                capital, risk_per_trade, stop_loss, price
            )
        
        else:
            return self._fixed_position_size(capital, risk_per_trade, price)
    
    def _fixed_position_size(self, capital: float, risk_per_trade: float, price: float) -> int:
        """固定仓位法"""
        position_value = capital * risk_per_trade
        shares = int(position_value / price / 100) * 100
        
        self.logger.debug(f"固定仓位: {shares}股")
        return shares
    
    def _kelly_position_size(
        self, capital: float, price: float, win_rate: float, win_loss_ratio: float
    ) -> int:
        """凯利公式法"""
        # Kelly % = (p * b - q) / b
        # p = 胜率, q = 败率, b = 盈亏比
        q = 1 - win_rate
        kelly_pct = (win_rate * win_loss_ratio - q) / win_loss_ratio
        
        # 限制最大仓位为25%（凯利公式可能给出过高的值）
        kelly_pct = min(max(kelly_pct, 0), 0.25)
        
        position_value = capital * kelly_pct
        shares = int(position_value / price / 100) * 100
        
        self.logger.debug(f"凯利仓位: {shares}股 (Kelly%: {kelly_pct:.2%})")
        return shares
    
    def _volatility_based_position_size(
        self, capital: float, risk_per_trade: float, price: float, volatility: float
    ) -> int:
        """基于波动率的仓位管理"""
        # 根据波动率调整仓位，波动率越高，仓位越小
        target_volatility = 0.15  # 目标年化波动率
        volatility_adj = min(target_volatility / (volatility * np.sqrt(252)), 1.0)
        
        position_value = capital * risk_per_trade * volatility_adj
        shares = int(position_value / price / 100) * 100
        
        self.logger.debug(f"波动率调整仓位: {shares}股")
        return shares
    
    def _atr_based_position_size(
        self, capital: float, risk_per_trade: float, stop_loss: float, price: float
    ) -> int:
        """基于ATR的仓位管理"""
        # 风险金额 = 资金 * 每笔交易风险
        risk_amount = capital * risk_per_trade
        
        # 每股风险 = 价格 * 止损比例
        risk_per_share = price * stop_loss
        
        # 股数 = 风险金额 / 每股风险
        shares = int(risk_amount / risk_per_share / 100) * 100
        
        self.logger.debug(f"ATR仓位: {shares}股")
        return shares


# ==========================================
# 🛡️ 风险管理器主类
# ==========================================

class RiskManager:
    """风险管理器"""
    
    def __init__(
        self,
        initial_capital: float = 1000000,
        risk_per_trade: float = 0.02,
        max_drawdown_limit: float = 0.2,
        position_sizing_method: str = 'fixed',
        **kwargs
    ):
        """
        初始化风险管理器
        
        Args:
            initial_capital: 初始资金
            risk_per_trade: 每笔交易风险
            max_drawdown_limit: 最大回撤限制
            position_sizing_method: 仓位管理方法
            **kwargs: 其他参数
        """
        self.initial_capital = initial_capital
        self.risk_per_trade = risk_per_trade
        self.max_drawdown_limit = max_drawdown_limit
        
        # 设置风险参数（兼容旧版本参数）
        self.max_position_size = kwargs.get('max_position_size', 0.1)
        self.max_portfolio_risk = kwargs.get('max_portfolio_risk', 0.02)
        self.stop_loss = kwargs.get('stop_loss', 0.05)
        self.take_profit = kwargs.get('take_profit', 0.15)
        
        # 仓位管理器
        self.position_sizer = PositionSizer(position_sizing_method)
        
        # 风险追踪
        self.current_positions = {}
        self.historical_drawdown = []
        
        # 设置日志
        self.logger = logging.getLogger('RiskManager')
        self.logger.info(
            f"风险管理器初始化 - 资金: {initial_capital:,.0f}, "
            f"单笔风险: {risk_per_trade:.1%}, 最大回撤: {max_drawdown_limit:.1%}"
        )
    
    # ==========================================
    # 📊 仓位管理
    # ==========================================
    
    def calculate_position_size(
        self,
        available_capital: float,
        price: float,
        volatility: float = 0.02,
        symbol: Optional[str] = None
    ) -> int:
        """
        计算仓位大小
        
        Args:
            available_capital: 可用资金
            price: 股票价格
            volatility: 波动率
            symbol: 股票代码（可选）
        
        Returns:
            建议持仓股数
        """
        # 使用仓位管理器计算基础仓位
        position_size = self.position_sizer.calculate_position_size(
            capital=available_capital,
            risk_per_trade=self.risk_per_trade,
            stop_loss=self.stop_loss,
            price=price,
            volatility=volatility
        )
        
        # 应用最大仓位限制
        max_shares = int(available_capital * self.max_position_size / price / 100) * 100
        position_size = min(position_size, max_shares)
        
        # 检查是否超过总资金限制
        position_value = position_size * price
        if position_value > available_capital * 0.95:  # 保留5%现金
            position_size = int(available_capital * 0.95 / price / 100) * 100
        
        self.logger.debug(
            f"计算仓位 - 股票: {symbol}, 价格: {price:.2f}, "
            f"建议仓位: {position_size}股"
        )
        
        return position_size
    
    # ==========================================
    # 📉 风险检查
    # ==========================================
    
    def check_risk_limits(
        self,
        current_drawdown: float,
        current_positions: Dict,
        total_capital: float
    ) -> Dict[str, bool]:
        """
        检查风险限制
        
        Args:
            current_drawdown: 当前回撤
            current_positions: 当前持仓
            total_capital: 总资金
        
        Returns:
            各项风险检查结果
        """
        checks = {
            'drawdown_ok': True,
            'position_concentration_ok': True,
            'portfolio_risk_ok': True,
            'leverage_ok': True
        }
        
        # 检查回撤限制
        if abs(current_drawdown) > self.max_drawdown_limit:
            checks['drawdown_ok'] = False
            self.logger.warning(f"回撤超限: {current_drawdown:.2%} > {self.max_drawdown_limit:.2%}")
        
        # 检查仓位集中度
        if current_positions:
            for symbol, position in current_positions.items():
                position_value = position.get('value', 0)
                position_pct = position_value / total_capital
                
                if position_pct > self.max_position_size:
                    checks['position_concentration_ok'] = False
                    self.logger.warning(
                        f"仓位集中度超限 - {symbol}: {position_pct:.2%} > {self.max_position_size:.2%}"
                    )
        
        # 检查组合风险
        total_exposure = sum(p.get('value', 0) for p in current_positions.values())
        exposure_pct = total_exposure / total_capital
        
        if exposure_pct > 0.95:  # 最大仓位95%
            checks['leverage_ok'] = False
            self.logger.warning(f"总仓位过高: {exposure_pct:.2%}")
        
        return checks
    
    # ==========================================
    # 📊 风险度量
    # ==========================================
    
    def calculate_var(
        self,
        returns: pd.Series,
        confidence_level: float = 0.95,
        method: str = 'historical'
    ) -> float:
        """
        计算VaR (Value at Risk)
        
        Args:
            returns: 收益率序列
            confidence_level: 置信水平
            method: 计算方法 ('historical', 'parametric', 'montecarlo')
        
        Returns:
            VaR值（负数表示损失）
        """
        if returns.empty:
            return 0.0
        
        if method == 'historical':
            # 历史模拟法
            var = np.percentile(returns, (1 - confidence_level) * 100)
        
        elif method == 'parametric':
            # 参数法（假设正态分布）
            mean = returns.mean()
            std = returns.std()
            from scipy import stats
            var = mean + std * stats.norm.ppf(1 - confidence_level)
        
        elif method == 'montecarlo':
            # 蒙特卡罗模拟
            mean = returns.mean()
            std = returns.std()
            simulated_returns = np.random.normal(mean, std, 10000)
            var = np.percentile(simulated_returns, (1 - confidence_level) * 100)
        
        else:
            var = np.percentile(returns, (1 - confidence_level) * 100)
        
        self.logger.debug(f"VaR({confidence_level:.0%}): {var:.2%}")
        return var
    
    def calculate_cvar(
        self,
        returns: pd.Series,
        confidence_level: float = 0.95
    ) -> float:
        """
        计算CVaR (Conditional Value at Risk)
        
        Args:
            returns: 收益率序列
            confidence_level: 置信水平
        
        Returns:
            CVaR值（负数表示损失）
        """
        var = self.calculate_var(returns, confidence_level)
        cvar = returns[returns <= var].mean()
        
        if np.isnan(cvar):
            cvar = var
        
        self.logger.debug(f"CVaR({confidence_level:.0%}): {cvar:.2%}")
        return cvar
    
    def calculate_portfolio_risk(
        self,
        positions: Dict,
        returns_data: pd.DataFrame,
        correlation_matrix: Optional[pd.DataFrame] = None
    ) -> float:
        """
        计算组合风险
        
        Args:
            positions: 持仓字典 {symbol: weight}
            returns_data: 各资产收益率数据
            correlation_matrix: 相关性矩阵（可选）
        
        Returns:
            组合标准差（年化）
        """
        if not positions or returns_data.empty:
            return 0.0
        
        # 获取持仓权重
        symbols = list(positions.keys())
        weights = np.array([positions[s] for s in symbols])
        weights = weights / weights.sum()  # 归一化
        
        # 计算协方差矩阵
        if correlation_matrix is None:
            returns_subset = returns_data[symbols] if symbols else returns_data
            cov_matrix = returns_subset.cov()
        else:
            # 使用提供的相关性矩阵计算协方差
            stds = returns_data[symbols].std()
            cov_matrix = correlation_matrix * np.outer(stds, stds)
        
        # 计算组合方差
        portfolio_variance = np.dot(weights, np.dot(cov_matrix, weights))
        portfolio_std = np.sqrt(portfolio_variance)
        
        # 年化
        annual_std = portfolio_std * np.sqrt(252)
        
        self.logger.debug(f"组合风险(年化): {annual_std:.2%}")
        return annual_std
    
    # ==========================================
    # 🛡️ 止损止盈
    # ==========================================
    
    def should_stop_loss(
        self,
        entry_price: float,
        current_price: float,
        stop_loss_pct: Optional[float] = None
    ) -> bool:
        """
        判断是否应该止损
        
        Args:
            entry_price: 入场价格
            current_price: 当前价格
            stop_loss_pct: 止损比例（可选，默认使用设置值）
        
        Returns:
            是否触发止损
        """
        stop_loss_pct = stop_loss_pct or self.stop_loss
        loss_pct = (current_price - entry_price) / entry_price
        
        should_stop = loss_pct <= -stop_loss_pct
        
        if should_stop:
            self.logger.info(f"触发止损: 亏损 {loss_pct:.2%}")
        
        return should_stop
    
    def should_take_profit(
        self,
        entry_price: float,
        current_price: float,
        take_profit_pct: Optional[float] = None
    ) -> bool:
        """
        判断是否应该止盈
        
        Args:
            entry_price: 入场价格
            current_price: 当前价格
            take_profit_pct: 止盈比例（可选，默认使用设置值）
        
        Returns:
            是否触发止盈
        """
        take_profit_pct = take_profit_pct or self.take_profit
        profit_pct = (current_price - entry_price) / entry_price
        
        should_take = profit_pct >= take_profit_pct
        
        if should_take:
            self.logger.info(f"触发止盈: 盈利 {profit_pct:.2%}")
        
        return should_take
    
    # ==========================================
    # 📈 风险报告
    # ==========================================
    
    def generate_risk_report(
        self,
        returns: pd.Series,
        positions: Dict,
        market_returns: Optional[pd.Series] = None
    ) -> RiskMetrics:
        """
        生成风险报告
        
        Args:
            returns: 策略收益率
            positions: 当前持仓
            market_returns: 市场收益率（可选）
        
        Returns:
            风险指标对象
        """
        # 计算VaR和CVaR
        var_95 = self.calculate_var(returns, 0.95)
        var_99 = self.calculate_var(returns, 0.99)
        cvar_95 = self.calculate_cvar(returns, 0.95)
        cvar_99 = self.calculate_cvar(returns, 0.99)
        
        # 计算最大仓位风险
        max_position_risk = max(
            (p.get('value', 0) / self.initial_capital for p in positions.values()),
            default=0
        )
        
        # 计算组合波动率
        portfolio_volatility = returns.std() * np.sqrt(252)
        
        # 计算贝塔和相关性（如果有市场数据）
        beta = 1.0
        correlation = 0.0
        
        if market_returns is not None and not market_returns.empty:
            # 计算贝塔
            covariance = returns.cov(market_returns)
            market_variance = market_returns.var()
            beta = covariance / market_variance if market_variance != 0 else 1.0
            
            # 计算相关性
            correlation = returns.corr(market_returns)
        
        metrics = RiskMetrics(
            var_95=var_95,
            var_99=var_99,
            cvar_95=cvar_95,
            cvar_99=cvar_99,
            max_position_risk=max_position_risk,
            portfolio_volatility=portfolio_volatility,
            beta=beta,
            correlation_with_market=correlation
        )
        
        self.logger.info("风险报告生成完成")
        return metrics


# ==========================================
# 📊 测试代码
# ==========================================

if __name__ == "__main__":
    print("风险管理器模块测试")
    print("=" * 60)
    
    # 创建风险管理器
    risk_manager = RiskManager(
        initial_capital=1000000,
        risk_per_trade=0.02,
        max_drawdown_limit=0.2,
        position_sizing_method='fixed'
    )
    
    print(f"✅ 风险管理器创建成功")
    print(f"   初始资金: ¥{risk_manager.initial_capital:,.0f}")
    print(f"   单笔风险: {risk_manager.risk_per_trade:.1%}")
    print(f"   最大回撤: {risk_manager.max_drawdown_limit:.1%}")
    
    # 测试仓位计算
    position_size = risk_manager.calculate_position_size(
        available_capital=1000000,
        price=50.0,
        volatility=0.02
    )
    print(f"\n仓位计算测试:")
    print(f"   建议仓位: {position_size}股")
    
    # 测试风险度量
    test_returns = pd.Series(np.random.randn(252) * 0.02)
    var_95 = risk_manager.calculate_var(test_returns, 0.95)
    cvar_95 = risk_manager.calculate_cvar(test_returns, 0.95)
    
    print(f"\n风险度量测试:")
    print(f"   VaR(95%): {var_95:.2%}")
    print(f"   CVaR(95%): {cvar_95:.2%}")
    
    print("\n✅ 风险管理器模块测试完成！")