#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
仓位管理器模块
==============

管理投资组合的仓位、风险控制和资金分配
"""

import pandas as pd
import numpy as np
from typing import Optional, Dict, Any, Union, Tuple, List
from datetime import datetime, timedelta
import logging
import warnings

warnings.filterwarnings('ignore')


class PositionManager:
    """
    仓位管理器
    负责管理投资组合的仓位分配和风险控制
    """
    
    def __init__(self, initial_capital: float = 1000000, max_position_size: float = 0.1):
        """
        初始化仓位管理器
        
        Args:
            initial_capital: 初始资金
            max_position_size: 单个股票最大仓位比例
        """
        self.initial_capital = initial_capital
        self.current_capital = initial_capital
        self.max_position_size = max_position_size
        
        # 持仓记录
        self.positions = {}  # {symbol: {'shares': int, 'avg_price': float}}
        self.position_history = []
        
        # 风险参数
        self.stop_loss = 0.05  # 止损比例
        self.take_profit = 0.15  # 止盈比例
        
        # 日志
        self.logger = logging.getLogger(__name__)
    
    def calculate_position_size(self, 
                               symbol: str,
                               current_price: float,
                               signal_strength: float = 1.0,
                               volatility: Optional[float] = None) -> int:
        """
        计算建议的仓位大小
        
        Args:
            symbol: 股票代码
            current_price: 当前价格
            signal_strength: 信号强度 (0-1)
            volatility: 波动率（用于动态仓位调整）
            
        Returns:
            建议买入的股数
        """
        # 计算可用资金
        available_capital = self.current_capital * self.max_position_size
        
        # 根据信号强度调整
        available_capital *= signal_strength
        
        # 根据波动率调整（如果提供）
        if volatility is not None and volatility > 0:
            # 波动率越高，仓位越小
            volatility_adjustment = min(1.0, 0.02 / volatility)  # 假设2%为标准波动率
            available_capital *= volatility_adjustment
        
        # 计算股数（向下取整到整手）
        shares = int(available_capital / current_price / 100) * 100
        
        return shares
    
    def check_position_limits(self, symbol: str, shares: int, price: float) -> bool:
        """
        检查仓位限制
        
        Args:
            symbol: 股票代码
            shares: 计划买入股数
            price: 买入价格
            
        Returns:
            是否符合仓位限制
        """
        # 计算买入后的仓位
        position_value = shares * price
        
        # 检查单个仓位限制
        if position_value > self.current_capital * self.max_position_size:
            self.logger.warning(f"{symbol} 仓位超限: {position_value/self.current_capital:.2%}")
            return False
        
        # 检查总仓位限制
        total_position_value = sum(
            pos['shares'] * price for pos in self.positions.values()
        )
        if (total_position_value + position_value) > self.current_capital * 0.95:
            self.logger.warning("总仓位超过95%")
            return False
        
        return True
    
    def add_position(self, symbol: str, shares: int, price: float, timestamp: datetime):
        """
        添加仓位
        
        Args:
            symbol: 股票代码
            shares: 买入股数
            price: 买入价格
            timestamp: 买入时间
        """
        if symbol in self.positions:
            # 加仓：计算新的平均价格
            old_shares = self.positions[symbol]['shares']
            old_avg_price = self.positions[symbol]['avg_price']
            
            total_value = old_shares * old_avg_price + shares * price
            total_shares = old_shares + shares
            
            self.positions[symbol] = {
                'shares': total_shares,
                'avg_price': total_value / total_shares,
                'last_update': timestamp
            }
        else:
            # 新建仓位
            self.positions[symbol] = {
                'shares': shares,
                'avg_price': price,
                'entry_time': timestamp,
                'last_update': timestamp
            }
        
        # 更新可用资金
        self.current_capital -= shares * price
        
        # 记录历史
        self.position_history.append({
            'timestamp': timestamp,
            'action': 'BUY',
            'symbol': symbol,
            'shares': shares,
            'price': price,
            'capital': self.current_capital
        })
    
    def reduce_position(self, symbol: str, shares: int, price: float, timestamp: datetime):
        """
        减少仓位
        
        Args:
            symbol: 股票代码
            shares: 卖出股数
            price: 卖出价格
            timestamp: 卖出时间
        """
        if symbol not in self.positions:
            self.logger.error(f"没有{symbol}的仓位")
            return
        
        current_shares = self.positions[symbol]['shares']
        if shares > current_shares:
            self.logger.warning(f"卖出股数{shares}超过持仓{current_shares}")
            shares = current_shares
        
        # 更新仓位
        if shares == current_shares:
            # 清仓
            del self.positions[symbol]
        else:
            # 减仓
            self.positions[symbol]['shares'] -= shares
            self.positions[symbol]['last_update'] = timestamp
        
        # 更新可用资金
        self.current_capital += shares * price
        
        # 记录历史
        self.position_history.append({
            'timestamp': timestamp,
            'action': 'SELL',
            'symbol': symbol,
            'shares': shares,
            'price': price,
            'capital': self.current_capital
        })
    
    def get_portfolio_value(self, current_prices: Dict[str, float]) -> float:
        """
        计算组合总价值
        
        Args:
            current_prices: 当前价格字典
            
        Returns:
            组合总价值
        """
        positions_value = sum(
            pos['shares'] * current_prices.get(symbol, pos['avg_price'])
            for symbol, pos in self.positions.items()
        )
        return self.current_capital + positions_value
    
    def get_position_pnl(self, symbol: str, current_price: float) -> Dict[str, float]:
        """
        计算单个仓位的盈亏
        
        Args:
            symbol: 股票代码
            current_price: 当前价格
            
        Returns:
            盈亏信息字典
        """
        if symbol not in self.positions:
            return {'pnl': 0, 'pnl_pct': 0}
        
        position = self.positions[symbol]
        cost = position['shares'] * position['avg_price']
        value = position['shares'] * current_price
        pnl = value - cost
        pnl_pct = pnl / cost if cost > 0 else 0
        
        return {
            'pnl': pnl,
            'pnl_pct': pnl_pct,
            'cost': cost,
            'value': value
        }
    
    def should_stop_loss(self, symbol: str, current_price: float) -> bool:
        """
        判断是否应该止损
        
        Args:
            symbol: 股票代码
            current_price: 当前价格
            
        Returns:
            是否触发止损
        """
        if symbol not in self.positions:
            return False
        
        avg_price = self.positions[symbol]['avg_price']
        loss_pct = (current_price - avg_price) / avg_price
        
        return loss_pct <= -self.stop_loss
    
    def should_take_profit(self, symbol: str, current_price: float) -> bool:
        """
        判断是否应该止盈
        
        Args:
            symbol: 股票代码
            current_price: 当前价格
            
        Returns:
            是否触发止盈
        """
        if symbol not in self.positions:
            return False
        
        avg_price = self.positions[symbol]['avg_price']
        profit_pct = (current_price - avg_price) / avg_price
        
        return profit_pct >= self.take_profit
    
    def get_portfolio_summary(self) -> Dict[str, Any]:
        """
        获取组合摘要信息
        
        Returns:
            组合摘要字典
        """
        return {
            'total_positions': len(self.positions),
            'positions': self.positions.copy(),
            'current_capital': self.current_capital,
            'initial_capital': self.initial_capital,
            'position_history_count': len(self.position_history)
        }
