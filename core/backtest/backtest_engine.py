#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
回测引擎 - backtest_engine.py
==============================

事件驱动的回测引擎，模拟真实的交易执行流程。

主要功能：
1. 事件驱动架构
2. 订单执行模拟
3. 交易成本计算（手续费、滑点）
4. 资金管理
5. 持仓跟踪

版本: 1.0.0
更新: 2025-08-29
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Any, Union
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict, deque
import logging
from abc import ABC, abstractmethod
import json
from pathlib import Path


# ==========================================
# 📊 事件类型定义
# ==========================================

class EventType(Enum):
    """事件类型枚举"""
    MARKET = "MARKET"    # 市场数据更新
    SIGNAL = "SIGNAL"    # 策略信号
    ORDER = "ORDER"      # 订单事件
    FILL = "FILL"        # 成交事件


class OrderType(Enum):
    """订单类型枚举"""
    MARKET = "MARKET"    # 市价单
    LIMIT = "LIMIT"      # 限价单
    STOP = "STOP"        # 止损单
    STOP_LIMIT = "STOP_LIMIT"  # 止损限价单


class OrderSide(Enum):
    """订单方向枚举"""
    BUY = "BUY"
    SELL = "SELL"


# ==========================================
# 📋 事件类（不使用继承，避免dataclass问题）
# ==========================================

@dataclass
class MarketEvent:
    """市场数据事件"""
    timestamp: datetime
    symbol: str
    data: pd.Series
    type: EventType = field(default_factory=lambda: EventType.MARKET)


@dataclass
class SignalEvent:
    """策略信号事件"""
    timestamp: datetime
    symbol: str
    signal_type: str  # 'LONG', 'SHORT', 'EXIT'
    strength: float   # 信号强度 0-1
    price: float
    type: EventType = field(default_factory=lambda: EventType.SIGNAL)


@dataclass
class OrderEvent:
    """订单事件"""
    timestamp: datetime
    symbol: str
    order_type: OrderType
    side: OrderSide
    quantity: int
    price: Optional[float] = None
    stop_price: Optional[float] = None
    order_id: Optional[str] = None
    type: EventType = field(default_factory=lambda: EventType.ORDER)


@dataclass
class FillEvent:
    """成交事件"""
    timestamp: datetime
    symbol: str
    exchange: str
    quantity: int
    side: OrderSide
    fill_price: float
    commission: float
    slippage: float
    order_id: Optional[str] = None
    type: EventType = field(default_factory=lambda: EventType.FILL)


# 为了兼容性，创建一个Event类型联合
Event = Union[MarketEvent, SignalEvent, OrderEvent, FillEvent]


# ==========================================
# 📊 持仓和资金管理
# ==========================================

class Portfolio:
    """投资组合管理类"""
    
    def __init__(self, initial_capital: float = 1000000):
        """
        初始化投资组合
        
        Args:
            initial_capital: 初始资金
        """
        self.initial_capital = initial_capital
        self.current_capital = initial_capital
        self.positions = defaultdict(int)  # 当前持仓
        self.avg_price = defaultdict(float)  # 持仓均价
        
        # 历史记录
        self.equity_curve = []  # 权益曲线
        self.transactions = []  # 交易记录
        self.daily_returns = []  # 日收益率
        
        # 统计信息
        self.total_commission = 0
        self.total_slippage = 0
        self.total_trades = 0
        self.winning_trades = 0
        self.losing_trades = 0
        
    def update_positions(self, fill: FillEvent):
        """
        更新持仓
        
        Args:
            fill: 成交事件
        """
        # 更新持仓数量
        if fill.side == OrderSide.BUY:
            # 计算新的持仓均价
            total_value = self.positions[fill.symbol] * self.avg_price[fill.symbol]
            total_value += fill.quantity * fill.fill_price
            self.positions[fill.symbol] += fill.quantity
            if self.positions[fill.symbol] > 0:
                self.avg_price[fill.symbol] = total_value / self.positions[fill.symbol]
        else:  # SELL
            self.positions[fill.symbol] -= fill.quantity
            if self.positions[fill.symbol] == 0:
                self.avg_price[fill.symbol] = 0
        
        # 更新资金
        if fill.side == OrderSide.BUY:
            self.current_capital -= (fill.quantity * fill.fill_price + 
                                    fill.commission + fill.slippage)
        else:
            self.current_capital += (fill.quantity * fill.fill_price - 
                                    fill.commission - fill.slippage)
        
        # 更新统计
        self.total_commission += fill.commission
        self.total_slippage += fill.slippage
        self.total_trades += 1
        
        # 记录交易
        self.transactions.append({
            'timestamp': fill.timestamp,
            'symbol': fill.symbol,
            'side': fill.side.value,
            'quantity': fill.quantity,
            'price': fill.fill_price,
            'commission': fill.commission,
            'slippage': fill.slippage,
            'capital': self.current_capital
        })
    
    def get_total_value(self, current_prices: Dict[str, float]) -> float:
        """
        计算总资产价值
        
        Args:
            current_prices: 当前价格字典
            
        Returns:
            总资产价值
        """
        positions_value = sum(
            self.positions[symbol] * current_prices.get(symbol, 0)
            for symbol in self.positions
        )
        return self.current_capital + positions_value
    
    def get_positions_df(self) -> pd.DataFrame:
        """获取持仓DataFrame"""
        if not self.positions:
            return pd.DataFrame()
        
        positions_data = []
        for symbol, quantity in self.positions.items():
            if quantity != 0:
                positions_data.append({
                    'symbol': symbol,
                    'quantity': quantity,
                    'avg_price': self.avg_price[symbol]
                })
        
        return pd.DataFrame(positions_data)
    
    def get_transactions_df(self) -> pd.DataFrame:
        """获取交易记录DataFrame"""
        if not self.transactions:
            return pd.DataFrame()
        return pd.DataFrame(self.transactions)


# ==========================================
# 🚀 回测引擎主类
# ==========================================

class BacktestEngine:
    """
    事件驱动的回测引擎
    
    主要特性:
    1. 事件队列处理
    2. 策略信号执行
    3. 订单管理和成交模拟
    4. 性能统计
    """
    
    def __init__(self, 
                 initial_capital: float = 1000000,
                 commission: float = 0.002,
                 slippage: float = 0.001,
                 min_commission: float = 5,
                 log_level: str = 'INFO'):
        """
        初始化回测引擎
        
        Args:
            initial_capital: 初始资金
            commission: 手续费率
            slippage: 滑点率
            min_commission: 最小手续费
            log_level: 日志级别
        """
        self.initial_capital = initial_capital
        self.commission_rate = commission
        self.slippage_rate = slippage
        self.min_commission = min_commission
        
        # 组件初始化
        self.portfolio = Portfolio(initial_capital)
        self.events = deque()  # 事件队列
        self.data_handler = None
        self.strategy = None
        
        # 回测状态
        self.current_time = None
        self.is_running = False
        self.bar_index = 0
        
        # 数据存储
        self.market_data = {}
        self.current_prices = {}
        
        # 设置日志
        self.logger = self._setup_logger(log_level)
        
        self.logger.info(f"回测引擎初始化完成 - 初始资金: {initial_capital:,.2f}")
    
    def _setup_logger(self, level: str) -> logging.Logger:
        """设置日志"""
        logger = logging.getLogger('BacktestEngine')
        logger.setLevel(getattr(logging, level))
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
    
    def set_data(self, data: Union[pd.DataFrame, Dict[str, pd.DataFrame]]):
        """
        设置回测数据
        
        Args:
            data: 回测数据,可以是单个DataFrame或多个股票的字典
        """
        if isinstance(data, pd.DataFrame):
            # 单个股票
            symbol = data.get('symbol', 'DEFAULT').iloc[0] if 'symbol' in data.columns else 'DEFAULT'
            self.market_data = {symbol: data}
        else:
            # 多个股票
            self.market_data = data
        
        self.logger.info(f"设置回测数据 - 股票数量: {len(self.market_data)}")
    
    def set_strategy(self, strategy):
        """
        设置策略
        
        Args:
            strategy: 策略对象,需要有calculate_signals方法
        """
        self.strategy = strategy
        self.logger.info(f"设置策略: {strategy.__class__.__name__}")
    
    def _calculate_commission(self, quantity: int, price: float) -> float:
        """
        计算手续费
        
        Args:
            quantity: 成交数量
            price: 成交价格
            
        Returns:
            手续费金额
        """
        commission = quantity * price * self.commission_rate
        return max(commission, self.min_commission)
    
    def _calculate_slippage(self, quantity: int, price: float, side: OrderSide) -> float:
        """
        计算滑点
        
        Args:
            quantity: 成交数量
            price: 理论成交价格
            side: 买卖方向
            
        Returns:
            滑点成本
        """
        slippage_amount = price * self.slippage_rate
        # 买入时价格上滑,卖出时价格下滑
        if side == OrderSide.BUY:
            actual_price = price + slippage_amount
        else:
            actual_price = price - slippage_amount
        
        return abs(quantity * slippage_amount)
    
    def _process_order_event(self, order: OrderEvent):
        """
        处理订单事件
        
        Args:
            order: 订单事件
        """
        # 获取当前价格
        current_price = self.current_prices.get(order.symbol)
        if current_price is None:
            self.logger.warning(f"无法获取{order.symbol}的当前价格")
            return
        
        # 模拟成交
        if order.order_type == OrderType.MARKET:
            # 市价单立即成交
            fill_price = current_price
        elif order.order_type == OrderType.LIMIT:
            # 限价单检查价格
            if order.side == OrderSide.BUY and current_price <= order.price:
                fill_price = order.price
            elif order.side == OrderSide.SELL and current_price >= order.price:
                fill_price = order.price
            else:
                return  # 不成交
        else:
            return  # 其他订单类型暂不支持
        
        # 计算手续费和滑点
        commission = self._calculate_commission(order.quantity, fill_price)
        slippage = self._calculate_slippage(order.quantity, fill_price, order.side)
        
        # 调整实际成交价格(考虑滑点)
        if order.side == OrderSide.BUY:
            fill_price = fill_price * (1 + self.slippage_rate)
        else:
            fill_price = fill_price * (1 - self.slippage_rate)
        
        # 创建成交事件
        fill = FillEvent(
            timestamp=order.timestamp,
            symbol=order.symbol,
            exchange='BACKTEST',
            quantity=order.quantity,
            side=order.side,
            fill_price=fill_price,
            commission=commission,
            slippage=slippage,
            order_id=order.order_id
        )
        
        # 更新投资组合
        self.portfolio.update_positions(fill)
        
        self.logger.debug(f"订单成交: {order.symbol} {order.side.value} "
                         f"{order.quantity}股 @ {fill_price:.2f}")
    
    def _update_market_data(self, timestamp: datetime):
        """
        更新市场数据
        
        Args:
            timestamp: 当前时间戳
        """
        for symbol, data in self.market_data.items():
            # 获取当前时间的数据
            if timestamp in data.index:
                current_bar = data.loc[timestamp]
                self.current_prices[symbol] = current_bar['close']
                
                # 创建市场事件
                market_event = MarketEvent(
                    timestamp=timestamp,
                    symbol=symbol,
                    data=current_bar
                )
                self.events.append(market_event)
    
    def _process_events(self):
        """处理事件队列"""
        while self.events:
            event = self.events.popleft()
            
            if event.type == EventType.MARKET:
                # 市场数据事件,传递给策略
                if self.strategy:
                    signals = self.strategy.calculate_signals(event.data)
                    # 根据信号生成订单
                    self._generate_orders_from_signals(signals, event.symbol)
            
            elif event.type == EventType.ORDER:
                # 处理订单
                self._process_order_event(event)
    
    def _generate_orders_from_signals(self, signals: pd.Series, symbol: str):
        """
        根据策略信号生成订单
        
        Args:
            signals: 策略信号
            symbol: 股票代码
        """
        if signals is None or signals.empty:
            return
        
        # 获取最新信号
        signal = signals.iloc[-1] if isinstance(signals, pd.Series) else signals
        
        if signal > 0:  # 买入信号
            # 计算买入数量
            available_capital = self.portfolio.current_capital * 0.95  # 保留5%现金
            current_price = self.current_prices.get(symbol, 0)
            if current_price > 0:
                quantity = int(available_capital / current_price / 100) * 100  # 整手买入
                if quantity > 0:
                    order = OrderEvent(
                        timestamp=self.current_time,
                        symbol=symbol,
                        order_type=OrderType.MARKET,
                        side=OrderSide.BUY,
                        quantity=quantity
                    )
                    self.events.append(order)
        
        elif signal < 0:  # 卖出信号
            # 卖出所有持仓
            current_position = self.portfolio.positions.get(symbol, 0)
            if current_position > 0:
                order = OrderEvent(
                    timestamp=self.current_time,
                    symbol=symbol,
                    order_type=OrderType.MARKET,
                    side=OrderSide.SELL,
                    quantity=current_position
                )
                self.events.append(order)
    
    def run(self, 
            strategy=None,
            data=None,
            start_date: Optional[str] = None, 
            end_date: Optional[str] = None) -> Dict:
        """
        运行回测
        
        Args:
            strategy: 策略对象
            data: 回测数据
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            回测结果字典
        """
        # 设置策略和数据
        if strategy:
            self.set_strategy(strategy)
        if data is not None:
            self.set_data(data)
        
        if not self.strategy or not self.market_data:
            raise ValueError("请先设置策略和数据")
        
        self.logger.info("=" * 60)
        self.logger.info("开始回测")
        self.logger.info(f"时间范围: {start_date} 至 {end_date}")
        
        # 获取时间索引
        first_symbol = list(self.market_data.keys())[0]
        time_index = self.market_data[first_symbol].index
        
        # 过滤时间范围
        if start_date:
            time_index = time_index[time_index >= pd.to_datetime(start_date)]
        if end_date:
            time_index = time_index[time_index <= pd.to_datetime(end_date)]
        
        # 主回测循环
        self.is_running = True
        total_bars = len(time_index)
        
        for i, timestamp in enumerate(time_index):
            self.current_time = timestamp
            self.bar_index = i
            
            # 更新市场数据
            self._update_market_data(timestamp)
            
            # 处理事件队列
            self._process_events()
            
            # 记录每日权益
            total_value = self.portfolio.get_total_value(self.current_prices)
            self.portfolio.equity_curve.append({
                'timestamp': timestamp,
                'total_value': total_value,
                'cash': self.portfolio.current_capital,
                'positions_value': total_value - self.portfolio.current_capital
            })
            
            # 打印进度
            if (i + 1) % max(1, total_bars // 10) == 0:
                progress = (i + 1) / total_bars * 100
                self.logger.info(f"回测进度: {progress:.0f}% - "
                               f"日期: {timestamp.strftime('%Y-%m-%d')} - "
                               f"总资产: {total_value:,.2f}")
        
        self.is_running = False
        
        # 生成回测结果
        results = self._generate_results()
        
        self.logger.info("回测完成")
        self.logger.info("=" * 60)
        
        return results
    
    def _generate_results(self) -> Dict:
        """
        生成回测结果
        
        Returns:
            包含各种回测指标的字典
        """
        # 转换为DataFrame
        equity_df = pd.DataFrame(self.portfolio.equity_curve)
        if not equity_df.empty:
            equity_df.set_index('timestamp', inplace=True)
        
        transactions_df = self.portfolio.get_transactions_df()
        positions_df = self.portfolio.get_positions_df()
        
        # 计算收益率
        if not equity_df.empty:
            equity_df['returns'] = equity_df['total_value'].pct_change()
            equity_df['cum_returns'] = (1 + equity_df['returns']).cumprod() - 1
        
        # 基本统计
        final_value = equity_df['total_value'].iloc[-1] if not equity_df.empty else self.initial_capital
        total_return = (final_value - self.initial_capital) / self.initial_capital
        
        results = {
            'initial_capital': self.initial_capital,
            'final_value': final_value,
            'total_return': total_return,
            'total_trades': self.portfolio.total_trades,
            'total_commission': self.portfolio.total_commission,
            'total_slippage': self.portfolio.total_slippage,
            'equity_curve': equity_df,
            'transactions': transactions_df,
            'positions': positions_df,
            'current_prices': self.current_prices
        }
        
        # 计算年化收益率
        if not equity_df.empty and len(equity_df) > 1:
            days = (equity_df.index[-1] - equity_df.index[0]).days
            if days > 0:
                annual_return = (1 + total_return) ** (365 / days) - 1
                results['annual_return'] = annual_return
        
        # 计算最大回撤
        if not equity_df.empty:
            cum_returns = equity_df['cum_returns']
            running_max = cum_returns.expanding().max()
            drawdown = (cum_returns - running_max) / (1 + running_max)
            max_drawdown = drawdown.min()
            results['max_drawdown'] = max_drawdown
        
        # 计算夏普比率
        if not equity_df.empty and 'returns' in equity_df.columns:
            returns = equity_df['returns'].dropna()
            if len(returns) > 0 and returns.std() > 0:
                sharpe_ratio = returns.mean() / returns.std() * np.sqrt(252)
                results['sharpe_ratio'] = sharpe_ratio
        
        return results
    
    def get_portfolio_summary(self) -> Dict:
        """获取投资组合摘要"""
        return {
            'current_capital': self.portfolio.current_capital,
            'positions': dict(self.portfolio.positions),
            'avg_prices': dict(self.portfolio.avg_price),
            'total_trades': self.portfolio.total_trades,
            'total_commission': self.portfolio.total_commission,
            'total_slippage': self.portfolio.total_slippage
        }
    
    def save_results(self, filepath: str, results: Dict):
        """
        保存回测结果
        
        Args:
            filepath: 保存路径
            results: 回测结果
        """
        path = Path(filepath)
        path.parent.mkdir(parents=True, exist_ok=True)
        
        # 保存主要指标为JSON
        metrics = {
            k: v for k, v in results.items() 
            if not isinstance(v, pd.DataFrame)
        }
        
        with open(path, 'w') as f:
            json.dump(metrics, f, indent=4, default=str)
        
        # 保存DataFrame为CSV
        if 'equity_curve' in results and not results['equity_curve'].empty:
            results['equity_curve'].to_csv(path.parent / 'equity_curve.csv')
        
        if 'transactions' in results and not results['transactions'].empty:
            results['transactions'].to_csv(path.parent / 'transactions.csv')
        
        self.logger.info(f"回测结果已保存至: {filepath}")


# ==========================================
# 🔧 辅助函数
# ==========================================

def validate_backtest_data(data: pd.DataFrame) -> bool:
    """
    验证回测数据格式
    
    Args:
        data: 待验证的数据
        
    Returns:
        是否有效
    """
    required_columns = ['open', 'high', 'low', 'close', 'volume']
    
    # 检查必要列
    for col in required_columns:
        if col not in data.columns:
            print(f"❌ 缺少必要列: {col}")
            return False
    
    # 检查数据类型
    if not pd.api.types.is_datetime64_any_dtype(data.index):
        print("❌ 索引必须是日期时间类型")
        return False
    
    # 检查数据完整性
    if data.isnull().any().any():
        print("⚠️ 数据包含缺失值")
    
    return True


# ==========================================
# 📊 测试和示例
# ==========================================

if __name__ == "__main__":
    print("回测引擎模块测试")
    print("=" * 60)
    
    # 生成测试数据
    dates = pd.date_range('2023-01-01', '2023-12-31', freq='D')
    test_data = pd.DataFrame({
        'open': np.random.randn(len(dates)).cumsum() + 100,
        'high': np.random.randn(len(dates)).cumsum() + 101,
        'low': np.random.randn(len(dates)).cumsum() + 99,
        'close': np.random.randn(len(dates)).cumsum() + 100,
        'volume': np.random.randint(1000000, 10000000, len(dates))
    }, index=dates)
    
    # 验证数据
    if validate_backtest_data(test_data):
        print("✅ 测试数据验证通过")
    
    # 创建回测引擎
    engine = BacktestEngine(
        initial_capital=1000000,
        commission=0.002,
        slippage=0.001
    )
    
    print(f"✅ 回测引擎创建成功")
    print(f"   初始资金: {engine.initial_capital:,.2f}")
    print(f"   手续费率: {engine.commission_rate:.2%}")
    print(f"   滑点率: {engine.slippage_rate:.2%}")
    
    print("\n📊 回测引擎模块加载完成！")