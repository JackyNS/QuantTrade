#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
交易管理器 - 统一的交易执行和管理系统
===================================

功能:
1. 策略信号执行
2. 仓位管理
3. 风险控制
4. 交易记录

Author: QuantTrade Team
"""

import sys
import os
from pathlib import Path

# 添加项目路径
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import logging
import json

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class TradingManager:
    """交易管理器"""
    
    def __init__(self, initial_capital: float = 1000000):
        """
        初始化交易管理器
        
        Args:
            initial_capital: 初始资金
        """
        self.initial_capital = initial_capital
        self.current_capital = initial_capital
        self.positions = {}  # 当前持仓
        self.trade_history = []  # 交易历史
        self.daily_pnl = []  # 每日损益
        
        # 风控参数
        self.max_position_ratio = 0.1  # 单只股票最大仓位比例
        self.max_total_position = 0.95  # 最大总仓位比例
        self.stop_loss_ratio = 0.05    # 止损比例
        self.take_profit_ratio = 0.15  # 止盈比例
        
        logger.info(f"交易管理器初始化完成，初始资金: {initial_capital:,.2f}")
    
    def execute_signal(self, signal: Dict) -> Dict:
        """
        执行交易信号
        
        Args:
            signal: 交易信号字典
                {
                    'symbol': '股票代码',
                    'action': 'buy/sell',
                    'quantity': 数量,
                    'price': 价格,
                    'timestamp': 时间戳,
                    'strategy': '策略名称'
                }
        
        Returns:
            执行结果
        """
        try:
            symbol = signal['symbol']
            action = signal['action']
            quantity = signal['quantity']
            price = signal['price']
            
            # 风控检查
            if not self._risk_check(symbol, action, quantity, price):
                return {
                    'success': False,
                    'reason': 'Risk control failed',
                    'signal': signal
                }
            
            # 执行交易
            if action == 'buy':
                return self._execute_buy(symbol, quantity, price, signal)
            elif action == 'sell':
                return self._execute_sell(symbol, quantity, price, signal)
            else:
                logger.error(f"Unknown action: {action}")
                return {
                    'success': False,
                    'reason': f'Unknown action: {action}',
                    'signal': signal
                }
                
        except Exception as e:
            logger.error(f"执行交易信号时出错: {e}")
            return {
                'success': False,
                'reason': str(e),
                'signal': signal
            }
    
    def _risk_check(self, symbol: str, action: str, quantity: int, price: float) -> bool:
        """风险控制检查"""
        
        # 检查资金充足性
        if action == 'buy':
            required_capital = quantity * price
            available_capital = self.current_capital * (1 - self._get_total_position_ratio())
            
            if required_capital > available_capital:
                logger.warning(f"资金不足: 需要 {required_capital:,.2f}, 可用 {available_capital:,.2f}")
                return False
            
            # 检查单只股票仓位比例
            current_position_value = self._get_position_value(symbol)
            new_position_value = current_position_value + required_capital
            position_ratio = new_position_value / self.current_capital
            
            if position_ratio > self.max_position_ratio:
                logger.warning(f"单只股票仓位过高: {symbol} {position_ratio:.2%} > {self.max_position_ratio:.2%}")
                return False
        
        elif action == 'sell':
            # 检查持仓是否充足
            current_quantity = self.positions.get(symbol, {}).get('quantity', 0)
            if quantity > current_quantity:
                logger.warning(f"持仓不足: {symbol} 需要卖出 {quantity}, 当前持仓 {current_quantity}")
                return False
        
        return True
    
    def _execute_buy(self, symbol: str, quantity: int, price: float, signal: Dict) -> Dict:
        """执行买入操作"""
        
        cost = quantity * price
        commission = cost * 0.0003  # 手续费0.03%
        total_cost = cost + commission
        
        # 更新仓位
        if symbol in self.positions:
            # 已有持仓，计算平均成本
            current_pos = self.positions[symbol]
            total_quantity = current_pos['quantity'] + quantity
            total_value = current_pos['quantity'] * current_pos['avg_price'] + cost
            avg_price = total_value / total_quantity
            
            self.positions[symbol] = {
                'quantity': total_quantity,
                'avg_price': avg_price,
                'market_price': price,
                'last_update': datetime.now()
            }
        else:
            # 新开仓位
            self.positions[symbol] = {
                'quantity': quantity,
                'avg_price': price,
                'market_price': price,
                'last_update': datetime.now()
            }
        
        # 更新资金
        self.current_capital -= total_cost
        
        # 记录交易
        trade_record = {
            'timestamp': signal.get('timestamp', datetime.now()),
            'symbol': symbol,
            'action': 'buy',
            'quantity': quantity,
            'price': price,
            'cost': cost,
            'commission': commission,
            'strategy': signal.get('strategy', 'unknown'),
            'remaining_capital': self.current_capital
        }
        
        self.trade_history.append(trade_record)
        
        logger.info(f"买入成功: {symbol} {quantity}股 @ {price:.2f}, 总成本: {total_cost:,.2f}")
        
        return {
            'success': True,
            'trade': trade_record,
            'position': self.positions[symbol].copy()
        }
    
    def _execute_sell(self, symbol: str, quantity: int, price: float, signal: Dict) -> Dict:
        """执行卖出操作"""
        
        if symbol not in self.positions:
            return {
                'success': False,
                'reason': f'No position for {symbol}',
                'signal': signal
            }
        
        revenue = quantity * price
        commission = revenue * 0.0003  # 手续费0.03%
        stamp_tax = revenue * 0.001    # 印花税0.1%
        total_revenue = revenue - commission - stamp_tax
        
        # 计算盈亏
        avg_cost = self.positions[symbol]['avg_price']
        profit = (price - avg_cost) * quantity
        
        # 更新仓位
        current_pos = self.positions[symbol]
        remaining_quantity = current_pos['quantity'] - quantity
        
        if remaining_quantity == 0:
            # 清仓
            del self.positions[symbol]
        else:
            # 部分卖出
            self.positions[symbol]['quantity'] = remaining_quantity
            self.positions[symbol]['last_update'] = datetime.now()
        
        # 更新资金
        self.current_capital += total_revenue
        
        # 记录交易
        trade_record = {
            'timestamp': signal.get('timestamp', datetime.now()),
            'symbol': symbol,
            'action': 'sell',
            'quantity': quantity,
            'price': price,
            'revenue': revenue,
            'commission': commission,
            'stamp_tax': stamp_tax,
            'profit': profit,
            'strategy': signal.get('strategy', 'unknown'),
            'remaining_capital': self.current_capital
        }
        
        self.trade_history.append(trade_record)
        
        logger.info(f"卖出成功: {symbol} {quantity}股 @ {price:.2f}, 盈亏: {profit:+.2f}")
        
        return {
            'success': True,
            'trade': trade_record,
            'profit': profit
        }
    
    def _get_total_position_ratio(self) -> float:
        """获取总仓位比例"""
        if not self.positions:
            return 0.0
        
        total_position_value = sum(
            pos['quantity'] * pos['market_price'] 
            for pos in self.positions.values()
        )
        
        return total_position_value / self.current_capital
    
    def _get_position_value(self, symbol: str) -> float:
        """获取单只股票仓位价值"""
        if symbol not in self.positions:
            return 0.0
        
        pos = self.positions[symbol]
        return pos['quantity'] * pos['market_price']
    
    def update_market_prices(self, price_data: Dict[str, float]):
        """更新市场价格"""
        for symbol, price in price_data.items():
            if symbol in self.positions:
                self.positions[symbol]['market_price'] = price
                self.positions[symbol]['last_update'] = datetime.now()
    
    def get_portfolio_status(self) -> Dict:
        """获取投资组合状态"""
        total_position_value = 0
        total_profit = 0
        
        positions_detail = {}
        
        for symbol, pos in self.positions.items():
            current_value = pos['quantity'] * pos['market_price']
            cost_value = pos['quantity'] * pos['avg_price']
            profit = current_value - cost_value
            profit_ratio = profit / cost_value if cost_value > 0 else 0
            
            positions_detail[symbol] = {
                'quantity': pos['quantity'],
                'avg_price': pos['avg_price'],
                'market_price': pos['market_price'],
                'current_value': current_value,
                'cost_value': cost_value,
                'profit': profit,
                'profit_ratio': profit_ratio,
                'position_ratio': current_value / self.current_capital
            }
            
            total_position_value += current_value
            total_profit += profit
        
        total_value = self.current_capital + total_position_value
        total_return = (total_value - self.initial_capital) / self.initial_capital
        
        return {
            'timestamp': datetime.now().isoformat(),
            'initial_capital': self.initial_capital,
            'current_capital': self.current_capital,
            'total_position_value': total_position_value,
            'total_value': total_value,
            'total_profit': total_profit,
            'total_return': total_return,
            'position_ratio': total_position_value / total_value,
            'positions_count': len(self.positions),
            'positions_detail': positions_detail
        }
    
    def save_status(self, filepath: Optional[str] = None):
        """保存交易状态"""
        if filepath is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filepath = f"outputs/trading/trading_status_{timestamp}.json"
        
        # 确保目录存在
        Path(filepath).parent.mkdir(parents=True, exist_ok=True)
        
        status = {
            'portfolio': self.get_portfolio_status(),
            'trade_history': self.trade_history,
            'positions': {
                symbol: {
                    **pos,
                    'last_update': pos['last_update'].isoformat()
                }
                for symbol, pos in self.positions.items()
            }
        }
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(status, f, indent=2, ensure_ascii=False, default=str)
        
        logger.info(f"交易状态已保存: {filepath}")
        return filepath

def main():
    """主函数 - 交易管理器测试"""
    import argparse
    
    parser = argparse.ArgumentParser(description='交易管理器')
    parser.add_argument('--capital', type=float, default=1000000, help='初始资金')
    parser.add_argument('--test', action='store_true', help='运行测试模式')
    
    args = parser.parse_args()
    
    # 创建交易管理器
    manager = TradingManager(initial_capital=args.capital)
    
    if args.test:
        print("🧪 运行交易管理器测试...")
        
        # 模拟交易信号
        test_signals = [
            {
                'symbol': '000001',
                'action': 'buy',
                'quantity': 1000,
                'price': 15.50,
                'timestamp': datetime.now(),
                'strategy': 'test_strategy'
            },
            {
                'symbol': '000002', 
                'action': 'buy',
                'quantity': 2000,
                'price': 8.30,
                'timestamp': datetime.now(),
                'strategy': 'test_strategy'
            }
        ]
        
        # 执行交易
        for signal in test_signals:
            result = manager.execute_signal(signal)
            print(f"交易结果: {result['success']}")
        
        # 模拟价格变化
        manager.update_market_prices({
            '000001': 16.20,
            '000002': 8.80
        })
        
        # 显示投资组合状态
        status = manager.get_portfolio_status()
        print(f"\n📊 投资组合状态:")
        print(f"总资产: {status['total_value']:,.2f}")
        print(f"总收益率: {status['total_return']:.2%}")
        print(f"持仓数量: {status['positions_count']}")
        
        # 保存状态
        manager.save_status()
        
        return 0
    
    else:
        print("📈 交易管理器已启动")
        print("使用 --test 参数运行测试模式")
        return 0

if __name__ == "__main__":
    exit(main())