#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
交易配置
"""

class TradingConfig:
    """交易相关配置"""
    
    def __init__(self):
        # 资金配置
        self.INITIAL_CAPITAL = 1000000  # 初始资金100万
        
        # 交易成本
        self.COMMISSION = 0.0003  # 手续费 万3
        self.SLIPPAGE = 0.001    # 滑点 千1
        self.TAX = 0.001         # 印花税 千1（卖出时）
        
        # 仓位控制
        self.MAX_POSITION = 0.95   # 最大仓位 95%
        self.MAX_STOCKS = 20       # 最大持股数 20
        self.SINGLE_POSITION = 0.05 # 单股最大仓位 5%
        
        # 风险控制
        self.STOP_LOSS = 0.08      # 止损线 8%
        self.TAKE_PROFIT = 0.30    # 止盈线 30%
        self.MAX_DRAWDOWN = 0.15   # 最大回撤 15%
