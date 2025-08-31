#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""交易配置"""

class TradingConfig:
    def __init__(self):
        self.INITIAL_CAPITAL = 1000000
        self.COMMISSION = 0.0003
        self.SLIPPAGE = 0.001
        self.MAX_POSITION = 0.95
        self.MAX_STOCKS = 20
        self.SINGLE_POSITION = 0.05
        self.STOP_LOSS = 0.08
        self.TAKE_PROFIT = 0.30
