#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
策略运行脚本
============

执行交易策略并生成信号

Author: QuantTrader Team
Date: 2025-08-31
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from core.strategy import BaseStrategy
from core.data import DataManager
from core.config import Config
import pandas as pd
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class StrategyRunner:
    """策略运行器"""
    
    def __init__(self, strategy_name="MA_Cross"):
        self.config = Config()
        self.data_manager = DataManager(self.config)
        self.strategy_name = strategy_name
        self.signals = []
        
    def load_strategy(self):
        """加载策略"""
        # 这里可以根据策略名称动态加载不同策略
        if self.strategy_name == "MA_Cross":
            from core.strategy import MACrossStrategy
            return MACrossStrategy()
        elif self.strategy_name == "RSI":
            from core.strategy import RSIStrategy
            return RSIStrategy()
        else:
            logger.error(f"未知策略: {self.strategy_name}")
            return None
    
    def run(self, symbols=None, start_date=None, end_date=None):
        """运行策略"""
        strategy = self.load_strategy()
        if not strategy:
            return
        
        if not symbols:
            symbols = self.data_manager.get_active_stocks()[:10]  # 默认前10只
        
        logger.info(f"运行策略: {self.strategy_name}")
        logger.info(f"股票数量: {len(symbols)}")
        
        all_signals = []
        
        for symbol in symbols:
            try:
                # 获取数据
                data = self.data_manager.get_stock_data(
                    symbol, 
                    start_date or self.config.START_DATE,
                    end_date or self.config.END_DATE
                )
                
                if data is None or data.empty:
                    continue
                
                # 生成信号
                signals = strategy.generate_signals(data)
                
                if not signals.empty:
                    signals['symbol'] = symbol
                    all_signals.append(signals)
                    
            except Exception as e:
                logger.error(f"处理 {symbol} 失败: {e}")
        
        if all_signals:
            # 合并所有信号
            self.signals = pd.concat(all_signals, ignore_index=True)
            
            # 保存信号
            signal_file = f"./data/signals/{self.strategy_name}_{datetime.now().strftime('%Y%m%d')}.csv"
            self.signals.to_csv(signal_file, index=False)
            logger.info(f"信号已保存: {signal_file}")
            
            return self.signals
        else:
            logger.warning("未生成任何信号")
            return pd.DataFrame()

def main():
    import argparse
    parser = argparse.ArgumentParser(description='运行交易策略')
    parser.add_argument('--strategy', default='MA_Cross', help='策略名称')
    parser.add_argument('--symbols', nargs='+', help='股票代码列表')
    
    args = parser.parse_args()
    
    runner = StrategyRunner(args.strategy)
    signals = runner.run(symbols=args.symbols)
    
    if not signals.empty:
        print(f"\n生成 {len(signals)} 个交易信号")
        print(signals.head())

if __name__ == "__main__":
    main()