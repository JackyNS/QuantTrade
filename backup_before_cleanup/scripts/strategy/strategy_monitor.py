#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
策略监控脚本
============

实时监控策略运行状态和性能

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
import time
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class StrategyMonitor:
    """策略监控器"""
    
    def __init__(self, strategy_name: str):
        self.strategy_name = strategy_name
        self.monitor_dir = Path("./data/monitoring")
        self.monitor_dir.mkdir(parents=True, exist_ok=True)
        self.alert_thresholds = {
            'max_drawdown': 0.2,
            'daily_loss': 0.05,
            'position_limit': 10
        }
        
    def check_performance(self, portfolio_value: pd.Series) -> Dict:
        """检查策略表现"""
        current_drawdown = self._calculate_current_drawdown(portfolio_value)
        daily_return = portfolio_value.pct_change().iloc[-1]
        
        alerts = []
        
        if current_drawdown < -self.alert_thresholds['max_drawdown']:
            alerts.append(f"回撤超限: {current_drawdown:.2%}")
            
        if daily_return < -self.alert_thresholds['daily_loss']:
            alerts.append(f"日亏损超限: {daily_return:.2%}")
            
        return {
            'timestamp': datetime.now(),
            'current_value': portfolio_value.iloc[-1],
            'current_drawdown': current_drawdown,
            'daily_return': daily_return,
            'alerts': alerts
        }
    
    def _calculate_current_drawdown(self, portfolio_value: pd.Series) -> float:
        """计算当前回撤"""
        peak = portfolio_value.max()
        current = portfolio_value.iloc[-1]
        return (current - peak) / peak
    
    def save_monitor_log(self, status: Dict):
        """保存监控日志"""
        log_file = self.monitor_dir / f"{self.strategy_name}_monitor.json"
        
        if log_file.exists():
            with open(log_file, 'r') as f:
                logs = json.load(f)
        else:
            logs = []
            
        logs.append({
            'timestamp': status['timestamp'].isoformat(),
            'value': status['current_value'],
            'drawdown': status['current_drawdown'],
            'daily_return': status['daily_return'],
            'alerts': status['alerts']
        })
        
        # 只保留最近100条记录
        logs = logs[-100:]
        
        with open(log_file, 'w') as f:
            json.dump(logs, f, indent=2)
            
        if status['alerts']:
            logger.warning(f"策略告警: {status['alerts']}")

def main():
    monitor = StrategyMonitor("MA_Cross")
    logger.info(f"开始监控策略: {monitor.strategy_name}")

if __name__ == "__main__":
    main()