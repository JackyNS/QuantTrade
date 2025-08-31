#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
筛选监控脚本
============

实时监控符合筛选条件的股票

Author: QuantTrader Team
Date: 2025-08-31
"""

from typing import Dict, List

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from core.screening import StockScreener
from core.data import DataManager
from core.config import Config
import pandas as pd
from datetime import datetime
import time
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ScreeningMonitor:
    """筛选监控器"""
    
    def __init__(self):
        self.config = Config()
        self.data_manager = DataManager(self.config)
        self.screener = StockScreener(self.config)
        self.alert_list = []
        
    def monitor_conditions(self, conditions: Dict, interval: int = 300):
        """监控筛选条件"""
        logger.info(f"开始监控，检查间隔: {interval}秒")
        
        while True:
            try:
                # 执行筛选
                results = self.screener.screen(conditions)
                
                if not results.empty:
                    new_stocks = self._check_new_entries(results)
                    
                    if new_stocks:
                        self._send_alert(new_stocks, conditions)
                        
                # 保存监控日志
                self._save_monitor_log(results)
                
                time.sleep(interval)
                
            except KeyboardInterrupt:
                logger.info("监控已停止")
                break
            except Exception as e:
                logger.error(f"监控错误: {e}")
                time.sleep(interval)
    
    def _check_new_entries(self, results: pd.DataFrame) -> List:
        """检查新出现的股票"""
        current_stocks = results['symbol'].tolist()
        new_stocks = [s for s in current_stocks if s not in self.alert_list]
        self.alert_list.extend(new_stocks)
        
        # 只保留最近100个
        self.alert_list = self.alert_list[-100:]
        
        return new_stocks
    
    def _send_alert(self, stocks: List, conditions: Dict):
        """发送提醒"""
        logger.info(f"🔔 新增符合条件的股票: {stocks}")
        
        alert = {
            'timestamp': datetime.now().isoformat(),
            'stocks': stocks,
            'conditions': conditions
        }
        
        # 保存到文件
        alert_file = f"./data/alerts/screening_{datetime.now().strftime('%Y%m%d')}.json"
        
        try:
            with open(alert_file, 'a') as f:
                json.dump(alert, f)
                f.write('\n')
        except:
            pass
    
    def _save_monitor_log(self, results: pd.DataFrame):
        """保存监控日志"""
        log_entry = {
            'timestamp': datetime.now().isoformat(),
            'matched_count': len(results),
            'top_stocks': results.head(10)['symbol'].tolist() if not results.empty else []
        }
        
        log_file = "./data/monitoring/screening_monitor.json"
        
        try:
            if os.path.exists(log_file):
                with open(log_file, 'r') as f:
                    logs = json.load(f)
            else:
                logs = []
            
            logs.append(log_entry)
            logs = logs[-100:]  # 保留最近100条
            
            with open(log_file, 'w') as f:
                json.dump(logs, f, indent=2)
        except:
            pass

def main():
    monitor = ScreeningMonitor()
    
    # 示例筛选条件
    conditions = {
        'volume_ratio': {'min': 2.0},  # 量比大于2
        'price_change': {'min': 0.03},  # 涨幅大于3%
        'turnover_rate': {'min': 0.05}  # 换手率大于5%
    }
    
    monitor.monitor_conditions(conditions, interval=60)

if __name__ == "__main__":
    main()