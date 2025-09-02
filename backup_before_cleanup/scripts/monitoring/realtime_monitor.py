#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
实时监控脚本
============

监控市场和策略的实时状态

Author: QuantTrader Team
Date: 2025-08-31
"""

from typing import Dict

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import pandas as pd
import time
from datetime import datetime
import threading
import queue
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RealtimeMonitor:
    """实时监控器"""
    
    def __init__(self):
        self.running = False
        self.monitor_queue = queue.Queue()
        self.watchlist = []
        self.alerts = []
        
    def add_to_watchlist(self, symbol: str, conditions: Dict):
        """添加到监控列表"""
        self.watchlist.append({
            'symbol': symbol,
            'conditions': conditions,
            'last_check': None,
            'triggered': False
        })
        logger.info(f"添加 {symbol} 到监控列表")
    
    def start_monitoring(self, interval: int = 5):
        """启动监控"""
        self.running = True
        
        # 启动监控线程
        monitor_thread = threading.Thread(
            target=self._monitor_loop,
            args=(interval,),
            daemon=True
        )
        monitor_thread.start()
        
        # 启动告警处理线程
        alert_thread = threading.Thread(
            target=self._alert_handler,
            daemon=True
        )
        alert_thread.start()
        
        logger.info(f"实时监控已启动，刷新间隔: {interval}秒")
    
    def _monitor_loop(self, interval: int):
        """监控循环"""
        while self.running:
            try:
                for item in self.watchlist:
                    if not item['triggered']:
                        # 获取实时数据（这里用模拟数据）
                        realtime_data = self._get_realtime_data(item['symbol'])
                        
                        # 检查条件
                        if self._check_conditions(realtime_data, item['conditions']):
                            item['triggered'] = True
                            self.monitor_queue.put({
                                'type': 'alert',
                                'symbol': item['symbol'],
                                'data': realtime_data,
                                'conditions': item['conditions'],
                                'timestamp': datetime.now()
                            })
                    
                    item['last_check'] = datetime.now()
                
                time.sleep(interval)
                
            except Exception as e:
                logger.error(f"监控错误: {e}")
                time.sleep(interval)
    
    def _get_realtime_data(self, symbol: str) -> Dict:
        """获取实时数据（模拟）"""
        import random
        
        return {
            'symbol': symbol,
            'price': 10 + random.random() * 2,
            'volume': random.randint(1000000, 5000000),
            'change': random.uniform(-0.05, 0.05),
            'bid': 10 + random.random() * 2,
            'ask': 10 + random.random() * 2,
            'timestamp': datetime.now()
        }
    
    def _check_conditions(self, data: Dict, conditions: Dict) -> bool:
        """检查触发条件"""
        for key, condition in conditions.items():
            if key in data:
                value = data[key]
                
                if 'min' in condition and value < condition['min']:
                    return False
                if 'max' in condition and value > condition['max']:
                    return False
                if 'equal' in condition and value != condition['equal']:
                    return False
        
        return True
    
    def _alert_handler(self):
        """告警处理"""
        while self.running:
            try:
                alert = self.monitor_queue.get(timeout=1)
                
                if alert['type'] == 'alert':
                    self._process_alert(alert)
                    
            except queue.Empty:
                continue
            except Exception as e:
                logger.error(f"告警处理错误: {e}")
    
    def _process_alert(self, alert: Dict):
        """处理告警"""
        logger.warning(f"🔔 触发告警: {alert['symbol']} - {alert['conditions']}")
        
        # 保存告警记录
        self.alerts.append(alert)
        
        # 保存到文件
        alert_file = f"./data/alerts/realtime_{datetime.now().strftime('%Y%m%d')}.json"
        
        try:
            os.makedirs(os.path.dirname(alert_file), exist_ok=True)
            
            with open(alert_file, 'a') as f:
                json.dump({
                    'symbol': alert['symbol'],
                    'conditions': alert['conditions'],
                    'timestamp': alert['timestamp'].isoformat(),
                    'data': {k: v if not isinstance(v, datetime) else v.isoformat() 
                            for k, v in alert['data'].items()}
                }, f)
                f.write('\n')
        except Exception as e:
            logger.error(f"保存告警失败: {e}")
    
    def stop_monitoring(self):
        """停止监控"""
        self.running = False
        logger.info("实时监控已停止")
    
    def get_status(self) -> Dict:
        """获取监控状态"""
        return {
            'running': self.running,
            'watchlist_count': len(self.watchlist),
            'alerts_count': len(self.alerts),
            'last_alerts': self.alerts[-5:] if self.alerts else []
        }

def main():
    monitor = RealtimeMonitor()
    
    # 添加监控项
    monitor.add_to_watchlist('000001', {
        'price': {'min': 10.5},
        'volume': {'min': 2000000}
    })
    
    monitor.add_to_watchlist('000002', {
        'change': {'min': 0.03}
    })
    
    # 启动监控
    monitor.start_monitoring(interval=3)
    
    try:
        # 保持运行
        while True:
            time.sleep(10)
            status = monitor.get_status()
            logger.info(f"监控状态: {status['watchlist_count']} 个监控项, {status['alerts_count']} 个告警")
            
    except KeyboardInterrupt:
        monitor.stop_monitoring()
        logger.info("程序退出")

if __name__ == "__main__":
    main()