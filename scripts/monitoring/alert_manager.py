#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
告警管理脚本
============

管理和发送各类告警通知

Author: QuantTrader Team
Date: 2025-08-31
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import json
from datetime import datetime, timedelta
from typing import Dict, List
import smtplib
from email.mime.text import MIMEText
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AlertManager:
    """告警管理器"""
    
    def __init__(self):
        self.alert_rules = []
        self.alert_history = []
        self.notification_channels = []
        
    def add_alert_rule(self, rule: Dict):
        """添加告警规则"""
        rule['id'] = len(self.alert_rules) + 1
        rule['created_at'] = datetime.now()
        rule['enabled'] = True
        self.alert_rules.append(rule)
        
        logger.info(f"添加告警规则: {rule['name']}")
    
    def check_alert_conditions(self, data: Dict) -> List[Dict]:
        """检查告警条件"""
        triggered_alerts = []
        
        for rule in self.alert_rules:
            if not rule['enabled']:
                continue
            
            if self._evaluate_rule(rule, data):
                alert = {
                    'rule_id': rule['id'],
                    'rule_name': rule['name'],
                    'severity': rule.get('severity', 'info'),
                    'message': self._format_message(rule, data),
                    'timestamp': datetime.now(),
                    'data': data
                }
                
                triggered_alerts.append(alert)
                self.alert_history.append(alert)
        
        return triggered_alerts
    
    def _evaluate_rule(self, rule: Dict, data: Dict) -> bool:
        """评估规则条件"""
        condition_type = rule.get('condition_type', 'threshold')
        
        if condition_type == 'threshold':
            metric = rule['metric']
            operator = rule['operator']
            threshold = rule['threshold']
            
            if metric not in data:
                return False
            
            value = data[metric]
            
            if operator == '>':
                return value > threshold
            elif operator == '<':
                return value < threshold
            elif operator == '>=':
                return value >= threshold
            elif operator == '<=':
                return value <= threshold
            elif operator == '==':
                return value == threshold
            
        elif condition_type == 'change':
            # 检查变化率
            metric = rule['metric']
            change_threshold = rule['change_threshold']
            
            if f"{metric}_change" in data:
                return abs(data[f"{metric}_change"]) > change_threshold
        
        return False
    
    def _format_message(self, rule: Dict, data: Dict) -> str:
        """格式化告警消息"""
        template = rule.get('message_template', '告警: {rule_name}')
        
        return template.format(
            rule_name=rule['name'],
            **data
        )
    
    def send_notifications(self, alerts: List[Dict]):
        """发送通知"""
        for alert in alerts:
            for channel in self.notification_channels:
                try:
                    if channel['type'] == 'console':
                        self._send_console_notification(alert)
                    elif channel['type'] == 'file':
                        self._send_file_notification(alert)
                    elif channel['type'] == 'email':
                        self._send_email_notification(alert, channel['config'])
                except Exception as e:
                    logger.error(f"发送通知失败: {e}")
    
    def _send_console_notification(self, alert: Dict):
        """控制台通知"""
        severity_emoji = {
            'critical': '🔴',
            'warning': '🟡',
            'info': '🔵'
        }
        
        emoji = severity_emoji.get(alert['severity'], '⚪')
        
        logger.warning(f"{emoji} {alert['message']}")
    
    def _send_file_notification(self, alert: Dict):
        """文件通知"""
        alert_file = f"./data/alerts/alerts_{datetime.now().strftime('%Y%m%d')}.json"
        
        os.makedirs(os.path.dirname(alert_file), exist_ok=True)
        
        with open(alert_file, 'a') as f:
            json.dump({
                'timestamp': alert['timestamp'].isoformat(),
                'severity': alert['severity'],
                'message': alert['message'],
                'rule_name': alert['rule_name']
            }, f)
            f.write('\n')
    
    def _send_email_notification(self, alert: Dict, config: Dict):
        """邮件通知（示例，实际使用需要配置）"""
        # 这里仅作示例，实际使用需要配置SMTP服务器
        logger.info(f"邮件通知（模拟）: {alert['message']}")
    
    def get_alert_summary(self, hours: int = 24) -> Dict:
        """获取告警摘要"""
        cutoff_time = datetime.now() - timedelta(hours=hours)
        
        recent_alerts = [
            alert for alert in self.alert_history
            if alert['timestamp'] > cutoff_time
        ]
        
        summary = {
            'total_alerts': len(recent_alerts),
            'by_severity': {},
            'by_rule': {}
        }
        
        for alert in recent_alerts:
            # 按严重程度统计
            severity = alert['severity']
            summary['by_severity'][severity] = summary['by_severity'].get(severity, 0) + 1
            
            # 按规则统计
            rule_name = alert['rule_name']
            summary['by_rule'][rule_name] = summary['by_rule'].get(rule_name, 0) + 1
        
        return summary

def main():
    manager = AlertManager()
    
    # 添加通知渠道
    manager.notification_channels.append({'type': 'console'})
    manager.notification_channels.append({'type': 'file'})
    
    # 添加告警规则
    manager.add_alert_rule({
        'name': '价格异常波动',
        'condition_type': 'threshold',
        'metric': 'price_change',
        'operator': '>',
        'threshold': 0.05,
        'severity': 'warning',
        'message_template': '股票 {symbol} 价格波动超过5%'
    })
    
    # 模拟数据检查
    test_data = {
        'symbol': '000001',
        'price_change': 0.06
    }
    
    alerts = manager.check_alert_conditions(test_data)
    if alerts:
        manager.send_notifications(alerts)
    
    # 显示摘要
    summary = manager.get_alert_summary(24)
    print(f"告警摘要: {summary}")

if __name__ == "__main__":
    main()