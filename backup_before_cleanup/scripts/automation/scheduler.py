#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
任务调度脚本
============

管理和执行定时任务

Author: QuantTrader Team
Date: 2025-08-31
"""

from typing import Dict, List


import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import schedule
import time
from datetime import datetime
import subprocess
import threading
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TaskScheduler:
    """任务调度器"""
    
    def __init__(self):
        self.running = False
        self.tasks = []
        self.scripts_dir = Path(__file__).parent.parent
        
    def add_task(self, name: str, script: str, schedule_time: str, frequency: str = 'daily'):
        """添加定时任务"""
        task = {
            'name': name,
            'script': script,
            'schedule_time': schedule_time,
            'frequency': frequency,
            'last_run': None,
            'status': 'pending'
        }
        
        # 设置调度
        if frequency == 'daily':
            schedule.every().day.at(schedule_time).do(self._run_task, task)
        elif frequency == 'hourly':
            schedule.every().hour.at(schedule_time).do(self._run_task, task)
        elif frequency == 'weekly':
            schedule.every().week.at(schedule_time).do(self._run_task, task)
        
        self.tasks.append(task)
        logger.info(f"添加任务: {name} - {frequency} at {schedule_time}")
    
    def _run_task(self, task: Dict):
        """执行任务"""
        logger.info(f"执行任务: {task['name']}")
        task['last_run'] = datetime.now()
        task['status'] = 'running'
        
        try:
            # 构建脚本路径
            script_path = self.scripts_dir / task['script']
            
            # 执行脚本
            result = subprocess.run(
                [sys.executable, str(script_path)],
                capture_output=True,
                text=True,
                timeout=3600  # 1小时超时
            )
            
            if result.returncode == 0:
                task['status'] = 'success'
                logger.info(f"任务完成: {task['name']}")
            else:
                task['status'] = 'failed'
                logger.error(f"任务失败: {task['name']} - {result.stderr}")
                
        except subprocess.TimeoutExpired:
            task['status'] = 'timeout'
            logger.error(f"任务超时: {task['name']}")
        except Exception as e:
            task['status'] = 'error'
            logger.error(f"任务错误: {task['name']} - {e}")
    
    def setup_default_tasks(self):
        """设置默认任务"""
        
        # 每日数据更新
        self.add_task(
            name="每日数据更新",
            script="data/update_daily.py",
            schedule_time="08:30",
            frequency="daily"
        )
        
        # 每日报告生成
        self.add_task(
            name="每日报告",
            script="reporting/daily_report.py",
            schedule_time="17:00",
            frequency="daily"
        )
        
        # 每周报告
        self.add_task(
            name="周报生成",
            script="reporting/weekly_report.py",
            schedule_time="18:00",
            frequency="weekly"
        )
        
        # 数据清理（每周日）
        self.add_task(
            name="数据清理",
            script="data/data_cleanup.py",
            schedule_time="02:00",
            frequency="weekly"
        )
        
        logger.info("默认任务已设置")
    
    def start(self):
        """启动调度器"""
        self.running = True
        logger.info("任务调度器已启动")
        
        # 在独立线程中运行调度器
        def run_schedule():
            while self.running:
                schedule.run_pending()
                time.sleep(60)  # 每分钟检查一次
        
        schedule_thread = threading.Thread(target=run_schedule, daemon=True)
        schedule_thread.start()
    
    def stop(self):
        """停止调度器"""
        self.running = False
        logger.info("任务调度器已停止")
    
    def get_status(self) -> List[Dict]:
        """获取任务状态"""
        return [
            {
                'name': task['name'],
                'frequency': task['frequency'],
                'last_run': task['last_run'].isoformat() if task['last_run'] else None,
                'status': task['status']
            }
            for task in self.tasks
        ]
    
    def run_immediate(self, task_name: str):
        """立即执行指定任务"""
        for task in self.tasks:
            if task['name'] == task_name:
                logger.info(f"立即执行任务: {task_name}")
                self._run_task(task)
                return True
        
        logger.error(f"未找到任务: {task_name}")
        return False

def main():
    scheduler = TaskScheduler()
    
    # 设置默认任务
    scheduler.setup_default_tasks()
    
    # 启动调度器
    scheduler.start()
    
    try:
        logger.info("调度器运行中，按Ctrl+C退出")
        while True:
            time.sleep(60)
            
            # 定期显示状态
            status = scheduler.get_status()
            logger.info(f"当前任务数: {len(status)}")
            
    except KeyboardInterrupt:
        scheduler.stop()
        logger.info("调度器已退出")

if __name__ == "__main__":
    main()