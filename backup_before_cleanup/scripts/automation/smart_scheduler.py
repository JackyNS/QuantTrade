#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
智能调度器 - 自动化任务调度和管理系统
=================================

功能:
1. 定时任务调度
2. 市场时间感知
3. 任务依赖管理
4. 失败重试机制
5. 性能监控

Author: QuantTrade Team
"""

import sys
import os
from pathlib import Path

# 添加项目路径
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import schedule
import time
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Callable, Optional
import logging
import json
from dataclasses import dataclass, asdict
from enum import Enum
import subprocess

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class TaskStatus(Enum):
    """任务状态枚举"""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"

class TaskPriority(Enum):
    """任务优先级"""
    LOW = 1
    NORMAL = 2
    HIGH = 3
    CRITICAL = 4

@dataclass
class TaskResult:
    """任务执行结果"""
    task_id: str
    status: TaskStatus
    start_time: datetime
    end_time: Optional[datetime]
    duration: float
    output: Optional[str]
    error: Optional[str]
    retry_count: int

@dataclass 
class ScheduledTask:
    """调度任务"""
    task_id: str
    name: str
    command: str
    schedule_expr: str  # cron表达式或简单表达式
    priority: TaskPriority
    max_retries: int = 3
    timeout: int = 3600  # 超时时间(秒)
    market_hours_only: bool = False
    dependencies: List[str] = None
    enabled: bool = True
    
    def __post_init__(self):
        if self.dependencies is None:
            self.dependencies = []

class SmartScheduler:
    """智能调度器"""
    
    def __init__(self):
        self.tasks: Dict[str, ScheduledTask] = {}
        self.task_history: List[TaskResult] = []
        self.running_tasks: Dict[str, threading.Thread] = {}
        self.is_running = False
        self.scheduler_thread: Optional[threading.Thread] = None
        
        # 市场交易时间配置 (9:30-11:30, 13:00-15:00)
        self.market_open_morning = (9, 30)
        self.market_close_morning = (11, 30)
        self.market_open_afternoon = (13, 0)
        self.market_close_afternoon = (15, 0)
        
        logger.info("智能调度器初始化完成")
    
    def add_task(self, task: ScheduledTask) -> bool:
        """添加调度任务"""
        try:
            if task.task_id in self.tasks:
                logger.warning(f"任务 {task.task_id} 已存在，将被覆盖")
            
            # 验证调度表达式
            if not self._validate_schedule_expression(task.schedule_expr):
                logger.error(f"无效的调度表达式: {task.schedule_expr}")
                return False
            
            self.tasks[task.task_id] = task
            
            # 注册到调度器
            self._register_task(task)
            
            logger.info(f"添加调度任务: {task.name} ({task.task_id})")
            return True
            
        except Exception as e:
            logger.error(f"添加任务失败: {e}")
            return False
    
    def remove_task(self, task_id: str) -> bool:
        """移除调度任务"""
        if task_id not in self.tasks:
            logger.warning(f"任务 {task_id} 不存在")
            return False
        
        # 如果任务正在运行，先停止
        if task_id in self.running_tasks:
            logger.info(f"正在停止运行中的任务: {task_id}")
            # 这里可以添加优雅停止的逻辑
        
        del self.tasks[task_id]
        logger.info(f"移除调度任务: {task_id}")
        return True
    
    def _validate_schedule_expression(self, expr: str) -> bool:
        """验证调度表达式"""
        # 支持的简单表达式格式
        valid_patterns = [
            'every_minute',
            'every_5_minutes', 
            'every_10_minutes',
            'every_30_minutes',
            'every_hour',
            'market_open',
            'market_close',
            'daily_9_00',
            'daily_15_30',
            'weekdays_only'
        ]
        
        return expr in valid_patterns or expr.startswith('every_') or expr.startswith('daily_')
    
    def _register_task(self, task: ScheduledTask):
        """注册任务到调度器"""
        def job_wrapper():
            if not self.is_market_hours() and task.market_hours_only:
                logger.info(f"跳过非交易时间任务: {task.name}")
                return
            
            if not self._check_dependencies(task):
                logger.warning(f"任务依赖未满足: {task.name}")
                return
            
            self._execute_task(task)
        
        # 根据调度表达式注册任务
        expr = task.schedule_expr
        
        if expr == 'every_minute':
            schedule.every().minute.do(job_wrapper)
        elif expr == 'every_5_minutes':
            schedule.every(5).minutes.do(job_wrapper)
        elif expr == 'every_10_minutes':
            schedule.every(10).minutes.do(job_wrapper)
        elif expr == 'every_30_minutes':
            schedule.every(30).minutes.do(job_wrapper)
        elif expr == 'every_hour':
            schedule.every().hour.do(job_wrapper)
        elif expr == 'market_open':
            schedule.every().day.at("09:30").do(job_wrapper)
        elif expr == 'market_close':
            schedule.every().day.at("15:00").do(job_wrapper)
        elif expr.startswith('daily_'):
            # 解析 daily_HH_MM 格式
            time_part = expr.replace('daily_', '').replace('_', ':')
            schedule.every().day.at(time_part).do(job_wrapper)
        elif expr == 'weekdays_only':
            schedule.every().monday.at("09:00").do(job_wrapper)
            schedule.every().tuesday.at("09:00").do(job_wrapper)
            schedule.every().wednesday.at("09:00").do(job_wrapper)
            schedule.every().thursday.at("09:00").do(job_wrapper)
            schedule.every().friday.at("09:00").do(job_wrapper)
    
    def is_market_hours(self) -> bool:
        """检查是否在交易时间内"""
        now = datetime.now()
        current_time = (now.hour, now.minute)
        
        # 检查是否为工作日
        if now.weekday() > 4:  # 周六周日
            return False
        
        # 检查上午时段
        if (self.market_open_morning <= current_time <= self.market_close_morning):
            return True
        
        # 检查下午时段
        if (self.market_open_afternoon <= current_time <= self.market_close_afternoon):
            return True
        
        return False
    
    def _check_dependencies(self, task: ScheduledTask) -> bool:
        """检查任务依赖"""
        if not task.dependencies:
            return True
        
        # 检查依赖任务是否都成功执行
        for dep_task_id in task.dependencies:
            # 查找最近的依赖任务执行结果
            recent_results = [
                r for r in self.task_history 
                if r.task_id == dep_task_id and 
                r.start_time.date() == datetime.now().date()
            ]
            
            if not recent_results or recent_results[-1].status != TaskStatus.SUCCESS:
                logger.warning(f"依赖任务 {dep_task_id} 未成功执行")
                return False
        
        return True
    
    def _execute_task(self, task: ScheduledTask):
        """执行任务"""
        if task.task_id in self.running_tasks:
            logger.warning(f"任务 {task.task_id} 正在运行中，跳过")
            return
        
        # 创建任务线程
        thread = threading.Thread(
            target=self._run_task_with_retry,
            args=(task,),
            name=f"Task-{task.task_id}"
        )
        
        self.running_tasks[task.task_id] = thread
        thread.start()
    
    def _run_task_with_retry(self, task: ScheduledTask):
        """带重试机制的任务执行"""
        retry_count = 0
        start_time = datetime.now()
        
        while retry_count <= task.max_retries:
            try:
                logger.info(f"执行任务: {task.name} (第 {retry_count + 1} 次)")
                
                # 执行命令
                result = subprocess.run(
                    task.command,
                    shell=True,
                    capture_output=True,
                    text=True,
                    timeout=task.timeout,
                    cwd=str(project_root)
                )
                
                end_time = datetime.now()
                duration = (end_time - start_time).total_seconds()
                
                if result.returncode == 0:
                    # 任务成功
                    task_result = TaskResult(
                        task_id=task.task_id,
                        status=TaskStatus.SUCCESS,
                        start_time=start_time,
                        end_time=end_time,
                        duration=duration,
                        output=result.stdout,
                        error=None,
                        retry_count=retry_count
                    )
                    
                    logger.info(f"任务执行成功: {task.name}")
                    break
                else:
                    # 任务失败，准备重试
                    logger.warning(f"任务执行失败: {task.name}, 错误: {result.stderr}")
                    retry_count += 1
                    
                    if retry_count > task.max_retries:
                        # 重试次数用完，标记为失败
                        task_result = TaskResult(
                            task_id=task.task_id,
                            status=TaskStatus.FAILED,
                            start_time=start_time,
                            end_time=end_time,
                            duration=duration,
                            output=result.stdout,
                            error=result.stderr,
                            retry_count=retry_count - 1
                        )
                        logger.error(f"任务最终失败: {task.name}")
                    else:
                        # 等待后重试
                        wait_time = min(60 * (2 ** retry_count), 300)  # 指数退避，最多5分钟
                        logger.info(f"等待 {wait_time} 秒后重试...")
                        time.sleep(wait_time)
                        continue
                
            except subprocess.TimeoutExpired:
                logger.error(f"任务执行超时: {task.name}")
                task_result = TaskResult(
                    task_id=task.task_id,
                    status=TaskStatus.FAILED,
                    start_time=start_time,
                    end_time=datetime.now(),
                    duration=task.timeout,
                    output=None,
                    error="Task timeout",
                    retry_count=retry_count
                )
                break
                
            except Exception as e:
                logger.error(f"任务执行异常: {task.name}, 错误: {e}")
                task_result = TaskResult(
                    task_id=task.task_id,
                    status=TaskStatus.FAILED,
                    start_time=start_time,
                    end_time=datetime.now(),
                    duration=(datetime.now() - start_time).total_seconds(),
                    output=None,
                    error=str(e),
                    retry_count=retry_count
                )
                break
        
        # 记录任务结果
        self.task_history.append(task_result)
        
        # 清理运行中任务记录
        if task.task_id in self.running_tasks:
            del self.running_tasks[task.task_id]
    
    def start(self):
        """启动调度器"""
        if self.is_running:
            logger.warning("调度器已在运行中")
            return
        
        self.is_running = True
        
        def scheduler_loop():
            logger.info("调度器开始运行...")
            while self.is_running:
                try:
                    schedule.run_pending()
                    time.sleep(1)
                except Exception as e:
                    logger.error(f"调度器运行异常: {e}")
                    time.sleep(10)  # 异常后等待10秒
        
        self.scheduler_thread = threading.Thread(target=scheduler_loop, name="Scheduler")
        self.scheduler_thread.start()
        
        logger.info("智能调度器已启动")
    
    def stop(self):
        """停止调度器"""
        logger.info("正在停止调度器...")
        self.is_running = False
        
        if self.scheduler_thread:
            self.scheduler_thread.join(timeout=10)
        
        # 等待所有运行中的任务完成
        for task_id, thread in self.running_tasks.items():
            logger.info(f"等待任务完成: {task_id}")
            thread.join(timeout=30)
        
        schedule.clear()
        logger.info("调度器已停止")
    
    def get_status(self) -> Dict:
        """获取调度器状态"""
        recent_results = [
            r for r in self.task_history 
            if r.start_time > datetime.now() - timedelta(days=1)
        ]
        
        success_count = sum(1 for r in recent_results if r.status == TaskStatus.SUCCESS)
        failed_count = sum(1 for r in recent_results if r.status == TaskStatus.FAILED)
        
        return {
            'is_running': self.is_running,
            'total_tasks': len(self.tasks),
            'running_tasks': len(self.running_tasks),
            'is_market_hours': self.is_market_hours(),
            'recent_executions': len(recent_results),
            'recent_success': success_count,
            'recent_failed': failed_count,
            'success_rate': success_count / len(recent_results) if recent_results else 0
        }
    
    def save_config(self, filepath: Optional[str] = None):
        """保存调度器配置"""
        if filepath is None:
            filepath = "config/scheduler_config.json"
        
        Path(filepath).parent.mkdir(parents=True, exist_ok=True)
        
        config = {
            'tasks': {
                task_id: asdict(task) 
                for task_id, task in self.tasks.items()
            },
            'last_updated': datetime.now().isoformat()
        }
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(config, f, indent=2, ensure_ascii=False, default=str)
        
        logger.info(f"调度器配置已保存: {filepath}")

def load_default_tasks() -> List[ScheduledTask]:
    """加载默认任务配置"""
    return [
        ScheduledTask(
            task_id="data_update",
            name="数据更新",
            command="python tools/data_download/uqer_connection_manager.py download",
            schedule_expr="daily_8_30",
            priority=TaskPriority.HIGH,
            market_hours_only=False,
            max_retries=3
        ),
        ScheduledTask(
            task_id="strategy_run",
            name="策略执行",
            command="python scripts/trading/trading_manager.py",
            schedule_expr="market_open",
            priority=TaskPriority.CRITICAL,
            market_hours_only=True,
            dependencies=["data_update"],
            max_retries=2
        ),
        ScheduledTask(
            task_id="portfolio_monitor",
            name="投资组合监控",
            command="python scripts/monitoring/realtime_monitor.py",
            schedule_expr="every_10_minutes",
            priority=TaskPriority.NORMAL,
            market_hours_only=True,
            max_retries=1
        ),
        ScheduledTask(
            task_id="daily_report",
            name="日报生成",
            command="python scripts/reporting/daily_report.py",
            schedule_expr="daily_16_00",
            priority=TaskPriority.NORMAL,
            market_hours_only=False,
            dependencies=["strategy_run"],
            max_retries=2
        )
    ]

def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='智能调度器')
    parser.add_argument('--config', type=str, help='配置文件路径')
    parser.add_argument('--daemon', action='store_true', help='后台运行模式')
    parser.add_argument('--status', action='store_true', help='显示状态信息')
    
    args = parser.parse_args()
    
    scheduler = SmartScheduler()
    
    if args.status:
        status = scheduler.get_status()
        print("📊 调度器状态:")
        for key, value in status.items():
            print(f"  {key}: {value}")
        return 0
    
    # 加载默认任务
    default_tasks = load_default_tasks()
    for task in default_tasks:
        scheduler.add_task(task)
    
    # 保存配置
    scheduler.save_config()
    
    try:
        # 启动调度器
        scheduler.start()
        
        if args.daemon:
            # 后台运行模式
            logger.info("后台运行模式，按 Ctrl+C 停止")
            while True:
                time.sleep(60)
                # 定期输出状态
                status = scheduler.get_status()
                logger.info(f"运行状态: {status['running_tasks']} 个任务运行中")
        else:
            # 交互模式
            print("📋 调度器已启动，输入命令:")
            print("  status - 显示状态")
            print("  stop - 停止调度器")
            print("  quit - 退出程序")
            
            while scheduler.is_running:
                try:
                    cmd = input("> ").strip().lower()
                    
                    if cmd == "status":
                        status = scheduler.get_status()
                        print("📊 当前状态:")
                        for key, value in status.items():
                            print(f"  {key}: {value}")
                    
                    elif cmd == "stop":
                        scheduler.stop()
                        break
                    
                    elif cmd in ["quit", "exit"]:
                        break
                    
                    else:
                        print("未知命令")
                        
                except EOFError:
                    break
        
    except KeyboardInterrupt:
        logger.info("收到中断信号，正在停止...")
    
    finally:
        scheduler.stop()
    
    return 0

if __name__ == "__main__":
    exit(main())