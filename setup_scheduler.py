#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
定时任务配置脚本
==============

配置优矿数据的每日自动更新定时任务

支持的调度方式：
1. crontab (Linux/macOS)
2. Windows任务计划程序
3. Python APScheduler

Author: QuantTrader Team
Date: 2025-08-31
"""

import sys
import os
import platform
from pathlib import Path
import subprocess

def detect_system():
    """检测操作系统"""
    system = platform.system().lower()
    print(f"🖥️ 检测到操作系统: {system}")
    return system

def setup_crontab():
    """配置Linux/macOS crontab定时任务"""
    print("⚙️ 配置crontab定时任务...")
    
    # 获取当前脚本的绝对路径
    current_dir = Path(__file__).parent.absolute()
    python_path = sys.executable
    script_path = current_dir / "daily_update_uqer.py"
    log_path = current_dir / "logs" / "cron.log"
    
    # crontab条目 - 每个交易日早上6点执行
    cron_entry = f"0 6 * * 1-5 cd {current_dir} && {python_path} {script_path} >> {log_path} 2>&1"
    
    print(f"📝 crontab条目:")
    print(f"   {cron_entry}")
    
    try:
        # 获取当前crontab
        result = subprocess.run(['crontab', '-l'], capture_output=True, text=True)
        current_crontab = result.stdout if result.returncode == 0 else ""
        
        # 检查是否已存在相同任务
        if "daily_update_uqer.py" in current_crontab:
            print("⚠️ 检测到已存在优矿更新任务，是否替换？")
            choice = input("请输入 y/n: ").strip().lower()
            if choice != 'y':
                print("❌ 用户取消操作")
                return False
        
        # 添加新的crontab条目
        new_crontab = current_crontab.strip()
        if new_crontab:
            new_crontab += "\n"
        new_crontab += cron_entry + "\n"
        
        # 写入临时文件
        temp_cron_file = "/tmp/temp_crontab"
        with open(temp_cron_file, 'w') as f:
            f.write(new_crontab)
        
        # 应用新的crontab
        result = subprocess.run(['crontab', temp_cron_file], capture_output=True, text=True)
        
        if result.returncode == 0:
            print("✅ crontab定时任务配置成功")
            print("📅 任务将在每个交易日（周一到周五）早上6点执行")
            
            # 清理临时文件
            os.remove(temp_cron_file)
            return True
        else:
            print(f"❌ crontab配置失败: {result.stderr}")
            return False
            
    except FileNotFoundError:
        print("❌ crontab命令不可用")
        return False
    except Exception as e:
        print(f"❌ 配置crontab时出错: {e}")
        return False

def setup_windows_task():
    """配置Windows任务计划程序"""
    print("⚙️ 配置Windows任务计划程序...")
    
    current_dir = Path(__file__).parent.absolute()
    python_path = sys.executable
    script_path = current_dir / "daily_update_uqer.py"
    
    # Windows任务计划程序命令
    task_name = "UqerDataUpdate"
    
    # 删除可能存在的旧任务
    subprocess.run(['schtasks', '/delete', '/tn', task_name, '/f'], 
                   capture_output=True, text=True)
    
    # 创建新任务 - 每个工作日早上6点执行
    cmd = [
        'schtasks', '/create', '/tn', task_name,
        '/tr', f'"{python_path}" "{script_path}"',
        '/sc', 'weekly',
        '/d', 'MON,TUE,WED,THU,FRI',
        '/st', '06:00',
        '/f'
    ]
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode == 0:
            print("✅ Windows任务计划程序配置成功")
            print("📅 任务将在每个工作日早上6点执行")
            print(f"🎯 任务名称: {task_name}")
            return True
        else:
            print(f"❌ 任务计划程序配置失败: {result.stderr}")
            return False
            
    except FileNotFoundError:
        print("❌ schtasks命令不可用")
        return False
    except Exception as e:
        print(f"❌ 配置任务计划程序时出错: {e}")
        return False

def setup_python_scheduler():
    """配置Python APScheduler后台服务"""
    print("⚙️ 配置Python APScheduler服务...")
    
    scheduler_script = """#!/usr/bin/env python3
# -*- coding: utf-8 -*-
\"\"\"
优矿数据更新调度服务
==================

使用APScheduler实现的后台调度服务
\"\"\"

import sys
import os
from pathlib import Path
import logging
from datetime import datetime
import signal
import time

# 添加项目路径
current_dir = Path(__file__).parent.absolute()
sys.path.insert(0, str(current_dir))

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from daily_update_uqer import DailyUqerUpdater

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/scheduler.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class UqerSchedulerService:
    \"\"\"优矿数据调度服务\"\"\"
    
    def __init__(self):
        self.scheduler = BlockingScheduler()
        self.running = False
        
        # 注册信号处理
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
    
    def signal_handler(self, signum, frame):
        \"\"\"信号处理器\"\"\"
        logger.info(f"收到信号 {signum}，正在停止服务...")
        self.stop()
    
    def update_job(self):
        \"\"\"更新任务\"\"\"
        logger.info("🚀 开始执行优矿数据更新任务")
        try:
            updater = DailyUqerUpdater()
            success = updater.run()
            
            if success:
                logger.info("✅ 更新任务执行成功")
            else:
                logger.error("❌ 更新任务执行失败")
                
        except Exception as e:
            logger.error(f"❌ 更新任务异常: {e}", exc_info=True)
    
    def start(self):
        \"\"\"启动调度服务\"\"\"
        try:
            # 添加定时任务 - 每个工作日早上6点
            self.scheduler.add_job(
                self.update_job,
                trigger=CronTrigger(
                    day_of_week='mon-fri',  # 周一到周五
                    hour=6,
                    minute=0
                ),
                id='uqer_daily_update',
                name='优矿数据每日更新',
                max_instances=1
            )
            
            logger.info("📅 调度服务启动，任务计划:")
            for job in self.scheduler.get_jobs():
                logger.info(f"   {job.name}: {job.trigger}")
            
            self.running = True
            logger.info("🎯 调度服务正在运行，按Ctrl+C停止")
            self.scheduler.start()
            
        except Exception as e:
            logger.error(f"❌ 调度服务启动失败: {e}", exc_info=True)
    
    def stop(self):
        \"\"\"停止调度服务\"\"\"
        if self.running:
            self.scheduler.shutdown()
            self.running = False
            logger.info("🛑 调度服务已停止")

def main():
    \"\"\"主函数\"\"\"
    # 创建必要目录
    os.makedirs('logs', exist_ok=True)
    
    service = UqerSchedulerService()
    service.start()

if __name__ == "__main__":
    main()
"""
    
    # 保存调度服务脚本
    scheduler_file = Path("uqer_scheduler_service.py")
    with open(scheduler_file, 'w', encoding='utf-8') as f:
        f.write(scheduler_script)
    
    print(f"✅ 调度服务脚本已创建: {scheduler_file}")
    
    # 检查是否需要安装APScheduler
    try:
        import apscheduler
        print("✅ APScheduler已安装")
    except ImportError:
        print("📦 需要安装APScheduler...")
        try:
            subprocess.run([sys.executable, '-m', 'pip', 'install', 'apscheduler'], check=True)
            print("✅ APScheduler安装成功")
        except subprocess.CalledProcessError:
            print("❌ APScheduler安装失败")
            return False
    
    print("🎯 调度服务配置完成")
    print("💡 运行方式:")
    print(f"   python {scheduler_file}")
    print("   或者作为后台服务运行")
    
    return True

def create_service_scripts():
    """创建服务启动脚本"""
    print("📝 创建服务启动脚本...")
    
    # Linux/macOS启动脚本
    start_script_unix = """#!/bin/bash
# 优矿数据更新调度服务启动脚本

cd "$(dirname "$0")"
echo "🚀 启动优矿数据调度服务..."

# 激活虚拟环境（如果存在）
if [ -d ".venv" ]; then
    source .venv/bin/activate
    echo "✅ 虚拟环境已激活"
fi

# 启动调度服务
python uqer_scheduler_service.py
"""
    
    with open("start_uqer_scheduler.sh", 'w') as f:
        f.write(start_script_unix)
    
    # 设置执行权限
    os.chmod("start_uqer_scheduler.sh", 0o755)
    
    # Windows启动脚本
    start_script_win = """@echo off
REM 优矿数据更新调度服务启动脚本

cd /d "%~dp0"
echo 🚀 启动优矿数据调度服务...

REM 激活虚拟环境（如果存在）
if exist ".venv\\Scripts\\activate.bat" (
    call .venv\\Scripts\\activate.bat
    echo ✅ 虚拟环境已激活
)

REM 启动调度服务
python uqer_scheduler_service.py
pause
"""
    
    with open("start_uqer_scheduler.bat", 'w') as f:
        f.write(start_script_win)
    
    print("✅ 启动脚本已创建:")
    print("   Linux/macOS: start_uqer_scheduler.sh")
    print("   Windows: start_uqer_scheduler.bat")

def main():
    """主函数"""
    print("=" * 60)
    print("⚙️ 优矿数据自动更新定时任务配置")
    print("=" * 60)
    
    system = detect_system()
    
    print("\n📋 可用的调度方式:")
    print("1. 系统定时任务（推荐）")
    print("2. Python调度服务")
    print("3. 手动配置")
    
    while True:
        choice = input("\n请选择配置方式 (1-3): ").strip()
        
        if choice == "1":
            # 系统定时任务
            if system in ["linux", "darwin"]:  # Linux或macOS
                success = setup_crontab()
            elif system == "windows":
                success = setup_windows_task()
            else:
                print(f"❌ 不支持的操作系统: {system}")
                success = False
            
            if success:
                print("\n🎉 系统定时任务配置成功！")
                print("💡 数据将在每个交易日早上6点自动更新")
            break
            
        elif choice == "2":
            # Python调度服务
            success = setup_python_scheduler()
            if success:
                create_service_scripts()
                print("\n🎉 Python调度服务配置成功！")
                print("💡 运行 start_uqer_scheduler.sh (或.bat) 启动服务")
            break
            
        elif choice == "3":
            # 手动配置
            print("\n📖 手动配置说明:")
            print("1. 每日执行: python daily_update_uqer.py")
            print("2. 建议时间: 每个交易日早上6-8点")
            print("3. 确保优矿API Token已配置")
            print("4. 检查logs目录查看运行日志")
            break
            
        else:
            print("❌ 无效选择，请输入1-3")
    
    print("\n" + "=" * 60)
    print("✅ 定时任务配置完成")
    print("📝 相关文件:")
    print("   - download_uqer_data.py: 全量数据下载")
    print("   - daily_update_uqer.py: 每日增量更新")
    print("   - uqer_setup_guide.md: 配置指南")
    print("=" * 60)

if __name__ == "__main__":
    main()