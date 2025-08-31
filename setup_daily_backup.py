#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
设置每日自动备份到GitHub
"""

import os
import sys
from pathlib import Path
import subprocess

class DailyBackupSetup:
    """每日备份设置器"""
    
    def __init__(self):
        self.project_root = Path(__file__).parent
        self.script_path = self.project_root / "auto_backup.py"
        
    def create_launchd_plist(self):
        """创建macOS LaunchAgent配置"""
        plist_content = f'''<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.quanttrade.autobackup</string>
    
    <key>Program</key>
    <string>{sys.executable}</string>
    
    <key>ProgramArguments</key>
    <array>
        <string>{sys.executable}</string>
        <string>{self.script_path}</string>
        <string>backup</string>
    </array>
    
    <key>WorkingDirectory</key>
    <string>{self.project_root}</string>
    
    <key>StartCalendarInterval</key>
    <dict>
        <key>Hour</key>
        <integer>18</integer>
        <key>Minute</key>
        <integer>0</integer>
    </dict>
    
    <key>StandardOutPath</key>
    <string>{self.project_root}/backup_output.log</string>
    
    <key>StandardErrorPath</key>
    <string>{self.project_root}/backup_error.log</string>
    
    <key>RunAtLoad</key>
    <false/>
    
    <key>KeepAlive</key>
    <false/>
</dict>
</plist>'''
        
        return plist_content
    
    def create_cron_job(self):
        """创建Linux/Windows cron任务"""
        cron_command = f"0 18 * * * cd {self.project_root} && {sys.executable} {self.script_path} backup >> {self.project_root}/backup_cron.log 2>&1"
        return cron_command
    
    def create_windows_task(self):
        """创建Windows计划任务"""
        bat_content = f'''@echo off
cd /d "{self.project_root}"
"{sys.executable}" "{self.script_path}" backup
'''
        
        bat_file = self.project_root / "daily_backup.bat"
        with open(bat_file, 'w', encoding='utf-8') as f:
            f.write(bat_content)
        
        return bat_file
    
    def setup_macos(self):
        """设置macOS自动备份"""
        print("🍎 设置macOS自动备份...")
        
        # 创建plist文件
        plist_content = self.create_launchd_plist()
        plist_file = Path.home() / "Library/LaunchAgents/com.quanttrade.autobackup.plist"
        
        # 创建目录
        plist_file.parent.mkdir(exist_ok=True)
        
        # 写入文件
        with open(plist_file, 'w', encoding='utf-8') as f:
            f.write(plist_content)
        
        print(f"✅ 已创建LaunchAgent配置: {plist_file}")
        
        # 加载任务
        try:
            subprocess.run(['launchctl', 'load', str(plist_file)], check=True)
            print("✅ LaunchAgent已加载")
            
            # 显示状态
            result = subprocess.run(['launchctl', 'list', 'com.quanttrade.autobackup'], 
                                  capture_output=True, text=True)
            if result.returncode == 0:
                print("✅ 自动备份任务已激活")
            else:
                print("⚠️ 任务可能未正确加载")
                
        except subprocess.CalledProcessError as e:
            print(f"❌ 加载LaunchAgent失败: {e}")
            print("📝 手动加载命令:")
            print(f"   launchctl load {plist_file}")
        
        print(f"\n📋 备份计划: 每天18:00执行")
        print(f"📄 日志文件: {self.project_root}/backup_output.log")
        print(f"🔧 卸载命令: launchctl unload {plist_file}")
    
    def setup_linux(self):
        """设置Linux自动备份"""
        print("🐧 设置Linux自动备份...")
        
        cron_command = self.create_cron_job()
        
        print("📝 请手动添加以下cron任务:")
        print("1. 运行: crontab -e")
        print("2. 添加以下行:")
        print(f"   {cron_command}")
        print("3. 保存退出")
        print(f"\n📋 备份计划: 每天18:00执行")
        print(f"📄 日志文件: {self.project_root}/backup_cron.log")
        
        # 保存到文件
        cron_file = self.project_root / "cron_backup.txt"
        with open(cron_file, 'w', encoding='utf-8') as f:
            f.write(cron_command + '\n')
        print(f"💾 cron命令已保存到: {cron_file}")
    
    def setup_windows(self):
        """设置Windows自动备份"""
        print("🪟 设置Windows自动备份...")
        
        bat_file = self.create_windows_task()
        print(f"✅ 已创建批处理文件: {bat_file}")
        
        print("\n📝 请手动设置Windows计划任务:")
        print("1. 打开 任务计划程序 (Task Scheduler)")
        print("2. 点击 创建基本任务")
        print("3. 设置任务信息:")
        print("   - 名称: QuantTrade 自动备份")
        print("   - 描述: 每日自动备份项目到GitHub")
        print("4. 触发器: 每天 18:00")
        print(f"5. 操作: 启动程序 - {bat_file}")
        print("6. 完成创建")
        
        print(f"\n📋 备份计划: 每天18:00执行")
        print(f"📄 批处理文件: {bat_file}")
    
    def test_backup(self):
        """测试备份功能"""
        print("🧪 测试自动备份...")
        
        try:
            result = subprocess.run([sys.executable, str(self.script_path), 'status'], 
                                  cwd=self.project_root,
                                  capture_output=True, text=True,
                                  timeout=30)
            
            print("📊 测试结果:")
            print(result.stdout)
            
            if result.stderr:
                print("⚠️ 错误信息:")
                print(result.stderr)
            
            if result.returncode == 0:
                print("✅ 备份脚本工作正常")
                return True
            else:
                print("❌ 备份脚本存在问题")
                return False
                
        except Exception as e:
            print(f"❌ 测试失败: {e}")
            return False
    
    def show_instructions(self):
        """显示使用说明"""
        print("📖 自动备份使用说明")
        print("="*50)
        print("🕒 执行时间: 每天18:00")
        print("📋 备份内容: 所有代码变更")
        print("🎯 目标位置: GitHub远程仓库")
        print("\n📝 手动命令:")
        print(f"  python {self.script_path} backup  # 立即备份")
        print(f"  python {self.script_path} status  # 查看状态")
        print("\n📄 日志文件:")
        print(f"  {self.project_root}/backup.log")
        print(f"  {self.project_root}/backup_output.log")

def main():
    """主函数"""
    setup = DailyBackupSetup()
    
    print("⚙️ QuantTrade 每日自动备份设置")
    print("="*50)
    
    # 测试备份功能
    if not setup.test_backup():
        print("❌ 请先解决备份脚本问题")
        return
    
    # 检测操作系统
    import platform
    system = platform.system().lower()
    
    if system == "darwin":  # macOS
        setup.setup_macos()
    elif system == "linux":
        setup.setup_linux()
    elif system == "windows":
        setup.setup_windows()
    else:
        print(f"❌ 不支持的操作系统: {system}")
    
    print("\n" + "="*50)
    setup.show_instructions()

if __name__ == "__main__":
    main()