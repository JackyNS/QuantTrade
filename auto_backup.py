#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
自动备份脚本 - 每日备份项目到GitHub
"""

import subprocess
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
import json
import logging

class AutoBackup:
    """自动备份管理器"""
    
    def __init__(self):
        self.project_root = Path(__file__).parent
        self.backup_log = self.project_root / "backup.log"
        self.setup_logging()
        
    def setup_logging(self):
        """配置日志"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(self.backup_log, encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def check_git_status(self):
        """检查Git状态"""
        try:
            # 检查是否在Git仓库中
            result = subprocess.run(['git', 'status', '--porcelain'], 
                                  cwd=self.project_root,
                                  capture_output=True, text=True)
            
            if result.returncode != 0:
                self.logger.error("❌ 不在Git仓库中或Git未初始化")
                return False, []
            
            # 获取变更文件
            changed_files = []
            if result.stdout.strip():
                for line in result.stdout.strip().split('\n'):
                    if line.strip():
                        changed_files.append(line.strip())
            
            return True, changed_files
            
        except Exception as e:
            self.logger.error(f"❌ 检查Git状态失败: {e}")
            return False, []
    
    def check_remote_connection(self):
        """检查远程仓库连接"""
        try:
            result = subprocess.run(['git', 'remote', 'get-url', 'origin'], 
                                  cwd=self.project_root,
                                  capture_output=True, text=True)
            
            if result.returncode == 0:
                remote_url = result.stdout.strip()
                self.logger.info(f"📡 远程仓库: {remote_url}")
                
                # 测试连接
                test_result = subprocess.run(['git', 'ls-remote', 'origin'], 
                                           cwd=self.project_root,
                                           capture_output=True, text=True,
                                           timeout=30)
                
                if test_result.returncode == 0:
                    self.logger.info("✅ 远程仓库连接正常")
                    return True
                else:
                    self.logger.error(f"❌ 无法连接远程仓库: {test_result.stderr}")
                    return False
            else:
                self.logger.error("❌ 未配置远程仓库")
                return False
                
        except subprocess.TimeoutExpired:
            self.logger.error("❌ 连接远程仓库超时")
            return False
        except Exception as e:
            self.logger.error(f"❌ 检查远程连接失败: {e}")
            return False
    
    def generate_backup_message(self, changed_files):
        """生成备份提交信息"""
        now = datetime.now()
        date_str = now.strftime("%Y-%m-%d")
        time_str = now.strftime("%H:%M:%S")
        
        # 分析变更类型
        file_changes = {
            'added': [],
            'modified': [],
            'deleted': [],
            'renamed': []
        }
        
        for file_line in changed_files:
            status = file_line[:2]
            filename = file_line[3:]
            
            if status.startswith('A'):
                file_changes['added'].append(filename)
            elif status.startswith('M'):
                file_changes['modified'].append(filename)
            elif status.startswith('D'):
                file_changes['deleted'].append(filename)
            elif status.startswith('R'):
                file_changes['renamed'].append(filename)
        
        # 生成消息
        message_parts = [f"📅 Daily backup {date_str} {time_str}"]
        
        if file_changes['added']:
            message_parts.append(f"✨ Added: {len(file_changes['added'])} files")
        if file_changes['modified']:
            message_parts.append(f"📝 Modified: {len(file_changes['modified'])} files")
        if file_changes['deleted']:
            message_parts.append(f"🗑️ Deleted: {len(file_changes['deleted'])} files")
        
        # 限制文件列表长度
        if len(changed_files) <= 10:
            message_parts.append("\nChanged files:")
            for file_line in changed_files[:10]:
                filename = file_line[3:]
                message_parts.append(f"  • {filename}")
        else:
            message_parts.append(f"\nTotal changes: {len(changed_files)} files")
        
        message_parts.extend([
            "",
            "🤖 Automated backup by auto_backup.py",
            "",
            "Co-Authored-By: Claude <noreply@anthropic.com>"
        ])
        
        return '\n'.join(message_parts)
    
    def create_backup_commit(self, changed_files):
        """创建备份提交"""
        try:
            # 添加所有变更
            self.logger.info("📦 添加变更文件...")
            result = subprocess.run(['git', 'add', '-A'], 
                                  cwd=self.project_root,
                                  capture_output=True, text=True)
            
            if result.returncode != 0:
                self.logger.error(f"❌ 添加文件失败: {result.stderr}")
                return False
            
            # 生成提交信息
            commit_message = self.generate_backup_message(changed_files)
            
            # 创建提交
            self.logger.info("💾 创建备份提交...")
            result = subprocess.run(['git', 'commit', '-m', commit_message], 
                                  cwd=self.project_root,
                                  capture_output=True, text=True)
            
            if result.returncode == 0:
                self.logger.info("✅ 备份提交创建成功")
                return True
            else:
                # 可能没有变更
                if "nothing to commit" in result.stdout:
                    self.logger.info("ℹ️ 没有需要备份的变更")
                    return True
                else:
                    self.logger.error(f"❌ 创建提交失败: {result.stderr}")
                    return False
            
        except Exception as e:
            self.logger.error(f"❌ 创建备份提交时出错: {e}")
            return False
    
    def push_to_remote(self):
        """推送到远程仓库"""
        try:
            self.logger.info("🚀 推送到远程仓库...")
            result = subprocess.run(['git', 'push', 'origin', 'main'], 
                                  cwd=self.project_root,
                                  capture_output=True, text=True,
                                  timeout=300)  # 5分钟超时
            
            if result.returncode == 0:
                self.logger.info("✅ 推送成功")
                return True
            else:
                self.logger.error(f"❌ 推送失败: {result.stderr}")
                # 尝试其他分支名
                if "main" in result.stderr and "master" in result.stderr:
                    self.logger.info("🔄 尝试推送到master分支...")
                    result2 = subprocess.run(['git', 'push', 'origin', 'master'], 
                                           cwd=self.project_root,
                                           capture_output=True, text=True,
                                           timeout=300)
                    if result2.returncode == 0:
                        self.logger.info("✅ 推送到master分支成功")
                        return True
                return False
                
        except subprocess.TimeoutExpired:
            self.logger.error("❌ 推送超时")
            return False
        except Exception as e:
            self.logger.error(f"❌ 推送时出错: {e}")
            return False
    
    def run_backup(self):
        """执行完整备份流程"""
        self.logger.info("🚀 开始自动备份...")
        
        # 1. 检查Git状态
        git_ok, changed_files = self.check_git_status()
        if not git_ok:
            return False
        
        if not changed_files:
            self.logger.info("ℹ️ 没有需要备份的变更")
            return True
        
        self.logger.info(f"📋 发现 {len(changed_files)} 个变更")
        
        # 2. 检查远程连接
        if not self.check_remote_connection():
            self.logger.warning("⚠️ 远程仓库连接失败，仅创建本地提交")
            # 只创建本地提交
            return self.create_backup_commit(changed_files)
        
        # 3. 创建提交
        if not self.create_backup_commit(changed_files):
            return False
        
        # 4. 推送到远程
        if not self.push_to_remote():
            self.logger.warning("⚠️ 推送失败，但本地提交已创建")
            return False
        
        self.logger.info("🎉 自动备份完成!")
        return True
    
    def show_backup_status(self):
        """显示备份状态"""
        print("📊 备份状态检查")
        print("="*50)
        
        git_ok, changed_files = self.check_git_status()
        if git_ok:
            print(f"📋 待备份文件: {len(changed_files)} 个")
            if changed_files and len(changed_files) <= 10:
                for file_line in changed_files[:10]:
                    print(f"   • {file_line}")
        
        remote_ok = self.check_remote_connection()
        print(f"📡 远程仓库: {'✅ 正常' if remote_ok else '❌ 异常'}")
        
        # 检查最后提交时间
        try:
            result = subprocess.run(['git', 'log', '-1', '--format=%cd', '--date=relative'], 
                                  cwd=self.project_root,
                                  capture_output=True, text=True)
            if result.returncode == 0:
                last_commit = result.stdout.strip()
                print(f"📅 最后提交: {last_commit}")
        except:
            pass

def main():
    """主函数"""
    backup = AutoBackup()
    
    if len(sys.argv) > 1:
        command = sys.argv[1].lower()
        if command == 'status':
            backup.show_backup_status()
        elif command == 'backup':
            backup.run_backup()
        else:
            print("📖 使用方法:")
            print("  python auto_backup.py backup  # 执行备份")
            print("  python auto_backup.py status  # 查看状态")
    else:
        # 默认执行备份
        backup.run_backup()

if __name__ == "__main__":
    main()