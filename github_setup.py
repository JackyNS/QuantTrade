#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
GitHub仓库设置助手
"""

import subprocess
import json
import os
from pathlib import Path

class GitHubSetupHelper:
    """GitHub设置助手"""
    
    def __init__(self):
        self.repo_name = "QuantTrade"
        self.repo_description = "🚀 Enterprise-grade Quantitative Trading Platform | 企业级量化交易平台"
        
    def check_gh_cli(self):
        """检查GitHub CLI是否安装"""
        try:
            result = subprocess.run(['gh', '--version'], 
                                  capture_output=True, text=True)
            if result.returncode == 0:
                print("✅ GitHub CLI已安装")
                print(f"   版本: {result.stdout.split()[2]}")
                return True
        except FileNotFoundError:
            pass
        
        print("❌ GitHub CLI未安装")
        print("\n📥 安装方法:")
        print("   macOS: brew install gh")
        print("   Windows: winget install GitHub.cli")
        print("   Linux: apt install gh 或 yum install gh")
        print("\n🔗 详情: https://cli.github.com/")
        return False
    
    def check_gh_auth(self):
        """检查GitHub CLI认证状态"""
        try:
            result = subprocess.run(['gh', 'auth', 'status'], 
                                  capture_output=True, text=True)
            if result.returncode == 0:
                print("✅ GitHub认证已完成")
                return True
        except:
            pass
        
        print("❌ GitHub未认证")
        print("\n🔐 认证方法:")
        print("   1. 运行: gh auth login")
        print("   2. 选择 GitHub.com")
        print("   3. 选择认证方式 (浏览器/Token)")
        print("   4. 按提示完成认证")
        return False
    
    def create_github_repo(self):
        """创建GitHub仓库"""
        if not self.check_gh_cli():
            return False
        
        if not self.check_gh_auth():
            return False
        
        print(f"\n🚀 创建GitHub仓库: {self.repo_name}")
        
        try:
            # 创建仓库
            cmd = [
                'gh', 'repo', 'create', self.repo_name,
                '--description', self.repo_description,
                '--public',  # 公开仓库，如需私有请改为 --private
                '--source=.',
                '--remote=origin',
                '--push'
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            if result.returncode == 0:
                print("✅ GitHub仓库创建成功!")
                print(f"🔗 仓库地址: https://github.com/{self.get_username()}/{self.repo_name}")
                return True
            else:
                print(f"❌ 仓库创建失败: {result.stderr}")
                return False
                
        except Exception as e:
            print(f"❌ 创建仓库时发生错误: {e}")
            return False
    
    def get_username(self):
        """获取GitHub用户名"""
        try:
            result = subprocess.run(['gh', 'api', 'user'], 
                                  capture_output=True, text=True)
            if result.returncode == 0:
                user_info = json.loads(result.stdout)
                return user_info.get('login', 'unknown')
        except:
            pass
        return 'unknown'
    
    def setup_remote_manually(self):
        """手动设置远程仓库"""
        print("\n📝 手动设置GitHub仓库:")
        print("1. 访问 https://github.com/new")
        print(f"2. 仓库名称: {self.repo_name}")
        print(f"3. 描述: {self.repo_description}")
        print("4. 选择 Public (公开) 或 Private (私有)")
        print("5. 不要初始化README (我们已有文件)")
        print("6. 点击 Create repository")
        print("\n7. 创建后，复制仓库URL并运行:")
        
        username = input("\n请输入您的GitHub用户名: ").strip()
        if username:
            repo_url = f"https://github.com/{username}/{self.repo_name}.git"
            print(f"\n🔗 推荐运行:")
            print(f"git remote add origin {repo_url}")
            print(f"git branch -M main")
            print(f"git push -u origin main")
            
            return repo_url
        return None
    
    def push_to_github(self, manual_url=None):
        """推送到GitHub"""
        try:
            # 检查是否已有远程仓库
            result = subprocess.run(['git', 'remote', 'get-url', 'origin'], 
                                  capture_output=True, text=True)
            
            if result.returncode != 0 and manual_url:
                # 添加远程仓库
                subprocess.run(['git', 'remote', 'add', 'origin', manual_url])
                print(f"✅ 已添加远程仓库: {manual_url}")
            
            # 设置主分支
            subprocess.run(['git', 'branch', '-M', 'main'])
            
            # 推送
            result = subprocess.run(['git', 'push', '-u', 'origin', 'main'], 
                                  capture_output=True, text=True)
            
            if result.returncode == 0:
                print("✅ 代码推送成功!")
                return True
            else:
                print(f"❌ 推送失败: {result.stderr}")
                return False
                
        except Exception as e:
            print(f"❌ 推送时发生错误: {e}")
            return False

def main():
    """主函数"""
    helper = GitHubSetupHelper()
    
    print("🚀 GitHub仓库设置助手")
    print("="*50)
    
    # 方式1: 使用GitHub CLI自动创建
    if helper.create_github_repo():
        return
    
    # 方式2: 手动创建仓库
    print("\n" + "="*50)
    print("🔄 切换到手动模式")
    
    repo_url = helper.setup_remote_manually()
    
    if repo_url:
        confirm = input(f"\n✅ 是否推送到 {repo_url} ? (y/N): ").strip().lower()
        if confirm in ['y', 'yes']:
            helper.push_to_github(repo_url)

if __name__ == "__main__":
    main()