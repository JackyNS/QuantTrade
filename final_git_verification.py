#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
最终Git状态验证
"""

import subprocess
import os
from pathlib import Path

def check_git_status():
    """检查当前Git状态"""
    print("🔍 Git仓库最终状态检查")
    print("=" * 40)
    
    # 获取Git跟踪的文件
    result = subprocess.run(['git', 'ls-files'], capture_output=True, text=True)
    git_files = set(result.stdout.strip().split('\n'))
    
    # 检查文件是否存在
    missing_files = []
    existing_files = []
    
    for file_path in git_files:
        if file_path:  # 忽略空行
            if os.path.exists(file_path):
                existing_files.append(file_path)
            else:
                missing_files.append(file_path)
    
    print(f"📊 Git跟踪文件总数: {len(git_files)}")
    print(f"✅ 存在的文件: {len(existing_files)}")
    print(f"❌ 缺失的文件: {len(missing_files)}")
    
    if missing_files:
        print(f"\n⚠️ 以下文件在Git中跟踪但在文件系统中不存在:")
        for file in missing_files:
            print(f"  - {file}")
    else:
        print(f"\n🎉 所有Git跟踪的文件都存在于文件系统中！")
    
    # 检查Git状态
    status_result = subprocess.run(['git', 'status', '--porcelain'], capture_output=True, text=True)
    if status_result.stdout.strip():
        print(f"\n📝 有未提交的更改:")
        print(status_result.stdout)
    else:
        print(f"\n✅ 工作区干净，没有未提交的更改")
    
    return len(missing_files) == 0

if __name__ == "__main__":
    is_clean = check_git_status()
    if is_clean:
        print(f"\n🎊 Git仓库状态完美！没有冗余文件。")
    else:
        print(f"\n⚠️ 发现问题，需要进一步清理。")