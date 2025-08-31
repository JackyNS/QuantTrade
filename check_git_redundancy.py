#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Git仓库冗余文件检查器
检查Git跟踪的文件是否与实际文件系统一致，找出可能的重复文件
"""

import os
import subprocess
from pathlib import Path
from collections import defaultdict
import json

class GitRedundancyChecker:
    """Git冗余检查器"""
    
    def __init__(self):
        self.root = Path(".")
        self.git_files = set()
        self.filesystem_files = set()
        self.redundant_files = []
        self.missing_files = []
        
    def get_git_tracked_files(self):
        """获取Git跟踪的所有文件"""
        try:
            result = subprocess.run(['git', 'ls-files'], capture_output=True, text=True, encoding='utf-8')
            if result.returncode == 0:
                files = [f for f in result.stdout.strip().split('\n') if f]
                self.git_files = set(files)
                print(f"📊 Git跟踪文件: {len(self.git_files)} 个")
            else:
                print("❌ 无法获取Git文件列表")
        except Exception as e:
            print(f"❌ Git命令执行失败: {e}")
    
    def get_filesystem_files(self):
        """获取文件系统中的所有文件（排除.git等）"""
        ignore_patterns = {'.git', '__pycache__', '.DS_Store', '.pytest_cache', 'node_modules'}
        ignore_extensions = {'.pyc', '.pyo', '.log'}
        
        for item in self.root.rglob("*"):
            if item.is_file():
                # 检查是否应该忽略
                if any(pattern in str(item) for pattern in ignore_patterns):
                    continue
                if item.suffix in ignore_extensions:
                    continue
                    
                relative_path = item.relative_to(self.root)
                self.filesystem_files.add(str(relative_path))
        
        print(f"📁 文件系统文件: {len(self.filesystem_files)} 个")
    
    def find_redundant_files(self):
        """查找冗余文件"""
        # Git中有但文件系统中没有的文件（可能是已删除但Git未清理）
        git_only = self.git_files - self.filesystem_files
        
        # 文件系统中有但Git未跟踪的文件
        fs_only = self.filesystem_files - self.git_files
        
        print(f"\n🔍 冗余文件分析:")
        print(f"📦 Git中但已删除: {len(git_only)} 个")
        print(f"📁 本地未跟踪: {len(fs_only)} 个")
        
        return git_only, fs_only
    
    def analyze_moved_files(self):
        """分析可能被移动的文件"""
        moved_patterns = []
        
        # 检查可能的文件移动模式
        for git_file in self.git_files:
            if git_file not in self.filesystem_files:
                # 查找同名文件是否在其他位置
                filename = Path(git_file).name
                for fs_file in self.filesystem_files:
                    if Path(fs_file).name == filename and fs_file not in self.git_files:
                        moved_patterns.append({
                            'old_location': git_file,
                            'new_location': fs_file,
                            'filename': filename
                        })
        
        return moved_patterns
    
    def check_specific_redundancy(self):
        """检查特定的冗余情况"""
        redundancy_report = {
            'duplicate_tools': [],
            'duplicate_docs': [],
            'archive_issues': []
        }
        
        # 检查tools目录的文件是否在根目录也有Git记录
        tools_files = [f for f in self.git_files if f.startswith('tools/')]
        root_py_files = [f for f in self.git_files if f.endswith('.py') and '/' not in f]
        
        for tool_file in tools_files:
            tool_name = Path(tool_file).name
            # 检查根目录是否有同名文件
            for root_file in root_py_files:
                if Path(root_file).name == tool_name:
                    redundancy_report['duplicate_tools'].append({
                        'tool_location': tool_file,
                        'root_location': root_file,
                        'status': 'potential_duplicate'
                    })
        
        return redundancy_report
    
    def generate_cleanup_script(self, git_only_files):
        """生成清理脚本"""
        if not git_only_files:
            return None
            
        cleanup_script = """#!/bin/bash
# Git仓库清理脚本 - 移除已删除但仍被跟踪的文件

echo "🧹 开始清理Git仓库冗余文件..."

"""
        
        for file in git_only_files:
            cleanup_script += f'git rm "{file}"\n'
        
        cleanup_script += """
echo "✅ 清理完成，准备提交..."
git commit -m "🗑️ Remove redundant files from Git tracking

🧹 Cleaned up files that were moved/deleted but still tracked in Git
📊 Removed files: """ + str(len(git_only_files)) + """"

🤖 Generated with [Claude Code](https://claude.ai/code)

Co-Authored-By: Claude <noreply@anthropic.com>
"

echo "🎉 Git仓库清理完成！"
"""
        
        return cleanup_script
    
    def run_check(self):
        """运行完整检查"""
        print("🔍 开始Git仓库冗余检查...")
        print("=" * 50)
        
        # 获取文件列表
        self.get_git_tracked_files()
        self.get_filesystem_files()
        
        # 查找冗余文件
        git_only, fs_only = self.find_redundant_files()
        
        # 分析移动的文件
        moved_files = self.analyze_moved_files()
        
        # 检查特定冗余
        redundancy_report = self.check_specific_redundancy()
        
        # 生成报告
        report = {
            'timestamp': str(Path('.')),
            'summary': {
                'git_tracked': len(self.git_files),
                'filesystem_files': len(self.filesystem_files),
                'git_only': len(git_only),
                'fs_only': len(fs_only),
                'moved_files': len(moved_files)
            },
            'git_only_files': list(git_only),
            'fs_only_files': list(fs_only),
            'moved_files': moved_files,
            'redundancy_analysis': redundancy_report
        }
        
        # 输出详细结果
        print(f"\n📋 详细分析结果:")
        
        if git_only:
            print(f"\n⚠️ Git中跟踪但本地已删除的文件 ({len(git_only)}个):")
            for file in sorted(git_only):
                print(f"  - {file}")
        
        if fs_only:
            print(f"\n📁 本地存在但未被Git跟踪的文件 ({len(fs_only)}个):")
            for file in sorted(list(fs_only)[:10]):  # 只显示前10个
                print(f"  - {file}")
            if len(fs_only) > 10:
                print(f"  ... 还有 {len(fs_only)-10} 个文件")
        
        if moved_files:
            print(f"\n🔄 可能被移动的文件 ({len(moved_files)}个):")
            for move in moved_files:
                print(f"  - {move['filename']}: {move['old_location']} → {move['new_location']}")
        
        # 生成清理脚本
        if git_only:
            cleanup_script = self.generate_cleanup_script(git_only)
            with open('git_cleanup.sh', 'w') as f:
                f.write(cleanup_script)
            os.chmod('git_cleanup.sh', 0o755)
            print(f"\n🔧 已生成清理脚本: git_cleanup.sh")
        
        # 保存报告
        with open('git_redundancy_report.json', 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        print(f"\n📊 详细报告已保存: git_redundancy_report.json")
        
        # 结论
        if git_only:
            print(f"\n⚠️ 发现问题: Git中有 {len(git_only)} 个冗余文件需要清理")
            print("💡 建议: 运行 ./git_cleanup.sh 来清理这些文件")
        else:
            print(f"\n✅ 良好: Git跟踪状态与文件系统一致，无冗余文件")
        
        return report

def main():
    """主函数"""
    checker = GitRedundancyChecker()
    checker.run_check()

if __name__ == "__main__":
    main()