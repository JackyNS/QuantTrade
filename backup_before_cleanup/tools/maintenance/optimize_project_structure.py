#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
项目结构优化工具 - 清理冗余文件并重组目录
"""

import os
import shutil
from pathlib import Path
from datetime import datetime

class ProjectOptimizer:
    """项目结构优化器"""
    
    def __init__(self):
        self.root = Path(".")
        self.archive_dir = Path("archive")
        self.tools_dir = Path("tools")
        self.cleaned_files = []
        self.moved_files = []
        
    def create_directories(self):
        """创建必要的目录结构"""
        print("📁 创建目录结构...")
        
        directories = [
            "archive",
            "tools/data_download", 
            "tools/analysis",
            "tools/github_setup",
            "docs/guides"
        ]
        
        for dir_path in directories:
            Path(dir_path).mkdir(parents=True, exist_ok=True)
            print(f"   ✅ 创建: {dir_path}/")
    
    def clean_temporary_files(self):
        """清理临时和不再需要的文件"""
        print("\n🧹 清理临时文件...")
        
        # GitHub设置完成后不再需要的文件
        github_setup_files = [
            "github_setup.py",
            "push_to_github.py",
            "GITHUB_PUSH_INSTRUCTIONS.md",
            "QUICK_GITHUB_SETUP.md",
            "FINAL_GITHUB_PUSH_SOLUTION.md"
        ]
        
        for filename in github_setup_files:
            file_path = Path(filename)
            if file_path.exists():
                # 移动到归档而不是删除
                archive_path = self.archive_dir / "github_setup" / filename
                archive_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(str(file_path), str(archive_path))
                self.moved_files.append(f"{filename} → archive/github_setup/")
                print(f"   📦 归档: {filename}")
        
        # 重复的分析文件
        redundant_analysis = [
            "analyze_existing_data.py"  # detailed_data_analysis.py更完整
        ]
        
        for filename in redundant_analysis:
            file_path = Path(filename)
            if file_path.exists():
                archive_path = self.archive_dir / "analysis" / filename
                archive_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(str(file_path), str(archive_path))
                self.moved_files.append(f"{filename} → archive/analysis/")
                print(f"   📦 归档: {filename}")
    
    def organize_data_downloaders(self):
        """整理数据下载器"""
        print("\n📥 整理数据下载器...")
        
        # 保留在根目录的核心下载器
        core_downloaders = [
            "priority_market_flow_downloader.py",
            "start_smart_download.py", 
            "start_historical_download.py",
            "daily_update_uqer.py"
        ]
        
        # 移动到tools的下载器
        tool_downloaders = [
            "simple_download_example.py",
            "download_data_example.py",
            "download_uqer_data.py",
            "stock_only_downloader.py",
            "smart_historical_downloader.py",
            "comprehensive_data_download_plan.py"
        ]
        
        for filename in tool_downloaders:
            file_path = Path(filename)
            if file_path.exists():
                target_path = self.tools_dir / "data_download" / filename
                shutil.move(str(file_path), str(target_path))
                self.moved_files.append(f"{filename} → tools/data_download/")
                print(f"   📦 移动: {filename}")
        
        # 测试和检查工具
        test_tools = [
            "simple_uqer_test.py",
            "test_uqer_connection.py",
            "check_uqer_status.py"
        ]
        
        for filename in test_tools:
            file_path = Path(filename)
            if file_path.exists():
                target_path = self.tools_dir / "data_download" / filename
                shutil.move(str(file_path), str(target_path))
                self.moved_files.append(f"{filename} → tools/data_download/")
                print(f"   📦 移动: {filename}")
    
    def organize_analysis_tools(self):
        """整理分析工具"""
        print("\n📊 整理分析工具...")
        
        # 保留在根目录的核心分析工具
        core_analysis = [
            "monitor_download_progress.py",
            "data_usage_guide.py"
        ]
        
        # 移动到tools的分析工具
        tool_analysis = [
            "data_quality_checker.py",
            "detailed_data_analysis.py", 
            "data_optimizer.py",
            "analyze_data_structure.py",
            "project_analyzer.py"
        ]
        
        for filename in tool_analysis:
            file_path = Path(filename)
            if file_path.exists():
                target_path = self.tools_dir / "analysis" / filename
                shutil.move(str(file_path), str(target_path))
                self.moved_files.append(f"{filename} → tools/analysis/")
                print(f"   📦 移动: {filename}")
    
    def organize_documentation(self):
        """整理文档"""
        print("\n📋 整理文档...")
        
        # 核心文档保留在根目录
        core_docs = [
            "README.md",
            "PROJECT_OVERVIEW.md", 
            "FINAL_SETUP_SUMMARY.md"
        ]
        
        # 设置指南移动到docs
        guide_docs = [
            "GITHUB_SETUP_GUIDE.md",
            "uqer_setup_guide.md",
            "UQER_COMPLETE_SETUP.md",
            "UQER_STATUS_SUMMARY.md"
        ]
        
        for filename in guide_docs:
            file_path = Path(filename)
            if file_path.exists():
                target_path = Path("docs/guides") / filename
                shutil.move(str(file_path), str(target_path))
                self.moved_files.append(f"{filename} → docs/guides/")
                print(f"   📦 移动: {filename}")
        
        # 历史文档归档
        archive_docs = [
            "ARCHITECTURE_COMPLETED.md",
            "COMPREHENSIVE_DATA_REPORT.md",
            "CORRECT_DATA_ANALYSIS_REPORT.md", 
            "MIGRATION_NOTICE.md",
            "migration_summary.md",
            "project_structure.md",
            "project_cleanup_report.md",
            "scripts_migration_report.md"
        ]
        
        for filename in archive_docs:
            file_path = Path(filename)
            if file_path.exists():
                target_path = self.archive_dir / "docs" / filename
                target_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(str(file_path), str(target_path))
                self.moved_files.append(f"{filename} → archive/docs/")
                print(f"   📦 归档: {filename}")
    
    def clean_temporary_analysis_files(self):
        """清理临时分析文件"""
        print("\n🧹 清理临时分析文件...")
        
        # 生成的分析文件
        temp_files = [
            "data_usage_examples.py",
            "project_optimization_report.json",
            "cleanup_project.py"
        ]
        
        for filename in temp_files:
            file_path = Path(filename)
            if file_path.exists():
                target_path = self.archive_dir / "temp" / filename
                target_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(str(file_path), str(target_path))
                self.moved_files.append(f"{filename} → archive/temp/")
                print(f"   📦 归档: {filename}")
    
    def update_gitignore(self):
        """更新.gitignore"""
        print("\n📝 更新.gitignore...")
        
        gitignore_additions = '''
# ========================
# Project Organization
# ========================
# Archive directory (historical files)
archive/

# Tools directory outputs
tools/**/*.log
tools/**/temp_*
tools/**/*_temp.py

'''
        
        gitignore_path = Path(".gitignore")
        if gitignore_path.exists():
            with open(gitignore_path, 'a', encoding='utf-8') as f:
                f.write(gitignore_additions)
            print("   ✅ 已更新 .gitignore")
    
    def create_tools_readme(self):
        """为tools目录创建README"""
        readme_content = '''# 🛠️ Tools Directory

## 📁 目录结构

### data_download/
数据下载相关工具和脚本
- 示例下载器
- 测试连接工具
- 专用下载器

### analysis/
数据分析和质量检查工具
- 数据质量检查
- 结构分析工具
- 优化工具

## 📖 使用说明

这些工具主要用于开发和维护，不是核心业务逻辑的一部分。

### 运行工具
```bash
# 从项目根目录运行
python tools/analysis/data_quality_checker.py
python tools/data_download/simple_uqer_test.py
```

### 注意事项
- 工具脚本可能依赖项目根目录的配置
- 建议从项目根目录运行工具脚本
- 部分工具可能需要特定的数据环境
'''
        
        tools_readme = self.tools_dir / "README.md"
        with open(tools_readme, 'w', encoding='utf-8') as f:
            f.write(readme_content)
        print("   ✅ 创建 tools/README.md")
    
    def generate_summary(self):
        """生成优化总结"""
        summary = f'''# 📊 项目结构优化总结

## 🕒 优化时间
{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

## 📁 创建的目录
- archive/ - 历史文件归档
- tools/data_download/ - 数据下载工具
- tools/analysis/ - 分析工具
- docs/guides/ - 指南文档

## 📦 移动的文件 ({len(self.moved_files)} 个)
'''
        
        for move in self.moved_files:
            summary += f"- {move}\n"
        
        summary += f'''
## ✅ 优化效果
- 根目录Python文件: 29 → ~15 个
- 文档更有组织性
- 工具分类清晰
- 历史文件归档保存

## 🎯 优化后的根目录结构
```
QuantTrade/
├── main.py                 # 主入口
├── setup.py               # 项目设置
├── auto_backup.py         # 自动备份
├── setup_daily_backup.py  # 备份设置
├── data_usage_guide.py    # 数据使用指南
├── monitor_download_progress.py  # 进度监控
├── priority_market_flow_downloader.py  # 优先级下载器
├── start_smart_download.py       # 智能下载器
├── start_historical_download.py  # 历史下载器
├── daily_update_uqer.py          # 日更新
├── core/                  # 核心框架
├── data/                  # 数据资产
├── scripts/               # 执行脚本
├── tools/                 # 开发工具
├── archive/               # 历史归档
└── docs/                  # 文档
```

## 📋 建议
1. 检查移动后的文件路径引用
2. 更新相关文档中的文件路径
3. 测试核心功能确保正常工作
'''
        
        summary_file = Path("PROJECT_OPTIMIZATION_SUMMARY.md")
        with open(summary_file, 'w', encoding='utf-8') as f:
            f.write(summary)
        
        return summary
    
    def optimize(self):
        """执行完整优化"""
        print("🚀 开始项目结构优化...")
        print("="*60)
        
        # 执行优化步骤
        self.create_directories()
        self.clean_temporary_files()
        self.organize_data_downloaders()
        self.organize_analysis_tools()
        self.organize_documentation()
        self.clean_temporary_analysis_files()
        self.update_gitignore()
        self.create_tools_readme()
        
        # 生成总结
        summary = self.generate_summary()
        
        print("\n" + "="*60)
        print("🎉 项目结构优化完成!")
        print("="*60)
        print(f"📦 移动文件: {len(self.moved_files)} 个")
        print(f"📁 创建目录: archive/, tools/, docs/guides/")
        print(f"📋 优化总结: PROJECT_OPTIMIZATION_SUMMARY.md")
        
        return True

def main():
    """主函数"""
    optimizer = ProjectOptimizer()
    
    print("⚠️  准备优化项目结构...")
    print("这将移动和重组文件，建议先备份项目!")
    
    confirm = input("\n确认继续? (y/N): ").strip().lower()
    if confirm in ['y', 'yes']:
        optimizer.optimize()
    else:
        print("❌ 取消优化")

if __name__ == "__main__":
    main()