#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
项目结构分析和优化工具
"""

import os
from pathlib import Path
from datetime import datetime
import json

class ProjectAnalyzer:
    """项目结构分析器"""
    
    def __init__(self):
        self.root = Path(".")
        self.analysis = {
            'redundant_files': [],
            'temporary_files': [],
            'outdated_files': [],
            'duplicate_functionality': [],
            'optimization_suggestions': []
        }
    
    def analyze_root_files(self):
        """分析根目录文件"""
        print("🔍 分析根目录文件...")
        
        # 分类文件
        categories = {
            'core_scripts': [],
            'data_downloaders': [],
            'analysis_tools': [],
            'setup_tools': [],
            'test_files': [],
            'temporary_files': [],
            'documentation': [],
            'config_files': []
        }
        
        # Python 文件分析
        py_files = list(self.root.glob("*.py"))
        print(f"📊 根目录Python文件: {len(py_files)} 个")
        
        for file in py_files:
            filename = file.name
            
            # 核心脚本
            if filename in ['main.py', 'setup.py']:
                categories['core_scripts'].append(filename)
            
            # 数据下载器
            elif any(keyword in filename for keyword in ['download', 'uqer', 'historical', 'priority', 'smart']):
                categories['data_downloaders'].append(filename)
            
            # 分析工具
            elif any(keyword in filename for keyword in ['analyze', 'analysis', 'monitor', 'quality', 'optimizer']):
                categories['analysis_tools'].append(filename)
            
            # 设置工具
            elif any(keyword in filename for keyword in ['setup', 'github', 'backup', 'scheduler']):
                categories['setup_tools'].append(filename)
            
            # 测试文件
            elif any(keyword in filename for keyword in ['test', 'check', 'simple', 'example']):
                categories['test_files'].append(filename)
            
            # 临时文件
            elif any(keyword in filename for keyword in ['temp', 'tmp', 'old', 'backup']):
                categories['temporary_files'].append(filename)
        
        # Markdown 文件
        md_files = list(self.root.glob("*.md"))
        print(f"📋 根目录Markdown文件: {len(md_files)} 个")
        
        for file in md_files:
            categories['documentation'].append(file.name)
        
        # 配置文件
        config_files = list(self.root.glob("*.json")) + list(self.root.glob("*.txt"))
        for file in config_files:
            categories['config_files'].append(file.name)
        
        return categories
    
    def identify_redundant_files(self, categories):
        """识别冗余文件"""
        print("\n🔍 识别冗余和重复文件...")
        
        redundant_files = []
        
        # 数据下载器重复检查
        downloaders = categories['data_downloaders']
        if len(downloaders) > 5:  # 超过5个下载器可能有冗余
            redundant_files.extend([
                {
                    'file': f,
                    'reason': '可能的冗余下载器',
                    'action': 'review_and_merge'
                } for f in downloaders[5:]  # 保留前5个
            ])
        
        # 测试文件检查
        test_files = categories['test_files']
        redundant_files.extend([
            {
                'file': f,
                'reason': '临时测试文件',
                'action': 'move_to_tests_dir'
            } for f in test_files if 'simple' in f or 'example' in f
        ])
        
        # 分析工具重复检查
        analysis_files = categories['analysis_tools']
        if 'analyze_existing_data.py' in analysis_files and 'detailed_data_analysis.py' in analysis_files:
            redundant_files.append({
                'file': 'analyze_existing_data.py',
                'reason': 'detailed_data_analysis.py提供更完整功能',
                'action': 'remove'
            })
        
        return redundant_files
    
    def identify_temporary_files(self, categories):
        """识别临时文件"""
        temporary_files = []
        
        # 明显的临时文件
        for file in categories['temporary_files']:
            temporary_files.append({
                'file': file,
                'reason': '临时文件',
                'action': 'remove'
            })
        
        # GitHub设置相关文件
        github_setup_files = [f for f in categories['setup_tools'] 
                             if 'github' in f and f != 'auto_backup.py']
        for file in github_setup_files:
            if file != 'setup_daily_backup.py':  # 保留有用的设置脚本
                temporary_files.append({
                    'file': file,
                    'reason': 'GitHub设置完成后不再需要',
                    'action': 'move_to_archive'
                })
        
        return temporary_files
    
    def check_file_usage(self):
        """检查文件使用情况"""
        print("\n🔍 检查文件使用和依赖...")
        
        # 检查import使用情况
        py_files = list(self.root.glob("*.py"))
        usage_map = {}
        
        for file in py_files:
            usage_map[file.name] = {
                'imported_by': [],
                'imports': [],
                'size_kb': file.stat().st_size / 1024
            }
        
        # 分析import关系
        for file in py_files:
            try:
                content = file.read_text(encoding='utf-8')
                for other_file in py_files:
                    if other_file != file:
                        module_name = other_file.stem
                        if f"import {module_name}" in content or f"from {module_name}" in content:
                            usage_map[other_file.name]['imported_by'].append(file.name)
            except:
                continue
        
        return usage_map
    
    def generate_optimization_plan(self, categories, redundant_files, temporary_files, usage_map):
        """生成优化方案"""
        print("\n💡 生成优化方案...")
        
        optimization_plan = {
            'immediate_actions': [],
            'reorganization': [],
            'consolidation': [],
            'archive': []
        }
        
        # 立即清理
        for item in temporary_files:
            optimization_plan['immediate_actions'].append({
                'action': 'delete',
                'file': item['file'],
                'reason': item['reason']
            })
        
        # 重组建议
        if len(categories['data_downloaders']) > 5:
            optimization_plan['reorganization'].append({
                'action': 'create_directory',
                'name': 'tools/data_download/',
                'move_files': categories['data_downloaders']
            })
        
        if len(categories['analysis_tools']) > 3:
            optimization_plan['reorganization'].append({
                'action': 'create_directory', 
                'name': 'tools/analysis/',
                'move_files': categories['analysis_tools']
            })
        
        # 合并建议
        similar_files = self._find_similar_files(categories)
        for group in similar_files:
            if len(group) > 1:
                optimization_plan['consolidation'].append({
                    'action': 'merge_files',
                    'files': group,
                    'target': f"merged_{group[0]}"
                })
        
        # 归档建议
        documentation_files = [f for f in categories['documentation'] 
                              if any(keyword in f.lower() for keyword in ['setup', 'guide', 'instructions'])]
        if documentation_files:
            optimization_plan['archive'].append({
                'action': 'move_to_docs',
                'files': documentation_files
            })
        
        return optimization_plan
    
    def _find_similar_files(self, categories):
        """找到相似功能的文件"""
        similar_groups = []
        
        # 数据下载相关
        downloaders = categories['data_downloaders']
        if 'download_uqer_data.py' in downloaders and 'download_data_example.py' in downloaders:
            similar_groups.append(['download_uqer_data.py', 'download_data_example.py'])
        
        # GitHub设置相关
        setup_tools = categories['setup_tools']
        github_files = [f for f in setup_tools if 'github' in f]
        if len(github_files) > 1:
            similar_groups.append(github_files)
        
        return similar_groups
    
    def create_cleanup_script(self, optimization_plan):
        """创建清理脚本"""
        script_content = '''#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
项目清理脚本 - 自动执行优化建议
"""

import os
import shutil
from pathlib import Path

def cleanup_project():
    """执行项目清理"""
    print("🧹 开始项目清理...")
    
    # 创建归档目录
    archive_dir = Path("archive")
    archive_dir.mkdir(exist_ok=True)
    
    tools_dir = Path("tools")
    tools_dir.mkdir(exist_ok=True)
    
'''
        
        # 添加具体清理操作
        for action in optimization_plan['immediate_actions']:
            if action['action'] == 'delete':
                script_content += f'''    
    # 删除 {action['file']} - {action['reason']}
    if Path("{action['file']}").exists():
        Path("{action['file']}").unlink()
        print("🗑️ 删除: {action['file']}")
'''
        
        script_content += '''
    print("✅ 项目清理完成!")

if __name__ == "__main__":
    cleanup_project()
'''
        
        return script_content
    
    def generate_report(self):
        """生成完整分析报告"""
        print("🚀 开始项目结构分析...\n")
        
        # 执行分析
        categories = self.analyze_root_files()
        redundant_files = self.identify_redundant_files(categories)
        temporary_files = self.identify_temporary_files(categories)
        usage_map = self.check_file_usage()
        optimization_plan = self.generate_optimization_plan(categories, redundant_files, temporary_files, usage_map)
        
        # 生成报告
        report = {
            'analysis_time': datetime.now().isoformat(),
            'file_categories': categories,
            'redundant_files': redundant_files,
            'temporary_files': temporary_files,
            'usage_map': usage_map,
            'optimization_plan': optimization_plan,
            'summary': {
                'total_py_files': len(list(self.root.glob("*.py"))),
                'total_md_files': len(list(self.root.glob("*.md"))),
                'redundant_count': len(redundant_files),
                'temporary_count': len(temporary_files),
                'optimization_actions': sum(len(actions) for actions in optimization_plan.values())
            }
        }
        
        # 保存报告
        report_file = Path("project_optimization_report.json")
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        # 创建清理脚本
        cleanup_script = self.create_cleanup_script(optimization_plan)
        with open("cleanup_project.py", 'w', encoding='utf-8') as f:
            f.write(cleanup_script)
        
        return report
    
    def print_summary(self, report):
        """打印分析摘要"""
        print("\n" + "="*60)
        print("📊 项目结构优化分析报告")
        print("="*60)
        
        summary = report['summary']
        print(f"📁 根目录Python文件: {summary['total_py_files']} 个")
        print(f"📋 根目录Markdown文件: {summary['total_md_files']} 个")
        print(f"🔍 发现冗余文件: {summary['redundant_count']} 个")
        print(f"🧹 临时文件: {summary['temporary_count']} 个")
        print(f"💡 优化建议: {summary['optimization_actions']} 项")
        
        # 显示文件分类
        print(f"\n📂 文件分类:")
        categories = report['file_categories']
        for category, files in categories.items():
            if files:
                print(f"   {category}: {len(files)} 个")
        
        # 显示主要优化建议
        print(f"\n💡 主要优化建议:")
        plan = report['optimization_plan']
        
        if plan['immediate_actions']:
            print(f"   🗑️ 立即清理: {len(plan['immediate_actions'])} 个文件")
        
        if plan['reorganization']:
            print(f"   📁 重新组织: {len(plan['reorganization'])} 项")
        
        if plan['consolidation']:
            print(f"   🔄 合并文件: {len(plan['consolidation'])} 组")
        
        # 显示具体建议
        print(f"\n🎯 具体建议:")
        for action in plan['immediate_actions'][:5]:  # 显示前5个
            print(f"   • 删除 {action['file']} - {action['reason']}")

def main():
    """主函数"""
    analyzer = ProjectAnalyzer()
    report = analyzer.generate_report()
    analyzer.print_summary(report)
    
    print(f"\n📋 详细报告已保存: project_optimization_report.json")
    print(f"🧹 清理脚本已创建: cleanup_project.py")

if __name__ == "__main__":
    main()