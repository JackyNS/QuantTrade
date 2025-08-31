#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
根目录深度分析器 - 识别进一步优化机会
"""

import os
from pathlib import Path
from datetime import datetime
import json

class RootDirectoryAnalyzer:
    """根目录分析器"""
    
    def __init__(self):
        self.root = Path(".")
        
    def analyze_python_files(self):
        """分析根目录Python文件"""
        print("🐍 分析Python文件...")
        
        py_files = list(self.root.glob("*.py"))
        categories = {
            'core_business': [],      # 核心业务
            'utility_tools': [],      # 工具脚本
            'analysis_tools': [],     # 分析工具
            'maintenance': [],        # 维护脚本
            'temporary': []           # 临时文件
        }
        
        for py_file in py_files:
            name = py_file.name
            
            # 核心业务文件
            if name in ['main.py', 'setup.py', 'data_usage_guide.py', 
                       'daily_update_uqer.py', 'monitor_download_progress.py',
                       'priority_market_flow_downloader.py', 
                       'start_historical_download.py', 'start_smart_download.py']:
                categories['core_business'].append(name)
                
            # 工具脚本  
            elif name in ['auto_backup.py', 'setup_daily_backup.py', 'setup_scheduler.py']:
                categories['utility_tools'].append(name)
                
            # 分析工具
            elif 'git' in name or 'redundancy' in name or 'verification' in name:
                categories['analysis_tools'].append(name)
                
            # 维护脚本
            elif 'optimize' in name or 'cleanup' in name or 'execute' in name:
                categories['maintenance'].append(name)
                
            else:
                categories['temporary'].append(name)
        
        return categories
    
    def analyze_markdown_files(self):
        """分析Markdown文件"""
        print("📋 分析Markdown文件...")
        
        md_files = list(self.root.glob("*.md"))
        categories = {
            'core_docs': [],          # 核心文档
            'process_reports': [],    # 过程报告
            'project_summaries': [],  # 项目总结
            'redundant': []           # 冗余文档
        }
        
        for md_file in md_files:
            name = md_file.name
            
            if name in ['README.md', 'CLAUDE.md']:
                categories['core_docs'].append(name)
            elif 'REPORT' in name or 'SUMMARY' in name:
                categories['process_reports'].append(name)
            elif 'OVERVIEW' in name or 'STRUCTURE' in name:
                categories['project_summaries'].append(name)
            else:
                categories['redundant'].append(name)
        
        return categories
    
    def analyze_directories(self):
        """分析目录结构"""
        print("📁 分析目录结构...")
        
        directories = [d for d in self.root.iterdir() 
                      if d.is_dir() and not d.name.startswith('.')]
        
        analysis = {}
        
        for directory in directories:
            if directory.name in ['core', 'data']:
                continue  # 跳过核心模块
                
            file_count = len(list(directory.rglob("*")))
            recent_files = sum(1 for f in directory.rglob("*") 
                             if f.is_file() and 
                             (datetime.now().timestamp() - f.stat().st_mtime) < 86400*7)
            
            analysis[directory.name] = {
                'total_files': file_count,
                'recent_files': recent_files,
                'purpose': self._classify_directory(directory.name),
                'optimization_potential': self._assess_optimization_potential(directory)
            }
        
        return analysis
    
    def _classify_directory(self, name):
        """分类目录用途"""
        classifications = {
            'tools': 'development_tools',
            'archive': 'historical_storage',
            'docs': 'documentation', 
            'logs': 'system_logs',
            'reports': 'analysis_output',
            'results': 'computation_output',
            'scripts': 'execution_scripts',
            'notebooks': 'development_environment',
            'tests': 'testing_suite'
        }
        return classifications.get(name, 'unknown')
    
    def _assess_optimization_potential(self, directory):
        """评估优化潜力"""
        name = directory.name
        file_count = len(list(directory.rglob("*")))
        
        # 检查是否有重复功能的目录
        if name in ['logs', 'reports', 'results']:
            return 'merge_potential'  # 可能可以合并
        elif name == 'scripts' and file_count < 10:
            return 'integration_potential'  # 可能可以集成到其他地方
        elif name == 'notebooks' and file_count > 50:
            return 'cleanup_needed'  # 需要清理
        else:
            return 'well_organized'
    
    def identify_optimization_opportunities(self, py_analysis, md_analysis, dir_analysis):
        """识别优化机会"""
        print("\n🎯 识别优化机会...")
        
        opportunities = {
            'file_consolidation': [],
            'directory_merging': [],
            'cleanup_candidates': [],
            'restructuring': []
        }
        
        # 文件整合机会
        if py_analysis['analysis_tools']:
            opportunities['file_consolidation'].append({
                'category': 'analysis_tools',
                'files': py_analysis['analysis_tools'],
                'suggestion': '移动到 tools/analysis/ 目录'
            })
            
        if py_analysis['maintenance']:
            opportunities['file_consolidation'].append({
                'category': 'maintenance_scripts', 
                'files': py_analysis['maintenance'],
                'suggestion': '移动到 tools/maintenance/ 目录'
            })
        
        # 文档整合
        if len(md_analysis['process_reports']) > 3:
            opportunities['file_consolidation'].append({
                'category': 'process_reports',
                'files': md_analysis['process_reports'],
                'suggestion': '合并为单一总结文档或移动到 archive/docs/'
            })
        
        # 目录合并机会
        merge_candidates = []
        for dir_name, info in dir_analysis.items():
            if info['optimization_potential'] == 'merge_potential':
                merge_candidates.append(dir_name)
        
        if 'reports' in merge_candidates and 'results' in merge_candidates:
            opportunities['directory_merging'].append({
                'directories': ['reports', 'results'],
                'suggestion': '合并为单一输出目录 outputs/'
            })
        
        # 清理候选
        for dir_name, info in dir_analysis.items():
            if info['recent_files'] == 0 and info['total_files'] > 5:
                opportunities['cleanup_candidates'].append({
                    'directory': dir_name,
                    'reason': f'无最近活动文件，包含{info["total_files"]}个旧文件'
                })
        
        return opportunities
    
    def generate_optimization_plan(self, opportunities):
        """生成优化计划"""
        plan = {
            'immediate_actions': [],
            'optional_improvements': [],
            'long_term_considerations': []
        }
        
        # 立即行动
        for consolidation in opportunities['file_consolidation']:
            if consolidation['category'] in ['analysis_tools', 'maintenance_scripts']:
                plan['immediate_actions'].append({
                    'action': f"mkdir -p tools/{consolidation['category'].split('_')[0]}",
                    'description': f"移动 {len(consolidation['files'])} 个{consolidation['category']}文件"
                })
        
        # 可选改进
        for merge in opportunities['directory_merging']:
            plan['optional_improvements'].append({
                'action': f"合并目录: {' + '.join(merge['directories'])}",
                'description': merge['suggestion']
            })
        
        # 长期考虑
        for cleanup in opportunities['cleanup_candidates']:
            plan['long_term_considerations'].append({
                'action': f"review_{cleanup['directory']}",
                'description': f"审查 {cleanup['directory']} 目录: {cleanup['reason']}"
            })
        
        return plan
    
    def run_analysis(self):
        """运行完整分析"""
        print("🔍 开始根目录深度分析...")
        print("=" * 50)
        
        # 分析文件
        py_analysis = self.analyze_python_files()
        md_analysis = self.analyze_markdown_files()
        dir_analysis = self.analyze_directories()
        
        # 识别优化机会
        opportunities = self.identify_optimization_opportunities(py_analysis, md_analysis, dir_analysis)
        
        # 生成计划
        plan = self.generate_optimization_plan(opportunities)
        
        # 输出结果
        print(f"\n📊 分析结果:")
        print(f"📝 Python文件: {sum(len(files) for files in py_analysis.values())} 个")
        print(f"📋 Markdown文件: {sum(len(files) for files in md_analysis.values())} 个")
        print(f"📁 目录: {len(dir_analysis)} 个")
        
        print(f"\n🎯 优化建议:")
        
        if opportunities['file_consolidation']:
            print(f"\n📦 文件整合机会 ({len(opportunities['file_consolidation'])} 项):")
            for item in opportunities['file_consolidation']:
                print(f"  • {item['category']}: {len(item['files'])} 个文件")
                print(f"    建议: {item['suggestion']}")
        
        if opportunities['directory_merging']:
            print(f"\n🔄 目录合并机会 ({len(opportunities['directory_merging'])} 项):")
            for item in opportunities['directory_merging']:
                print(f"  • 合并: {' + '.join(item['directories'])}")
                print(f"    建议: {item['suggestion']}")
        
        if opportunities['cleanup_candidates']:
            print(f"\n🧹 清理候选 ({len(opportunities['cleanup_candidates'])} 项):")
            for item in opportunities['cleanup_candidates']:
                print(f"  • {item['directory']}: {item['reason']}")
        
        # 保存详细报告
        report = {
            'analysis_time': datetime.now().isoformat(),
            'python_analysis': py_analysis,
            'markdown_analysis': md_analysis,
            'directory_analysis': dir_analysis,
            'optimization_opportunities': opportunities,
            'optimization_plan': plan
        }
        
        with open('root_analysis_report.json', 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        print(f"\n📊 详细报告已保存: root_analysis_report.json")
        
        return report

def main():
    """主函数"""
    analyzer = RootDirectoryAnalyzer()
    analyzer.run_analysis()

if __name__ == "__main__":
    main()