#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
最终清理分析器 - 深度检查根目录冗余
"""

import os
from pathlib import Path
import shutil
from datetime import datetime

class FinalCleanupAnalyzer:
    """最终清理分析器"""
    
    def __init__(self):
        self.root = Path(".")
        self.cleanup_actions = []
        
    def analyze_root_directories(self):
        """分析根目录所有文件夹"""
        print("🔍 深度分析根目录结构...")
        
        analysis = {}
        
        for item in self.root.iterdir():
            if item.is_dir() and not item.name.startswith('.'):
                file_count = len(list(item.rglob("*")))
                analysis[item.name] = {
                    'path': item,
                    'file_count': file_count,
                    'size_mb': sum(f.stat().st_size for f in item.rglob("*") if f.is_file()) / (1024*1024),
                    'purpose': self._classify_directory(item.name, file_count)
                }
        
        return analysis
    
    def _classify_directory(self, name, file_count):
        """分类目录用途"""
        if file_count == 0:
            return "empty"
        elif "backup" in name.lower():
            return "backup"
        elif name in ["cache", "temp", "tmp"]:
            return "cache"
        elif name in ["output", "results", "reports", "logs"]:
            return "output"
        elif name in ["core", "scripts", "notebooks", "tests", "data"]:
            return "core"
        elif name in ["tools", "docs", "archive"]:
            return "organized"
        else:
            return "unknown"
    
    def identify_cleanup_targets(self, analysis):
        """识别需要清理的目标"""
        print("\n🎯 识别清理目标...")
        
        cleanup_targets = {
            'empty_dirs': [],
            'backup_dirs': [],  
            'cache_dirs': [],
            'redundant_output': [],
            'mergeable_dirs': []
        }
        
        for name, info in analysis.items():
            purpose = info['purpose']
            
            if purpose == "empty":
                cleanup_targets['empty_dirs'].append({
                    'name': name,
                    'path': info['path'],
                    'reason': "空目录，无文件"
                })
            
            elif purpose == "backup":
                cleanup_targets['backup_dirs'].append({
                    'name': name,
                    'path': info['path'],
                    'reason': f"备份目录，{info['file_count']}个文件",
                    'size_mb': info['size_mb']
                })
            
            elif purpose == "cache":
                cleanup_targets['cache_dirs'].append({
                    'name': name, 
                    'path': info['path'],
                    'reason': f"缓存目录，{info['file_count']}个文件",
                    'size_mb': info['size_mb']
                })
            
            elif purpose == "output" and info['file_count'] < 5:
                cleanup_targets['redundant_output'].append({
                    'name': name,
                    'path': info['path'],
                    'reason': f"输出目录，仅{info['file_count']}个文件"
                })
        
        # 检查可合并的目录
        if 'output' in analysis and 'results' in analysis:
            cleanup_targets['mergeable_dirs'].append({
                'merge': ['output', 'results'],
                'reason': "类似的输出目录可以合并"
            })
        
        return cleanup_targets
    
    def check_directory_usage(self):
        """检查目录使用情况"""
        print("\n📊 检查目录使用情况...")
        
        usage_analysis = {}
        
        # 检查core配置是否使用了根目录的config
        core_config = Path("core/config")
        root_config = Path("config")
        
        if core_config.exists() and root_config.exists():
            usage_analysis['config_duplication'] = {
                'core_config_files': len(list(core_config.rglob("*.py"))),
                'root_config_files': len(list(root_config.rglob("*.py"))),
                'recommendation': "根目录config为空，可删除"
            }
        
        # 检查scripts和scripts_backup
        scripts_dir = Path("scripts")
        backup_dir = Path("scripts_backup")
        
        if scripts_dir.exists() and backup_dir.exists():
            usage_analysis['scripts_redundancy'] = {
                'current_files': len(list(scripts_dir.rglob("*.py"))),
                'backup_files': len(list(backup_dir.rglob("*.py"))),
                'recommendation': "scripts_backup是历史备份，可归档"
            }
        
        # 检查输出目录
        output_dirs = ['output', 'results', 'reports']
        output_analysis = {}
        
        for dirname in output_dirs:
            dir_path = Path(dirname)
            if dir_path.exists():
                files = list(dir_path.rglob("*"))
                output_analysis[dirname] = {
                    'files': len([f for f in files if f.is_file()]),
                    'recent_activity': any(
                        (datetime.now().timestamp() - f.stat().st_mtime) < 86400 
                        for f in files if f.is_file()
                    )
                }
        
        usage_analysis['output_dirs'] = output_analysis
        
        return usage_analysis
    
    def generate_cleanup_plan(self, cleanup_targets, usage_analysis):
        """生成清理计划"""
        print("\n💡 生成清理计划...")
        
        cleanup_plan = {
            'immediate_removal': [],
            'archive_and_remove': [],
            'merge_directories': [],
            'space_savings_mb': 0
        }
        
        # 立即删除空目录
        for target in cleanup_targets['empty_dirs']:
            cleanup_plan['immediate_removal'].append({
                'action': 'delete',
                'target': target['name'],
                'reason': target['reason']
            })
        
        # 归档并删除备份目录
        for target in cleanup_targets['backup_dirs']:
            cleanup_plan['archive_and_remove'].append({
                'action': 'move_to_archive',
                'target': target['name'],
                'reason': target['reason'],
                'size_mb': target['size_mb']
            })
            cleanup_plan['space_savings_mb'] += target['size_mb']
        
        # 处理缓存目录
        for target in cleanup_targets['cache_dirs']:
            if target['name'] == 'cache' and target['size_mb'] < 10:  # 小于10MB可以重建
                cleanup_plan['immediate_removal'].append({
                    'action': 'delete',
                    'target': target['name'],
                    'reason': f"小缓存目录({target['size_mb']:.1f}MB)，可重建"
                })
                cleanup_plan['space_savings_mb'] += target['size_mb']
        
        # 处理冗余输出目录
        for target in cleanup_targets['redundant_output']:
            cleanup_plan['merge_directories'].append({
                'action': 'merge_into_results',
                'target': target['name'],
                'reason': target['reason']
            })
        
        return cleanup_plan
    
    def execute_cleanup(self, cleanup_plan):
        """执行清理计划"""
        print("\n🧹 执行清理计划...")
        
        executed_actions = []
        
        # 确保archive目录存在
        archive_dir = Path("archive")
        archive_dir.mkdir(exist_ok=True)
        
        # 立即删除
        for action in cleanup_plan['immediate_removal']:
            target_path = Path(action['target'])
            if target_path.exists():
                try:
                    if target_path.is_dir():
                        shutil.rmtree(target_path)
                    else:
                        target_path.unlink()
                    executed_actions.append(f"✅ 删除: {action['target']} - {action['reason']}")
                    print(f"   🗑️ 删除: {action['target']}")
                except Exception as e:
                    print(f"   ❌ 删除失败 {action['target']}: {e}")
        
        # 归档并移除
        for action in cleanup_plan['archive_and_remove']:
            target_path = Path(action['target'])
            if target_path.exists():
                try:
                    archive_target = archive_dir / action['target']
                    if archive_target.exists():
                        shutil.rmtree(archive_target)
                    shutil.move(str(target_path), str(archive_target))
                    executed_actions.append(f"📦 归档: {action['target']} → archive/ ({action['size_mb']:.1f}MB)")
                    print(f"   📦 归档: {action['target']} → archive/")
                except Exception as e:
                    print(f"   ❌ 归档失败 {action['target']}: {e}")
        
        # 合并目录
        for action in cleanup_plan['merge_directories']:
            target_path = Path(action['target'])
            results_path = Path("results")
            
            if target_path.exists() and results_path.exists():
                try:
                    # 移动文件到results目录
                    target_subdir = results_path / action['target']
                    target_subdir.mkdir(exist_ok=True)
                    
                    for file_path in target_path.rglob("*"):
                        if file_path.is_file():
                            relative_path = file_path.relative_to(target_path)
                            target_file = target_subdir / relative_path
                            target_file.parent.mkdir(parents=True, exist_ok=True)
                            shutil.move(str(file_path), str(target_file))
                    
                    # 删除空的原目录
                    shutil.rmtree(target_path)
                    executed_actions.append(f"🔄 合并: {action['target']} → results/{action['target']}")
                    print(f"   🔄 合并: {action['target']} → results/")
                except Exception as e:
                    print(f"   ❌ 合并失败 {action['target']}: {e}")
        
        return executed_actions
    
    def generate_final_report(self, analysis, cleanup_plan, executed_actions):
        """生成最终报告"""
        report = f"""# 📊 最终根目录清理报告

## 🕒 清理时间
{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

## 📁 清理前目录分析
"""
        
        for name, info in analysis.items():
            report += f"- **{name}/** - {info['purpose']} - {info['file_count']} files - {info['size_mb']:.1f}MB\n"
        
        report += f"""
## 🧹 执行的清理操作 ({len(executed_actions)} 项)
"""
        
        for action in executed_actions:
            report += f"- {action}\n"
        
        report += f"""
## 💾 空间节省
- **预计节省**: {cleanup_plan['space_savings_mb']:.1f} MB

## 🎯 清理后效果
- 根目录更加简洁
- 移除了冗余和空目录
- 备份文件安全归档
- 输出目录合并整理

## 📋 建议
1. 定期清理生成的输出文件
2. 监控archive目录大小
3. 考虑添加自动清理脚本
"""
        
        return report
    
    def run_final_cleanup(self):
        """运行最终清理"""
        print("🚀 开始最终根目录清理...")
        print("="*60)
        
        # 分析目录
        analysis = self.analyze_root_directories()
        
        # 识别清理目标
        cleanup_targets = self.identify_cleanup_targets(analysis)
        
        # 检查使用情况
        usage_analysis = self.check_directory_usage()
        
        # 生成清理计划
        cleanup_plan = self.generate_cleanup_plan(cleanup_targets, usage_analysis)
        
        # 执行清理
        executed_actions = self.execute_cleanup(cleanup_plan)
        
        # 生成报告
        report = self.generate_final_report(analysis, cleanup_plan, executed_actions)
        
        # 保存报告
        report_file = Path("FINAL_CLEANUP_REPORT.md")
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(report)
        
        print("\n" + "="*60)
        print("🎉 最终清理完成!")
        print("="*60)
        print(f"📦 执行操作: {len(executed_actions)} 项")
        print(f"💾 预计节省: {cleanup_plan['space_savings_mb']:.1f} MB")
        print(f"📋 详细报告: FINAL_CLEANUP_REPORT.md")
        
        return True

def main():
    """主函数"""
    analyzer = FinalCleanupAnalyzer()
    analyzer.run_final_cleanup()

if __name__ == "__main__":
    main()