#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
QuantTrade Scripts 统一入口
==========================

提供所有scripts模块的统一访问接口

使用方法:
python scripts/run_scripts.py [模块] [操作] [选项]

模块列表:
- automation: 自动化任务 (scheduler, alerts, backup)
- reporting: 报告生成 (weekly, monthly, dashboard)
- analysis: 分析工具 (market, portfolio, sector)
- optimization: 优化工具 (allocation, portfolio, risk)
- monitoring: 监控系统 (realtime, performance, system)
- backtest: 回测系统 (engine, batch, analysis)
- trading: 交易系统 (execution, position, management)

示例:
python scripts/run_scripts.py reporting weekly
python scripts/run_scripts.py monitoring realtime --symbol=000001
python scripts/run_scripts.py backtest batch --start=2024-01-01
"""

import sys
import os
import argparse
from pathlib import Path
from importlib import import_module

# 添加项目路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

class ScriptsRunner:
    """Scripts统一运行器"""
    
    def __init__(self):
        self.scripts_dir = Path(__file__).parent
        self.available_modules = self._discover_modules()
    
    def _discover_modules(self):
        """发现可用的模块"""
        modules = {}
        
        for item in self.scripts_dir.iterdir():
            if item.is_dir() and not item.name.startswith('_'):
                scripts = []
                for script in item.glob("*.py"):
                    if script.name != "__init__.py":
                        scripts.append(script.stem)
                
                if scripts:
                    modules[item.name] = scripts
        
        return modules
    
    def list_modules(self):
        """列出所有可用模块"""
        print("📋 可用的Scripts模块:")
        print("=" * 40)
        
        for module_name, scripts in self.available_modules.items():
            print(f"\n📁 {module_name}:")
            for script in scripts:
                print(f"  • {script}")
    
    def run_script(self, module_name, script_name, args=None):
        """运行指定脚本"""
        if module_name not in self.available_modules:
            print(f"❌ 模块 '{module_name}' 不存在")
            self.list_modules()
            return False
        
        if script_name not in self.available_modules[module_name]:
            print(f"❌ 脚本 '{script_name}' 在模块 '{module_name}' 中不存在")
            print(f"可用脚本: {', '.join(self.available_modules[module_name])}")
            return False
        
        try:
            # 动态导入并运行脚本
            module_path = f"scripts.{module_name}.{script_name}"
            script_module = import_module(module_path)
            
            if hasattr(script_module, 'main'):
                # 传递命令行参数
                original_argv = sys.argv
                sys.argv = [script_name] + (args or [])
                
                try:
                    print(f"🚀 运行: {module_name}/{script_name}")
                    result = script_module.main()
                    return result
                finally:
                    sys.argv = original_argv
            else:
                print(f"⚠️ 脚本 {script_name} 没有main()函数")
                return False
                
        except Exception as e:
            print(f"❌ 运行脚本时出错: {e}")
            return False

def main():
    parser = argparse.ArgumentParser(
        description='QuantTrade Scripts统一入口',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    parser.add_argument('module', nargs='?', help='模块名称')
    parser.add_argument('script', nargs='?', help='脚本名称')
    parser.add_argument('--list', '-l', action='store_true', help='列出所有可用模块')
    parser.add_argument('script_args', nargs='*', help='传递给脚本的参数')
    
    args = parser.parse_args()
    
    runner = ScriptsRunner()
    
    if args.list or not args.module:
        runner.list_modules()
        return 0
    
    if not args.script:
        print(f"❌ 请指定要运行的脚本")
        if args.module in runner.available_modules:
            print(f"可用脚本: {', '.join(runner.available_modules[args.module])}")
        return 1
    
    success = runner.run_script(args.module, args.script, args.script_args)
    return 0 if success else 1

if __name__ == "__main__":
    exit(main())
