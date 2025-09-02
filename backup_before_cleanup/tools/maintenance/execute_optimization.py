#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
自动执行项目优化 - 无需用户确认
"""

from optimize_project_structure import ProjectOptimizer

def main():
    """自动执行优化"""
    print("🚀 自动执行项目结构优化...")
    
    optimizer = ProjectOptimizer()
    success = optimizer.optimize()
    
    if success:
        print("\n✅ 项目结构优化完成!")
        print("📋 查看详情: PROJECT_OPTIMIZATION_SUMMARY.md")
    else:
        print("\n❌ 优化过程中出现问题")

if __name__ == "__main__":
    main()