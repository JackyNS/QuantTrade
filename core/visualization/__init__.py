#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
可视化模块初始化文件 - visualization/__init__.py
================================================

可视化模块是量化交易框架的重要组件，负责数据可视化和报告生成：

📁 可视化模块结构:
├── charts.py        # 图表生成器 - K线图、技术指标、收益曲线等
├── dashboard.py     # 交互式面板 - 实时监控、策略分析、绩效展示
├── reports.py       # 报告模板 - PDF/HTML报告、Excel导出
└── __init__.py      # 本文件 - 模块初始化和导出接口

💡 设计特点:
- 🎨 丰富的图表类型，支持多种可视化需求
- 🌐 基于Plotly的交互式图表
- 📊 专业的金融图表样式
- 📈 实时数据更新支持
- 📋 自动化报告生成

📋 使用示例:
```python
# 导入可视化模块
from core.visualization import Charts, Dashboard, Reports

# 创建图表生成器
charts = Charts()
fig = charts.plot_candlestick(data)
fig.show()

# 启动交互式面板
dashboard = Dashboard()
dashboard.launch()

# 生成报告
reports = Reports()
reports.generate_backtest_report(results)
```

版本: 1.0.0
更新: 2025-08-29
"""

import sys
import warnings
from typing import Dict, List, Optional, Any, Union
from datetime import datetime
from pathlib import Path

# 忽略警告信息
warnings.filterwarnings('ignore')

print("🎨 量化交易框架 - 可视化模块初始化")
print("=" * 50)
print(f"📅 初始化时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"🐍 Python版本: {sys.version.split()[0]}")

# ==========================================
# 📦 检查依赖包
# ==========================================

required_packages = {
    'plotly': '交互式图表',
    'matplotlib': '静态图表',
    'seaborn': '统计图表',
    'pandas': '数据处理'
}

available_packages = {}
missing_packages = []

for package, description in required_packages.items():
    try:
        __import__(package)
        available_packages[package] = description
    except ImportError:
        missing_packages.append(package)

if available_packages:
    print("\n✅ 可用的依赖包:")
    for pkg, desc in available_packages.items():
        print(f"   - {pkg}: {desc}")

if missing_packages:
    print("\n⚠️ 缺少的依赖包:")
    for pkg in missing_packages:
        print(f"   - {pkg}")
    print(f"\n💡 安装命令: pip install {' '.join(missing_packages)}")

# ==========================================
# 🔧 模块导入
# ==========================================

print("\n📦 导入子模块...")

# 导入核心组件
try:
    from .charts import Charts
    print("✅ charts.py    - 图表生成器 (已导入)")
except ImportError as e:
    print(f"⚠️ charts.py    - 图表生成器 (导入失败: {e})")
    Charts = None

try:
    from .dashboard import Dashboard
    print("✅ dashboard.py - 交互式面板 (已导入)")
except ImportError as e:
    print(f"⚠️ dashboard.py - 交互式面板 (导入失败: {e})")
    Dashboard = None

try:
    from .reports import Reports
    print("✅ reports.py   - 报告模板 (已导入)")
except ImportError as e:
    print(f"⚠️ reports.py   - 报告模板 (导入失败: {e})")
    Reports = None

# ==========================================
# 🏭 工厂函数
# ==========================================

def create_charts(config: Optional[Dict] = None) -> 'Charts':
    """
    创建图表生成器实例
    
    Args:
        config: 可选的配置字典
        
    Returns:
        Charts: 图表生成器实例
    """
    if Charts is None:
        raise ImportError("Charts类未能成功导入，请检查charts.py文件")
    
    return Charts(config)

def create_dashboard(config: Optional[Dict] = None) -> 'Dashboard':
    """
    创建交互式面板实例
    
    Args:
        config: 可选的配置字典
        
    Returns:
        Dashboard: 交互式面板实例
    """
    if Dashboard is None:
        raise ImportError("Dashboard类未能成功导入，请检查dashboard.py文件")
    
    return Dashboard(config)

def create_reports(config: Optional[Dict] = None) -> 'Reports':
    """
    创建报告生成器实例
    
    Args:
        config: 可选的配置字典
        
    Returns:
        Reports: 报告生成器实例
    """
    if Reports is None:
        raise ImportError("Reports类未能成功导入，请检查reports.py文件")
    
    return Reports(config)

# ==========================================
# 📊 模块状态检查
# ==========================================

def get_module_status() -> Dict[str, Any]:
    """
    获取可视化模块状态
    
    Returns:
        Dict: 模块状态信息
    """
    status = {
        'module': 'visualization',
        'version': '1.0.0',
        'components': {
            'Charts': Charts is not None,
            'Dashboard': Dashboard is not None,
            'Reports': Reports is not None
        },
        'dependencies': {
            pkg: pkg in available_packages 
            for pkg in required_packages
        },
        'status': 'operational' if all([
            Charts is not None,
            Dashboard is not None,
            Reports is not None
        ]) else 'partial'
    }
    
    return status

# ==========================================
# 📤 模块导出
# ==========================================

__version__ = "1.0.0"
__author__ = "QuantTrader Team"
__description__ = "量化交易框架可视化模块 - 图表、面板和报告生成"

__all__ = [
    # 核心类
    'Charts',
    'Dashboard',
    'Reports',
    
    # 工厂函数
    'create_charts',
    'create_dashboard',
    'create_reports',
    
    # 状态检查
    'get_module_status',
    
    # 版本信息
    '__version__',
    '__author__',
    '__description__'
]

# 创建默认输出目录
output_dirs = [
    Path('./results/charts'),
    Path('./results/reports'),
    Path('./results/dashboard')
]

for dir_path in output_dirs:
    if not dir_path.exists():
        dir_path.mkdir(parents=True, exist_ok=True)
        print(f"📁 创建目录: {dir_path}")

# 显示模块状态
status = get_module_status()
print(f"\n📊 可视化模块状态:")
print(f"   📦 版本: {status['version']}")
print(f"   🔧 状态: {status['status']}")
print(f"   ✅ 可用组件: {sum(status['components'].values())}/3")

print("\n" + "=" * 50)
print("🎊 可视化模块初始化完成!")