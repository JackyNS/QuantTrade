#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
回测模块初始化文件 - backtest/__init__.py
=========================================

回测引擎是量化交易框架的核心组件，负责策略的历史回测和性能评估。

📁 回测模块结构:
├── backtest_engine.py        # 回测引擎 - 事件驱动的回测核心
├── performance_analyzer.py   # 性能分析器 - 计算各种性能指标
├── risk_manager.py          # 风险管理器 - 风险控制和评估
├── report_generator.py      # 报告生成器 - 生成回测报告
└── __init__.py             # 本文件 - 模块初始化和导出接口

💡 设计特点:
- 🔄 事件驱动架构，模拟真实交易流程
- 📊 详细的性能指标计算（夏普、最大回撤等）
- 💰 精确的交易成本模拟（手续费、滑点）
- 🛡️ 完善的风险管理机制
- 📈 专业的回测报告生成

📋 使用示例:
```python
from core.backtest import BacktestEngine, PerformanceAnalyzer

# 创建回测引擎
engine = BacktestEngine(
    initial_capital=1000000,
    commission=0.002,
    slippage=0.001
)

# 运行回测
results = engine.run(
    strategy=my_strategy,
    data=historical_data,
    start_date='2023-01-01',
    end_date='2024-01-01'
)

# 分析性能
analyzer = PerformanceAnalyzer(results)
metrics = analyzer.calculate_all_metrics()
```

版本: 1.0.0
更新: 2025-08-29
"""

import sys
import warnings
from typing import Dict, List, Optional, Any
from datetime import datetime
from pathlib import Path

# 忽略警告信息
warnings.filterwarnings('ignore')

print("🚀 量化交易框架 - 回测模块初始化")
print("=" * 50)
print(f"📅 初始化时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"🐍 Python版本: {sys.version.split()[0]}")

# 检查必要的依赖包
required_packages = {
    'pandas': '数据处理',
    'numpy': '数值计算',
    'scipy': '统计分析'
}

missing_packages = []
for package, description in required_packages.items():
    try:
        __import__(package)
        print(f"✅ {package:10} - {description}")
    except ImportError:
        missing_packages.append(package)
        print(f"❌ {package:10} - {description} (未安装)")

if missing_packages:
    print(f"\n⚠️ 警告: 缺少必要的依赖包: {', '.join(missing_packages)}")
    print("请运行: pip install " + " ".join(missing_packages))

# ==========================================
# 📋 模块组件导入
# ==========================================

try:
    from .backtest_engine import BacktestEngine, Event, OrderEvent, FillEvent
    from .performance_analyzer import PerformanceAnalyzer, PerformanceMetrics
    from .risk_manager import RiskManager, RiskMetrics, PositionSizer
    from .report_generator import ReportGenerator, BacktestReport
    
    __all__ = [
        'BacktestEngine',
        'Event',
        'OrderEvent', 
        'FillEvent',
        'PerformanceAnalyzer',
        'PerformanceMetrics',
        'RiskManager',
        'RiskMetrics',
        'PositionSizer',
        'ReportGenerator',
        'BacktestReport'
    ]
    
    print("\n✅ 所有回测组件导入成功")
    
except ImportError as e:
    print(f"\n⚠️ 部分组件导入失败: {e}")
    print("使用工厂函数创建组件...")
    
    # 提供工厂函数作为备选方案
    def create_backtest_engine(**kwargs):
        """创建回测引擎实例"""
        try:
            from .backtest_engine import BacktestEngine
            return BacktestEngine(**kwargs)
        except ImportError:
            print("❌ 回测引擎模块未实现")
            return None
    
    def create_performance_analyzer(results: Dict):
        """创建性能分析器实例"""
        try:
            from .performance_analyzer import PerformanceAnalyzer
            return PerformanceAnalyzer(results)
        except ImportError:
            print("❌ 性能分析器模块未实现")
            return None
    
    def create_risk_manager(**kwargs):
        """创建风险管理器实例"""
        try:
            from .risk_manager import RiskManager
            return RiskManager(**kwargs)
        except ImportError:
            print("❌ 风险管理器模块未实现")
            return None
    
    def create_report_generator(results: Dict):
        """创建报告生成器实例"""
        try:
            from .report_generator import ReportGenerator
            return ReportGenerator(results)
        except ImportError:
            print("❌ 报告生成器模块未实现")
            return None
    
    __all__ = [
        'create_backtest_engine',
        'create_performance_analyzer',
        'create_risk_manager',
        'create_report_generator'
    ]

# ==========================================
# 📊 模块状态检查
# ==========================================

def get_module_status() -> Dict[str, Any]:
    """获取回测模块状态"""
    status = {
        'module': 'backtest',
        'version': '1.0.0',
        'status': 'developing',
        'components': {
            'backtest_engine': False,
            'performance_analyzer': False,
            'risk_manager': False,
            'report_generator': False
        },
        'dependencies': {
            'pandas': 'pandas' in sys.modules,
            'numpy': 'numpy' in sys.modules,
            'scipy': 'scipy' in sys.modules
        }
    }
    
    # 检查各组件是否可用
    try:
        from .backtest_engine import BacktestEngine
        status['components']['backtest_engine'] = True
    except ImportError:
        pass
    
    try:
        from .performance_analyzer import PerformanceAnalyzer
        status['components']['performance_analyzer'] = True
    except ImportError:
        pass
    
    try:
        from .risk_manager import RiskManager
        status['components']['risk_manager'] = True
    except ImportError:
        pass
    
    try:
        from .report_generator import ReportGenerator
        status['components']['report_generator'] = True
    except ImportError:
        pass
    
    # 计算完成度
    total_components = len(status['components'])
    completed_components = sum(status['components'].values())
    status['completion_rate'] = f"{(completed_components/total_components)*100:.0f}%"
    
    return status

# ==========================================
# 🔧 快速开始函数
# ==========================================

def quick_backtest(strategy, data, **kwargs):
    """
    快速回测函数
    
    Args:
        strategy: 策略对象
        data: 历史数据
        **kwargs: 其他回测参数
    
    Returns:
        回测结果字典
    """
    # 默认参数
    default_params = {
        'initial_capital': 1000000,
        'commission': 0.002,
        'slippage': 0.001,
        'start_date': None,
        'end_date': None
    }
    
    # 合并参数
    params = {**default_params, **kwargs}
    
    try:
        # 创建回测引擎
        engine = create_backtest_engine(
            initial_capital=params['initial_capital'],
            commission=params['commission'],
            slippage=params['slippage']
        )
        
        if engine:
            # 运行回测
            results = engine.run(
                strategy=strategy,
                data=data,
                start_date=params['start_date'],
                end_date=params['end_date']
            )
            
            # 分析性能
            analyzer = create_performance_analyzer(results)
            if analyzer:
                metrics = analyzer.calculate_all_metrics()
                results['metrics'] = metrics
            
            # 生成报告
            report_gen = create_report_generator(results)
            if report_gen:
                report = report_gen.generate()
                results['report'] = report
            
            return results
        else:
            print("❌ 回测引擎创建失败")
            return None
            
    except Exception as e:
        print(f"❌ 快速回测失败: {e}")
        return None

# ==========================================
# 📋 模块信息
# ==========================================

print("\n📊 回测模块组件状态:")
status = get_module_status()
for component, available in status['components'].items():
    status_icon = "✅" if available else "🔧"
    print(f"  {status_icon} {component}")

print(f"\n📈 模块完成度: {status['completion_rate']}")
print("=" * 50)

# 模块元数据
__version__ = '1.0.0'
__author__ = 'QuantTrader Team'
__email__ = 'quant@trading.com'
__doc__ = """
回测模块 - 提供完整的策略回测功能

主要功能:
1. 事件驱动的回测引擎
2. 详细的性能指标分析
3. 专业的风险管理
4. 可视化回测报告生成
"""