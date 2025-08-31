#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
工具模块初始化文件 - utils/__init__.py
=====================================

工具模块为量化交易框架提供基础设施支持：

📁 工具模块结构:
├── logger.py          # 日志系统 - 统一的日志记录和管理
├── decorators.py      # 装饰器集合 - 性能监控、缓存、重试等
├── validators.py      # 数据验证 - 输入验证、数据质量检查
├── helpers.py         # 辅助函数 - 通用工具函数
├── exceptions.py      # 自定义异常 - 框架特定的异常类
└── __init__.py        # 本文件 - 模块初始化和导出接口

💡 设计特点:
- 🔧 提供框架级的基础设施
- 📊 统一的日志和监控
- 🛡️ 完善的数据验证
- 🚀 性能优化装饰器
- 📝 详细的错误信息

📋 使用示例:
```python
# 导入工具模块
from core.utils import get_logger, timeit, validate_dataframe

# 使用日志
logger = get_logger(__name__)
logger.info("开始处理数据")

# 使用装饰器
@timeit
@retry(max_attempts=3)
def process_data(df):
    return df.dropna()

# 使用验证器
is_valid = validate_dataframe(df, required_columns=['open', 'close'])
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

print("🔧 量化交易框架 - 工具模块初始化")
print("=" * 50)
print(f"📅 初始化时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# ==========================================
# 模块导入和状态检查
# ==========================================

# 存储导入的组件
imported_components = {}
failed_imports = {}

# 1. 导入日志系统
try:
    from .logger import (
        Logger, 
        get_logger, 
        setup_logger,
        set_log_level,
        add_file_handler,
        get_log_stats
    )
    imported_components['logger'] = {
        'Logger': Logger,
        'get_logger': get_logger,
        'setup_logger': setup_logger,
        'set_log_level': set_log_level,
        'add_file_handler': add_file_handler,
        'get_log_stats': get_log_stats
    }
    print("✅ 日志系统加载成功")
except ImportError as e:
    failed_imports['logger'] = str(e)
    print(f"❌ 日志系统加载失败: {e}")

# 2. 导入装饰器
try:
    from .decorators import (
        timeit,
        retry,
        cache_result,
        validate_input,
        log_execution,
        rate_limit,
        deprecated,
        async_timeit
    )
    imported_components['decorators'] = {
        'timeit': timeit,
        'retry': retry,
        'cache_result': cache_result,
        'validate_input': validate_input,
        'log_execution': log_execution,
        'rate_limit': rate_limit,
        'deprecated': deprecated,
        'async_timeit': async_timeit
    }
    print("✅ 装饰器集合加载成功")
except ImportError as e:
    failed_imports['decorators'] = str(e)
    print(f"❌ 装饰器集合加载失败: {e}")

# 3. 导入验证器
try:
    from .validators import (
        validate_dataframe,
        validate_date_range,
        validate_stock_code,
        validate_price_data,
        validate_config,
        validate_strategy_params,
        DataValidator,
        ConfigValidator
    )
    imported_components['validators'] = {
        'validate_dataframe': validate_dataframe,
        'validate_date_range': validate_date_range,
        'validate_stock_code': validate_stock_code,
        'validate_price_data': validate_price_data,
        'validate_config': validate_config,
        'validate_strategy_params': validate_strategy_params,
        'DataValidator': DataValidator,
        'ConfigValidator': ConfigValidator
    }
    print("✅ 验证器加载成功")
except ImportError as e:
    failed_imports['validators'] = str(e)
    print(f"❌ 验证器加载失败: {e}")

# 4. 导入辅助函数
try:
    from .helpers import (
        create_dirs,
        clean_old_files,
        format_number,
        format_percentage,
        format_large_number,
        convert_to_datetime,
        calculate_trading_days,
        get_stock_name,
        chunk_list,
        parallel_process,
        safe_divide,
        moving_average,
        exponential_smoothing,
        save_json,  # 添加这个
        load_json   # 添加这个
    )
    imported_components['helpers'] = {
        'create_dirs': create_dirs,
        'clean_old_files': clean_old_files,
        'format_number': format_number,
        'format_percentage': format_percentage,
        'format_large_number': format_large_number,
        'convert_to_datetime': convert_to_datetime,
        'calculate_trading_days': calculate_trading_days,
        'get_stock_name': get_stock_name,
        'chunk_list': chunk_list,
        'parallel_process': parallel_process,
        'safe_divide': safe_divide,
        'moving_average': moving_average,
        'exponential_smoothing': exponential_smoothing,
        'save_json': save_json,  # 添加这个
        'load_json': load_json   # 添加这个
    }
    print("✅ 辅助函数加载成功")
except ImportError as e:
    failed_imports['helpers'] = str(e)
    print(f"❌ 辅助函数加载失败: {e}")

# 5. 导入自定义异常
try:
    from .exceptions import (
        QuantFrameworkError,
        ConfigError,
        DataError,
        StrategyError,
        BacktestError,
        ValidationError,
        APIError,
        InsufficientDataError,
        InvalidParameterError,
        ExceptionHandler  # 添加这个
    )
    imported_components['exceptions'] = {
        'QuantFrameworkError': QuantFrameworkError,
        'ConfigError': ConfigError,
        'DataError': DataError,
        'StrategyError': StrategyError,
        'BacktestError': BacktestError,
        'ValidationError': ValidationError,
        'APIError': APIError,
        'InsufficientDataError': InsufficientDataError,
        'InvalidParameterError': InvalidParameterError,
        'ExceptionHandler': ExceptionHandler
    }
    print("✅ 自定义异常加载成功")
except ImportError as e:
    failed_imports['exceptions'] = str(e)
    print(f"❌ 自定义异常加载失败: {e}")

# ==========================================
# 模块状态和信息函数
# ==========================================

def get_module_status() -> Dict[str, Any]:
    """
    获取工具模块的状态信息
    
    Returns:
        Dict: 包含模块状态的字典
    """
    total_components = 5
    loaded_components = len(imported_components)
    
    return {
        'module_name': 'utils',
        'version': '1.0.0',
        'status': '✅ 可用' if loaded_components == total_components else '⚠️ 部分可用',
        'components': {
            'logger': 'logger' in imported_components,
            'decorators': 'decorators' in imported_components,
            'validators': 'validators' in imported_components,
            'helpers': 'helpers' in imported_components,
            'exceptions': 'exceptions' in imported_components
        },
        'loaded_count': loaded_components,
        'total_count': total_components,
        'completion_rate': f"{loaded_components}/{total_components} ({loaded_components/total_components*100:.0f}%)",
        'failed_imports': failed_imports
    }

def validate_utils_module() -> bool:
    """
    验证工具模块是否正常工作
    
    Returns:
        bool: 如果所有组件正常则返回True
    """
    status = get_module_status()
    
    # 检查关键组件
    critical_components = ['logger', 'validators', 'exceptions']
    for comp in critical_components:
        if not status['components'].get(comp, False):
            print(f"❌ 关键组件 {comp} 不可用")
            return False
    
    print("✅ 工具模块验证通过")
    return True

def get_utils_info() -> str:
    """
    获取工具模块的详细信息
    
    Returns:
        str: 模块信息文本
    """
    status = get_module_status()
    
    info = f"""
╔══════════════════════════════════════════╗
║          工具模块 (Utils Module)          ║
╚══════════════════════════════════════════╝

📦 版本: {status['version']}
📊 状态: {status['status']}
🔧 完成度: {status['completion_rate']}

📋 组件状态:
{'='*40}
"""
    
    component_names = {
        'logger': '日志系统',
        'decorators': '装饰器',
        'validators': '验证器',
        'helpers': '辅助函数',
        'exceptions': '异常类'
    }
    
    for comp, name in component_names.items():
        status_icon = "✅" if status['components'].get(comp, False) else "❌"
        info += f"  {status_icon} {name:12} - {comp}.py\n"
    
    if status['failed_imports']:
        info += f"\n⚠️ 导入失败:\n"
        for comp, error in status['failed_imports'].items():
            info += f"  - {comp}: {error}\n"
    
    info += f"""
💡 主要功能:
{'='*40}
  📝 日志管理 - 统一的日志记录和分析
  ⏱️ 性能监控 - 执行时间和资源监控
  🔄 重试机制 - 自动重试失败的操作
  ✅ 数据验证 - 输入和输出验证
  🛡️ 异常处理 - 统一的错误处理
  🔧 工具函数 - 常用的辅助功能

📚 使用示例:
{'='*40}
```python
from core.utils import get_logger, timeit, validate_dataframe

# 创建日志器
logger = get_logger(__name__)

# 使用装饰器
@timeit
def process_data(df):
    logger.info(f"处理 {len(df)} 条数据")
    return df.dropna()

# 验证数据
is_valid = validate_dataframe(df, ['open', 'close'])
```
"""
    
    return info

# ==========================================
# 导出的接口
# ==========================================

# 版本信息
__version__ = "1.0.0"
__author__ = "QuantTrader Team"
__updated__ = "2025-08-29"

# 导出所有成功导入的组件
__all__ = [
    # 模块信息函数
    'get_module_status',
    'validate_utils_module',
    'get_utils_info',
    
    # 版本信息
    '__version__',
    '__author__',
    '__updated__'
]

# 动态添加导入的组件到导出列表
for component_dict in imported_components.values():
    for name in component_dict.keys():
        __all__.append(name)

# ==========================================
# 初始化完成
# ==========================================

# 显示模块状态
status = get_module_status()
print(f"\n📊 工具模块加载完成:")
print(f"   状态: {status['status']}")
print(f"   完成度: {status['completion_rate']}")

if status['loaded_count'] < status['total_count']:
    print(f"   ⚠️ 有 {status['total_count'] - status['loaded_count']} 个组件未加载")

# 创建默认日志器（如果可用）
if 'logger' in imported_components:
    try:
        default_logger = get_logger('quant_framework')
        default_logger.info("工具模块初始化完成")
    except Exception as e:
        print(f"⚠️ 创建默认日志器失败: {e}")

print("=" * 50)
print("🎉 工具模块初始化完成!\n")