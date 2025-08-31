"""
量化交易框架 - 核心模块初始化
=====================================

这是量化交易框架的核心模块，包含以下子模块：

📁 core/
├── config/          # ✅ 配置管理 - 全局设置、交易配置、数据库配置
├── data/            # 🔧 数据处理 - 数据获取、预处理、特征工程
├── strategy/        # 📋 策略模块 - 策略基类、机器学习、技术分析
├── backtest/        # 📋 回测引擎 - 回测执行、性能分析、风险管理
├── visualization/   # 📋 可视化 - 图表生成、交互面板、报告模板
└── utils/           # 📋 工具模块 - 日志系统、装饰器、验证器

🎯 设计原则：
- 模块化设计，各组件独立可用
- 统一的接口和配置管理
- 完善的错误处理和日志记录
- 高性能和可扩展性

🚀 快速开始：
```python
# 导入核心模块
from core import Config, DataManager, BacktestEngine

# 初始化配置
config = Config()

# 创建数据管理器
data_manager = DataManager(config)

# 运行完整流水线
result = data_manager.run_complete_pipeline()
```

👤 开发者: QuantTrader Team
📦 版本: 1.0.0
📅 更新时间: 2025-08-26
"""

import sys
import warnings
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Any, Union

# 忽略常见警告
warnings.filterwarnings('ignore', category=UserWarning)
warnings.filterwarnings('ignore', category=FutureWarning)

print("🚀 量化交易框架 - 核心模块初始化")
print("=" * 60)
print(f"📅 初始化时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"🐍 Python版本: {sys.version.split()[0]}")
print(f"📂 工作目录: {Path.cwd()}")

# ===========================================
# 📋 模块状态检查和导入
# ===========================================

# 定义模块状态
MODULE_STATUS = {
    'config': {
        'status': '✅',
        'description': '配置管理模块',
        'components': ['Settings', 'TradingConfig', 'DatabaseConfig']
    },
    'data': {
        'status': '🔧', 
        'description': '数据处理模块',
        'components': ['DataLoader', 'DataProcessor', 'FeatureEngineer', 'DataManager']
    },
    'strategy': {
        'status': '📋',
        'description': '策略模块',
        'components': ['BaseStrategy', 'MLStrategy', 'TechnicalStrategy', 'FactorStrategy']
    },
    'backtest': {
        'status': '📋',
        'description': '回测引擎',
        'components': ['BacktestEngine', 'PerformanceAnalyzer', 'RiskManager']
    },
    'visualization': {
        'status': '📋',
        'description': '可视化模块',
        'components': ['Charts', 'Dashboard', 'Reports']
    },
    'utils': {
        'status': '📋',
        'description': '工具模块',
        'components': ['Logger', 'Decorators', 'Validators', 'Helpers']
    }
}

# 尝试导入各个模块
imported_modules = {}
failed_imports = {}

print("\n📦 检查和导入子模块...")

# 1. 配置模块 (已完成)
try:
    from .config import Config, TradingConfig, DatabaseConfig
    imported_modules['config'] = {
        'Config': Config,
        'TradingConfig': TradingConfig, 
        'DatabaseConfig': DatabaseConfig
    }
    print("✅ config       - 配置管理模块 (已导入)")
except ImportError as e:
    failed_imports['config'] = str(e)
    print(f"⚠️ config       - 配置管理模块 (导入失败: {e})")

# 2. 数据模块 (部分完成) 
try:
    from .data import (
        create_data_loader, create_data_processor, 
        create_feature_engineer, create_data_manager,
        get_module_status, validate_data_pipeline
    )
    imported_modules['data'] = {
        'create_data_loader': create_data_loader,
        'create_data_processor': create_data_processor,
        'create_feature_engineer': create_feature_engineer, 
        'create_data_manager': create_data_manager,
        'get_module_status': get_module_status,
        'validate_data_pipeline': validate_data_pipeline
    }
    print("🔧 data         - 数据处理模块 (已导入工厂函数)")
except ImportError as e:
    failed_imports['data'] = str(e)
    print(f"⚠️ data         - 数据处理模块 (导入失败: {e})")

# 3. 策略模块 (待开发)
try:
    from .strategy import BaseStrategy, MLStrategy, TechnicalStrategy
    imported_modules['strategy'] = {
        'BaseStrategy': BaseStrategy,
        'MLStrategy': MLStrategy,
        'TechnicalStrategy': TechnicalStrategy
    }
    print("✅ strategy     - 策略模块 (已导入)")
except ImportError as e:
    failed_imports['strategy'] = str(e)
    print(f"📋 strategy     - 策略模块 (待开发)")

# 4. 回测模块 (待开发)
try:
    from .backtest import BacktestEngine, PerformanceAnalyzer, RiskManager
    imported_modules['backtest'] = {
        'BacktestEngine': BacktestEngine,
        'PerformanceAnalyzer': PerformanceAnalyzer,
        'RiskManager': RiskManager
    }
    print("✅ backtest     - 回测引擎 (已导入)")
except ImportError as e:
    failed_imports['backtest'] = str(e)
    print(f"📋 backtest     - 回测引擎 (待开发)")

# 5. 可视化模块 (待开发)
try:
    from .visualization import Charts, Dashboard, Reports
    imported_modules['visualization'] = {
        'Charts': Charts,
        'Dashboard': Dashboard, 
        'Reports': Reports
    }
    print("✅ visualization - 可视化模块 (已导入)")
except ImportError as e:
    failed_imports['visualization'] = str(e)
    print(f"📋 visualization - 可视化模块 (待开发)")

# 6. 工具模块 (待开发)
try:
    from .utils import Logger, get_logger, validate_data, create_dirs
    imported_modules['utils'] = {
        'Logger': Logger,
        'get_logger': get_logger,
        'validate_data': validate_data,
        'create_dirs': create_dirs
    }
    print("✅ utils        - 工具模块 (已导入)")
except ImportError as e:
    failed_imports['utils'] = str(e)
    print(f"📋 utils        - 工具模块 (待开发)")

# ===========================================
# 🔧 核心功能函数
# ===========================================

def get_framework_status() -> Dict[str, Any]:
    """
    获取整个框架的状态信息
    
    Returns:
        Dict: 包含各模块状态的字典
    """
    status_info = {
        'framework_version': '1.0.0',
        'python_version': sys.version.split()[0],
        'initialization_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'modules': {}
    }
    
    # 统计模块状态
    total_modules = len(MODULE_STATUS)
    available_modules = len(imported_modules)
    pending_modules = len(failed_imports)
    
    status_info['summary'] = {
        'total_modules': total_modules,
        'available_modules': available_modules,
        'pending_modules': pending_modules,
        'completion_rate': f"{available_modules}/{total_modules} ({available_modules/total_modules*100:.1f}%)"
    }
    
    # 详细模块信息
    for module_name, module_info in MODULE_STATUS.items():
        status_info['modules'][module_name] = {
            'status': module_info['status'],
            'description': module_info['description'],
            'components': module_info['components'],
            'available': module_name in imported_modules,
            'import_error': failed_imports.get(module_name, None)
        }
    
    return status_info

def create_framework_instance(config_path: Optional[str] = None) -> Dict[str, Any]:
    """
    创建框架的主要实例
    
    Args:
        config_path: 可选的配置文件路径
        
    Returns:
        Dict: 包含各模块实例的字典
    """
    instances = {}
    
    # 创建配置实例
    if 'config' in imported_modules:
        try:
            Config = imported_modules['config']['Config']
            instances['config'] = Config()
            print("✅ 配置实例创建成功")
        except Exception as e:
            print(f"⚠️ 配置实例创建失败: {e}")
    
    # 创建数据管理器实例
    if 'data' in imported_modules:
        try:
            create_data_manager = imported_modules['data']['create_data_manager']
            instances['data_manager'] = create_data_manager()
            print("✅ 数据管理器实例创建成功")
        except Exception as e:
            print(f"⚠️ 数据管理器实例创建失败: {e}")
    
    return instances

def validate_framework_dependencies() -> Dict[str, bool]:
    """
    验证框架的依赖项
    
    Returns:
        Dict: 依赖项检查结果
    """
    dependencies = {
        # 必需依赖
        'pandas': False,
        'numpy': False,
        'scipy': False,
        'pathlib': False,
        
        # 可选依赖  
        'talib': False,
        'uqer': False,
        'matplotlib': False,
        'seaborn': False,
        'plotly': False,
        'scikit-learn': False
    }
    
    for package in dependencies:
        try:
            __import__(package)
            dependencies[package] = True
        except ImportError:
            dependencies[package] = False
    
    return dependencies

def get_quick_start_guide() -> str:
    """
    获取快速开始指南
    
    Returns:
        str: 使用指南文本
    """
    guide = """
🚀 量化交易框架 - 快速开始指南
=====================================

📋 1. 基础使用 (配置模块):
```python
from core import Config, TradingConfig, DatabaseConfig

# 创建配置实例
config = Config()
trading_config = TradingConfig()
db_config = DatabaseConfig()

# 查看配置
print(f"起始日期: {config.START_DATE}")
print(f"手续费率: {trading_config.COMMISSION_RATE}")
```

📊 2. 数据处理流水线:
```python
from core import create_data_manager

# 创建数据管理器
dm = create_data_manager()

# 运行完整数据流水线
result = dm.run_complete_pipeline()
features = result['features']
```

🔍 3. 查看框架状态:
```python  
from core import get_framework_status

# 获取状态信息
status = get_framework_status()
print(f"完成度: {status['summary']['completion_rate']}")
```

⚡ 4. 创建框架实例:
```python
from core import create_framework_instance

# 创建主要实例
instances = create_framework_instance()
config = instances.get('config')
data_manager = instances.get('data_manager')
```

💡 5. 依赖检查:
```python
from core import validate_framework_dependencies

# 检查依赖
deps = validate_framework_dependencies()
missing = [k for k, v in deps.items() if not v]
print(f"缺少依赖: {missing}")
```

🔧 更多信息请查看各模块的详细文档。
    """
    return guide

print("✅ 核心功能函数创建完成")

# ===========================================
# 📤 模块导出和版本信息  
# ===========================================

# 版本信息
__version__ = "1.0.0"
__author__ = "QuantTrader Team"  
__description__ = "量化交易框架核心模块 - 统一管理配置、数据、策略、回测和可视化"
__updated__ = "2025-08-26"

# 导出的主要函数和类
__all__ = [
    # 核心功能函数
    'get_framework_status',
    'create_framework_instance', 
    'validate_framework_dependencies',
    'get_quick_start_guide',
    
    # 版本信息
    '__version__',
    '__author__',
    '__description__',
    '__updated__'
]

# 动态添加可用的模块导出
for module_name, module_items in imported_modules.items():
    for item_name in module_items.keys():
        __all__.append(item_name)

# 创建默认实例
try:
    default_instances = create_framework_instance()
    if default_instances:
        print("✅ 默认框架实例创建成功")
except Exception as e:
    print(f"⚠️ 创建默认实例时出现警告: {e}")

# 状态总结
print(f"\n📋 核心模块信息:")
print(f"   📦 框架版本: {__version__}")
print(f"   👤 开发团队: {__author__}")
print(f"   📝 描述: {__description__}")
print(f"   📅 更新时间: {__updated__}")

framework_status = get_framework_status()
summary = framework_status['summary']
print(f"\n📊 框架状态总结:")
print(f"   🔧 可用模块: {summary['available_modules']}/{summary['total_modules']}")
print(f"   📈 完成度: {summary['completion_rate']}")

# 检查依赖
dependencies = validate_framework_dependencies()
missing_required = [k for k, v in dependencies.items() if not v and k in ['pandas', 'numpy', 'scipy']]
missing_optional = [k for k, v in dependencies.items() if not v and k not in ['pandas', 'numpy', 'scipy']]

if missing_required:
    print(f"   ⚠️ 缺少必需依赖: {', '.join(missing_required)}")
    print(f"   💡 安装命令: pip install {' '.join(missing_required)}")

if missing_optional:
    print(f"   📦 可选依赖: {', '.join(missing_optional)}")
    
if not missing_required:
    print("   ✅ 核心依赖检查通过")

print("\n" + "=" * 60)
print("🎊 量化交易框架核心模块初始化完成!")
print("=" * 60)
print(f"⏰ 完成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

print(f"\n📚 可用导入方式:")
print(f"   from core import get_framework_status")
print(f"   from core import create_framework_instance")
print(f"   from core import validate_framework_dependencies")

if 'config' in imported_modules:
    print(f"   from core import Config, TradingConfig, DatabaseConfig")

if 'data' in imported_modules:
    print(f"   from core import create_data_manager, validate_data_pipeline")

print(f"\n💡 快速开始:")
print(f"   运行 get_quick_start_guide() 查看详细使用指南")
print(f"   运行 get_framework_status() 查看框架状态")
print(f"   运行 validate_framework_dependencies() 检查依赖")

if failed_imports:
    print(f"\n🔧 待开发模块: {', '.join(failed_imports.keys())}")

print(f"\n🚀 量化交易框架已准备就绪，开始您的量化交易之旅!")
print("=" * 60)