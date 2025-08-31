#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据模块初始化文件 - data/__init__.py
=====================================

数据模块是量化交易框架的核心组件，负责统一管理所有数据相关功能：

📁 数据模块结构:
├── data_loader.py           # 数据获取器 - 从优矿API等数据源获取原始数据
├── data_processor.py        # 数据预处理 - 清洗、标准化、筛选股票池
├── feature_engineer.py      # 特征工程 - 技术指标、因子特征生成
├── data_manager.py          # 数据管理器 - 统一协调各组件的数据流水线
└── __init__.py              # 本文件 - 模块初始化和导出接口

💡 设计特点:
- 🔗 统一的数据接口，屏蔽底层实现细节
- 🚀 高效的缓存机制，避免重复数据获取
- 🛡️ 完善的错误处理和数据质量检查
- 🎯 模块化设计，支持独立使用各个组件
- 📊 丰富的特征工程功能，支持多种技术指标

📋 使用示例:
```python
# 导入数据模块
from data import DataManager, DataLoader, DataProcessor, FeatureEngineer

# 方式1: 使用统一的数据管理器 (推荐)
dm = DataManager()
result = dm.run_complete_pipeline()
features = result['features']

# 方式2: 分步骤处理
loader = DataLoader()
raw_data = loader.load_price_data()

processor = DataProcessor() 
clean_data = processor.clean_price_data(raw_data)

engineer = FeatureEngineer(clean_data)
features = engineer.generate_all_features()
```

🔧 环境要求:
- Python 3.8+
- pandas, numpy, scipy
- talib (技术指标计算)
- uqer (优矿API，可选)

版本: 2.0.0
更新: 2024-08-26 (修复.py格式转换)
"""

import sys
import warnings
from typing import Dict, List, Optional, Any
from datetime import datetime

# 忽略警告信息
warnings.filterwarnings('ignore')

print("🚀 量化交易框架 - 数据模块初始化")
print("=" * 50)
print(f"📅 初始化时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"🐍 Python版本: {sys.version.split()[0]}")

# 检查必要的依赖包
required_packages = {
    'pandas': '数据处理',
    'numpy': '数值计算', 
    'scipy': '科学计算',
    'pathlib': '路径处理'
}

optional_packages = {
    'talib': '技术指标计算',
    'uqer': '优矿API数据源'
}

print("\n📦 检查依赖包...")
missing_required = []
missing_optional = []

# 检查必需包
for package, description in required_packages.items():
    try:
        __import__(package)
        print(f"✅ {package:<12} - {description}")
    except ImportError:
        print(f"❌ {package:<12} - {description} (未安装)")
        missing_required.append(package)

# 检查可选包
for package, description in optional_packages.items():
    try:
        __import__(package)
        print(f"✅ {package:<12} - {description}")
    except ImportError:
        print(f"⚠️ {package:<12} - {description} (可选,未安装)")
        missing_optional.append(package)

if missing_required:
    print(f"\n❌ 缺少必需依赖: {', '.join(missing_required)}")
    print("💡 请运行: pip install pandas numpy scipy")

if missing_optional:
    print(f"\n⚠️ 缺少可选依赖: {', '.join(missing_optional)}")
    print("💡 如需完整功能，请运行: pip install talib uqer")
    print("   注：TA-Lib可能需要系统级安装，详见官方文档")

# ===========================================
# 🔧 导入数据模块组件
# ===========================================

print("\n🔧 导入数据模块组件...")

# 导入配置
try:
    from config.settings import Config
    print("✅ Config - 配置模块已加载")
    _config_available = True
except ImportError:
    print("⚠️ Config - 配置模块未找到，将使用默认配置")
    # 创建默认配置类
    class Config:
        UQER_TOKEN = ""
        START_DATE = '2020-01-01'
        END_DATE = '2024-08-20'
        UNIVERSE = 'CSI300'
        INDEX_CODE = '000300'
        CACHE_DIR = './cache'
        ENABLE_CACHE = True
        CACHE_EXPIRE_HOURS = 24
    _config_available = False

# 导入核心组件
try:
    # 导入数据加载器
    try:
        from .data_loader import DataLoader, create_data_loader
        print("📥 DataLoader - 数据获取器 (准备就绪)")
        _loader_available = True
    except ImportError as e:
        print(f"⚠️ DataLoader - 数据获取器导入失败: {e}")
        _loader_available = False

    # 导入数据预处理器
    try:
        from .data_processor import DataProcessor, create_data_processor
        print("🧹 DataProcessor - 数据预处理器 (准备就绪)")
        _processor_available = True
    except ImportError as e:
        print(f"⚠️ DataProcessor - 数据预处理器导入失败: {e}")
        _processor_available = False

    # 导入特征工程器
    try:
        from .feature_engineer import FeatureEngineer, create_feature_engineer
        print("🔬 FeatureEngineer - 特征工程器 (准备就绪)")
        _engineer_available = True
    except ImportError as e:
        print(f"⚠️ FeatureEngineer - 特征工程器导入失败: {e}")
        _engineer_available = False

    # 导入数据管理器
    try:
        from .data_manager import DataManager, create_data_manager
        print("🎯 DataManager - 数据管理器 (准备就绪)")
        _manager_available = True
    except ImportError as e:
        print(f"⚠️ DataManager - 数据管理器导入失败: {e}")
        _manager_available = False

except Exception as e:
    print(f"❌ 模块导入过程中发生错误: {e}")
    # 设置所有组件为不可用
    _loader_available = False
    _processor_available = False
    _engineer_available = False
    _manager_available = False

print("\n✅ 数据模块组件导入完成!")

# ===========================================
# 🏭 创建工厂函数和便捷接口
# ===========================================

def create_data_loader_safe(config: Optional[Config] = None, **kwargs):
    """
    创建数据加载器实例（安全版本）
    
    Args:
        config: 配置对象
        **kwargs: 其他参数
        
    Returns:
        DataLoader实例，如果不可用则返回None
    """
    if not _loader_available:
        print("❌ DataLoader模块不可用")
        return None
    
    try:
        return create_data_loader(config, **kwargs)
    except Exception as e:
        print(f"❌ DataLoader创建失败: {e}")
        return None

def create_data_processor_safe(config: Optional[Dict] = None, **kwargs):
    """
    创建数据预处理器实例（安全版本）
    
    Args:
        config: 配置字典
        **kwargs: 其他参数
        
    Returns:
        DataProcessor实例，如果不可用则返回None
    """
    if not _processor_available:
        print("❌ DataProcessor模块不可用")
        return None
    
    try:
        return create_data_processor(config, **kwargs)
    except Exception as e:
        print(f"❌ DataProcessor创建失败: {e}")
        return None

def create_feature_engineer_safe(price_data=None, config: Optional[Dict] = None, **kwargs):
    """
    创建特征工程器实例（安全版本）
    
    Args:
        price_data: 价格数据DataFrame
        config: 配置字典
        **kwargs: 其他参数
        
    Returns:
        FeatureEngineer实例，如果不可用则返回None
    """
    if not _engineer_available:
        print("❌ FeatureEngineer模块不可用")
        return None
    
    try:
        return create_feature_engineer(price_data, config, **kwargs)
    except Exception as e:
        print(f"❌ FeatureEngineer创建失败: {e}")
        return None

def create_data_manager_safe(config: Optional[Dict] = None, **kwargs):
    """
    创建数据管理器实例（安全版本，推荐的统一入口）
    
    Args:
        config: 配置字典
        **kwargs: 其他参数
        
    Returns:
        DataManager实例，如果不可用则返回None
    """
    if not _manager_available:
        print("❌ DataManager模块不可用")
        return None
    
    try:
        return create_data_manager(config, **kwargs)
    except Exception as e:
        print(f"❌ DataManager创建失败: {e}")
        return None

print("✅ 工厂函数创建完成")

# ===========================================
# 🛠️ 实用工具函数
# ===========================================

def get_module_status() -> Dict[str, Any]:
    """
    获取数据模块各组件的状态信息
    
    Returns:
        包含各组件状态的字典
    """
    return {
        'module_name': 'data',
        'version': '2.0.0',
        'init_time': datetime.now().isoformat(),
        'components': {
            'config': _config_available,
            'data_loader': _loader_available,
            'data_processor': _processor_available,
            'feature_engineer': _engineer_available,
            'data_manager': _manager_available
        },
        'dependencies': {
            'required': {pkg: pkg not in missing_required for pkg in required_packages.keys()},
            'optional': {pkg: pkg not in missing_optional for pkg in optional_packages.keys()}
        },
        'ready': _manager_available and _loader_available and _processor_available
    }

def validate_data_pipeline() -> bool:
    """
    验证数据流水线是否就绪
    
    Returns:
        True如果流水线就绪，否则False
    """
    status = get_module_status()
    
    # 检查核心组件
    core_ready = (
        status['components']['data_loader'] and
        status['components']['data_processor'] and
        status['components']['feature_engineer'] and
        status['components']['data_manager']
    )
    
    # 检查必需依赖
    deps_ready = isinstance(status['dependencies']['required'], dict) and all(status['dependencies']['required'].values())
    
    pipeline_ready = core_ready and deps_ready
    
    print(f"🔍 数据流水线验证: {'✅ 就绪' if pipeline_ready else '❌ 未就绪'}")
    
    if not pipeline_ready:
        print("⚠️ 问题详情:")
        if not core_ready:
            print("   - 核心组件不完整")
            for comp, status in status['components'].items():
                if not status:
                    print(f"     * {comp}: 不可用")
        if not deps_ready:
            print("   - 缺少必需依赖")
            if isinstance(status['dependencies']['required'], dict):
                for pkg, available in status['dependencies']['required'].items():
                    if not available:
                        print(f"     * {pkg}: 未安装")
            else:
                print("     * 依赖信息不可用")
    
    return pipeline_ready

def get_quick_start_guide() -> str:
    """
    获取快速开始指南
    
    Returns:
        快速开始指南文本
    """
    guide = f"""
🚀 数据模块快速开始指南
{'='*50}

📋 1. 检查模块状态:
```python
from data import get_module_status, validate_data_pipeline

# 查看详细状态
status = get_module_status()
print(status)

# 验证流水线
is_ready = validate_data_pipeline()
```

🎯 2. 使用数据管理器 (推荐):
```python
from data import create_data_manager_safe

# 创建数据管理器
dm = create_data_manager_safe()

if dm:
    # 运行完整流水线
    results = dm.run_complete_pipeline()
    
    if 'features' in results:
        features = results['features']
        print(f"特征数据: {features.shape}")
```

📊 3. 分步骤使用:
```python
from data import (create_data_loader_safe, create_data_processor_safe, 
                 create_feature_engineer_safe)

# 步骤1: 数据获取
loader = create_data_loader_safe()
raw_data = loader.get_complete_dataset() if loader else None

# 步骤2: 数据预处理
processor = create_data_processor_safe()
clean_data = processor.clean_price_data(raw_data['price_data']) if processor and raw_data else None

# 步骤3: 特征工程
engineer = create_feature_engineer_safe(clean_data)
features = engineer.generate_all_features() if engineer else None
```

📊 查看模块状态:
```python
from data import get_module_status, validate_data_pipeline

# 查看状态
status = get_module_status()
print(status)

# 验证流水线
is_ready = validate_data_pipeline()
```

💡 更多信息请查看各模块的详细文档。
    """
    return guide

print("✅ 工具函数创建完成")

# ===========================================
# 📤 模块导出和版本信息
# ===========================================

# 版本信息
__version__ = "2.0.0"
__author__ = "QuantTrader"
__description__ = "量化交易框架数据模块 - 统一数据获取、处理和特征工程"

# 主要导出的类和函数
__all__ = [
    # 核心类 (如果可用)
    'Config',
    
    # 工厂函数
    'create_data_loader_safe',
    'create_data_processor_safe',
    'create_feature_engineer_safe', 
    'create_data_manager_safe',
    
    # 工具函数
    'get_module_status',
    'validate_data_pipeline',
    'get_quick_start_guide',
    
    # 版本信息
    '__version__',
    '__author__',
    '__description__'
]

# 动态添加可用的类到导出列表
if _loader_available:
    __all__.extend(['DataLoader', 'create_data_loader'])
    
if _processor_available:
    __all__.extend(['DataProcessor', 'create_data_processor'])
    
if _engineer_available:
    __all__.extend(['FeatureEngineer', 'create_feature_engineer'])
    
if _manager_available:
    __all__.extend(['DataManager', 'create_data_manager'])

# 创建全局实例 (如果可用的话)
try:
    # 创建默认配置实例
    default_config = Config()
    print(f"✅ 默认配置实例已创建")
    
    # 可以选择性地创建全局数据管理器实例
    if _manager_available:
        global_data_manager = create_data_manager_safe(config={'cache_dir': './cache'})
        if global_data_manager:
            print("✅ 全局数据管理器实例已创建")
    
except Exception as e:
    print(f"⚠️ 创建全局实例时出现警告: {e}")

print(f"\n📋 数据模块信息:")
print(f"   📦 模块版本: {__version__}")
print(f"   👤 开发者: {__author__}")
print(f"   📝 描述: {__description__}")

print(f"\n📊 模块状态总结:")
available_components = sum([_config_available, _loader_available, _processor_available, _engineer_available, _manager_available])
print(f"   🔧 可用组件: {available_components}/5")

pipeline_ready = validate_data_pipeline()
print(f"   🚀 流水线状态: {'就绪' if pipeline_ready else '需要修复'}")

print(f"\n{'='*60}")
print("🎊 数据模块初始化完成!")
print(f"{'='*60}")
print(f"⏰ 完成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

print(f"\n📚 可用导入方式:")
if _manager_available:
    print("   from data import create_data_manager_safe")
if _loader_available:
    print("   from data import create_data_loader_safe, create_data_processor_safe")
print("   from data import get_module_status, validate_data_pipeline")

print(f"\n💡 快速开始:")
print("   运行 get_quick_start_guide() 查看详细使用指南")
print("   运行 validate_data_pipeline() 检查流水线状态")
if missing_optional:
    print(f"   可选依赖: {', '.join(missing_optional)}")
    print("   增强功能: pip install talib uqer")

if pipeline_ready:
    print(f"\n🚀 数据模块已准备就绪，开始您的量化交易之旅!")
else:
    print(f"\n⚠️ 数据模块需要完善，请查看上述状态信息")

print(f"{'='*60}")

# 运行时检查和提示
if __name__ == "__main__":
    # 如果直接运行此模块，显示状态报告
    print("\n🔍 直接运行数据模块，显示详细状态...")
    
    status = get_module_status()
    print(f"\n📊 详细状态报告:")
    print(f"   模块名称: {status['module_name']}")
    print(f"   版本: {status['version']}")
    print(f"   初始化时间: {status['init_time']}")
    
    print(f"\n🔧 组件状态:")
    for component, available in status['components'].items():
        print(f"   {component}: {'✅ 可用' if available else '❌ 不可用'}")
    
    print(f"\n📦 依赖状态:")
    print("   必需依赖:")
    if isinstance(status['dependencies']['required'], dict):
        for pkg, available in status['dependencies']['required'].items():
            print(f"     {pkg}: {'✅ 已安装' if available else '❌ 未安装'}")
    else:
        print("     依赖信息不可用")
    print("   可选依赖:")
    if isinstance(status['dependencies']['optional'], dict):
        for pkg, available in status['dependencies']['optional'].items():
            print(f"     {pkg}: {'✅ 已安装' if available else '⚠️ 未安装'}")
    else:
        print("     依赖信息不可用")
    
    # 运行验证
    print(f"\n🔍 运行流水线验证...")
    is_ready = validate_data_pipeline()
    
    if is_ready:
        print(f"\n🎉 数据模块完全就绪!")
        print(f"💡 可以开始使用数据流水线功能")
    else:
        print(f"\n⚠️ 数据模块未完全就绪")
        print(f"💡 请根据上述提示完善环境配置")