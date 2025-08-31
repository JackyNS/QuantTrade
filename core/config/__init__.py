#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
配置模块初始化
"""

# 导入配置类
from .settings import Config
from .trading_config import TradingConfig
from .database_config import DatabaseConfig

# 创建默认实例
config = Config()
trading_config = TradingConfig()
database_config = DatabaseConfig()

# 导出
__all__ = [
    'Config',
    'TradingConfig', 
    'DatabaseConfig',
    'config',
    'trading_config',
    'database_config'
]

print("✅ 配置模块加载完成")
