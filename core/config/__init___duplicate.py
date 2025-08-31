#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""配置模块初始化"""

try:
    from .settings import Config
    from .trading_config import TradingConfig
    from .database_config import DatabaseConfig
    
    config = Config()
    trading_config = TradingConfig()
    database_config = DatabaseConfig()
    
    __all__ = ['Config', 'TradingConfig', 'DatabaseConfig', 'config', 'trading_config', 'database_config']
    print("✅ 配置模块加载完成")
    
except ImportError as e:
    print(f"⚠️ 配置模块加载出错: {e}")
    
    # 提供默认类
    class Config:
        def __init__(self):
            from pathlib import Path
            self.BASE_DIR = Path.cwd()
            self.DATA_DIR = self.BASE_DIR / 'data'
            self.CACHE_DIR = self.BASE_DIR / 'cache'
            self.enable_cache = True
            self.UQER_TOKEN = ''
        def get(self, key, default=None):
            return getattr(self, key, default)
    
    class TradingConfig:
        def __init__(self):
            self.INITIAL_CAPITAL = 1000000
    
    class DatabaseConfig:
        def __init__(self):
            self.DB_TYPE = 'sqlite'
