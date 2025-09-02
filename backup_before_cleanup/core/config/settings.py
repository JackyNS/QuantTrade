#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
基础配置文件
"""

import os
from pathlib import Path
from datetime import datetime

class Config:
    """全局配置类"""
    
    def __init__(self):
        # 基础路径
        self.BASE_DIR = Path.cwd()
        self.DATA_DIR = self.BASE_DIR / 'data'
        self.CACHE_DIR = self.BASE_DIR / 'cache'
        self.LOG_DIR = self.BASE_DIR / 'logs'
        self.RESULT_DIR = self.BASE_DIR / 'results'
        
        # 创建必要的目录
        for dir_path in [self.DATA_DIR, self.CACHE_DIR, self.LOG_DIR, self.RESULT_DIR]:
            dir_path.mkdir(parents=True, exist_ok=True)
        
        # 系统配置
        self.DEBUG = False
        self.LOG_LEVEL = 'INFO'
        self.TIMEZONE = 'Asia/Shanghai'
        
        # API配置
        self.UQER_TOKEN = os.getenv('UQER_TOKEN', '')
        self.UQER_API_BASE = 'https://api.wmcloud.com/data/v1'
        
        # 数据配置
        self.DEFAULT_START_DATE = '2020-01-01'
        self.DEFAULT_END_DATE = datetime.now().strftime('%Y-%m-%d')
        
        # 缓存配置（数据模块需要）
        self.enable_cache = True
        self.cache_expire_hours = 24
        self.MAX_CACHE_SIZE_MB = 1000
        
        # 其他
        self.MAX_WORKERS = 4
        self.CHUNK_SIZE = 100
        
    def validate(self):
        """验证配置"""
        return True
