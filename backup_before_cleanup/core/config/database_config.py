#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据库配置
"""

from pathlib import Path

class DatabaseConfig:
    """数据库配置"""
    
    def __init__(self):
        # 数据库类型
        self.DB_TYPE = 'sqlite'  # sqlite/mysql/postgresql
        
        # SQLite配置
        self.SQLITE_PATH = Path('./data/quant.db')
        
        # MySQL配置（示例）
        self.MYSQL_HOST = 'localhost'
        self.MYSQL_PORT = 3306
        self.MYSQL_USER = 'root'
        self.MYSQL_PASSWORD = ''
        self.MYSQL_DATABASE = 'quantdb'
        
        # 连接池配置
        self.POOL_SIZE = 5
        self.MAX_OVERFLOW = 10
        
        # 其他
        self.ECHO = False  # 是否打印SQL语句
