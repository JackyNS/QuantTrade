#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""数据库配置"""

from pathlib import Path

class DatabaseConfig:
    def __init__(self):
        self.DB_TYPE = 'sqlite'
        self.SQLITE_PATH = Path('./data/quant.db')
        self.ECHO = False
