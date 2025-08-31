#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据下载器模块
=============

统一的数据下载功能，整合所有数据获取来源：
- A股日线数据下载
- 策略数据下载  
- 技术指标数据下载
- 实时数据下载

Author: QuantTrader Team
"""

from .a_shares_downloader import ASharesDownloader
from .strategy_downloader import StrategyDownloader
from .indicator_downloader import IndicatorDownloader

__all__ = [
    'ASharesDownloader',
    'StrategyDownloader', 
    'IndicatorDownloader'
]

__version__ = '2.0.0'