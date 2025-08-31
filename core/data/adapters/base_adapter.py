#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
基础数据源适配器
================

定义统一的数据源接口规范
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Union, Tuple, Any
import pandas as pd
from datetime import datetime, date
import logging

logger = logging.getLogger(__name__)

class BaseDataAdapter(ABC):
    """数据源适配器基类
    
    定义所有数据源适配器必须实现的接口
    """
    
    def __init__(self, config: Optional[Dict] = None):
        """初始化适配器
        
        Args:
            config: 配置参数字典
        """
        self.config = config or {}
        self.name = self.__class__.__name__
        self._connected = False
        
    @abstractmethod
    def connect(self) -> bool:
        """连接到数据源
        
        Returns:
            bool: 连接是否成功
        """
        pass
    
    @abstractmethod
    def disconnect(self) -> None:
        """断开数据源连接"""
        pass
    
    @abstractmethod
    def get_stock_list(self, 
                      market: Optional[str] = None,
                      **kwargs) -> pd.DataFrame:
        """获取股票列表
        
        Args:
            market: 市场代码 ('SSE', 'SZSE', 'BSE', 等)
            **kwargs: 其他参数
            
        Returns:
            pd.DataFrame: 包含股票信息的DataFrame
                - symbol: 股票代码
                - name: 股票名称  
                - market: 交易所
                - list_date: 上市日期
                - delist_date: 退市日期(如有)
        """
        pass
    
    @abstractmethod
    def get_price_data(self,
                      symbols: Union[str, List[str]],
                      start_date: Union[str, date, datetime],
                      end_date: Union[str, date, datetime],
                      fields: Optional[List[str]] = None,
                      **kwargs) -> pd.DataFrame:
        """获取价格数据
        
        Args:
            symbols: 股票代码或代码列表
            start_date: 开始日期
            end_date: 结束日期  
            fields: 需要获取的字段列表
            **kwargs: 其他参数
            
        Returns:
            pd.DataFrame: 价格数据
                - date: 交易日期
                - symbol: 股票代码
                - open: 开盘价
                - high: 最高价
                - low: 最低价
                - close: 收盘价
                - volume: 成交量
                - amount: 成交额
        """
        pass
    
    @abstractmethod 
    def get_financial_data(self,
                          symbols: Union[str, List[str]],
                          start_date: Union[str, date, datetime],
                          end_date: Union[str, date, datetime],
                          report_type: str = 'annual',
                          **kwargs) -> pd.DataFrame:
        """获取财务数据
        
        Args:
            symbols: 股票代码或代码列表
            start_date: 开始日期
            end_date: 结束日期
            report_type: 报告类型 ('annual', 'quarterly', 'ttm')
            **kwargs: 其他参数
            
        Returns:
            pd.DataFrame: 财务数据
        """
        pass
    
    def get_index_components(self,
                           index_code: str,
                           date: Optional[Union[str, date, datetime]] = None,
                           **kwargs) -> pd.DataFrame:
        """获取指数成分股
        
        Args:
            index_code: 指数代码
            date: 日期，如果为None则获取最新的
            **kwargs: 其他参数
            
        Returns:
            pd.DataFrame: 成分股信息
        """
        # 默认实现，子类可以重写
        logger.warning(f"{self.name} 不支持获取指数成分股")
        return pd.DataFrame()
    
    def get_market_data(self,
                       data_type: str,
                       start_date: Union[str, date, datetime],
                       end_date: Union[str, date, datetime],
                       **kwargs) -> pd.DataFrame:
        """获取市场数据
        
        Args:
            data_type: 数据类型 ('index', 'sector', 'macro', 等)
            start_date: 开始日期
            end_date: 结束日期
            **kwargs: 其他参数
            
        Returns:
            pd.DataFrame: 市场数据
        """
        # 默认实现，子类可以重写
        logger.warning(f"{self.name} 不支持获取市场数据")
        return pd.DataFrame()
    
    def test_connection(self) -> Dict[str, Any]:
        """测试连接状态
        
        Returns:
            Dict: 测试结果
        """
        try:
            success = self.connect()
            return {
                'success': success,
                'adapter': self.name,
                'message': 'Connection successful' if success else 'Connection failed'
            }
        except Exception as e:
            return {
                'success': False,
                'adapter': self.name, 
                'message': f'Connection error: {str(e)}'
            }
    
    @property
    def is_connected(self) -> bool:
        """检查是否已连接"""
        return self._connected
    
    def __enter__(self):
        """上下文管理器入口"""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器出口"""
        self.disconnect()
    
    def __repr__(self) -> str:
        status = "Connected" if self._connected else "Disconnected" 
        return f"{self.name}({status})"