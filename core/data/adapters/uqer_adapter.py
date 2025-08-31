#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
优矿数据源适配器
===============

优矿(uqer)API的统一适配器实现
"""

import os
from typing import Dict, List, Optional, Union, Tuple, Any
import pandas as pd
from datetime import datetime, date
import logging
import time

from .base_adapter import BaseDataAdapter

logger = logging.getLogger(__name__)

class UqerAdapter(BaseDataAdapter):
    """优矿数据源适配器"""
    
    def __init__(self, config: Optional[Dict] = None):
        """初始化优矿适配器
        
        Args:
            config: 配置参数，包含token等
        """
        super().__init__(config)
        
        # 获取API token
        self.token = (
            self.config.get('token') or 
            self.config.get('UQER_TOKEN') or
            os.environ.get('UQER_TOKEN', '')
        )
        
        self.api_client = None
        self._rate_limit_delay = 0.1  # 100ms延迟避免频率限制
        
        if not self.token:
            logger.warning("未设置优矿API Token，某些功能可能无法使用")
    
    def connect(self) -> bool:
        """连接到优矿API"""
        try:
            # 尝试导入uqer
            import uqer
            
            if self.token:
                # 设置token
                uqer.Client(token=self.token)
                self.api_client = uqer
                
                # 测试连接
                test_result = uqer.DataAPI.EquGet(
                    listStatusCD='L',
                    field='ticker,secShortName',
                    pandas='1'
                )
                
                if isinstance(test_result, pd.DataFrame) and not test_result.empty:
                    self._connected = True
                    logger.info("✅ 优矿API连接成功")
                    return True
                else:
                    logger.error("优矿API连接测试失败")
                    return False
            else:
                # 无token模式，仅基础功能可用
                self.api_client = uqer
                self._connected = True
                logger.warning("⚠️ 优矿API无token连接，功能受限")
                return True
                
        except ImportError:
            logger.error("❌ 无法导入uqer库，请先安装: pip install uqer")
            return False
        except Exception as e:
            logger.error(f"❌ 优矿API连接失败: {str(e)}")
            return False
    
    def disconnect(self) -> None:
        """断开连接"""
        self.api_client = None
        self._connected = False
        logger.info("优矿API连接已断开")
    
    def _rate_limit(self):
        """API调用频率限制"""
        time.sleep(self._rate_limit_delay)
    
    def get_stock_list(self, 
                      market: Optional[str] = None,
                      **kwargs) -> pd.DataFrame:
        """获取股票列表"""
        if not self.is_connected:
            raise ConnectionError("未连接到优矿API")
        
        try:
            self._rate_limit()
            
            # 构建查询参数
            params = {
                'listStatusCD': 'L',  # 上市状态
                'field': 'ticker,secShortName,exchangeCD,listDate,delistDate',
                'pandas': '1'
            }
            
            if market:
                # 市场过滤
                market_map = {
                    'SSE': 'XSHE',
                    'SZSE': 'XSHG', 
                    'BSE': 'XBSE'
                }
                if market in market_map:
                    params['exchangeCD'] = market_map[market]
            
            # 调用API
            result = self.api_client.DataAPI.EquGet(**params)
            
            if isinstance(result, pd.DataFrame) and not result.empty:
                # 统一字段名
                result = result.rename(columns={
                    'ticker': 'symbol',
                    'secShortName': 'name',
                    'exchangeCD': 'market',
                    'listDate': 'list_date',
                    'delistDate': 'delist_date'
                })
                
                logger.info(f"✅ 获取股票列表成功，共{len(result)}只股票")
                return result
            else:
                logger.warning("⚠️ 未获取到股票列表数据")
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"❌ 获取股票列表失败: {str(e)}")
            return pd.DataFrame()
    
    def get_price_data(self,
                      symbols: Union[str, List[str]],
                      start_date: Union[str, date, datetime],
                      end_date: Union[str, date, datetime],
                      fields: Optional[List[str]] = None,
                      **kwargs) -> pd.DataFrame:
        """获取价格数据"""
        if not self.is_connected:
            raise ConnectionError("未连接到优矿API")
        
        try:
            # 标准化symbols
            if isinstance(symbols, str):
                symbols = [symbols]
            
            # 标准化日期
            if isinstance(start_date, (date, datetime)):
                start_date = start_date.strftime('%Y-%m-%d')
            if isinstance(end_date, (date, datetime)):
                end_date = end_date.strftime('%Y-%m-%d')
            
            # 默认字段
            if fields is None:
                fields = ['tradeDate', 'ticker', 'openPrice', 'highestPrice', 
                         'lowestPrice', 'closePrice', 'turnoverVol', 'turnoverValue']
            
            all_data = []
            
            for symbol in symbols:
                self._rate_limit()
                
                # 调用API
                params = {
                    'ticker': symbol,
                    'beginDate': start_date,
                    'endDate': end_date,
                    'field': ','.join(fields),
                    'pandas': '1'
                }
                
                result = self.api_client.DataAPI.MktEqudGet(**params)
                
                if isinstance(result, pd.DataFrame) and not result.empty:
                    all_data.append(result)
            
            if all_data:
                # 合并数据
                combined = pd.concat(all_data, ignore_index=True)
                
                # 统一字段名
                combined = combined.rename(columns={
                    'tradeDate': 'date',
                    'ticker': 'symbol', 
                    'openPrice': 'open',
                    'highestPrice': 'high',
                    'lowestPrice': 'low',
                    'closePrice': 'close',
                    'turnoverVol': 'volume',
                    'turnoverValue': 'amount'
                })
                
                # 确保日期格式
                if 'date' in combined.columns:
                    combined['date'] = pd.to_datetime(combined['date'])
                
                logger.info(f"✅ 获取价格数据成功，共{len(combined)}条记录")
                return combined
            else:
                logger.warning("⚠️ 未获取到价格数据")
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"❌ 获取价格数据失败: {str(e)}")
            return pd.DataFrame()
    
    def get_financial_data(self,
                          symbols: Union[str, List[str]],
                          start_date: Union[str, date, datetime],
                          end_date: Union[str, date, datetime],
                          report_type: str = 'annual',
                          **kwargs) -> pd.DataFrame:
        """获取财务数据"""
        if not self.is_connected:
            raise ConnectionError("未连接到优矿API")
        
        try:
            # 标准化symbols
            if isinstance(symbols, str):
                symbols = [symbols]
            
            # 标准化日期
            if isinstance(start_date, (date, datetime)):
                start_date = start_date.strftime('%Y-%m-%d')
            if isinstance(end_date, (date, datetime)):
                end_date = end_date.strftime('%Y-%m-%d')
            
            # 报告期类型映射
            report_map = {
                'annual': 'A',      # 年报
                'quarterly': 'Q',   # 季报
                'semi': 'S',        # 半年报
                'ttm': 'TTM'        # TTM
            }
            
            period_type = report_map.get(report_type, 'A')
            
            all_data = []
            
            for symbol in symbols:
                self._rate_limit()
                
                # 获取利润表数据
                params = {
                    'ticker': symbol,
                    'beginDate': start_date,
                    'endDate': end_date,
                    'reportType': period_type,
                    'field': 'ticker,endDate,revenue,netProfit,totalAssets,totalLiab',
                    'pandas': '1'
                }
                
                result = self.api_client.DataAPI.FdmtIncome(**params)
                
                if isinstance(result, pd.DataFrame) and not result.empty:
                    all_data.append(result)
            
            if all_data:
                combined = pd.concat(all_data, ignore_index=True)
                
                # 统一字段名
                combined = combined.rename(columns={
                    'ticker': 'symbol',
                    'endDate': 'report_date'
                })
                
                logger.info(f"✅ 获取财务数据成功，共{len(combined)}条记录")
                return combined
            else:
                logger.warning("⚠️ 未获取到财务数据")
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"❌ 获取财务数据失败: {str(e)}")
            return pd.DataFrame()
    
    def get_index_components(self,
                           index_code: str,
                           date: Optional[Union[str, date, datetime]] = None,
                           **kwargs) -> pd.DataFrame:
        """获取指数成分股"""
        if not self.is_connected:
            raise ConnectionError("未连接到优矿API")
        
        try:
            self._rate_limit()
            
            # 标准化日期
            if date is None:
                date = datetime.now().strftime('%Y-%m-%d')
            elif isinstance(date, (date, datetime)):
                date = date.strftime('%Y-%m-%d')
            
            # 调用API
            params = {
                'indexID': index_code,
                'endDate': date,
                'field': 'consTickerSymbol,consShortName,weight',
                'pandas': '1'
            }
            
            result = self.api_client.DataAPI.IdxConsGet(**params)
            
            if isinstance(result, pd.DataFrame) and not result.empty:
                # 统一字段名
                result = result.rename(columns={
                    'consTickerSymbol': 'symbol',
                    'consShortName': 'name',
                    'weight': 'weight'
                })
                
                logger.info(f"✅ 获取指数成分股成功，共{len(result)}只股票")
                return result
            else:
                logger.warning("⚠️ 未获取到指数成分股数据")
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"❌ 获取指数成分股失败: {str(e)}")
            return pd.DataFrame()