#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Tushare数据源适配器
==================

Tushare API的统一适配器实现
"""

import os
from typing import Dict, List, Optional, Union, Tuple, Any
import pandas as pd
from datetime import datetime, date
import logging
import time

from .base_adapter import BaseDataAdapter

logger = logging.getLogger(__name__)

class TushareAdapter(BaseDataAdapter):
    """Tushare数据源适配器"""
    
    def __init__(self, config: Optional[Dict] = None):
        """初始化Tushare适配器
        
        Args:
            config: 配置参数，包含token等
        """
        super().__init__(config)
        
        # 获取API token
        self.token = (
            self.config.get('token') or 
            self.config.get('TUSHARE_TOKEN') or
            os.environ.get('TUSHARE_TOKEN', '')
        )
        
        self.api_client = None
        self._rate_limit_delay = 0.1  # 100ms延迟避免频率限制
        
        if not self.token:
            logger.warning("未设置Tushare API Token，功能将受到限制")
    
    def connect(self) -> bool:
        """连接到Tushare API"""
        try:
            # 尝试导入tushare
            import tushare as ts
            
            if self.token:
                # 设置token
                ts.set_token(self.token)
                self.api_client = ts.pro_api()
                
                # 测试连接
                test_result = self.api_client.stock_basic(
                    exchange='', 
                    list_status='L',
                    fields='ts_code,symbol,name,area,industry,list_date'
                )
                
                if isinstance(test_result, pd.DataFrame) and not test_result.empty:
                    self._connected = True
                    logger.info("✅ Tushare API连接成功")
                    return True
                else:
                    logger.error("Tushare API连接测试失败")
                    return False
            else:
                logger.error("❌ 缺少Tushare API Token")
                return False
                
        except ImportError:
            logger.error("❌ 无法导入tushare库，请先安装: pip install tushare")
            return False
        except Exception as e:
            logger.error(f"❌ Tushare API连接失败: {str(e)}")
            return False
    
    def disconnect(self) -> None:
        """断开连接"""
        self.api_client = None
        self._connected = False
        logger.info("Tushare API连接已断开")
    
    def _rate_limit(self):
        """API调用频率限制"""
        time.sleep(self._rate_limit_delay)
    
    def _normalize_symbol(self, symbol: str) -> str:
        """标准化股票代码格式"""
        if '.' not in symbol:
            # 如果没有交易所后缀，根据代码判断
            if symbol.startswith('6'):
                return f"{symbol}.SH"
            elif symbol.startswith(('0', '3')):
                return f"{symbol}.SZ"
            elif symbol.startswith('8'):
                return f"{symbol}.BJ"
        return symbol
    
    def get_stock_list(self, 
                      market: Optional[str] = None,
                      **kwargs) -> pd.DataFrame:
        """获取股票列表"""
        if not self.is_connected:
            raise ConnectionError("未连接到Tushare API")
        
        try:
            self._rate_limit()
            
            # 市场映射
            exchange_map = {
                'SSE': 'SSE',      # 上海证券交易所
                'SZSE': 'SZSE',    # 深圳证券交易所
                'BSE': 'BSE'       # 北京证券交易所
            }
            
            exchange = exchange_map.get(market, '') if market else ''
            
            # 调用API
            result = self.api_client.stock_basic(
                exchange=exchange,
                list_status='L',
                fields='ts_code,symbol,name,area,industry,market,list_date,delist_date'
            )
            
            if isinstance(result, pd.DataFrame) and not result.empty:
                # 统一字段名
                result = result.rename(columns={
                    'ts_code': 'symbol',
                    'symbol': 'code',
                    'name': 'name',
                    'market': 'market',
                    'list_date': 'list_date',
                    'delist_date': 'delist_date'
                })
                
                # 转换日期格式
                date_cols = ['list_date', 'delist_date']
                for col in date_cols:
                    if col in result.columns:
                        result[col] = pd.to_datetime(result[col], errors='coerce')
                
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
            raise ConnectionError("未连接到Tushare API")
        
        try:
            # 标准化symbols
            if isinstance(symbols, str):
                symbols = [symbols]
            
            # 标准化股票代码
            symbols = [self._normalize_symbol(s) for s in symbols]
            
            # 标准化日期
            if isinstance(start_date, (date, datetime)):
                start_date = start_date.strftime('%Y%m%d')
            else:
                start_date = start_date.replace('-', '')
                
            if isinstance(end_date, (date, datetime)):
                end_date = end_date.strftime('%Y%m%d')
            else:
                end_date = end_date.replace('-', '')
            
            all_data = []
            
            for symbol in symbols:
                self._rate_limit()
                
                # 调用API
                result = self.api_client.daily(
                    ts_code=symbol,
                    start_date=start_date,
                    end_date=end_date
                )
                
                if isinstance(result, pd.DataFrame) and not result.empty:
                    all_data.append(result)
            
            if all_data:
                # 合并数据
                combined = pd.concat(all_data, ignore_index=True)
                
                # 统一字段名
                combined = combined.rename(columns={
                    'trade_date': 'date',
                    'ts_code': 'symbol',
                    'open': 'open',
                    'high': 'high',
                    'low': 'low',
                    'close': 'close',
                    'vol': 'volume',
                    'amount': 'amount'
                })
                
                # 确保日期格式
                if 'date' in combined.columns:
                    combined['date'] = pd.to_datetime(combined['date'])
                
                # 按日期和代码排序
                combined = combined.sort_values(['symbol', 'date']).reset_index(drop=True)
                
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
            raise ConnectionError("未连接到Tushare API")
        
        try:
            # 标准化symbols
            if isinstance(symbols, str):
                symbols = [symbols]
                
            # 标准化股票代码
            symbols = [self._normalize_symbol(s) for s in symbols]
            
            # 标准化日期
            if isinstance(start_date, (date, datetime)):
                start_date = start_date.strftime('%Y%m%d')
            else:
                start_date = start_date.replace('-', '')
                
            if isinstance(end_date, (date, datetime)):
                end_date = end_date.strftime('%Y%m%d')
            else:
                end_date = end_date.replace('-', '')
            
            # 报告期类型映射
            period_map = {
                'annual': '4',      # 年报
                'quarterly': '',    # 所有季报
                'semi': '2',        # 半年报
            }
            
            period = period_map.get(report_type, '')
            
            all_data = []
            
            for symbol in symbols:
                self._rate_limit()
                
                # 获取利润表数据
                result = self.api_client.income(
                    ts_code=symbol,
                    start_date=start_date,
                    end_date=end_date,
                    period=period
                )
                
                if isinstance(result, pd.DataFrame) and not result.empty:
                    all_data.append(result)
            
            if all_data:
                combined = pd.concat(all_data, ignore_index=True)
                
                # 统一字段名
                combined = combined.rename(columns={
                    'ts_code': 'symbol',
                    'end_date': 'report_date',
                    'revenue': 'revenue',
                    'n_income': 'net_profit',
                    'total_assets': 'total_assets',
                    'total_liab': 'total_liab'
                })
                
                # 确保日期格式
                if 'report_date' in combined.columns:
                    combined['report_date'] = pd.to_datetime(combined['report_date'])
                
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
            raise ConnectionError("未连接到Tushare API")
        
        try:
            self._rate_limit()
            
            # 标准化日期
            if date is None:
                trade_date = datetime.now().strftime('%Y%m%d')
            elif isinstance(date, (date, datetime)):
                trade_date = date.strftime('%Y%m%d')
            else:
                trade_date = date.replace('-', '')
            
            # 指数代码映射
            index_map = {
                '000300': '000300.SH',  # 沪深300
                '000905': '000905.SH',  # 中证500  
                '000016': '000016.SH',  # 上证50
                '399006': '399006.SZ',  # 创业板指
            }
            
            ts_code = index_map.get(index_code, index_code)
            
            # 调用API
            result = self.api_client.index_weight(
                index_code=ts_code,
                trade_date=trade_date
            )
            
            if isinstance(result, pd.DataFrame) and not result.empty:
                # 统一字段名
                result = result.rename(columns={
                    'con_code': 'symbol',
                    'weight': 'weight'
                })
                
                # 获取股票名称
                if 'symbol' in result.columns:
                    symbols = result['symbol'].tolist()
                    stock_info = self.api_client.stock_basic(
                        ts_code=','.join(symbols[:50]),  # API限制，分批获取
                        fields='ts_code,name'
                    )
                    
                    if isinstance(stock_info, pd.DataFrame):
                        name_map = dict(zip(stock_info['ts_code'], stock_info['name']))
                        result['name'] = result['symbol'].map(name_map)
                
                logger.info(f"✅ 获取指数成分股成功，共{len(result)}只股票")
                return result
            else:
                logger.warning("⚠️ 未获取到指数成分股数据")
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"❌ 获取指数成分股失败: {str(e)}")
            return pd.DataFrame()