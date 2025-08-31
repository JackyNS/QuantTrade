#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AKShare数据源适配器
==================

AKShare库的统一适配器实现
"""

from typing import Dict, List, Optional, Union, Tuple, Any
import pandas as pd
from datetime import datetime, date
import logging
import time

from .base_adapter import BaseDataAdapter

logger = logging.getLogger(__name__)

class AKShareAdapter(BaseDataAdapter):
    """AKShare数据源适配器"""
    
    def __init__(self, config: Optional[Dict] = None):
        """初始化AKShare适配器
        
        Args:
            config: 配置参数
        """
        super().__init__(config)
        self.api_client = None
        self._rate_limit_delay = 0.2  # 200ms延迟
        
    def connect(self) -> bool:
        """连接到AKShare"""
        try:
            # 尝试导入akshare
            import akshare as ak
            
            self.api_client = ak
            
            # 测试连接 - 获取股票列表
            test_result = ak.stock_info_a_code_name()
            
            if isinstance(test_result, pd.DataFrame) and not test_result.empty:
                self._connected = True
                logger.info("✅ AKShare连接成功")
                return True
            else:
                logger.error("AKShare连接测试失败")
                return False
                
        except ImportError:
            logger.error("❌ 无法导入akshare库，请先安装: pip install akshare")
            return False
        except Exception as e:
            logger.error(f"❌ AKShare连接失败: {str(e)}")
            return False
    
    def disconnect(self) -> None:
        """断开连接"""
        self.api_client = None
        self._connected = False
        logger.info("AKShare连接已断开")
    
    def _rate_limit(self):
        """API调用频率限制"""
        time.sleep(self._rate_limit_delay)
    
    def _normalize_symbol(self, symbol: str) -> str:
        """标准化股票代码格式"""
        # 移除交易所后缀，AKShare使用纯数字代码
        if '.' in symbol:
            symbol = symbol.split('.')[0]
        return symbol
    
    def get_stock_list(self, 
                      market: Optional[str] = None,
                      **kwargs) -> pd.DataFrame:
        """获取股票列表"""
        if not self.is_connected:
            raise ConnectionError("未连接到AKShare")
        
        try:
            self._rate_limit()
            
            # 获取A股股票列表
            result = self.api_client.stock_info_a_code_name()
            
            if isinstance(result, pd.DataFrame) and not result.empty:
                # 统一字段名
                result = result.rename(columns={
                    'code': 'symbol',
                    'name': 'name'
                })
                
                # 添加市场信息
                def get_market(code):
                    if code.startswith('6'):
                        return 'SSE'  # 上海证券交易所
                    elif code.startswith(('0', '3')):
                        return 'SZSE'  # 深圳证券交易所
                    elif code.startswith('8'):
                        return 'BSE'  # 北京证券交易所
                    else:
                        return 'OTHER'
                
                result['market'] = result['symbol'].apply(get_market)
                
                # 添加空的日期字段
                result['list_date'] = None
                result['delist_date'] = None
                
                # 市场过滤
                if market:
                    result = result[result['market'] == market]
                
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
            raise ConnectionError("未连接到AKShare")
        
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
                
                try:
                    # 调用API获取历史数据
                    result = self.api_client.stock_zh_a_hist(
                        symbol=symbol,
                        period="daily",
                        start_date=start_date,
                        end_date=end_date,
                        adjust=""
                    )
                    
                    if isinstance(result, pd.DataFrame) and not result.empty:
                        # 添加股票代码
                        result['symbol'] = symbol
                        
                        # 统一字段名
                        result = result.rename(columns={
                            '日期': 'date',
                            '开盘': 'open',
                            '最高': 'high', 
                            '最低': 'low',
                            '收盘': 'close',
                            '成交量': 'volume',
                            '成交额': 'amount'
                        })
                        
                        # 选择需要的列
                        columns = ['date', 'symbol', 'open', 'high', 'low', 'close', 'volume', 'amount']
                        result = result[[col for col in columns if col in result.columns]]
                        
                        all_data.append(result)
                        
                except Exception as e:
                    logger.warning(f"⚠️ 获取{symbol}数据失败: {str(e)}")
                    continue
            
            if all_data:
                # 合并数据
                combined = pd.concat(all_data, ignore_index=True)
                
                # 确保日期格式
                if 'date' in combined.columns:
                    combined['date'] = pd.to_datetime(combined['date'])
                
                # 数值列转换
                numeric_cols = ['open', 'high', 'low', 'close', 'volume', 'amount']
                for col in numeric_cols:
                    if col in combined.columns:
                        combined[col] = pd.to_numeric(combined[col], errors='coerce')
                
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
            raise ConnectionError("未连接到AKShare")
        
        try:
            # 标准化symbols
            if isinstance(symbols, str):
                symbols = [symbols]
                
            # 标准化股票代码
            symbols = [self._normalize_symbol(s) for s in symbols]
            
            all_data = []
            
            for symbol in symbols:
                self._rate_limit()
                
                try:
                    # AKShare财务数据接口
                    if report_type == 'annual':
                        # 年报数据
                        result = self.api_client.stock_financial_abstract(symbol=symbol)
                    else:
                        # 季报数据
                        result = self.api_client.stock_financial_abstract(symbol=symbol)
                    
                    if isinstance(result, pd.DataFrame) and not result.empty:
                        # 添加股票代码
                        result['symbol'] = symbol
                        
                        # 尝试标准化字段名(AKShare字段名可能变化)
                        rename_map = {}
                        for col in result.columns:
                            col_lower = col.lower()
                            if '报告期' in col or 'date' in col_lower:
                                rename_map[col] = 'report_date'
                            elif '营业收入' in col or 'revenue' in col_lower:
                                rename_map[col] = 'revenue'
                            elif '净利润' in col or 'profit' in col_lower:
                                rename_map[col] = 'net_profit'
                            elif '总资产' in col or 'assets' in col_lower:
                                rename_map[col] = 'total_assets'
                            elif '总负债' in col or 'liab' in col_lower:
                                rename_map[col] = 'total_liab'
                        
                        if rename_map:
                            result = result.rename(columns=rename_map)
                        
                        all_data.append(result)
                        
                except Exception as e:
                    logger.warning(f"⚠️ 获取{symbol}财务数据失败: {str(e)}")
                    continue
            
            if all_data:
                combined = pd.concat(all_data, ignore_index=True)
                
                # 确保日期格式
                if 'report_date' in combined.columns:
                    combined['report_date'] = pd.to_datetime(combined['report_date'], errors='coerce')
                
                logger.info(f"✅ 获取财务数据成功，共{len(combined)}条记录")
                return combined
            else:
                logger.warning("⚠️ 未获取到财务数据")
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"❌ 获取财务数据失败: {str(e)}")
            return pd.DataFrame()
    
    def get_market_data(self,
                       data_type: str,
                       start_date: Union[str, date, datetime],
                       end_date: Union[str, date, datetime],
                       **kwargs) -> pd.DataFrame:
        """获取市场数据"""
        if not self.is_connected:
            raise ConnectionError("未连接到AKShare")
        
        try:
            self._rate_limit()
            
            # 标准化日期格式
            if isinstance(start_date, (date, datetime)):
                start_date = start_date.strftime('%Y%m%d')
            else:
                start_date = start_date.replace('-', '')
                
            if isinstance(end_date, (date, datetime)):
                end_date = end_date.strftime('%Y%m%d')
            else:
                end_date = end_date.replace('-', '')
            
            if data_type == 'index':
                # 获取指数数据
                indices = ['sh000001', 'sz399001', 'sz399006']  # 上证、深成、创业板
                all_data = []
                
                for index in indices:
                    try:
                        result = self.api_client.stock_zh_index_daily(symbol=index)
                        if isinstance(result, pd.DataFrame) and not result.empty:
                            # 过滤日期范围
                            result['date'] = pd.to_datetime(result['date'])
                            start_dt = pd.to_datetime(start_date)
                            end_dt = pd.to_datetime(end_date)
                            result = result[(result['date'] >= start_dt) & (result['date'] <= end_dt)]
                            
                            result['symbol'] = index
                            all_data.append(result)
                    except Exception as e:
                        logger.warning(f"⚠️ 获取指数{index}数据失败: {str(e)}")
                        continue
                
                if all_data:
                    combined = pd.concat(all_data, ignore_index=True)
                    logger.info(f"✅ 获取指数数据成功，共{len(combined)}条记录")
                    return combined
            
            elif data_type == 'macro':
                # 获取宏观数据
                try:
                    # GDP数据
                    gdp_data = self.api_client.macro_china_gdp()
                    if isinstance(gdp_data, pd.DataFrame) and not gdp_data.empty:
                        logger.info(f"✅ 获取宏观数据成功，共{len(gdp_data)}条记录")
                        return gdp_data
                except Exception as e:
                    logger.warning(f"⚠️ 获取宏观数据失败: {str(e)}")
            
            logger.warning(f"⚠️ 未获取到{data_type}市场数据")
            return pd.DataFrame()
            
        except Exception as e:
            logger.error(f"❌ 获取市场数据失败: {str(e)}")
            return pd.DataFrame()