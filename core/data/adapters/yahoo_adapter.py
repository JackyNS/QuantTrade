#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Yahoo Finance数据源适配器
========================

Yahoo Finance API的统一适配器实现
"""

from typing import Dict, List, Optional, Union, Tuple, Any
import pandas as pd
from datetime import datetime, date, timedelta
import logging
import time

from .base_adapter import BaseDataAdapter

logger = logging.getLogger(__name__)

class YahooFinanceAdapter(BaseDataAdapter):
    """Yahoo Finance数据源适配器"""
    
    def __init__(self, config: Optional[Dict] = None):
        """初始化Yahoo Finance适配器
        
        Args:
            config: 配置参数
        """
        super().__init__(config)
        self.api_client = None
        self._rate_limit_delay = 0.5  # 500ms延迟，Yahoo Finance对频率敏感
        
    def connect(self) -> bool:
        """连接到Yahoo Finance API"""
        try:
            # 尝试导入yfinance
            import yfinance as yf
            
            self.api_client = yf
            
            # 测试连接 - 获取AAPL的基本信息
            test_ticker = yf.Ticker("AAPL")
            test_info = test_ticker.info
            
            if test_info and 'symbol' in test_info:
                self._connected = True
                logger.info("✅ Yahoo Finance API连接成功")
                return True
            else:
                logger.error("Yahoo Finance API连接测试失败")
                return False
                
        except ImportError:
            logger.error("❌ 无法导入yfinance库，请先安装: pip install yfinance")
            return False
        except Exception as e:
            logger.error(f"❌ Yahoo Finance API连接失败: {str(e)}")
            return False
    
    def disconnect(self) -> None:
        """断开连接"""
        self.api_client = None
        self._connected = False
        logger.info("Yahoo Finance API连接已断开")
    
    def _rate_limit(self):
        """API调用频率限制"""
        time.sleep(self._rate_limit_delay)
    
    def _normalize_symbol(self, symbol: str, market: str = 'US') -> str:
        """标准化股票代码格式"""
        if market.upper() == 'CN':
            # 中国市场代码转换
            if symbol.endswith('.SH'):
                return symbol.replace('.SH', '.SS')  # 上海交易所
            elif symbol.endswith('.SZ'):
                return symbol.replace('.SZ', '.SZ')  # 深圳交易所
            elif len(symbol) == 6 and symbol.isdigit():
                # 纯数字代码，根据前缀判断
                if symbol.startswith('6'):
                    return f"{symbol}.SS"
                elif symbol.startswith(('0', '3')):
                    return f"{symbol}.SZ"
        
        # 美股等其他市场直接返回
        return symbol
    
    def get_stock_list(self, 
                      market: Optional[str] = None,
                      **kwargs) -> pd.DataFrame:
        """获取股票列表
        
        Note: Yahoo Finance不提供完整的股票列表API，
        这里提供一些主要指数成分股作为示例
        """
        if not self.is_connected:
            raise ConnectionError("未连接到Yahoo Finance API")
        
        try:
            self._rate_limit()
            
            # 预定义一些主要市场的代表性股票
            stock_lists = {
                'US': [
                    ('AAPL', 'Apple Inc.', 'NASDAQ'),
                    ('MSFT', 'Microsoft Corporation', 'NASDAQ'),
                    ('GOOGL', 'Alphabet Inc.', 'NASDAQ'),
                    ('AMZN', 'Amazon.com Inc.', 'NASDAQ'),
                    ('TSLA', 'Tesla Inc.', 'NASDAQ'),
                    ('META', 'Meta Platforms Inc.', 'NASDAQ'),
                    ('NVDA', 'NVIDIA Corporation', 'NASDAQ'),
                    ('JPM', 'JPMorgan Chase & Co.', 'NYSE'),
                    ('JNJ', 'Johnson & Johnson', 'NYSE'),
                    ('V', 'Visa Inc.', 'NYSE')
                ],
                'CN': [
                    ('000001.SS', '上证指数', 'SSE'),
                    ('399001.SZ', '深证成指', 'SZSE'),
                    ('000300.SS', '沪深300', 'SSE'),
                    ('000905.SS', '中证500', 'SSE')
                ]
            }
            
            target_market = market or 'US'
            stocks = stock_lists.get(target_market, stock_lists['US'])
            
            # 创建DataFrame
            df = pd.DataFrame(stocks, columns=['symbol', 'name', 'market'])
            df['list_date'] = None
            df['delist_date'] = None
            
            logger.info(f"✅ 获取{target_market}市场股票列表，共{len(df)}只股票")
            logger.warning("⚠️ Yahoo Finance不提供完整股票列表，这是示例数据")
            
            return df
                
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
            raise ConnectionError("未连接到Yahoo Finance API")
        
        try:
            # 标准化symbols
            if isinstance(symbols, str):
                symbols = [symbols]
            
            # 检测市场并标准化代码
            market = kwargs.get('market', 'US')
            symbols = [self._normalize_symbol(s, market) for s in symbols]
            
            # 标准化日期
            if isinstance(start_date, str):
                start_date = datetime.strptime(start_date, '%Y-%m-%d')
            if isinstance(end_date, str):
                end_date = datetime.strptime(end_date, '%Y-%m-%d')
            
            all_data = []
            
            for symbol in symbols:
                self._rate_limit()
                
                try:
                    # 创建ticker对象
                    ticker = self.api_client.Ticker(symbol)
                    
                    # 获取历史数据
                    hist_data = ticker.history(
                        start=start_date,
                        end=end_date + timedelta(days=1),  # 包含结束日期
                        auto_adjust=True,
                        back_adjust=True
                    )
                    
                    if not hist_data.empty:
                        # 重置索引，将日期作为列
                        hist_data = hist_data.reset_index()
                        hist_data['symbol'] = symbol
                        
                        # 重命名列
                        hist_data = hist_data.rename(columns={
                            'Date': 'date',
                            'Open': 'open',
                            'High': 'high', 
                            'Low': 'low',
                            'Close': 'close',
                            'Volume': 'volume'
                        })
                        
                        # 计算成交额 (估算)
                        hist_data['amount'] = hist_data['close'] * hist_data['volume']
                        
                        # 选择需要的列
                        columns = ['date', 'symbol', 'open', 'high', 'low', 'close', 'volume', 'amount']
                        hist_data = hist_data[columns]
                        
                        all_data.append(hist_data)
                        
                except Exception as e:
                    logger.warning(f"⚠️ 获取{symbol}数据失败: {str(e)}")
                    continue
            
            if all_data:
                # 合并数据
                combined = pd.concat(all_data, ignore_index=True)
                
                # 确保日期格式
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
            raise ConnectionError("未连接到Yahoo Finance API")
        
        try:
            # 标准化symbols
            if isinstance(symbols, str):
                symbols = [symbols]
            
            # 检测市场并标准化代码
            market = kwargs.get('market', 'US')
            symbols = [self._normalize_symbol(s, market) for s in symbols]
            
            all_data = []
            
            for symbol in symbols:
                self._rate_limit()
                
                try:
                    # 创建ticker对象
                    ticker = self.api_client.Ticker(symbol)
                    
                    # 获取财务数据
                    if report_type == 'annual':
                        financials = ticker.financials
                        balance_sheet = ticker.balance_sheet
                    else:  # quarterly
                        financials = ticker.quarterly_financials  
                        balance_sheet = ticker.quarterly_balance_sheet
                    
                    if not financials.empty and not balance_sheet.empty:
                        # 合并财务数据
                        data_dict = {
                            'symbol': symbol,
                            'report_date': [],
                            'revenue': [],
                            'net_profit': [],
                            'total_assets': [],
                            'total_liab': []
                        }
                        
                        # 提取数据
                        for date_col in financials.columns:
                            data_dict['report_date'].append(date_col)
                            
                            # 收入
                            revenue = financials.loc[financials.index.str.contains('Total Revenue', case=False, na=False), date_col]
                            data_dict['revenue'].append(revenue.iloc[0] if not revenue.empty else None)
                            
                            # 净利润
                            net_income = financials.loc[financials.index.str.contains('Net Income', case=False, na=False), date_col]
                            data_dict['net_profit'].append(net_income.iloc[0] if not net_income.empty else None)
                            
                            # 总资产
                            if date_col in balance_sheet.columns:
                                total_assets = balance_sheet.loc[balance_sheet.index.str.contains('Total Assets', case=False, na=False), date_col]
                                data_dict['total_assets'].append(total_assets.iloc[0] if not total_assets.empty else None)
                                
                                # 总负债
                                total_liab = balance_sheet.loc[balance_sheet.index.str.contains('Total Liab', case=False, na=False), date_col]
                                data_dict['total_liab'].append(total_liab.iloc[0] if not total_liab.empty else None)
                            else:
                                data_dict['total_assets'].append(None)
                                data_dict['total_liab'].append(None)
                        
                        # 创建DataFrame
                        df = pd.DataFrame(data_dict)
                        all_data.append(df)
                        
                except Exception as e:
                    logger.warning(f"⚠️ 获取{symbol}财务数据失败: {str(e)}")
                    continue
            
            if all_data:
                combined = pd.concat(all_data, ignore_index=True)
                
                # 确保日期格式
                combined['report_date'] = pd.to_datetime(combined['report_date'])
                
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
            raise ConnectionError("未连接到Yahoo Finance API")
        
        try:
            # 预定义的市场指数
            market_indices = {
                'index': {
                    'SPY': 'S&P 500',
                    'QQQ': 'NASDAQ 100', 
                    'DIA': 'Dow Jones',
                    'IWM': 'Russell 2000',
                    '000001.SS': '上证指数',
                    '399001.SZ': '深证成指'
                },
                'sector': {
                    'XLK': 'Technology',
                    'XLF': 'Financial',
                    'XLE': 'Energy',
                    'XLV': 'Healthcare',
                    'XLI': 'Industrial'
                }
            }
            
            if data_type not in market_indices:
                logger.warning(f"⚠️ 不支持的市场数据类型: {data_type}")
                return pd.DataFrame()
            
            symbols = list(market_indices[data_type].keys())
            
            # 使用价格数据获取方法
            return self.get_price_data(
                symbols=symbols,
                start_date=start_date,
                end_date=end_date,
                **kwargs
            )
            
        except Exception as e:
            logger.error(f"❌ 获取市场数据失败: {str(e)}")
            return pd.DataFrame()