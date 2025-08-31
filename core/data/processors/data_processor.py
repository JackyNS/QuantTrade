#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
统一数据处理器
=============

集成数据清洗、转换、处理功能的统一处理器

Author: QuantTrader Team
Date: 2025-08-31
"""

from typing import Dict, List, Optional, Union, Any, Callable
from datetime import datetime, date
import logging

try:
    import pandas as pd
    import numpy as np
except ImportError:
    pd = None
    np = None

from .data_cleaner import DataCleaner
from .data_transformer import DataTransformer

logger = logging.getLogger(__name__)

class DataProcessor:
    """统一数据处理器
    
    提供完整的数据处理流水线：
    - 数据清洗和验证
    - 数据转换和标准化
    - 缺失值和异常值处理
    - 数据聚合和计算
    """
    
    def __init__(self, config: Optional[Dict] = None):
        """初始化数据处理器
        
        Args:
            config: 配置参数
        """
        if pd is None:
            raise ImportError("数据处理器需要pandas，请安装: pip install pandas")
        
        self.config = config or {}
        
        # 初始化子组件
        self.cleaner = DataCleaner(self.config.get('cleaner', {}))
        self.transformer = DataTransformer(self.config.get('transformer', {}))
        
        # 处理流水线配置
        self.pipeline_steps = self.config.get('pipeline_steps', [
            'clean_data',
            'handle_missing_values',
            'remove_outliers',
            'standardize_columns',
            'validate_data'
        ])
        
        logger.info("✅ 数据处理器初始化完成")
    
    def process_price_data(self, 
                         data: pd.DataFrame,
                         symbol: Optional[str] = None,
                         apply_pipeline: bool = True) -> pd.DataFrame:
        """处理价格数据
        
        Args:
            data: 原始价格数据
            symbol: 股票代码（用于日志）
            apply_pipeline: 是否应用完整处理流水线
            
        Returns:
            处理后的数据
        """
        if data.empty:
            logger.warning(f"输入数据为空: {symbol}")
            return data.copy()
        
        logger.debug(f"开始处理价格数据: {symbol}, 原始行数: {len(data)}")
        
        result = data.copy()
        
        if apply_pipeline:
            # 执行处理流水线
            for step in self.pipeline_steps:
                try:
                    if step == 'clean_data':
                        result = self.cleaner.clean_price_data(result)
                    elif step == 'handle_missing_values':
                        result = self.cleaner.handle_missing_values(result)
                    elif step == 'remove_outliers':
                        result = self.cleaner.remove_outliers(result)
                    elif step == 'standardize_columns':
                        result = self.transformer.standardize_columns(result)
                    elif step == 'validate_data':
                        if not self._validate_price_data(result):
                            logger.warning(f"数据验证失败: {symbol}")
                    
                    logger.debug(f"{symbol}: {step} 完成，行数: {len(result)}")
                    
                except Exception as e:
                    logger.error(f"{symbol}: 处理步骤 {step} 失败 - {e}")
                    continue
        
        logger.debug(f"价格数据处理完成: {symbol}, 最终行数: {len(result)}")
        return result
    
    def process_financial_data(self, 
                             data: pd.DataFrame,
                             symbol: Optional[str] = None) -> pd.DataFrame:
        """处理财务数据
        
        Args:
            data: 原始财务数据
            symbol: 股票代码
            
        Returns:
            处理后的数据
        """
        if data.empty:
            return data.copy()
        
        logger.debug(f"开始处理财务数据: {symbol}")
        
        result = data.copy()
        
        try:
            # 财务数据特定处理
            result = self.cleaner.clean_financial_data(result)
            result = self.transformer.normalize_financial_ratios(result)
            
            logger.debug(f"财务数据处理完成: {symbol}")
            
        except Exception as e:
            logger.error(f"处理财务数据失败: {symbol} - {e}")
        
        return result
    
    def process_strategy_data(self,
                            data: pd.DataFrame,
                            data_type: str,
                            symbol: Optional[str] = None) -> pd.DataFrame:
        """处理策略数据
        
        Args:
            data: 策略数据
            data_type: 数据类型 ('capital_flow', 'sentiment', etc.)
            symbol: 股票代码
            
        Returns:
            处理后的数据
        """
        if data.empty:
            return data.copy()
        
        logger.debug(f"开始处理策略数据: {symbol}, 类型: {data_type}")
        
        result = data.copy()
        
        try:
            if data_type == 'capital_flow':
                result = self._process_capital_flow_data(result)
            elif data_type == 'market_sentiment':
                result = self._process_sentiment_data(result)
            else:
                # 通用处理
                result = self.cleaner.clean_data(result)
                result = self.transformer.standardize_columns(result)
            
            logger.debug(f"策略数据处理完成: {symbol}, 类型: {data_type}")
            
        except Exception as e:
            logger.error(f"处理策略数据失败: {symbol}, {data_type} - {e}")
        
        return result
    
    def _process_capital_flow_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """处理资金流向数据"""
        result = data.copy()
        
        # 清理资金流数据
        result = self.cleaner.clean_data(result)
        
        # 计算资金流向指标
        if all(col in result.columns for col in ['buy_amount', 'sell_amount']):
            result['net_flow'] = result['buy_amount'] - result['sell_amount']
            result['flow_ratio'] = result['buy_amount'] / (result['buy_amount'] + result['sell_amount'])
        
        return result
    
    def _process_sentiment_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """处理市场情绪数据"""
        result = data.copy()
        
        # 清理情绪数据
        result = self.cleaner.clean_data(result)
        
        # 标准化情绪指标
        sentiment_cols = [col for col in result.columns if 'sentiment' in col.lower()]
        for col in sentiment_cols:
            if result[col].dtype in ['float64', 'int64']:
                result[col] = self.transformer.normalize_column(result[col])
        
        return result
    
    def aggregate_data(self,
                      data: pd.DataFrame,
                      group_by: Union[str, List[str]],
                      agg_functions: Dict[str, Union[str, List[str], Callable]],
                      date_column: str = 'date') -> pd.DataFrame:
        """数据聚合
        
        Args:
            data: 原始数据
            group_by: 分组字段
            agg_functions: 聚合函数配置
            date_column: 日期列名
            
        Returns:
            聚合后的数据
        """
        try:
            if isinstance(group_by, str):
                group_by = [group_by]
            
            # 确保日期列在分组中
            if date_column in data.columns and date_column not in group_by:
                group_by.append(date_column)
            
            # 执行聚合
            result = data.groupby(group_by).agg(agg_functions)
            
            # 重置索引
            result = result.reset_index()
            
            logger.debug(f"数据聚合完成，原始行数: {len(data)}, 聚合后: {len(result)}")
            return result
            
        except Exception as e:
            logger.error(f"数据聚合失败: {e}")
            return data.copy()
    
    def merge_datasets(self,
                      left: pd.DataFrame,
                      right: pd.DataFrame,
                      on: Union[str, List[str]],
                      how: str = 'inner',
                      suffixes: tuple = ('_x', '_y')) -> pd.DataFrame:
        """合并数据集
        
        Args:
            left: 左侧数据
            right: 右侧数据
            on: 连接字段
            how: 连接方式
            suffixes: 重复列后缀
            
        Returns:
            合并后的数据
        """
        try:
            result = pd.merge(left, right, on=on, how=how, suffixes=suffixes)
            logger.debug(f"数据合并完成，结果行数: {len(result)}")
            return result
        except Exception as e:
            logger.error(f"数据合并失败: {e}")
            return left.copy()
    
    def calculate_returns(self,
                         data: pd.DataFrame,
                         price_column: str = 'close',
                         periods: List[int] = None) -> pd.DataFrame:
        """计算收益率
        
        Args:
            data: 价格数据
            price_column: 价格列名
            periods: 计算周期列表
            
        Returns:
            包含收益率的数据
        """
        if periods is None:
            periods = [1, 5, 10, 20]
        
        result = data.copy()
        
        try:
            if price_column not in result.columns:
                logger.warning(f"价格列 '{price_column}' 不存在")
                return result
            
            # 计算不同周期的收益率
            for period in periods:
                return_col = f'return_{period}d'
                result[return_col] = result[price_column].pct_change(periods=period)
            
            # 计算对数收益率
            result['log_return'] = np.log(result[price_column] / result[price_column].shift(1))
            
            logger.debug(f"收益率计算完成，周期: {periods}")
            
        except Exception as e:
            logger.error(f"计算收益率失败: {e}")
        
        return result
    
    def _validate_price_data(self, data: pd.DataFrame) -> bool:
        """验证价格数据的完整性"""
        required_columns = ['open', 'high', 'low', 'close', 'volume']
        
        # 检查必需列
        missing_columns = [col for col in required_columns if col not in data.columns]
        if missing_columns:
            logger.warning(f"缺少必需列: {missing_columns}")
            return False
        
        # 检查价格逻辑
        invalid_rows = (
            (data['high'] < data['low']) |
            (data['high'] < data['open']) |
            (data['high'] < data['close']) |
            (data['low'] > data['open']) |
            (data['low'] > data['close'])
        ).sum()
        
        if invalid_rows > 0:
            logger.warning(f"发现 {invalid_rows} 行价格逻辑错误")
            return False
        
        return True
    
    def get_processing_stats(self) -> Dict[str, Any]:
        """获取处理统计信息"""
        return {
            'pipeline_steps': self.pipeline_steps,
            'cleaner_stats': self.cleaner.get_stats() if hasattr(self.cleaner, 'get_stats') else {},
            'transformer_stats': self.transformer.get_stats() if hasattr(self.transformer, 'get_stats') else {}
        }