#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据转换器
=========

专门负责数据转换和标准化的组件

Author: QuantTrader Team
Date: 2025-08-31
"""

from typing import Dict, List, Optional, Union, Any, Tuple
import logging

try:
    import pandas as pd
    import numpy as np
except ImportError:
    pd = None
    np = None

logger = logging.getLogger(__name__)

class DataTransformer:
    """数据转换器
    
    负责：
    - 数据标准化和归一化
    - 列名标准化
    - 数据格式转换
    - 特征缩放
    - 数据重采样
    """
    
    def __init__(self, config: Optional[Dict] = None):
        """初始化数据转换器
        
        Args:
            config: 转换配置参数
        """
        if pd is None:
            raise ImportError("数据转换器需要pandas，请安装: pip install pandas")
        
        self.config = config or {}
        
        # 转换参数
        self.column_mapping = self.config.get('column_mapping', {})
        self.standardize_names = self.config.get('standardize_names', True)
        self.default_scaling_method = self.config.get('scaling_method', 'minmax')
        
        # 标准列名映射
        self.standard_columns = {
            # 价格数据
            'open': ['open', 'Open', 'OPEN', 'openPrice'],
            'high': ['high', 'High', 'HIGH', 'highPrice', 'highestPrice'],
            'low': ['low', 'Low', 'LOW', 'lowPrice', 'lowestPrice'],
            'close': ['close', 'Close', 'CLOSE', 'closePrice'],
            'volume': ['volume', 'Volume', 'VOLUME', 'vol', 'turnoverVol'],
            'amount': ['amount', 'Amount', 'turnoverValue', 'dealAmount'],
            
            # 时间相关
            'date': ['date', 'Date', 'DATE', 'tradeDate', 'trade_date'],
            'datetime': ['datetime', 'Datetime', 'DATETIME', 'timestamp'],
            
            # 股票信息
            'symbol': ['symbol', 'Symbol', 'SYMBOL', 'code', 'Code', 'secID', 'ticker'],
            'name': ['name', 'Name', 'NAME', 'secShortName', 'stock_name'],
            
            # 财务指标
            'pe': ['pe', 'PE', 'pe_ratio', 'PE_ratio'],
            'pb': ['pb', 'PB', 'pb_ratio', 'PB_ratio'],
            'roe': ['roe', 'ROE'],
            'roa': ['roa', 'ROA']
        }
        
        # 统计信息
        self.stats = {
            'columns_standardized': 0,
            'rows_transformed': 0,
            'scaling_applied': 0
        }
        
        logger.debug("✅ 数据转换器初始化完成")
    
    def standardize_columns(self, data: pd.DataFrame) -> pd.DataFrame:
        """标准化列名
        
        Args:
            data: 原始数据
            
        Returns:
            标准化列名后的数据
        """
        if data.empty:
            return data.copy()
        
        result = data.copy()
        
        try:
            if self.standardize_names:
                rename_mapping = {}
                
                # 使用自定义映射
                for new_name, old_names in self.column_mapping.items():
                    for old_name in old_names:
                        if old_name in result.columns:
                            rename_mapping[old_name] = new_name
                            break
                
                # 使用标准映射
                for standard_name, possible_names in self.standard_columns.items():
                    for possible_name in possible_names:
                        if possible_name in result.columns and possible_name not in rename_mapping:
                            rename_mapping[possible_name] = standard_name
                            break
                
                if rename_mapping:
                    result = result.rename(columns=rename_mapping)
                    self.stats['columns_standardized'] += len(rename_mapping)
                    logger.debug(f"标准化列名: {rename_mapping}")
            
            # 清理列名（移除特殊字符，转换为小写）
            cleaned_columns = {}
            for col in result.columns:
                cleaned_col = str(col).strip().replace(' ', '_').replace('-', '_').lower()
                if cleaned_col != col:
                    cleaned_columns[col] = cleaned_col
            
            if cleaned_columns:
                result = result.rename(columns=cleaned_columns)
                logger.debug(f"清理列名: {len(cleaned_columns)}个列名")
        
        except Exception as e:
            logger.error(f"标准化列名时出现错误: {e}")
        
        return result
    
    def normalize_column(self, 
                        series: pd.Series,
                        method: str = 'minmax') -> pd.Series:
        """标准化单个列
        
        Args:
            series: 要标准化的序列
            method: 标准化方法 ('minmax', 'zscore', 'robust')
            
        Returns:
            标准化后的序列
        """
        if series.empty or series.dtype not in ['float64', 'int64']:
            return series.copy()
        
        try:
            if method == 'minmax':
                # Min-Max归一化 [0, 1]
                min_val = series.min()
                max_val = series.max()
                if max_val != min_val:
                    return (series - min_val) / (max_val - min_val)
                else:
                    return pd.Series([0.5] * len(series), index=series.index)
            
            elif method == 'zscore':
                # Z-score标准化
                return (series - series.mean()) / series.std()
            
            elif method == 'robust':
                # 鲁棒标准化（使用中位数和IQR）
                median = series.median()
                q25 = series.quantile(0.25)
                q75 = series.quantile(0.75)
                iqr = q75 - q25
                if iqr != 0:
                    return (series - median) / iqr
                else:
                    return pd.Series([0] * len(series), index=series.index)
            
            else:
                logger.warning(f"未知的标准化方法: {method}")
                return series.copy()
        
        except Exception as e:
            logger.error(f"标准化列时出现错误: {e}")
            return series.copy()
    
    def scale_features(self,
                      data: pd.DataFrame,
                      columns: Optional[List[str]] = None,
                      method: Optional[str] = None) -> pd.DataFrame:
        """特征缩放
        
        Args:
            data: 原始数据
            columns: 要缩放的列名列表，None表示所有数值列
            method: 缩放方法
            
        Returns:
            缩放后的数据
        """
        if data.empty:
            return data.copy()
        
        method = method or self.default_scaling_method
        result = data.copy()
        
        if columns is None:
            # 选择所有数值列
            columns = result.select_dtypes(include=[np.number]).columns.tolist()
        
        try:
            for column in columns:
                if column in result.columns:
                    result[column] = self.normalize_column(result[column], method)
                    logger.debug(f"缩放列: {column} (方法: {method})")
            
            self.stats['scaling_applied'] += len(columns)
        
        except Exception as e:
            logger.error(f"特征缩放时出现错误: {e}")
        
        return result
    
    def normalize_financial_ratios(self, data: pd.DataFrame) -> pd.DataFrame:
        """标准化财务比率
        
        Args:
            data: 财务数据
            
        Returns:
            标准化后的数据
        """
        if data.empty:
            return data.copy()
        
        result = data.copy()
        
        try:
            # 识别比率类型的列
            ratio_columns = []
            for col in result.columns:
                col_lower = col.lower()
                if any(keyword in col_lower for keyword in [
                    'ratio', 'rate', 'margin', 'percent', 'pe', 'pb', 'roe', 'roa'
                ]):
                    if result[col].dtype in ['float64', 'int64']:
                        ratio_columns.append(col)
            
            # 对比率列进行特殊处理
            for col in ratio_columns:
                # 处理PE、PB等可能的极值
                if any(keyword in col.lower() for keyword in ['pe', 'pb']):
                    # 对PE、PB进行winsorization（截尾处理）
                    q5 = result[col].quantile(0.05)
                    q95 = result[col].quantile(0.95)
                    result[col] = result[col].clip(lower=q5, upper=q95)
                
                # 对所有比率进行robust标准化
                result[col] = self.normalize_column(result[col], method='robust')
                logger.debug(f"标准化财务比率: {col}")
        
        except Exception as e:
            logger.error(f"标准化财务比率时出现错误: {e}")
        
        return result
    
    def resample_data(self,
                     data: pd.DataFrame,
                     freq: str,
                     date_column: str = 'date',
                     agg_methods: Optional[Dict[str, str]] = None) -> pd.DataFrame:
        """数据重采样
        
        Args:
            data: 原始数据
            freq: 重采样频率 ('D', 'W', 'M', 'Q', 'Y')
            date_column: 日期列名
            agg_methods: 聚合方法字典
            
        Returns:
            重采样后的数据
        """
        if data.empty or date_column not in data.columns:
            return data.copy()
        
        result = data.copy()
        
        try:
            # 确保日期列是datetime类型
            if not pd.api.types.is_datetime64_any_dtype(result[date_column]):
                result[date_column] = pd.to_datetime(result[date_column])
            
            # 设置日期列为索引
            result = result.set_index(date_column)
            
            # 默认聚合方法
            if agg_methods is None:
                agg_methods = {}
                for col in result.columns:
                    if col.lower() in ['open']:
                        agg_methods[col] = 'first'
                    elif col.lower() in ['high']:
                        agg_methods[col] = 'max'
                    elif col.lower() in ['low']:
                        agg_methods[col] = 'min'
                    elif col.lower() in ['close']:
                        agg_methods[col] = 'last'
                    elif col.lower() in ['volume', 'amount']:
                        agg_methods[col] = 'sum'
                    else:
                        agg_methods[col] = 'mean'
            
            # 执行重采样
            result = result.resample(freq).agg(agg_methods)
            
            # 重置索引
            result = result.reset_index()
            
            logger.debug(f"数据重采样完成: {freq}, 行数: {len(data)} -> {len(result)}")
        
        except Exception as e:
            logger.error(f"数据重采样时出现错误: {e}")
            return data.copy()
        
        return result
    
    def pivot_data(self,
                  data: pd.DataFrame,
                  index: Union[str, List[str]],
                  columns: str,
                  values: str,
                  fill_value: Any = None) -> pd.DataFrame:
        """数据透视
        
        Args:
            data: 原始数据
            index: 行索引
            columns: 列索引
            values: 值列
            fill_value: 填充值
            
        Returns:
            透视后的数据
        """
        try:
            result = data.pivot_table(
                index=index,
                columns=columns,
                values=values,
                fill_value=fill_value,
                aggfunc='mean'  # 默认聚合函数
            )
            
            # 重置索引
            result = result.reset_index()
            
            # 平整化列名
            if isinstance(result.columns, pd.MultiIndex):
                result.columns = [f"{col[0]}_{col[1]}" if col[1] else str(col[0]) 
                                for col in result.columns]
            
            logger.debug(f"数据透视完成: {data.shape} -> {result.shape}")
            return result
        
        except Exception as e:
            logger.error(f"数据透视时出现错误: {e}")
            return data.copy()
    
    def encode_categorical(self,
                          data: pd.DataFrame,
                          columns: Optional[List[str]] = None,
                          method: str = 'onehot') -> pd.DataFrame:
        """编码分类变量
        
        Args:
            data: 原始数据
            columns: 要编码的列，None表示所有object类型列
            method: 编码方法 ('onehot', 'label')
            
        Returns:
            编码后的数据
        """
        if data.empty:
            return data.copy()
        
        result = data.copy()
        
        if columns is None:
            columns = result.select_dtypes(include=['object', 'category']).columns.tolist()
        
        try:
            for column in columns:
                if column in result.columns:
                    if method == 'onehot':
                        # 独热编码
                        dummies = pd.get_dummies(result[column], prefix=column)
                        result = pd.concat([result.drop(columns=[column]), dummies], axis=1)
                        logger.debug(f"独热编码: {column} -> {len(dummies.columns)}列")
                    
                    elif method == 'label':
                        # 标签编码
                        unique_values = result[column].unique()
                        label_mapping = {val: idx for idx, val in enumerate(unique_values)}
                        result[column] = result[column].map(label_mapping)
                        logger.debug(f"标签编码: {column} -> {len(unique_values)}个标签")
        
        except Exception as e:
            logger.error(f"编码分类变量时出现错误: {e}")
        
        return result
    
    def get_stats(self) -> Dict[str, Any]:
        """获取转换统计信息"""
        return self.stats.copy()
    
    def reset_stats(self):
        """重置统计信息"""
        for key in self.stats:
            self.stats[key] = 0