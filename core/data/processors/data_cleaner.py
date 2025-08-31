#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据清洗器
=========

专门负责数据清洗和预处理的组件

Author: QuantTrader Team
Date: 2025-08-31
"""

from typing import Dict, List, Optional, Union, Any
import logging

try:
    import pandas as pd
    import numpy as np
except ImportError:
    pd = None
    np = None

logger = logging.getLogger(__name__)

class DataCleaner:
    """数据清洗器
    
    负责：
    - 缺失值处理
    - 异常值检测和处理
    - 重复数据处理
    - 数据类型转换
    - 基础数据验证
    """
    
    def __init__(self, config: Optional[Dict] = None):
        """初始化数据清洗器
        
        Args:
            config: 清洗配置参数
        """
        if pd is None:
            raise ImportError("数据清洗器需要pandas，请安装: pip install pandas")
        
        self.config = config or {}
        
        # 清洗参数
        self.missing_threshold = self.config.get('missing_threshold', 0.5)  # 缺失值阈值
        self.outlier_method = self.config.get('outlier_method', 'zscore')  # 异常值检测方法
        self.outlier_threshold = self.config.get('outlier_threshold', 3.0)  # 异常值阈值
        self.remove_duplicates = self.config.get('remove_duplicates', True)  # 是否删除重复行
        
        # 统计信息
        self.stats = {
            'rows_processed': 0,
            'rows_removed': 0,
            'missing_values_filled': 0,
            'outliers_removed': 0,
            'duplicates_removed': 0
        }
        
        logger.debug("✅ 数据清洗器初始化完成")
    
    def clean_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """通用数据清洗
        
        Args:
            data: 原始数据
            
        Returns:
            清洗后的数据
        """
        if data.empty:
            return data.copy()
        
        original_rows = len(data)
        result = data.copy()
        
        logger.debug(f"开始数据清洗，原始行数: {original_rows}")
        
        # 1. 删除重复行
        if self.remove_duplicates:
            before_dedup = len(result)
            result = result.drop_duplicates()
            duplicates_removed = before_dedup - len(result)
            self.stats['duplicates_removed'] += duplicates_removed
            if duplicates_removed > 0:
                logger.debug(f"删除 {duplicates_removed} 个重复行")
        
        # 2. 处理缺失值
        result = self.handle_missing_values(result)
        
        # 3. 数据类型转换
        result = self._convert_data_types(result)
        
        # 更新统计
        self.stats['rows_processed'] += original_rows
        self.stats['rows_removed'] += original_rows - len(result)
        
        logger.debug(f"数据清洗完成，最终行数: {len(result)}")
        return result
    
    def clean_price_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """清洗价格数据
        
        Args:
            data: 价格数据
            
        Returns:
            清洗后的数据
        """
        if data.empty:
            return data.copy()
        
        result = data.copy()
        
        # 通用清洗
        result = self.clean_data(result)
        
        # 价格数据特定清洗
        try:
            # 删除价格为0或负数的行
            price_cols = ['open', 'high', 'low', 'close']
            valid_price_cols = [col for col in price_cols if col in result.columns]
            
            if valid_price_cols:
                # 删除价格<=0的行
                before_price_clean = len(result)
                for col in valid_price_cols:
                    result = result[result[col] > 0]
                
                price_rows_removed = before_price_clean - len(result)
                if price_rows_removed > 0:
                    logger.debug(f"删除 {price_rows_removed} 行无效价格数据")
            
            # 检查价格逻辑关系
            if all(col in result.columns for col in ['high', 'low', 'open', 'close']):
                # high >= max(open, close, low)
                # low <= min(open, close, high)
                valid_logic = (
                    (result['high'] >= result[['open', 'close', 'low']].max(axis=1)) &
                    (result['low'] <= result[['open', 'close', 'high']].min(axis=1))
                )
                
                invalid_rows = (~valid_logic).sum()
                if invalid_rows > 0:
                    logger.warning(f"发现 {invalid_rows} 行价格逻辑错误，已删除")
                    result = result[valid_logic]
            
            # 处理成交量
            if 'volume' in result.columns:
                # 删除成交量为负数的行
                before_vol_clean = len(result)
                result = result[result['volume'] >= 0]
                vol_rows_removed = before_vol_clean - len(result)
                if vol_rows_removed > 0:
                    logger.debug(f"删除 {vol_rows_removed} 行无效成交量数据")
        
        except Exception as e:
            logger.error(f"价格数据清洗出现错误: {e}")
        
        return result
    
    def clean_financial_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """清洗财务数据
        
        Args:
            data: 财务数据
            
        Returns:
            清洗后的数据
        """
        if data.empty:
            return data.copy()
        
        result = data.copy()
        
        # 通用清洗
        result = self.clean_data(result)
        
        try:
            # 财务数据特定处理
            
            # 处理极值（如PE、PB等比率指标）
            ratio_columns = [col for col in result.columns if any(
                keyword in col.lower() for keyword in ['ratio', 'pe', 'pb', 'roe', 'roa']
            )]
            
            for col in ratio_columns:
                if result[col].dtype in ['float64', 'int64']:
                    # 删除极端异常值
                    q1 = result[col].quantile(0.01)
                    q99 = result[col].quantile(0.99)
                    before_clean = len(result)
                    result = result[(result[col] >= q1) & (result[col] <= q99)]
                    
                    extreme_removed = before_clean - len(result)
                    if extreme_removed > 0:
                        logger.debug(f"{col}: 删除 {extreme_removed} 个极端值")
        
        except Exception as e:
            logger.error(f"财务数据清洗出现错误: {e}")
        
        return result
    
    def handle_missing_values(self, 
                            data: pd.DataFrame,
                            method: str = 'auto') -> pd.DataFrame:
        """处理缺失值
        
        Args:
            data: 包含缺失值的数据
            method: 处理方法 ('auto', 'drop', 'fill_forward', 'fill_mean', 'interpolate')
            
        Returns:
            处理后的数据
        """
        if data.empty:
            return data.copy()
        
        result = data.copy()
        
        try:
            if method == 'auto':
                # 自动选择处理方法
                for column in result.columns:
                    missing_ratio = result[column].isnull().sum() / len(result)
                    
                    if missing_ratio > self.missing_threshold:
                        # 缺失率过高，删除列
                        result = result.drop(columns=[column])
                        logger.debug(f"删除高缺失率列: {column} ({missing_ratio:.1%})")
                    elif missing_ratio > 0:
                        # 根据数据类型填充
                        if result[column].dtype in ['object', 'string']:
                            # 字符串类型用众数填充
                            mode_value = result[column].mode()
                            if not mode_value.empty:
                                result[column] = result[column].fillna(mode_value.iloc[0])
                        else:
                            # 数值类型用前向填充
                            result[column] = result[column].fillna(method='ffill')
                            # 如果还有缺失值，用均值填充
                            if result[column].isnull().sum() > 0:
                                result[column] = result[column].fillna(result[column].mean())
                        
                        filled_count = (result[column].notnull() & data[column].isnull()).sum()
                        self.stats['missing_values_filled'] += filled_count
                        logger.debug(f"{column}: 填充 {filled_count} 个缺失值")
            
            elif method == 'drop':
                # 删除含有缺失值的行
                before_drop = len(result)
                result = result.dropna()
                dropped_rows = before_drop - len(result)
                if dropped_rows > 0:
                    logger.debug(f"删除 {dropped_rows} 行缺失值")
            
            elif method == 'fill_forward':
                # 前向填充
                result = result.fillna(method='ffill')
            
            elif method == 'fill_mean':
                # 均值填充（仅数值列）
                numeric_columns = result.select_dtypes(include=[np.number]).columns
                result[numeric_columns] = result[numeric_columns].fillna(result[numeric_columns].mean())
            
            elif method == 'interpolate':
                # 插值填充（仅数值列）
                numeric_columns = result.select_dtypes(include=[np.number]).columns
                result[numeric_columns] = result[numeric_columns].interpolate()
        
        except Exception as e:
            logger.error(f"处理缺失值时出现错误: {e}")
        
        return result
    
    def remove_outliers(self,
                       data: pd.DataFrame,
                       method: Optional[str] = None,
                       columns: Optional[List[str]] = None) -> pd.DataFrame:
        """删除异常值
        
        Args:
            data: 原始数据
            method: 检测方法 ('zscore', 'iqr', 'isolation_forest')
            columns: 要处理的列名列表
            
        Returns:
            删除异常值后的数据
        """
        if data.empty:
            return data.copy()
        
        method = method or self.outlier_method
        result = data.copy()
        
        if columns is None:
            # 只处理数值列
            columns = result.select_dtypes(include=[np.number]).columns.tolist()
        
        try:
            if method == 'zscore':
                # Z-score方法
                for column in columns:
                    if column in result.columns and result[column].dtype in ['float64', 'int64']:
                        z_scores = np.abs((result[column] - result[column].mean()) / result[column].std())
                        outlier_mask = z_scores > self.outlier_threshold
                        
                        outliers_count = outlier_mask.sum()
                        if outliers_count > 0:
                            result = result[~outlier_mask]
                            self.stats['outliers_removed'] += outliers_count
                            logger.debug(f"{column}: 删除 {outliers_count} 个异常值 (Z-score)")
            
            elif method == 'iqr':
                # IQR方法
                for column in columns:
                    if column in result.columns and result[column].dtype in ['float64', 'int64']:
                        Q1 = result[column].quantile(0.25)
                        Q3 = result[column].quantile(0.75)
                        IQR = Q3 - Q1
                        
                        lower_bound = Q1 - 1.5 * IQR
                        upper_bound = Q3 + 1.5 * IQR
                        
                        outlier_mask = (result[column] < lower_bound) | (result[column] > upper_bound)
                        outliers_count = outlier_mask.sum()
                        
                        if outliers_count > 0:
                            result = result[~outlier_mask]
                            self.stats['outliers_removed'] += outliers_count
                            logger.debug(f"{column}: 删除 {outliers_count} 个异常值 (IQR)")
        
        except Exception as e:
            logger.error(f"删除异常值时出现错误: {e}")
        
        return result
    
    def _convert_data_types(self, data: pd.DataFrame) -> pd.DataFrame:
        """转换数据类型"""
        result = data.copy()
        
        try:
            # 尝试转换日期列
            date_columns = [col for col in result.columns if 'date' in col.lower() or 'time' in col.lower()]
            for col in date_columns:
                try:
                    result[col] = pd.to_datetime(result[col])
                    logger.debug(f"转换 {col} 为日期类型")
                except:
                    continue
            
            # 尝试转换数值列
            for col in result.columns:
                if result[col].dtype == 'object':
                    try:
                        # 尝试转换为数值
                        result[col] = pd.to_numeric(result[col], errors='ignore')
                    except:
                        continue
        
        except Exception as e:
            logger.error(f"数据类型转换时出现错误: {e}")
        
        return result
    
    def get_stats(self) -> Dict[str, Any]:
        """获取清洗统计信息"""
        return self.stats.copy()
    
    def reset_stats(self):
        """重置统计信息"""
        for key in self.stats:
            self.stats[key] = 0