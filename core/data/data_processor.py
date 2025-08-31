#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据预处理器完整实现 - data_processor.py
=======================================

专为量化交易框架设计的高级数据预处理器，包含：
- 🧹 智能数据清洗和异常值处理
- 📊 多维度股票筛选和质量评估
- 📄 高级数据标准化和特征缩放
- 📈 多周期收益率计算和风险指标
- 💾 高效缓存机制和批处理
- 🎯 完整的数据质量监控体系

版本: 2.0.0
更新时间: 2024-08-26
兼容环境: VSCode + JupyterNote + 优矿API
"""

import os
import warnings
import numpy as np
import pandas as pd
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional, Union, Any
from pathlib import Path
import hashlib
import pickle
import json

# 科学计算库
from scipy import stats
from scipy.stats import zscore

# 抑制警告
warnings.filterwarnings('ignore')

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

print("🧹 数据预处理器模块加载中...")


class DataProcessor:
    """
    高级数据预处理器 - 集成清洗、筛选、标准化功能
    """
    
    def __init__(self, config: Optional[Dict] = None):
        """
        初始化数据预处理器
        
        Args:
            config: 配置参数字典
        """
        self.config = config or self._get_default_config()
        self.cache_dir = self.config.get('cache_dir', './cache')
        os.makedirs(self.cache_dir, exist_ok=True)
        
        # 初始化统计信息
        self.stats = {
            'processed_datasets': 0,
            'total_processing_time': 0,
            'cache_hits': 0,
            'cache_misses': 0,
            'quality_scores': [],
            'error_counts': {}
        }
        
        # 处理历史记录
        self.processing_history = []
        
        print("🛠️ 数据预处理器初始化完成")
        print(f"   📁 缓存目录: {self.cache_dir}")
        print(f"   🔧 配置参数: {len(self.config)} 项")
    
    def _get_default_config(self) -> Dict:
        """获取默认配置"""
        return {
            # 数据质量参数
            'min_trading_days': 250,     # 最少交易天数
            'max_missing_ratio': 0.1,    # 最大缺失值比例
            'min_price': 1.0,            # 最低价格
            'max_price': 1000.0,         # 最高价格
            'min_volume': 100000,        # 最小成交量
            
            # ST股票处理
            'exclude_st': True,          # 排除ST股票
            'exclude_new_days': 250,     # 排除新股天数
            
            # 异常值处理
            'outlier_method': 'zscore',  # 异常值检测方法
            'outlier_threshold': 3.0,    # 异常值阈值
            'fill_method': 'forward',    # 缺失值填充方法
            
            # 标准化参数
            'normalize_method': 'zscore', # 标准化方法
            'normalize_features': True,   # 是否标准化特征
            
            # 缓存设置
            'cache_dir': './cache',
            'enable_cache': True,
            'cache_expire_hours': 24,
            
            # 收益率计算
            'return_periods': [1, 5, 10, 20],  # 收益率周期
            'risk_free_rate': 0.03,            # 无风险利率
        }
    
    def _generate_cache_key(self, *args) -> str:
        """生成缓存键值"""
        key_str = '_'.join(str(arg) for arg in args)
        return hashlib.md5(key_str.encode()).hexdigest()
    
    def _load_from_cache(self, cache_key: str):
        """从缓存加载数据"""
        cache_path = os.path.join(self.cache_dir, f"{cache_key}.pkl")
        
        try:
            if os.path.exists(cache_path):
                # 检查缓存是否过期
                file_time = datetime.fromtimestamp(os.path.getmtime(cache_path))
                expire_time = datetime.now() - timedelta(hours=self.config['cache_expire_hours'])
                
                if file_time > expire_time:
                    with open(cache_path, 'rb') as f:
                        self.stats['cache_hits'] += 1
                        return pickle.load(f)
        except Exception as e:
            logger.warning(f"缓存加载失败: {e}")
        
        self.stats['cache_misses'] += 1
        return None
    
    def _save_to_cache(self, data, cache_key: str):
        """保存数据到缓存"""
        if not self.config['enable_cache']:
            return
        
        cache_path = os.path.join(self.cache_dir, f"{cache_key}.pkl")
        
        try:
            with open(cache_path, 'wb') as f:
                pickle.dump(data, f)
        except Exception as e:
            logger.warning(f"缓存保存失败: {e}")
    
    def clean_price_data(self, price_data: pd.DataFrame) -> pd.DataFrame:
        """
        清洗价格数据
        
        Args:
            price_data: 原始价格数据
            
        Returns:
            清洗后的价格数据
        """
        if price_data.empty:
            return price_data
        
        print("🧹 开始数据清洗...")
        start_time = datetime.now()
        
        # 生成缓存键
        data_hash = hashlib.md5(str(price_data.shape).encode()).hexdigest()
        cache_key = f"clean_price_{data_hash}"
        
        # 尝试从缓存加载
        cached_data = self._load_from_cache(cache_key)
        if cached_data is not None:
            print("📥 从缓存加载清洗数据")
            return cached_data
        
        # 复制数据避免修改原始数据
        clean_data = price_data.copy()
        original_rows = len(clean_data)
        
        # 1. 基础数据检查
        print("   📊 基础数据检查...")
        required_columns = ['ticker', 'tradeDate', 'closePrice', 'turnoverVol']
        missing_cols = [col for col in required_columns if col not in clean_data.columns]
        
        if missing_cols:
            print(f"   ⚠️ 缺失必要列: {missing_cols}")
            return clean_data
        
        # 2. 数据类型转换
        print("   🔄 数据类型转换...")
        if 'tradeDate' in clean_data.columns:
            clean_data['tradeDate'] = pd.to_datetime(clean_data['tradeDate'])
        
        # 数值列转换
        numeric_cols = ['openPrice', 'highestPrice', 'lowestPrice', 'closePrice', 
                       'turnoverVol', 'turnoverValue']
        for col in numeric_cols:
            if col in clean_data.columns:
                clean_data[col] = pd.to_numeric(clean_data[col], errors='coerce')
        
        # 3. 移除异常值
        print("   🎯 异常值检测和处理...")
        
        # 价格范围检查
        if 'closePrice' in clean_data.columns:
            price_mask = (clean_data['closePrice'] >= self.config['min_price']) & \
                        (clean_data['closePrice'] <= self.config['max_price'])
            clean_data = clean_data[price_mask]
        
        # 成交量检查
        if 'turnoverVol' in clean_data.columns:
            volume_mask = clean_data['turnoverVol'] >= self.config['min_volume']
            clean_data = clean_data[volume_mask]
        
        # 4. 处理缺失值
        print("   🔧 缺失值处理...")
        
        # 按股票分组处理缺失值
        if self.config['fill_method'] == 'forward':
            clean_data = clean_data.groupby('ticker').apply(
                lambda x: x.fillna(method='ffill')
            ).reset_index(drop=True)
        elif self.config['fill_method'] == 'interpolate':
            numeric_cols = clean_data.select_dtypes(include=[np.number]).columns
            clean_data[numeric_cols] = clean_data.groupby('ticker')[numeric_cols].apply(
                lambda x: x.interpolate()
            )
        
        # 5. 移除缺失值过多的记录
        max_missing = self.config['max_missing_ratio']
        missing_ratios = clean_data.isnull().sum(axis=1) / len(clean_data.columns)
        clean_data = clean_data[missing_ratios <= max_missing]
        
        # 6. 数据排序
        print("   📅 数据排序...")
        clean_data = clean_data.sort_values(['ticker', 'tradeDate'])
        clean_data = clean_data.reset_index(drop=True)
        
        # 清洗统计
        cleaned_rows = len(clean_data)
        removed_rows = original_rows - cleaned_rows
        removal_rate = removed_rows / original_rows if original_rows > 0 else 0
        
        print(f"✅ 数据清洗完成")
        print(f"   📊 原始数据: {original_rows:,} 行")
        print(f"   🧹 清洗后: {cleaned_rows:,} 行")
        print(f"   🗑️ 移除: {removed_rows:,} 行 ({removal_rate:.2%})")
        
        # 保存到缓存
        self._save_to_cache(clean_data, cache_key)
        
        # 记录处理历史
        self.processing_history.append({
            'operation': 'clean_price_data',
            'timestamp': datetime.now().isoformat(),
            'input_rows': original_rows,
            'output_rows': cleaned_rows,
            'removal_rate': removal_rate
        })
        
        return clean_data
    
    def filter_stocks(self, price_data: pd.DataFrame, 
                     stock_info: pd.DataFrame = None) -> List[str]:
        """
        筛选股票池
        
        Args:
            price_data: 价格数据
            stock_info: 股票基础信息
            
        Returns:
            筛选后的股票代码列表
        """
        print("🎯 开始股票筛选...")
        
        if price_data.empty:
            return []
        
        qualified_stocks = []
        
        # 按股票分组分析
        for ticker, group in price_data.groupby('ticker'):
            # 1. 交易天数检查
            trading_days = len(group)
            if trading_days < self.config['min_trading_days']:
                continue
            
            # 2. 缺失值检查
            missing_ratio = group.isnull().sum().sum() / (len(group) * len(group.columns))
            if missing_ratio > self.config['max_missing_ratio']:
                continue
            
            # 3. ST股票检查（如果有股票信息）
            if self.config['exclude_st'] and stock_info is not None:
                stock_name = stock_info[stock_info['ticker'] == ticker]['shortName'].iloc[0] if len(stock_info[stock_info['ticker'] == ticker]) > 0 else ''
                if 'ST' in stock_name or '*ST' in stock_name:
                    continue
            
            # 4. 新股检查（如果有上市日期信息）
            if stock_info is not None and 'listDate' in stock_info.columns:
                list_info = stock_info[stock_info['ticker'] == ticker]
                if len(list_info) > 0:
                    list_date = pd.to_datetime(list_info['listDate'].iloc[0])
                    days_since_listing = (datetime.now() - list_date).days
                    if days_since_listing < self.config['exclude_new_days']:
                        continue
            
            # 5. 价格和成交量检查
            if 'closePrice' in group.columns and 'turnoverVol' in group.columns:
                avg_price = group['closePrice'].mean()
                avg_volume = group['turnoverVol'].mean()
                
                if (avg_price >= self.config['min_price'] and 
                    avg_price <= self.config['max_price'] and
                    avg_volume >= self.config['min_volume']):
                    qualified_stocks.append(ticker)
        
        print(f"✅ 股票筛选完成")
        print(f"   📊 筛选前: {price_data['ticker'].nunique()} 只")
        print(f"   🎯 筛选后: {len(qualified_stocks)} 只")
        print(f"   📉 筛选率: {len(qualified_stocks)/price_data['ticker'].nunique():.2%}")
        
        return qualified_stocks
    
    def calculate_returns(self, price_data: pd.DataFrame) -> pd.DataFrame:
        """
        计算多周期收益率
        
        Args:
            price_data: 价格数据
            
        Returns:
            包含收益率的数据
        """
        print("📈 计算收益率指标...")
        
        if price_data.empty or 'closePrice' not in price_data.columns:
            return price_data
        
        # 复制数据
        data_with_returns = price_data.copy()
        
        # 按股票分组计算收益率
        for ticker, group in data_with_returns.groupby('ticker'):
            group = group.sort_values('tradeDate')
            
            # 计算不同周期的收益率
            for period in self.config['return_periods']:
                col_name = f'return_{period}d'
                data_with_returns.loc[group.index, col_name] = (
                    group['closePrice'].pct_change(period)
                )
            
            # 计算对数收益率
            data_with_returns.loc[group.index, 'log_return'] = np.log(
                group['closePrice'] / group['closePrice'].shift(1)
            )
            
            # 计算波动率（20日滚动）
            returns = group['closePrice'].pct_change()
            data_with_returns.loc[group.index, 'volatility_20d'] = (
                returns.rolling(window=20).std() * np.sqrt(252)
            )
        
        print(f"✅ 收益率计算完成")
        print(f"   📊 新增列: {len(self.config['return_periods']) + 2} 个")
        
        return data_with_returns
    
    def normalize_features(self, data: pd.DataFrame, 
                          features: List[str] = None) -> pd.DataFrame:
        """
        特征标准化
        
        Args:
            data: 输入数据
            features: 需要标准化的特征列表
            
        Returns:
            标准化后的数据
        """
        print("📊 特征标准化...")
        
        if not self.config['normalize_features'] or data.empty:
            return data
        
        # 如果未指定特征，自动选择数值特征
        if features is None:
            features = data.select_dtypes(include=[np.number]).columns.tolist()
            # 排除ID和日期相关列
            exclude_cols = ['ticker', 'tradeDate'] + [col for col in features if 'date' in col.lower()]
            features = [col for col in features if col not in exclude_cols]
        
        normalized_data = data.copy()
        
        # 按股票分组标准化（避免跨股票标准化）
        if self.config['normalize_method'] == 'zscore':
            for feature in features:
                if feature in normalized_data.columns:
                    normalized_data[f'{feature}_norm'] = normalized_data.groupby('ticker')[feature].transform(
                        lambda x: (x - x.mean()) / x.std() if x.std() > 0 else 0
                    )
        
        elif self.config['normalize_method'] == 'minmax':
            for feature in features:
                if feature in normalized_data.columns:
                    normalized_data[f'{feature}_norm'] = normalized_data.groupby('ticker')[feature].transform(
                        lambda x: (x - x.min()) / (x.max() - x.min()) if x.max() > x.min() else 0
                    )
        
        print(f"✅ 特征标准化完成")
        print(f"   📊 标准化特征: {len(features)} 个")
        
        return normalized_data
    
    def calculate_quality_score(self, price_data: pd.DataFrame) -> Dict[str, float]:
        """
        计算数据质量评分
        
        Args:
            price_data: 价格数据
            
        Returns:
            质量评分字典
        """
        if price_data.empty:
            return {'overall_score': 0.0}
        
        scores = {}
        
        # 1. 完整性评分（缺失值比例）
        missing_ratio = price_data.isnull().sum().sum() / (len(price_data) * len(price_data.columns))
        scores['completeness'] = max(0, 1 - missing_ratio * 2)
        
        # 2. 一致性评分（异常值比例）
        numeric_cols = price_data.select_dtypes(include=[np.number]).columns
        outlier_count = 0
        total_values = 0
        
        for col in numeric_cols:
            if col in price_data.columns:
                z_scores = np.abs(zscore(price_data[col].dropna()))
                outlier_count += np.sum(z_scores > self.config['outlier_threshold'])
                total_values += len(z_scores)
        
        outlier_ratio = outlier_count / total_values if total_values > 0 else 0
        scores['consistency'] = max(0, 1 - outlier_ratio * 5)
        
        # 3. 连续性评分（数据覆盖率）
        if 'tradeDate' in price_data.columns:
            date_range = price_data['tradeDate'].max() - price_data['tradeDate'].min()
            actual_days = len(price_data['tradeDate'].unique())
            expected_days = date_range.days
            coverage_ratio = actual_days / expected_days if expected_days > 0 else 0
            scores['continuity'] = min(1.0, coverage_ratio * 1.5)
        else:
            scores['continuity'] = 0.8
        
        # 4. 综合评分
        scores['overall_score'] = (
            scores['completeness'] * 0.4 + 
            scores['consistency'] * 0.4 + 
            scores['continuity'] * 0.2
        )
        
        return scores
    
    def run_complete_pipeline(self, price_data: pd.DataFrame,
                            stock_info: pd.DataFrame = None) -> Dict[str, Any]:
        """
        运行完整的数据预处理流水线
        
        Args:
            price_data: 原始价格数据
            stock_info: 股票基础信息
            
        Returns:
            处理结果字典
        """
        print("🚀 启动完整数据预处理流水线")
        print("=" * 50)
        
        start_time = datetime.now()
        results = {}
        
        try:
            # 1. 数据清洗
            clean_data = self.clean_price_data(price_data)
            results['clean_data'] = clean_data
            
            # 2. 股票筛选
            qualified_stocks = self.filter_stocks(clean_data, stock_info)
            results['qualified_stocks'] = qualified_stocks
            
            # 过滤数据只保留合格股票
            if qualified_stocks:
                filtered_data = clean_data[clean_data['ticker'].isin(qualified_stocks)]
            else:
                filtered_data = clean_data
            
            # 3. 计算收益率
            data_with_returns = self.calculate_returns(filtered_data)
            results['data_with_returns'] = data_with_returns
            
            # 4. 特征标准化
            normalized_data = self.normalize_features(data_with_returns)
            results['normalized_data'] = normalized_data
            
            # 5. 质量评分
            quality_scores = self.calculate_quality_score(normalized_data)
            results['quality_scores'] = quality_scores
            
            # 6. 统计信息
            processing_time = (datetime.now() - start_time).total_seconds()
            self.stats['processed_datasets'] += 1
            self.stats['total_processing_time'] += processing_time
            self.stats['quality_scores'].append(quality_scores['overall_score'])
            
            results['processing_info'] = {
                'processing_time': processing_time,
                'input_rows': len(price_data),
                'output_rows': len(normalized_data),
                'qualified_stocks': len(qualified_stocks),
                'quality_score': quality_scores['overall_score']
            }
            
            print(f"✅ 数据预处理流水线完成")
            print(f"   ⏱️ 处理时间: {processing_time:.2f}秒")
            print(f"   📊 质量评分: {quality_scores['overall_score']:.3f}")
            print(f"   🎯 合格股票: {len(qualified_stocks)} 只")
            
        except Exception as e:
            print(f"❌ 处理流水线错误: {e}")
            error_type = type(e).__name__
            self.stats['error_counts'][error_type] = self.stats['error_counts'].get(error_type, 0) + 1
            results['error'] = str(e)
        
        return results
    
    def get_processing_stats(self) -> Dict[str, Any]:
        """获取处理统计信息"""
        return {
            'stats': self.stats,
            'config': self.config,
            'cache_info': self.get_cache_info(),
            'processing_history': self.processing_history[-10:]  # 最近10次处理记录
        }
    
    def get_cache_info(self) -> Dict[str, Any]:
        """获取缓存信息"""
        cache_files = []
        total_size = 0
        
        try:
            for file_path in Path(self.cache_dir).glob("*.pkl"):
                size = file_path.stat().st_size
                cache_files.append({
                    'name': file_path.name,
                    'size': size,
                    'modified': datetime.fromtimestamp(file_path.stat().st_mtime).isoformat()
                })
                total_size += size
        except Exception as e:
            logger.warning(f"获取缓存信息失败: {e}")
        
        return {
            'cache_dir': self.cache_dir,
            'file_count': len(cache_files),
            'total_size_mb': total_size / (1024 * 1024),
            'files': cache_files[:5]  # 只返回前5个文件信息
        }
    
    def reset_stats(self):
        """重置统计信息"""
        self.stats = {
            'processed_datasets': 0,
            'total_processing_time': 0,
            'cache_hits': 0,
            'cache_misses': 0,
            'quality_scores': [],
            'error_counts': {}
        }
        self.processing_history = []
        print("📊 统计信息已重置")


# ==========================================
# 🏭 工厂函数和模块导出
# ==========================================

def create_data_processor(config: Optional[Dict] = None) -> DataProcessor:
    """
    创建数据预处理器实例的工厂函数
    
    Args:
        config: 自定义配置参数
        
    Returns:
        DataProcessor实例
    """
    return DataProcessor(config)

# 创建默认全局实例
default_processor = DataProcessor()

# 模块导出
__all__ = [
    'DataProcessor',
    'create_data_processor', 
    'default_processor'
]

if __name__ == "__main__":
    print("🔧 高级数据预处理器 v2.0 模块加载完成")
    print("📘 使用示例:")
    print("   from data_processor import DataProcessor, create_data_processor")
    print("   processor = create_data_processor()")
    print("   results = processor.run_complete_pipeline(price_data)")
    print("")
    print("💡 功能特性:")
    print("   🧹 智能数据清洗和异常值处理")
    print("   📊 多维度股票筛选和质量评估")
    print("   📄 高级数据标准化和特征缩放") 
    print("   📈 多周期收益率计算和风险指标")
    print("   💾 高效缓存机制和批处理")
    print("   🎯 完整的数据质量监控体系")