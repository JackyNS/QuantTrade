#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
因子排序器 - Factor Ranker
===========================

多因子评分和排序系统

作者: QuantTrader Team
版本: 1.0.0
更新: 2025-08-30
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Union
from scipy import stats
import logging

class FactorRanker:
    """
    因子排序器
    
    功能：
    1. 因子标准化和去极值
    2. 因子打分和加权
    3. 综合评分计算
    4. 分组和排序
    5. 因子有效性分析
    """
    
    def __init__(self, config: Optional[Dict] = None):
        """初始化因子排序器"""
        self.config = config or self._get_default_config()
        self.logger = self._setup_logger()
        self.factor_scores = {}
        self.factor_weights = {}
        
    def _get_default_config(self) -> Dict:
        """默认配置"""
        return {
            # 因子权重
            'weights': {
                'value': 0.25,       # 价值因子
                'growth': 0.25,      # 成长因子
                'quality': 0.20,     # 质量因子
                'momentum': 0.15,    # 动量因子
                'volatility': 0.15,  # 波动因子
            },
            
            # 因子配置
            'factors': {
                'value': ['pe_inverse', 'pb_inverse', 'ps_inverse', 'dividend_yield'],
                'growth': ['revenue_growth', 'profit_growth', 'roe_growth'],
                'quality': ['roe', 'roa', 'gross_margin', 'debt_to_equity_inverse'],
                'momentum': ['return_1m', 'return_3m', 'return_6m', 'rsi'],
                'volatility': ['volatility_inverse', 'sharpe_ratio', 'max_drawdown_inverse'],
            },
            
            # 标准化方法
            'normalization': {
                'method': 'zscore',  # zscore/minmax/rank/percentile
                'winsorize': True,   # 是否去极值
                'winsorize_limits': (0.01, 0.99),  # 去极值范围
            },
            
            # 评分方法
            'scoring': {
                'method': 'linear',  # linear/nonlinear/rank
                'scale': (0, 100),  # 评分范围
            },
            
            # 分组配置
            'grouping': {
                'n_groups': 10,     # 分组数量
                'group_names': ['G1', 'G2', 'G3', 'G4', 'G5', 
                              'G6', 'G7', 'G8', 'G9', 'G10'],
            }
        }
    
    def preprocess_factors(self, data: pd.DataFrame, 
                          factors: List[str]) -> pd.DataFrame:
        """
        因子预处理
        
        Args:
            data: 原始数据
            factors: 因子列表
            
        Returns:
            预处理后的数据
        """
        processed = data[factors].copy()
        
        # 处理缺失值
        processed = processed.fillna(processed.median())
        
        # 去极值
        if self.config['normalization']['winsorize']:
            lower, upper = self.config['normalization']['winsorize_limits']
            for factor in factors:
                if factor in processed.columns:
                    low_val = processed[factor].quantile(lower)
                    high_val = processed[factor].quantile(upper)
                    processed[factor] = processed[factor].clip(low_val, high_val)
        
        return processed
    
    def normalize_factors(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        因子标准化
        
        Args:
            data: 因子数据
            
        Returns:
            标准化后的数据
        """
        normalized = data.copy()
        method = self.config['normalization']['method']
        
        for column in normalized.columns:
            if method == 'zscore':
                # Z-score标准化
                mean = normalized[column].mean()
                std = normalized[column].std()
                if std > 0:
                    normalized[column] = (normalized[column] - mean) / std
                else:
                    normalized[column] = 0
                    
            elif method == 'minmax':
                # Min-Max标准化
                min_val = normalized[column].min()
                max_val = normalized[column].max()
                if max_val > min_val:
                    normalized[column] = (normalized[column] - min_val) / (max_val - min_val)
                else:
                    normalized[column] = 0.5
                    
            elif method == 'rank':
                # 排序标准化
                normalized[column] = normalized[column].rank(pct=True)
                
            elif method == 'percentile':
                # 百分位数标准化
                normalized[column] = stats.rankdata(normalized[column], method='average') / len(normalized)
        
        return normalized
    
    def calculate_factor_scores(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        计算因子得分
        
        Args:
            data: 标准化后的因子数据
            
        Returns:
            包含因子得分的数据
        """
        scores = pd.DataFrame(index=data.index)
        factor_config = self.config['factors']
        
        for category, factor_list in factor_config.items():
            # 获取该类别的因子
            available_factors = [f for f in factor_list if f in data.columns]
            
            if available_factors:
                # 计算该类别的平均得分
                category_score = data[available_factors].mean(axis=1)
                scores[f'{category}_score'] = category_score
                
                # 保存详细得分
                self.factor_scores[category] = {
                    'factors': available_factors,
                    'scores': category_score
                }
        
        return scores
    
    def calculate_composite_score(self, scores: pd.DataFrame) -> pd.Series:
        """
        计算综合得分
        
        Args:
            scores: 各类因子得分
            
        Returns:
            综合得分
        """
        weights = self.config['weights']
        composite = pd.Series(0, index=scores.index)
        
        total_weight = 0
        for category, weight in weights.items():
            score_col = f'{category}_score'
            if score_col in scores.columns:
                composite += scores[score_col] * weight
                total_weight += weight
        
        # 归一化
        if total_weight > 0:
            composite = composite / total_weight
        
        # 缩放到指定范围
        min_scale, max_scale = self.config['scoring']['scale']
        composite = min_scale + (max_scale - min_scale) * (composite - composite.min()) / (composite.max() - composite.min())
        
        return composite
    
    def rank_stocks(self, data: pd.DataFrame, 
                   ascending: bool = False) -> pd.DataFrame:
        """
        股票排序
        
        Args:
            data: 包含综合得分的数据
            ascending: 是否升序
            
        Returns:
            排序后的数据
        """
        if 'composite_score' not in data.columns:
            self.logger.warning("未找到综合得分列")
            return data
        
        # 排序
        ranked = data.sort_values('composite_score', ascending=ascending)
        
        # 添加排名
        ranked['rank'] = range(1, len(ranked) + 1)
        
        # 添加百分位
        ranked['percentile'] = 100 * (1 - ranked['rank'] / len(ranked))
        
        return ranked
    
    def group_stocks(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        股票分组
        
        Args:
            data: 包含综合得分的数据
            
        Returns:
            包含分组的数据
        """
        if 'composite_score' not in data.columns:
            return data
        
        data = data.copy()
        n_groups = self.config['grouping']['n_groups']
        group_names = self.config['grouping']['group_names']
        
        # 使用qcut进行等频分组
        data['group'] = pd.qcut(
            data['composite_score'], 
            n_groups, 
            labels=group_names[:n_groups]
        )
        
        return data
    
    def analyze_factors(self, data: pd.DataFrame, 
                       returns: pd.Series) -> Dict:
        """
        因子有效性分析
        
        Args:
            data: 因子数据
            returns: 收益率数据
            
        Returns:
            分析结果
        """
        analysis = {}
        
        for factor in data.columns:
            if factor in ['ticker', 'date', 'composite_score']:
                continue
            
            # 计算IC（信息系数）
            ic = data[factor].corr(returns, method='spearman')
            
            # 计算IR（信息比率）
            ic_series = data.groupby('date')[factor].apply(
                lambda x: x.corr(returns.loc[x.index], method='spearman')
            )
            ir = ic_series.mean() / ic_series.std() if ic_series.std() > 0 else 0
            
            analysis[factor] = {
                'ic': ic,
                'ir': ir,
                'ic_mean': ic_series.mean(),
                'ic_std': ic_series.std(),
                'positive_ratio': (ic_series > 0).mean()
            }
        
        return analysis
    
    def rank(self, data: pd.DataFrame,
            factors: List[str] = None,
            return_details: bool = False) -> Union[pd.DataFrame, Tuple[pd.DataFrame, Dict]]:
        """
        执行因子排序
        
        Args:
            data: 原始数据
            factors: 因子列表
            return_details: 是否返回详细信息
            
        Returns:
            排序后的数据（和详细信息）
        """
        self.logger.info("🎯 开始因子排序...")
        
        # 确定要使用的因子
        if factors is None:
            # 使用配置中的所有因子
            factors = []
            for factor_list in self.config['factors'].values():
                factors.extend(factor_list)
        
        # 筛选存在的因子
        available_factors = [f for f in factors if f in data.columns]
        
        if not available_factors:
            self.logger.warning("未找到可用因子")
            return data
        
        self.logger.info(f"使用 {len(available_factors)} 个因子")
        
        # 1. 预处理
        processed = self.preprocess_factors(data, available_factors)
        
        # 2. 标准化
        normalized = self.normalize_factors(processed)
        
        # 3. 计算因子得分
        scores = self.calculate_factor_scores(normalized)
        
        # 4. 计算综合得分
        data['composite_score'] = self.calculate_composite_score(scores)
        
        # 5. 排序
        ranked = self.rank_stocks(data)
        
        # 6. 分组
        grouped = self.group_stocks(ranked)
        
        self.logger.info(f"✅ 因子排序完成，最高分: {grouped['composite_score'].max():.2f}")
        
        if return_details:
            details = {
                'factor_scores': self.factor_scores,
                'weights': self.config['weights'],
                'factors_used': available_factors
            }
            return grouped, details
        
        return grouped
    
    def get_top_stocks(self, data: pd.DataFrame, 
                      n: int = 10) -> pd.DataFrame:
        """获取得分最高的股票"""
        if 'composite_score' not in data.columns:
            return data.head(n)
        
        return data.nlargest(n, 'composite_score')
    
    def _setup_logger(self) -> logging.Logger:
        """设置日志"""
        logger = logging.getLogger('FactorRanker')
        logger.setLevel(logging.INFO)
        return logger