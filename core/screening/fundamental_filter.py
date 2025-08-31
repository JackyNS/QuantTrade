#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
基本面筛选器 - Fundamental Filter
==================================

专注于基本面指标的深度筛选，包括财务指标、估值指标、成长指标等

作者: QuantTrader Team
版本: 1.0.0
更新: 2025-08-30
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
import logging

class FundamentalFilter:
    """
    基本面筛选器
    
    筛选维度：
    1. 估值指标：PE、PB、PS、PEG等
    2. 盈利能力：ROE、ROA、毛利率、净利率等
    3. 成长能力：营收增长、利润增长、ROE增长等
    4. 财务健康：负债率、流动比率、速动比率等
    5. 运营效率：存货周转、应收账款周转等
    6. 股东回报：分红率、股息率等
    """
    
    def __init__(self, config: Optional[Dict] = None):
        """初始化基本面筛选器"""
        self.config = config or self._get_default_config()
        self.logger = self._setup_logger()
        self.filter_results = {}
        
    def _get_default_config(self) -> Dict:
        """默认配置"""
        return {
            # 估值指标
            'valuation': {
                'pe_ratio': {'min': 0, 'max': 30},
                'pe_ttm': {'min': 0, 'max': 30},
                'pb_ratio': {'min': 0, 'max': 5},
                'ps_ratio': {'min': 0, 'max': 10},
                'peg_ratio': {'min': 0, 'max': 2},
                'ev_ebitda': {'min': 0, 'max': 20},
            },
            
            # 盈利能力
            'profitability': {
                'roe': {'min': 0.10, 'max': None},
                'roa': {'min': 0.05, 'max': None},
                'gross_margin': {'min': 0.20, 'max': None},
                'net_margin': {'min': 0.05, 'max': None},
                'operating_margin': {'min': 0.10, 'max': None},
            },
            
            # 成长能力
            'growth': {
                'revenue_growth_yoy': {'min': 0, 'max': None},
                'revenue_growth_3y': {'min': 0.10, 'max': None},
                'profit_growth_yoy': {'min': 0, 'max': None},
                'profit_growth_3y': {'min': 0.10, 'max': None},
                'roe_growth': {'min': 0, 'max': None},
            },
            
            # 财务健康
            'financial_health': {
                'debt_to_equity': {'min': 0, 'max': 1.0},
                'debt_to_asset': {'min': 0, 'max': 0.6},
                'current_ratio': {'min': 1.0, 'max': None},
                'quick_ratio': {'min': 0.8, 'max': None},
                'interest_coverage': {'min': 2.0, 'max': None},
            },
            
            # 运营效率
            'efficiency': {
                'inventory_turnover': {'min': 4, 'max': None},
                'receivable_turnover': {'min': 6, 'max': None},
                'asset_turnover': {'min': 0.5, 'max': None},
                'cash_conversion_cycle': {'min': None, 'max': 90},
            },
            
            # 股东回报
            'shareholder_return': {
                'dividend_yield': {'min': 0.02, 'max': None},
                'payout_ratio': {'min': 0.20, 'max': 0.70},
                'buyback_yield': {'min': 0, 'max': None},
                'total_yield': {'min': 0.03, 'max': None},
            },
            
            # 质量因子
            'quality': {
                'earnings_quality': {'min': 0.6, 'max': None},
                'accruals_ratio': {'min': None, 'max': 0.1},
                'cash_to_income': {'min': 0.8, 'max': None},
            }
        }
    
    def filter_valuation(self, data: pd.DataFrame) -> pd.DataFrame:
        """估值指标筛选"""
        self.logger.info("📊 估值指标筛选...")
        filtered = data.copy()
        criteria = self.config['valuation']
        
        for metric, bounds in criteria.items():
            if metric in filtered.columns:
                if bounds['min'] is not None:
                    filtered = filtered[filtered[metric] >= bounds['min']]
                if bounds['max'] is not None:
                    filtered = filtered[filtered[metric] <= bounds['max']]
        
        self.filter_results['valuation'] = len(filtered)
        return filtered
    
    def filter_profitability(self, data: pd.DataFrame) -> pd.DataFrame:
        """盈利能力筛选"""
        self.logger.info("💰 盈利能力筛选...")
        filtered = data.copy()
        criteria = self.config['profitability']
        
        for metric, bounds in criteria.items():
            if metric in filtered.columns:
                if bounds['min'] is not None:
                    filtered = filtered[filtered[metric] >= bounds['min']]
                if bounds['max'] is not None:
                    filtered = filtered[filtered[metric] <= bounds['max']]
        
        self.filter_results['profitability'] = len(filtered)
        return filtered
    
    def filter_growth(self, data: pd.DataFrame) -> pd.DataFrame:
        """成长能力筛选"""
        self.logger.info("📈 成长能力筛选...")
        filtered = data.copy()
        criteria = self.config['growth']
        
        for metric, bounds in criteria.items():
            if metric in filtered.columns:
                if bounds['min'] is not None:
                    filtered = filtered[filtered[metric] >= bounds['min']]
                if bounds['max'] is not None:
                    filtered = filtered[filtered[metric] <= bounds['max']]
        
        self.filter_results['growth'] = len(filtered)
        return filtered
    
    def filter_financial_health(self, data: pd.DataFrame) -> pd.DataFrame:
        """财务健康筛选"""
        self.logger.info("🏥 财务健康筛选...")
        filtered = data.copy()
        criteria = self.config['financial_health']
        
        for metric, bounds in criteria.items():
            if metric in filtered.columns:
                if bounds['min'] is not None:
                    filtered = filtered[filtered[metric] >= bounds['min']]
                if bounds['max'] is not None:
                    filtered = filtered[filtered[metric] <= bounds['max']]
        
        self.filter_results['financial_health'] = len(filtered)
        return filtered
    
    def calculate_quality_score(self, data: pd.DataFrame) -> pd.Series:
        """计算质量分数"""
        score = pd.Series(0, index=data.index)
        
        # ROE质量（25%）
        if 'roe' in data.columns:
            roe_score = np.clip(data['roe'] / 0.15, 0, 1)  # ROE 15%为满分
            score += roe_score * 0.25
        
        # 盈利稳定性（25%）
        if 'earnings_volatility' in data.columns:
            stability_score = 1 - np.clip(data['earnings_volatility'] / 0.3, 0, 1)
            score += stability_score * 0.25
        
        # 负债水平（25%）
        if 'debt_to_equity' in data.columns:
            debt_score = 1 - np.clip(data['debt_to_equity'] / 1.0, 0, 1)
            score += debt_score * 0.25
        
        # 现金流质量（25%）
        if 'cash_to_income' in data.columns:
            cash_score = np.clip(data['cash_to_income'], 0, 1)
            score += cash_score * 0.25
        
        return score
    
    def filter(self, data: pd.DataFrame, 
              dimensions: List[str] = None) -> pd.DataFrame:
        """
        执行基本面筛选
        
        Args:
            data: 股票数据
            dimensions: 筛选维度
            
        Returns:
            筛选后的数据
        """
        if dimensions is None:
            dimensions = ['valuation', 'profitability', 'growth', 'financial_health']
        
        result = data.copy()
        
        for dim in dimensions:
            if dim == 'valuation':
                result = self.filter_valuation(result)
            elif dim == 'profitability':
                result = self.filter_profitability(result)
            elif dim == 'growth':
                result = self.filter_growth(result)
            elif dim == 'financial_health':
                result = self.filter_financial_health(result)
            
            if result.empty:
                break
        
        # 计算质量分数
        if not result.empty:
            result['quality_score'] = self.calculate_quality_score(result)
        
        return result
    
    def get_filter_summary(self) -> Dict:
        """获取筛选摘要"""
        return {
            'filter_results': self.filter_results,
            'config': self.config
        }
    
    def _setup_logger(self) -> logging.Logger:
        """设置日志"""
        logger = logging.getLogger('FundamentalFilter')
        logger.setLevel(logging.INFO)
        return logger