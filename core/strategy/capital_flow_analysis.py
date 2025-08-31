#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
from typing import Optional, Dict, Any, Union, Tuple, List, Callable
from datetime import datetime, timedelta
from abc import ABC, abstractmethod
import logging
import warnings

warnings.filterwarnings('ignore')

class CapitalFlowAnalyzer:
    """
    资金流向分析器 - 分析主力资金动向
    """
    
    def __init__(self):
        """初始化资金流向分析器"""
        self.flow_thresholds = {
            'super_large': 1e8,  # 超大单 1亿
            'large': 5e7,        # 大单 5千万
            'medium': 1e7,       # 中单 1千万
            'small': 1e6         # 小单 100万
        }
    
    def analyze_money_flow(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        分析资金流向
        
        Args:
            data: 包含资金流数据的DataFrame
            
        Returns:
            包含资金流分析结果的DataFrame
        """
        result = data.copy()
        
        # 主力净流入
        if 'main_net_flow' in data.columns:
            result['main_flow_ratio'] = data['main_net_flow'] / data['turnover']
            result['main_flow_ma5'] = result['main_net_flow'].rolling(5).mean()
            result['main_flow_ma10'] = result['main_net_flow'].rolling(10).mean()
            
            # 主力流入强度
            result['main_flow_strength'] = self._calculate_flow_strength(data['main_net_flow'])
            
            # 连续流入/流出天数
            result['consecutive_inflow_days'] = self._calculate_consecutive_days(data['main_net_flow'], positive=True)
            result['consecutive_outflow_days'] = self._calculate_consecutive_days(data['main_net_flow'], positive=False)
        
        # 各类资金流向
        flow_types = ['super', 'large', 'medium', 'small']
        for flow_type in flow_types:
            inflow_col = f'{flow_type}_inflow'
            outflow_col = f'{flow_type}_outflow'
            
            if inflow_col in data.columns and outflow_col in data.columns:
                net_flow = data[inflow_col] - data[outflow_col]
                result[f'{flow_type}_net_flow'] = net_flow
                result[f'{flow_type}_flow_ratio'] = net_flow / data['turnover']
        
        # 资金流向评分
        result['capital_flow_score'] = self._calculate_flow_score(result)
        
        # 资金流向信号
        result['capital_flow_signal'] = self._generate_flow_signals(result)
        
        return result
    
    def _calculate_flow_strength(self, net_flow: pd.Series) -> pd.Series:
        """计算资金流强度"""
        # 标准化资金流
        if net_flow.std() > 0:
            normalized = (net_flow - net_flow.mean()) / net_flow.std()
        else:
            normalized = pd.Series(0, index=net_flow.index)
        
        # 转换为0-100的强度值
        strength = 50 + normalized * 20
        strength = strength.clip(0, 100)
        
        return strength
    
    def _calculate_consecutive_days(self, net_flow: pd.Series, positive: bool = True) -> pd.Series:
        """计算连续流入/流出天数"""
        if positive:
            condition = net_flow > 0
        else:
            condition = net_flow < 0
        
        # 计算连续天数
        consecutive = condition.astype(int)
        consecutive = consecutive.groupby((consecutive != consecutive.shift()).cumsum()).cumsum()
        consecutive[~condition] = 0
        
        return consecutive
    
    def _calculate_flow_score(self, data: pd.DataFrame) -> pd.Series:
        """计算资金流向综合评分"""
        score = pd.Series(50, index=data.index)  # 基础分50
        
        # 主力净流入加分
        if 'main_flow_ratio' in data.columns:
            score += data['main_flow_ratio'] * 100
        
        # 连续流入加分
        if 'consecutive_inflow_days' in data.columns:
            score += data['consecutive_inflow_days'] * 2
        
        # 资金流强度加分
        if 'main_flow_strength' in data.columns:
            score += (data['main_flow_strength'] - 50) * 0.5
        
        # 限制在0-100范围
        score = score.clip(0, 100)
        
        return score
    
    def _generate_flow_signals(self, data: pd.DataFrame) -> pd.Series:
        """生成资金流向信号"""
        signals = pd.Series(0, index=data.index)
        
        if 'capital_flow_score' not in data.columns:
            return signals
        
        # 强烈买入信号
        strong_buy = (
            (data['capital_flow_score'] > 70) &
            (data.get('consecutive_inflow_days', 0) >= 3) &
            (data.get('main_flow_strength', 50) > 60)
        )
        signals[strong_buy] = 2
        
        # 买入信号
        buy = (
            (data['capital_flow_score'] > 60) &
            (data.get('main_flow_ratio', 0) > 0.05)
        )
        signals[buy & ~strong_buy] = 1
        
        # 卖出信号
        sell = (
            (data['capital_flow_score'] < 40) &
            (data.get('consecutive_outflow_days', 0) >= 3)
        )
        signals[sell] = -1
        
        # 强烈卖出信号
        strong_sell = (
            (data['capital_flow_score'] < 30) &
            (data.get('consecutive_outflow_days', 0) >= 5) &
            (data.get('main_flow_strength', 50) < 40)
        )
        signals[strong_sell] = -2
        
        return signals
    
    def detect_abnormal_flow(self, data: pd.DataFrame, threshold: float = 3) -> pd.DataFrame:
        """
        检测异常资金流动
        
        Args:
            data: 资金流数据
            threshold: Z-score阈值
            
        Returns:
            异常流动标记
        """
        result = pd.DataFrame(index=data.index)
        
        if 'main_net_flow' in data.columns:
            # 计算Z-score
            z_score = (data['main_net_flow'] - data['main_net_flow'].mean()) / data['main_net_flow'].std()
            
            # 标记异常
            result['abnormal_inflow'] = z_score > threshold
            result['abnormal_outflow'] = z_score < -threshold
            result['abnormal_flow'] = result['abnormal_inflow'] | result['abnormal_outflow']
        
        return result