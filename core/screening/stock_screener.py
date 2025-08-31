#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
股票筛选模块 - Stock Screener
============================

专业的多维度股票筛选系统，支持基本面、技术面、资金面等多维度筛选

作者: QuantTrader Team
版本: 1.0.0
更新: 2025-08-30
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any, Union
from datetime import datetime, timedelta
from abc import ABC, abstractmethod
import logging

class StockScreener:
    """
    股票筛选器 - 多维度股票筛选
    
    支持的筛选维度：
    1. 基本面筛选（市值、市盈率、ROE等）
    2. 技术面筛选（价格、成交量、技术指标等）
    3. 资金面筛选（资金流向、融资融券等）
    4. 市场面筛选（行业、概念、地区等）
    5. 风险面筛选（ST、退市、停牌等）
    """
    
    def __init__(self, config: Optional[Dict] = None):
        """初始化筛选器"""
        self.config = config or self._get_default_config()
        self.logger = self._setup_logger()
        
        # 筛选结果缓存
        self.screening_results = {}
        self.filter_stats = {}
        
    def _get_default_config(self) -> Dict:
        """默认筛选配置"""
        return {
            # 基本面筛选
            'fundamental': {
                'market_cap': {'min': 10e8, 'max': None},  # 市值10亿以上
                'pe_ratio': {'min': 0, 'max': 50},         # PE 0-50倍
                'pb_ratio': {'min': 0, 'max': 10},         # PB 0-10倍
                'roe': {'min': 0.05, 'max': None},         # ROE > 5%
                'revenue_growth': {'min': 0, 'max': None}, # 营收增长 > 0
                'profit_growth': {'min': 0, 'max': None},  # 利润增长 > 0
                'debt_ratio': {'min': None, 'max': 0.7},   # 负债率 < 70%
                'current_ratio': {'min': 1.0, 'max': None}, # 流动比率 > 1
            },
            
            # 技术面筛选
            'technical': {
                'price': {'min': 5, 'max': 500},           # 股价5-500元
                'volume': {'min': 1e7, 'max': None},       # 成交额>1000万
                'turnover_rate': {'min': 0.01, 'max': 0.3},# 换手率1%-30%
                'volatility': {'min': None, 'max': 0.05},  # 波动率<5%
                'ma_trend': 'bullish',                     # 均线趋势
                'rsi': {'min': 30, 'max': 70},            # RSI 30-70
                'volume_ratio': {'min': 0.8, 'max': 3},    # 量比0.8-3
            },
            
            # 资金面筛选
            'capital_flow': {
                'main_net_flow': {'min': 0, 'max': None},  # 主力净流入>0
                'main_flow_days': {'min': 3, 'max': None}, # 连续流入天数
                'smart_money': True,                       # 聪明钱信号
                'north_flow': {'min': 0, 'max': None},     # 北向资金流入
            },
            
            # 市场面筛选
            'market': {
                'industries': [],          # 行业白名单
                'exclude_industries': [],  # 行业黑名单
                'concepts': [],           # 概念板块
                'regions': [],            # 地区
                'index_constituent': [],  # 指数成分股
            },
            
            # 风险筛选
            'risk': {
                'exclude_st': True,       # 排除ST
                'exclude_star_st': True,  # 排除*ST
                'exclude_suspended': True, # 排除停牌
                'exclude_new': True,      # 排除次新股
                'new_days': 60,          # 次新股天数
                'min_trading_days': 60,  # 最少交易天数
            },
            
            # 筛选方法
            'method': {
                'mode': 'AND',           # AND/OR 筛选逻辑
                'ranking': False,        # 是否排序
                'top_k': 50,            # 选择前K只
                'weight_mode': 'equal',  # 权重模式
            }
        }
    
    def screen_fundamental(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        基本面筛选
        
        Args:
            data: 包含基本面数据的DataFrame
            
        Returns:
            筛选后的数据
        """
        self.logger.info("📊 开始基本面筛选...")
        filtered = data.copy()
        initial_count = len(filtered)
        
        criteria = self.config['fundamental']
        
        # 市值筛选
        if 'market_cap' in filtered.columns:
            if criteria['market_cap']['min']:
                filtered = filtered[filtered['market_cap'] >= criteria['market_cap']['min']]
            if criteria['market_cap']['max']:
                filtered = filtered[filtered['market_cap'] <= criteria['market_cap']['max']]
        
        # PE筛选
        if 'pe_ratio' in filtered.columns:
            if criteria['pe_ratio']['min'] is not None:
                filtered = filtered[filtered['pe_ratio'] >= criteria['pe_ratio']['min']]
            if criteria['pe_ratio']['max'] is not None:
                filtered = filtered[filtered['pe_ratio'] <= criteria['pe_ratio']['max']]
        
        # PB筛选
        if 'pb_ratio' in filtered.columns:
            if criteria['pb_ratio']['min'] is not None:
                filtered = filtered[filtered['pb_ratio'] >= criteria['pb_ratio']['min']]
            if criteria['pb_ratio']['max'] is not None:
                filtered = filtered[filtered['pb_ratio'] <= criteria['pb_ratio']['max']]
        
        # ROE筛选
        if 'roe' in filtered.columns:
            if criteria['roe']['min']:
                filtered = filtered[filtered['roe'] >= criteria['roe']['min']]
        
        # 营收增长
        if 'revenue_growth' in filtered.columns:
            if criteria['revenue_growth']['min'] is not None:
                filtered = filtered[filtered['revenue_growth'] >= criteria['revenue_growth']['min']]
        
        final_count = len(filtered)
        self.logger.info(f"   基本面筛选: {initial_count} → {final_count} ({final_count/initial_count:.1%})")
        
        return filtered
    
    def screen_technical(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        技术面筛选
        
        Args:
            data: 包含技术指标的DataFrame
            
        Returns:
            筛选后的数据
        """
        self.logger.info("📈 开始技术面筛选...")
        filtered = data.copy()
        initial_count = len(filtered)
        
        criteria = self.config['technical']
        
        # 价格筛选
        if 'close' in filtered.columns:
            if criteria['price']['min']:
                filtered = filtered[filtered['close'] >= criteria['price']['min']]
            if criteria['price']['max']:
                filtered = filtered[filtered['close'] <= criteria['price']['max']]
        
        # 成交量筛选
        if 'volume' in filtered.columns or 'turnover' in filtered.columns:
            vol_col = 'turnover' if 'turnover' in filtered.columns else 'volume'
            if criteria['volume']['min']:
                filtered = filtered[filtered[vol_col] >= criteria['volume']['min']]
        
        # 换手率筛选
        if 'turnover_rate' in filtered.columns:
            if criteria['turnover_rate']['min']:
                filtered = filtered[filtered['turnover_rate'] >= criteria['turnover_rate']['min']]
            if criteria['turnover_rate']['max']:
                filtered = filtered[filtered['turnover_rate'] <= criteria['turnover_rate']['max']]
        
        # RSI筛选
        if 'rsi' in filtered.columns:
            if criteria['rsi']['min'] and criteria['rsi']['max']:
                filtered = filtered[
                    (filtered['rsi'] >= criteria['rsi']['min']) &
                    (filtered['rsi'] <= criteria['rsi']['max'])
                ]
        
        # 均线趋势筛选
        if criteria['ma_trend'] == 'bullish':
            if all(col in filtered.columns for col in ['ma5', 'ma20', 'ma60']):
                filtered = filtered[
                    (filtered['ma5'] > filtered['ma20']) &
                    (filtered['ma20'] > filtered['ma60'])
                ]
        
        final_count = len(filtered)
        self.logger.info(f"   技术面筛选: {initial_count} → {final_count} ({final_count/initial_count:.1%})")
        
        return filtered
    
    def screen_capital_flow(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        资金面筛选
        
        Args:
            data: 包含资金流数据的DataFrame
            
        Returns:
            筛选后的数据
        """
        self.logger.info("💰 开始资金面筛选...")
        filtered = data.copy()
        initial_count = len(filtered)
        
        criteria = self.config['capital_flow']
        
        # 主力净流入筛选
        if 'main_net_flow' in filtered.columns:
            if criteria['main_net_flow']['min'] is not None:
                filtered = filtered[filtered['main_net_flow'] >= criteria['main_net_flow']['min']]
        
        # 连续流入天数
        if 'main_flow_days' in filtered.columns:
            if criteria['main_flow_days']['min']:
                filtered = filtered[filtered['main_flow_days'] >= criteria['main_flow_days']['min']]
        
        # 北向资金
        if 'north_flow' in filtered.columns:
            if criteria['north_flow']['min'] is not None:
                filtered = filtered[filtered['north_flow'] >= criteria['north_flow']['min']]
        
        final_count = len(filtered)
        self.logger.info(f"   资金面筛选: {initial_count} → {final_count} ({final_count/initial_count:.1%})")
        
        return filtered
    
    def screen_risk(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        风险筛选（排除高风险股票）
        
        Args:
            data: 股票数据
            
        Returns:
            筛选后的数据
        """
        self.logger.info("⚠️ 开始风险筛选...")
        filtered = data.copy()
        initial_count = len(filtered)
        
        criteria = self.config['risk']
        
        # 排除ST股票
        if criteria['exclude_st'] and 'name' in filtered.columns:
            filtered = filtered[~filtered['name'].str.contains('ST', na=False)]
        
        # 排除*ST股票
        if criteria['exclude_star_st'] and 'name' in filtered.columns:
            filtered = filtered[~filtered['name'].str.contains('\\*ST', na=False)]
        
        # 排除停牌股票
        if criteria['exclude_suspended'] and 'is_suspended' in filtered.columns:
            filtered = filtered[filtered['is_suspended'] == False]
        
        # 排除次新股
        if criteria['exclude_new'] and 'list_date' in filtered.columns:
            list_date = pd.to_datetime(filtered['list_date'])
            days_since_list = (datetime.now() - list_date).dt.days
            filtered = filtered[days_since_list > criteria['new_days']]
        
        final_count = len(filtered)
        self.logger.info(f"   风险筛选: {initial_count} → {final_count} ({final_count/initial_count:.1%})")
        
        return filtered
    
    def apply_scoring(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        综合评分排序
        
        Args:
            data: 筛选后的数据
            
        Returns:
            包含评分的数据
        """
        self.logger.info("🎯 计算综合评分...")
        
        # 初始化评分
        data['score'] = 0
        
        # 基本面评分（40%）
        if 'roe' in data.columns:
            data['score'] += self._normalize_score(data['roe'], higher_better=True) * 0.15
        if 'revenue_growth' in data.columns:
            data['score'] += self._normalize_score(data['revenue_growth'], higher_better=True) * 0.15
        if 'pe_ratio' in data.columns:
            data['score'] += self._normalize_score(data['pe_ratio'], higher_better=False) * 0.10
        
        # 技术面评分（30%）
        if 'rsi' in data.columns:
            # RSI接近50最好
            data['score'] += self._normalize_score(abs(data['rsi'] - 50), higher_better=False) * 0.10
        if 'volume_ratio' in data.columns:
            data['score'] += self._normalize_score(data['volume_ratio'], higher_better=True) * 0.10
        if 'ma_trend_strength' in data.columns:
            data['score'] += self._normalize_score(data['ma_trend_strength'], higher_better=True) * 0.10
        
        # 资金面评分（30%）
        if 'main_net_flow' in data.columns:
            data['score'] += self._normalize_score(data['main_net_flow'], higher_better=True) * 0.15
        if 'smart_money_signal' in data.columns:
            data['score'] += data['smart_money_signal'] * 0.15
        
        # 按评分排序
        data = data.sort_values('score', ascending=False)
        
        return data
    
    def _normalize_score(self, series: pd.Series, higher_better: bool = True) -> pd.Series:
        """标准化评分到0-1"""
        if series.empty or series.std() == 0:
            return pd.Series(0.5, index=series.index)
        
        normalized = (series - series.min()) / (series.max() - series.min())
        
        if not higher_better:
            normalized = 1 - normalized
        
        return normalized
    
    def screen(self, 
              data: pd.DataFrame,
              dimensions: List[str] = None,
              return_scores: bool = False) -> Union[pd.DataFrame, List[str]]:
        """
        执行多维度筛选
        
        Args:
            data: 原始数据
            dimensions: 筛选维度列表，默认全部
            return_scores: 是否返回评分
            
        Returns:
            筛选结果（DataFrame或股票代码列表）
        """
        self.logger.info("🚀 开始多维度股票筛选")
        self.logger.info("=" * 50)
        
        # 默认筛选所有维度
        if dimensions is None:
            dimensions = ['risk', 'fundamental', 'technical', 'capital_flow']
        
        result = data.copy()
        stats = {'initial': len(result)}
        
        # 按维度筛选
        for dim in dimensions:
            if dim == 'fundamental':
                result = self.screen_fundamental(result)
            elif dim == 'technical':
                result = self.screen_technical(result)
            elif dim == 'capital_flow':
                result = self.screen_capital_flow(result)
            elif dim == 'risk':
                result = self.screen_risk(result)
            
            stats[dim] = len(result)
            
            if result.empty:
                self.logger.warning(f"⚠️ {dim}筛选后无符合条件的股票")
                break
        
        # 综合评分和排序
        if not result.empty and self.config['method']['ranking']:
            result = self.apply_scoring(result)
            
            # 选择Top K
            if self.config['method']['top_k']:
                result = result.head(self.config['method']['top_k'])
        
        # 保存筛选统计
        self.filter_stats = stats
        self.screening_results = result
        
        # 输出筛选报告
        self._generate_screening_report(stats)
        
        if return_scores:
            return result
        else:
            return result['ticker'].unique().tolist() if 'ticker' in result.columns else []
    
    def _generate_screening_report(self, stats: Dict):
        """生成筛选报告"""
        self.logger.info("\n📊 筛选报告")
        self.logger.info("=" * 50)
        
        for stage, count in stats.items():
            if stage == 'initial':
                self.logger.info(f"初始股票数: {count}")
            else:
                prev_count = list(stats.values())[list(stats.keys()).index(stage) - 1]
                self.logger.info(f"{stage:15} {prev_count:4} → {count:4} ({count/prev_count:.1%})")
        
        initial = stats['initial']
        final = list(stats.values())[-1]
        self.logger.info(f"\n最终选中: {final}/{initial} ({final/initial:.1%})")
    
    def get_screening_summary(self) -> Dict:
        """获取筛选摘要"""
        return {
            'total_screened': len(self.screening_results),
            'filter_stats': self.filter_stats,
            'top_stocks': self.screening_results.head(10)['ticker'].tolist() if not self.screening_results.empty else [],
            'config': self.config
        }
    
    def _setup_logger(self) -> logging.Logger:
        """设置日志"""
        logger = logging.getLogger('StockScreener')
        logger.setLevel(logging.INFO)
        return logger


class CustomScreener(StockScreener):
    """
    自定义筛选器示例
    可以继承StockScreener创建自己的筛选策略
    """
    
    def __init__(self):
        super().__init__()
        
        # 自定义配置
        self.config['fundamental']['roe']['min'] = 0.15  # ROE > 15%
        self.config['technical']['ma_trend'] = 'bullish'
        self.config['method']['top_k'] = 30
    
    def screen_custom_factor(self, data: pd.DataFrame) -> pd.DataFrame:
        """自定义因子筛选"""
        # 实现自己的筛选逻辑
        return data