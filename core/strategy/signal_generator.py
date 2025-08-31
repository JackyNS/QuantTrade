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

class SignalGenerator:
    """
    高级信号生成器 - 整合多维度分析生成交易信号
    支持信号过滤、确认、优先级排序等高级功能
    """
    
    def __init__(self, config: Optional[Dict] = None):
        """
        初始化信号生成器
        
        Args:
            config: 配置参数
        """
        self.config = config or self._get_default_config()
        self.signal_history = []
        self.signal_stats = {
            'total_generated': 0,
            'buy_signals': 0,
            'sell_signals': 0,
            'filtered_out': 0
        }
        
        self.logger = self._setup_logger()
        self.logger.info("信号生成器初始化完成")
    
    def _get_default_config(self) -> Dict:
        """获取默认配置"""
        return {
            'weights': {
                'technical': 0.35,      # 技术指标权重
                'capital_flow': 0.25,   # 资金流权重
                'sentiment': 0.20,      # 市场情绪权重  
                'pattern': 0.20        # K线形态权重
            },
            'thresholds': {
                'strong_buy': 75,       # 强烈买入阈值
                'buy': 60,             # 买入阈值
                'sell': 40,            # 卖出阈值
                'strong_sell': 25,     # 强烈卖出阈值
                'neutral_zone': 10     # 中性区间
            },
            'filters': {
                'min_volume': 1e7,     # 最小成交额
                'min_confidence': 0.6,  # 最小置信度
                'require_confirmation': True,   # 需要确认信号
                'confirmation_periods': 2,      # 确认周期数
                'max_correlation': 0.7,         # 最大相关性
                'enable_divergence_check': True # 背离检查
            },
            'signal_decay': {
                'enabled': True,        # 是否启用信号衰减
                'half_life': 5,        # 半衰期(天)
                'min_strength': 0.3    # 最小强度
            }
        }
    
    def generate_signals(self,
                        technical_data: pd.DataFrame,
                        capital_data: Optional[pd.DataFrame] = None,
                        sentiment_data: Optional[pd.DataFrame] = None,
                        pattern_data: Optional[pd.DataFrame] = None,
                        market_data: Optional[pd.DataFrame] = None) -> pd.DataFrame:
        """
        生成综合交易信号
        
        Args:
            technical_data: 技术指标数据
            capital_data: 资金流数据
            sentiment_data: 情绪数据
            pattern_data: 形态数据
            market_data: 市场数据
            
        Returns:
            信号DataFrame
        """
        self.logger.info("开始生成交易信号...")
        
        # 初始化信号表
        signals = pd.DataFrame(index=technical_data.index)
        
        # 1. 计算各维度得分
        scores = self._calculate_dimensional_scores(
            technical_data, capital_data, sentiment_data, pattern_data
        )
        
        # 2. 计算综合得分
        signals['composite_score'] = self._calculate_composite_score(scores)
        
        # 3. 生成原始信号
        signals['raw_signal'] = self._generate_raw_signals(signals['composite_score'])
        
        # 4. 信号确认
        if self.config['filters']['require_confirmation']:
            signals['confirmed_signal'] = self._confirm_signals(
                signals['raw_signal'], 
                technical_data
            )
        else:
            signals['confirmed_signal'] = signals['raw_signal']
        
        # 5. 计算信号强度
        signals['signal_strength'] = self._calculate_signal_strength(
            signals['composite_score'],
            scores
        )
        
        # 6. 计算置信度
        signals['confidence'] = self._calculate_confidence(scores, technical_data)
        
        # 7. 检测背离
        if self.config['filters']['enable_divergence_check']:
            signals['divergence'] = self._detect_divergence(technical_data, signals)
        
        # 8. 信号优先级
        signals['priority'] = self._calculate_priority(signals)
        
        # 9. 最终信号
        signals['signal'] = self._finalize_signals(signals)
        
        # 10. 信号原因
        signals['reason'] = self._get_signal_reasons(signals, scores)
        
        # 11. 风险评分
        signals['risk_score'] = self._calculate_risk_score(technical_data, signals)
        
        # 更新统计
        self._update_statistics(signals)
        
        # 保存历史
        self.signal_history.append({
            'timestamp': datetime.now(),
            'signals': signals.copy()
        })
        
        self.logger.info(f"信号生成完成: 买入{self.signal_stats['buy_signals']}, "
                        f"卖出{self.signal_stats['sell_signals']}")
        
        return signals
    
    def _calculate_dimensional_scores(self,
                                     technical_data: pd.DataFrame,
                                     capital_data: Optional[pd.DataFrame],
                                     sentiment_data: Optional[pd.DataFrame],
                                     pattern_data: Optional[pd.DataFrame]) -> Dict[str, pd.Series]:
        """计算各维度得分"""
        scores = {}
        
        # 技术指标得分
        scores['technical'] = self._calculate_technical_score(technical_data)
        
        # 资金流得分
        if capital_data is not None and not capital_data.empty:
            scores['capital'] = self._calculate_capital_score(capital_data)
        else:
            scores['capital'] = pd.Series(50, index=technical_data.index)
        
        # 情绪得分
        if sentiment_data is not None and not sentiment_data.empty:
            scores['sentiment'] = self._calculate_sentiment_score(sentiment_data)
        else:
            scores['sentiment'] = pd.Series(50, index=technical_data.index)
        
        # 形态得分
        if pattern_data is not None and not pattern_data.empty:
            scores['pattern'] = self._calculate_pattern_score(pattern_data)
        else:
            scores['pattern'] = pd.Series(50, index=technical_data.index)
        
        return scores
    
    def _calculate_technical_score(self, data: pd.DataFrame) -> pd.Series:
        """计算技术指标得分"""
        score = pd.Series(50, index=data.index)
        
        # RSI
        if 'rsi' in data.columns:
            rsi = data['rsi']
            score[rsi < 30] += 15  # 超卖
            score[rsi < 20] += 10  # 极度超卖
            score[rsi > 70] -= 15  # 超买
            score[rsi > 80] -= 10  # 极度超买
        
        # MACD
        if 'macd' in data.columns and 'signal' in data.columns:
            macd_cross_up = (data['macd'] > data['signal']) & \
                           (data['macd'].shift(1) <= data['signal'].shift(1))
            macd_cross_down = (data['macd'] < data['signal']) & \
                             (data['macd'].shift(1) >= data['signal'].shift(1))
            
            score[macd_cross_up] += 20
            score[macd_cross_down] -= 20
            
            # MACD柱状图
            if 'histogram' in data.columns:
                hist_growing = data['histogram'] > data['histogram'].shift(1)
                score[hist_growing] += 5
                score[~hist_growing] -= 5
        
        # 布林带
        if all(col in data.columns for col in ['close', 'bb_upper', 'bb_lower']):
            near_lower = (data['close'] - data['bb_lower']) / \
                        (data['bb_upper'] - data['bb_lower']) < 0.2
            near_upper = (data['close'] - data['bb_lower']) / \
                        (data['bb_upper'] - data['bb_lower']) > 0.8
            
            score[near_lower] += 10
            score[near_upper] -= 10
        
        # 移动平均线
        if all(col in data.columns for col in ['close', 'sma_20', 'sma_60']):
            # 金叉死叉
            golden_cross = (data['sma_20'] > data['sma_60']) & \
                          (data['sma_20'].shift(1) <= data['sma_60'].shift(1))
            death_cross = (data['sma_20'] < data['sma_60']) & \
                         (data['sma_20'].shift(1) >= data['sma_60'].shift(1))
            
            score[golden_cross] += 15
            score[death_cross] -= 15
            
            # 价格相对位置
            above_ma = data['close'] > data['sma_20']
            score[above_ma] += 5
            score[~above_ma] -= 5
        
        # KDJ
        if all(col in data.columns for col in ['k', 'd', 'j']):
            kdj_golden = (data['k'] > data['d']) & (data['k'].shift(1) <= data['d'].shift(1))
            kdj_death = (data['k'] < data['d']) & (data['k'].shift(1) >= data['d'].shift(1))
            
            score[kdj_golden] += 10
            score[kdj_death] -= 10
        
        return score.clip(0, 100)
    
    def _calculate_capital_score(self, data: pd.DataFrame) -> pd.Series:
        """计算资金流得分"""
        score = pd.Series(50, index=data.index)
        
        if 'capital_flow_score' in data.columns:
            score = data['capital_flow_score']
        elif 'main_flow_ratio' in data.columns:
            # 主力资金流入比例
            score = 50 + data['main_flow_ratio'] * 100
        
        return score.clip(0, 100)
    
    def _calculate_sentiment_score(self, data: pd.DataFrame) -> pd.Series:
        """计算情绪得分"""
        score = pd.Series(50, index=data.index)
        
        if 'fear_greed_index' in data.columns:
            # 反向操作:极度恐慌时买入,极度贪婪时卖出
            fgi = data['fear_greed_index']
            score = 100 - fgi  # 反向
        elif 'sentiment_momentum' in data.columns:
            score = data['sentiment_momentum']
        
        return score.clip(0, 100)
    
    def _calculate_pattern_score(self, data: pd.DataFrame) -> pd.Series:
        """计算形态得分"""
        score = pd.Series(50, index=data.index)
        
        if 'pattern_strength' in data.columns:
            score = data['pattern_strength']
        elif 'pattern_signal' in data.columns:
            # 根据形态信号调整得分
            score[data['pattern_signal'] > 0] = 70
            score[data['pattern_signal'] < 0] = 30
            score[data['pattern_signal'] == 2] = 90
            score[data['pattern_signal'] == -2] = 10
        
        return score.clip(0, 100)
    
    def _calculate_composite_score(self, scores: Dict[str, pd.Series]) -> pd.Series:
        """计算综合得分"""
        composite = pd.Series(0, index=list(scores.values())[0].index)
        
        # 加权平均
        total_weight = sum(self.config['weights'].values())
        for dimension, score in scores.items():
            if dimension in self.config['weights']:
                weight = self.config['weights'][dimension] / total_weight
                composite += score * weight
        
        return composite
    
    def _generate_raw_signals(self, composite_score: pd.Series) -> pd.Series:
        """生成原始信号"""
        signals = pd.Series(0, index=composite_score.index)
        
        thresholds = self.config['thresholds']
        
        # 强烈买入
        signals[composite_score >= thresholds['strong_buy']] = 2
        
        # 买入
        signals[(composite_score >= thresholds['buy']) & 
               (composite_score < thresholds['strong_buy'])] = 1
        
        # 卖出
        signals[(composite_score <= thresholds['sell']) & 
               (composite_score > thresholds['strong_sell'])] = -1
        
        # 强烈卖出
        signals[composite_score <= thresholds['strong_sell']] = -2
        
        return signals
    
    def _confirm_signals(self, raw_signals: pd.Series, data: pd.DataFrame) -> pd.Series:
        """确认信号(避免假信号)"""
        confirmed = raw_signals.copy()
        periods = self.config['filters']['confirmation_periods']
        
        # 需要连续N个周期的同向信号才确认
        for i in range(periods, len(confirmed)):
            # 检查之前的信号
            prev_signals = raw_signals.iloc[i-periods:i]
            
            # 买入确认
            if raw_signals.iloc[i] > 0:
                if not all(prev_signals >= 0):
                    confirmed.iloc[i] = 0
            
            # 卖出确认
            elif raw_signals.iloc[i] < 0:
                if not all(prev_signals <= 0):
                    confirmed.iloc[i] = 0
        
        return confirmed
    
    def _calculate_signal_strength(self, 
                                  composite_score: pd.Series,
                                  scores: Dict[str, pd.Series]) -> pd.Series:
        """计算信号强度"""
        # 基础强度来自综合得分
        strength = abs(composite_score - 50) / 50
        
        # 考虑各维度一致性
        consistency = pd.Series(1, index=composite_score.index)
        for dimension in scores.values():
            dim_direction = np.sign(dimension - 50)
            score_direction = np.sign(composite_score - 50)
            consistency *= (dim_direction == score_direction).astype(float)
        
        # 信号衰减
        if self.config['signal_decay']['enabled']:
            strength = self._apply_signal_decay(strength)
        
        return (strength * consistency).clip(0, 1)
    
    def _apply_signal_decay(self, strength: pd.Series) -> pd.Series:
        """应用信号衰减"""
        half_life = self.config['signal_decay']['half_life']
        min_strength = self.config['signal_decay']['min_strength']
        
        # 简单的指数衰减模型
        decay_factor = 0.5 ** (1 / half_life)
        decayed = strength.copy()
        
        for i in range(1, len(decayed)):
            if decayed.iloc[i] == decayed.iloc[i-1]:
                # 信号持续,强度衰减
                decayed.iloc[i] = max(decayed.iloc[i] * decay_factor, min_strength)
        
        return decayed
    
    def _calculate_confidence(self, scores: Dict[str, pd.Series], 
                            data: pd.DataFrame) -> pd.Series:
        """计算置信度"""
        confidence = pd.Series(50, index=list(scores.values())[0].index)
        
        # 基于各维度得分的标准差
        scores_df = pd.DataFrame(scores)
        score_std = scores_df.std(axis=1)
        
        # 标准差越小,一致性越高,置信度越高
        confidence = 100 - score_std
        
        # 考虑成交量
        if 'volume' in data.columns:
            volume_ratio = data['volume'] / data['volume'].rolling(20).mean()
            confidence *= np.where(volume_ratio > 1.5, 1.2, 1.0)
        
        return confidence.clip(0, 100)
    
    def _detect_divergence(self, data: pd.DataFrame, signals: pd.DataFrame) -> pd.Series:
        """检测背离"""
        divergence = pd.Series(0, index=data.index)
        
        if 'close' in data.columns and 'rsi' in data.columns:
            # 价格创新高但RSI没有
            price_high = data['close'] == data['close'].rolling(20).max()
            rsi_not_high = data['rsi'] < data['rsi'].rolling(20).max()
            bearish_divergence = price_high & rsi_not_high
            
            # 价格创新低但RSI没有
            price_low = data['close'] == data['close'].rolling(20).min()
            rsi_not_low = data['rsi'] > data['rsi'].rolling(20).min()
            bullish_divergence = price_low & rsi_not_low
            
            divergence[bullish_divergence] = 1  # 看涨背离
            divergence[bearish_divergence] = -1  # 看跌背离
        
        return divergence
    
    def _calculate_priority(self, signals: pd.DataFrame) -> pd.Series:
        """计算信号优先级"""
        priority = pd.Series(0, index=signals.index)
        
        # 基于信号强度和置信度
        if 'signal_strength' in signals.columns and 'confidence' in signals.columns:
            priority = signals['signal_strength'] * signals['confidence'] / 100
        
        # 考虑背离
        if 'divergence' in signals.columns:
            # 背离信号优先级更高
            priority[signals['divergence'] != 0] *= 1.5
        
        return priority
    
    def _finalize_signals(self, signals: pd.DataFrame) -> pd.Series:
        """生成最终信号"""
        final = signals['confirmed_signal'].copy()
        
        # 过滤低置信度信号
        min_confidence = self.config['filters']['min_confidence'] * 100
        if 'confidence' in signals.columns:
            low_confidence = signals['confidence'] < min_confidence
            final[low_confidence] = 0
            self.signal_stats['filtered_out'] += low_confidence.sum()
        
        # 考虑背离调整
        if 'divergence' in signals.columns:
            # 如果有背离,可能调整或取消信号
            bearish_divergence = signals['divergence'] == -1
            bullish_divergence = signals['divergence'] == 1
            
            # 背离时减弱反向信号
            final[bearish_divergence & (final > 0)] = 0
            final[bullish_divergence & (final < 0)] = 0
        
        return final
    
    def _get_signal_reasons(self, signals: pd.DataFrame, 
                           scores: Dict[str, pd.Series]) -> pd.Series:
        """获取信号原因"""
        reasons = pd.Series("", index=signals.index)
        
        for i in signals.index:
            if signals.loc[i, 'signal'] != 0:
                reason_parts = []
                
                # 找出贡献最大的维度
                for dim, score in scores.items():
                    if abs(score.loc[i] - 50) > 20:
                        if score.loc[i] > 70:
                            reason_parts.append(f"{dim}:bullish")
                        elif score.loc[i] < 30:
                            reason_parts.append(f"{dim}:bearish")
                
                # 特殊情况
                if 'divergence' in signals.columns and signals.loc[i, 'divergence'] != 0:
                    reason_parts.append("divergence")
                
                reasons.loc[i] = "|".join(reason_parts) if reason_parts else "composite"
        
        return reasons
    
    def _calculate_risk_score(self, data: pd.DataFrame, signals: pd.DataFrame) -> pd.Series:
        """计算风险评分"""
        risk = pd.Series(50, index=signals.index)
        
        # 波动率风险
        if 'volatility' in data.columns:
            vol_percentile = data['volatility'].rank(pct=True)
            risk += vol_percentile * 20
        
        # ATR风险
        if 'atr' in data.columns:
            atr_percentile = data['atr'].rank(pct=True)
            risk += atr_percentile * 20
        
        # 信号强度反向(强信号低风险)
        if 'signal_strength' in signals.columns:
            risk -= signals['signal_strength'] * 20
        
        return risk.clip(0, 100)
    
    def _update_statistics(self, signals: pd.DataFrame):
        """更新统计信息"""
        self.signal_stats['total_generated'] = len(signals)
        self.signal_stats['buy_signals'] = (signals['signal'] > 0).sum()
        self.signal_stats['sell_signals'] = (signals['signal'] < 0).sum()
    
    def _setup_logger(self) -> logging.Logger:
        """设置日志"""
        logger = logging.getLogger("SignalGenerator")
        logger.setLevel(logging.INFO)
        return logger