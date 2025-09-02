#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
策略模块初始化
"""

# 尝试导入所有组件
components = []

try:
    from .base_strategy import BaseStrategy
    components.append('BaseStrategy')
except ImportError:
    pass

try:
    from .technical_indicators import TechnicalIndicators
    components.append('TechnicalIndicators')
except ImportError:
    pass

try:
    from .capital_flow_analysis import CapitalFlowAnalyzer
    components.append('CapitalFlowAnalyzer')
except ImportError:
    pass

try:
    from .market_sentiment import MarketSentimentAnalyzer
    components.append('MarketSentimentAnalyzer')
except ImportError:
    pass

try:
    from .pattern_recognition import PatternRecognizer
    components.append('PatternRecognizer')
except ImportError:
    pass

try:
    from .signal_generator import SignalGenerator
    components.append('SignalGenerator')
except ImportError:
    pass

try:
    from .position_manager import PositionManager
    components.append('PositionManager')
except ImportError:
    pass

try:
    from .strategy_optimizer import StrategyOptimizer
    components.append('StrategyOptimizer')
except ImportError:
    pass

__all__ = components

print(f"✅ 策略模块加载: {len(components)}/8 组件")


class TechnicalStrategy(BaseStrategy):
    """技术分析策略基类"""
    
    def __init__(self, name="technical_strategy", **kwargs):
        super().__init__(name, **kwargs)
        self.indicators = {}
    
    def calculate_indicators(self, data):
        """计算技术指标"""
        self.indicators = {}
        
        # 基础移动平均线
        if 'closePrice' in data.columns:
            self.indicators['ma5'] = data['closePrice'].rolling(5).mean()
            self.indicators['ma20'] = data['closePrice'].rolling(20).mean()
            self.indicators['ma60'] = data['closePrice'].rolling(60).mean()
        
        return self.indicators
    
    def generate_signals(self, data):
        """生成交易信号"""
        self.calculate_indicators(data)
        
        # 简单均线交叉策略
        if 'ma5' in self.indicators and 'ma20' in self.indicators:
            signals = (self.indicators['ma5'] > self.indicators['ma20']).astype(int)
            return signals
        
        return pd.Series([0] * len(data))

class MLStrategy(BaseStrategy):
    """机器学习策略基类"""
    
    def __init__(self, name="ml_strategy", **kwargs):
        super().__init__(name, **kwargs)
        self.model = None
        self.features = []
    
    def calculate_indicators(self, data):
        """计算指标特征"""
        self.indicators = {}
        
        if 'closePrice' in data.columns:
            # 价格相关特征
            self.indicators['returns'] = data['closePrice'].pct_change()
            self.indicators['volatility'] = self.indicators['returns'].rolling(20).std()
            self.indicators['rsi'] = self.calculate_rsi(data['closePrice'])
        
        return self.indicators
    
    def calculate_rsi(self, prices, window=14):
        """计算RSI指标"""
        delta = prices.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=window).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=window).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        return rsi
    
    def generate_signals(self, data):
        """生成ML信号"""
        self.calculate_indicators(data)
        
        # 简化的ML信号逻辑
        if 'rsi' in self.indicators:
            rsi = self.indicators['rsi']
            signals = pd.Series([0] * len(data))
            signals[rsi < 30] = 1  # 超卖买入
            signals[rsi > 70] = -1  # 超买卖出
            return signals
        
        return pd.Series([0] * len(data))

