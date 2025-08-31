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
