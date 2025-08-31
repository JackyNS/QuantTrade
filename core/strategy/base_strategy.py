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

class BaseStrategy(ABC):
    """
    策略基类 - 所有策略的父类
    提供统一的接口和基础功能
    """
    
    def __init__(self, name: str = "BaseStrategy", config: Optional[Dict] = None):
        """
        初始化策略
        
        Args:
            name: 策略名称
            config: 策略配置
        """
        self.name = name
        self.config = config or self._get_default_config()
        self.params = self._get_default_params()
        
        # 策略状态
        self.signals = pd.DataFrame()
        self.positions = pd.DataFrame()
        self.performance = {}
        self.indicators = {}
        
        # 日志设置
        self.logger = self._setup_logger()
        
        # 策略元数据
        self.metadata = {
            'created_at': datetime.now(),
            'version': '1.0.0',
            'status': 'initialized'
        }
        
        self.logger.info(f"策略 {name} 初始化完成")
        
    def _get_default_config(self) -> Dict:
        """获取默认配置"""
        return {
            'mode': 'backtest',  # backtest/live/paper
            'universe': 'A股',    # 交易市场
            'frequency': 'daily', # 数据频率
            'commission': 0.0003, # 手续费率
            'slippage': 0.001,   # 滑点
            'initial_capital': 1000000,  # 初始资金
            'benchmark': '000300.SH'     # 基准指数
        }
    
    def _get_default_params(self) -> Dict:
        """获取默认参数"""
        return {
            # 风险控制参数
            'stop_loss': 0.05,          # 止损比例 5%
            'take_profit': 0.15,        # 止盈比例 15%
            'max_position': 0.95,       # 最大仓位 95%
            'single_position': 0.05,    # 单股最大仓位 5%
            'max_stocks': 20,           # 最大持股数
            
            # 信号参数
            'signal_threshold': 0.6,    # 信号阈值
            'min_holding_days': 1,      # 最小持仓天数
            'max_holding_days': 20,     # 最大持仓天数
            
            # 过滤条件
            'min_volume': 1e7,          # 最小成交额
            'min_price': 2.0,           # 最低价格
            'max_price': 500.0,         # 最高价格
            'exclude_st': True,         # 排除ST股票
            'exclude_new': True,        # 排除次新股
            'new_days_threshold': 60    # 次新股天数阈值
        }
    
    @abstractmethod
    def calculate_indicators(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        计算技术指标(子类必须实现)
        
        Args:
            data: 原始数据
            
        Returns:
            包含指标的数据
        """
        pass
    
    @abstractmethod
    def generate_signals(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        生成交易信号(子类必须实现)
        
        Args:
            data: 包含指标的数据
            
        Returns:
            包含信号的数据
        """
        pass
    
    def filter_stocks(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        股票筛选
        
        Args:
            data: 原始数据
            
        Returns:
            筛选后的数据
        """
        filtered = data.copy()
        initial_count = len(filtered['ticker'].unique()) if 'ticker' in filtered.columns else len(filtered)
        
        # 成交额过滤
        if 'turnover' in filtered.columns:
            filtered = filtered[filtered['turnover'] >= self.params['min_volume']]
        
        # 价格过滤
        if 'close' in filtered.columns:
            filtered = filtered[
                (filtered['close'] >= self.params['min_price']) &
                (filtered['close'] <= self.params['max_price'])
            ]
        
        # ST股票过滤
        if self.params['exclude_st'] and 'name' in filtered.columns:
            filtered = filtered[~filtered['name'].str.contains('ST', na=False)]
        
        # 次新股过滤
        if self.params['exclude_new'] and 'list_days' in filtered.columns:
            filtered = filtered[filtered['list_days'] > self.params['new_days_threshold']]
        
        final_count = len(filtered['ticker'].unique()) if 'ticker' in filtered.columns else len(filtered)
        self.logger.info(f"股票筛选: {initial_count} -> {final_count}")
        
        return filtered
    
    def risk_control(self, signals: pd.DataFrame, positions: pd.DataFrame) -> pd.DataFrame:
        """
        风险控制
        
        Args:
            signals: 原始信号
            positions: 当前持仓
            
        Returns:
            调整后的信号
        """
        adjusted_signals = signals.copy()
        
        if positions.empty:
            return adjusted_signals
        
        # 计算当前仓位
        current_position = positions['value'].sum() if 'value' in positions.columns else 0
        
        for ticker in positions['ticker'].unique():
            pos = positions[positions['ticker'] == ticker].iloc[0]
            
            # 计算收益率
            if 'current_price' in pos and 'entry_price' in pos:
                returns = (pos['current_price'] - pos['entry_price']) / pos['entry_price']
                
                # 止损
                if returns <= -self.params['stop_loss']:
                    adjusted_signals.loc[adjusted_signals['ticker'] == ticker, 'signal'] = -1
                    adjusted_signals.loc[adjusted_signals['ticker'] == ticker, 'reason'] = 'stop_loss'
                    self.logger.info(f"止损触发: {ticker}, 亏损 {returns:.2%}")
                
                # 止盈
                elif returns >= self.params['take_profit']:
                    adjusted_signals.loc[adjusted_signals['ticker'] == ticker, 'signal'] = -1
                    adjusted_signals.loc[adjusted_signals['ticker'] == ticker, 'reason'] = 'take_profit'
                    self.logger.info(f"止盈触发: {ticker}, 盈利 {returns:.2%}")
            
            # 最大持仓时间
            if 'entry_date' in pos:
                holding_days = (datetime.now() - pos['entry_date']).days
                if holding_days >= self.params['max_holding_days']:
                    adjusted_signals.loc[adjusted_signals['ticker'] == ticker, 'signal'] = -1
                    adjusted_signals.loc[adjusted_signals['ticker'] == ticker, 'reason'] = 'max_holding'
                    self.logger.info(f"超时平仓: {ticker}, 持仓 {holding_days} 天")
        
        # 仓位控制
        if current_position >= self.params['max_position']:
            # 禁止新开仓
            adjusted_signals.loc[adjusted_signals['signal'] == 1, 'signal'] = 0
            adjusted_signals.loc[adjusted_signals['signal'] == 0, 'reason'] = 'position_limit'
            self.logger.warning(f"仓位限制: 当前仓位 {current_position:.2%}")
        
        # 持股数限制
        if len(positions) >= self.params['max_stocks']:
            adjusted_signals.loc[adjusted_signals['signal'] == 1, 'signal'] = 0
            adjusted_signals.loc[adjusted_signals['signal'] == 0, 'reason'] = 'stock_limit'
            self.logger.warning(f"持股数限制: 当前持有 {len(positions)} 只")
        
        return adjusted_signals
    
    def run(self, data: pd.DataFrame) -> Dict:
        """
        运行策略
        
        Args:
            data: 输入数据
            
        Returns:
            策略结果
        """
        self.logger.info(f"开始运行策略: {self.name}")
        start_time = datetime.now()
        
        try:
            # 1. 数据过滤
            self.logger.info("步骤1: 数据过滤")
            filtered_data = self.filter_stocks(data)
            
            # 2. 计算指标
            self.logger.info("步骤2: 计算技术指标")
            data_with_indicators = self.calculate_indicators(filtered_data)
            self.indicators = data_with_indicators
            
            # 3. 生成信号
            self.logger.info("步骤3: 生成交易信号")
            raw_signals = self.generate_signals(data_with_indicators)
            
            # 4. 风险控制
            self.logger.info("步骤4: 风险控制")
            final_signals = self.risk_control(raw_signals, self.positions)
            self.signals = final_signals
            
            # 5. 计算性能
            self.logger.info("步骤5: 计算策略性能")
            self.performance = self._calculate_performance(final_signals)
            
            # 更新状态
            self.metadata['status'] = 'completed'
            self.metadata['last_run'] = datetime.now()
            self.metadata['run_time'] = (datetime.now() - start_time).total_seconds()
            
            self.logger.info(f"策略运行完成,耗时 {self.metadata['run_time']:.2f} 秒")
            
            return {
                'signals': self.signals,
                'indicators': self.indicators,
                'performance': self.performance,
                'metadata': self.metadata
            }
            
        except Exception as e:
            self.logger.error(f"策略运行失败: {str(e)}")
            self.metadata['status'] = 'failed'
            self.metadata['error'] = str(e)
            raise
    
    def _calculate_performance(self, signals: pd.DataFrame) -> Dict:
        """计算策略性能指标"""
        perf = {
            'total_signals': len(signals),
            'buy_signals': len(signals[signals['signal'] == 1]),
            'sell_signals': len(signals[signals['signal'] == -1]),
            'hold_signals': len(signals[signals['signal'] == 0]),
            'signal_ratio': len(signals[signals['signal'] != 0]) / max(len(signals), 1)
        }
        
        # 添加更多性能指标(根据具体需求)
        if 'returns' in signals.columns:
            perf['total_return'] = signals['returns'].sum()
            perf['avg_return'] = signals['returns'].mean()
            perf['max_return'] = signals['returns'].max()
            perf['min_return'] = signals['returns'].min()
            perf['sharpe_ratio'] = self._calculate_sharpe_ratio(signals['returns'])
        
        return perf
    
    def _calculate_sharpe_ratio(self, returns: pd.Series, risk_free_rate: float = 0.03) -> float:
        """计算夏普比率"""
        if returns.empty or returns.std() == 0:
            return 0
        
        excess_returns = returns.mean() - risk_free_rate / 252
        return excess_returns / returns.std() * np.sqrt(252)
    
    def _setup_logger(self) -> logging.Logger:
        """设置日志"""
        logger = logging.getLogger(f"Strategy.{self.name}")
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            # 控制台处理器
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.INFO)
            
            # 文件处理器
            log_dir = Path("logs/strategy")
            log_dir.mkdir(parents=True, exist_ok=True)
            file_handler = logging.FileHandler(
                log_dir / f"{self.name}_{datetime.now().strftime('%Y%m%d')}.log"
            )
            file_handler.setLevel(logging.DEBUG)
            
            # 格式化器
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            console_handler.setFormatter(formatter)
            file_handler.setFormatter(formatter)
            
            logger.addHandler(console_handler)
            logger.addHandler(file_handler)
        
        return logger
    
    def save_state(self, filepath: str):
        """保存策略状态"""
        state = {
            'name': self.name,
            'params': self.params,
            'config': self.config,
            'metadata': self.metadata,
            'performance': self.performance
        }
        
        with open(filepath, 'w') as f:
            json.dump(state, f, indent=4, default=str)
        
        self.logger.info(f"策略状态已保存: {filepath}")
    
    def load_state(self, filepath: str):
        """加载策略状态"""
        with open(filepath, 'r') as f:
            state = json.load(f)
        
        self.name = state['name']
        self.params = state['params']
        self.config = state['config']
        self.metadata = state['metadata']
        self.performance = state['performance']
        
        self.logger.info(f"策略状态已加载: {filepath}")