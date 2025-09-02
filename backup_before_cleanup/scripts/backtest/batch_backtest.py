#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
批量回测脚本
============

批量运行多个策略回测

Author: QuantTrader Team
Date: 2025-08-31
"""

from typing import Dict, List

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from core.backtest import BacktestEngine
from core.config import Config
import pandas as pd
import numpy as np
from itertools import product
from concurrent.futures import ProcessPoolExecutor
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BatchBacktester:
    """批量回测器"""
    
    def __init__(self):
        self.config = Config()
        self.results = []
        
    def run_single_backtest(self, params: Dict) -> Dict:
        """运行单个回测"""
        try:
            engine = BacktestEngine(self.config)
            
            result = engine.run(
                strategy=params['strategy'],
                symbols=params.get('symbols', ['000001', '000002']),
                start_date=params.get('start_date', self.config.START_DATE),
                end_date=params.get('end_date', self.config.END_DATE),
                **params.get('strategy_params', {})
            )
            
            return {
                'params': params,
                'success': True,
                'metrics': result
            }
            
        except Exception as e:
            logger.error(f"回测失败: {e}")
            return {
                'params': params,
                'success': False,
                'error': str(e)
            }
    
    def run_parameter_optimization(self, strategy_name: str, param_grid: Dict):
        """参数优化"""
        logger.info(f"开始参数优化: {strategy_name}")
        
        # 生成参数组合
        param_names = list(param_grid.keys())
        param_values = list(param_grid.values())
        param_combinations = list(product(*param_values))
        
        logger.info(f"参数组合数: {len(param_combinations)}")
        
        tasks = []
        for combination in param_combinations:
            params = dict(zip(param_names, combination))
            tasks.append({
                'strategy': strategy_name,
                'strategy_params': params
            })
        
        # 并行运行
        with ProcessPoolExecutor(max_workers=4) as executor:
            results = list(executor.map(self.run_single_backtest, tasks))
        
        # 找出最佳参数
        successful_results = [r for r in results if r['success']]
        if successful_results:
            best_result = max(successful_results, 
                            key=lambda x: x['metrics'].get('sharpe_ratio', 0))
            
            logger.info(f"最佳参数: {best_result['params']['strategy_params']}")
            logger.info(f"夏普比率: {best_result['metrics'].get('sharpe_ratio', 0):.2f}")
            
            return best_result
        
        return None
    
    def run_strategy_comparison(self, strategies: List[str], symbols: List[str]):
        """策略对比"""
        logger.info(f"对比策略: {strategies}")
        
        results = []
        for strategy in strategies:
            result = self.run_single_backtest({
                'strategy': strategy,
                'symbols': symbols
            })
            results.append(result)
        
        # 生成对比报告
        comparison = pd.DataFrame([
            {
                'strategy': r['params']['strategy'],
                'total_return': r['metrics'].get('total_return', 0),
                'sharpe_ratio': r['metrics'].get('sharpe_ratio', 0),
                'max_drawdown': r['metrics'].get('max_drawdown', 0)
            }
            for r in results if r['success']
        ])
        
        print("\n策略对比结果:")
        print(comparison.to_string())
        
        return comparison

def main():
    backtester = BatchBacktester()
    
    # 参数优化示例
    param_grid = {
        'fast_period': [5, 10, 15],
        'slow_period': [20, 30, 40]
    }
    
    backtester.run_parameter_optimization('MA_Cross', param_grid)

if __name__ == "__main__":
    main()