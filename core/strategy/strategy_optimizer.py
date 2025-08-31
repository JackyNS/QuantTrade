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

class StrategyOptimizer:
    """
    高级策略优化器 - 支持多种优化方法
    包括网格搜索、贝叶斯优化、遗传算法等
    """
    
    def __init__(self, strategy_class: Any, config: Optional[Dict] = None):
        """
        初始化策略优化器
        
        Args:
            strategy_class: 策略类
            config: 配置参数
        """
        self.strategy_class = strategy_class
        self.config = config or self._get_default_config()
        
        # 优化结果
        self.optimization_results = []
        self.best_params = None
        self.best_score = -np.inf
        
        # 优化历史
        self.optimization_history = {
            'params': [],
            'scores': [],
            'timestamps': [],
            'method': None
        }
        
        # 性能指标
        self.performance_metrics = {}
        
        self.logger = self._setup_logger()
        self.logger.info("策略优化器初始化完成")
    
    def _get_default_config(self) -> Dict:
        """获取默认配置"""
        return {
            'optimization': {
                'method': 'grid_search',  # grid_search/random_search/bayesian/genetic/differential_evolution
                'metric': 'sharpe_ratio',  # sharpe_ratio/total_return/win_rate/calmar_ratio
                'direction': 'maximize',    # maximize/minimize
                'n_trials': 100,           # 试验次数
                'n_jobs': -1,              # 并行数(-1为全部CPU)
                'timeout': 3600,           # 超时时间(秒)
                'early_stopping': True,     # 早停
                'patience': 20             # 早停耐心值
            },
            'validation': {
                'method': 'time_series_split',  # time_series_split/walk_forward/expanding_window
                'n_splits': 5,                  # 折数
                'test_size': 0.2,               # 测试集比例
                'gap': 0                       # 训练集和测试集之间的间隔
            },
            'param_space': {
                'stop_loss': {'min': 0.03, 'max': 0.15, 'step': 0.01},
                'take_profit': {'min': 0.10, 'max': 0.50, 'step': 0.05},
                'signal_threshold': {'min': 0.5, 'max': 0.8, 'step': 0.05},
                'max_position': {'min': 0.8, 'max': 1.0, 'step': 0.05},
                'max_stocks': {'min': 5, 'max': 30, 'step': 5}
            },
            'constraints': {
                'min_trades': 10,          # 最小交易次数
                'max_drawdown': 0.20,      # 最大回撤限制
                'min_win_rate': 0.40       # 最小胜率
            },
            'robustness': {
                'monte_carlo': True,        # 蒙特卡洛模拟
                'n_simulations': 100,       # 模拟次数
                'confidence_level': 0.95    # 置信水平
            }
        }
    
    def optimize(self, data: pd.DataFrame, initial_capital: float = 1000000) -> Dict:
        """
        执行策略优化
        
        Args:
            data: 历史数据
            initial_capital: 初始资金
            
        Returns:
            优化结果
        """
        self.logger.info(f"开始策略优化,方法: {self.config['optimization']['method']}")
        
        method = self.config['optimization']['method']
        
        if method == 'grid_search':
            results = self._grid_search(data, initial_capital)
        elif method == 'random_search':
            results = self._random_search(data, initial_capital)
        elif method == 'bayesian':
            results = self._bayesian_optimization(data, initial_capital)
        elif method == 'genetic':
            results = self._genetic_algorithm(data, initial_capital)
        elif method == 'differential_evolution':
            results = self._differential_evolution(data, initial_capital)
        else:
            raise ValueError(f"未知的优化方法: {method}")
        
        # 验证最佳参数
        if self.best_params:
            validation_score = self._validate_params(self.best_params, data, initial_capital)
            results['validation_score'] = validation_score
            
            # 稳健性测试
            if self.config['robustness']['monte_carlo']:
                robustness = self._monte_carlo_test(self.best_params, data, initial_capital)
                results['robustness'] = robustness
        
        self.logger.info(f"优化完成,最佳得分: {self.best_score:.4f}")
        
        return results
    
    def _grid_search(self, data: pd.DataFrame, initial_capital: float) -> Dict:
        """网格搜索"""
        param_space = self.config['param_space']
        
        # 生成参数网格
        param_grid = []
        for params in ParameterGrid(self._create_grid(param_space)):
            param_grid.append(params)
        
        self.logger.info(f"网格搜索空间: {len(param_grid)} 个参数组合")
        
        # 遍历参数组合
        for i, params in enumerate(param_grid):
            if i % 10 == 0:
                self.logger.info(f"进度: {i}/{len(param_grid)}")
            
            score = self._evaluate_params(params, data, initial_capital)
            
            # 更新最佳参数
            if score > self.best_score:
                self.best_score = score
                self.best_params = params
                self.logger.info(f"新的最佳参数: {params}, 得分: {score:.4f}")
            
            # 保存历史
            self._save_optimization_step(params, score)
            
            # 早停检查
            if self._check_early_stopping():
                self.logger.info("触发早停")
                break
        
        return {
            'best_params': self.best_params,
            'best_score': self.best_score,
            'total_evaluations': len(self.optimization_history['params']),
            'method': 'grid_search'
        }
    
    def _random_search(self, data: pd.DataFrame, initial_capital: float) -> Dict:
        """随机搜索"""
        param_space = self.config['param_space']
        n_trials = self.config['optimization']['n_trials']
        
        self.logger.info(f"随机搜索: {n_trials} 次试验")
        
        for i in range(n_trials):
            if i % 10 == 0:
                self.logger.info(f"进度: {i}/{n_trials}")
            
            # 随机采样参数
            params = self._random_sample_params(param_space)
            
            score = self._evaluate_params(params, data, initial_capital)
            
            # 更新最佳参数
            if score > self.best_score:
                self.best_score = score
                self.best_params = params
                self.logger.info(f"新的最佳参数: {params}, 得分: {score:.4f}")
            
            # 保存历史
            self._save_optimization_step(params, score)
            
            # 早停检查
            if self._check_early_stopping():
                self.logger.info("触发早停")
                break
        
        return {
            'best_params': self.best_params,
            'best_score': self.best_score,
            'total_evaluations': len(self.optimization_history['params']),
            'method': 'random_search'
        }
    
    def _differential_evolution(self, data: pd.DataFrame, initial_capital: float) -> Dict:
        """差分进化算法"""
        param_space = self.config['param_space']
        
        # 定义参数边界
        bounds = []
        param_names = []
        for param, config in param_space.items():
            bounds.append((config['min'], config['max']))
            param_names.append(param)
        
        # 定义目标函数
        def objective(x):
            params = dict(zip(param_names, x))
            score = self._evaluate_params(params, data, initial_capital)
            return -score  # 最小化负分数
        
        # 运行优化
        result = differential_evolution(
            objective,
            bounds,
            maxiter=self.config['optimization']['n_trials'],
            popsize=15,
            tol=0.01,
            seed=42
        )
        
        # 保存结果
        self.best_params = dict(zip(param_names, result.x))
        self.best_score = -result.fun
        
        self.logger.info(f"差分进化完成: 最佳得分 {self.best_score:.4f}")
        
        return {
            'best_params': self.best_params,
            'best_score': self.best_score,
            'total_evaluations': result.nfev,
            'method': 'differential_evolution',
            'convergence': result.success
        }
    
    def _evaluate_params(self, params: Dict, data: pd.DataFrame, initial_capital: float) -> float:
        """
        评估参数性能
        
        Args:
            params: 参数字典
            data: 数据
            initial_capital: 初始资金
            
        Returns:
            评分
        """
        try:
            # 创建策略实例
            strategy = self.strategy_class()
            strategy.params.update(params)
            
            # 运行回测
            results = self._run_backtest(strategy, data, initial_capital)
            
            # 检查约束条件
            if not self._check_constraints(results):
                return -np.inf
            
            # 计算评分
            metric = self.config['optimization']['metric']
            score = results.get(metric, -np.inf)
            
            return score
            
        except Exception as e:
            self.logger.error(f"参数评估失败: {str(e)}")
            return -np.inf
    
    def _run_backtest(self, strategy: Any, data: pd.DataFrame, initial_capital: float) -> Dict:
        """运行回测(简化版本)"""
        # 这里应该调用完整的回测引擎
        # 简化处理,返回模拟结果
        
        # 运行策略
        signals = strategy.run(data)
        
        # 模拟交易
        trades = []
        position = None
        
        for i in range(len(signals['signals'])):
            signal = signals['signals'].iloc[i]
            
            if signal.get('signal') > 0 and position is None:
                # 开仓
                position = {
                    'entry_price': data.iloc[i]['close'],
                    'entry_date': data.iloc[i]['date']
                }
            elif signal.get('signal') < 0 and position is not None:
                # 平仓
                exit_price = data.iloc[i]['close']
                returns = (exit_price - position['entry_price']) / position['entry_price']
                trades.append(returns)
                position = None
        
        # 计算性能指标
        if len(trades) == 0:
            return {'sharpe_ratio': -np.inf, 'total_return': 0, 'win_rate': 0}
        
        trades = np.array(trades)
        
        return {
            'sharpe_ratio': np.mean(trades) / (np.std(trades) + 1e-6) * np.sqrt(252),
            'total_return': np.sum(trades),
            'win_rate': np.mean(trades > 0),
            'max_drawdown': self._calculate_max_drawdown(trades),
            'n_trades': len(trades),
            'avg_return': np.mean(trades),
            'calmar_ratio': np.mean(trades) / (self._calculate_max_drawdown(trades) + 1e-6)
        }
    
    def _calculate_max_drawdown(self, returns: np.ndarray) -> float:
        """计算最大回撤"""
        cumulative = np.cumprod(1 + returns)
        running_max = np.maximum.accumulate(cumulative)
        drawdown = (cumulative - running_max) / running_max
        return abs(np.min(drawdown))
    
    def _validate_params(self, params: Dict, data: pd.DataFrame, initial_capital: float) -> float:
        """验证参数(使用交叉验证)"""
        validation_method = self.config['validation']['method']
        
        if validation_method == 'time_series_split':
            return self._time_series_validation(params, data, initial_capital)
        else:
            # 其他验证方法
            return self._evaluate_params(params, data, initial_capital)
    
    def _time_series_validation(self, params: Dict, data: pd.DataFrame, initial_capital: float) -> float:
        """时间序列交叉验证"""
        n_splits = self.config['validation']['n_splits']
        tscv = TimeSeriesSplit(n_splits=n_splits)
        
        scores = []
        
        for train_idx, test_idx in tscv.split(data):
            train_data = data.iloc[train_idx]
            test_data = data.iloc[test_idx]
            
            # 在训练集上训练
            strategy = self.strategy_class()
            strategy.params.update(params)
            
            # 在测试集上评估
            test_results = self._run_backtest(strategy, test_data, initial_capital)
            metric = self.config['optimization']['metric']
            scores.append(test_results.get(metric, -np.inf))
        
        return np.mean(scores)
    
    def _monte_carlo_test(self, params: Dict, data: pd.DataFrame, initial_capital: float) -> Dict:
        """蒙特卡洛稳健性测试"""
        n_simulations = self.config['robustness']['n_simulations']
        confidence_level = self.config['robustness']['confidence_level']
        
        self.logger.info(f"运行蒙特卡洛模拟: {n_simulations} 次")
        
        results = []
        
        for i in range(n_simulations):
            # 添加随机扰动
            perturbed_data = self._add_noise_to_data(data, noise_level=0.01)
            
            # 运行回测
            score = self._evaluate_params(params, perturbed_data, initial_capital)
            results.append(score)
        
        results = np.array(results)
        
        return {
            'mean': np.mean(results),
            'std': np.std(results),
            'confidence_interval': (
                np.percentile(results, (1 - confidence_level) / 2 * 100),
                np.percentile(results, (1 + confidence_level) / 2 * 100)
            ),
            'worst_case': np.min(results),
            'best_case': np.max(results),
            'stability': 1 - np.std(results) / (np.mean(results) + 1e-6)
        }
    
    def _add_noise_to_data(self, data: pd.DataFrame, noise_level: float) -> pd.DataFrame:
        """向数据添加噪声"""
        noisy_data = data.copy()
        
        for col in ['open', 'high', 'low', 'close']:
            if col in noisy_data.columns:
                noise = np.random.normal(0, noise_level, len(noisy_data))
                noisy_data[col] *= (1 + noise)
        
        return noisy_data
    
    def _check_constraints(self, results: Dict) -> bool:
        """检查约束条件"""
        constraints = self.config['constraints']
        
        if results.get('n_trades', 0) < constraints['min_trades']:
            return False
        
        if results.get('max_drawdown', 1.0) > constraints['max_drawdown']:
            return False
        
        if results.get('win_rate', 0) < constraints['min_win_rate']:
            return False
        
        return True
    
    def _check_early_stopping(self) -> bool:
        """检查早停条件"""
        if not self.config['optimization']['early_stopping']:
            return False
        
        if len(self.optimization_history['scores']) < self.config['optimization']['patience']:
            return False
        
        # 检查最近N次是否有改进
        recent_scores = self.optimization_history['scores'][-self.config['optimization']['patience']:]
        best_recent = max(recent_scores)
        
        return best_recent <= self.best_score
    
    def _save_optimization_step(self, params: Dict, score: float):
        """保存优化步骤"""
        self.optimization_history['params'].append(params)
        self.optimization_history['scores'].append(score)
        self.optimization_history['timestamps'].append(datetime.now())
        
        # 保存到结果列表
        self.optimization_results.append({
            'params': params,
            'score': score,
            'timestamp': datetime.now()
        })
    
    def _create_grid(self, param_space: Dict) -> Dict:
        """创建参数网格"""
        grid = {}
        
        for param, config in param_space.items():
            if 'step' in config:
                grid[param] = np.arange(config['min'], config['max'] + config['step'], config['step'])
            else:
                grid[param] = [config['min'], config['max']]
        
        return grid
    
    def _random_sample_params(self, param_space: Dict) -> Dict:
        """随机采样参数"""
        params = {}
        
        for param, config in param_space.items():
            if 'step' in config:
                # 离散参数
                values = np.arange(config['min'], config['max'] + config['step'], config['step'])
                params[param] = np.random.choice(values)
            else:
                # 连续参数
                params[param] = np.random.uniform(config['min'], config['max'])
        
        return params
    
    def get_optimization_report(self) -> Dict:
        """获取优化报告"""
        if not self.optimization_results:
            return {}
        
        scores = self.optimization_history['scores']
        
        return {
            'best_params': self.best_params,
            'best_score': self.best_score,
            'total_evaluations': len(self.optimization_results),
            'convergence_history': scores,
            'improvement_rate': (self.best_score - scores[0]) / abs(scores[0]) if scores else 0,
            'average_score': np.mean(scores) if scores else 0,
            'score_std': np.std(scores) if scores else 0,
            'top_10_params': sorted(self.optimization_results, key=lambda x: x['score'], reverse=True)[:10]
        }
    
    def save_results(self, filepath: str):
        """保存优化结果"""
        results = {
            'best_params': self.best_params,
            'best_score': self.best_score,
            'optimization_history': self.optimization_history,
            'optimization_results': self.optimization_results,
            'config': self.config,
            'report': self.get_optimization_report()
        }
        
        with open(filepath, 'wb') as f:
            pickle.dump(results, f)
        
        self.logger.info(f"优化结果已保存: {filepath}")
    
    def load_results(self, filepath: str):
        """加载优化结果"""
        with open(filepath, 'rb') as f:
            results = pickle.load(f)
        
        self.best_params = results['best_params']
        self.best_score = results['best_score']
        self.optimization_history = results['optimization_history']
        self.optimization_results = results['optimization_results']
        
        self.logger.info(f"优化结果已加载: {filepath}")
    
    def _setup_logger(self) -> logging.Logger:
        """设置日志"""
        logger = logging.getLogger("StrategyOptimizer")
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger