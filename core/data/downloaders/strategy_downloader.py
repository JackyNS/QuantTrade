#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
策略数据下载器 - 统一版本
=======================

从scripts/data/迁移而来，集成到统一数据架构中
支持多种策略数据下载：资金流向、龙虎榜、市场情绪等

Author: QuantTrader Team
Date: 2025-08-31
"""

import os
import json
import time
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Optional, Union, Dict, Any
import logging
from tqdm import tqdm

try:
    import pandas as pd
except ImportError:
    pd = None

from ..adapters.data_source_manager import DataSourceManager
from ..cache_manager import SmartCacheManager

logger = logging.getLogger(__name__)

class StrategyDownloader:
    """策略数据下载器
    
    功能：
    1. 资金流向数据下载
    2. 龙虎榜数据下载
    3. 市场情绪数据下载
    4. 技术形态数据下载
    5. 断点续传和进度管理
    """
    
    def __init__(self, config: Optional[Dict] = None):
        """初始化下载器
        
        Args:
            config: 配置参数
        """
        self.config = config or {}
        
        # 初始化数据源管理器
        self.data_source_manager = DataSourceManager(self.config)
        
        # 缓存配置
        cache_config = {
            'cache_dir': self.config.get('cache_dir', './data/cache'),
            'max_memory_size': 30 * 1024 * 1024,  # 30MB
            'default_expire_hours': 6  # 策略数据更新频率高，缓存时间短
        }
        self.cache_manager = SmartCacheManager(cache_config)
        
        # 数据目录配置
        data_dir = self.config.get('data_dir', './data')
        self.strategy_data_dir = Path(data_dir) / 'strategy'
        self.metadata_dir = Path(data_dir) / 'metadata'
        
        # 创建子目录
        self.directories = {
            'capital_flow': self.strategy_data_dir / 'capital_flow',
            'dragon_tiger': self.strategy_data_dir / 'dragon_tiger',
            'market_sentiment': self.strategy_data_dir / 'market_sentiment',
            'pattern': self.strategy_data_dir / 'pattern',
            'technical': self.strategy_data_dir / 'technical'
        }
        
        # 创建所有目录
        for dir_path in self.directories.values():
            dir_path.mkdir(parents=True, exist_ok=True)
        self.metadata_dir.mkdir(parents=True, exist_ok=True)
        
        # 进度文件
        self.progress_file = self.metadata_dir / 'strategy_download_progress.json'
        self.progress = self._load_progress()
        
        # 下载配置
        self.batch_size = self.config.get('batch_size', 20)
        self.delay = self.config.get('delay', 0.3)
        self.max_retry = self.config.get('max_retry', 2)
        self.default_lookback_days = self.config.get('lookback_days', 250)
        
        # 加载股票列表
        self.stock_list = self._load_stock_list()
    
    def _load_progress(self) -> Dict[str, Any]:
        """加载下载进度"""
        try:
            if self.progress_file.exists():
                with open(self.progress_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
        except Exception as e:
            logger.warning(f"加载策略数据进度文件失败: {e}")
        
        return {
            'capital_flow': {'completed': [], 'failed': [], 'last_update': None},
            'dragon_tiger': {'completed': [], 'failed': [], 'last_update': None},
            'market_sentiment': {'completed': [], 'failed': [], 'last_update': None},
            'pattern': {'completed': [], 'failed': [], 'last_update': None},
            'technical': {'completed': [], 'failed': [], 'last_update': None},
            'statistics': {
                'total_symbols': 0,
                'total_downloaded': 0,
                'success_rate': 0.0
            }
        }
    
    def _save_progress(self):
        """保存下载进度"""
        try:
            # 更新统计信息
            total_completed = sum(len(data['completed']) for data in self.progress.values() 
                                if isinstance(data, dict) and 'completed' in data)
            total_failed = sum(len(data['failed']) for data in self.progress.values()
                             if isinstance(data, dict) and 'failed' in data)
            total_attempted = total_completed + total_failed
            
            self.progress['statistics'] = {
                'total_symbols': len(self.stock_list),
                'total_downloaded': total_completed,
                'success_rate': total_completed / total_attempted if total_attempted > 0 else 0.0,
                'last_update': datetime.now().isoformat()
            }
            
            with open(self.progress_file, 'w', encoding='utf-8') as f:
                json.dump(self.progress, f, indent=2, ensure_ascii=False)
                
        except Exception as e:
            logger.error(f"保存策略数据进度文件失败: {e}")
    
    def _load_stock_list(self) -> List[str]:
        """加载股票列表"""
        # 从元数据文件加载
        metadata_file = self.metadata_dir / 'all_stocks.json'
        
        try:
            if metadata_file.exists():
                with open(metadata_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    
                if 'stocks' in data:
                    symbols = data['stocks']
                elif 'symbols' in data:
                    symbols = data['symbols']
                else:
                    symbols = []
                
                logger.info(f"从元数据加载 {len(symbols)} 只股票")
                return symbols
            
            # 尝试从数据源获取
            df = self.data_source_manager.get_stock_list()
            if df is not None and not df.empty:
                if 'symbol' in df.columns:
                    symbols = df['symbol'].tolist()
                elif 'code' in df.columns:
                    symbols = df['code'].tolist()
                else:
                    symbols = df.iloc[:, 0].tolist()
                
                logger.info(f"从数据源获取 {len(symbols)} 只股票")
                return symbols
                
        except Exception as e:
            logger.error(f"加载股票列表失败: {e}")
        
        return []
    
    def download_capital_flow_data(self, 
                                 symbols: List[str],
                                 start_date: Union[str, datetime] = None,
                                 end_date: Union[str, datetime] = None) -> Dict[str, int]:
        """下载资金流向数据
        
        Args:
            symbols: 股票代码列表
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            下载统计结果
        """
        logger.info(f"开始下载资金流向数据，共 {len(symbols)} 只股票")
        
        if not start_date:
            start_date = (datetime.now() - timedelta(days=self.default_lookback_days)).strftime('%Y-%m-%d')
        if not end_date:
            end_date = datetime.now().strftime('%Y-%m-%d')
        
        success_count = 0
        failed_count = 0
        
        progress_key = 'capital_flow'
        completed_symbols = set(self.progress[progress_key]['completed'])
        
        with tqdm(total=len(symbols), desc="资金流向数据") as pbar:
            for symbol in symbols:
                if symbol in completed_symbols:
                    pbar.update(1)
                    continue
                
                try:
                    # 尝试从缓存获取
                    cache_key = f"capital_flow_{symbol}_{start_date}_{end_date}"
                    cached_data = self.cache_manager.get('capital_flow', {
                        'symbol': symbol, 
                        'start_date': start_date, 
                        'end_date': end_date
                    })
                    
                    if cached_data is not None:
                        data = cached_data
                    else:
                        # 从数据源获取资金流向数据
                        # 这里需要根据实际API调整
                        data = self._fetch_capital_flow_data(symbol, start_date, end_date)
                        
                        # 缓存数据
                        if data is not None:
                            self.cache_manager.put('capital_flow', {
                                'symbol': symbol,
                                'start_date': start_date,
                                'end_date': end_date
                            }, data, expire_hours=6)
                    
                    if data is not None:
                        # 保存数据
                        file_path = self.directories['capital_flow'] / f"{symbol}.csv"
                        if pd is not None and hasattr(data, 'to_csv'):
                            data.to_csv(file_path, index=False)
                        else:
                            # 备用保存方法
                            with open(file_path, 'w', encoding='utf-8') as f:
                                f.write(str(data))
                        
                        success_count += 1
                        self.progress[progress_key]['completed'].append(symbol)
                        
                        # 从失败列表移除
                        if symbol in self.progress[progress_key]['failed']:
                            self.progress[progress_key]['failed'].remove(symbol)
                    else:
                        failed_count += 1
                        if symbol not in self.progress[progress_key]['failed']:
                            self.progress[progress_key]['failed'].append(symbol)
                
                except Exception as e:
                    logger.error(f"下载 {symbol} 资金流向数据失败: {e}")
                    failed_count += 1
                    if symbol not in self.progress[progress_key]['failed']:
                        self.progress[progress_key]['failed'].append(symbol)
                
                pbar.update(1)
                pbar.set_postfix({
                    '成功': success_count,
                    '失败': failed_count
                })
                
                time.sleep(self.delay)
        
        # 更新最后更新时间
        self.progress[progress_key]['last_update'] = datetime.now().isoformat()
        self._save_progress()
        
        logger.info(f"资金流向数据下载完成: 成功 {success_count}, 失败 {failed_count}")
        return {'success': success_count, 'failed': failed_count}
    
    def _fetch_capital_flow_data(self, symbol: str, start_date: str, end_date: str):
        """获取资金流向数据的具体实现"""
        try:
            # 尝试使用不同的数据源获取资金流向数据
            # 这里需要根据实际数据源API调整
            
            # 方法1：使用通用市场数据接口
            df = self.data_source_manager.get_market_data(
                data_type='capital_flow',
                start_date=start_date,
                end_date=end_date,
                symbol=symbol
            )
            
            return df
            
        except Exception as e:
            logger.debug(f"获取 {symbol} 资金流向数据失败: {e}")
            return None
    
    def download_market_sentiment_data(self, 
                                     start_date: Union[str, datetime] = None,
                                     end_date: Union[str, datetime] = None) -> Dict[str, int]:
        """下载市场情绪数据
        
        Args:
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            下载统计结果
        """
        logger.info("开始下载市场情绪数据")
        
        if not start_date:
            start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        if not end_date:
            end_date = datetime.now().strftime('%Y-%m-%d')
        
        try:
            # 获取市场情绪数据
            sentiment_data = self._fetch_market_sentiment_data(start_date, end_date)
            
            if sentiment_data is not None:
                # 保存数据
                file_path = self.directories['market_sentiment'] / f"sentiment_{start_date}_{end_date}.csv"
                
                if pd is not None and hasattr(sentiment_data, 'to_csv'):
                    sentiment_data.to_csv(file_path, index=False)
                else:
                    with open(file_path, 'w', encoding='utf-8') as f:
                        f.write(str(sentiment_data))
                
                self.progress['market_sentiment']['last_update'] = datetime.now().isoformat()
                self._save_progress()
                
                logger.info(f"市场情绪数据下载完成，保存到: {file_path}")
                return {'success': 1, 'failed': 0}
            else:
                logger.warning("未获取到市场情绪数据")
                return {'success': 0, 'failed': 1}
                
        except Exception as e:
            logger.error(f"下载市场情绪数据失败: {e}")
            return {'success': 0, 'failed': 1}
    
    def _fetch_market_sentiment_data(self, start_date: str, end_date: str):
        """获取市场情绪数据的具体实现"""
        try:
            # 使用数据源管理器获取市场情绪相关数据
            df = self.data_source_manager.get_market_data(
                data_type='market_sentiment',
                start_date=start_date,
                end_date=end_date
            )
            
            return df
            
        except Exception as e:
            logger.debug(f"获取市场情绪数据失败: {e}")
            return None
    
    def download_all_strategy_data(self, 
                                 symbols: Optional[List[str]] = None,
                                 data_types: List[str] = None) -> Dict[str, Dict[str, int]]:
        """下载所有策略数据
        
        Args:
            symbols: 股票代码列表，如果为None则使用全部股票
            data_types: 数据类型列表，如果为None则下载所有类型
            
        Returns:
            各类型数据的下载统计结果
        """
        if symbols is None:
            symbols = self.stock_list
        
        if data_types is None:
            data_types = ['capital_flow', 'market_sentiment']
        
        logger.info(f"开始下载策略数据，股票数量: {len(symbols)}, 数据类型: {data_types}")
        
        results = {}
        
        # 下载资金流向数据
        if 'capital_flow' in data_types and symbols:
            results['capital_flow'] = self.download_capital_flow_data(symbols)
        
        # 下载市场情绪数据
        if 'market_sentiment' in data_types:
            results['market_sentiment'] = self.download_market_sentiment_data()
        
        # 总计统计
        total_success = sum(result.get('success', 0) for result in results.values())
        total_failed = sum(result.get('failed', 0) for result in results.values())
        
        logger.info("="*60)
        logger.info(f"策略数据下载完成!")
        logger.info(f"总成功: {total_success}")
        logger.info(f"总失败: {total_failed}")
        
        for data_type, result in results.items():
            logger.info(f"  {data_type}: 成功 {result.get('success', 0)}, 失败 {result.get('failed', 0)}")
        
        logger.info("="*60)
        
        return results
    
    def get_download_status(self) -> Dict[str, Any]:
        """获取下载状态报告"""
        return {
            'stock_count': len(self.stock_list),
            'progress': self.progress,
            'data_directories': {k: str(v) for k, v in self.directories.items()},
            'available_data_sources': self.data_source_manager.get_available_sources(),
            'cache_stats': self.cache_manager.get_cache_stats()
        }
    
    def cleanup_cache(self):
        """清理缓存"""
        self.cache_manager.clear_cache()
        logger.info("策略数据缓存已清理")
    
    def __enter__(self):
        """上下文管理器入口"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器出口"""
        self._save_progress()
        if hasattr(self.data_source_manager, 'cleanup'):
            self.data_source_manager.cleanup()