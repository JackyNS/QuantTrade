#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
A股数据下载器 - 统一版本
=====================

从scripts/data/迁移而来，集成到统一数据架构中
支持多数据源、智能缓存、质量检查

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
from ..quality_checker import DataQualityChecker

logger = logging.getLogger(__name__)

class ASharesDownloader:
    """A股数据下载器
    
    功能：
    1. 多数据源支持（优矿、Tushare、Yahoo、AKShare）
    2. 断点续传和进度管理
    3. 数据质量检查
    4. 智能缓存
    5. 批量下载优化
    """
    
    def __init__(self, config: Optional[Dict] = None):
        """初始化下载器
        
        Args:
            config: 配置参数，包含数据目录、API配置等
        """
        self.config = config or {}
        
        # 初始化核心组件
        self.data_source_manager = DataSourceManager(self.config)
        
        # 缓存配置
        cache_config = {
            'cache_dir': self.config.get('cache_dir', './data/cache'),
            'max_memory_size': 50 * 1024 * 1024,  # 50MB
            'default_expire_hours': 24
        }
        self.cache_manager = SmartCacheManager(cache_config)
        
        # 质量检查器
        self.quality_checker = DataQualityChecker()
        
        # 数据目录
        data_dir = self.config.get('data_dir', './data')
        self.data_dir = Path(data_dir) / 'raw' / 'daily'
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        # 进度文件
        self.progress_file = Path(data_dir) / 'metadata' / 'download_progress.json'
        self.progress_file.parent.mkdir(parents=True, exist_ok=True)
        self.progress = self._load_progress()
        
        # 下载配置
        self.batch_size = self.config.get('batch_size', 50)
        self.delay = self.config.get('delay', 0.2)
        self.max_retry = self.config.get('max_retry', 3)
        
    def _load_progress(self) -> Dict[str, Any]:
        """加载下载进度"""
        try:
            if self.progress_file.exists():
                with open(self.progress_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
        except Exception as e:
            logger.warning(f"加载进度文件失败: {e}")
        
        return {
            'completed': [],
            'failed': [],
            'total': 0,
            'last_update': None,
            'statistics': {
                'success_rate': 0.0,
                'total_downloaded': 0,
                'avg_download_time': 0.0
            }
        }
    
    def _save_progress(self):
        """保存下载进度"""
        try:
            self.progress['last_update'] = datetime.now().isoformat()
            
            # 更新统计信息
            completed = len(self.progress['completed'])
            failed = len(self.progress['failed'])
            total_attempted = completed + failed
            
            if total_attempted > 0:
                self.progress['statistics']['success_rate'] = completed / total_attempted
                self.progress['statistics']['total_downloaded'] = completed
            
            with open(self.progress_file, 'w', encoding='utf-8') as f:
                json.dump(self.progress, f, indent=2, ensure_ascii=False)
                
        except Exception as e:
            logger.error(f"保存进度文件失败: {e}")
    
    def get_stock_list(self, market: Optional[str] = None) -> List[str]:
        """获取股票列表
        
        Args:
            market: 市场代码，如 'sz', 'sh', 'bj'
            
        Returns:
            股票代码列表
        """
        logger.info("获取股票列表...")
        
        try:
            # 使用数据源管理器获取股票列表
            df = self.data_source_manager.get_stock_list(market=market)
            
            if df is not None and not df.empty:
                # 假设股票代码在'symbol'或'code'列中
                if 'symbol' in df.columns:
                    symbols = df['symbol'].tolist()
                elif 'code' in df.columns:
                    symbols = df['code'].tolist()
                else:
                    # 使用第一列
                    symbols = df.iloc[:, 0].tolist()
                
                logger.info(f"从数据源获取到 {len(symbols)} 只股票")
                self.progress['total'] = len(symbols)
                return symbols
            
            # 备用方案：从本地文件读取
            stock_file = self.progress_file.parent / 'all_stocks.json'
            if stock_file.exists():
                with open(stock_file, 'r', encoding='utf-8') as f:
                    stock_data = json.load(f)
                    symbols = stock_data.get('stocks', [])
                    
                logger.info(f"从本地文件获取到 {len(symbols)} 只股票")
                self.progress['total'] = len(symbols)
                return symbols
            
        except Exception as e:
            logger.error(f"获取股票列表失败: {e}")
        
        return []
    
    def download_single_stock(self, 
                            symbol: str,
                            start_date: Union[str, datetime] = None,
                            end_date: Union[str, datetime] = None,
                            use_cache: bool = True,
                            quality_check: bool = True) -> bool:
        """下载单只股票数据
        
        Args:
            symbol: 股票代码
            start_date: 开始日期
            end_date: 结束日期
            use_cache: 是否使用缓存
            quality_check: 是否进行质量检查
            
        Returns:
            是否下载成功
        """
        start_time = time.time()
        
        try:
            # 检查缓存
            cache_key = f"stock_data_{symbol}_{start_date}_{end_date}"
            if use_cache:
                cached_data = self.cache_manager.get('price_data', {'symbol': symbol})
                if cached_data is not None:
                    logger.debug(f"{symbol}: 使用缓存数据")
                    return self._save_stock_data(symbol, cached_data, quality_check)
            
            # 从数据源获取数据
            df = self.data_source_manager.get_price_data(
                symbols=[symbol],
                start_date=start_date or '2020-01-01',
                end_date=end_date or datetime.now().strftime('%Y-%m-%d')
            )
            
            if df is not None and not df.empty:
                # 缓存数据
                if use_cache:
                    self.cache_manager.put('price_data', {'symbol': symbol}, df)
                
                # 保存数据
                success = self._save_stock_data(symbol, df, quality_check)
                
                # 记录下载时间
                download_time = time.time() - start_time
                logger.debug(f"{symbol}: 下载完成，用时 {download_time:.2f}s")
                
                return success
            else:
                logger.warning(f"{symbol}: 获取到空数据")
                return False
                
        except Exception as e:
            logger.error(f"{symbol}: 下载失败 - {str(e)}")
            return False
    
    def _save_stock_data(self, symbol: str, df, quality_check: bool = True) -> bool:
        """保存股票数据到文件
        
        Args:
            symbol: 股票代码
            df: 数据DataFrame
            quality_check: 是否进行质量检查
            
        Returns:
            是否保存成功
        """
        try:
            # 数据质量检查
            if quality_check and pd is not None:
                quality_result = self.quality_checker.check_missing_data(df)
                if quality_result['overall']['missing_rate'] > 0.1:
                    logger.warning(f"{symbol}: 数据质量警告，缺失率 {quality_result['overall']['missing_rate']:.1%}")
            
            # 保存到CSV文件
            file_path = self.data_dir / f"{symbol}.csv"
            
            if pd is not None:
                df.to_csv(file_path, index=True)
            else:
                # 如果pandas不可用，使用基础方法保存
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(str(df))
            
            logger.debug(f"{symbol}: 保存到 {file_path}")
            return True
            
        except Exception as e:
            logger.error(f"{symbol}: 保存失败 - {str(e)}")
            return False
    
    def download_batch(self, 
                      symbols: List[str],
                      start_date: Union[str, datetime] = None,
                      end_date: Union[str, datetime] = None,
                      resume: bool = True) -> Dict[str, int]:
        """批量下载股票数据
        
        Args:
            symbols: 股票代码列表
            start_date: 开始日期
            end_date: 结束日期
            resume: 是否断点续传
            
        Returns:
            下载统计结果
        """
        # 过滤已完成的股票
        if resume:
            remaining_symbols = [s for s in symbols if s not in self.progress['completed']]
            logger.info(f"断点续传: 跳过 {len(symbols) - len(remaining_symbols)} 只已完成的股票")
        else:
            remaining_symbols = symbols
        
        if not remaining_symbols:
            logger.info("所有股票都已下载完成")
            return {'success': len(self.progress['completed']), 'failed': len(self.progress['failed'])}
        
        logger.info(f"开始批量下载 {len(remaining_symbols)} 只股票")
        
        success_count = 0
        failed_count = 0
        
        # 批量处理
        with tqdm(total=len(remaining_symbols), desc="下载进度") as pbar:
            for i, symbol in enumerate(remaining_symbols):
                try:
                    if self.download_single_stock(symbol, start_date, end_date):
                        success_count += 1
                        if symbol not in self.progress['completed']:
                            self.progress['completed'].append(symbol)
                        
                        # 从失败列表中移除（如果存在）
                        if symbol in self.progress['failed']:
                            self.progress['failed'].remove(symbol)
                    else:
                        failed_count += 1
                        if symbol not in self.progress['failed']:
                            self.progress['failed'].append(symbol)
                    
                    # 更新进度条
                    pbar.update(1)
                    pbar.set_postfix({
                        '成功': success_count,
                        '失败': failed_count,
                        '成功率': f"{success_count/(success_count+failed_count)*100:.1f}%" if (success_count+failed_count) > 0 else "0%"
                    })
                    
                    # 控制请求频率
                    time.sleep(self.delay)
                    
                    # 定期保存进度
                    if (i + 1) % 50 == 0:
                        self._save_progress()
                        logger.debug(f"已保存进度: {i + 1}/{len(remaining_symbols)}")
                        
                except KeyboardInterrupt:
                    logger.info("用户中断下载")
                    self._save_progress()
                    break
                except Exception as e:
                    logger.error(f"下载 {symbol} 时发生异常: {e}")
                    failed_count += 1
                    if symbol not in self.progress['failed']:
                        self.progress['failed'].append(symbol)
        
        # 最终保存进度
        self._save_progress()
        
        # 统计结果
        result = {
            'success': success_count,
            'failed': failed_count,
            'total': len(remaining_symbols),
            'success_rate': success_count / len(remaining_symbols) if remaining_symbols else 0
        }
        
        logger.info("=" * 60)
        logger.info(f"批量下载完成!")
        logger.info(f"成功: {success_count} ({result['success_rate']*100:.1f}%)")
        logger.info(f"失败: {failed_count}")
        logger.info(f"总计: {len(self.progress['completed'])} 只股票已完成")
        logger.info("=" * 60)
        
        return result
    
    def download_all(self,
                    market: Optional[str] = None,
                    start_date: Union[str, datetime] = None,
                    end_date: Union[str, datetime] = None,
                    resume: bool = True) -> Dict[str, int]:
        """下载所有股票数据
        
        Args:
            market: 市场代码
            start_date: 开始日期
            end_date: 结束日期
            resume: 是否断点续传
            
        Returns:
            下载统计结果
        """
        # 获取股票列表
        stock_list = self.get_stock_list(market)
        
        if not stock_list:
            logger.error("无法获取股票列表")
            return {'success': 0, 'failed': 0, 'total': 0}
        
        return self.download_batch(stock_list, start_date, end_date, resume)
    
    def retry_failed(self) -> Dict[str, int]:
        """重试失败的股票"""
        failed_symbols = self.progress['failed'].copy()
        
        if not failed_symbols:
            logger.info("没有失败的股票需要重试")
            return {'success': 0, 'failed': 0, 'total': 0}
        
        logger.info(f"重试 {len(failed_symbols)} 只失败的股票")
        
        # 清空失败列表，重新开始
        self.progress['failed'] = []
        
        return self.download_batch(failed_symbols, resume=False)
    
    def get_download_status(self) -> Dict[str, Any]:
        """获取下载状态报告"""
        total = self.progress['total']
        completed = len(self.progress['completed'])
        failed = len(self.progress['failed'])
        
        return {
            'total': total,
            'completed': completed,
            'failed': failed,
            'remaining': max(0, total - completed - failed),
            'progress_rate': completed / total if total > 0 else 0,
            'success_rate': self.progress['statistics']['success_rate'],
            'last_update': self.progress['last_update'],
            'data_sources': self.data_source_manager.get_available_sources(),
            'cache_stats': self.cache_manager.get_cache_stats()
        }
    
    def cleanup_cache(self):
        """清理缓存"""
        self.cache_manager.clear_cache()
        logger.info("缓存已清理")
    
    def __enter__(self):
        """上下文管理器入口"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器出口"""
        self._save_progress()
        if hasattr(self.data_source_manager, 'cleanup'):
            self.data_source_manager.cleanup()