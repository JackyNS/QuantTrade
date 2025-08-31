#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
辅助函数模块 - utils/helpers.py
================================

提供常用的辅助函数，简化日常开发任务。

主要功能:
- 📁 文件操作
- 🔢 数字格式化
- 📅 日期处理
- 📊 数据处理
- 🔧 通用工具

使用示例:
```python
from core.utils import format_number, create_dirs, calculate_trading_days

# 格式化数字
print(format_number(1234567.89))  # "1,234,567.89"

# 创建目录
create_dirs(['data/raw', 'data/processed'])

# 计算交易日
days = calculate_trading_days('2024-01-01', '2024-12-31')
```

作者: QuantTrader Team
版本: 1.0.0
更新: 2025-08-29
"""

import os
import re
import json
import pickle
import hashlib
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Union, Callable, Iterable
from datetime import datetime, date, timedelta
import pandas as pd
import numpy as np
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from functools import partial
import warnings

# 获取日志器
logger = logging.getLogger(__name__)

# ==========================================
# 文件和目录操作
# ==========================================

def create_dirs(paths: Union[str, List[str]], exist_ok: bool = True) -> List[Path]:
    """
    创建目录（支持批量创建）
    
    Args:
        paths: 目录路径或路径列表
        exist_ok: 如果目录存在是否报错
    
    Returns:
        创建的目录Path对象列表
    """
    if isinstance(paths, str):
        paths = [paths]
    
    created_paths = []
    for path_str in paths:
        path = Path(path_str)
        try:
            path.mkdir(parents=True, exist_ok=exist_ok)
            created_paths.append(path)
            logger.debug(f"创建目录: {path}")
        except Exception as e:
            logger.error(f"创建目录失败 {path}: {e}")
    
    return created_paths

def clean_old_files(directory: str, 
                   days: int = 30,
                   pattern: str = '*',
                   dry_run: bool = False) -> List[str]:
    """
    清理旧文件
    
    Args:
        directory: 目录路径
        days: 保留天数
        pattern: 文件匹配模式
        dry_run: 是否仅模拟运行
    
    Returns:
        删除的文件列表
    """
    dir_path = Path(directory)
    if not dir_path.exists():
        logger.warning(f"目录不存在: {directory}")
        return []
    
    cutoff_time = datetime.now().timestamp() - (days * 24 * 60 * 60)
    deleted_files = []
    
    for file_path in dir_path.glob(pattern):
        if file_path.is_file():
            if file_path.stat().st_mtime < cutoff_time:
                if not dry_run:
                    try:
                        file_path.unlink()
                        logger.info(f"删除旧文件: {file_path}")
                    except Exception as e:
                        logger.error(f"删除文件失败 {file_path}: {e}")
                        continue
                deleted_files.append(str(file_path))
    
    return deleted_files

def save_json(data: Any, filepath: str, indent: int = 2, ensure_ascii: bool = False):
    """
    保存JSON文件
    
    Args:
        data: 要保存的数据
        filepath: 文件路径
        indent: 缩进空格数
        ensure_ascii: 是否确保ASCII编码
    """
    filepath = Path(filepath)
    filepath.parent.mkdir(parents=True, exist_ok=True)
    
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=indent, ensure_ascii=ensure_ascii, default=str)
    
    logger.debug(f"保存JSON文件: {filepath}")

def load_json(filepath: str, default: Any = None) -> Any:
    """
    加载JSON文件
    
    Args:
        filepath: 文件路径
        default: 默认值（文件不存在时返回）
    
    Returns:
        加载的数据
    """
    filepath = Path(filepath)
    if not filepath.exists():
        logger.warning(f"JSON文件不存在: {filepath}")
        return default
    
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"加载JSON文件失败 {filepath}: {e}")
        return default

def save_pickle(data: Any, filepath: str):
    """保存Pickle文件"""
    filepath = Path(filepath)
    filepath.parent.mkdir(parents=True, exist_ok=True)
    
    with open(filepath, 'wb') as f:
        pickle.dump(data, f)
    
    logger.debug(f"保存Pickle文件: {filepath}")

def load_pickle(filepath: str, default: Any = None) -> Any:
    """加载Pickle文件"""
    filepath = Path(filepath)
    if not filepath.exists():
        logger.warning(f"Pickle文件不存在: {filepath}")
        return default
    
    try:
        with open(filepath, 'rb') as f:
            return pickle.load(f)
    except Exception as e:
        logger.error(f"加载Pickle文件失败 {filepath}: {e}")
        return default

# ==========================================
# 数字格式化
# ==========================================

def format_number(value: float, 
                 decimal_places: int = 2,
                 use_commas: bool = True,
                 prefix: str = '',
                 suffix: str = '') -> str:
    """
    格式化数字
    
    Args:
        value: 数值
        decimal_places: 小数位数
        use_commas: 是否使用千分位分隔符
        prefix: 前缀
        suffix: 后缀
    
    Returns:
        格式化的字符串
    """
    if pd.isna(value):
        return 'N/A'
    
    if use_commas:
        formatted = f"{value:,.{decimal_places}f}"
    else:
        formatted = f"{value:.{decimal_places}f}"
    
    return f"{prefix}{formatted}{suffix}"

def format_percentage(value: float, 
                     decimal_places: int = 2,
                     multiply: bool = True) -> str:
    """
    格式化百分比
    
    Args:
        value: 数值
        decimal_places: 小数位数
        multiply: 是否乘以100
    
    Returns:
        格式化的百分比字符串
    """
    if pd.isna(value):
        return 'N/A'
    
    if multiply:
        value = value * 100
    
    return f"{value:.{decimal_places}f}%"

def format_large_number(value: float, precision: int = 2) -> str:
    """
    格式化大数字（使用K/M/B等单位）
    
    Args:
        value: 数值
        precision: 精度
    
    Returns:
        格式化的字符串
    """
    if pd.isna(value) or value == 0:
        return '0'
    
    units = ['', 'K', 'M', 'B', 'T']
    unit_index = 0
    abs_value = abs(value)
    
    while abs_value >= 1000 and unit_index < len(units) - 1:
        abs_value /= 1000
        unit_index += 1
    
    if value < 0:
        abs_value = -abs_value
    
    return f"{abs_value:.{precision}f}{units[unit_index]}"

# ==========================================
# 日期处理
# ==========================================

def convert_to_datetime(date_input: Union[str, date, datetime],
                       format: Optional[str] = None) -> datetime:
    """
    转换为datetime对象
    
    Args:
        date_input: 日期输入
        format: 日期格式
    
    Returns:
        datetime对象
    """
    if isinstance(date_input, datetime):
        return date_input
    elif isinstance(date_input, date):
        return datetime.combine(date_input, datetime.min.time())
    elif isinstance(date_input, str):
        if format:
            return datetime.strptime(date_input, format)
        else:
            return pd.to_datetime(date_input)
    else:
        raise ValueError(f"不支持的日期类型: {type(date_input)}")

def calculate_trading_days(start_date: Union[str, datetime],
                          end_date: Union[str, datetime],
                          holidays: Optional[List[str]] = None) -> int:
    """
    计算交易日数量（简化版）
    
    Args:
        start_date: 开始日期
        end_date: 结束日期
        holidays: 节假日列表
    
    Returns:
        交易日数量
    """
    start = convert_to_datetime(start_date)
    end = convert_to_datetime(end_date)
    
    # 生成日期范围
    date_range = pd.date_range(start, end, freq='B')  # B = business days
    
    # 排除节假日
    if holidays:
        holiday_dates = pd.to_datetime(holidays)
        date_range = date_range.difference(holiday_dates)
    
    return len(date_range)

def get_previous_trading_day(date: Union[str, datetime],
                            holidays: Optional[List[str]] = None) -> datetime:
    """获取上一个交易日"""
    current = convert_to_datetime(date)
    
    while True:
        current = current - timedelta(days=1)
        # 跳过周末
        if current.weekday() < 5:  # Monday = 0, Friday = 4
            # 检查是否为节假日
            if holidays and current.strftime('%Y-%m-%d') in holidays:
                continue
            return current

def get_next_trading_day(date: Union[str, datetime],
                         holidays: Optional[List[str]] = None) -> datetime:
    """获取下一个交易日"""
    current = convert_to_datetime(date)
    
    while True:
        current = current + timedelta(days=1)
        # 跳过周末
        if current.weekday() < 5:
            # 检查是否为节假日
            if holidays and current.strftime('%Y-%m-%d') in holidays:
                continue
            return current

# ==========================================
# 股票相关
# ==========================================

def get_stock_name(code: str) -> str:
    """
    获取股票名称（示例函数，实际需要从数据源获取）
    
    Args:
        code: 股票代码
    
    Returns:
        股票名称
    """
    # 这里只是示例，实际应该从数据库或API获取
    stock_names = {
        '000001.SZ': '平安银行',
        '000002.SZ': '万科A',
        '600000.SH': '浦发银行',
        '600036.SH': '招商银行',
        '000858.SZ': '五粮液',
    }
    
    return stock_names.get(code, code)

def parse_stock_code(code: str) -> Dict[str, str]:
    """
    解析股票代码
    
    Args:
        code: 股票代码（如 '000001.SZ'）
    
    Returns:
        包含代码和市场信息的字典
    """
    match = re.match(r'^(\d{6})\.([A-Z]{2})$', code)
    if match:
        return {
            'code': match.group(1),
            'market': match.group(2),
            'full_code': code
        }
    return {'code': code, 'market': '', 'full_code': code}

# ==========================================
# 数据处理
# ==========================================

def chunk_list(lst: List[Any], chunk_size: int) -> List[List[Any]]:
    """
    将列表分块
    
    Args:
        lst: 原始列表
        chunk_size: 块大小
    
    Returns:
        分块后的列表
    """
    return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]

def parallel_process(func: Callable,
                    items: Iterable,
                    max_workers: Optional[int] = None,
                    use_process: bool = False,
                    show_progress: bool = False) -> List[Any]:
    """
    并行处理数据
    
    Args:
        func: 处理函数
        items: 待处理项
        max_workers: 最大工作线程/进程数
        use_process: 是否使用进程池（默认线程池）
        show_progress: 是否显示进度
    
    Returns:
        处理结果列表
    """
    executor_class = ProcessPoolExecutor if use_process else ThreadPoolExecutor
    
    results = []
    with executor_class(max_workers=max_workers) as executor:
        futures = [executor.submit(func, item) for item in items]
        
        for i, future in enumerate(futures):
            try:
                result = future.result()
                results.append(result)
                
                if show_progress and (i + 1) % 10 == 0:
                    logger.info(f"处理进度: {i + 1}/{len(futures)}")
                    
            except Exception as e:
                logger.error(f"并行处理错误: {e}")
                results.append(None)
    
    return results

def safe_divide(numerator: float, 
               denominator: float,
               default: float = 0.0) -> float:
    """
    安全除法
    
    Args:
        numerator: 分子
        denominator: 分母
        default: 默认值（分母为0时返回）
    
    Returns:
        除法结果
    """
    if denominator == 0 or pd.isna(denominator):
        return default
    return numerator / denominator

def moving_average(data: Union[List[float], pd.Series],
                  window: int,
                  min_periods: Optional[int] = None) -> pd.Series:
    """
    计算移动平均
    
    Args:
        data: 数据
        window: 窗口大小
        min_periods: 最小周期数
    
    Returns:
        移动平均序列
    """
    if not isinstance(data, pd.Series):
        data = pd.Series(data)
    
    return data.rolling(window=window, min_periods=min_periods).mean()

def exponential_smoothing(data: Union[List[float], pd.Series],
                         alpha: float = 0.3) -> pd.Series:
    """
    指数平滑
    
    Args:
        data: 数据
        alpha: 平滑系数（0-1）
    
    Returns:
        平滑后的序列
    """
    if not isinstance(data, pd.Series):
        data = pd.Series(data)
    
    return data.ewm(alpha=alpha, adjust=False).mean()

# ==========================================
# 其他工具函数
# ==========================================

def generate_hash(data: Any, algorithm: str = 'md5') -> str:
    """
    生成数据哈希值
    
    Args:
        data: 数据
        algorithm: 哈希算法
    
    Returns:
        哈希字符串
    """
    if algorithm == 'md5':
        hasher = hashlib.md5()
    elif algorithm == 'sha256':
        hasher = hashlib.sha256()
    else:
        raise ValueError(f"不支持的哈希算法: {algorithm}")
    
    # 转换为字节
    if isinstance(data, str):
        data_bytes = data.encode('utf-8')
    else:
        data_bytes = pickle.dumps(data)
    
    hasher.update(data_bytes)
    return hasher.hexdigest()

def retry_on_exception(func: Callable,
                      max_retries: int = 3,
                      delay: float = 1.0,
                      exceptions: tuple = (Exception,)) -> Any:
    """
    异常重试执行
    
    Args:
        func: 要执行的函数
        max_retries: 最大重试次数
        delay: 重试延迟
        exceptions: 要捕获的异常类型
    
    Returns:
        函数执行结果
    """
    import time
    
    for attempt in range(max_retries):
        try:
            return func()
        except exceptions as e:
            if attempt == max_retries - 1:
                raise
            logger.warning(f"执行失败，{delay}秒后重试: {e}")
            time.sleep(delay)

def flatten_dict(d: Dict[str, Any], 
                 parent_key: str = '',
                 sep: str = '.') -> Dict[str, Any]:
    """
    展平嵌套字典
    
    Args:
        d: 嵌套字典
        parent_key: 父键
        sep: 分隔符
    
    Returns:
        展平后的字典
    """
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)

# ==========================================
# 导出接口
# ==========================================

__all__ = [
    # 文件操作
    'create_dirs',
    'clean_old_files',
    'save_json',
    'load_json',
    'save_pickle',
    'load_pickle',
    
    # 数字格式化
    'format_number',
    'format_percentage',
    'format_large_number',
    
    # 日期处理
    'convert_to_datetime',
    'calculate_trading_days',
    'get_previous_trading_day',
    'get_next_trading_day',
    
    # 股票相关
    'get_stock_name',
    'parse_stock_code',
    
    # 数据处理
    'chunk_list',
    'parallel_process',
    'safe_divide',
    'moving_average',
    'exponential_smoothing',
    
    # 其他工具
    'generate_hash',
    'retry_on_exception',
    'flatten_dict'
]