#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
装饰器集合模块 - utils/decorators.py
=====================================

提供常用的装饰器，用于性能监控、缓存、重试等功能增强。

主要装饰器:
- ⏱️ timeit: 测量函数执行时间
- 🔄 retry: 自动重试失败的操作
- 💾 cache_result: 缓存函数结果
- ✅ validate_input: 验证输入参数
- 📝 log_execution: 记录函数执行
- 🚦 rate_limit: 限制调用频率
- ⚠️ deprecated: 标记废弃的函数
- ⚡ async_timeit: 异步函数计时

使用示例:
```python
from core.utils import timeit, retry, cache_result

@timeit
@retry(max_attempts=3)
@cache_result(ttl=3600)
def fetch_data(symbol):
    # 获取数据的操作
    return data
```

作者: QuantTrader Team
版本: 1.0.0
更新: 2025-08-29
"""

import functools
import time
import logging
import warnings
import hashlib
import pickle
import asyncio
from typing import Any, Callable, Optional, Dict, Tuple, Union
from datetime import datetime, timedelta
from collections import OrderedDict
from threading import Lock, RLock
import inspect

# 获取日志器
logger = logging.getLogger(__name__)

# ==========================================
# 性能监控装饰器
# ==========================================

def timeit(func: Callable = None, 
          *, 
          log_level: str = 'INFO',
          prefix: str = '',
          include_args: bool = False) -> Callable:
    """
    测量函数执行时间
    
    Args:
        func: 被装饰的函数
        log_level: 日志级别
        prefix: 日志前缀
        include_args: 是否记录参数
    
    Example:
        @timeit
        def process_data(df):
            return df.dropna()
        
        @timeit(log_level='DEBUG', include_args=True)
        def calculate(x, y):
            return x + y
    """
    def decorator(f):
        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            start_time = time.perf_counter()
            
            # 构建日志消息
            func_name = f.__name__
            if prefix:
                func_name = f"{prefix}.{func_name}"
            
            if include_args:
                args_repr = ', '.join(repr(arg) for arg in args)
                kwargs_repr = ', '.join(f"{k}={v!r}" for k, v in kwargs.items())
                all_args = ', '.join(filter(None, [args_repr, kwargs_repr]))
                func_call = f"{func_name}({all_args})"
            else:
                func_call = f"{func_name}()"
            
            try:
                result = f(*args, **kwargs)
                elapsed_time = time.perf_counter() - start_time
                
                # 格式化时间
                if elapsed_time < 0.001:
                    time_str = f"{elapsed_time * 1000000:.2f}μs"
                elif elapsed_time < 1:
                    time_str = f"{elapsed_time * 1000:.2f}ms"
                else:
                    time_str = f"{elapsed_time:.2f}s"
                
                # 记录日志
                log_msg = f"{func_call} 执行时间: {time_str}"
                getattr(logger, log_level.lower())(log_msg)
                
                return result
                
            except Exception as e:
                elapsed_time = time.perf_counter() - start_time
                logger.error(f"{func_call} 执行失败 (耗时: {elapsed_time:.2f}s): {e}")
                raise
        
        return wrapper
    
    if func is None:
        return decorator
    else:
        return decorator(func)

def async_timeit(func: Callable = None,
                *, 
                log_level: str = 'INFO',
                prefix: str = '') -> Callable:
    """
    测量异步函数执行时间
    
    Args:
        func: 被装饰的异步函数
        log_level: 日志级别
        prefix: 日志前缀
    
    Example:
        @async_timeit
        async def fetch_data_async(url):
            async with aiohttp.ClientSession() as session:
                return await session.get(url)
    """
    def decorator(f):
        @functools.wraps(f)
        async def wrapper(*args, **kwargs):
            start_time = time.perf_counter()
            func_name = f"{prefix}.{f.__name__}" if prefix else f.__name__
            
            try:
                result = await f(*args, **kwargs)
                elapsed_time = time.perf_counter() - start_time
                
                # 格式化时间
                if elapsed_time < 0.001:
                    time_str = f"{elapsed_time * 1000000:.2f}μs"
                elif elapsed_time < 1:
                    time_str = f"{elapsed_time * 1000:.2f}ms"
                else:
                    time_str = f"{elapsed_time:.2f}s"
                
                getattr(logger, log_level.lower())(f"{func_name} 异步执行时间: {time_str}")
                return result
                
            except Exception as e:
                elapsed_time = time.perf_counter() - start_time
                logger.error(f"{func_name} 异步执行失败 (耗时: {elapsed_time:.2f}s): {e}")
                raise
        
        return wrapper
    
    if func is None:
        return decorator
    else:
        return decorator(func)

# ==========================================
# 重试装饰器
# ==========================================

def retry(max_attempts: int = 3,
         delay: float = 1.0,
         backoff: float = 2.0,
         exceptions: Tuple[Exception, ...] = (Exception,),
         on_retry: Optional[Callable] = None) -> Callable:
    """
    自动重试失败的操作
    
    Args:
        max_attempts: 最大重试次数
        delay: 初始延迟时间（秒）
        backoff: 延迟时间的倍增因子
        exceptions: 需要重试的异常类型
        on_retry: 重试时的回调函数
    
    Example:
        @retry(max_attempts=3, delay=1, backoff=2)
        def fetch_api_data():
            response = requests.get(url)
            return response.json()
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            attempt = 0
            current_delay = delay
            
            while attempt < max_attempts:
                try:
                    return func(*args, **kwargs)
                    
                except exceptions as e:
                    attempt += 1
                    
                    if attempt >= max_attempts:
                        logger.error(f"{func.__name__} 重试{max_attempts}次后仍失败: {e}")
                        raise
                    
                    logger.warning(f"{func.__name__} 失败，{current_delay:.1f}秒后重试 "
                                 f"(第{attempt}/{max_attempts}次): {e}")
                    
                    if on_retry:
                        on_retry(attempt, e)
                    
                    time.sleep(current_delay)
                    current_delay *= backoff
            
            return None
        
        return wrapper
    
    return decorator

# ==========================================
# 缓存装饰器
# ==========================================

class LRUCache:
    """LRU缓存实现"""
    
    def __init__(self, maxsize: int = 128, ttl: Optional[int] = None):
        self.maxsize = maxsize
        self.ttl = ttl
        self.cache = OrderedDict()
        self.timestamps = {}
        self.lock = RLock()
        self.hits = 0
        self.misses = 0
    
    def get(self, key: str) -> Optional[Any]:
        with self.lock:
            if key not in self.cache:
                self.misses += 1
                return None
            
            # 检查TTL
            if self.ttl:
                if time.time() - self.timestamps[key] > self.ttl:
                    del self.cache[key]
                    del self.timestamps[key]
                    self.misses += 1
                    return None
            
            # 移到末尾（最近使用）
            self.cache.move_to_end(key)
            self.hits += 1
            return self.cache[key]
    
    def set(self, key: str, value: Any):
        with self.lock:
            if key in self.cache:
                self.cache.move_to_end(key)
            else:
                if len(self.cache) >= self.maxsize:
                    # 删除最旧的项
                    oldest = next(iter(self.cache))
                    del self.cache[oldest]
                    if oldest in self.timestamps:
                        del self.timestamps[oldest]
            
            self.cache[key] = value
            if self.ttl:
                self.timestamps[key] = time.time()
    
    def clear(self):
        with self.lock:
            self.cache.clear()
            self.timestamps.clear()
            self.hits = 0
            self.misses = 0
    
    def get_stats(self) -> Dict[str, Any]:
        with self.lock:
            total = self.hits + self.misses
            hit_rate = self.hits / total if total > 0 else 0
            return {
                'size': len(self.cache),
                'maxsize': self.maxsize,
                'hits': self.hits,
                'misses': self.misses,
                'hit_rate': hit_rate
            }

def cache_result(maxsize: int = 128,
                ttl: Optional[int] = None,
                key_func: Optional[Callable] = None) -> Callable:
    """
    缓存函数结果
    
    Args:
        maxsize: 最大缓存数量
        ttl: 缓存过期时间（秒）
        key_func: 自定义缓存键生成函数
    
    Example:
        @cache_result(maxsize=100, ttl=3600)
        def get_stock_data(symbol, date):
            return fetch_from_api(symbol, date)
    """
    cache = LRUCache(maxsize=maxsize, ttl=ttl)
    
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # 生成缓存键
            if key_func:
                cache_key = key_func(*args, **kwargs)
            else:
                # 默认使用参数的哈希值
                key_data = (args, tuple(sorted(kwargs.items())))
                cache_key = hashlib.md5(
                    pickle.dumps(key_data)
                ).hexdigest()
            
            # 尝试从缓存获取
            result = cache.get(cache_key)
            if result is not None:
                logger.debug(f"{func.__name__} 缓存命中: {cache_key[:8]}")
                return result
            
            # 计算结果并缓存
            result = func(*args, **kwargs)
            cache.set(cache_key, result)
            logger.debug(f"{func.__name__} 计算并缓存: {cache_key[:8]}")
            
            return result
        
        # 添加缓存管理方法
        wrapper.cache_clear = cache.clear
        wrapper.cache_stats = cache.get_stats
        
        return wrapper
    
    return decorator

# ==========================================
# 验证装饰器
# ==========================================

def validate_input(**validators) -> Callable:
    """
    验证函数输入参数
    
    Args:
        **validators: 参数名和验证函数的映射
    
    Example:
        @validate_input(
            x=lambda v: v > 0,
            y=lambda v: isinstance(v, (int, float))
        )
        def divide(x, y):
            return x / y
    """
    def decorator(func):
        sig = inspect.signature(func)
        
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # 绑定参数
            bound = sig.bind(*args, **kwargs)
            bound.apply_defaults()
            
            # 验证参数
            for param_name, validator in validators.items():
                if param_name in bound.arguments:
                    value = bound.arguments[param_name]
                    
                    try:
                        if not validator(value):
                            raise ValueError(f"参数 {param_name}={value} 验证失败")
                    except Exception as e:
                        raise ValueError(f"参数 {param_name} 验证错误: {e}")
            
            return func(*args, **kwargs)
        
        return wrapper
    
    return decorator

# ==========================================
# 日志装饰器
# ==========================================

def log_execution(log_level: str = 'INFO',
                 log_args: bool = True,
                 log_result: bool = False,
                 log_exception: bool = True) -> Callable:
    """
    记录函数执行信息
    
    Args:
        log_level: 日志级别
        log_args: 是否记录参数
        log_result: 是否记录返回值
        log_exception: 是否记录异常
    
    Example:
        @log_execution(log_result=True)
        def calculate_return(prices):
            return prices.pct_change()
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            func_name = func.__name__
            log_func = getattr(logger, log_level.lower())
            
            # 记录调用
            if log_args:
                args_repr = ', '.join(repr(arg)[:100] for arg in args)
                kwargs_repr = ', '.join(f"{k}={v!r}"[:100] for k, v in kwargs.items())
                all_args = ', '.join(filter(None, [args_repr, kwargs_repr]))
                log_func(f"调用 {func_name}({all_args})")
            else:
                log_func(f"调用 {func_name}")
            
            try:
                result = func(*args, **kwargs)
                
                if log_result:
                    result_repr = repr(result)[:200]
                    log_func(f"{func_name} 返回: {result_repr}")
                
                return result
                
            except Exception as e:
                if log_exception:
                    logger.exception(f"{func_name} 执行异常: {e}")
                raise
        
        return wrapper
    
    return decorator

# ==========================================
# 限流装饰器
# ==========================================

class RateLimiter:
    """限流器"""
    
    def __init__(self, max_calls: int, period: float):
        self.max_calls = max_calls
        self.period = period
        self.calls = []
        self.lock = Lock()
    
    def __call__(self, func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            with self.lock:
                now = time.time()
                
                # 清理过期的调用记录
                self.calls = [t for t in self.calls if now - t < self.period]
                
                # 检查是否超过限制
                if len(self.calls) >= self.max_calls:
                    sleep_time = self.period - (now - self.calls[0])
                    if sleep_time > 0:
                        logger.warning(f"{func.__name__} 触发限流，等待 {sleep_time:.2f}秒")
                        time.sleep(sleep_time)
                        # 重新清理
                        now = time.time()
                        self.calls = [t for t in self.calls if now - t < self.period]
                
                # 记录本次调用
                self.calls.append(now)
            
            return func(*args, **kwargs)
        
        return wrapper

def rate_limit(max_calls: int, period: float) -> Callable:
    """
    限制函数调用频率
    
    Args:
        max_calls: 时间窗口内最大调用次数
        period: 时间窗口（秒）
    
    Example:
        @rate_limit(max_calls=10, period=60)  # 每分钟最多10次
        def fetch_api_data():
            return requests.get(api_url)
    """
    return RateLimiter(max_calls, period)

# ==========================================
# 废弃警告装饰器
# ==========================================

def deprecated(reason: str = '',
              version: str = '',
              alternative: str = '') -> Callable:
    """
    标记废弃的函数
    
    Args:
        reason: 废弃原因
        version: 废弃版本
        alternative: 替代方案
    
    Example:
        @deprecated(
            reason="使用新的API",
            version="2.0.0",
            alternative="use new_function()"
        )
        def old_function():
            pass
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            msg = f"函数 '{func.__name__}' 已废弃"
            
            if version:
                msg += f" (自版本 {version})"
            if reason:
                msg += f": {reason}"
            if alternative:
                msg += f". 建议使用: {alternative}"
            
            warnings.warn(msg, DeprecationWarning, stacklevel=2)
            logger.warning(msg)
            
            return func(*args, **kwargs)
        
        # 更新文档字符串
        doc = wrapper.__doc__ or ""
        deprecation_note = f"\n\n.. deprecated:: {version}\n   {reason}"
        if alternative:
            deprecation_note += f"\n   使用 :func:`{alternative}` 替代。"
        wrapper.__doc__ = doc + deprecation_note
        
        return wrapper
    
    return decorator

# ==========================================
# 单例装饰器
# ==========================================

def singleton(cls):
    """
    单例模式装饰器
    
    Example:
        @singleton
        class DatabaseConnection:
            def __init__(self):
                self.connection = create_connection()
    """
    instances = {}
    lock = Lock()
    
    @functools.wraps(cls)
    def wrapper(*args, **kwargs):
        if cls not in instances:
            with lock:
                if cls not in instances:
                    instances[cls] = cls(*args, **kwargs)
        return instances[cls]
    
    return wrapper

# ==========================================
# 导出接口
# ==========================================

__all__ = [
    'timeit',
    'async_timeit',
    'retry',
    'cache_result',
    'validate_input',
    'log_execution',
    'rate_limit',
    'deprecated',
    'singleton',
    'LRUCache',
    'RateLimiter'
]