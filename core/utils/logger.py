#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
日志系统模块 - utils/logger.py
================================

提供统一的日志记录和管理功能，支持多种输出格式和目标。

主要功能:
- 🔧 统一的日志配置
- 📝 多级别日志记录
- 📁 文件和控制台输出
- 🔄 日志轮转
- 📊 日志统计分析
- 🎨 彩色控制台输出

使用示例:
```python
from core.utils import get_logger

# 创建日志器
logger = get_logger(__name__)

# 记录不同级别的日志
logger.debug("调试信息")
logger.info("一般信息")
logger.warning("警告信息")
logger.error("错误信息")
logger.critical("严重错误")
```

作者: QuantTrader Team
版本: 1.0.0
更新: 2025-08-29
"""

import logging
import logging.handlers
import sys
import os
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Any, List
from collections import defaultdict
import json
import threading

# ==========================================
# 日志配置
# ==========================================

# 默认配置
DEFAULT_LOG_LEVEL = logging.INFO
DEFAULT_LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
DEFAULT_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'
DEFAULT_LOG_DIR = 'logs'
DEFAULT_MAX_BYTES = 10 * 1024 * 1024  # 10MB
DEFAULT_BACKUP_COUNT = 5

# 彩色输出配置（用于控制台）
COLOR_CODES = {
    'DEBUG': '\033[36m',      # 青色
    'INFO': '\033[32m',       # 绿色
    'WARNING': '\033[33m',    # 黄色
    'ERROR': '\033[31m',      # 红色
    'CRITICAL': '\033[35m',   # 紫色
    'RESET': '\033[0m'        # 重置
}

# 全局日志器缓存
_logger_cache = {}
_logger_lock = threading.Lock()

# 日志统计
_log_stats = defaultdict(lambda: defaultdict(int))

# ==========================================
# 自定义格式化器
# ==========================================

class ColoredFormatter(logging.Formatter):
    """彩色日志格式化器"""
    
    def __init__(self, fmt=None, datefmt=None, use_colors=True):
        super().__init__(fmt, datefmt)
        self.use_colors = use_colors and sys.stdout.isatty()
    
    def format(self, record):
        if self.use_colors:
            levelname = record.levelname
            if levelname in COLOR_CODES:
                record.levelname = f"{COLOR_CODES[levelname]}{levelname}{COLOR_CODES['RESET']}"
                record.msg = f"{COLOR_CODES[levelname]}{record.msg}{COLOR_CODES['RESET']}"
        
        return super().format(record)

class JsonFormatter(logging.Formatter):
    """JSON格式化器"""
    
    def format(self, record):
        log_obj = {
            'timestamp': datetime.utcnow().isoformat(),
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage(),
            'module': record.module,
            'function': record.funcName,
            'line': record.lineno
        }
        
        if record.exc_info:
            log_obj['exception'] = self.formatException(record.exc_info)
        
        return json.dumps(log_obj, ensure_ascii=False)

# ==========================================
# 自定义处理器
# ==========================================

class StatsHandler(logging.Handler):
    """统计日志处理器"""
    
    def emit(self, record):
        """记录日志统计信息"""
        _log_stats[record.name][record.levelname] += 1
        _log_stats['_total'][record.levelname] += 1

# ==========================================
# 日志器类
# ==========================================

class Logger:
    """增强的日志器类"""
    
    def __init__(self, name: str, level: int = DEFAULT_LOG_LEVEL):
        """
        初始化日志器
        
        Args:
            name: 日志器名称
            level: 日志级别
        """
        self.name = name
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)
        self.logger.propagate = False
        
        # 添加统计处理器
        stats_handler = StatsHandler()
        self.logger.addHandler(stats_handler)
        
        # 配置标志
        self._console_configured = False
        self._file_configured = False
    
    def add_console_handler(self, 
                           level: Optional[int] = None,
                           format_str: Optional[str] = None,
                           use_colors: bool = True):
        """
        添加控制台处理器
        
        Args:
            level: 日志级别
            format_str: 格式字符串
            use_colors: 是否使用彩色输出
        """
        if self._console_configured:
            return
        
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(level or self.logger.level)
        
        formatter = ColoredFormatter(
            fmt=format_str or DEFAULT_LOG_FORMAT,
            datefmt=DEFAULT_DATE_FORMAT,
            use_colors=use_colors
        )
        console_handler.setFormatter(formatter)
        
        self.logger.addHandler(console_handler)
        self._console_configured = True
    
    def add_file_handler(self,
                        filename: Optional[str] = None,
                        level: Optional[int] = None,
                        format_str: Optional[str] = None,
                        max_bytes: int = DEFAULT_MAX_BYTES,
                        backup_count: int = DEFAULT_BACKUP_COUNT,
                        use_json: bool = False):
        """
        添加文件处理器
        
        Args:
            filename: 日志文件名
            level: 日志级别
            format_str: 格式字符串
            max_bytes: 单个文件最大字节数
            backup_count: 备份文件数量
            use_json: 是否使用JSON格式
        """
        if self._file_configured:
            return
        
        # 确保日志目录存在
        log_dir = Path(DEFAULT_LOG_DIR)
        log_dir.mkdir(parents=True, exist_ok=True)
        
        # 生成文件名
        if filename is None:
            timestamp = datetime.now().strftime('%Y%m%d')
            filename = log_dir / f"{self.name}_{timestamp}.log"
        else:
            filename = log_dir / filename
        
        # 创建轮转文件处理器
        file_handler = logging.handlers.RotatingFileHandler(
            filename=str(filename),
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding='utf-8'
        )
        file_handler.setLevel(level or self.logger.level)
        
        # 设置格式化器
        if use_json:
            formatter = JsonFormatter()
        else:
            formatter = logging.Formatter(
                fmt=format_str or DEFAULT_LOG_FORMAT,
                datefmt=DEFAULT_DATE_FORMAT
            )
        file_handler.setFormatter(formatter)
        
        self.logger.addHandler(file_handler)
        self._file_configured = True
    
    def set_level(self, level: int):
        """设置日志级别"""
        self.logger.setLevel(level)
    
    # 代理方法
    def debug(self, msg, *args, **kwargs):
        self.logger.debug(msg, *args, **kwargs)
    
    def info(self, msg, *args, **kwargs):
        self.logger.info(msg, *args, **kwargs)
    
    def warning(self, msg, *args, **kwargs):
        self.logger.warning(msg, *args, **kwargs)
    
    def error(self, msg, *args, **kwargs):
        self.logger.error(msg, *args, **kwargs)
    
    def critical(self, msg, *args, **kwargs):
        self.logger.critical(msg, *args, **kwargs)
    
    def exception(self, msg, *args, **kwargs):
        """记录异常信息"""
        self.logger.exception(msg, *args, **kwargs)

# ==========================================
# 工厂函数
# ==========================================

def get_logger(name: str = None, 
               level: int = DEFAULT_LOG_LEVEL,
               console: bool = True,
               file: bool = False,
               **kwargs) -> Logger:
    """
    获取或创建日志器
    
    Args:
        name: 日志器名称，默认使用调用模块名
        level: 日志级别
        console: 是否输出到控制台
        file: 是否输出到文件
        **kwargs: 其他配置参数
    
    Returns:
        Logger: 日志器实例
    """
    if name is None:
        # 获取调用者的模块名
        import inspect
        frame = inspect.currentframe()
        if frame and frame.f_back:
            name = frame.f_back.f_globals.get('__name__', 'quant_framework')
        else:
            name = 'quant_framework'
    
    # 使用缓存
    with _logger_lock:
        if name not in _logger_cache:
            logger = Logger(name, level)
            
            if console:
                logger.add_console_handler(
                    use_colors=kwargs.get('use_colors', True)
                )
            
            if file:
                logger.add_file_handler(
                    filename=kwargs.get('filename'),
                    use_json=kwargs.get('use_json', False)
                )
            
            _logger_cache[name] = logger
        
        return _logger_cache[name]

def setup_logger(name: str = 'quant_framework',
                level: str = 'INFO',
                log_file: bool = True,
                log_dir: str = DEFAULT_LOG_DIR,
                console_colors: bool = True,
                json_format: bool = False) -> Logger:
    """
    设置并配置日志器
    
    Args:
        name: 日志器名称
        level: 日志级别字符串
        log_file: 是否记录到文件
        log_dir: 日志目录
        console_colors: 控制台是否使用彩色
        json_format: 是否使用JSON格式
    
    Returns:
        Logger: 配置好的日志器
    """
    # 转换日志级别
    level_map = {
        'DEBUG': logging.DEBUG,
        'INFO': logging.INFO,
        'WARNING': logging.WARNING,
        'ERROR': logging.ERROR,
        'CRITICAL': logging.CRITICAL
    }
    log_level = level_map.get(level.upper(), logging.INFO)
    
    # 设置全局日志目录
    global DEFAULT_LOG_DIR
    DEFAULT_LOG_DIR = log_dir
    
    # 创建日志器
    logger = get_logger(
        name=name,
        level=log_level,
        console=True,
        file=log_file,
        use_colors=console_colors,
        use_json=json_format
    )
    
    logger.info(f"日志系统初始化完成 - 级别: {level}, 文件: {log_file}")
    
    return logger

# ==========================================
# 辅助函数
# ==========================================

def set_log_level(logger_name: str, level: str):
    """
    设置指定日志器的级别
    
    Args:
        logger_name: 日志器名称
        level: 日志级别
    """
    if logger_name in _logger_cache:
        level_value = getattr(logging, level.upper(), logging.INFO)
        _logger_cache[logger_name].set_level(level_value)

def add_file_handler(logger_name: str, filename: str = None):
    """
    为指定日志器添加文件处理器
    
    Args:
        logger_name: 日志器名称
        filename: 日志文件名
    """
    if logger_name in _logger_cache:
        _logger_cache[logger_name].add_file_handler(filename=filename)

def get_log_stats(logger_name: Optional[str] = None) -> Dict[str, Any]:
    """
    获取日志统计信息
    
    Args:
        logger_name: 日志器名称，None表示获取所有
    
    Returns:
        Dict: 统计信息
    """
    if logger_name:
        return dict(_log_stats.get(logger_name, {}))
    else:
        return {
            'loggers': dict(_log_stats),
            'total': dict(_log_stats.get('_total', {})),
            'logger_count': len(_logger_cache)
        }

def clear_log_stats():
    """清空日志统计"""
    global _log_stats
    _log_stats = defaultdict(lambda: defaultdict(int))

def cleanup_old_logs(days: int = 30):
    """
    清理旧日志文件
    
    Args:
        days: 保留天数
    """
    log_dir = Path(DEFAULT_LOG_DIR)
    if not log_dir.exists():
        return
    
    cutoff_time = datetime.now().timestamp() - (days * 24 * 60 * 60)
    
    for log_file in log_dir.glob("*.log*"):
        if log_file.stat().st_mtime < cutoff_time:
            try:
                log_file.unlink()
                print(f"删除旧日志: {log_file}")
            except Exception as e:
                print(f"删除日志失败 {log_file}: {e}")

# ==========================================
# 模块初始化
# ==========================================

# 创建默认日志器
default_logger = get_logger('quant_framework')

# 导出接口
__all__ = [
    'Logger',
    'get_logger',
    'setup_logger',
    'set_log_level',
    'add_file_handler',
    'get_log_stats',
    'clear_log_stats',
    'cleanup_old_logs',
    'ColoredFormatter',
    'JsonFormatter'
]