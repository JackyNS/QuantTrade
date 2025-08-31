#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
自定义异常模块 - utils/exceptions.py
=====================================

定义框架专用的异常类，提供详细的错误信息和上下文。

异常层次结构:
```
QuantFrameworkError (基础异常)
├── ConfigError (配置错误)
│   ├── MissingConfigError
│   └── InvalidConfigError
├── DataError (数据错误)
│   ├── InsufficientDataError
│   ├── DataQualityError
│   └── DataLoadError
├── StrategyError (策略错误)
│   ├── InvalidParameterError
│   └── SignalGenerationError
├── BacktestError (回测错误)
│   ├── BacktestConfigError
│   └── PerformanceCalculationError
├── ValidationError (验证错误)
└── APIError (API错误)
    ├── RateLimitError
    └── AuthenticationError
```

使用示例:
```python
from core.utils import DataError, InvalidParameterError

# 抛出数据错误
if len(df) < 100:
    raise InsufficientDataError(
        "数据不足",
        required=100,
        actual=len(df)
    )

# 抛出参数错误
if window < 1:
    raise InvalidParameterError(
        "window",
        window,
        "窗口大小必须大于0"
    )
```

作者: QuantTrader Team
版本: 1.0.0
更新: 2025-08-29
"""

import traceback
from typing import Any, Dict, Optional, List
from datetime import datetime

# ==========================================
# 基础异常类
# ==========================================

class QuantFrameworkError(Exception):
    """
    量化框架基础异常类
    
    所有自定义异常的基类，提供标准的错误信息格式和上下文管理。
    """
    
    def __init__(self, 
                 message: str,
                 error_code: Optional[str] = None,
                 context: Optional[Dict[str, Any]] = None,
                 suggestions: Optional[List[str]] = None):
        """
        初始化异常
        
        Args:
            message: 错误消息
            error_code: 错误代码
            context: 错误上下文信息
            suggestions: 修复建议
        """
        self.message = message
        self.error_code = error_code or self.__class__.__name__
        self.context = context or {}
        self.suggestions = suggestions or []
        self.timestamp = datetime.now()
        self.traceback = traceback.format_exc()
        
        # 构建完整的错误消息
        full_message = self._build_full_message()
        super().__init__(full_message)
    
    def _build_full_message(self) -> str:
        """构建完整的错误消息"""
        parts = [
            f"[{self.error_code}] {self.message}"
        ]
        
        if self.context:
            context_str = ", ".join(f"{k}={v}" for k, v in self.context.items())
            parts.append(f"Context: {context_str}")
        
        if self.suggestions:
            suggestions_str = "; ".join(self.suggestions)
            parts.append(f"Suggestions: {suggestions_str}")
        
        return " | ".join(parts)
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            'error_code': self.error_code,
            'message': self.message,
            'context': self.context,
            'suggestions': self.suggestions,
            'timestamp': self.timestamp.isoformat(),
            'traceback': self.traceback
        }

# ==========================================
# 配置相关异常
# ==========================================

class ConfigError(QuantFrameworkError):
    """配置错误基类"""
    
    def __init__(self, message: str, config_key: Optional[str] = None, **kwargs):
        context = kwargs.pop('context', {})
        if config_key:
            context['config_key'] = config_key
        super().__init__(message, context=context, **kwargs)

class MissingConfigError(ConfigError):
    """缺少配置项错误"""
    
    def __init__(self, config_key: str, **kwargs):
        message = f"缺少必需的配置项: {config_key}"
        suggestions = [
            f"请在配置文件中添加 {config_key}",
            "或设置相应的环境变量"
        ]
        super().__init__(
            message,
            config_key=config_key,
            suggestions=suggestions,
            **kwargs
        )

class InvalidConfigError(ConfigError):
    """无效配置错误"""
    
    def __init__(self, config_key: str, value: Any, reason: str, **kwargs):
        message = f"配置项 {config_key} 的值无效: {value}"
        context = {
            'config_key': config_key,
            'invalid_value': value,
            'reason': reason
        }
        super().__init__(message, context=context, **kwargs)

# ==========================================
# 数据相关异常
# ==========================================

class DataError(QuantFrameworkError):
    """数据错误基类"""
    pass

class InsufficientDataError(DataError):
    """数据不足错误"""
    
    def __init__(self, 
                 message: str,
                 required: int,
                 actual: int,
                 data_type: str = "rows",
                 **kwargs):
        context = {
            'required': required,
            'actual': actual,
            'data_type': data_type,
            'shortage': required - actual
        }
        
        full_message = f"{message}: 需要{required}{data_type}，实际只有{actual}{data_type}"
        
        suggestions = [
            f"请提供至少{required}{data_type}的数据",
            "或调整策略参数以适应较少的数据"
        ]
        
        super().__init__(
            full_message,
            context=context,
            suggestions=suggestions,
            **kwargs
        )

class DataQualityError(DataError):
    """数据质量错误"""
    
    def __init__(self,
                 message: str,
                 issues: List[str],
                 affected_columns: Optional[List[str]] = None,
                 **kwargs):
        context = {
            'issues': issues,
            'issue_count': len(issues)
        }
        
        if affected_columns:
            context['affected_columns'] = affected_columns
        
        suggestions = [
            "检查数据源质量",
            "运行数据清洗程序",
            "考虑使用备用数据源"
        ]
        
        super().__init__(
            message,
            context=context,
            suggestions=suggestions,
            **kwargs
        )

class DataLoadError(DataError):
    """数据加载错误"""
    
    def __init__(self,
                 source: str,
                 reason: str,
                 **kwargs):
        message = f"从 {source} 加载数据失败: {reason}"
        context = {
            'source': source,
            'reason': reason
        }
        
        suggestions = [
            "检查数据源是否可用",
            "验证访问权限",
            "检查网络连接"
        ]
        
        super().__init__(
            message,
            context=context,
            suggestions=suggestions,
            **kwargs
        )

# ==========================================
# 策略相关异常
# ==========================================

class StrategyError(QuantFrameworkError):
    """策略错误基类"""
    pass

class InvalidParameterError(StrategyError):
    """无效参数错误"""
    
    def __init__(self,
                 param_name: str,
                 param_value: Any,
                 reason: str,
                 valid_range: Optional[str] = None,
                 **kwargs):
        message = f"参数 {param_name} 的值 {param_value} 无效: {reason}"
        
        context = {
            'param_name': param_name,
            'param_value': param_value,
            'reason': reason
        }
        
        suggestions = []
        if valid_range:
            context['valid_range'] = valid_range
            suggestions.append(f"请将 {param_name} 设置在 {valid_range} 范围内")
        
        super().__init__(
            message,
            context=context,
            suggestions=suggestions,
            **kwargs
        )

class SignalGenerationError(StrategyError):
    """信号生成错误"""
    
    def __init__(self,
                 strategy_name: str,
                 reason: str,
                 **kwargs):
        message = f"策略 {strategy_name} 生成信号失败: {reason}"
        
        context = {
            'strategy_name': strategy_name,
            'reason': reason
        }
        
        suggestions = [
            "检查策略参数设置",
            "验证输入数据完整性",
            "查看策略日志获取详细信息"
        ]
        
        super().__init__(
            message,
            context=context,
            suggestions=suggestions,
            **kwargs
        )

# ==========================================
# 回测相关异常
# ==========================================

class BacktestError(QuantFrameworkError):
    """回测错误基类"""
    pass

class BacktestConfigError(BacktestError):
    """回测配置错误"""
    
    def __init__(self,
                 config_issue: str,
                 **kwargs):
        message = f"回测配置错误: {config_issue}"
        
        suggestions = [
            "检查回测起止日期",
            "验证初始资金设置",
            "确认手续费参数"
        ]
        
        super().__init__(
            message,
            suggestions=suggestions,
            **kwargs
        )

class PerformanceCalculationError(BacktestError):
    """绩效计算错误"""
    
    def __init__(self,
                 metric: str,
                 reason: str,
                 **kwargs):
        message = f"计算 {metric} 指标失败: {reason}"
        
        context = {
            'metric': metric,
            'reason': reason
        }
        
        super().__init__(
            message,
            context=context,
            **kwargs
        )

# ==========================================
# 验证相关异常
# ==========================================

class ValidationError(QuantFrameworkError):
    """验证错误"""
    
    def __init__(self,
                 validation_type: str,
                 errors: List[str],
                 **kwargs):
        message = f"{validation_type} 验证失败"
        
        context = {
            'validation_type': validation_type,
            'error_count': len(errors),
            'errors': errors
        }
        
        suggestions = [
            "检查输入数据格式",
            "查看验证错误详情",
            "参考文档了解数据要求"
        ]
        
        super().__init__(
            message,
            context=context,
            suggestions=suggestions,
            **kwargs
        )

# ==========================================
# API相关异常
# ==========================================

class APIError(QuantFrameworkError):
    """API错误基类"""
    
    def __init__(self,
                 api_name: str,
                 status_code: Optional[int] = None,
                 **kwargs):
        context = kwargs.pop('context', {})
        context['api_name'] = api_name
        if status_code:
            context['status_code'] = status_code
        
        super().__init__(
            context=context,
            **kwargs
        )

class RateLimitError(APIError):
    """API限流错误"""
    
    def __init__(self,
                 api_name: str,
                 retry_after: Optional[int] = None,
                 **kwargs):
        message = f"API {api_name} 请求频率超限"
        
        context = {}
        if retry_after:
            context['retry_after'] = retry_after
            message += f"，请在 {retry_after} 秒后重试"
        
        suggestions = [
            "降低请求频率",
            "使用批量请求接口",
            "考虑升级API套餐"
        ]
        
        super().__init__(
            message,
            api_name=api_name,
            context=context,
            suggestions=suggestions,
            **kwargs
        )

class AuthenticationError(APIError):
    """API认证错误"""
    
    def __init__(self,
                 api_name: str,
                 **kwargs):
        message = f"API {api_name} 认证失败"
        
        suggestions = [
            "检查API密钥是否正确",
            "验证密钥是否过期",
            "确认账户权限"
        ]
        
        super().__init__(
            message,
            api_name=api_name,
            suggestions=suggestions,
            **kwargs
        )

# ==========================================
# 异常处理工具
# ==========================================

class ExceptionHandler:
    """异常处理器"""
    
    def __init__(self, logger=None):
        self.logger = logger
        self.exception_history = []
    
    def handle(self, exception: Exception, reraise: bool = True) -> Optional[Dict[str, Any]]:
        """
        处理异常
        
        Args:
            exception: 异常对象
            reraise: 是否重新抛出异常
        
        Returns:
            异常信息字典
        """
        # 记录异常
        exception_info = {
            'type': type(exception).__name__,
            'message': str(exception),
            'timestamp': datetime.now()
        }
        
        # 如果是框架异常，获取详细信息
        if isinstance(exception, QuantFrameworkError):
            exception_info.update(exception.to_dict())
        
        # 记录到历史
        self.exception_history.append(exception_info)
        
        # 记录日志
        if self.logger:
            self.logger.error(f"捕获异常: {exception_info}")
        
        # 重新抛出
        if reraise:
            raise exception
        
        return exception_info
    
    def get_statistics(self) -> Dict[str, Any]:
        """获取异常统计"""
        if not self.exception_history:
            return {}
        
        # 统计各类异常数量
        exception_counts = {}
        for exc in self.exception_history:
            exc_type = exc['type']
            exception_counts[exc_type] = exception_counts.get(exc_type, 0) + 1
        
        return {
            'total_exceptions': len(self.exception_history),
            'exception_counts': exception_counts,
            'last_exception': self.exception_history[-1] if self.exception_history else None
        }

# ==========================================
# 导出接口
# ==========================================

__all__ = [
    # 基础异常
    'QuantFrameworkError',
    
    # 配置异常
    'ConfigError',
    'MissingConfigError',
    'InvalidConfigError',
    
    # 数据异常
    'DataError',
    'InsufficientDataError',
    'DataQualityError',
    'DataLoadError',
    
    # 策略异常
    'StrategyError',
    'InvalidParameterError',
    'SignalGenerationError',
    
    # 回测异常
    'BacktestError',
    'BacktestConfigError',
    'PerformanceCalculationError',
    
    # 验证异常
    'ValidationError',
    
    # API异常
    'APIError',
    'RateLimitError',
    'AuthenticationError',
    
    # 工具类
    'ExceptionHandler'
]