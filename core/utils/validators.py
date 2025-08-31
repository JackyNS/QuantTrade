#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据验证模块 - utils/validators.py
====================================

提供数据验证功能，确保数据质量和参数正确性。

主要功能:
- 📊 DataFrame验证
- 📅 日期范围验证
- 🏷️ 股票代码验证
- 💹 价格数据验证
- ⚙️ 配置验证
- 📈 策略参数验证

使用示例:
```python
from core.utils import validate_dataframe, validate_date_range

# 验证DataFrame
is_valid = validate_dataframe(df, required_columns=['open', 'close'])

# 验证日期范围
is_valid = validate_date_range(start_date, end_date)
```

作者: QuantTrader Team
版本: 1.0.0
更新: 2025-08-29
"""

import re
import logging
from typing import Any, Dict, List, Optional, Tuple, Union, Callable
from datetime import datetime, date, timedelta
import pandas as pd
import numpy as np

# 获取日志器
logger = logging.getLogger(__name__)

# ==========================================
# 常量定义
# ==========================================

# A股股票代码正则表达式
STOCK_CODE_PATTERNS = {
    'A_SHARE': r'^[0-9]{6}\.(SH|SZ|BJ)$',  # A股
    'SH_MAIN': r'^60[0-9]{4}\.SH$',        # 上海主板
    'SH_STAR': r'^68[0-9]{4}\.SH$',        # 科创板
    'SZ_MAIN': r'^00[0-9]{4}\.SZ$',        # 深圳主板
    'SZ_SME': r'^002[0-9]{3}\.SZ$',        # 中小板
    'SZ_GEM': r'^30[0-9]{4}\.SZ$',         # 创业板
    'BJ': r'^[48][0-9]{5}\.BJ$',           # 北交所
}

# 必需的价格数据列
REQUIRED_PRICE_COLUMNS = ['open', 'high', 'low', 'close', 'volume']
OPTIONAL_PRICE_COLUMNS = ['amount', 'turnover', 'pct_change']

# 日期格式
DATE_FORMATS = [
    '%Y-%m-%d',
    '%Y/%m/%d',
    '%Y%m%d',
    '%Y-%m-%d %H:%M:%S',
    '%Y/%m/%d %H:%M:%S'
]

# ==========================================
# DataFrame验证
# ==========================================

def validate_dataframe(df: Any,
                      required_columns: Optional[List[str]] = None,
                      min_rows: int = 1,
                      check_na: bool = True,
                      check_duplicates: bool = False,
                      check_types: Optional[Dict[str, type]] = None) -> Tuple[bool, List[str]]:
    """
    验证DataFrame数据
    
    Args:
        df: 要验证的DataFrame
        required_columns: 必需的列名列表
        min_rows: 最小行数
        check_na: 是否检查缺失值
        check_duplicates: 是否检查重复
        check_types: 列类型检查字典
    
    Returns:
        (is_valid, errors): 验证结果和错误列表
    """
    errors = []
    
    # 检查是否为DataFrame
    if not isinstance(df, pd.DataFrame):
        errors.append(f"数据类型错误: 期望DataFrame，实际{type(df)}")
        return False, errors
    
    # 检查是否为空
    if df.empty:
        errors.append("DataFrame为空")
        return False, errors
    
    # 检查行数
    if len(df) < min_rows:
        errors.append(f"行数不足: 需要至少{min_rows}行，实际{len(df)}行")
    
    # 检查必需列
    if required_columns:
        missing_cols = set(required_columns) - set(df.columns)
        if missing_cols:
            errors.append(f"缺少必需列: {missing_cols}")
    
    # 检查缺失值
    if check_na:
        na_cols = df.columns[df.isna().any()].tolist()
        if na_cols:
            na_info = {col: df[col].isna().sum() for col in na_cols}
            errors.append(f"存在缺失值: {na_info}")
    
    # 检查重复
    if check_duplicates:
        if df.duplicated().any():
            dup_count = df.duplicated().sum()
            errors.append(f"存在{dup_count}行重复数据")
    
    # 检查列类型
    if check_types:
        for col, expected_type in check_types.items():
            if col in df.columns:
                actual_type = df[col].dtype
                if not np.issubdtype(actual_type, expected_type):
                    errors.append(f"列{col}类型错误: 期望{expected_type}，实际{actual_type}")
    
    is_valid = len(errors) == 0
    
    if not is_valid:
        logger.warning(f"DataFrame验证失败: {errors}")
    
    return is_valid, errors

# ==========================================
# 日期验证
# ==========================================

def validate_date_range(start_date: Union[str, datetime, date],
                       end_date: Union[str, datetime, date],
                       max_days: Optional[int] = None,
                       min_days: int = 1,
                       allow_future: bool = False) -> Tuple[bool, List[str]]:
    """
    验证日期范围
    
    Args:
        start_date: 开始日期
        end_date: 结束日期
        max_days: 最大天数限制
        min_days: 最小天数限制
        allow_future: 是否允许未来日期
    
    Returns:
        (is_valid, errors): 验证结果和错误列表
    """
    errors = []
    
    try:
        # 转换为datetime对象
        if isinstance(start_date, str):
            start_dt = pd.to_datetime(start_date)
        elif isinstance(start_date, date):
            start_dt = pd.Timestamp(start_date)
        else:
            start_dt = start_date
        
        if isinstance(end_date, str):
            end_dt = pd.to_datetime(end_date)
        elif isinstance(end_date, date):
            end_dt = pd.Timestamp(end_date)
        else:
            end_dt = end_date
        
    except Exception as e:
        errors.append(f"日期格式错误: {e}")
        return False, errors
    
    # 检查日期顺序
    if start_dt > end_dt:
        errors.append(f"开始日期({start_dt.date()})晚于结束日期({end_dt.date()})")
    
    # 检查未来日期
    if not allow_future:
        today = pd.Timestamp.now().normalize()
        if start_dt > today:
            errors.append(f"开始日期({start_dt.date()})是未来日期")
        if end_dt > today:
            errors.append(f"结束日期({end_dt.date()})是未来日期")
    
    # 检查日期范围
    days_diff = (end_dt - start_dt).days
    
    if days_diff < min_days:
        errors.append(f"日期范围太短: 至少需要{min_days}天，实际{days_diff}天")
    
    if max_days and days_diff > max_days:
        errors.append(f"日期范围太长: 最多{max_days}天，实际{days_diff}天")
    
    is_valid = len(errors) == 0
    
    if not is_valid:
        logger.warning(f"日期范围验证失败: {errors}")
    
    return is_valid, errors

# ==========================================
# 股票代码验证
# ==========================================

def validate_stock_code(code: str,
                       market: Optional[str] = None) -> Tuple[bool, str]:
    """
    验证股票代码
    
    Args:
        code: 股票代码
        market: 市场类型（可选）
    
    Returns:
        (is_valid, market_type): 验证结果和市场类型
    """
    if not isinstance(code, str):
        logger.warning(f"股票代码类型错误: {type(code)}")
        return False, ""
    
    code = code.upper()
    
    # 如果指定了市场，只验证该市场
    if market:
        pattern = STOCK_CODE_PATTERNS.get(market.upper())
        if pattern and re.match(pattern, code):
            return True, market.upper()
        return False, ""
    
    # 检查所有市场
    for market_type, pattern in STOCK_CODE_PATTERNS.items():
        if re.match(pattern, code):
            return True, market_type
    
    logger.warning(f"无效的股票代码: {code}")
    return False, ""

# ==========================================
# 价格数据验证
# ==========================================

def validate_price_data(df: pd.DataFrame,
                       check_ohlc: bool = True,
                       check_volume: bool = True,
                       check_logic: bool = True) -> Tuple[bool, List[str]]:
    """
    验证价格数据
    
    Args:
        df: 价格数据DataFrame
        check_ohlc: 是否检查OHLC关系
        check_volume: 是否检查成交量
        check_logic: 是否检查逻辑关系
    
    Returns:
        (is_valid, errors): 验证结果和错误列表
    """
    errors = []
    
    # 基础DataFrame验证
    is_valid, base_errors = validate_dataframe(
        df, 
        required_columns=REQUIRED_PRICE_COLUMNS if check_volume else ['open', 'high', 'low', 'close']
    )
    
    if not is_valid:
        return False, base_errors
    
    # 检查OHLC关系
    if check_ohlc:
        # High应该是最高价
        invalid_high = df['high'] < df[['open', 'close']].max(axis=1)
        if invalid_high.any():
            errors.append(f"最高价异常: {invalid_high.sum()}行")
        
        # Low应该是最低价
        invalid_low = df['low'] > df[['open', 'close']].min(axis=1)
        if invalid_low.any():
            errors.append(f"最低价异常: {invalid_low.sum()}行")
        
        # High >= Low
        invalid_hl = df['high'] < df['low']
        if invalid_hl.any():
            errors.append(f"高低价关系异常: {invalid_hl.sum()}行")
    
    # 检查成交量
    if check_volume and 'volume' in df.columns:
        # 成交量应该非负
        negative_volume = df['volume'] < 0
        if negative_volume.any():
            errors.append(f"负成交量: {negative_volume.sum()}行")
        
        # 检查成交量为0的天数
        zero_volume = df['volume'] == 0
        if zero_volume.sum() > len(df) * 0.1:  # 超过10%的天数成交量为0
            errors.append(f"成交量为0天数过多: {zero_volume.sum()}天")
    
    # 检查价格逻辑
    if check_logic:
        # 检查价格是否为正
        for col in ['open', 'high', 'low', 'close']:
            if col in df.columns:
                negative_price = df[col] <= 0
                if negative_price.any():
                    errors.append(f"{col}价格非正: {negative_price.sum()}行")
        
        # 检查价格变化是否合理（涨跌停板）
        if 'close' in df.columns:
            pct_change = df['close'].pct_change()
            extreme_change = (pct_change.abs() > 0.2)  # 超过20%的变化
            if extreme_change.any():
                errors.append(f"价格变化异常(>20%): {extreme_change.sum()}次")
    
    is_valid = len(errors) == 0
    
    if not is_valid:
        logger.warning(f"价格数据验证失败: {errors}")
    
    return is_valid, errors

# ==========================================
# 配置验证
# ==========================================

def validate_config(config: Dict[str, Any],
                   required_keys: Optional[List[str]] = None,
                   type_checks: Optional[Dict[str, type]] = None,
                   value_checks: Optional[Dict[str, Callable]] = None) -> Tuple[bool, List[str]]:
    """
    验证配置字典
    
    Args:
        config: 配置字典
        required_keys: 必需的键列表
        type_checks: 类型检查字典
        value_checks: 值检查函数字典
    
    Returns:
        (is_valid, errors): 验证结果和错误列表
    """
    errors = []
    
    if not isinstance(config, dict):
        errors.append(f"配置类型错误: 期望dict，实际{type(config)}")
        return False, errors
    
    # 检查必需键
    if required_keys:
        missing_keys = set(required_keys) - set(config.keys())
        if missing_keys:
            errors.append(f"缺少必需配置: {missing_keys}")
    
    # 检查类型
    if type_checks:
        for key, expected_type in type_checks.items():
            if key in config:
                actual_type = type(config[key])
                if not isinstance(config[key], expected_type):
                    errors.append(f"配置{key}类型错误: 期望{expected_type.__name__}，实际{actual_type.__name__}")
    
    # 检查值
    if value_checks:
        for key, check_func in value_checks.items():
            if key in config:
                try:
                    if not check_func(config[key]):
                        errors.append(f"配置{key}值验证失败: {config[key]}")
                except Exception as e:
                    errors.append(f"配置{key}验证异常: {e}")
    
    is_valid = len(errors) == 0
    
    if not is_valid:
        logger.warning(f"配置验证失败: {errors}")
    
    return is_valid, errors

# ==========================================
# 策略参数验证
# ==========================================

def validate_strategy_params(params: Dict[str, Any],
                            param_ranges: Optional[Dict[str, Tuple[float, float]]] = None,
                            param_types: Optional[Dict[str, type]] = None) -> Tuple[bool, List[str]]:
    """
    验证策略参数
    
    Args:
        params: 策略参数字典
        param_ranges: 参数范围字典 {param: (min, max)}
        param_types: 参数类型字典
    
    Returns:
        (is_valid, errors): 验证结果和错误列表
    """
    errors = []
    
    # 类型检查
    if param_types:
        for param, expected_type in param_types.items():
            if param in params:
                if not isinstance(params[param], expected_type):
                    errors.append(f"参数{param}类型错误: 期望{expected_type.__name__}")
    
    # 范围检查
    if param_ranges:
        for param, (min_val, max_val) in param_ranges.items():
            if param in params:
                value = params[param]
                if value < min_val or value > max_val:
                    errors.append(f"参数{param}超出范围: {value} not in [{min_val}, {max_val}]")
    
    # 特定参数验证
    # 移动平均周期
    for param in ['sma_period', 'ema_period', 'window', 'lookback']:
        if param in params:
            if not isinstance(params[param], int) or params[param] < 1:
                errors.append(f"参数{param}应为正整数: {params[param]}")
    
    # 百分比参数
    for param in ['stop_loss', 'take_profit', 'position_size']:
        if param in params:
            value = params[param]
            if not 0 <= value <= 1:
                errors.append(f"参数{param}应在[0,1]范围内: {value}")
    
    is_valid = len(errors) == 0
    
    if not is_valid:
        logger.warning(f"策略参数验证失败: {errors}")
    
    return is_valid, errors

# ==========================================
# 验证器类
# ==========================================

class DataValidator:
    """数据验证器类"""
    
    def __init__(self, strict: bool = True):
        """
        初始化验证器
        
        Args:
            strict: 是否严格模式
        """
        self.strict = strict
        self.validation_history = []
    
    def validate(self, data: Any, rules: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """
        通用验证方法
        
        Args:
            data: 要验证的数据
            rules: 验证规则字典
        
        Returns:
            (is_valid, errors): 验证结果和错误列表
        """
        errors = []
        
        # DataFrame验证
        if 'dataframe' in rules:
            if isinstance(data, pd.DataFrame):
                is_valid, df_errors = validate_dataframe(
                    data,
                    **rules['dataframe']
                )
                errors.extend(df_errors)
        
        # 日期范围验证
        if 'date_range' in rules:
            date_rules = rules['date_range']
            is_valid, date_errors = validate_date_range(
                date_rules.get('start'),
                date_rules.get('end'),
                **{k: v for k, v in date_rules.items() if k not in ['start', 'end']}
            )
            errors.extend(date_errors)
        
        # 自定义验证函数
        if 'custom' in rules:
            for name, validator_func in rules['custom'].items():
                try:
                    if not validator_func(data):
                        errors.append(f"自定义验证{name}失败")
                except Exception as e:
                    errors.append(f"自定义验证{name}异常: {e}")
        
        is_valid = len(errors) == 0 or (not self.strict and len(errors) < 3)
        
        # 记录验证历史
        self.validation_history.append({
            'timestamp': datetime.now(),
            'is_valid': is_valid,
            'errors': errors
        })
        
        return is_valid, errors
    
    def get_stats(self) -> Dict[str, Any]:
        """获取验证统计"""
        if not self.validation_history:
            return {}
        
        total = len(self.validation_history)
        valid = sum(1 for v in self.validation_history if v['is_valid'])
        
        return {
            'total_validations': total,
            'valid_count': valid,
            'invalid_count': total - valid,
            'success_rate': valid / total if total > 0 else 0,
            'last_validation': self.validation_history[-1]
        }

class ConfigValidator:
    """配置验证器类"""
    
    def __init__(self):
        self.schemas = {}
    
    def register_schema(self, name: str, schema: Dict[str, Any]):
        """注册配置模式"""
        self.schemas[name] = schema
    
    def validate_against_schema(self, config: Dict[str, Any], 
                               schema_name: str) -> Tuple[bool, List[str]]:
        """根据模式验证配置"""
        if schema_name not in self.schemas:
            return False, [f"未找到模式: {schema_name}"]
        
        schema = self.schemas[schema_name]
        
        return validate_config(
            config,
            required_keys=schema.get('required'),
            type_checks=schema.get('types'),
            value_checks=schema.get('validators')
        )

# ==========================================
# 辅助函数
# ==========================================

def is_trading_day(date_str: str) -> bool:
    """判断是否为交易日（简化版）"""
    dt = pd.to_datetime(date_str)
    # 周末不是交易日
    if dt.weekday() >= 5:
        return False
    # TODO: 添加节假日判断
    return True

def validate_portfolio_weights(weights: Dict[str, float],
                              tolerance: float = 0.01) -> Tuple[bool, str]:
    """
    验证投资组合权重
    
    Args:
        weights: 权重字典
        tolerance: 容差
    
    Returns:
        (is_valid, message): 验证结果和消息
    """
    # 检查权重是否为正
    negative_weights = {k: v for k, v in weights.items() if v < 0}
    if negative_weights:
        return False, f"负权重: {negative_weights}"
    
    # 检查权重和是否为1
    total_weight = sum(weights.values())
    if abs(total_weight - 1.0) > tolerance:
        return False, f"权重和不为1: {total_weight}"
    
    return True, "权重验证通过"

# ==========================================
# 导出接口
# ==========================================

__all__ = [
    'validate_dataframe',
    'validate_date_range',
    'validate_stock_code',
    'validate_price_data',
    'validate_config',
    'validate_strategy_params',
    'validate_portfolio_weights',
    'is_trading_day',
    'DataValidator',
    'ConfigValidator',
    'STOCK_CODE_PATTERNS',
    'REQUIRED_PRICE_COLUMNS',
    'DATE_FORMATS'
]