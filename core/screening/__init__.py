#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
股票筛选模块初始化文件 - screening/__init__.py
=============================================

完整的股票筛选模块，包含多维度筛选和因子排序功能
"""

import sys
import warnings
from typing import Dict, List, Optional, Any
from datetime import datetime

warnings.filterwarnings('ignore')

print("🚀 量化交易框架 - 股票筛选模块初始化")
print("=" * 50)

# 导入所有组件
try:
    from .stock_screener import StockScreener, CustomScreener
    from .fundamental_filter import FundamentalFilter
    from .technical_filter import TechnicalFilter
    from .factor_ranker import FactorRanker
    
    __all__ = [
        'StockScreener',
        'CustomScreener',
        'FundamentalFilter',
        'TechnicalFilter', 
        'FactorRanker',
        'create_screener',
        'create_filter',
        'create_ranker',
        'get_module_status'
    ]
    
    print("✅ 所有筛选组件导入成功")
    
except ImportError as e:
    print(f"⚠️ 部分组件导入失败: {e}")

# 工厂函数
def create_filter(filter_type: str = 'fundamental', config: Optional[Dict] = None):
    """创建筛选器"""
    if filter_type == 'fundamental':
        return FundamentalFilter(config)
    elif filter_type == 'technical':
        return TechnicalFilter(config)
    else:
        return StockScreener(config)

def create_ranker(config: Optional[Dict] = None):
    """创建排序器"""
    return FactorRanker(config)

def get_module_status() -> Dict[str, Any]:
    """获取模块状态"""
    status = {
        'module': 'screening',
        'version': '1.0.0',
        'components': {
            'stock_screener': False,
            'fundamental_filter': False,
            'technical_filter': False,
            'factor_ranker': False
        }
    }
    
    # 检查各组件
    try:
        from .stock_screener import StockScreener
        status['components']['stock_screener'] = True
    except: pass
    
    try:
        from .fundamental_filter import FundamentalFilter
        status['components']['fundamental_filter'] = True
    except: pass
    
    try:
        from .technical_filter import TechnicalFilter
        status['components']['technical_filter'] = True
    except: pass
    
    try:
        from .factor_ranker import FactorRanker
        status['components']['factor_ranker'] = True
    except: pass
    
    # 计算完成度
    total = len(status['components'])
    completed = sum(status['components'].values())
    status['completion_rate'] = f"{(completed/total)*100:.0f}%"
    
    return status

# 显示状态
status = get_module_status()
print(f"\n📊 筛选模块完成度: {status['completion_rate']}")
for comp, available in status['components'].items():
    print(f"  {'✅' if available else '❌'} {comp}")

print("=" * 50)