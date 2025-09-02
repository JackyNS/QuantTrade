#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
简化版十周线上穿百周线策略测试
"""

import sys
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# 确保能导入TA-Lib
try:
    import talib
    print("✅ TA-Lib 可用")
except ImportError:
    print("❌ TA-Lib 不可用")
    talib = None

def calculate_weekly_ma(prices, period=20):
    """计算周线移动平均"""
    if isinstance(prices, list):
        prices = np.array(prices)
    
    if len(prices) < period:
        return np.array([])
    
    if talib:
        return talib.MA(prices, timeperiod=period)
    else:
        # 简单移动平均
        return pd.Series(prices).rolling(period).mean().values

def generate_test_data(days=2500):
    """生成测试数据"""
    print(f"📊 生成 {days} 天测试数据...")
    
    # 生成模拟价格数据
    np.random.seed(42)
    base_price = 100
    returns = np.random.normal(0.001, 0.02, days)  # 日收益率
    
    prices = [base_price]
    for ret in returns[1:]:
        prices.append(prices[-1] * (1 + ret))
    
    # 创建日期索引
    dates = pd.date_range('2023-01-01', periods=days, freq='D')
    
    # 创建DataFrame
    data = pd.DataFrame({
        'tradeDate': dates,
        'closePrice': prices,
        'openPrice': [p * (1 + np.random.normal(0, 0.005)) for p in prices],
        'highPrice': [p * (1 + abs(np.random.normal(0, 0.01))) for p in prices],
        'lowPrice': [p * (1 - abs(np.random.normal(0, 0.01))) for p in prices],
        'volume': np.random.randint(1000000, 10000000, days)
    })
    
    return data

def test_ma_crossover_strategy():
    """测试MA交叉策略"""
    print("🚀 测试十周线上穿百周线策略")
    print("=" * 50)
    
    # 1. 生成测试数据
    test_data = generate_test_data(2500)
    print(f"✅ 测试数据: {len(test_data)} 条记录")
    print(f"   日期范围: {test_data['tradeDate'].min()} 至 {test_data['tradeDate'].max()}")
    print(f"   价格范围: {test_data['closePrice'].min():.2f} - {test_data['closePrice'].max():.2f}")
    
    # 2. 转换为周线数据
    print("\n📈 转换为周线数据...")
    weekly_data = test_data.set_index('tradeDate').resample('W')['closePrice'].last().dropna()
    print(f"✅ 周线数据: {len(weekly_data)} 条记录")
    
    # 3. 计算移动平均线
    print("\n🧮 计算移动平均线...")
    
    # 10周移动平均
    ma10 = calculate_weekly_ma(weekly_data.values, 10)
    ma10_series = pd.Series(ma10, index=weekly_data.index[:len(ma10)])
    
    # 100周移动平均  
    ma100 = calculate_weekly_ma(weekly_data.values, 100)
    ma100_series = pd.Series(ma100, index=weekly_data.index[:len(ma100)])
    
    print(f"✅ MA10: {len(ma10_series)} 个有效值")
    print(f"✅ MA100: {len(ma100_series)} 个有效值")
    
    if len(ma100_series) == 0:
        print("❌ 数据不足，无法计算100周移动平均线")
        return
    
    # 4. 计算交叉信号
    print("\n🎯 计算交叉信号...")
    
    # 找到共同的日期索引
    common_dates = ma10_series.index.intersection(ma100_series.index)
    if len(common_dates) < 2:
        print("❌ 有效数据不足，无法计算交叉信号")
        return
    
    # 对齐数据
    ma10_aligned = ma10_series.loc[common_dates]
    ma100_aligned = ma100_series.loc[common_dates]
    
    # 计算位置关系
    position = (ma10_aligned > ma100_aligned).astype(int)
    crossover = position.diff()
    
    # 找到交叉点
    golden_cross = crossover > 0  # 黄金交叉 (10周线上穿100周线)
    death_cross = crossover < 0   # 死叉 (10周线下穿100周线)
    
    golden_dates = golden_cross[golden_cross].index
    death_dates = death_cross[death_cross].index
    
    print(f"✅ 黄金交叉: {len(golden_dates)} 次")
    print(f"✅ 死叉: {len(death_dates)} 次")
    
    # 5. 显示详细结果
    print(f"\n📋 策略分析结果:")
    print(f"   📊 分析周期: {len(common_dates)} 周")
    print(f"   🟢 黄金交叉次数: {len(golden_dates)}")
    print(f"   🔴 死叉次数: {len(death_dates)}")
    print(f"   📈 当前状态: {'多头' if position.iloc[-1] > 0 else '空头'}")
    
    if len(golden_dates) > 0:
        print(f"\n🌟 最近黄金交叉:")
        for i, date in enumerate(golden_dates[-3:]):  # 显示最近3次
            price = weekly_data.loc[date] if date in weekly_data.index else 'N/A'
            print(f"   {i+1}. {date.strftime('%Y-%m-%d')}: 价格 {price:.2f}")
    
    if len(death_dates) > 0:
        print(f"\n💀 最近死叉:")
        for i, date in enumerate(death_dates[-3:]):  # 显示最近3次
            price = weekly_data.loc[date] if date in weekly_data.index else 'N/A'
            print(f"   {i+1}. {date.strftime('%Y-%m-%d')}: 价格 {price:.2f}")
    
    # 6. 简单的收益分析
    print(f"\n💰 简单收益分析:")
    if len(golden_dates) > 0 and len(death_dates) > 0:
        total_trades = min(len(golden_dates), len(death_dates))
        if total_trades > 0:
            returns = []
            for i in range(total_trades):
                if i < len(golden_dates) and i < len(death_dates):
                    buy_date = golden_dates[i]
                    sell_date = death_dates[death_dates > buy_date]
                    if len(sell_date) > 0:
                        sell_date = sell_date[0]
                        buy_price = weekly_data.loc[buy_date] if buy_date in weekly_data.index else None
                        sell_price = weekly_data.loc[sell_date] if sell_date in weekly_data.index else None
                        if buy_price and sell_price:
                            ret = (sell_price - buy_price) / buy_price
                            returns.append(ret)
            
            if returns:
                avg_return = np.mean(returns) * 100
                win_rate = len([r for r in returns if r > 0]) / len(returns) * 100
                print(f"   📊 完整交易次数: {len(returns)}")
                print(f"   📈 平均收益率: {avg_return:.2f}%")
                print(f"   🎯 胜率: {win_rate:.1f}%")
    
    print(f"\n🎉 策略测试完成!")
    
    return {
        'total_weeks': len(common_dates),
        'golden_crosses': len(golden_dates),
        'death_crosses': len(death_dates),
        'current_position': '多头' if position.iloc[-1] > 0 else '空头',
        'golden_cross_dates': golden_dates.tolist() if len(golden_dates) > 0 else [],
        'death_cross_dates': death_dates.tolist() if len(death_dates) > 0 else []
    }

if __name__ == "__main__":
    result = test_ma_crossover_strategy()