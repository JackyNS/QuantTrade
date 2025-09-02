#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
创建样本股票数据用于十周线上穿百周线策略测试
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os

def create_sample_stock_data(stock_code, start_date='2020-01-01', days=2000):
    """
    创建单只股票的样本数据
    
    Args:
        stock_code: 股票代码
        start_date: 开始日期
        days: 生成天数
        
    Returns:
        DataFrame: 股票数据
    """
    print(f"📊 生成股票 {stock_code} 的样本数据 ({days} 天)...")
    
    # 设置随机种子以确保可重现性
    np.random.seed(hash(stock_code) % (2**32))
    
    # 生成日期序列
    start_date = pd.to_datetime(start_date)
    dates = pd.date_range(start_date, periods=days, freq='D')
    
    # 过滤工作日 (简化版，实际应该过滤节假日)
    business_dates = dates[dates.dayofweek < 5]
    
    # 生成价格数据
    base_price = np.random.uniform(10, 100)  # 基础价格在10-100之间
    
    # 生成收益率序列 (带趋势)
    trend = np.random.choice([-0.0002, 0.0001, 0.0003], p=[0.3, 0.4, 0.3])  # 随机趋势
    volatility = np.random.uniform(0.015, 0.035)  # 波动率
    
    returns = []
    for i in range(len(business_dates)):
        # 基础收益率
        daily_return = np.random.normal(trend, volatility)
        
        # 添加一些特殊模式
        if i > 100:  # 前100天后开始添加模式
            # 10%概率出现大涨
            if np.random.random() < 0.05:
                daily_return += np.random.uniform(0.03, 0.08)
            # 10%概率出现大跌  
            elif np.random.random() < 0.05:
                daily_return -= np.random.uniform(0.03, 0.08)
        
        returns.append(daily_return)
    
    # 计算累积价格
    prices = [base_price]
    for ret in returns:
        prices.append(prices[-1] * (1 + ret))
    
    prices = prices[:len(business_dates)]  # 确保长度匹配
    
    # 生成OHLC数据
    data_list = []
    for i, (date, close_price) in enumerate(zip(business_dates, prices)):
        # 生成当日波动
        daily_volatility = abs(np.random.normal(0, 0.01))
        high_price = close_price * (1 + daily_volatility * np.random.uniform(0.5, 1.5))
        low_price = close_price * (1 - daily_volatility * np.random.uniform(0.5, 1.5))
        
        # 开盘价基于前一日收盘价
        if i == 0:
            open_price = close_price * (1 + np.random.normal(0, 0.005))
        else:
            open_price = prices[i-1] * (1 + np.random.normal(0, 0.01))
        
        # 确保OHLC关系合理
        high_price = max(high_price, open_price, close_price)
        low_price = min(low_price, open_price, close_price)
        
        # 生成成交量 (价格上涨时成交量偏大)
        price_change = (close_price - open_price) / open_price if open_price > 0 else 0
        volume_base = np.random.uniform(1000000, 10000000)
        if price_change > 0.02:  # 大涨时放量
            volume_multiplier = np.random.uniform(1.5, 3.0)
        elif price_change < -0.02:  # 大跌时也放量
            volume_multiplier = np.random.uniform(1.3, 2.5)
        else:
            volume_multiplier = np.random.uniform(0.7, 1.5)
        
        volume = int(volume_base * volume_multiplier)
        
        data_list.append({
            'tradeDate': date,
            'openPrice': round(open_price, 2),
            'highPrice': round(high_price, 2),
            'lowPrice': round(low_price, 2),
            'closePrice': round(close_price, 2),
            'volume': volume,
            'ticker': stock_code
        })
    
    df = pd.DataFrame(data_list)
    
    print(f"   ✅ 生成完成: {len(df)} 条记录")
    print(f"   📅 日期范围: {df['tradeDate'].min().date()} - {df['tradeDate'].max().date()}")
    print(f"   💰 价格范围: {df['closePrice'].min():.2f} - {df['closePrice'].max():.2f}")
    
    return df

def create_sample_dataset():
    """创建样本股票数据集"""
    print("🚀 创建十周线上穿百周线策略测试数据集")
    print("=" * 60)
    
    # 股票列表 (使用真实A股代码)
    stock_list = [
        '000001',  # 平安银行
        '000002',  # 万科A  
        '000858',  # 五粮液
        '600036',  # 招商银行
        '600519',  # 贵州茅台
        '002415',  # 海康威视
        '000725',  # 京东方A
        '600887',  # 伊利股份
        '000063',  # 中兴通讯
        '600276'   # 恒瑞医药
    ]
    
    # 创建输出目录
    output_dir = Path("sample_stock_data")
    output_dir.mkdir(exist_ok=True)
    
    all_results = []
    
    # 为每只股票生成数据
    for i, stock_code in enumerate(stock_list, 1):
        print(f"\n📈 [{i}/{len(stock_list)}] 处理股票: {stock_code}")
        
        try:
            # 生成股票数据
            stock_data = create_sample_stock_data(
                stock_code=stock_code,
                start_date='2020-01-01', 
                days=2000  # 约5.5年的数据
            )
            
            # 保存为CSV
            csv_filename = output_dir / f"{stock_code}_daily.csv"
            stock_data.to_csv(csv_filename, index=False)
            print(f"   💾 保存到: {csv_filename}")
            
            all_results.append({
                'stock_code': stock_code,
                'file_path': str(csv_filename),
                'records': len(stock_data),
                'date_range': f"{stock_data['tradeDate'].min().date()} - {stock_data['tradeDate'].max().date()}",
                'price_range': f"{stock_data['closePrice'].min():.2f} - {stock_data['closePrice'].max():.2f}"
            })
            
        except Exception as e:
            print(f"   ❌ 生成失败: {e}")
            continue
    
    # 生成汇总信息
    summary_file = output_dir / "dataset_summary.json"
    summary_data = {
        'creation_time': datetime.now().isoformat(),
        'total_stocks': len(all_results),
        'dataset_info': {
            'purpose': '十周线上穿百周线策略测试',
            'data_type': '模拟股票日线数据',
            'time_range': '2020-2025年',
            'fields': ['tradeDate', 'openPrice', 'highPrice', 'lowPrice', 'closePrice', 'volume', 'ticker']
        },
        'stocks': all_results
    }
    
    import json
    with open(summary_file, 'w', encoding='utf-8') as f:
        json.dump(summary_data, f, ensure_ascii=False, indent=2)
    
    print(f"\n📋 数据集创建完成:")
    print(f"   📁 输出目录: {output_dir}")
    print(f"   📊 股票数量: {len(all_results)}")
    print(f"   📄 汇总文件: {summary_file}")
    
    return output_dir, all_results

if __name__ == "__main__":
    from pathlib import Path
    output_dir, results = create_sample_dataset()