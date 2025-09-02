#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
快速股票统计
快速统计所有股票数量和基本分布
"""

import pandas as pd
from pathlib import Path
from collections import defaultdict
import warnings
warnings.filterwarnings('ignore')

def quick_stock_analysis():
    """快速分析股票分布"""
    print("⚡ 快速股票统计...")
    
    daily_path = Path("/Users/jackstudio/QuantTrade/data/priority_download/market_data/daily")
    batch_files = list(daily_path.glob("*.csv"))
    
    all_stocks = set()
    year_stocks = defaultdict(set)
    
    # 快速扫描前50个文件获取概况
    print(f"📄 快速扫描 {min(50, len(batch_files))} 个批次文件...")
    
    for file_path in batch_files[:50]:
        try:
            year = file_path.stem.split('_')[0]
            df = pd.read_csv(file_path)
            stocks_in_file = set(df['secID'].unique())
            
            all_stocks.update(stocks_in_file)
            year_stocks[year].update(stocks_in_file)
            
            print(f"   {file_path.name}: {len(stocks_in_file)} 只股票")
            
        except Exception as e:
            continue
    
    print(f"\\n📊 快速统计结果:")
    print(f"   📈 总股票数(样本): {len(all_stocks)}")
    
    # 按年份显示股票数量
    for year in sorted(year_stocks.keys()):
        print(f"   {year}: {len(year_stocks[year])} 只股票")
    
    # 显示样本股票
    print(f"\\n📋 样本股票 (前20只):")
    for i, stock in enumerate(list(all_stocks)[:20], 1):
        print(f"   {i:2}. {stock}")
    
    return len(all_stocks)

if __name__ == "__main__":
    quick_stock_analysis()