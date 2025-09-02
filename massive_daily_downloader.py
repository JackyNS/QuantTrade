#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
大规模日行情数据下载器
===================

专门下载全市场股票的日行情数据 (2010-2025年)
采用分年分批策略，确保数据完整性
"""

import uqer
import pandas as pd
from pathlib import Path
from datetime import datetime
import time

# 优矿Token
UQER_TOKEN = "68b9922817ae6273137bda7acba81e293582ba347281dfcc056dcb245b23faf3"

class MassiveDailyDownloader:
    """大规模日行情下载器"""
    
    def __init__(self):
        uqer.Client(token=UQER_TOKEN)
        self.client = uqer
        self.base_path = Path("/Users/jackstudio/QuantTrade/data/daily")
        self.base_path.mkdir(exist_ok=True)
        
        print("📈 大规模日行情数据下载器")
        print("🎯 目标: 全市场股票日行情 (2010-2025)")
        print("=" * 50)
    
    def get_all_stocks(self):
        """获取全部股票"""
        try:
            result = self.client.DataAPI.EquGet(
                listStatusCD='L',
                field='secID,ticker',
                pandas='1'
            )
            stocks = result['secID'].unique().tolist()
            print(f"✅ 获取 {len(stocks)} 只股票")
            return stocks
        except Exception as e:
            print(f"❌ 获取股票失败: {e}")
            return []
    
    def download_yearly_data(self, stocks, year):
        """按年下载日行情数据"""
        print(f"\n📊 下载 {year} 年数据...")
        
        batch_size = 50  # 小批次确保稳定性
        all_data = []
        
        for i in range(0, len(stocks), batch_size):
            batch_stocks = stocks[i:i+batch_size]
            batch_tickers = ','.join([s.split('.')[0] for s in batch_stocks])
            
            print(f"  🔄 批次 {i//batch_size + 1}/{(len(stocks)-1)//batch_size + 1}: {len(batch_stocks)} 只股票")
            
            try:
                result = self.client.DataAPI.MktEqudGet(
                    secID='',
                    ticker=batch_tickers,
                    beginDate=f'{year}0101',
                    endDate=f'{year}1231',
                    field='secID,ticker,tradeDate,preClosePrice,openPrice,highestPrice,lowestPrice,closePrice,turnoverVol,marketValue',
                    pandas='1'
                )
                
                if isinstance(result, pd.DataFrame) and not result.empty:
                    all_data.append(result)
                    print(f"    ✅ 完成: {len(result)} 条记录")
                else:
                    print(f"    ⚠️ 无数据")
                
                time.sleep(0.5)  # 防止频率限制
                
            except Exception as e:
                print(f"    ❌ 批次失败: {e}")
                time.sleep(1)
                continue
        
        # 保存年度数据
        if all_data:
            combined = pd.concat(all_data, ignore_index=True)
            output_file = self.base_path / f"daily_{year}.csv"
            combined.to_csv(output_file, index=False)
            
            print(f"✅ {year}年完成: {len(combined)} 条记录")
            return len(combined)
        
        return 0
    
    def run_massive_download(self):
        """运行大规模下载"""
        start_time = datetime.now()
        
        # 获取股票列表
        stocks = self.get_all_stocks()
        if not stocks:
            return
        
        # 分年下载 (从最近开始)
        years = [2024, 2023, 2022, 2021, 2020, 2019, 2018, 2017, 2016, 2015]
        total_records = 0
        
        print(f"\n🚀 开始大规模下载: {len(stocks)} 只股票 x {len(years)} 年")
        print("⏱️ 预计时间: 3-4小时")
        
        for year in years:
            year_records = self.download_yearly_data(stocks, year)
            total_records += year_records
            
            print(f"📊 累计下载: {total_records:,} 条记录")
            time.sleep(2)  # 年度间休息
        
        # 最终报告
        end_time = datetime.now()
        duration = end_time - start_time
        
        print(f"\n🎊 大规模下载完成!")
        print(f"⏱️ 总耗时: {duration}")
        print(f"📈 总记录: {total_records:,} 条")
        
        # 显示文件
        files = list(self.base_path.glob("daily_*.csv"))
        total_size = sum(f.stat().st_size for f in files) / (1024*1024*1024)
        print(f"📁 生成文件: {len(files)} 个")
        print(f"💾 总大小: {total_size:.2f} GB")

def main():
    downloader = MassiveDailyDownloader()
    downloader.run_massive_download()

if __name__ == "__main__":
    main()