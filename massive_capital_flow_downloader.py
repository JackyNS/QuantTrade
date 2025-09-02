#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
大规模资金流向数据下载器
=====================

专门下载全市场股票的资金流向数据
"""

import uqer
import pandas as pd
from pathlib import Path
from datetime import datetime
import time

# 优矿Token
UQER_TOKEN = "68b9922817ae6273137bda7acba81e293582ba347281dfcc056dcb245b23faf3"

class MassiveCapitalFlowDownloader:
    """大规模资金流向下载器"""
    
    def __init__(self):
        uqer.Client(token=UQER_TOKEN)
        self.client = uqer
        self.base_path = Path("/Users/jackstudio/QuantTrade/data/capital_flow")
        self.base_path.mkdir(exist_ok=True)
        
        print("💰 大规模资金流向数据下载器")
        print("🎯 目标: 全市场资金流向数据")
        print("=" * 50)
    
    def download_capital_flow_by_year(self, stocks, year):
        """按年下载资金流向数据"""
        print(f"\n💰 下载 {year} 年资金流向数据...")
        
        batch_size = 30  # 资金流向数据批次要小
        all_data = []
        
        for i in range(0, len(stocks), batch_size):
            batch_stocks = stocks[i:i+batch_size]
            batch_tickers = ','.join([s.split('.')[0] for s in batch_stocks])
            
            print(f"  🔄 批次 {i//batch_size + 1}/{(len(stocks)-1)//batch_size + 1}: {len(batch_stocks)} 只股票")
            
            try:
                result = self.client.DataAPI.MktEquFlowGet(
                    secID='',
                    ticker=batch_tickers,
                    beginDate=f'{year}0101',
                    endDate=f'{year}1231',
                    field='secID,ticker,tradeDate,mainNetFlow,superNetFlow,largeNetFlow,mediumNetFlow,smallNetFlow',
                    pandas='1'
                )
                
                if isinstance(result, pd.DataFrame) and not result.empty:
                    all_data.append(result)
                    print(f"    ✅ 完成: {len(result)} 条记录")
                
                time.sleep(1)  # 资金流向需要更长间隔
                
            except Exception as e:
                print(f"    ❌ 批次失败: {e}")
                time.sleep(2)
                continue
        
        if all_data:
            combined = pd.concat(all_data, ignore_index=True)
            output_file = self.base_path / f"capital_flow_{year}.csv"
            combined.to_csv(output_file, index=False)
            
            print(f"✅ {year}年资金流向完成: {len(combined)} 条记录")
            return len(combined)
        
        return 0
    
    def run_capital_flow_download(self):
        """运行资金流向下载"""
        start_time = datetime.now()
        
        try:
            # 获取股票列表
            result = self.client.DataAPI.EquGet(listStatusCD='L', field='secID', pandas='1')
            stocks = result['secID'].unique().tolist()
            print(f"✅ 获取 {len(stocks)} 只股票")
            
            # 下载近几年的资金流向数据
            years = [2024, 2023, 2022, 2021]
            total_records = 0
            
            for year in years:
                records = self.download_capital_flow_by_year(stocks, year)
                total_records += records
                print(f"📊 累计: {total_records:,} 条资金流向记录")
                time.sleep(3)  # 年度间休息
            
            end_time = datetime.now()
            duration = end_time - start_time
            
            print(f"\n🎊 资金流向下载完成!")
            print(f"⏱️ 耗时: {duration}")
            print(f"💰 总记录: {total_records:,} 条")
            
        except Exception as e:
            print(f"❌ 下载异常: {e}")

def main():
    downloader = MassiveCapitalFlowDownloader()
    downloader.run_capital_flow_download()

if __name__ == "__main__":
    main()