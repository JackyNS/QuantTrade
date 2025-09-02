#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
核心数据下载器 - 快速版本
========================

专注下载最关键的数据，分阶段完成
"""

import uqer
import pandas as pd
from pathlib import Path
from datetime import datetime
import time
import logging

# 优矿Token
UQER_TOKEN = "68b9922817ae6273137bda7acba81e293582ba347281dfcc056dcb245b23faf3"

class CoreDataDownloader:
    """核心数据下载器"""
    
    def __init__(self, max_stocks=500):
        # 初始化uqer客户端
        uqer.Client(token=UQER_TOKEN)
        self.client = uqer
        self.base_path = Path("/Users/jackstudio/QuantTrade/data")
        self.base_path.mkdir(exist_ok=True)
        self.max_stocks = max_stocks
        
        # 设置日志
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
        print(f"🎯 核心数据快速下载器")
        print(f"📊 范围: 前{max_stocks}只股票, 2010-2025年")
        print("=" * 50)
    
    def get_stock_list(self):
        """获取股票列表"""
        try:
            result = self.client.DataAPI.EquGet(
                listStatusCD='L',
                field='secID,ticker',
                pandas='1'
            )
            
            if isinstance(result, pd.DataFrame) and not result.empty:
                stocks = result['secID'].unique().tolist()
                print(f"✅ 获取到 {len(stocks)} 只股票")
                return stocks[:self.max_stocks]  # 限制数量
            return []
        except Exception as e:
            print(f"❌ 获取股票列表失败: {e}")
            return []
    
    def download_daily_data(self, stocks):
        """分批下载日行情数据"""
        print("\n📈 下载日行情数据...")
        
        output_dir = self.base_path / "daily"
        output_dir.mkdir(exist_ok=True)
        
        batch_size = 50  # 减少批次大小
        all_data = []
        
        for i in range(0, len(stocks), batch_size):
            batch_stocks = stocks[i:i+batch_size]
            batch_tickers = ','.join([s.split('.')[0] for s in batch_stocks])
            
            print(f"  🔄 批次 {i//batch_size + 1}/{(len(stocks)-1)//batch_size + 1}: {len(batch_stocks)} 只股票")
            
            try:
                # 分年下载减少数据量
                for year in [2024, 2023, 2022]:
                    result = self.client.DataAPI.MktEqudGet(
                        secID='',
                        ticker=batch_tickers,
                        beginDate=f'{year}0101',
                        endDate=f'{year}1231',
                        field='secID,ticker,tradeDate,openPrice,highestPrice,lowestPrice,closePrice,turnoverVol,marketValue',
                        pandas='1'
                    )
                    
                    if isinstance(result, pd.DataFrame) and not result.empty:
                        all_data.append(result)
                        print(f"    ✅ {year}年: {len(result)} 条记录")
                    
                    time.sleep(0.3)  # 防止频率限制
                
            except Exception as e:
                print(f"    ❌ 批次失败: {e}")
                continue
                
            time.sleep(1)  # 批次间停顿
        
        # 保存数据
        if all_data:
            combined_data = pd.concat(all_data, ignore_index=True)
            output_file = output_dir / "daily_core_2022_2024.csv"
            combined_data.to_csv(output_file, index=False)
            print(f"✅ 日行情完成: {len(combined_data)} 条记录")
            return True
        
        return False
    
    def download_capital_flow(self, stocks):
        """下载资金流向数据"""
        print("\n💰 下载资金流向数据...")
        
        output_dir = self.base_path / "capital_flow"
        output_dir.mkdir(exist_ok=True)
        
        batch_size = 30  # 更小批次
        all_data = []
        
        for i in range(0, len(stocks), batch_size):
            batch_stocks = stocks[i:i+batch_size]
            batch_tickers = ','.join([s.split('.')[0] for s in batch_stocks])
            
            print(f"  🔄 批次 {i//batch_size + 1}: {len(batch_stocks)} 只股票")
            
            try:
                result = self.client.DataAPI.MktEquFlowGet(
                    secID='',
                    ticker=batch_tickers,
                    beginDate='20240101',
                    endDate='20241231',
                    field='secID,ticker,tradeDate,mainNetFlow,superNetFlow,largeNetFlow',
                    pandas='1'
                )
                
                if isinstance(result, pd.DataFrame) and not result.empty:
                    all_data.append(result)
                    print(f"    ✅ 完成: {len(result)} 条记录")
                
                time.sleep(1)
                
            except Exception as e:
                print(f"    ❌ 批次失败: {e}")
                continue
        
        if all_data:
            combined_data = pd.concat(all_data, ignore_index=True)
            output_file = output_dir / "capital_flow_2024.csv"
            combined_data.to_csv(output_file, index=False)
            print(f"✅ 资金流向完成: {len(combined_data)} 条记录")
            return True
        
        return False
    
    def run_core_download(self):
        """运行核心数据下载"""
        start_time = datetime.now()
        
        try:
            # 1. 获取股票列表
            stocks = self.get_stock_list()
            if not stocks:
                print("❌ 无法获取股票列表")
                return
            
            success_count = 0
            
            # 2. 下载日行情数据
            if self.download_daily_data(stocks):
                success_count += 1
            
            # 3. 下载资金流向数据
            if self.download_capital_flow(stocks):
                success_count += 1
            
            # 4. 报告结果
            end_time = datetime.now()
            duration = end_time - start_time
            
            print(f"\n🎊 下载完成!")
            print(f"⏱️ 耗时: {duration}")
            print(f"✅ 成功: {success_count} 个数据集")
            print(f"📊 股票: {len(stocks)} 只")
            
        except Exception as e:
            print(f"❌ 下载异常: {e}")

def main():
    downloader = CoreDataDownloader(max_stocks=500)
    downloader.run_core_download()

if __name__ == "__main__":
    main()