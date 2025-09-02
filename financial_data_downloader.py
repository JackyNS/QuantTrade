#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
财务数据下载器
=============

专门下载极速版可用的财务数据
"""

import uqer
import pandas as pd
from pathlib import Path
from datetime import datetime
import time

# 优矿Token
UQER_TOKEN = "68b9922817ae6273137bda7acba81e293582ba347281dfcc056dcb245b23faf3"

class FinancialDataDownloader:
    """财务数据下载器"""
    
    def __init__(self):
        # 初始化uqer客户端
        uqer.Client(token=UQER_TOKEN)
        self.client = uqer
        self.base_path = Path("/Users/jackstudio/QuantTrade/data/financial")
        self.base_path.mkdir(exist_ok=True)
        
        print("💰 财务数据专用下载器")
        print("🎯 目标: 下载极速版财务数据")
        print("=" * 50)
    
    def download_balance_sheet(self):
        """下载资产负债表数据"""
        print("\n📊 下载资产负债表(2018新准则)...")
        
        try:
            result = self.client.DataAPI.FdmtBs2018Get(
                reportType='A',  # 年报
                beginDate='20180101',
                endDate='20241231',
                field='secID,ticker,endDate,totalAssets,totalLiab,totalShrhldrEqty,totalNonCurLiab,totalCurLiab',
                pandas='1'
            )
            
            if isinstance(result, pd.DataFrame) and not result.empty:
                output_file = self.base_path / "balance_sheet_2018.csv"
                result.to_csv(output_file, index=False)
                
                print(f"✅ 资产负债表完成: {len(result)} 条记录")
                print(f"📅 时间范围: {result['endDate'].min()} 至 {result['endDate'].max()}")
                print(f"🏢 公司数量: {result['ticker'].nunique()} 家")
                return True
            else:
                print("❌ 资产负债表无数据")
                return False
                
        except Exception as e:
            print(f"❌ 资产负债表下载失败: {e}")
            return False
    
    def download_derived_data(self):
        """下载财务衍生数据"""
        print("\n📈 下载财务衍生数据...")
        
        try:
            result = self.client.DataAPI.FdmtDerGet(
                reportType='A',  # 年报
                beginDate='20100101',
                endDate='20241231',
                field='secID,ticker,endDate,revenue,netProfit,roe,roa,totalAssets,totalLiab',
                pandas='1'
            )
            
            if isinstance(result, pd.DataFrame) and not result.empty:
                output_file = self.base_path / "financial_derived.csv"
                result.to_csv(output_file, index=False)
                
                print(f"✅ 财务衍生数据完成: {len(result)} 条记录")
                print(f"📅 时间范围: {result['endDate'].min()} 至 {result['endDate'].max()}")
                print(f"🏢 公司数量: {result['ticker'].nunique()} 家")
                return True
            else:
                print("❌ 财务衍生数据无数据")
                return False
                
        except Exception as e:
            print(f"❌ 财务衍生数据下载失败: {e}")
            return False
    
    def download_performance_forecast(self):
        """下载业绩快报"""
        print("\n🎯 下载业绩快报...")
        
        try:
            result = self.client.DataAPI.FdmtEeGet(
                beginDate='20100101',
                endDate='20241231',
                field='secID,ticker,endDate,revenue,netProfit,totalAssets,totalShrhldrEqty',
                pandas='1'
            )
            
            if isinstance(result, pd.DataFrame) and not result.empty:
                output_file = self.base_path / "performance_forecast.csv"
                result.to_csv(output_file, index=False)
                
                print(f"✅ 业绩快报完成: {len(result)} 条记录")
                print(f"📅 时间范围: {result['endDate'].min()} 至 {result['endDate'].max()}")
                print(f"🏢 公司数量: {result['ticker'].nunique()} 家")
                return True
            else:
                print("❌ 业绩快报无数据")
                return False
                
        except Exception as e:
            print(f"❌ 业绩快报下载失败: {e}")
            return False
    
    def run_financial_download(self):
        """执行财务数据下载"""
        start_time = datetime.now()
        success_count = 0
        
        # 下载各类财务数据
        if self.download_balance_sheet():
            success_count += 1
            
        time.sleep(1)
        
        if self.download_derived_data():
            success_count += 1
            
        time.sleep(1)
        
        if self.download_performance_forecast():
            success_count += 1
        
        # 生成报告
        end_time = datetime.now()
        duration = end_time - start_time
        
        print(f"\n🎊 财务数据下载完成!")
        print(f"⏱️ 耗时: {duration}")
        print(f"✅ 成功: {success_count} 个财务数据集")
        
        # 检查文件
        files = list(self.base_path.glob("*.csv"))
        if files:
            print(f"\n📁 已生成文件:")
            total_size = 0
            for file in files:
                size_mb = file.stat().st_size / (1024*1024)
                total_size += size_mb
                print(f"   📄 {file.name}: {size_mb:.1f}MB")
            print(f"💾 总计: {total_size:.1f}MB")

def main():
    downloader = FinancialDataDownloader()
    downloader.run_financial_download()

if __name__ == "__main__":
    main()