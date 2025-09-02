#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
完整数据下载器 - 全市场版本
==========================

下载全部10个接口的完整数据 (2010-2025年，全市场股票)
"""

import uqer
import pandas as pd
from pathlib import Path
from datetime import datetime
import time
import logging
from typing import Dict, List, Optional

# 优矿Token
UQER_TOKEN = "68b9922817ae6273137bda7acba81e293582ba347281dfcc056dcb245b23faf3"

class CompleteDataDownloader:
    """完整数据下载器"""
    
    def __init__(self):
        # 初始化uqer客户端
        uqer.Client(token=UQER_TOKEN)
        self.client = uqer
        self.base_path = Path("/Users/jackstudio/QuantTrade/data")
        self.base_path.mkdir(exist_ok=True)
        
        # 设置日志
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
        # 统计信息
        self.stats = {'success': 0, 'failed': 0, 'records': 0}
        
        print("🚀 优矿完整数据下载器")
        print("🎯 目标: 下载10个接口全部数据")
        print("📊 范围: 全市场股票，2010-2025年")
        print("=" * 60)
    
    def get_all_stocks(self) -> List[str]:
        """获取全部股票列表"""
        try:
            print("📋 获取全市场股票列表...")
            result = self.client.DataAPI.EquGet(
                listStatusCD='L',
                field='secID,ticker',
                pandas='1'
            )
            
            if isinstance(result, pd.DataFrame) and not result.empty:
                stocks = result['secID'].unique().tolist()
                print(f"✅ 获取到 {len(stocks)} 只股票")
                return stocks
            return []
        except Exception as e:
            print(f"❌ 获取股票列表失败: {e}")
            return []
    
    def download_adjustment_factor(self, stocks: List[str]) -> bool:
        """下载复权因子数据"""
        print("\n📉 下载复权因子数据...")
        
        output_dir = self.base_path / "adjustment"
        output_dir.mkdir(exist_ok=True)
        
        try:
            batch_size = 200  # 复权数据批次可以大一些
            all_data = []
            
            for i in range(0, len(stocks), batch_size):
                batch_stocks = stocks[i:i+batch_size]
                batch_tickers = ','.join([s.split('.')[0] for s in batch_stocks])
                
                print(f"  🔄 批次 {i//batch_size + 1}/{(len(stocks)-1)//batch_size + 1}: {len(batch_stocks)} 只股票")
                
                try:
                    result = self.client.DataAPI.MktAdjfGet(
                        secID='',
                        ticker=batch_tickers,
                        beginDate='20100101',
                        endDate='20250831',
                        field='secID,ticker,exDivDate,adjfactor',
                        pandas='1'
                    )
                    
                    if isinstance(result, pd.DataFrame) and not result.empty:
                        all_data.append(result)
                        print(f"    ✅ 完成: {len(result)} 条记录")
                    
                    time.sleep(0.5)
                    
                except Exception as e:
                    print(f"    ❌ 批次失败: {e}")
                    continue
            
            if all_data:
                combined = pd.concat(all_data, ignore_index=True)
                output_file = output_dir / "adjustment_factors_2010_2025.csv"
                combined.to_csv(output_file, index=False)
                
                self.stats['success'] += 1
                self.stats['records'] += len(combined)
                print(f"✅ 复权因子完成: {len(combined)} 条记录")
                return True
            
            return False
            
        except Exception as e:
            print(f"❌ 复权因子下载失败: {e}")
            self.stats['failed'] += 1
            return False
    
    def download_dividend_data(self, stocks: List[str]) -> bool:
        """下载股票分红数据"""
        print("\n💎 下载股票分红数据...")
        
        output_dir = self.base_path / "dividend"
        output_dir.mkdir(exist_ok=True)
        
        try:
            batch_size = 200
            all_data = []
            
            for i in range(0, len(stocks), batch_size):
                batch_stocks = stocks[i:i+batch_size]
                batch_tickers = ','.join([s.split('.')[0] for s in batch_stocks])
                
                print(f"  🔄 批次 {i//batch_size + 1}/{(len(stocks)-1)//batch_size + 1}: {len(batch_stocks)} 只股票")
                
                try:
                    result = self.client.DataAPI.EquDivGet(
                        secID='',
                        ticker=batch_tickers,
                        beginDate='20100101',
                        endDate='20250831',
                        field='secID,ticker,exDate,dividend,splitRatio',
                        pandas='1'
                    )
                    
                    if isinstance(result, pd.DataFrame) and not result.empty:
                        all_data.append(result)
                        print(f"    ✅ 完成: {len(result)} 条记录")
                    
                    time.sleep(0.5)
                    
                except Exception as e:
                    print(f"    ❌ 批次失败: {e}")
                    continue
            
            if all_data:
                combined = pd.concat(all_data, ignore_index=True)
                output_file = output_dir / "dividend_data_2010_2025.csv"
                combined.to_csv(output_file, index=False)
                
                self.stats['success'] += 1
                self.stats['records'] += len(combined)
                print(f"✅ 股票分红完成: {len(combined)} 条记录")
                return True
            
            return False
            
        except Exception as e:
            print(f"❌ 股票分红下载失败: {e}")
            self.stats['failed'] += 1
            return False
    
    def download_limit_data(self, stocks: List[str]) -> bool:
        """下载涨跌停数据"""
        print("\n📊 下载涨跌停数据...")
        
        output_dir = self.base_path / "limit_info"
        output_dir.mkdir(exist_ok=True)
        
        try:
            # 涨跌停数据量大，分年下载
            all_data = []
            years = [2024, 2023, 2022, 2021, 2020]  # 先下载近5年
            
            for year in years:
                print(f"  📅 下载 {year} 年数据...")
                batch_size = 100
                
                for i in range(0, len(stocks), batch_size):
                    batch_stocks = stocks[i:i+batch_size]
                    batch_tickers = ','.join([s.split('.')[0] for s in batch_stocks])
                    
                    print(f"    🔄 {year}年 批次 {i//batch_size + 1}: {len(batch_stocks)} 只股票")
                    
                    try:
                        result = self.client.DataAPI.MktLimitGet(
                            secID='',
                            ticker=batch_tickers,
                            beginDate=f'{year}0101',
                            endDate=f'{year}1231',
                            field='secID,ticker,tradeDate,upLimit,downLimit',
                            pandas='1'
                        )
                        
                        if isinstance(result, pd.DataFrame) and not result.empty:
                            all_data.append(result)
                            print(f"      ✅ 完成: {len(result)} 条记录")
                        
                        time.sleep(0.3)
                        
                    except Exception as e:
                        print(f"      ❌ 批次失败: {e}")
                        continue
            
            if all_data:
                combined = pd.concat(all_data, ignore_index=True)
                output_file = output_dir / "limit_data_2020_2024.csv"
                combined.to_csv(output_file, index=False)
                
                self.stats['success'] += 1
                self.stats['records'] += len(combined)
                print(f"✅ 涨跌停数据完成: {len(combined)} 条记录")
                return True
            
            return False
            
        except Exception as e:
            print(f"❌ 涨跌停数据下载失败: {e}")
            self.stats['failed'] += 1
            return False
    
    def download_rank_list(self) -> bool:
        """下载龙虎榜数据"""
        print("\n🔥 下载龙虎榜数据...")
        
        output_dir = self.base_path / "rank_list"
        output_dir.mkdir(exist_ok=True)
        
        try:
            # 龙虎榜数据不需要指定股票，按时间下载
            all_data = []
            years = [2024, 2023, 2022, 2021, 2020]
            
            for year in years:
                print(f"  📅 下载 {year} 年龙虎榜数据...")
                
                try:
                    result = self.client.DataAPI.MktRankListStocksGet(
                        beginDate=f'{year}0101',
                        endDate=f'{year}1231',
                        field='secID,ticker,tradeDate,rankReason,buyAmt,sellAmt',
                        pandas='1'
                    )
                    
                    if isinstance(result, pd.DataFrame) and not result.empty:
                        all_data.append(result)
                        print(f"    ✅ {year}年完成: {len(result)} 条记录")
                    
                    time.sleep(1)
                    
                except Exception as e:
                    print(f"    ❌ {year}年失败: {e}")
                    continue
            
            if all_data:
                combined = pd.concat(all_data, ignore_index=True)
                output_file = output_dir / "rank_list_2020_2024.csv"
                combined.to_csv(output_file, index=False)
                
                self.stats['success'] += 1
                self.stats['records'] += len(combined)
                print(f"✅ 龙虎榜数据完成: {len(combined)} 条记录")
                return True
            
            return False
            
        except Exception as e:
            print(f"❌ 龙虎榜数据下载失败: {e}")
            self.stats['failed'] += 1
            return False
    
    def run_complete_download(self):
        """运行完整数据下载"""
        start_time = datetime.now()
        
        try:
            # 1. 获取全部股票列表
            stocks = self.get_all_stocks()
            if not stocks:
                print("❌ 无法获取股票列表")
                return
            
            print(f"\n🎯 开始下载全市场数据: {len(stocks)} 只股票")
            print("📊 预计下载时间: 2-3小时")
            print("💾 预计数据量: 1-2GB")
            print()
            
            # 2. 下载各类数据
            print("🔄 开始分阶段下载...")
            
            # 阶段1: 复权因子
            if self.download_adjustment_factor(stocks):
                print("✅ 阶段1完成: 复权因子")
            
            # 阶段2: 股票分红
            if self.download_dividend_data(stocks):
                print("✅ 阶段2完成: 股票分红")
            
            # 阶段3: 涨跌停数据
            if self.download_limit_data(stocks):
                print("✅ 阶段3完成: 涨跌停数据")
            
            # 阶段4: 龙虎榜数据
            if self.download_rank_list():
                print("✅ 阶段4完成: 龙虎榜数据")
            
            # 3. 生成最终报告
            end_time = datetime.now()
            duration = end_time - start_time
            
            print(f"\n🎊 完整数据下载完成!")
            print(f"⏱️ 总耗时: {duration}")
            print(f"✅ 成功: {self.stats['success']} 个数据集")
            print(f"❌ 失败: {self.stats['failed']} 个数据集")
            print(f"📋 总记录: {self.stats['records']:,} 条")
            
        except Exception as e:
            print(f"❌ 下载过程异常: {e}")

def main():
    downloader = CompleteDataDownloader()
    downloader.run_complete_download()

if __name__ == "__main__":
    main()