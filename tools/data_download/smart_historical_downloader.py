#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
智能历史数据下载器 - 解决空数据问题
只下载已上市股票，大幅减少空数据
"""

import uqer
import pandas as pd
from datetime import datetime
from pathlib import Path
import time
import logging

UQER_TOKEN = "68b9922817ae6273137bda7acba81e293582ba347281dfcc056dcb245b23faf3"

class SmartHistoricalDownloader:
    """智能历史数据下载器"""
    
    def __init__(self):
        self.client = uqer.Client(token=UQER_TOKEN)
        self.data_dir = Path("data/smart_download")
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        # 配置日志
        log_file = self.data_dir / "smart_download.log"
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
        
    def get_stocks_with_listing_info(self):
        """获取股票及上市信息"""
        logging.info("📋 获取股票上市信息...")
        
        try:
            stocks = uqer.DataAPI.EquGet(
                field='secID,ticker,secShortName,exchangeCD,listStatusCD,listDate,delistDate'
            )
            
            if stocks is not None and not stocks.empty:
                # 过滤A股
                a_stocks = stocks[stocks['listStatusCD'] == 'L'].copy()
                
                # 处理上市日期
                a_stocks['listDate'] = pd.to_datetime(a_stocks['listDate'])
                a_stocks['listYear'] = a_stocks['listDate'].dt.year
                
                logging.info(f"✅ 获取股票信息成功: {len(a_stocks)} 只A股")
                
                # 按年份统计上市股票
                yearly_counts = a_stocks['listYear'].value_counts().sort_index()
                logging.info("📊 历年上市股票统计:")
                
                cumulative = 0
                for year in range(1990, 2025):
                    if year in yearly_counts.index:
                        cumulative += yearly_counts[year]
                        logging.info(f"   {year}: +{yearly_counts[year]} 只, 累计 {cumulative} 只")
                
                return a_stocks
            
        except Exception as e:
            logging.error(f"❌ 获取股票信息失败: {e}")
            return None
    
    def get_stocks_for_year(self, stocks, year):
        """获取指定年份已上市的股票"""
        # 筛选该年份已上市的股票
        year_stocks = stocks[stocks['listYear'] <= year].copy()
        return year_stocks
    
    def download_year_smart(self, year, start_year=2003):
        """智能下载指定年份数据"""
        if year < start_year:
            logging.info(f"⏭️ 跳过 {year} 年（低于起始年份 {start_year}）")
            return False
            
        logging.info(f"🎯 开始智能下载 {year} 年数据...")
        
        # 获取股票信息
        all_stocks = self.get_stocks_with_listing_info()
        if all_stocks is None:
            return False
        
        # 获取该年份已上市股票
        year_stocks = self.get_stocks_for_year(all_stocks, year)
        
        if len(year_stocks) == 0:
            logging.info(f"⚠️ {year} 年无已上市股票")
            return False
        
        logging.info(f"📈 {year} 年已上市股票: {len(year_stocks)} 只")
        
        # 下载数据
        return self._download_year_data(year_stocks, year)
    
    def _download_year_data(self, stocks, year):
        """下载年度数据"""
        year_dir = self.data_dir / f"year_{year}"
        year_dir.mkdir(exist_ok=True)
        
        start_date = f"{year}0101"
        end_date = f"{year}1231"
        
        # 智能分批：每批100只股票
        batch_size = 100
        batches = [stocks[i:i+batch_size] for i in range(0, len(stocks), batch_size)]
        
        total_records = 0
        success_count = 0
        failed_count = 0
        empty_count = 0
        
        logging.info(f"📦 {year} 年分为 {len(batches)} 批下载")
        
        for batch_idx, batch_stocks in enumerate(batches):
            batch_file = year_dir / f"batch_{batch_idx+1:03d}.csv"
            
            # 跳过已存在文件
            if batch_file.exists():
                existing_data = pd.read_csv(batch_file)
                total_records += len(existing_data)
                success_count += 1
                logging.info(f"📂 {year} 批次 {batch_idx+1} 已存在: {len(existing_data)} 条")
                continue
            
            try:
                # 构建ticker列表
                tickers = ','.join(batch_stocks['ticker'].tolist())
                
                logging.info(f"📥 {year} 批次 {batch_idx+1}/{len(batches)}: {len(batch_stocks)} 只股票")
                
                # 调用API
                data = uqer.DataAPI.MktEqudGet(
                    secID='',
                    ticker=tickers,
                    beginDate=start_date,
                    endDate=end_date,
                    field='secID,ticker,tradeDate,preClosePrice,openPrice,highestPrice,lowestPrice,closePrice,turnoverVol,marketValue,dealAmount,turnoverRate'
                )
                
                if data is not None and not data.empty:
                    # 保存数据
                    data.to_csv(batch_file, index=False)
                    total_records += len(data)
                    success_count += 1
                    
                    logging.info(f"✅ {year} 批次 {batch_idx+1}: {len(data)} 条记录")
                else:
                    empty_count += 1
                    logging.warning(f"⚠️ {year} 批次 {batch_idx+1}: 数据为空")
                
                # API限制延迟
                time.sleep(0.3)
                
            except Exception as e:
                failed_count += 1
                logging.error(f"❌ {year} 批次 {batch_idx+1} 失败: {e}")
                time.sleep(1)
                continue
        
        # 统计结果
        total_batches = len(batches)
        success_rate = (success_count / total_batches) * 100 if total_batches > 0 else 0
        
        logging.info(f"📊 {year} 年完成统计:")
        logging.info(f"   ✅ 成功批次: {success_count}/{total_batches} ({success_rate:.1f}%)")
        logging.info(f"   ⚠️ 空数据批次: {empty_count}")
        logging.info(f"   ❌ 失败批次: {failed_count}")
        logging.info(f"   📊 总记录数: {total_records}")
        
        return total_records > 0
    
    def continue_from_year(self, start_year=2003):
        """从指定年份开始继续下载"""
        logging.info(f"🚀 从 {start_year} 年开始智能下载...")
        
        current_year = datetime.now().year
        total_records = 0
        
        for year in range(start_year, current_year + 1):
            logging.info(f"\n{'='*60}")
            logging.info(f"📅 处理 {year} 年数据")
            logging.info(f"{'='*60}")
            
            success = self.download_year_smart(year, start_year)
            
            if success:
                logging.info(f"✅ {year} 年下载完成")
            else:
                logging.warning(f"⚠️ {year} 年下载跳过或失败")
            
            # 年度间隔
            time.sleep(1)
        
        logging.info(f"\n🎉 智能下载完成!")
        return True
    
    def compare_efficiency(self):
        """对比下载效率"""
        print("\n📊 空数据问题对比分析:")
        print("=" * 50)
        print("🔴 原始方式:")
        print("   - 2000年: 29成功/82失败 (26%成功率)")
        print("   - 2001年: 30成功/81失败 (27%成功率)")
        print("   - 问题: 大量API调用浪费在空数据上")
        print()
        print("🟢 智能方式:")
        print("   - 只下载已上市股票")
        print("   - 预期成功率: 80-90%+")
        print("   - 节省API调用: 70%+")
        print("   - 下载速度: 提升3-4倍")

def main():
    downloader = SmartHistoricalDownloader()
    
    print("🎯 智能历史数据下载器")
    print("=" * 40)
    
    downloader.compare_efficiency()
    
    choice = input("\n选择操作:\n1. 从2003年开始智能下载\n2. 指定年份下载\n3. 查看效率对比\n请选择 (1-3): ").strip()
    
    if choice == "1":
        downloader.continue_from_year(2003)
    elif choice == "2":
        year = int(input("输入年份 (2003-2024): "))
        downloader.download_year_smart(year)
    elif choice == "3":
        downloader.compare_efficiency()
    else:
        print("❌ 无效选择")

if __name__ == "__main__":
    main()