#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
启动智能下载 - 从2003年开始，解决空数据问题
"""

import uqer
import pandas as pd
from datetime import datetime
from pathlib import Path
import time
import logging

UQER_TOKEN = "68b9922817ae6273137bda7acba81e293582ba347281dfcc056dcb245b23faf3"

def setup_logging():
    """配置日志"""
    log_dir = Path("data/smart_download")
    log_dir.mkdir(parents=True, exist_ok=True)
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_dir / "smart_download.log"),
            logging.StreamHandler()
        ]
    )

def get_stocks_by_year():
    """获取按年份分类的股票信息"""
    logging.info("📋 分析股票上市时间分布...")
    
    try:
        client = uqer.Client(token=UQER_TOKEN)
        
        stocks = uqer.DataAPI.EquGet(
            field='secID,ticker,secShortName,exchangeCD,listStatusCD,listDate'
        )
        
        if stocks is not None and not stocks.empty:
            # 过滤A股
            a_stocks = stocks[stocks['listStatusCD'] == 'L'].copy()
            a_stocks['listDate'] = pd.to_datetime(a_stocks['listDate'])
            a_stocks['listYear'] = a_stocks['listDate'].dt.year
            
            # 按年份统计
            yearly_stats = {}
            cumulative = 0
            
            for year in range(2000, 2025):
                year_stocks = a_stocks[a_stocks['listYear'] <= year]
                yearly_stats[year] = {
                    'count': len(year_stocks),
                    'new_listings': len(a_stocks[a_stocks['listYear'] == year])
                }
                
            logging.info("📊 历年A股数量统计:")
            for year in range(2000, 2025):
                stats = yearly_stats[year]
                logging.info(f"   {year}: {stats['count']} 只 (+{stats['new_listings']} 新上市)")
            
            return a_stocks
        
    except Exception as e:
        logging.error(f"❌ 获取股票信息失败: {e}")
        return None

def download_year_smart(stocks, year):
    """智能下载指定年份数据"""
    logging.info(f"\n{'='*60}")
    logging.info(f"🎯 开始下载 {year} 年数据")
    logging.info(f"{'='*60}")
    
    # 筛选已上市股票
    year_stocks = stocks[stocks['listYear'] <= year].copy()
    
    if len(year_stocks) == 0:
        logging.warning(f"⚠️ {year} 年无已上市股票，跳过")
        return False
    
    logging.info(f"📈 {year} 年已上市股票: {len(year_stocks)} 只")
    
    # 创建目录
    year_dir = Path("data/smart_download") / f"year_{year}"
    year_dir.mkdir(parents=True, exist_ok=True)
    
    # 分批下载
    batch_size = 80  # 减小批次大小，提高成功率
    batches = [year_stocks[i:i+batch_size] for i in range(0, len(year_stocks), batch_size)]
    
    start_date = f"{year}0101"
    end_date = f"{year}1231"
    
    success_count = 0
    total_records = 0
    
    logging.info(f"📦 分为 {len(batches)} 批下载，每批 {batch_size} 只股票")
    
    for batch_idx, batch_stocks in enumerate(batches):
        batch_file = year_dir / f"batch_{batch_idx+1:03d}.csv"
        
        # 跳过已存在文件
        if batch_file.exists():
            existing_data = pd.read_csv(batch_file)
            total_records += len(existing_data)
            success_count += 1
            logging.info(f"📂 批次 {batch_idx+1} 已存在: {len(existing_data)} 条")
            continue
        
        try:
            tickers = ','.join(batch_stocks['ticker'].tolist())
            
            logging.info(f"📥 批次 {batch_idx+1}/{len(batches)}: {len(batch_stocks)} 只股票")
            
            data = uqer.DataAPI.MktEqudGet(
                secID='',
                ticker=tickers,
                beginDate=start_date,
                endDate=end_date,
                field='secID,ticker,tradeDate,preClosePrice,openPrice,highestPrice,lowestPrice,closePrice,turnoverVol,marketValue,dealAmount,turnoverRate'
            )
            
            if data is not None and not data.empty:
                data.to_csv(batch_file, index=False)
                total_records += len(data)
                success_count += 1
                
                logging.info(f"✅ 批次 {batch_idx+1}: {len(data)} 条记录")
            else:
                logging.warning(f"⚠️ 批次 {batch_idx+1}: 数据为空")
            
            time.sleep(0.2)
            
        except Exception as e:
            logging.error(f"❌ 批次 {batch_idx+1} 失败: {e}")
            time.sleep(1)
            continue
    
    # 统计结果
    success_rate = (success_count / len(batches)) * 100 if len(batches) > 0 else 0
    
    logging.info(f"\n📊 {year} 年下载完成:")
    logging.info(f"   ✅ 成功批次: {success_count}/{len(batches)} ({success_rate:.1f}%)")
    logging.info(f"   📊 总记录数: {total_records:,} 条")
    
    return total_records > 0

def main():
    """主函数"""
    setup_logging()
    
    logging.info("🚀 启动智能历史数据下载器")
    logging.info("解决空数据问题，提升下载效率")
    
    # 获取股票信息
    stocks = get_stocks_by_year()
    if stocks is None:
        logging.error("❌ 无法获取股票信息，退出")
        return
    
    # 从2003年开始智能下载
    start_year = 2003
    current_year = datetime.now().year
    
    logging.info(f"\n🎯 计划下载: {start_year} - {current_year} 年 ({current_year - start_year + 1} 年)")
    
    total_success = 0
    
    for year in range(start_year, current_year + 1):
        success = download_year_smart(stocks, year)
        if success:
            total_success += 1
            
        # 年度间隔
        time.sleep(2)
    
    logging.info(f"\n🎉 智能下载完成!")
    logging.info(f"📊 成功年份: {total_success}/{current_year - start_year + 1}")
    
    # 对比效果
    logging.info(f"\n📈 效果对比:")
    logging.info(f"   🔴 原始方式: 26-27% 成功率，大量空数据")
    logging.info(f"   🟢 智能方式: 预期 80%+ 成功率，避免空数据")

if __name__ == "__main__":
    main()