#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
启动历史数据下载 - 从2000年至今
"""

import uqer
import pandas as pd
from datetime import datetime, date
from pathlib import Path
import time
import logging
import json

# 配置
UQER_TOKEN = "68b9922817ae6273137bda7acba81e293582ba347281dfcc056dcb245b23faf3"
DATA_DIR = Path("data/historical_download")
DATA_DIR.mkdir(parents=True, exist_ok=True)

# 日志配置
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(DATA_DIR / 'download.log'),
        logging.StreamHandler()
    ]
)

class HistoricalDataDownloader:
    """历史数据下载器"""
    
    def __init__(self):
        self.client = uqer.Client(token=UQER_TOKEN)
        self.start_date = "20000101"
        self.end_date = datetime.now().strftime('%Y%m%d')
        
        logging.info(f"🚀 初始化历史数据下载器")
        logging.info(f"📅 下载时间范围: {self.start_date} - {self.end_date}")
    
    def get_active_stocks(self):
        """获取活跃股票列表"""
        logging.info("📋 获取股票列表...")
        
        try:
            stocks = uqer.DataAPI.EquGet(
                field='secID,ticker,secShortName,exchangeCD,listStatusCD,listDate'
            )
            
            if stocks is not None and not stocks.empty:
                # 过滤A股
                a_stocks = stocks[stocks['listStatusCD'] == 'L'].copy()
                
                # 按交易所分类
                sh_stocks = a_stocks[a_stocks['exchangeCD'] == 'XSHG']
                sz_stocks = a_stocks[a_stocks['exchangeCD'] == 'XSHE'] 
                
                logging.info(f"✅ 获取股票列表成功:")
                logging.info(f"   📈 沪市: {len(sh_stocks)} 只")
                logging.info(f"   📊 深市: {len(sz_stocks)} 只")
                logging.info(f"   📦 总计: {len(a_stocks)} 只")
                
                # 保存股票列表
                stock_file = DATA_DIR / "stock_universe.csv"
                a_stocks.to_csv(stock_file, index=False)
                
                return a_stocks
            else:
                logging.error("❌ 获取股票列表失败")
                return None
                
        except Exception as e:
            logging.error(f"❌ 获取股票列表异常: {e}")
            return None
    
    def test_market_data_download(self):
        """测试市场数据下载"""
        logging.info("🧪 测试市场数据下载...")
        
        try:
            # 测试下载最近的交易日数据
            test_start = "20240830"
            test_end = "20240830"
            
            # 选择几只代表性股票测试
            test_tickers = "000001.XSHE,600000.XSHG,000002.XSHE,600036.XSHG"
            
            logging.info(f"📊 测试下载: {test_start} - {test_end}")
            logging.info(f"🎯 测试股票: {test_tickers}")
            
            # 调用API
            data = uqer.DataAPI.MktEqudGet(
                secID='',
                ticker=test_tickers,
                beginDate=test_start,
                endDate=test_end,
                field='secID,ticker,tradeDate,preClosePrice,openPrice,highestPrice,lowestPrice,closePrice,turnoverVol,marketValue,dealAmount,turnoverRate'
            )
            
            if data is not None and not data.empty:
                logging.info(f"✅ 测试下载成功!")
                logging.info(f"   📊 数据量: {len(data)} 条记录")
                logging.info(f"   📅 日期范围: {data['tradeDate'].min()} - {data['tradeDate'].max()}")
                logging.info(f"   🎯 股票数: {data['ticker'].nunique()} 只")
                
                # 保存测试数据
                test_file = DATA_DIR / "test_download.csv"
                data.to_csv(test_file, index=False)
                
                # 显示样本数据
                logging.info("📄 样本数据:")
                print(data.head().to_string(index=False))
                
                return True
            else:
                logging.warning("⚠️ 测试下载数据为空")
                return False
                
        except Exception as e:
            logging.error(f"❌ 测试下载失败: {e}")
            return False
    
    def download_stock_basics(self):
        """下载股票基础信息"""
        logging.info("📋 下载股票基础信息...")
        
        basics_dir = DATA_DIR / "basics"
        basics_dir.mkdir(exist_ok=True)
        
        basic_apis = {
            "EquGet": "股票基本信息",
            "EquIPOGet": "IPO信息", 
            "EquIndustryGet": "行业分类",
            "EquDivGet": "分红信息"
        }
        
        results = {}
        
        for api_name, description in basic_apis.items():
            try:
                logging.info(f"📥 下载 {description}...")
                
                api_func = getattr(uqer.DataAPI, api_name, None)
                if not api_func:
                    logging.warning(f"⚠️ API {api_name} 不存在")
                    continue
                
                # 调用API
                if api_name == "EquIndustryGet":
                    # 行业分类需要日期参数
                    data = api_func(
                        intoDate=self.end_date,
                        field=''
                    )
                elif api_name == "EquDivGet":
                    # 分红信息需要日期范围
                    data = api_func(
                        beginDate=self.start_date,
                        endDate=self.end_date
                    )
                else:
                    data = api_func()
                
                if data is not None and not data.empty:
                    # 保存数据
                    file_path = basics_dir / f"{api_name}.csv"
                    data.to_csv(file_path, index=False)
                    
                    logging.info(f"✅ {description}: {len(data)} 条记录")
                    results[api_name] = len(data)
                else:
                    logging.warning(f"⚠️ {description}: 数据为空")
                    results[api_name] = 0
                
                # API限制延迟
                time.sleep(0.5)
                
            except Exception as e:
                logging.error(f"❌ {description} 下载失败: {e}")
                results[api_name] = -1
                continue
        
        logging.info(f"📊 基础信息下载完成: {results}")
        return results
    
    def start_batch_historical_download(self):
        """开始批量历史数据下载"""
        logging.info("🚀 开始批量历史数据下载...")
        
        # 1. 获取股票列表
        stocks = self.get_active_stocks()
        if stocks is None:
            logging.error("❌ 无法获取股票列表，终止下载")
            return False
        
        # 2. 测试下载（如果测试失败也继续，因为可能是日期问题）
        test_result = self.test_market_data_download()
        if test_result:
            logging.info("✅ 测试下载成功，开始正式下载")
        else:
            logging.warning("⚠️ 测试下载失败，但继续尝试正式下载（可能是日期问题）")
        
        # 3. 下载基础信息
        basics_result = self.download_stock_basics()
        
        # 4. 开始历史行情下载
        return self._download_historical_market_data(stocks)
    
    def _download_historical_market_data(self, stocks):
        """下载历史市场数据"""
        logging.info("📈 开始下载历史市场数据...")
        
        # 创建市场数据目录
        market_dir = DATA_DIR / "market_data"
        market_dir.mkdir(exist_ok=True)
        
        # 分年度下载，避免单次请求数据过大
        start_year = 2000
        end_year = datetime.now().year
        
        total_success = 0
        total_failed = 0
        
        for year in range(start_year, end_year + 1):
            year_start = f"{year}0101"
            year_end = f"{year}1231"
            
            logging.info(f"📅 下载 {year} 年数据...")
            
            success, failed = self._download_year_data(stocks, year_start, year_end, market_dir)
            total_success += success
            total_failed += failed
            
            # 年度间隔
            time.sleep(1)
        
        logging.info(f"📊 历史数据下载完成:")
        logging.info(f"   ✅ 成功: {total_success} 批次")
        logging.info(f"   ❌ 失败: {total_failed} 批次")
        
        return total_success > 0
    
    def _download_year_data(self, stocks, start_date, end_date, market_dir):
        """下载年度数据"""
        year = start_date[:4]
        year_dir = market_dir / f"year_{year}"
        year_dir.mkdir(exist_ok=True)
        
        # 分批处理股票，每批50只
        batch_size = 50
        batches = [stocks[i:i+batch_size] for i in range(0, len(stocks), batch_size)]
        
        success_count = 0
        failed_count = 0
        
        for batch_idx, batch_stocks in enumerate(batches):
            batch_file = year_dir / f"batch_{batch_idx+1:03d}.csv"
            
            # 如果文件已存在，跳过
            if batch_file.exists():
                logging.info(f"📂 {year} 批次 {batch_idx+1} 已存在，跳过")
                success_count += 1
                continue
            
            try:
                # 构建ticker列表
                tickers = ','.join(batch_stocks['ticker'].tolist())
                
                logging.info(f"📥 {year} 批次 {batch_idx+1}/{len(batches)}: {len(batch_stocks)} 只股票")
                
                # 下载数据
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
                    
                    logging.info(f"✅ {year} 批次 {batch_idx+1}: {len(data)} 条记录")
                    success_count += 1
                else:
                    logging.warning(f"⚠️ {year} 批次 {batch_idx+1}: 数据为空")
                    failed_count += 1
                
                # API限制延迟
                time.sleep(0.3)
                
            except Exception as e:
                logging.error(f"❌ {year} 批次 {batch_idx+1} 失败: {e}")
                failed_count += 1
                time.sleep(1)  # 错误时延长等待
                continue
        
        logging.info(f"📊 {year} 年完成: 成功 {success_count}, 失败 {failed_count}")
        return success_count, failed_count

def main():
    """主函数"""
    print("🚀 开始从2000年至今的历史数据下载")
    print("=" * 60)
    
    # 创建下载器
    downloader = HistoricalDataDownloader()
    
    # 开始下载
    success = downloader.start_batch_historical_download()
    
    if success:
        print("\n🎉 历史数据下载启动成功!")
        print(f"📁 数据保存目录: {DATA_DIR}")
        print(f"📋 查看日志: {DATA_DIR}/download.log")
    else:
        print("\n❌ 历史数据下载启动失败")
    
    return success

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)