#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
优先级下载器 - 行情数据(5个) + 资金流向(2个)
高优先级核心接口，支持量化策略开发
"""

import uqer
import pandas as pd
from datetime import datetime
from pathlib import Path
import time
import logging

UQER_TOKEN = "68b9922817ae6273137bda7acba81e293582ba347281dfcc056dcb245b23faf3"

class PriorityMarketFlowDownloader:
    """优先级市场数据和资金流向下载器"""
    
    def __init__(self):
        self.client = uqer.Client(token=UQER_TOKEN)
        self.data_dir = Path("data/priority_download")
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        # 7个高优先级接口配置
        self.priority_apis = {
            # 行情数据 (5个) - 支持技术分析和趋势策略
            "market_data": {
                "MktEqudGet": {
                    "desc": "股票日行情", 
                    "dir": "daily",
                    "params": {"field": "secID,ticker,tradeDate,preClosePrice,openPrice,highestPrice,lowestPrice,closePrice,turnoverVol,marketValue,dealAmount,turnoverRate"},
                    "time_range": True
                },
                "MktEquwGet": {
                    "desc": "股票周行情",
                    "dir": "weekly", 
                    "params": {"field": "secID,ticker,endDate,preClosePrice,openPrice,highestPrice,lowestPrice,closePrice,turnoverVol"},
                    "time_range": True
                },
                "MktEqumGet": {
                    "desc": "股票月行情",
                    "dir": "monthly",
                    "params": {"field": "secID,ticker,endDate,preClosePrice,openPrice,highestPrice,lowestPrice,closePrice,turnoverVol"},
                    "time_range": True
                },
                "MktEqudAdjGet": {
                    "desc": "前复权日行情",
                    "dir": "adj_daily",
                    "params": {"field": "secID,ticker,tradeDate,preClosePrice,openPrice,highestPrice,lowestPrice,closePrice,turnoverVol"},
                    "time_range": True
                },
                "MktAdjfGet": {
                    "desc": "复权因子",
                    "dir": "adj_factor",
                    "params": {"field": "secID,ticker,exDivDate,accumAdjFactor"},
                    "time_range": True
                }
            },
            
            # 资金流向 (2个) - 支持资金面策略
            "flow_data": {
                "MktEquFlowGet": {
                    "desc": "个股资金流向",
                    "dir": "stock_flow",
                    "params": {"field": "secID,ticker,tradeDate,buySmallAmount,buyMediumAmount,buyLargeAmount,buyExtraLargeAmount,sellSmallAmount,sellMediumAmount,sellLargeAmount,sellExtraLargeAmount"},
                    "time_range": True
                },
                "MktIndustryFlowGet": {
                    "desc": "行业资金流向", 
                    "dir": "industry_flow",
                    "params": {"field": "industryID,industryName,tradeDate,buySmallAmount,buyMediumAmount,buyLargeAmount,buyExtraLargeAmount,sellSmallAmount,sellMediumAmount,sellLargeAmount,sellExtraLargeAmount"},
                    "time_range": True,
                    "no_ticker": True  # 行业数据不需要ticker参数
                }
            }
        }
        
        # 配置日志
        log_file = self.data_dir / "priority_download.log"
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
        
    def get_stock_info(self):
        """获取股票信息"""
        logging.info("📋 获取股票列表...")
        
        try:
            stocks = uqer.DataAPI.EquGet(
                field='secID,ticker,secShortName,exchangeCD,listStatusCD,listDate'
            )
            
            if stocks is not None and not stocks.empty:
                a_stocks = stocks[stocks['listStatusCD'] == 'L'].copy()
                a_stocks['listDate'] = pd.to_datetime(a_stocks['listDate'])
                a_stocks['listYear'] = a_stocks['listDate'].dt.year
                
                logging.info(f"✅ 获取股票信息: {len(a_stocks)} 只A股")
                return a_stocks
            
        except Exception as e:
            logging.error(f"❌ 获取股票信息失败: {e}")
            return None
    
    def download_api_data(self, api_name, api_config, category, year=None, stocks=None):
        """下载单个API数据"""
        desc = api_config["desc"]
        data_dir = self.data_dir / category / api_config["dir"]
        data_dir.mkdir(parents=True, exist_ok=True)
        
        logging.info(f"📥 开始下载 {desc}...")
        
        try:
            # 获取API函数
            api_func = getattr(uqer.DataAPI, api_name, None)
            if not api_func:
                logging.error(f"❌ API {api_name} 不存在")
                return False
            
            # 构建参数
            params = api_config["params"].copy()
            
            if api_config.get("time_range") and year:
                # 需要时间范围的接口
                params["beginDate"] = f"{year}0101"
                params["endDate"] = f"{year}1231"
                
                if not api_config.get("no_ticker") and stocks is not None:
                    # 需要股票代码的接口，分批下载
                    return self._download_with_stocks(api_func, params, desc, data_dir, year, stocks)
                else:
                    # 不需要股票代码的接口（如行业数据）
                    return self._download_without_stocks(api_func, params, desc, data_dir, year)
            else:
                # 不需要时间范围的接口
                return self._download_static_data(api_func, params, desc, data_dir)
            
        except Exception as e:
            logging.error(f"❌ {desc} 下载失败: {e}")
            return False
    
    def _download_with_stocks(self, api_func, params, desc, data_dir, year, stocks):
        """分批下载股票数据"""
        # 筛选该年度已上市股票
        year_stocks = stocks[stocks['listYear'] <= year].copy()
        
        if len(year_stocks) == 0:
            logging.warning(f"⚠️ {year}年{desc}无已上市股票")
            return False
        
        # 分批下载
        batch_size = 100
        batches = [year_stocks[i:i+batch_size] for i in range(0, len(year_stocks), batch_size)]
        
        total_records = 0
        success_count = 0
        
        for batch_idx, batch_stocks in enumerate(batches):
            batch_file = data_dir / f"{year}_batch_{batch_idx+1:03d}.csv"
            
            # 跳过已存在文件
            if batch_file.exists():
                existing_data = pd.read_csv(batch_file)
                total_records += len(existing_data)
                success_count += 1
                continue
            
            try:
                # 构建ticker列表
                tickers = ','.join(batch_stocks['ticker'].tolist())
                params["ticker"] = tickers
                
                logging.info(f"📦 {desc} {year}年 批次{batch_idx+1}/{len(batches)}: {len(batch_stocks)}只")
                
                # 调用API
                data = api_func(**params)
                
                if data is not None and not data.empty:
                    data.to_csv(batch_file, index=False)
                    total_records += len(data)
                    success_count += 1
                    
                    logging.info(f"✅ {desc} {year}年 批次{batch_idx+1}: {len(data)}条")
                else:
                    logging.warning(f"⚠️ {desc} {year}年 批次{batch_idx+1}: 数据为空")
                
                time.sleep(0.3)
                
            except Exception as e:
                logging.error(f"❌ {desc} {year}年 批次{batch_idx+1} 失败: {e}")
                continue
        
        logging.info(f"📊 {desc} {year}年完成: {total_records}条记录, {success_count}/{len(batches)}批次成功")
        return total_records > 0
    
    def _download_without_stocks(self, api_func, params, desc, data_dir, year):
        """下载不需要股票代码的数据（如行业数据）"""
        file_path = data_dir / f"{year}_data.csv"
        
        # 跳过已存在文件
        if file_path.exists():
            existing_data = pd.read_csv(file_path)
            logging.info(f"📂 {desc} {year}年已存在: {len(existing_data)}条")
            return True
        
        try:
            logging.info(f"📥 {desc} {year}年...")
            
            data = api_func(**params)
            
            if data is not None and not data.empty:
                data.to_csv(file_path, index=False)
                logging.info(f"✅ {desc} {year}年: {len(data)}条记录")
                return True
            else:
                logging.warning(f"⚠️ {desc} {year}年: 数据为空")
                return False
                
        except Exception as e:
            logging.error(f"❌ {desc} {year}年失败: {e}")
            return False
    
    def _download_static_data(self, api_func, params, desc, data_dir):
        """下载静态数据（不依赖时间）"""
        file_path = data_dir / "static_data.csv"
        
        # 跳过已存在文件
        if file_path.exists():
            existing_data = pd.read_csv(file_path)
            logging.info(f"📂 {desc}已存在: {len(existing_data)}条")
            return True
        
        try:
            logging.info(f"📥 {desc}...")
            
            data = api_func(**params)
            
            if data is not None and not data.empty:
                data.to_csv(file_path, index=False)
                logging.info(f"✅ {desc}: {len(data)}条记录")
                return True
            else:
                logging.warning(f"⚠️ {desc}: 数据为空")
                return False
                
        except Exception as e:
            logging.error(f"❌ {desc}失败: {e}")
            return False
    
    def download_all_priority_data(self, start_year=2020, end_year=None):
        """下载所有优先级数据"""
        if end_year is None:
            end_year = datetime.now().year
        
        logging.info("🚀 开始下载高优先级数据")
        logging.info("📋 包含: 行情数据(5个) + 资金流向(2个)")
        
        # 获取股票信息
        stocks = self.get_stock_info()
        if stocks is None:
            return False
        
        total_success = 0
        total_apis = sum(len(category) for category in self.priority_apis.values())
        
        # 按年份下载
        for year in range(start_year, end_year + 1):
            logging.info(f"\n{'='*60}")
            logging.info(f"📅 下载 {year} 年数据")
            logging.info(f"{'='*60}")
            
            year_success = 0
            
            # 下载各类数据
            for category, apis in self.priority_apis.items():
                logging.info(f"\n📂 {category} 类别数据:")
                
                for api_name, api_config in apis.items():
                    success = self.download_api_data(api_name, api_config, category, year, stocks)
                    if success:
                        year_success += 1
                        total_success += 1
                    
                    time.sleep(0.2)
            
            logging.info(f"📊 {year}年完成: {year_success}/{len(self.priority_apis['market_data']) + len(self.priority_apis['flow_data'])}个接口成功")
            time.sleep(1)
        
        # 总结
        logging.info(f"\n🎉 优先级数据下载完成!")
        logging.info(f"📊 总成功: {total_success}/{total_apis * (end_year - start_year + 1)}个接口")
        
        return True
    
    def show_download_plan(self):
        """显示下载计划"""
        print("🎯 优先级数据下载计划")
        print("=" * 50)
        
        total_apis = 0
        for category, apis in self.priority_apis.items():
            print(f"\n📂 {category}:")
            for api_name, config in apis.items():
                print(f"   ✅ {config['desc']}")
                total_apis += 1
        
        print(f"\n📊 总接口数: {total_apis} 个")
        print(f"💾 数据保存: {self.data_dir}")

def main():
    """主函数"""
    downloader = PriorityMarketFlowDownloader()
    
    downloader.show_download_plan()
    
    print("\n选择操作:")
    print("1. 下载最近5年数据 (2020-2025)")
    print("2. 下载最近3年数据 (2022-2025)")
    print("3. 下载全部历史数据 (2000-2025)")
    
    choice = input("请选择 (1-3): ").strip()
    
    if choice == "1":
        downloader.download_all_priority_data(2020)
    elif choice == "2":
        downloader.download_all_priority_data(2022)
    elif choice == "3":
        downloader.download_all_priority_data(2000)
    else:
        print("❌ 无效选择，默认下载最近5年数据")
        downloader.download_all_priority_data(2020)

if __name__ == "__main__":
    main()