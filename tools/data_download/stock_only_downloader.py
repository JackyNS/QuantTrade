#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
股票专用数据下载器 - 只下载股票相关数据
"""

import uqer
import pandas as pd
from datetime import datetime
from pathlib import Path
import time
import logging

# 配置
UQER_TOKEN = "68b9922817ae6273137bda7acba81e293582ba347281dfcc056dcb245b23faf3"

class StockOnlyDownloader:
    """股票专用数据下载器"""
    
    def __init__(self):
        self.client = uqer.Client(token=UQER_TOKEN)
        self.data_dir = Path("data/stock_only")
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        # 只选择股票相关的核心接口
        self.stock_apis = {
            # 1. 股票基础信息 (必需)
            "basic_info": {
                "EquGet": "股票基本信息",
                "EquIPOGet": "IPO上市信息", 
                "EquIndustryGet": "股票行业分类",
                "EquDivGet": "股票分红信息",
                "EquSplitsGet": "股票拆股信息",
                "EquAllotGet": "股票配股信息"
            },
            
            # 2. 股票行情数据 (核心)
            "market_data": {
                "MktEqudGet": "日行情数据",
                "MktEquwGet": "周行情数据",
                "MktEqumGet": "月行情数据",
                "MktEqudAdjGet": "前复权日行情",
                "MktAdjfGet": "复权因子"
            },
            
            # 3. 股票财务数据 (重要)
            "financial_data": {
                "FdmtBSAllLatestGet": "资产负债表",
                "FdmtISAllLatestGet": "利润表", 
                "FdmtCFAllLatestGet": "现金流量表",
                "FdmtDerGet": "财务衍生指标",
                "FdmtIndiPSGet": "每股指标",
                "FdmtIndiGrowthGet": "成长能力指标",
                "FdmtIndiRtnGet": "盈利能力指标",
                "FdmtIndiLqdGet": "偿债能力指标"
            },
            
            # 4. 股票资金流向 (策略相关)
            "flow_data": {
                "MktEquFlowGet": "个股资金流向",
                "MktIndustryFlowGet": "行业资金流向"
            },
            
            # 5. 股票技术因子 (量化必需)
            "factor_data": {
                "StockFactorsDateRangeGet": "股票因子时间序列"
            },
            
            # 6. 股票市场微观结构
            "microstructure": {
                "MktBlockdGet": "大宗交易数据",
                "FstTotalGet": "融资融券汇总",
                "MktLimitGet": "涨跌停数据",
                "SecHaltGet": "停复牌数据"
            }
        }
        
        # 配置日志
        log_file = self.data_dir / "stock_download.log"
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
        
    def get_stock_list_with_listing_dates(self):
        """获取股票列表及上市日期"""
        logging.info("📋 获取股票列表及上市日期...")
        
        try:
            stocks = uqer.DataAPI.EquGet(
                field='secID,ticker,secShortName,exchangeCD,listStatusCD,listDate,delistDate'
            )
            
            if stocks is not None and not stocks.empty:
                # 只保留A股
                a_stocks = stocks[stocks['listStatusCD'] == 'L'].copy()
                
                # 转换上市日期格式
                a_stocks['listDate'] = pd.to_datetime(a_stocks['listDate'])
                a_stocks['listYear'] = a_stocks['listDate'].dt.year
                
                logging.info(f"✅ 获取股票列表成功: {len(a_stocks)} 只A股")
                
                # 按年份统计
                yearly_stats = a_stocks['listYear'].value_counts().sort_index()
                logging.info("📊 按年度上市股票分布:")
                for year in range(1990, 2025):
                    if year in yearly_stats.index:
                        logging.info(f"   {year}年: {yearly_stats[year]} 只")
                
                return a_stocks
            
        except Exception as e:
            logging.error(f"❌ 获取股票列表失败: {e}")
            return None
    
    def download_optimized_historical_data(self):
        """优化的历史数据下载"""
        logging.info("🚀 开始优化的股票历史数据下载...")
        
        # 获取股票列表
        stocks = self.get_stock_list_with_listing_dates()
        if stocks is None:
            return False
        
        # 按年度智能下载
        return self._download_by_year_smart(stocks)
    
    def _download_by_year_smart(self, stocks):
        """按年度智能下载，只下载已上市的股票"""
        logging.info("📈 开始按年度智能下载历史行情...")
        
        market_dir = self.data_dir / "market_data"
        market_dir.mkdir(exist_ok=True)
        
        total_records = 0
        
        for year in range(2000, datetime.now().year + 1):
            logging.info(f"📅 处理 {year} 年数据...")
            
            # 筛选该年度已上市的股票
            year_stocks = stocks[stocks['listYear'] <= year].copy()
            
            if len(year_stocks) == 0:
                logging.info(f"⏭️ {year} 年无已上市股票，跳过")
                continue
            
            logging.info(f"🎯 {year} 年已上市股票: {len(year_stocks)} 只")
            
            # 下载该年度数据
            year_records = self._download_year_data_smart(year_stocks, year, market_dir)
            total_records += year_records
            
            logging.info(f"✅ {year} 年完成: {year_records} 条记录")
        
        logging.info(f"🎉 历史行情下载完成: 总计 {total_records} 条记录")
        return total_records > 0
    
    def _download_year_data_smart(self, stocks, year, market_dir):
        """智能下载年度数据"""
        year_dir = market_dir / f"year_{year}"
        year_dir.mkdir(exist_ok=True)
        
        start_date = f"{year}0101"
        end_date = f"{year}1231"
        
        # 分批下载，每批100只股票
        batch_size = 100
        batches = [stocks[i:i+batch_size] for i in range(0, len(stocks), batch_size)]
        
        total_records = 0
        
        for batch_idx, batch_stocks in enumerate(batches):
            batch_file = year_dir / f"batch_{batch_idx+1:03d}.csv"
            
            # 跳过已存在的文件
            if batch_file.exists():
                existing_data = pd.read_csv(batch_file)
                total_records += len(existing_data)
                continue
            
            try:
                tickers = ','.join(batch_stocks['ticker'].tolist())
                
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
                    logging.info(f"✅ {year} 批次 {batch_idx+1}: {len(data)} 条")
                else:
                    logging.info(f"⚠️ {year} 批次 {batch_idx+1}: 空数据")
                
                time.sleep(0.2)
                
            except Exception as e:
                logging.error(f"❌ {year} 批次 {batch_idx+1} 失败: {e}")
                continue
        
        return total_records
    
    def download_stock_basics_only(self):
        """只下载股票基础信息"""
        logging.info("📋 下载股票基础信息...")
        
        basics_dir = self.data_dir / "basics"
        basics_dir.mkdir(exist_ok=True)
        
        for api_name, description in self.stock_apis["basic_info"].items():
            try:
                logging.info(f"📥 {description}...")
                
                api_func = getattr(uqer.DataAPI, api_name, None)
                if not api_func:
                    continue
                
                # 根据接口调整参数
                if api_name == "EquIndustryGet":
                    data = api_func(intoDate="20251231")
                elif api_name in ["EquDivGet", "EquSplitsGet", "EquAllotGet"]:
                    data = api_func(beginDate="20000101", endDate="20251231")
                else:
                    data = api_func()
                
                if data is not None and not data.empty:
                    file_path = basics_dir / f"{api_name}.csv"
                    data.to_csv(file_path, index=False)
                    logging.info(f"✅ {description}: {len(data)} 条")
                
                time.sleep(0.3)
                
            except Exception as e:
                logging.error(f"❌ {description} 失败: {e}")
    
    def show_download_plan(self):
        """显示下载计划"""
        total_apis = sum(len(category) for category in self.stock_apis.values())
        
        print("🎯 股票专用数据下载计划")
        print("=" * 50)
        print(f"📊 总API数量: {total_apis} 个 (纯股票相关)")
        print(f"📅 时间范围: 2000年-至今")
        print(f"🎯 数据类型: 仅股票数据")
        print("\n📋 API分类:")
        
        for category, apis in self.stock_apis.items():
            print(f"\n{category}:")
            for api, desc in apis.items():
                print(f"  - {desc}")
        
        print(f"\n💾 数据保存: {self.data_dir}")
        
def main():
    downloader = StockOnlyDownloader()
    
    print("选择下载模式:")
    print("1. 只下载基础信息")
    print("2. 完整历史数据下载")
    print("3. 显示下载计划")
    
    choice = input("请选择 (1-3): ").strip()
    
    if choice == "1":
        downloader.download_stock_basics_only()
    elif choice == "2":
        downloader.download_optimized_historical_data()
    elif choice == "3":
        downloader.show_download_plan()
    else:
        print("❌ 无效选择")

if __name__ == "__main__":
    main()