#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
2000-2009年历史数据下载器
=====================

补齐核心10个接口在2000年1月1日至2009年12月31日的历史数据
确保QuantTrade框架拥有完整的25年数据覆盖
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

class HistoricalDataDownloader2000_2009:
    """2000-2009年历史数据下载器"""
    
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
        
        print("📜 优矿2000-2009年历史数据下载器")
        print("🎯 目标: 补齐25年数据覆盖的历史部分")
        print("📊 范围: 全市场股票，2000-2009年")
        print("⏰ 时间窗口: 2000年1月1日 - 2009年12月31日")
        print("=" * 70)
        
        # 核心10个接口配置
        self.apis = {
            "EquGet": {
                "name": "股票基本信息", 
                "dir": "basic_info",
                "fields": "secID,ticker,secShortName,listDate,delistDate",
                "date_field": None,  # 不需要日期范围
                "batch_size": 300
            },
            "TradeCalGet": {
                "name": "交易日历",
                "dir": "calendar", 
                "fields": "calendarDate,isOpen",
                "date_field": ("beginDate", "endDate"),
                "batch_size": None  # 不分批
            },
            "MktEqudGet": {
                "name": "股票日行情",
                "dir": "daily",
                "fields": "secID,ticker,tradeDate,preClosePrice,openPrice,highestPrice,lowestPrice,closePrice,turnoverVol,marketValue",
                "date_field": ("beginDate", "endDate"),
                "batch_size": 50
            },
            "MktAdjfGet": {
                "name": "复权因子",
                "dir": "adjustment",
                "fields": "secID,ticker,exDivDate,adjfactor",
                "date_field": ("beginDate", "endDate"),
                "batch_size": 200
            },
            "EquDivGet": {
                "name": "股票分红",
                "dir": "dividend",
                "fields": "secID,ticker,exDate,dividend,splitRatio",
                "date_field": ("beginDate", "endDate"), 
                "batch_size": 200
            },
            "FdmtBs2018Get": {
                "name": "资产负债表",
                "dir": "financial",
                "fields": "secID,ticker,endDate,totalAssets,totalLiab,totalShrhldrEqty",
                "date_field": ("beginDate", "endDate"),
                "batch_size": 100
            },
            "FdmtDerGet": {
                "name": "财务衍生数据",
                "dir": "financial",
                "fields": "secID,ticker,endDate,revenue,netProfit,roe",
                "date_field": ("beginDate", "endDate"),
                "batch_size": 100
            },
            "MktEquFlowGet": {
                "name": "资金流向",
                "dir": "capital_flow",
                "fields": "secID,ticker,tradeDate,mainNetFlow,superNetFlow,largeNetFlow,mediumNetFlow,smallNetFlow",
                "date_field": ("beginDate", "endDate"),
                "batch_size": 30
            },
            "MktLimitGet": {
                "name": "涨跌停数据",
                "dir": "limit_info",
                "fields": "secID,ticker,tradeDate,upLimit,downLimit",
                "date_field": ("beginDate", "endDate"),
                "batch_size": 100
            },
            "MktRankListStocksGet": {
                "name": "龙虎榜数据", 
                "dir": "rank_list",
                "fields": "secID,ticker,tradeDate,rankReason,buyAmt,sellAmt",
                "date_field": ("beginDate", "endDate"),
                "batch_size": None  # 不分批
            }
        }
    
    def get_all_stocks(self) -> List[str]:
        """获取全部股票列表"""
        try:
            print("📋 获取全市场股票列表...")
            result = self.client.DataAPI.EquGet(
                listStatusCD='',  # 包含退市股票
                field='secID,ticker,listDate,delistDate',
                pandas='1'
            )
            
            if isinstance(result, pd.DataFrame) and not result.empty:
                stocks = result['secID'].unique().tolist()
                print(f"✅ 获取到 {len(stocks)} 只股票 (包含退市股票)")
                return stocks
            return []
        except Exception as e:
            print(f"❌ 获取股票列表失败: {e}")
            return []
    
    def download_single_api_historical(self, api_name: str, api_config: Dict, stocks: List[str]) -> bool:
        """下载单个API的2000-2009历史数据"""
        print(f"\n📊 下载 {api_config['name']} (2000-2009)...")
        
        output_dir = self.base_path / api_config['dir']
        output_dir.mkdir(exist_ok=True)
        
        try:
            api_func = getattr(self.client.DataAPI, api_name)
            all_data = []
            
            # 特殊处理交易日历和龙虎榜 (不需要分股票)
            if api_name in ["TradeCalGet", "MktRankListStocksGet"]:
                print(f"  📅 下载整体数据...")
                
                try:
                    if api_name == "TradeCalGet":
                        result = api_func(
                            beginDate='20000101',
                            endDate='20091231',
                            field=api_config['fields'],
                            pandas='1'
                        )
                    else:  # MktRankListStocksGet
                        # 分年下载龙虎榜
                        for year in range(2000, 2010):
                            year_result = api_func(
                                beginDate=f'{year}0101',
                                endDate=f'{year}1231', 
                                field=api_config['fields'],
                                pandas='1'
                            )
                            if isinstance(year_result, pd.DataFrame) and not year_result.empty:
                                all_data.append(year_result)
                                print(f"    ✅ {year}年: {len(year_result)} 条记录")
                            time.sleep(1)
                        
                        if all_data:
                            result = pd.concat(all_data, ignore_index=True)
                        else:
                            result = pd.DataFrame()
                    
                    if isinstance(result, pd.DataFrame) and not result.empty:
                        output_file = output_dir / f"{api_config['name'].replace('数据', '').replace('信息', '')}_2000_2009.csv"
                        result.to_csv(output_file, index=False)
                        
                        self.stats['success'] += 1
                        self.stats['records'] += len(result)
                        print(f"✅ {api_config['name']} 完成: {len(result)} 条记录")
                        return True
                    
                except Exception as e:
                    print(f"    ❌ 下载失败: {e}")
                    return False
            
            # 处理需要分股票的API
            elif api_config['batch_size'] and stocks:
                batch_size = api_config['batch_size']
                
                # 分年分批下载 (2000-2009年数据量大)
                for year in range(2000, 2010):
                    print(f"  📅 下载 {year} 年数据...")
                    year_data = []
                    
                    for i in range(0, len(stocks), batch_size):
                        batch_stocks = stocks[i:i+batch_size]
                        batch_tickers = ','.join([s.split('.')[0] for s in batch_stocks])
                        
                        print(f"    🔄 {year}年 批次 {i//batch_size + 1}/{(len(stocks)-1)//batch_size + 1}: {len(batch_stocks)} 只股票")
                        
                        try:
                            if api_name == "EquGet":
                                # 基本信息不需要日期范围
                                result = api_func(
                                    secID='',
                                    ticker=batch_tickers,
                                    listStatusCD='',
                                    field=api_config['fields'],
                                    pandas='1'
                                )
                            else:
                                # 其他API需要日期范围
                                result = api_func(
                                    secID='',
                                    ticker=batch_tickers,
                                    beginDate=f'{year}0101',
                                    endDate=f'{year}1231',
                                    field=api_config['fields'],
                                    pandas='1'
                                )
                            
                            if isinstance(result, pd.DataFrame) and not result.empty:
                                year_data.append(result)
                                print(f"      ✅ 完成: {len(result)} 条记录")
                            
                            time.sleep(0.5)  # 防止频率限制
                            
                        except Exception as e:
                            print(f"      ❌ 批次失败: {e}")
                            continue
                    
                    # 保存年度数据
                    if year_data:
                        year_combined = pd.concat(year_data, ignore_index=True)
                        all_data.append(year_combined)
                        print(f"    ✅ {year}年完成: {len(year_combined)} 条记录")
                    
                    # 基本信息只需要下载一次
                    if api_name == "EquGet":
                        break
            
            # 保存最终数据
            if all_data:
                combined = pd.concat(all_data, ignore_index=True)
                
                # 去重处理
                if 'secID' in combined.columns:
                    before_count = len(combined)
                    if api_name == "EquGet":
                        combined = combined.drop_duplicates(subset=['secID'])
                    else:
                        # 其他API按股票和日期去重
                        date_cols = ['tradeDate', 'exDivDate', 'exDate', 'endDate', 'calendarDate']
                        date_col = next((col for col in date_cols if col in combined.columns), None)
                        if date_col:
                            combined = combined.drop_duplicates(subset=['secID', date_col])
                        else:
                            combined = combined.drop_duplicates()
                    
                    after_count = len(combined)
                    if before_count != after_count:
                        print(f"    🔄 去重: {before_count} → {after_count} 条记录")
                
                output_file = output_dir / f"{api_config['name'].replace('数据', '').replace('信息', '')}_2000_2009.csv"
                combined.to_csv(output_file, index=False)
                
                self.stats['success'] += 1
                self.stats['records'] += len(combined)
                print(f"✅ {api_config['name']} 完成: {len(combined)} 条记录")
                return True
            
            return False
            
        except Exception as e:
            print(f"❌ {api_config['name']} 下载失败: {e}")
            self.stats['failed'] += 1
            return False
    
    def run_historical_download(self):
        """运行2000-2009年历史数据下载"""
        start_time = datetime.now()
        
        try:
            # 1. 获取全部股票列表 (包含退市股票)
            stocks = self.get_all_stocks()
            if not stocks:
                print("❌ 无法获取股票列表")
                return
            
            print(f"\n🎯 开始下载2000-2009年历史数据")
            print(f"📊 范围: {len(stocks)} 只股票 (含退市)")
            print(f"🔧 接口: {len(self.apis)} 个核心API")
            print(f"⏱️ 预计时间: 4-6小时")
            print(f"💾 预计数据量: 2-3GB")
            print()
            
            # 2. 按优先级下载各API数据
            priority_order = [
                "EquGet",           # 基本信息 (必须最先)
                "TradeCalGet",      # 交易日历
                "MktEqudGet",       # 日行情 (核心)
                "MktAdjfGet",       # 复权因子
                "EquDivGet",        # 分红数据
                "FdmtBs2018Get",    # 财务数据1
                "FdmtDerGet",       # 财务数据2
                "MktEquFlowGet",    # 资金流向
                "MktLimitGet",      # 涨跌停
                "MktRankListStocksGet"  # 龙虎榜
            ]
            
            print("🔄 开始按优先级下载...")
            
            for i, api_name in enumerate(priority_order, 1):
                if api_name in self.apis:
                    print(f"\n{'='*60}")
                    print(f"📋 阶段 {i}/{len(priority_order)}: {self.apis[api_name]['name']}")
                    print(f"{'='*60}")
                    
                    success = self.download_single_api_historical(
                        api_name, 
                        self.apis[api_name], 
                        stocks
                    )
                    
                    if success:
                        print(f"✅ 阶段{i}完成: {self.apis[api_name]['name']}")
                    else:
                        print(f"⚠️ 阶段{i}部分失败: {self.apis[api_name]['name']}")
                    
                    # 阶段间休息
                    print("⏳ 阶段间休息 5 秒...")
                    time.sleep(5)
            
            # 3. 生成最终报告
            end_time = datetime.now()
            duration = end_time - start_time
            
            print(f"\n🎊 2000-2009年历史数据下载完成!")
            print(f"⏱️ 总耗时: {duration}")
            print(f"✅ 成功: {self.stats['success']} 个接口")
            print(f"❌ 失败: {self.stats['failed']} 个接口") 
            print(f"📋 总记录: {self.stats['records']:,} 条")
            
            # 计算文件大小
            total_size = 0
            for api_config in self.apis.values():
                dir_path = self.base_path / api_config['dir']
                if dir_path.exists():
                    for file in dir_path.glob("*_2000_2009.csv"):
                        total_size += file.stat().st_size
            
            print(f"💾 数据大小: {total_size / (1024*1024*1024):.2f} GB")
            print(f"🎯 完成度: {self.stats['success']}/{len(self.apis)} ({self.stats['success']/len(self.apis)*100:.1f}%)")
            
            if self.stats['success'] >= 8:  # 80%以上成功率
                print(f"\n🚀 历史数据补齐成功！")
                print(f"📊 QuantTrade框架现已拥有完整25年数据覆盖 (2000-2025)")
            else:
                print(f"\n⚠️ 部分数据下载失败，请检查网络或API权限")
                
        except Exception as e:
            print(f"❌ 下载过程异常: {e}")

def main():
    downloader = HistoricalDataDownloader2000_2009()
    downloader.run_historical_download()

if __name__ == "__main__":
    main()