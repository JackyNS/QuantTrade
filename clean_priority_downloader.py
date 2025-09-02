#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
优矿核心数据优先下载器 - 清洁版
===============================

基于优矿API和core模块需求的优先级下载方案
"""

import uqer
import pandas as pd
from pathlib import Path
from datetime import datetime
import time
import logging
import json
from typing import Dict, List, Optional, Tuple

# 优矿Token
UQER_TOKEN = "68b9922817ae6273137bda7acba81e293582ba347281dfcc056dcb245b23faf3"

class CleanPriorityDownloader:
    """优矿核心数据优先下载器"""
    
    def __init__(self):
        # 初始化uqer客户端
        uqer.Client(token=UQER_TOKEN)
        self.client = uqer
        self.base_path = Path("/Users/jackstudio/QuantTrade/data")
        self.base_path.mkdir(exist_ok=True)
        
        # 设置简单日志
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
        # 下载统计
        self.stats = {'success': 0, 'failed': 0, 'records': 0}
        
        # 优先API配置
        self.priority_apis = {
            # 第1优先级：股票基本信息（静态数据）
            "EquGet": {
                "name": "股票基本信息",
                "dir": "basic_info",
                "fields": "secID,ticker,secShortName,exchangeCD,listStatusCD,listDate,delistDate",
                "time_based": False
            },
            
            # 第2优先级：股票日行情（核心）
            "MktEqudGet": {
                "name": "股票日行情",
                "dir": "daily", 
                "fields": "secID,ticker,tradeDate,preClosePrice,openPrice,highestPrice,lowestPrice,closePrice,turnoverVol,marketValue",
                "time_based": True
            },
            
            # 第3优先级：交易日历
            "TradeCalGet": {
                "name": "交易日历",
                "dir": "calendar", 
                "fields": "calendarDate,exchangeCD,isOpen",
                "time_based": True,
                "special": True
            },
            
            # 第4优先级：复权因子
            "MktAdjfGet": {
                "name": "复权因子",
                "dir": "adjustment",
                "fields": "secID,ticker,exDivDate,adjfactor",
                "time_based": True
            },
            
            # 第5优先级：分红数据  
            "EquDivGet": {
                "name": "股票分红",
                "dir": "dividend",
                "fields": "secID,ticker,exDate,dividend,splitRatio",
                "time_based": True
            },
            
            # 第6优先级：市值数据 (使用MktEqud中的marketValue，无需单独API)
            # 注释：市值数据已包含在MktEqudGet中的marketValue字段
            
            # 第7优先级：财务数据 - 资产负债表
            "FdmtBs2018Get": {
                "name": "资产负债表(2018新准则)",
                "dir": "financial",
                "fields": "secID,ticker,endDate,totalAssets,totalLiab,totalShrhldrEqty",
                "time_based": True,
                "special": True
            },
            
            # 第7.5优先级：财务衍生数据
            "FdmtDerGet": {
                "name": "财务衍生数据",
                "dir": "financial",
                "fields": "secID,ticker,endDate,revenue,netProfit,roe,roa",
                "time_based": True,
                "special": True
            },
            
            # 第8优先级：资金流向（情绪核心）
            "MktEquFlowGet": {
                "name": "个股资金流向",
                "dir": "capital_flow",
                "fields": "secID,ticker,tradeDate,mainNetFlow,superNetFlow,largeNetFlow,mediumNetFlow,smallNetFlow",
                "time_based": True
            },
            
            # 第9优先级：涨跌停限制（市场情绪）
            "MktLimitGet": {
                "name": "涨跌停限制",
                "dir": "limit_info",
                "fields": "secID,ticker,tradeDate,upLimit,downLimit,limitStatus",
                "time_based": True
            },
            
            # 第10优先级：龙虎榜数据（异动情绪）
            "MktRankListStocksGet": {
                "name": "龙虎榜数据", 
                "dir": "rank_list",
                "fields": "secID,ticker,tradeDate,rankReason,buyAmt,sellAmt",
                "time_based": True
            }
        }
    
    def get_stock_list(self) -> List[str]:
        """获取股票列表"""
        try:
            print("📋 获取股票列表...")
            result = self.client.DataAPI.EquGet(
                listStatusCD='L',  # 只要上市股票
                field='secID,ticker',
                pandas='1'
            )
            
            if isinstance(result, pd.DataFrame) and not result.empty:
                stocks = result['secID'].unique().tolist()
                print(f"✅ 获取到 {len(stocks)} 只股票")
                print(f"📊 将下载全部 {len(stocks)} 只A股历史数据")
                return stocks  # 下载全部股票
            return []
            
        except Exception as e:
            print(f"❌ 获取股票列表失败: {e}")
            return []
    
    def download_static_data(self, api_name: str, config: Dict) -> bool:
        """下载静态数据"""
        try:
            print(f"📊 下载 {config['name']}...")
            
            # 创建目录
            output_dir = self.base_path / config['dir'] 
            output_dir.mkdir(exist_ok=True)
            
            # 调用API
            api_func = getattr(self.client.DataAPI, api_name)
            result = api_func(
                field=config['fields'],
                pandas='1'
            )
            
            if isinstance(result, pd.DataFrame) and not result.empty:
                output_file = output_dir / f"{api_name.lower()}.csv"
                result.to_csv(output_file, index=False)
                
                self.stats['success'] += 1
                self.stats['records'] += len(result)
                print(f"✅ {config['name']} 完成: {len(result)} 条记录")
                return True
            else:
                print(f"❌ {config['name']} 无数据")
                return False
                
        except Exception as e:
            print(f"❌ {config['name']} 失败: {e}")
            self.stats['failed'] += 1
            return False
    
    def download_time_series(self, api_name: str, config: Dict, stocks: List[str]) -> bool:
        """下载时间序列数据 - 支持分批下载"""
        try:
            print(f"📈 下载 {config['name']}...")
            
            # 创建目录
            output_dir = self.base_path / config['dir']
            output_dir.mkdir(exist_ok=True)
            
            # 处理特殊API
            if config.get('special'):
                return self.download_special_api(api_name, config, output_dir)
            
            api_func = getattr(self.client.DataAPI, api_name)
            
            # 分批下载 - 每批100只股票
            batch_size = 100
            all_data = []
            
            print(f"   📊 分批下载: {len(stocks)} 只股票, 每批 {batch_size} 只")
            
            for i in range(0, len(stocks), batch_size):
                batch_stocks = stocks[i:i+batch_size]
                batch_tickers = ','.join([s.split('.')[0] for s in batch_stocks])
                
                print(f"   🔄 批次 {i//batch_size + 1}/{(len(stocks)-1)//batch_size + 1}: {len(batch_stocks)} 只股票")
                
                try:
                    result = api_func(
                        secID='',
                        ticker=batch_tickers,
                        beginDate='20100101',
                        endDate='20250831',
                        field=config['fields'],
                        pandas='1'
                    )
                    
                    if isinstance(result, pd.DataFrame) and not result.empty:
                        all_data.append(result)
                        print(f"   ✅ 批次完成: {len(result)} 条记录")
                    else:
                        print(f"   ⚠️ 批次无数据")
                    
                    # 批次间停顿
                    time.sleep(0.5)
                    
                except Exception as e:
                    print(f"   ❌ 批次失败: {e}")
                    continue
            
            # 合并所有数据
            if all_data:
                combined_data = pd.concat(all_data, ignore_index=True)
                output_file = output_dir / f"{api_name.lower()}_2010_2025.csv"
                combined_data.to_csv(output_file, index=False)
                
                self.stats['success'] += 1
                self.stats['records'] += len(combined_data)
                print(f"✅ {config['name']} 完成: {len(combined_data)} 条记录")
                return True
            else:
                print(f"❌ {config['name']} 无数据")
                return False
                
        except Exception as e:
            print(f"❌ {config['name']} 失败: {e}")
            self.stats['failed'] += 1
            return False
    
    def download_special_api(self, api_name: str, config: Dict, output_dir: Path) -> bool:
        """下载特殊API（如交易日历和财务数据）"""
        try:
            api_func = getattr(self.client.DataAPI, api_name)
            
            if api_name == 'TradeCalGet':
                # 交易日历
                result = api_func(
                    exchangeCD='XSHG,XSHE',
                    beginDate='20100101',
                    endDate='20251231',
                    field=config['fields'],
                    pandas='1'
                )
                output_file = output_dir / "trading_calendar.csv"
                
            elif api_name == 'FdmtBs2018Get':
                # 财务数据 - 资产负债表(2018新准则)
                result = api_func(
                    reportType='A',  # 年报
                    beginDate='20180101',  # 2018新准则开始
                    endDate='20251231', 
                    field=config['fields'],
                    pandas='1'
                )
                output_file = output_dir / "balance_sheet_2018.csv"
                
            elif api_name == 'FdmtDerGet':
                # 财务衍生数据
                result = api_func(
                    reportType='A',  # 年报
                    beginDate='20100101',
                    endDate='20251231',
                    field=config['fields'],
                    pandas='1'
                )
                output_file = output_dir / "financial_derived.csv"
            
            else:
                print(f"❌ 未支持的特殊API: {api_name}")
                return False
                
            if isinstance(result, pd.DataFrame) and not result.empty:
                result.to_csv(output_file, index=False)
                
                self.stats['success'] += 1
                self.stats['records'] += len(result)
                print(f"✅ {config['name']} 完成: {len(result)} 条记录")
                return True
            else:
                print(f"❌ {config['name']} 无数据")
                return False
            
        except Exception as e:
            print(f"❌ 特殊API {api_name} 失败: {e}")
            self.stats['failed'] += 1
            return False
    
    def run_download(self):
        """运行下载"""
        start_time = datetime.now()
        
        print("🚀 优矿核心数据正式下载器")
        print("🎯 目标: 下载完整A股数据 (2010-2025)")
        print("📊 范围: 全部A股, 15年历史数据")
        print("=" * 60)
        
        try:
            # 1. 获取股票列表
            stocks = self.get_stock_list()
            if not stocks:
                print("❌ 无法获取股票列表")
                return
            
            # 2. 按优先级下载
            for api_name, config in self.priority_apis.items():
                print(f"\n📡 {config['name']}")
                print("-" * 30)
                
                if config.get('time_based'):
                    success = self.download_time_series(api_name, config, stocks)
                else:
                    success = self.download_static_data(api_name, config)
                
                # API间停顿
                time.sleep(1)
            
            # 3. 生成报告
            self.generate_report(start_time)
            
        except Exception as e:
            print(f"❌ 下载异常: {e}")
    
    def generate_report(self, start_time):
        """生成简单报告"""
        end_time = datetime.now()
        duration = end_time - start_time
        
        print(f"\n🎊 下载完成!")
        print(f"⏱️ 耗时: {duration}")
        print(f"✅ 成功: {self.stats['success']}")
        print(f"❌ 失败: {self.stats['failed']}")
        print(f"📋 记录: {self.stats['records']:,}")
        
        # 显示数据结构
        print(f"\n📁 数据目录:")
        for item in self.base_path.iterdir():
            if item.is_dir():
                files = list(item.glob("*.csv"))
                if files:
                    print(f"   📂 {item.name}: {len(files)} 文件")

def main():
    downloader = CleanPriorityDownloader()
    downloader.run_download()

if __name__ == "__main__":
    main()