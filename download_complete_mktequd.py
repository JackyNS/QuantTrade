#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
完整A股日线数据下载器 (MktEqudGet)
下载全部A股从2000年至今的完整日线数据
"""

import sys
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
import warnings
import time
import os
import json
warnings.filterwarnings('ignore')

try:
    import uqer
    print("✅ UQER API 可用")
    UQER_AVAILABLE = True
except ImportError:
    print("❌ UQER API 不可用")
    UQER_AVAILABLE = False
    sys.exit(1)

class CompleteMktEqudDownloader:
    """完整A股日线数据下载器"""
    
    def __init__(self):
        """初始化下载器"""
        self.setup_uqer()
        self.setup_paths()
        self.download_stats = {
            'start_time': datetime.now(),
            'total_stocks': 0,
            'successful_downloads': 0,
            'failed_downloads': 0,
            'total_records': 0,
            'data_size_mb': 0
        }
        
    def setup_uqer(self):
        """设置UQER连接"""
        try:
            uqer_token = "68b9922817ae6273137bda7acba81e293582ba347281dfcc056dcb245b23faf3"
            uqer.Client(token=uqer_token)
            print("✅ UQER连接成功")
            self.uqer_connected = True
        except Exception as e:
            print(f"❌ UQER连接失败: {e}")
            self.uqer_connected = False
            sys.exit(1)
    
    def setup_paths(self):
        """设置存储路径"""
        self.base_path = Path("/Users/jackstudio/QuantTrade/data/mktequd_complete")
        self.base_path.mkdir(exist_ok=True)
        
        # 按年份创建子目录
        for year in range(2000, 2025):
            year_path = self.base_path / f"year_{year}"
            year_path.mkdir(exist_ok=True)
        
        print(f"📁 数据存储路径: {self.base_path}")
    
    def get_all_a_stocks(self):
        """获取全部A股股票列表"""
        print("📋 获取全部A股股票列表...")
        
        try:
            # 获取所有股票（包括已退市的）
            current_stocks = uqer.DataAPI.EquGet(
                listStatusCD='L',  # 当前上市
                pandas=1
            )
            
            # 获取已退市股票  
            delisted_stocks = uqer.DataAPI.EquGet(
                listStatusCD='DE',  # 已退市
                pandas=1
            )
            
            # 合并数据
            all_stocks = []
            if current_stocks is not None and len(current_stocks) > 0:
                all_stocks.append(current_stocks)
            if delisted_stocks is not None and len(delisted_stocks) > 0:
                all_stocks.append(delisted_stocks)
            
            if not all_stocks:
                print("❌ 获取股票列表失败")
                return []
            
            df = pd.concat(all_stocks, ignore_index=True)
            
            # 过滤A股 (深交所XSHE和上交所XSHG)
            a_stocks = df[
                df['secID'].str.contains('.XSHE|.XSHG', na=False)
            ].copy()
            
            # 排除指数等非股票代码
            a_stocks = a_stocks[
                ~a_stocks['secID'].str.contains('.ZICN|.INDX', na=False)
            ]
            
            stock_list = a_stocks['secID'].unique().tolist()
            
            print(f"✅ 获取到 {len(stock_list)} 只A股（含已退市）")
            print(f"   📊 当前上市: {len(current_stocks) if current_stocks is not None else 0}")
            print(f"   📊 已退市: {len(delisted_stocks) if delisted_stocks is not None else 0}")
            
            self.download_stats['total_stocks'] = len(stock_list)
            return stock_list
            
        except Exception as e:
            print(f"❌ 获取股票列表失败: {e}")
            # 使用备用股票列表
            backup_stocks = self.get_backup_stock_list()
            self.download_stats['total_stocks'] = len(backup_stocks)
            return backup_stocks
    
    def get_backup_stock_list(self):
        """备用股票列表（主要指数成分股）"""
        print("⚠️ 使用备用股票列表")
        return [
            # 主要大盘股
            '000001.XSHE', '000002.XSHE', '000858.XSHE', '600036.XSHG', '600519.XSHG',
            '002415.XSHE', '000725.XSHE', '600887.XSHG', '000063.XSHE', '600276.XSHG',
            '600000.XSHG', '300059.XSHE', '000166.XSHE', '002594.XSHE', '600031.XSHG',
            '000661.XSHE', '300015.XSHE', '300750.XSHE', '002129.XSHE', '688981.XSHG',
            '601318.XSHG', '601628.XSHG', '600030.XSHG', '000776.XSHE', '600028.XSHG'
        ]
    
    def download_stock_by_year(self, stock_code, year):
        """按年份下载股票数据"""
        try:
            start_date = f"{year}0101"
            end_date = f"{year}1231"
            
            # 调用UQER API
            result = uqer.DataAPI.MktEqudGet(
                secID=stock_code,
                beginDate=start_date,
                endDate=end_date,
                pandas=1
            )
            
            if result is None or len(result) == 0:
                return None
            
            # 数据处理
            df = result.copy()
            if 'tradeDate' in df.columns:
                df['tradeDate'] = pd.to_datetime(df['tradeDate'])
            
            # 重命名常用列
            column_mapping = {
                'highestPrice': 'highPrice',
                'lowestPrice': 'lowPrice',  
                'turnoverVol': 'volume',
                'turnoverValue': 'amount',
                'chgPct': 'changePct'
            }
            df = df.rename(columns=column_mapping)
            
            # 过滤有效数据
            if 'closePrice' in df.columns:
                df = df.dropna(subset=['closePrice'])
                df = df[df['closePrice'] > 0]
                df = df.sort_values('tradeDate')
            
            return df
            
        except Exception as e:
            return None
    
    def download_complete_data(self):
        """下载完整历史数据"""
        print("🚀 开始下载全部A股完整历史数据")
        print("   📅 时间范围: 2000年-2024年")
        print("   🎯 目标: 全部A股日线数据")
        print("=" * 80)
        
        stock_list = self.get_all_a_stocks()
        
        if not stock_list:
            print("❌ 无法获取股票列表")
            return
        
        # 按年份和股票下载
        for year in range(2000, 2025):
            print(f"\n📅 下载 {year} 年数据...")
            year_path = self.base_path / f"year_{year}"
            
            year_success = 0
            year_failed = 0
            
            for i, stock_code in enumerate(stock_list, 1):
                # 检查文件是否已存在
                stock_file = year_path / f"{stock_code.replace('.', '_')}.csv"
                if stock_file.exists():
                    year_success += 1
                    if i % 100 == 0:  # 每100只股票显示一次进度
                        print(f"   📊 [{i}/{len(stock_list)}] 进度更新...")
                    continue
                
                # 下载数据
                stock_data = self.download_stock_by_year(stock_code, year)
                
                if stock_data is not None and len(stock_data) > 0:
                    try:
                        # 保存数据
                        stock_data.to_csv(stock_file, index=False)
                        year_success += 1
                        self.download_stats['total_records'] += len(stock_data)
                        
                        # 计算文件大小
                        file_size = stock_file.stat().st_size / 1024 / 1024  # MB
                        self.download_stats['data_size_mb'] += file_size
                        
                        if i % 50 == 0:  # 每50只显示进度
                            print(f"   📈 [{i}/{len(stock_list)}] {stock_code}: {len(stock_data)}条记录")
                            
                    except Exception as e:
                        year_failed += 1
                        if i % 100 == 0:
                            print(f"   ❌ [{i}/{len(stock_list)}] {stock_code}: 保存失败")
                else:
                    year_failed += 1
                    if i % 100 == 0:
                        print(f"   ❌ [{i}/{len(stock_list)}] {stock_code}: 无数据")
                
                # 控制API调用频率
                if i % 10 == 0:
                    time.sleep(1)  # 每10个请求休息1秒
                else:
                    time.sleep(0.1)  # 短暂休息
            
            print(f"   🎯 {year}年完成: ✅{year_success} ❌{year_failed}")
            self.download_stats['successful_downloads'] += year_success
            self.download_stats['failed_downloads'] += year_failed
        
        self.create_download_summary()
    
    def create_download_summary(self):
        """创建下载总结"""
        end_time = datetime.now()
        duration = end_time - self.download_stats['start_time']
        
        summary = {
            'download_info': {
                'start_time': self.download_stats['start_time'].isoformat(),
                'end_time': end_time.isoformat(),
                'duration_hours': round(duration.total_seconds() / 3600, 2),
                'data_range': '2000-2024年',
                'api_used': 'MktEqudGet'
            },
            'statistics': {
                'total_stocks': self.download_stats['total_stocks'],
                'successful_downloads': self.download_stats['successful_downloads'],
                'failed_downloads': self.download_stats['failed_downloads'],
                'success_rate': f"{self.download_stats['successful_downloads']/(self.download_stats['successful_downloads']+self.download_stats['failed_downloads'])*100:.1f}%",
                'total_records': self.download_stats['total_records'],
                'total_size_mb': round(self.download_stats['data_size_mb'], 2),
                'total_size_gb': round(self.download_stats['data_size_mb'] / 1024, 2)
            },
            'file_structure': {
                'base_path': str(self.base_path),
                'organization': 'year_YYYY/*.csv',
                'file_format': 'stockcode_exchange.csv',
                'columns': 'secID,ticker,tradeDate,openPrice,highPrice,lowPrice,closePrice,volume,amount等'
            }
        }
        
        # 保存总结
        summary_file = self.base_path / 'download_summary.json'
        with open(summary_file, 'w', encoding='utf-8') as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
        
        print(f"\n🎊 全部A股历史数据下载完成!")
        print(f"=" * 80)
        print(f"📊 统计信息:")
        print(f"   🎯 总股票数: {summary['statistics']['total_stocks']}")
        print(f"   ✅ 成功下载: {summary['statistics']['successful_downloads']}")
        print(f"   ❌ 失败数量: {summary['statistics']['failed_downloads']}")
        print(f"   📈 成功率: {summary['statistics']['success_rate']}")
        print(f"   📝 总记录数: {summary['statistics']['total_records']:,}")
        print(f"   💾 数据大小: {summary['statistics']['total_size_gb']} GB")
        print(f"   ⏱️ 用时: {summary['download_info']['duration_hours']} 小时")
        print(f"   📁 存储位置: {self.base_path}")
        print(f"   📄 总结报告: {summary_file}")

def main():
    """主函数"""
    print("🚀 完整A股历史数据下载器")
    print("=" * 80)
    print("🎯 目标: 下载2000-2024年全部A股日线数据")
    print("📡 数据源: UQER MktEqudGet API")
    print("⚠️  注意: 这将是一个大型下载任务，可能需要数小时完成")
    
    # 直接开始下载
    downloader = CompleteMktEqudDownloader()
    downloader.download_complete_data()

if __name__ == "__main__":
    main()