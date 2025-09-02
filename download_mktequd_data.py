#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
下载个股日线数据 (MktEqudGet) - 这是进行技术分析的核心数据
补充之前遗漏的最重要数据
"""

import sys
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
import warnings
import time
import os
warnings.filterwarnings('ignore')

try:
    import uqer
    print("✅ UQER API 可用")
    UQER_AVAILABLE = True
except ImportError:
    print("❌ UQER API 不可用")
    UQER_AVAILABLE = False
    sys.exit(1)

class MktEqudDownloader:
    """个股日线数据下载器"""
    
    def __init__(self):
        """初始化下载器"""
        self.setup_uqer()
        self.setup_paths()
        
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
        self.base_path = Path("/Users/jackstudio/QuantTrade/data/mktequd_daily")
        self.base_path.mkdir(exist_ok=True)
        print(f"📁 数据存储路径: {self.base_path}")
    
    def get_stock_list(self):
        """获取A股股票列表"""
        print("📋 获取A股股票列表...")
        
        try:
            # 获取所有上市股票
            df = uqer.DataAPI.EquGet(
                listStatusCD='L',  # 仅上市状态的股票
                field=['secID','ticker','secShortName','listDate','exchangeCD'],
                pandas=1
            )
            
            if df is None or len(df) == 0:
                print("❌ 获取股票列表失败")
                return []
            
            # 过滤A股 (深交所XSHE和上交所XSHG)
            a_stocks = df[
                df['secID'].str.contains('.XSHE|.XSHG', na=False)
            ].copy()
            
            # 按交易所分组统计
            exchange_counts = a_stocks['exchangeCD'].value_counts()
            print(f"✅ 获取到 {len(a_stocks)} 只A股:")
            for exchange, count in exchange_counts.items():
                print(f"   {exchange}: {count} 只")
            
            return a_stocks['secID'].tolist()
            
        except Exception as e:
            print(f"❌ 获取股票列表失败: {e}")
            return []
    
    def download_stock_data(self, stock_code, start_date='2020-01-01', end_date='2024-09-02'):
        """
        下载单只股票的日线数据
        
        Args:
            stock_code: 股票代码
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            DataFrame: 股票日线数据
        """
        try:
            # 调用UQER API获取日线数据
            df = uqer.DataAPI.MktEqudGet(
                secID=stock_code,
                beginDate=start_date.replace('-', ''),
                endDate=end_date.replace('-', ''),
                field=['secID','ticker','secShortName','tradeDate','preClosePrice','openPrice','highestPrice','lowestPrice','closePrice','turnoverVol','turnoverValue','dealAmount','chg','chgPct','peTTM','pbMRQ','psTTM','pcfNcfTTM'],
                pandas=1
            )
            
            if df is None or len(df) == 0:
                return None
            
            # 清理和标准化数据
            df = df.copy()
            df['tradeDate'] = pd.to_datetime(df['tradeDate'])
            
            # 重命名列名以保持一致性
            column_mapping = {
                'highestPrice': 'highPrice',
                'lowestPrice': 'lowPrice',
                'turnoverVol': 'volume',
                'turnoverValue': 'amount',
                'chgPct': 'changePct'
            }
            df = df.rename(columns=column_mapping)
            
            # 过滤有效数据
            df = df.dropna(subset=['closePrice'])
            df = df[df['closePrice'] > 0]
            df = df.sort_values('tradeDate')
            
            return df
            
        except Exception as e:
            print(f"   ❌ 下载失败: {e}")
            return None
    
    def download_batch_data(self, stock_list, batch_size=10, max_stocks=100):
        """
        批量下载股票数据
        
        Args:
            stock_list: 股票代码列表
            batch_size: 批次大小
            max_stocks: 最大下载股票数（测试用）
        """
        print(f"📈 开始批量下载股票数据")
        print(f"   批次大小: {batch_size}")
        print(f"   限制数量: {max_stocks} (测试模式)")
        print("=" * 70)
        
        # 限制下载数量进行测试
        test_stocks = stock_list[:max_stocks]
        
        successful_downloads = 0
        failed_downloads = 0
        
        for i, stock_code in enumerate(test_stocks, 1):
            print(f"📊 [{i}/{len(test_stocks)}] 下载: {stock_code}")
            
            # 检查是否已存在
            stock_file = self.base_path / f"{stock_code.replace('.', '_')}_daily.csv"
            if stock_file.exists():
                print(f"   ⏭️ 已存在，跳过")
                continue
            
            # 下载数据
            stock_data = self.download_stock_data(stock_code)
            
            if stock_data is not None and len(stock_data) > 0:
                # 保存数据
                try:
                    stock_data.to_csv(stock_file, index=False)
                    successful_downloads += 1
                    print(f"   ✅ 成功: {len(stock_data)} 条记录")
                    print(f"   📅 范围: {stock_data['tradeDate'].min().date()} - {stock_data['tradeDate'].max().date()}")
                    print(f"   💾 保存: {stock_file}")
                except Exception as e:
                    print(f"   ❌ 保存失败: {e}")
                    failed_downloads += 1
            else:
                failed_downloads += 1
                print(f"   ❌ 无数据")
            
            # 控制API调用频率
            if i % batch_size == 0:
                print(f"   ⏸️ 休息2秒...")
                time.sleep(2)
            else:
                time.sleep(0.5)  # 短暂休息
            
            print()
        
        print(f"🎯 批量下载完成:")
        print(f"   ✅ 成功: {successful_downloads}")
        print(f"   ❌ 失败: {failed_downloads}")
        print(f"   📈 成功率: {successful_downloads/(successful_downloads+failed_downloads)*100:.1f}%")
        
        return successful_downloads, failed_downloads
    
    def create_summary(self):
        """创建下载汇总信息"""
        csv_files = list(self.base_path.glob("*.csv"))
        
        summary_data = {
            'download_time': datetime.now().isoformat(),
            'total_files': len(csv_files),
            'data_directory': str(self.base_path),
            'file_pattern': '*_daily.csv',
            'columns': [
                'secID', 'ticker', 'secShortName', 'tradeDate', 'preClosePrice',
                'openPrice', 'highPrice', 'lowPrice', 'closePrice', 'volume',
                'amount', 'dealAmount', 'chg', 'changePct', 'peTTM', 'pbMRQ',
                'psTTM', 'pcfNcfTTM'
            ],
            'sample_files': [f.name for f in csv_files[:10]]
        }
        
        # 保存汇总信息
        summary_file = self.base_path / "download_summary.json"
        import json
        with open(summary_file, 'w', encoding='utf-8') as f:
            json.dump(summary_data, f, ensure_ascii=False, indent=2)
        
        print(f"📄 汇总信息已保存: {summary_file}")
        return summary_data

def main():
    """主函数"""
    print("🚀 A股个股日线数据下载器 (MktEqudGet)")
    print("=" * 70)
    print("🎯 目标: 下载A股个股日线数据，用于技术分析")
    
    # 创建下载器
    downloader = MktEqudDownloader()
    
    # 获取股票列表
    stock_list = downloader.get_stock_list()
    
    if not stock_list:
        print("❌ 无法获取股票列表")
        return
    
    print(f"📊 准备下载 {len(stock_list)} 只股票的日线数据")
    
    # 批量下载数据 (先下载100只进行测试)
    success_count, fail_count = downloader.download_batch_data(
        stock_list, 
        batch_size=10, 
        max_stocks=100  # 测试模式：只下载前100只
    )
    
    # 创建汇总
    summary = downloader.create_summary()
    
    print(f"\n🎉 个股日线数据下载完成!")
    print(f"✅ 成功下载: {success_count} 只股票")
    print(f"📁 数据目录: {downloader.base_path}")
    print(f"💡 现在可以进行8月黄金交叉筛选了!")

if __name__ == "__main__":
    main()