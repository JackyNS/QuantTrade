#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
简化版个股日线数据下载器
直接下载知名A股的日线数据用于8月黄金交叉筛选
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

class SimpleMktEqudDownloader:
    """简化版个股日线数据下载器"""
    
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
    
    def get_known_a_stocks(self):
        """获取已知的A股股票列表"""
        # 主要的A股股票代码（涵盖各个行业和市值）
        known_stocks = [
            # 银行业
            '000001.XSHE',  # 平安银行
            '600036.XSHG',  # 招商银行
            '600000.XSHG',  # 浦发银行
            '601166.XSHG',  # 兴业银行
            '000002.XSHE',  # 万科A
            
            # 白酒食品
            '600519.XSHG',  # 贵州茅台
            '000858.XSHE',  # 五粮液
            '000568.XSHE',  # 泸州老窖
            '600887.XSHG',  # 伊利股份
            '002304.XSHE',  # 洋河股份
            
            # 科技股
            '000063.XSHE',  # 中兴通讯
            '002415.XSHE',  # 海康威视
            '000725.XSHE',  # 京东方A
            '300059.XSHE',  # 东方财富
            '002241.XSHE',  # 歌尔股份
            
            # 医药
            '600276.XSHG',  # 恒瑞医药
            '000661.XSHE',  # 长春高新
            '300015.XSHE',  # 爱尔眼科
            '002821.XSHE',  # 凯莱英
            
            # 新能源汽车
            '002594.XSHE',  # 比亚迪
            '300750.XSHE',  # 宁德时代
            '002129.XSHE',  # 中环股份
            '688981.XSHG',  # 中芯国际
            
            # 消费
            '600104.XSHG',  # 上汽集团
            '000338.XSHE',  # 潍柴动力
            '600009.XSHG',  # 上海机场
            '600031.XSHG',  # 三一重工
            
            # 房地产
            '000002.XSHE',  # 万科A (已重复)
            '001979.XSHE',  # 招商蛇口
            '600340.XSHG',  # 华夏幸福
            
            # 保险
            '601318.XSHG',  # 中国平安
            '601628.XSHG',  # 中国人寿
            '601601.XSHG',  # 中国太保
            
            # 券商
            '000166.XSHE',  # 申万宏源
            '600030.XSHG',  # 中信证券
            '000776.XSHE',  # 广发证券
            
            # 基建
            '600028.XSHG',  # 中国石化
            '601857.XSHG',  # 中国石油
            '000001.XSHG',  # 平安银行 (已重复，这里是上证指数，需要去除)
        ]
        
        # 去重并过滤
        unique_stocks = []
        seen = set()
        for stock in known_stocks:
            if stock not in seen and not stock.endswith('.XSHG') or not stock.startswith('000001.XSHG'):
                unique_stocks.append(stock)
                seen.add(stock)
        
        # 移除指数
        unique_stocks = [s for s in unique_stocks if s != '000001.XSHG']
        
        print(f"📋 准备下载 {len(unique_stocks)} 只知名A股")
        return unique_stocks
    
    def download_stock_data(self, stock_code, start_date='2020-01-01', end_date='2024-09-02'):
        """下载单只股票的日线数据"""
        try:
            print(f"   📡 API调用: MktEqudGet({stock_code})")
            
            # 调用UQER API
            result = uqer.DataAPI.MktEqudGet(
                secID=stock_code,
                beginDate=start_date.replace('-', ''),
                endDate=end_date.replace('-', ''),
                pandas=1
            )
            
            # 检查返回结果
            if result is None:
                print(f"   ❌ API返回None")
                return None
            
            if isinstance(result, str):
                print(f"   ❌ API返回错误信息: {result}")
                return None
            
            if not isinstance(result, pd.DataFrame):
                print(f"   ❌ API返回类型错误: {type(result)}")
                return None
                
            if len(result) == 0:
                print(f"   ❌ 返回数据为空")
                return None
            
            df = result.copy()
            
            # 数据清理
            if 'tradeDate' in df.columns:
                df['tradeDate'] = pd.to_datetime(df['tradeDate'])
            
            # 重命名列
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
            print(f"   ❌ 下载异常: {e}")
            return None
    
    def download_all_stocks(self):
        """下载所有股票数据"""
        print("🚀 开始下载A股日线数据")
        print("=" * 70)
        
        stock_list = self.get_known_a_stocks()
        
        successful_downloads = 0
        failed_downloads = 0
        
        for i, stock_code in enumerate(stock_list, 1):
            print(f"📊 [{i}/{len(stock_list)}] 下载: {stock_code}")
            
            # 检查文件是否已存在
            stock_file = self.base_path / f"{stock_code.replace('.', '_')}_daily.csv"
            if stock_file.exists():
                print(f"   ⏭️ 文件已存在，跳过")
                successful_downloads += 1
                continue
            
            # 下载数据
            stock_data = self.download_stock_data(stock_code)
            
            if stock_data is not None and len(stock_data) > 0:
                try:
                    # 保存数据
                    stock_data.to_csv(stock_file, index=False)
                    successful_downloads += 1
                    
                    print(f"   ✅ 成功: {len(stock_data)} 条记录")
                    if 'tradeDate' in stock_data.columns:
                        print(f"   📅 时间: {stock_data['tradeDate'].min().date()} - {stock_data['tradeDate'].max().date()}")
                    print(f"   💾 保存: {stock_file.name}")
                    
                except Exception as e:
                    print(f"   ❌ 保存失败: {e}")
                    failed_downloads += 1
            else:
                failed_downloads += 1
                print(f"   ❌ 下载失败或无数据")
            
            # 控制API调用频率
            time.sleep(1)  # 1秒间隔
            print()
        
        print(f"🎯 下载完成:")
        print(f"   ✅ 成功: {successful_downloads}")
        print(f"   ❌ 失败: {failed_downloads}")
        print(f"   📈 成功率: {successful_downloads/(successful_downloads+failed_downloads)*100:.1f}%")
        
        return successful_downloads, failed_downloads

def main():
    """主函数"""
    print("🚀 A股个股日线数据下载器 - 简化版")
    print("=" * 70)
    
    # 创建下载器
    downloader = SimpleMktEqudDownloader()
    
    # 下载数据
    success_count, fail_count = downloader.download_all_stocks()
    
    print(f"\n🎉 个股日线数据下载完成!")
    print(f"✅ 成功下载: {success_count} 只股票")
    print(f"📁 数据目录: {downloader.base_path}")
    print(f"💡 现在可以进行8月黄金交叉筛选了!")

if __name__ == "__main__":
    main()