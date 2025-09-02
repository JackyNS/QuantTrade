#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
现实可行的A股数据重建器
调整时间范围到2024年12月31日，分批高效重建
"""

import sys
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
import warnings
import json
import time
import os
from io import StringIO
import concurrent.futures
from threading import Lock
warnings.filterwarnings('ignore')

try:
    import uqer
    print("✅ UQER API 可用")
    UQER_AVAILABLE = True
except ImportError:
    print("❌ UQER API 不可用")
    UQER_AVAILABLE = False
    sys.exit(1)

class RealisticDataRebuilder:
    """现实可行的数据重建器"""
    
    def __init__(self):
        """初始化重建器"""
        self.setup_uqer()
        self.setup_paths()
        self.setup_time_ranges()
        self.stats = {
            'start_time': datetime.now(),
            'total_stocks': 0,
            'successful_stocks': 0,
            'failed_stocks': 0,
            'total_api_calls': 0,
            'total_records': 0
        }
        
    def setup_uqer(self):
        """设置UQER连接"""
        try:
            uqer_token = "68b9922817ae6273137bda7acba81e293582ba347281dfcc056dcb245b23faf3"
            uqer.Client(token=uqer_token)
            print("✅ UQER极速版连接成功")
            self.uqer_connected = True
        except Exception as e:
            print(f"❌ UQER连接失败: {e}")
            self.uqer_connected = False
            sys.exit(1)
    
    def setup_paths(self):
        """设置路径"""
        # 新的现实数据路径
        self.base_path = Path("/Users/jackstudio/QuantTrade/data/realistic_complete")
        self.base_path.mkdir(exist_ok=True)
        
        # 按数据类型组织
        self.data_types = {
            'daily': '日线数据',
            'daily_adj': '前复权日线',
            'weekly': '周线数据', 
            'weekly_adj': '前复权周线',
            'monthly': '月线数据',
            'monthly_adj': '前复权月线'
        }
        
        # 创建目录结构
        for data_type in self.data_types:
            type_path = self.base_path / data_type / "stocks"
            type_path.mkdir(parents=True, exist_ok=True)
            
        print(f"📁 现实数据重建路径: {self.base_path}")
        
    def setup_time_ranges(self):
        """设置现实的时间范围"""
        self.start_date = "20000101"  # 2000年1月1日
        self.end_date = "20241231"    # 2024年12月31日 (现实可行)
        
        print(f"📅 调整后时间范围: {self.start_date} - {self.end_date}")
        print(f"🎯 目标: 获取实际存在的历史数据，不追求未来数据")
    
    def get_active_stocks(self):
        """获取当前活跃A股列表"""
        print("📋 获取活跃A股股票列表...")
        
        try:
            # 获取当前上市的A股
            result = uqer.DataAPI.EquGet(
                listStatusCD="L",  # 上市状态
                pandas=1
            )
            
            if isinstance(result, str):
                df = pd.read_csv(StringIO(result))
            else:
                df = result
                
            # 筛选A股
            a_stocks = df[df['secID'].str.contains(r'\.(XSHE|XSHG)$', na=False)]
            
            print(f"   📈 活跃A股: {len(a_stocks)} 只")
            
            # 按上市时间排序，优先处理老股票
            if 'listDate' in a_stocks.columns:
                a_stocks['listDate'] = pd.to_datetime(a_stocks['listDate'])
                a_stocks = a_stocks.sort_values('listDate')
            
            return a_stocks['secID'].tolist()
            
        except Exception as e:
            print(f"❌ 获取股票列表失败: {e}")
            return []
    
    def download_stock_data(self, stock_id, batch_id=1, total_stocks=1):
        """下载单只股票的所有时间周期数据"""
        
        api_mappings = {
            'daily': uqer.DataAPI.MktEqudGet,
            'daily_adj': uqer.DataAPI.MktEqudAdjGet,
            'weekly': uqer.DataAPI.MktEquwGet,
            'weekly_adj': uqer.DataAPI.MktEquwAdjGet,
            'monthly': uqer.DataAPI.MktEqumGet,
            'monthly_adj': uqer.DataAPI.MktEqumAdjGet
        }
        
        print(f"📈 [{batch_id}/{total_stocks}] 处理: {stock_id}")
        
        success_count = 0
        total_records = 0
        
        for data_type, api_func in api_mappings.items():
            try:
                result = api_func(
                    secID=stock_id,
                    beginDate=self.start_date,
                    endDate=self.end_date,
                    pandas=1
                )
                
                self.stats['total_api_calls'] += 1
                
                if result is None:
                    continue
                
                # 处理API返回数据
                if isinstance(result, str):
                    df = pd.read_csv(StringIO(result))
                elif isinstance(result, pd.DataFrame):
                    df = result.copy()
                else:
                    continue
                
                if len(df) == 0:
                    continue
                
                # 验证时间范围（现实标准）
                if 'tradeDate' in df.columns:
                    df['tradeDate'] = pd.to_datetime(df['tradeDate'])
                    latest_date = df['tradeDate'].max()
                    
                    # 放宽时间要求：只要有2023年以后的数据就认为有效
                    if latest_date >= pd.Timestamp('2023-01-01'):
                        # 保存数据
                        save_path = self.base_path / data_type / "stocks"
                        file_name = f"{stock_id.replace('.', '_')}.csv"
                        file_path = save_path / file_name
                        
                        df.to_csv(file_path, index=False, encoding='utf-8')
                        success_count += 1
                        total_records += len(df)
                        
                        print(f"   ✅ {data_type}: {len(df)} 条记录 (到 {latest_date.date()})")
                    else:
                        print(f"   ⚠️ {data_type}: 数据过旧 (仅到 {latest_date.date()})")
                
                time.sleep(0.1)  # 适当限速
                
            except Exception as e:
                print(f"   ❌ {data_type}: 下载失败")
                continue
        
        if success_count > 0:
            print(f"   ✅ 成功: {success_count}/6 个时间周期，总计 {total_records} 条记录")
            self.stats['successful_stocks'] += 1
            self.stats['total_records'] += total_records
        else:
            print(f"   ❌ 失败: 0/6 个时间周期成功")
            self.stats['failed_stocks'] += 1
        
        return success_count > 0
    
    def rebuild_data_batch(self, stocks_list, batch_size=50):
        """分批重建数据"""
        print(f"🚀 开始现实数据重建")
        print(f"📅 时间范围: 2000年1月1日 - 2024年12月31日")
        print(f"📊 股票数量: {len(stocks_list)}")
        print(f"📦 批次大小: {batch_size}")
        print("=" * 80)
        
        total_stocks = len(stocks_list)
        self.stats['total_stocks'] = total_stocks
        
        # 分批处理
        for batch_start in range(0, total_stocks, batch_size):
            batch_end = min(batch_start + batch_size, total_stocks)
            batch_stocks = stocks_list[batch_start:batch_end]
            batch_num = batch_start // batch_size + 1
            
            print(f"\n📦 第{batch_num}批: 处理第{batch_start+1}-{batch_end}只股票")
            print("-" * 60)
            
            # 处理当前批次
            for i, stock_id in enumerate(batch_stocks, 1):
                global_index = batch_start + i
                self.download_stock_data(stock_id, global_index, total_stocks)
                
                # 每10只显示进度
                if i % 10 == 0 or i == len(batch_stocks):
                    progress = (global_index / total_stocks) * 100
                    print(f"   📈 批次进度: {i}/{len(batch_stocks)} | 总体进度: {global_index}/{total_stocks} ({progress:.1f}%)")
            
            # 批次间休息
            if batch_end < total_stocks:
                print(f"⏸️ 批次完成，休息30秒...")
                time.sleep(30)
        
        self.create_summary()
    
    def create_summary(self):
        """创建重建总结"""
        end_time = datetime.now()
        duration = end_time - self.stats['start_time']
        success_rate = (self.stats['successful_stocks'] / self.stats['total_stocks'] * 100) if self.stats['total_stocks'] > 0 else 0
        
        summary = {
            'realistic_rebuild_info': {
                'start_time': self.stats['start_time'].isoformat(),
                'end_time': end_time.isoformat(),
                'duration_minutes': round(duration.total_seconds() / 60, 2),
                'data_range': '2000年1月1日 - 2024年12月31日',
                'rebuild_strategy': '现实可行的历史数据重建'
            },
            'statistics': {
                'total_stocks': self.stats['total_stocks'],
                'successful_stocks': self.stats['successful_stocks'],
                'failed_stocks': self.stats['failed_stocks'],
                'success_rate': f"{success_rate:.1f}%",
                'total_api_calls': self.stats['total_api_calls'],
                'total_records': self.stats['total_records']
            },
            'data_types': self.data_types,
            'file_structure': {
                'base_path': str(self.base_path),
                'organization': '按数据类型和股票组织',
                'naming_convention': 'XXXXXX_XXXX.csv'
            }
        }
        
        # 保存总结
        summary_file = self.base_path / 'realistic_rebuild_summary.json'
        with open(summary_file, 'w', encoding='utf-8') as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
        
        print(f"\n🎊 现实数据重建完成!")
        print("=" * 80)
        print(f"📊 重建统计:")
        print(f"   📈 处理股票: {summary['statistics']['total_stocks']}")
        print(f"   ✅ 成功股票: {summary['statistics']['successful_stocks']}")
        print(f"   ❌ 失败股票: {summary['statistics']['failed_stocks']}")
        print(f"   🎯 成功率: {summary['statistics']['success_rate']}")
        print(f"   📞 API调用: {summary['statistics']['total_api_calls']}")
        print(f"   📋 总记录: {summary['statistics']['total_records']}")
        print(f"   ⏱️ 用时: {summary['realistic_rebuild_info']['duration_minutes']} 分钟")
        print(f"   📁 存储位置: {self.base_path}")
        print(f"   📄 详细报告: {summary_file}")

def main():
    """主函数"""
    print("🔄 现实可行的A股数据重建器")
    print("=" * 80)
    print("🎯 策略: 调整时间范围到现实可行的2024年12月31日")
    print("📡 数据源: UQER极速版 (268个API)")
    print("🏗️ 重建方式: 分批处理，优先活跃股票")
    
    rebuilder = RealisticDataRebuilder()
    
    # 获取活跃股票列表
    stocks = rebuilder.get_active_stocks()
    
    if not stocks:
        print("❌ 无法获取股票列表")
        return
    
    # 开始重建
    rebuilder.rebuild_data_batch(stocks, batch_size=50)

if __name__ == "__main__":
    main()