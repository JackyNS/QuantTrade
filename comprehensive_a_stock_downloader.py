#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
全面A股数据下载器
基于专业版验证成功，扩大范围到全部A股
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

class ComprehensiveAStockDownloader:
    """全面A股数据下载器"""
    
    def __init__(self):
        """初始化下载器"""
        self.setup_uqer()
        self.setup_paths()
        self.stats = {
            'start_time': datetime.now(),
            'total_stocks': 0,
            'successful_stocks': 0,
            'failed_stocks': 0,
            'total_api_calls': 0,
            'total_records': 0,
            'total_files': 0
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
        # 全面数据路径
        self.base_path = Path("/Users/jackstudio/QuantTrade/data/comprehensive_complete")
        self.base_path.mkdir(exist_ok=True)
        
        # 数据类型映射
        self.data_types = {
            'daily': '日线数据',
            'daily_adj': '前复权日线 [极速版]',
            'weekly': '周线数据', 
            'weekly_adj': '前复权周线 [极速版]',
            'monthly': '月线数据',
            'monthly_adj': '前复权月线'
        }
        
        # API映射
        self.api_mappings = {
            'daily': uqer.DataAPI.MktEqudGet,
            'daily_adj': uqer.DataAPI.MktEqudAdjGet,
            'weekly': uqer.DataAPI.MktEquwGet,
            'weekly_adj': uqer.DataAPI.MktEquwAdjGet,
            'monthly': uqer.DataAPI.MktEqumGet,
            'monthly_adj': uqer.DataAPI.MktEqumAdjGet
        }
        
        # 创建目录结构
        for data_type in self.data_types:
            type_path = self.base_path / data_type / "stocks"
            type_path.mkdir(parents=True, exist_ok=True)
            
        print(f"📁 全面数据存储路径: {self.base_path}")
        
    def get_all_a_stocks(self):
        """获取全部A股列表（上市+退市）"""
        print("📋 获取全部A股股票列表...")
        
        all_stocks = []
        
        try:
            # 获取当前上市的A股
            print("   📈 获取上市A股...")
            listed_result = uqer.DataAPI.EquGet(
                listStatusCD="L",  # 上市状态
                pandas=1
            )
            
            if isinstance(listed_result, str):
                listed_df = pd.read_csv(StringIO(listed_result))
            else:
                listed_df = listed_result
                
            listed_a_stocks = listed_df[listed_df['secID'].str.contains(r'\.(XSHE|XSHG)$', na=False)]
            all_stocks.extend(listed_a_stocks['secID'].tolist())
            print(f"      ✅ 上市A股: {len(listed_a_stocks)} 只")
            
            # 获取退市的A股
            print("   📉 获取退市A股...")
            delisted_result = uqer.DataAPI.EquGet(
                listStatusCD="DE",  # 退市状态
                pandas=1
            )
            
            if isinstance(delisted_result, str):
                delisted_df = pd.read_csv(StringIO(delisted_result))
            else:
                delisted_df = delisted_result
                
            delisted_a_stocks = delisted_df[delisted_df['secID'].str.contains(r'\.(XSHE|XSHG)$', na=False)]
            all_stocks.extend(delisted_a_stocks['secID'].tolist())
            print(f"      ✅ 退市A股: {len(delisted_a_stocks)} 只")
            
        except Exception as e:
            print(f"❌ 获取股票列表失败: {e}")
            return []
        
        # 去重并排序
        all_stocks = list(set(all_stocks))
        all_stocks.sort()
        
        print(f"   📊 全部A股总数: {len(all_stocks)} 只")
        
        # 保存股票列表
        stocks_info = {
            'total_count': len(all_stocks),
            'listed_count': len(listed_a_stocks),
            'delisted_count': len(delisted_a_stocks),
            'stock_list': all_stocks,
            'update_time': datetime.now().isoformat()
        }
        
        with open(self.base_path / 'a_stocks_list.json', 'w', encoding='utf-8') as f:
            json.dump(stocks_info, f, ensure_ascii=False, indent=2)
            
        return all_stocks
    
    def download_stock_data(self, stock_id, batch_id=1, total_stocks=1):
        """下载单只股票的所有时间周期数据"""
        
        print(f"📈 [{batch_id}/{total_stocks}] 处理: {stock_id}")
        
        success_count = 0
        total_records = 0
        
        for data_type, api_func in self.api_mappings.items():
            try:
                result = api_func(
                    secID=stock_id,
                    beginDate="20000101",
                    endDate="20250831",  # 恢复到原始正确目标
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
                
                # 验证时间范围
                date_column = 'tradeDate' if 'tradeDate' in df.columns else 'endDate'
                if date_column in df.columns:
                    df[date_column] = pd.to_datetime(df[date_column])
                    latest_date = df[date_column].max()
                    earliest_date = df[date_column].min()
                    
                    # 保存数据
                    save_path = self.base_path / data_type / "stocks"
                    file_name = f"{stock_id.replace('.', '_')}.csv"
                    file_path = save_path / file_name
                    
                    df.to_csv(file_path, index=False, encoding='utf-8')
                    success_count += 1
                    total_records += len(df)
                    self.stats['total_files'] += 1
                    
                    print(f"   ✅ {data_type}: {len(df)} 条记录 ({earliest_date.date()} - {latest_date.date()})")
                
                time.sleep(0.15)  # 适当限速
                
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
    
    def download_comprehensive_data(self, stocks_list, batch_size=100, max_stocks=None):
        """全面下载A股数据"""
        print(f"🚀 开始全面A股数据下载")
        print(f"📅 时间范围: 2000年1月1日 - 2025年8月31日")
        print(f"📊 股票总数: {len(stocks_list)}")
        print(f"📦 批次大小: {batch_size}")
        
        if max_stocks:
            stocks_list = stocks_list[:max_stocks]
            print(f"🎯 本次限制: {max_stocks} 只股票")
            
        print("=" * 80)
        
        total_stocks = len(stocks_list)
        self.stats['total_stocks'] = total_stocks
        
        # 分批处理
        for batch_start in range(0, total_stocks, batch_size):
            batch_end = min(batch_start + batch_size, total_stocks)
            batch_stocks = stocks_list[batch_start:batch_end]
            batch_num = batch_start // batch_size + 1
            
            print(f"\\n📦 第{batch_num}批: 处理第{batch_start+1}-{batch_end}只股票")
            print("-" * 60)
            
            # 处理当前批次
            for i, stock_id in enumerate(batch_stocks, 1):
                global_index = batch_start + i
                self.download_stock_data(stock_id, global_index, total_stocks)
                
                # 每25只显示进度
                if i % 25 == 0 or i == len(batch_stocks):
                    progress = (global_index / total_stocks) * 100
                    print(f"   📈 批次进度: {i}/{len(batch_stocks)} | 总体进度: {global_index}/{total_stocks} ({progress:.1f}%)")
            
            # 批次间休息
            if batch_end < total_stocks:
                print(f"⏸️ 批次完成，休息60秒...")
                time.sleep(60)
        
        self.create_summary()
    
    def create_summary(self):
        """创建下载总结"""
        end_time = datetime.now()
        duration = end_time - self.stats['start_time']
        success_rate = (self.stats['successful_stocks'] / self.stats['total_stocks'] * 100) if self.stats['total_stocks'] > 0 else 0
        
        summary = {
            'comprehensive_download_info': {
                'start_time': self.stats['start_time'].isoformat(),
                'end_time': end_time.isoformat(),
                'duration_minutes': round(duration.total_seconds() / 60, 2),
                'data_range': '2000年1月1日 - 2025年8月31日',
                'download_strategy': '全面A股数据下载（上市+退市）'
            },
            'statistics': {
                'total_stocks': self.stats['total_stocks'],
                'successful_stocks': self.stats['successful_stocks'],
                'failed_stocks': self.stats['failed_stocks'],
                'success_rate': f"{success_rate:.1f}%",
                'total_api_calls': self.stats['total_api_calls'],
                'total_records': self.stats['total_records'],
                'total_files': self.stats['total_files']
            },
            'data_types': self.data_types,
            'file_structure': {
                'base_path': str(self.base_path),
                'organization': '按数据类型和股票组织',
                'naming_convention': 'XXXXXX_XXXX.csv'
            }
        }
        
        # 保存总结
        summary_file = self.base_path / 'comprehensive_download_summary.json'
        with open(summary_file, 'w', encoding='utf-8') as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
        
        print(f"\\n🎊 全面A股数据下载完成!")
        print("=" * 80)
        print(f"📊 下载统计:")
        print(f"   📈 处理股票: {summary['statistics']['total_stocks']}")
        print(f"   ✅ 成功股票: {summary['statistics']['successful_stocks']}")
        print(f"   ❌ 失败股票: {summary['statistics']['failed_stocks']}")
        print(f"   🎯 成功率: {summary['statistics']['success_rate']}")
        print(f"   📞 API调用: {summary['statistics']['total_api_calls']}")
        print(f"   📋 总记录: {summary['statistics']['total_records']}")
        print(f"   📄 总文件: {summary['statistics']['total_files']}")
        print(f"   ⏱️ 用时: {summary['comprehensive_download_info']['duration_minutes']} 分钟")
        print(f"   📁 存储位置: {self.base_path}")
        print(f"   📄 详细报告: {summary_file}")

def main():
    """主函数"""
    print("🌟 全面A股数据下载器")
    print("=" * 80)
    print("🎯 目标: 基于专业版验证成功，下载全部A股数据")
    print("📡 数据源: UQER极速版 (268个API)")
    print("📅 时间: 2000年1月1日 - 2025年8月31日")
    print("🏗️ 策略: 分批下载，先测试500只")
    
    downloader = ComprehensiveAStockDownloader()
    
    # 获取全部A股列表
    stocks = downloader.get_all_a_stocks()
    
    if not stocks:
        print("❌ 无法获取股票列表")
        return
    
    # 开始下载（先测试500只）
    downloader.download_comprehensive_data(stocks, batch_size=50, max_stocks=500)

if __name__ == "__main__":
    main()