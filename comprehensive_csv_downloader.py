#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
全面CSV格式A股数据补全器
专门用于补全被清理的数据，统一使用CSV格式
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

class ComprehensiveCSVDownloader:
    """全面CSV数据补全器"""
    
    def __init__(self):
        """初始化下载器"""
        self.setup_uqer()
        self.setup_paths()
        self.download_stats = {
            'start_time': datetime.now(),
            'total_stocks': 0,
            'successful_downloads': 0,
            'failed_downloads': 0,
            'skipped_existing': 0,
            'total_records': 0,
            'data_size_mb': 0
        }
        self.progress_lock = Lock()
        
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
        # 主数据目录
        self.base_path = Path("/Users/jackstudio/QuantTrade/data/csv_complete")
        self.base_path.mkdir(exist_ok=True)
        
        # 日线数据目录
        self.daily_path = self.base_path / "daily"
        self.daily_path.mkdir(exist_ok=True)
        
        # 按股票代码组织（便于查找和使用）
        self.stock_path = self.daily_path / "stocks"
        self.stock_path.mkdir(exist_ok=True)
        
        # 年度汇总目录（便于按年份查看）
        self.yearly_path = self.daily_path / "yearly"
        self.yearly_path.mkdir(exist_ok=True)
        
        for year in range(2000, 2026):  # 包含2025年
            year_dir = self.yearly_path / f"year_{year}"
            year_dir.mkdir(exist_ok=True)
        
        print(f"📁 CSV数据存储路径: {self.base_path}")
        print(f"   📊 按股票: {self.stock_path}")
        print(f"   📅 按年份: {self.yearly_path}")
    
    def get_all_a_stocks(self):
        """获取全部A股股票列表"""
        print("📋 获取全部A股股票列表...")
        
        try:
            # 获取当前上市股票
            current_result = uqer.DataAPI.EquGet(
                listStatusCD='L',
                pandas=1
            )
            
            # 获取已退市股票  
            delisted_result = uqer.DataAPI.EquGet(
                listStatusCD='DE',
                pandas=1
            )
            
            all_stocks = []
            
            # 处理当前上市股票
            if current_result is not None:
                if isinstance(current_result, str):
                    current_df = pd.read_csv(StringIO(current_result))
                else:
                    current_df = current_result
                
                if len(current_df) > 0:
                    all_stocks.append(current_df)
                    print(f"   📈 当前上市: {len(current_df)} 只")
            
            # 处理已退市股票
            if delisted_result is not None:
                if isinstance(delisted_result, str):
                    delisted_df = pd.read_csv(StringIO(delisted_result))
                else:
                    delisted_df = delisted_result
                
                if len(delisted_df) > 0:
                    all_stocks.append(delisted_df)
                    print(f"   📉 已退市: {len(delisted_df)} 只")
            
            if not all_stocks:
                print("❌ 获取股票列表失败")
                return []
            
            # 合并数据
            df = pd.concat(all_stocks, ignore_index=True)
            
            # 过滤A股
            a_stocks = df[
                df['secID'].str.contains('.XSHE|.XSHG', na=False)
            ].copy()
            
            # 排除指数
            a_stocks = a_stocks[
                ~a_stocks['secID'].str.contains('.ZICN|.INDX|.XBEI', na=False)
            ]
            
            stock_list = a_stocks['secID'].unique().tolist()
            stock_list.sort()  # 排序便于管理
            
            print(f"✅ 获取到 {len(stock_list)} 只A股")
            self.download_stats['total_stocks'] = len(stock_list)
            
            # 保存股票列表
            list_file = self.base_path / 'a_stock_list.csv'
            a_stocks[['secID', 'ticker', 'secShortName', 'exchangeCD', 'listStatusCD', 'listDate', 'delistDate']].to_csv(
                list_file, index=False, encoding='utf-8'
            )
            print(f"📄 股票列表已保存: {list_file}")
            
            return stock_list
            
        except Exception as e:
            print(f"❌ 获取股票列表失败: {e}")
            return []
    
    def download_stock_data(self, stock_code, start_date='2000-01-01', end_date='2025-08-31'):
        """下载单只股票的完整历史数据"""
        try:
            # 检查是否已存在
            stock_file = self.stock_path / f"{stock_code.replace('.', '_')}.csv"
            if stock_file.exists():
                with self.progress_lock:
                    self.download_stats['skipped_existing'] += 1
                return {'status': 'exists', 'file': stock_file}
            
            # 调用UQER API
            result = uqer.DataAPI.MktEqudGet(
                secID=stock_code,
                beginDate=start_date.replace('-', ''),
                endDate=end_date.replace('-', ''),
                pandas=1
            )
            
            if result is None:
                return {'status': 'no_data', 'stock_code': stock_code}
            
            # 处理API返回的数据
            if isinstance(result, str):
                df = pd.read_csv(StringIO(result))
            elif isinstance(result, pd.DataFrame):
                df = result.copy()
            else:
                return {'status': 'invalid_format', 'stock_code': stock_code}
            
            if len(df) == 0:
                return {'status': 'empty_data', 'stock_code': stock_code}
            
            # 数据处理和标准化
            df = self.process_stock_data(df, stock_code)
            
            if df is None or len(df) == 0:
                return {'status': 'processing_failed', 'stock_code': stock_code}
            
            # 保存到按股票组织的目录
            df.to_csv(stock_file, index=False, encoding='utf-8')
            
            # 同时按年份保存
            self.save_by_year(df, stock_code)
            
            # 更新统计
            with self.progress_lock:
                self.download_stats['successful_downloads'] += 1
                self.download_stats['total_records'] += len(df)
                
                file_size = stock_file.stat().st_size / 1024 / 1024  # MB
                self.download_stats['data_size_mb'] += file_size
            
            return {
                'status': 'success',
                'stock_code': stock_code,
                'records': len(df),
                'file': stock_file,
                'date_range': f"{df['tradeDate'].min()} - {df['tradeDate'].max()}"
            }
            
        except Exception as e:
            with self.progress_lock:
                self.download_stats['failed_downloads'] += 1
            return {'status': 'error', 'stock_code': stock_code, 'error': str(e)}
    
    def process_stock_data(self, df, stock_code):
        """处理和标准化股票数据"""
        try:
            # 确保必要的列存在
            if 'tradeDate' not in df.columns or 'closePrice' not in df.columns:
                return None
            
            # 转换日期
            df['tradeDate'] = pd.to_datetime(df['tradeDate'])
            
            # 标准化列名
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
            
            # 确保股票代码正确
            df['secID'] = stock_code
            
            # 添加衍生字段
            if 'changePct' not in df.columns and 'preClosePrice' in df.columns:
                df['changePct'] = (df['closePrice'] - df['preClosePrice']) / df['preClosePrice'] * 100
            
            # 重新排列列顺序
            standard_columns = [
                'secID', 'ticker', 'secShortName', 'exchangeCD', 'tradeDate',
                'preClosePrice', 'openPrice', 'highPrice', 'lowPrice', 'closePrice',
                'volume', 'amount', 'changePct'
            ]
            
            # 保留存在的列
            available_columns = [col for col in standard_columns if col in df.columns]
            other_columns = [col for col in df.columns if col not in standard_columns]
            final_columns = available_columns + other_columns
            
            df = df[final_columns]
            
            return df.reset_index(drop=True)
            
        except Exception as e:
            return None
    
    def save_by_year(self, df, stock_code):
        """按年份保存数据"""
        try:
            df['year'] = df['tradeDate'].dt.year
            
            for year, year_data in df.groupby('year'):
                if 2000 <= year <= 2025:
                    year_file = self.yearly_path / f"year_{year}" / f"{stock_code.replace('.', '_')}.csv"
                    
                    # 如果年份文件已存在，追加数据
                    if year_file.exists():
                        existing_df = pd.read_csv(year_file)
                        existing_df['tradeDate'] = pd.to_datetime(existing_df['tradeDate'])
                        
                        # 合并并去重
                        combined_df = pd.concat([existing_df, year_data], ignore_index=True)
                        combined_df = combined_df.drop_duplicates(subset=['tradeDate']).sort_values('tradeDate')
                        combined_df.to_csv(year_file, index=False, encoding='utf-8')
                    else:
                        year_data_clean = year_data.drop(columns=['year'])
                        year_data_clean.to_csv(year_file, index=False, encoding='utf-8')
                        
        except Exception as e:
            pass  # 年份保存失败不影响主流程
    
    def download_all_stocks(self, batch_size=10, max_workers=5):
        """批量下载所有股票数据"""
        print("🚀 开始全面补全A股CSV数据")
        print("   📊 格式: 统一CSV格式")
        print("   🎯 范围: 全部A股 2000年1月1日-2025年8月31日")
        print("   🔧 方式: 多线程并发下载")
        print("=" * 80)
        
        stock_list = self.get_all_a_stocks()
        
        if not stock_list:
            print("❌ 无法获取股票列表")
            return
        
        print(f"📊 准备下载 {len(stock_list)} 只股票...")
        
        # 分批处理
        total_batches = (len(stock_list) + batch_size - 1) // batch_size
        
        for batch_idx in range(0, len(stock_list), batch_size):
            batch_stocks = stock_list[batch_idx:batch_idx + batch_size]
            current_batch = batch_idx // batch_size + 1
            
            print(f"\n📦 批次 {current_batch}/{total_batches}: 处理 {len(batch_stocks)} 只股票")
            
            # 多线程下载当前批次
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {
                    executor.submit(self.download_stock_data, stock_code): stock_code 
                    for stock_code in batch_stocks
                }
                
                batch_results = []
                for future in concurrent.futures.as_completed(futures):
                    stock_code = futures[future]
                    try:
                        result = future.result()
                        batch_results.append(result)
                        
                        # 打印进度
                        if result['status'] == 'success':
                            print(f"   ✅ {stock_code}: {result['records']} 条记录")
                        elif result['status'] == 'exists':
                            print(f"   ⏩ {stock_code}: 已存在")
                        else:
                            print(f"   ❌ {stock_code}: {result['status']}")
                            
                    except Exception as e:
                        print(f"   💥 {stock_code}: 下载异常 {e}")
            
            # 批次完成统计
            batch_success = sum(1 for r in batch_results if r['status'] == 'success')
            batch_exists = sum(1 for r in batch_results if r['status'] == 'exists')
            batch_failed = len(batch_results) - batch_success - batch_exists
            
            print(f"   📊 批次结果: ✅{batch_success} ⏩{batch_exists} ❌{batch_failed}")
            print(f"   📈 总体进度: {self.download_stats['successful_downloads'] + self.download_stats['skipped_existing']}/{len(stock_list)}")
            
            # 批次间休息
            if current_batch < total_batches:
                time.sleep(2)
        
        # 创建下载总结
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
                'data_format': 'CSV',
                'data_range': '2000年1月1日-2025年8月31日',
                'organization': 'stocks/ 和 yearly/ 双重组织'
            },
            'statistics': {
                'total_stocks': self.download_stats['total_stocks'],
                'successful_downloads': self.download_stats['successful_downloads'],
                'skipped_existing': self.download_stats['skipped_existing'],
                'failed_downloads': self.download_stats['failed_downloads'],
                'total_completed': self.download_stats['successful_downloads'] + self.download_stats['skipped_existing'],
                'completion_rate': f"{(self.download_stats['successful_downloads'] + self.download_stats['skipped_existing'])/self.download_stats['total_stocks']*100:.1f}%",
                'total_records': self.download_stats['total_records'],
                'total_size_mb': round(self.download_stats['data_size_mb'], 2),
                'total_size_gb': round(self.download_stats['data_size_mb'] / 1024, 2)
            },
            'file_structure': {
                'base_path': str(self.base_path),
                'stocks_directory': str(self.stock_path),
                'yearly_directory': str(self.yearly_path),
                'stock_list_file': str(self.base_path / 'a_stock_list.csv'),
                'file_naming': 'XXXXXX_XXXX.csv (如 000001_XSHE.csv)'
            }
        }
        
        # 保存总结
        summary_file = self.base_path / 'download_summary.json'
        with open(summary_file, 'w', encoding='utf-8') as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
        
        print(f"\n🎊 A股CSV数据补全完成!")
        print(f"=" * 80)
        print(f"📊 最终统计:")
        print(f"   🎯 目标股票: {summary['statistics']['total_stocks']}")
        print(f"   ✅ 成功下载: {summary['statistics']['successful_downloads']}")
        print(f"   ⏩ 已存在: {summary['statistics']['skipped_existing']}")
        print(f"   ❌ 失败数量: {summary['statistics']['failed_downloads']}")
        print(f"   📈 完成率: {summary['statistics']['completion_rate']}")
        print(f"   📝 总记录数: {summary['statistics']['total_records']:,}")
        print(f"   💾 数据大小: {summary['statistics']['total_size_gb']} GB")
        print(f"   ⏱️ 用时: {summary['download_info']['duration_hours']} 小时")
        print(f"   📁 存储位置: {self.base_path}")
        print(f"   📄 详细报告: {summary_file}")

def main():
    """主函数"""
    print("🔄 全面A股CSV数据补全器")
    print("=" * 80)
    print("🎯 目标: 补全被清理的数据，统一CSV格式")
    print("📅 时间: 2000年1月1日 - 2025年8月31日")
    print("📡 数据源: UQER MktEqudGet API")
    print("🔧 特点: 多线程并发 + 双重文件组织")
    
    downloader = ComprehensiveCSVDownloader()
    downloader.download_all_stocks(batch_size=20, max_workers=8)

if __name__ == "__main__":
    main()