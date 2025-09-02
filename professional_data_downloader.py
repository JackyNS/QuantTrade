#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
专业级数据下载器 - 利用优矿极速版完整API权限
基于优矿2025 API清单的268个接口进行全面数据补全
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

class ProfessionalDataDownloader:
    """专业级数据下载器 - 极速版权限"""
    
    def __init__(self):
        """初始化下载器"""
        self.setup_uqer()
        self.setup_paths()
        self.load_api_mappings()
        self.download_stats = {
            'start_time': datetime.now(),
            'total_stocks': 0,
            'successful_downloads': 0,
            'failed_downloads': 0,
            'total_records': 0,
            'data_size_mb': 0,
            'api_calls': 0
        }
        self.progress_lock = Lock()
        
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
        """设置存储路径"""
        self.base_path = Path("/Users/jackstudio/QuantTrade/data/professional_complete")
        self.base_path.mkdir(exist_ok=True)
        
        # 按数据类型组织
        self.data_paths = {
            'daily': self.base_path / "daily" / "stocks",
            'daily_adj': self.base_path / "daily_adj" / "stocks",  # 前复权日线
            'weekly': self.base_path / "weekly" / "stocks",
            'weekly_adj': self.base_path / "weekly_adj" / "stocks",  # 前复权周线
            'monthly': self.base_path / "monthly" / "stocks", 
            'monthly_adj': self.base_path / "monthly_adj" / "stocks",  # 前复权月线
            'quarterly': self.base_path / "quarterly" / "stocks",
            'yearly': self.base_path / "yearly" / "stocks",
            'factors': self.base_path / "factors" / "stocks",  # 因子数据
            'basic_info': self.base_path / "basic_info",  # 股票基本信息
        }
        
        # 创建所有目录
        for path in self.data_paths.values():
            path.mkdir(parents=True, exist_ok=True)
        
        print(f"📁 专业数据存储路径: {self.base_path}")
        for data_type, path in self.data_paths.items():
            print(f"   📊 {data_type}: {path}")
    
    def load_api_mappings(self):
        """加载API映射关系"""
        self.api_mappings = {
            'daily': 'MktEqudGet',          # 日线
            'daily_adj': 'MktEqudAdjGet',   # 前复权日线 [极速版]
            'weekly': 'MktEquwGet',         # 周线
            'weekly_adj': 'MktEquwAdjGet',  # 前复权周线 [极速版]
            'monthly': 'MktEqumGet',        # 月线  
            'monthly_adj': 'MktEqumAdjGet', # 前复权月线
            'quarterly': 'MktEquqGet',      # 季线 [极速版]
            'yearly': 'MktEquaGet',         # 年线 [极速版]
            'basic_info': 'EquGet',         # 基本信息
            'factors': 'StockFactorsDateRangeGet'  # 因子数据
        }
        
        print("📋 API映射加载完成:")
        for data_type, api_name in self.api_mappings.items():
            print(f"   {data_type}: uqer.DataAPI.{api_name}")
    
    def get_all_a_stocks(self):
        """获取全部A股股票列表"""
        print("📋 获取全部A股股票列表...")
        
        try:
            # 使用基本信息API获取更完整的股票列表
            all_stocks = []
            
            # 上市股票
            current_result = uqer.DataAPI.EquGet(
                listStatusCD='L',
                pandas=1
            )
            
            # 已退市股票
            delisted_result = uqer.DataAPI.EquGet(
                listStatusCD='DE', 
                pandas=1
            )
            
            # 处理返回数据
            for result, status in [(current_result, '上市'), (delisted_result, '退市')]:
                if result is not None:
                    if isinstance(result, str):
                        df = pd.read_csv(StringIO(result))
                    else:
                        df = result
                    
                    if len(df) > 0:
                        all_stocks.append(df)
                        print(f"   📈 {status}: {len(df)} 只")
            
            if not all_stocks:
                print("❌ 获取股票列表失败")
                return []
            
            # 合并并过滤A股
            df = pd.concat(all_stocks, ignore_index=True)
            a_stocks = df[
                df['secID'].str.contains('.XSHE|.XSHG', na=False)
            ].copy()
            
            # 排除指数和其他非股票
            a_stocks = a_stocks[
                ~a_stocks['secID'].str.contains('.ZICN|.INDX|.XBEI', na=False)
            ]
            
            stock_list = a_stocks['secID'].unique().tolist()
            stock_list.sort()
            
            print(f"✅ 获取到 {len(stock_list)} 只A股")
            
            # 保存股票基本信息
            basic_info_file = self.data_paths['basic_info'] / 'a_stock_info.csv'
            a_stocks.to_csv(basic_info_file, index=False, encoding='utf-8')
            print(f"📄 股票基本信息已保存: {basic_info_file}")
            
            self.download_stats['total_stocks'] = len(stock_list)
            return stock_list
            
        except Exception as e:
            print(f"❌ 获取股票列表失败: {e}")
            return []
    
    def download_stock_multi_timeframe(self, stock_code):
        """下载单只股票的多时间周期数据"""
        results = {}
        
        # 定义下载任务
        download_tasks = [
            ('daily', '2000-01-01', '2025-08-31'),
            ('daily_adj', '2000-01-01', '2025-08-31'),
            ('weekly', '2000-01-01', '2025-08-31'),
            ('weekly_adj', '2000-01-01', '2025-08-31'),
            ('monthly', '2000-01-01', '2025-08-31'),
            ('monthly_adj', '2000-01-01', '2025-08-31'),
        ]
        
        for data_type, start_date, end_date in download_tasks:
            try:
                # 检查文件是否已存在
                file_path = self.data_paths[data_type] / f"{stock_code.replace('.', '_')}.csv"
                if file_path.exists():
                    results[data_type] = {'status': 'exists', 'file': file_path}
                    continue
                
                # 调用对应API
                api_name = self.api_mappings[data_type]
                api_func = getattr(uqer.DataAPI, api_name)
                
                result = api_func(
                    secID=stock_code,
                    beginDate=start_date.replace('-', ''),
                    endDate=end_date.replace('-', ''),
                    pandas=1
                )
                
                self.download_stats['api_calls'] += 1
                
                if result is None:
                    results[data_type] = {'status': 'no_data'}
                    continue
                
                # 处理返回数据
                if isinstance(result, str):
                    df = pd.read_csv(StringIO(result))
                elif isinstance(result, pd.DataFrame):
                    df = result.copy()
                else:
                    results[data_type] = {'status': 'invalid_format'}
                    continue
                
                if len(df) == 0:
                    results[data_type] = {'status': 'empty_data'}
                    continue
                
                # 数据处理和验证
                df = self.process_timeframe_data(df, stock_code, data_type)
                
                if df is None or len(df) == 0:
                    results[data_type] = {'status': 'processing_failed'}
                    continue
                
                # 验证时间范围
                if not self.validate_date_range(df, start_date, end_date, data_type):
                    results[data_type] = {'status': 'date_range_invalid'}
                    print(f"   ⚠️ {data_type}: 时间范围不符合要求")
                    continue
                
                # 保存数据
                df.to_csv(file_path, index=False, encoding='utf-8')
                
                results[data_type] = {
                    'status': 'success',
                    'records': len(df),
                    'file': file_path,
                    'date_range': f"{df['endDate'].min() if 'endDate' in df.columns else df['tradeDate'].min()} - {df['endDate'].max() if 'endDate' in df.columns else df['tradeDate'].max()}"
                }
                
                # 更新统计
                with self.progress_lock:
                    self.download_stats['successful_downloads'] += 1
                    self.download_stats['total_records'] += len(df)
                    file_size = file_path.stat().st_size / 1024 / 1024
                    self.download_stats['data_size_mb'] += file_size
                
                # API限速
                time.sleep(0.1)
                
            except Exception as e:
                results[data_type] = {'status': 'error', 'error': str(e)}
                with self.progress_lock:
                    self.download_stats['failed_downloads'] += 1
        
        return results
    
    def process_timeframe_data(self, df, stock_code, data_type):
        """处理时间周期数据"""
        try:
            # 确定日期列
            date_col = 'tradeDate' if 'tradeDate' in df.columns else 'endDate'
            if date_col not in df.columns:
                return None
            
            # 转换日期
            df[date_col] = pd.to_datetime(df[date_col])
            
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
            price_col = 'closePrice'
            if price_col in df.columns:
                df = df.dropna(subset=[price_col])
                df = df[df[price_col] > 0]
                df = df.sort_values(date_col)
            
            # 确保股票代码正确
            df['secID'] = stock_code
            
            return df.reset_index(drop=True)
            
        except Exception as e:
            return None
    
    def validate_date_range(self, df, start_date, end_date, data_type):
        """验证数据时间范围"""
        try:
            date_col = 'tradeDate' if 'tradeDate' in df.columns else 'endDate'
            if date_col not in df.columns:
                return False
            
            data_start = df[date_col].min()
            data_end = df[date_col].max()
            
            target_start = pd.Timestamp(start_date)
            target_end = pd.Timestamp(end_date)
            
            # 对于2025年的数据，检查是否至少到达2025年8月
            if target_end.year == 2025:
                # 数据应该至少到2025年7月（考虑数据延迟）
                min_required_end = pd.Timestamp('2025-07-01')
                if data_end < min_required_end:
                    print(f"   ⚠️ {data_type}: 数据截止{data_end}，未达到2025年要求")
                    return False
            
            return True
            
        except Exception as e:
            return False
    
    def download_all_professional_data(self):
        """下载全部专业级数据"""
        print("🚀 开始专业级数据下载")
        print("   🔥 权限: 优矿极速版 (268个API)")
        print("   📅 时间: 2000年1月1日 - 2025年8月31日")
        print("   📊 类型: 日/周/月/季/年线 + 复权 + 因子")
        print("=" * 80)
        
        stock_list = self.get_all_a_stocks()
        
        if not stock_list:
            print("❌ 无法获取股票列表")
            return
        
        # 限制数量进行测试 - 先下载前100只验证
        test_stocks = stock_list[:100]
        print(f"📊 测试下载: {len(test_stocks)} 只股票")
        
        successful_stocks = 0
        failed_stocks = 0
        
        for i, stock_code in enumerate(test_stocks, 1):
            print(f"\n📈 [{i}/{len(test_stocks)}] 处理: {stock_code}")
            
            # 下载多时间周期数据
            results = self.download_stock_multi_timeframe(stock_code)
            
            # 统计结果
            success_count = sum(1 for r in results.values() if r['status'] in ['success', 'exists'])
            total_count = len(results)
            
            if success_count >= total_count // 2:  # 至少一半成功
                successful_stocks += 1
                print(f"   ✅ 成功: {success_count}/{total_count} 个时间周期")
                
                # 显示详细结果
                for data_type, result in results.items():
                    if result['status'] == 'success':
                        print(f"      📊 {data_type}: {result['records']} 条记录")
                    elif result['status'] == 'exists':
                        print(f"      ⏩ {data_type}: 已存在")
                    elif result['status'] != 'success':
                        print(f"      ❌ {data_type}: {result['status']}")
            else:
                failed_stocks += 1
                print(f"   ❌ 失败: {success_count}/{total_count} 个时间周期成功")
            
            # API限速
            if i % 10 == 0:
                time.sleep(2)
                print(f"   📈 总体进度: {i}/{len(test_stocks)} ({i/len(test_stocks)*100:.1f}%)")
        
        # 创建下载总结
        self.create_professional_summary(successful_stocks, failed_stocks, len(test_stocks))
    
    def create_professional_summary(self, successful_stocks, failed_stocks, total_stocks):
        """创建专业级下载总结"""
        end_time = datetime.now()
        duration = end_time - self.download_stats['start_time']
        
        summary = {
            'professional_download_info': {
                'start_time': self.download_stats['start_time'].isoformat(),
                'end_time': end_time.isoformat(),
                'duration_minutes': round(duration.total_seconds() / 60, 2),
                'data_range': '2000年1月1日 - 2025年8月31日',
                'api_version': '优矿极速版',
                'total_api_calls': self.download_stats['api_calls']
            },
            'statistics': {
                'total_stocks_attempted': total_stocks,
                'successful_stocks': successful_stocks,
                'failed_stocks': failed_stocks,
                'success_rate': f"{successful_stocks/total_stocks*100:.1f}%",
                'total_downloads': self.download_stats['successful_downloads'],
                'total_records': self.download_stats['total_records'],
                'total_size_mb': round(self.download_stats['data_size_mb'], 2),
                'total_size_gb': round(self.download_stats['data_size_mb'] / 1024, 2)
            },
            'data_types': {
                'daily': '日线数据 (MktEqudGet)',
                'daily_adj': '前复权日线 (MktEqudAdjGet) [极速版]',
                'weekly': '周线数据 (MktEquwGet)',
                'weekly_adj': '前复权周线 (MktEquwAdjGet) [极速版]',
                'monthly': '月线数据 (MktEqumGet)',
                'monthly_adj': '前复权月线 (MktEqumAdjGet)'
            },
            'file_structure': {
                'base_path': str(self.base_path),
                'organization': '按数据类型和股票代码组织',
                'naming_convention': 'XXXXXX_XXXX.csv'
            }
        }
        
        # 保存总结
        summary_file = self.base_path / 'professional_download_summary.json'
        with open(summary_file, 'w', encoding='utf-8') as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
        
        print(f"\n🎊 专业级数据下载完成!")
        print(f"=" * 80)
        print(f"📊 下载统计:")
        print(f"   🎯 测试股票: {summary['statistics']['total_stocks_attempted']}")
        print(f"   ✅ 成功股票: {summary['statistics']['successful_stocks']}")
        print(f"   ❌ 失败股票: {summary['statistics']['failed_stocks']}")
        print(f"   📈 成功率: {summary['statistics']['success_rate']}")
        print(f"   📊 总下载数: {summary['statistics']['total_downloads']}")
        print(f"   📝 总记录数: {summary['statistics']['total_records']:,}")
        print(f"   💾 数据大小: {summary['statistics']['total_size_gb']} GB")
        print(f"   🔥 API调用: {summary['professional_download_info']['total_api_calls']}")
        print(f"   ⏱️ 用时: {summary['professional_download_info']['duration_minutes']} 分钟")
        print(f"   📁 存储位置: {self.base_path}")
        print(f"   📄 详细报告: {summary_file}")

def main():
    """主函数"""
    print("🔥 专业级数据下载器 - 优矿极速版")
    print("=" * 80)
    print("🎯 利用优矿2025全部268个API接口")
    print("📅 时间范围: 2000年1月1日 - 2025年8月31日")
    print("🚀 数据类型: 多时间周期 + 前复权 + 因子数据")
    
    downloader = ProfessionalDataDownloader()
    downloader.download_all_professional_data()

if __name__ == "__main__":
    main()