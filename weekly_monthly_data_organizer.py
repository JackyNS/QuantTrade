#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
周线、月线数据整理和补全器
统一组织现有数据并补全缺失部分
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

class WeeklyMonthlyDataOrganizer:
    """周线、月线数据整理器"""
    
    def __init__(self):
        """初始化整理器"""
        self.setup_uqer()
        self.setup_paths()
        self.stats = {
            'start_time': datetime.now(),
            'weekly_files_found': 0,
            'monthly_files_found': 0,
            'stocks_organized': 0,
            'weekly_gaps_filled': 0,
            'monthly_gaps_filled': 0
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
        """设置路径"""
        # 现有数据路径
        self.existing_weekly_path = Path("/Users/jackstudio/QuantTrade/data/priority_download/market_data/weekly")
        self.existing_monthly_path = Path("/Users/jackstudio/QuantTrade/data/priority_download/market_data/monthly")
        
        # 新的统一数据路径
        self.base_path = Path("/Users/jackstudio/QuantTrade/data/csv_complete")
        self.base_path.mkdir(exist_ok=True)
        
        # 周线数据目录
        self.weekly_path = self.base_path / "weekly"
        self.weekly_path.mkdir(exist_ok=True)
        self.weekly_stocks_path = self.weekly_path / "stocks"
        self.weekly_stocks_path.mkdir(exist_ok=True)
        
        # 月线数据目录
        self.monthly_path = self.base_path / "monthly"
        self.monthly_path.mkdir(exist_ok=True)
        self.monthly_stocks_path = self.monthly_path / "stocks"
        self.monthly_stocks_path.mkdir(exist_ok=True)
        
        print(f"📁 数据整理路径:")
        print(f"   📊 周线: {self.weekly_path}")
        print(f"   📅 月线: {self.monthly_path}")
    
    def scan_existing_data(self):
        """扫描现有数据"""
        print("🔍 扫描现有周线、月线数据...")
        
        # 扫描周线文件
        weekly_files = list(self.existing_weekly_path.glob("*.csv"))
        monthly_files = list(self.existing_monthly_path.glob("*.csv"))
        
        self.stats['weekly_files_found'] = len(weekly_files)
        self.stats['monthly_files_found'] = len(monthly_files)
        
        print(f"   📊 周线文件: {len(weekly_files)} 个")
        print(f"   📅 月线文件: {len(monthly_files)} 个")
        
        return weekly_files, monthly_files
    
    def extract_stock_data(self, files, data_type="weekly"):
        """从批次文件中提取按股票组织的数据"""
        print(f"📦 整理{data_type}数据...")
        
        all_stocks_data = {}
        
        for file_path in files:
            try:
                df = pd.read_csv(file_path)
                
                if 'secID' not in df.columns or 'endDate' not in df.columns:
                    continue
                
                # 按股票分组
                for stock_id, stock_data in df.groupby('secID'):
                    if stock_id not in all_stocks_data:
                        all_stocks_data[stock_id] = []
                    
                    all_stocks_data[stock_id].append(stock_data)
                    
            except Exception as e:
                print(f"   ❌ 处理文件失败: {file_path.name}")
                continue
        
        # 合并每只股票的数据
        organized_data = {}
        for stock_id, data_list in all_stocks_data.items():
            try:
                combined_df = pd.concat(data_list, ignore_index=True)
                combined_df['endDate'] = pd.to_datetime(combined_df['endDate'])
                combined_df = combined_df.drop_duplicates(subset=['endDate']).sort_values('endDate')
                organized_data[stock_id] = combined_df
            except Exception as e:
                continue
        
        print(f"   ✅ 整理完成: {len(organized_data)} 只股票")
        return organized_data
    
    def save_organized_data(self, organized_data, data_type="weekly"):
        """保存整理后的数据"""
        save_path = self.weekly_stocks_path if data_type == "weekly" else self.monthly_stocks_path
        
        print(f"💾 保存{data_type}数据...")
        
        saved_count = 0
        for stock_id, df in organized_data.items():
            try:
                file_name = f"{stock_id.replace('.', '_')}.csv"
                file_path = save_path / file_name
                
                df.to_csv(file_path, index=False, encoding='utf-8')
                saved_count += 1
                
                if saved_count % 100 == 0:
                    print(f"   📊 已保存: {saved_count}/{len(organized_data)}")
                    
            except Exception as e:
                continue
        
        print(f"   ✅ 保存完成: {saved_count} 个文件")
        return saved_count
    
    def check_data_gaps(self, organized_data, data_type="weekly"):
        """检查数据缺口"""
        print(f"🔍 检查{data_type}数据缺口...")
        
        gaps_info = []
        target_end_date = pd.Timestamp('2025-08-31')
        
        for stock_id, df in organized_data.items():
            latest_date = df['endDate'].max()
            
            if latest_date < target_end_date:
                gap_days = (target_end_date - latest_date).days
                gaps_info.append({
                    'stock_id': stock_id,
                    'latest_date': latest_date,
                    'gap_days': gap_days
                })
        
        print(f"   📊 需要补全的股票: {len(gaps_info)}")
        return gaps_info
    
    def fill_data_gaps(self, gaps_info, data_type="weekly"):
        """补全数据缺口"""
        if not gaps_info:
            print(f"   ✅ {data_type}数据无需补全")
            return 0
        
        print(f"🔄 补全{data_type}数据缺口...")
        
        # 根据类型选择API
        if data_type == "weekly":
            api_func = uqer.DataAPI.MktEqudGet
            freq = 'W'
        else:  # monthly
            api_func = uqer.DataAPI.MktEqudGet
            freq = 'M'
        
        filled_count = 0
        save_path = self.weekly_stocks_path if data_type == "weekly" else self.monthly_stocks_path
        
        for i, gap_info in enumerate(gaps_info[:50], 1):  # 限制数量避免API超限
            stock_id = gap_info['stock_id']
            start_date = gap_info['latest_date'] + pd.Timedelta(days=1)
            
            try:
                print(f"   📈 [{i}/50] 补全: {stock_id}")
                
                # 获取日线数据然后转换
                result = api_func(
                    secID=stock_id,
                    beginDate=start_date.strftime('%Y%m%d'),
                    endDate='20250831',
                    pandas=1
                )
                
                if result is None:
                    continue
                
                # 处理API返回的数据
                if isinstance(result, str):
                    daily_df = pd.read_csv(StringIO(result))
                elif isinstance(result, pd.DataFrame):
                    daily_df = result.copy()
                else:
                    continue
                
                if len(daily_df) == 0:
                    continue
                
                # 转换为周线或月线
                converted_df = self.convert_to_period(daily_df, freq)
                
                if converted_df is not None and len(converted_df) > 0:
                    # 读取现有数据并合并
                    file_path = save_path / f"{stock_id.replace('.', '_')}.csv"
                    
                    if file_path.exists():
                        existing_df = pd.read_csv(file_path)
                        existing_df['endDate'] = pd.to_datetime(existing_df['endDate'])
                        
                        # 合并数据
                        combined_df = pd.concat([existing_df, converted_df], ignore_index=True)
                        combined_df = combined_df.drop_duplicates(subset=['endDate']).sort_values('endDate')
                        
                        combined_df.to_csv(file_path, index=False, encoding='utf-8')
                        filled_count += 1
                
                time.sleep(0.2)  # API限速
                
            except Exception as e:
                print(f"   ❌ 补全失败: {stock_id}")
                continue
        
        print(f"   ✅ 补全完成: {filled_count} 只股票")
        return filled_count
    
    def convert_to_period(self, daily_df, freq='W'):
        """将日线数据转换为周线或月线"""
        try:
            if 'tradeDate' not in daily_df.columns:
                return None
            
            daily_df['tradeDate'] = pd.to_datetime(daily_df['tradeDate'])
            daily_df = daily_df.set_index('tradeDate')
            
            # 重采样规则
            agg_rules = {
                'openPrice': 'first',
                'highestPrice': 'max',
                'lowestPrice': 'min', 
                'closePrice': 'last',
                'turnoverVol': 'sum',
                'turnoverValue': 'sum'
            }
            
            # 处理列名差异
            if 'highPrice' in daily_df.columns:
                agg_rules['highPrice'] = 'max'
                agg_rules.pop('highestPrice')
            if 'lowPrice' in daily_df.columns:
                agg_rules['lowPrice'] = 'min'
                agg_rules.pop('lowestPrice')
            if 'volume' in daily_df.columns:
                agg_rules['volume'] = 'sum'
                agg_rules.pop('turnoverVol')
            if 'amount' in daily_df.columns:
                agg_rules['amount'] = 'sum'
                agg_rules.pop('turnoverValue')
            
            # 重采样
            resampled = daily_df.resample(freq).agg(agg_rules).dropna()
            
            # 重置索引并重命名
            resampled = resampled.reset_index()
            resampled = resampled.rename(columns={'tradeDate': 'endDate'})
            
            # 添加基础字段
            if len(daily_df) > 0:
                resampled['secID'] = daily_df['secID'].iloc[0] if 'secID' in daily_df.columns else ""
                resampled['ticker'] = daily_df['ticker'].iloc[0] if 'ticker' in daily_df.columns else ""
            
            return resampled
            
        except Exception as e:
            return None
    
    def organize_all_data(self):
        """整理所有周线、月线数据"""
        print("🚀 开始整理周线、月线数据")
        print("=" * 80)
        
        # 扫描现有数据
        weekly_files, monthly_files = self.scan_existing_data()
        
        if not weekly_files and not monthly_files:
            print("❌ 未找到现有周线、月线数据")
            return
        
        # 整理周线数据
        if weekly_files:
            print("\n📊 处理周线数据:")
            weekly_data = self.extract_stock_data(weekly_files, "weekly")
            weekly_saved = self.save_organized_data(weekly_data, "weekly")
            weekly_gaps = self.check_data_gaps(weekly_data, "weekly")
            weekly_filled = self.fill_data_gaps(weekly_gaps, "weekly")
            
            self.stats['stocks_organized'] = max(self.stats['stocks_organized'], len(weekly_data))
            self.stats['weekly_gaps_filled'] = weekly_filled
        
        # 整理月线数据
        if monthly_files:
            print("\n📅 处理月线数据:")
            monthly_data = self.extract_stock_data(monthly_files, "monthly") 
            monthly_saved = self.save_organized_data(monthly_data, "monthly")
            monthly_gaps = self.check_data_gaps(monthly_data, "monthly")
            monthly_filled = self.fill_data_gaps(monthly_gaps, "monthly")
            
            self.stats['stocks_organized'] = max(self.stats['stocks_organized'], len(monthly_data))
            self.stats['monthly_gaps_filled'] = monthly_filled
        
        # 创建总结报告
        self.create_summary()
    
    def create_summary(self):
        """创建整理总结"""
        end_time = datetime.now()
        duration = end_time - self.stats['start_time']
        
        summary = {
            'organization_info': {
                'start_time': self.stats['start_time'].isoformat(),
                'end_time': end_time.isoformat(),
                'duration_minutes': round(duration.total_seconds() / 60, 2),
                'data_range': '2000年1月1日-2025年8月31日',
                'organization': '按股票组织的CSV文件'
            },
            'statistics': {
                'weekly_files_found': self.stats['weekly_files_found'],
                'monthly_files_found': self.stats['monthly_files_found'],
                'stocks_organized': self.stats['stocks_organized'],
                'weekly_gaps_filled': self.stats['weekly_gaps_filled'],
                'monthly_gaps_filled': self.stats['monthly_gaps_filled']
            },
            'file_structure': {
                'weekly_path': str(self.weekly_path),
                'monthly_path': str(self.monthly_path),
                'file_naming': 'XXXXXX_XXXX.csv (如 000001_XSHE.csv)'
            }
        }
        
        # 保存总结
        summary_file = self.base_path / 'weekly_monthly_summary.json'
        with open(summary_file, 'w', encoding='utf-8') as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
        
        print(f"\n🎊 周线、月线数据整理完成!")
        print(f"=" * 80)
        print(f"📊 整理统计:")
        print(f"   📊 原周线文件: {summary['statistics']['weekly_files_found']}")
        print(f"   📅 原月线文件: {summary['statistics']['monthly_files_found']}")
        print(f"   🎯 整理股票数: {summary['statistics']['stocks_organized']}")
        print(f"   📈 周线补全: {summary['statistics']['weekly_gaps_filled']}")
        print(f"   📅 月线补全: {summary['statistics']['monthly_gaps_filled']}")
        print(f"   ⏱️ 用时: {summary['organization_info']['duration_minutes']} 分钟")
        print(f"   📁 存储位置: {self.base_path}")
        print(f"   📄 详细报告: {summary_file}")

def main():
    """主函数"""
    print("📊 周线、月线数据整理器")
    print("=" * 80)
    print("🎯 目标: 整理现有数据并补全缺口")
    print("📅 时间: 统一到2025年8月31日")
    print("📡 补全数据源: UQER MktEqudGet API")
    
    organizer = WeeklyMonthlyDataOrganizer()
    organizer.organize_all_data()

if __name__ == "__main__":
    main()