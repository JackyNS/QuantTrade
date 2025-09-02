#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
2024年8月黄金交叉A股筛选器
从全A股中筛选出8月份发生十周线上穿百周线的股票
使用下载的完整历史数据进行分析
"""

import sys
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
import warnings
import json
import os
import time
warnings.filterwarnings('ignore')

try:
    import talib
    print("✅ TA-Lib 可用")
    TALIB_AVAILABLE = True
except ImportError:
    print("❌ TA-Lib 不可用，将使用pandas计算")
    TALIB_AVAILABLE = False

class AugustGoldenCrossAStockScreener:
    """2024年8月黄金交叉A股筛选器"""
    
    def __init__(self, data_path="/Users/jackstudio/QuantTrade/data/mktequd_complete"):
        """
        初始化筛选器
        
        Args:
            data_path: MktEqud数据路径
        """
        self.data_path = Path(data_path)
        self.results = []
        self.qualified_stocks = []
        
        print(f"📁 A股数据路径: {self.data_path}")
        
        # 检查数据目录
        self.check_data_availability()
        
    def check_data_availability(self):
        """检查数据可用性"""
        print("🔍 检查A股数据可用性...")
        
        if not self.data_path.exists():
            print(f"❌ 数据目录不存在: {self.data_path}")
            return False
        
        # 检查年份目录
        year_dirs = []
        for year in range(2020, 2025):  # 检查最近几年的数据
            year_path = self.data_path / f"year_{year}"
            if year_path.exists():
                csv_files = list(year_path.glob("*.csv"))
                year_dirs.append((year, len(csv_files)))
                print(f"   ✅ {year}年: {len(csv_files)} 只股票")
        
        if not year_dirs:
            print("❌ 未找到年份数据目录")
            return False
        
        self.available_years = year_dirs
        return True
    
    def get_stock_files(self):
        """获取所有股票文件"""
        print("📋 收集股票数据文件...")
        
        stock_files = {}
        
        # 从最近的年份目录收集股票文件
        latest_year = max([year for year, _ in self.available_years])
        latest_year_path = self.data_path / f"year_{latest_year}"
        
        csv_files = list(latest_year_path.glob("*.csv"))
        print(f"   📊 从{latest_year}年目录找到 {len(csv_files)} 只股票")
        
        # 为每只股票收集所有年份的数据文件
        for csv_file in csv_files:
            stock_code = csv_file.stem  # 去掉.csv后缀
            
            # 收集这只股票的所有年份数据
            stock_files[stock_code] = []
            for year in range(2020, 2025):  # 收集2020-2024年数据
                year_path = self.data_path / f"year_{year}"
                year_file = year_path / f"{stock_code}.csv"
                if year_file.exists():
                    stock_files[stock_code].append(year_file)
        
        # 只保留有足够历史数据的股票
        valid_stocks = {k: v for k, v in stock_files.items() if len(v) >= 3}  # 至少3年数据
        
        print(f"✅ 找到 {len(valid_stocks)} 只股票有足够历史数据")
        return valid_stocks
    
    def load_stock_complete_data(self, stock_files):
        """
        加载股票的完整历史数据
        
        Args:
            stock_files: 股票各年份文件列表
            
        Returns:
            DataFrame: 合并后的完整数据
        """
        try:
            all_data = []
            
            for file_path in stock_files:
                try:
                    df = pd.read_csv(file_path)
                    if len(df) > 0:
                        all_data.append(df)
                except Exception as e:
                    continue
            
            if not all_data:
                return None
            
            # 合并所有年份数据
            combined_df = pd.concat(all_data, ignore_index=True)
            
            # 数据清理
            if 'tradeDate' in combined_df.columns:
                combined_df['tradeDate'] = pd.to_datetime(combined_df['tradeDate'])
            
            # 去重和排序
            combined_df = combined_df.drop_duplicates(subset=['tradeDate'])
            combined_df = combined_df.sort_values('tradeDate')
            
            # 过滤有效数据
            if 'closePrice' in combined_df.columns:
                combined_df = combined_df.dropna(subset=['closePrice'])
                combined_df = combined_df[combined_df['closePrice'] > 0]
            
            return combined_df
            
        except Exception as e:
            return None
    
    def calculate_ma_crossover_august_2024(self, price_data, short_period=10, long_period=100):
        """
        计算2024年8月的MA交叉信号
        
        Args:
            price_data: 价格数据
            short_period: 短期MA周期 (10周)
            long_period: 长期MA周期 (100周)
            
        Returns:
            dict: 分析结果
        """
        try:
            if len(price_data) < long_period * 7:  # 需要足够的日线数据转换为周线
                return {'status': 'insufficient_data', 'data_length': len(price_data)}
            
            # 按日期排序
            df = price_data.sort_values('tradeDate').copy()
            
            # 转换为周线数据 (每周最后一个交易日)
            df_indexed = df.set_index('tradeDate')
            weekly_close = df_indexed['closePrice'].resample('W').last().dropna()
            
            if len(weekly_close) < long_period:
                return {'status': 'insufficient_weekly_data', 'weekly_length': len(weekly_close)}
            
            # 计算移动平均线
            if TALIB_AVAILABLE:
                ma_short = talib.MA(weekly_close.values, timeperiod=short_period)
                ma_long = talib.MA(weekly_close.values, timeperiod=long_period)
            else:
                ma_short = weekly_close.rolling(short_period).mean().values
                ma_long = weekly_close.rolling(long_period).mean().values
            
            # 创建Series
            ma_short_series = pd.Series(ma_short, index=weekly_close.index)
            ma_long_series = pd.Series(ma_long, index=weekly_close.index)
            
            # 去除NaN
            valid_idx = ma_short_series.dropna().index.intersection(ma_long_series.dropna().index)
            if len(valid_idx) < 2:
                return {'status': 'no_valid_data', 'valid_length': len(valid_idx)}
            
            ma_short_clean = ma_short_series.loc[valid_idx]
            ma_long_clean = ma_long_series.loc[valid_idx]
            
            # 计算交叉信号
            position = (ma_short_clean > ma_long_clean).astype(int)
            crossover = position.diff()
            
            # 找到所有黄金交叉
            golden_cross_mask = crossover > 0
            all_golden_dates = golden_cross_mask[golden_cross_mask].index
            
            # 筛选2024年8月的黄金交叉
            august_2024_start = pd.Timestamp('2024-08-01')
            august_2024_end = pd.Timestamp('2024-08-31')
            
            august_golden_crosses = all_golden_dates[
                (all_golden_dates >= august_2024_start) & 
                (all_golden_dates <= august_2024_end)
            ]
            
            # 当前状态 (最新数据)
            current_position = position.iloc[-1] if len(position) > 0 else 0
            latest_price = weekly_close.iloc[-1] if len(weekly_close) > 0 else 0
            latest_ma_short = ma_short_clean.iloc[-1] if len(ma_short_clean) > 0 else 0
            latest_ma_long = ma_long_clean.iloc[-1] if len(ma_long_clean) > 0 else 0
            
            # 历史黄金交叉统计
            total_golden_crosses = len(all_golden_dates)
            
            return {
                'status': 'success',
                'august_2024': {
                    'golden_crosses': list(august_golden_crosses),
                    'golden_count': len(august_golden_crosses),
                    'has_august_golden': len(august_golden_crosses) > 0
                },
                'historical': {
                    'total_golden_crosses': total_golden_crosses,
                    'all_golden_dates': list(all_golden_dates[-10:])  # 最近10次
                },
                'current_status': {
                    'position': 'bullish' if current_position > 0 else 'bearish',
                    'latest_price': round(latest_price, 2),
                    'ma_short': round(latest_ma_short, 2),
                    'ma_long': round(latest_ma_long, 2),
                    'ma_spread_pct': round((latest_ma_short - latest_ma_long) / latest_ma_long * 100, 2) if latest_ma_long > 0 else 0
                },
                'data_info': {
                    'daily_records': len(price_data),
                    'weekly_records': len(weekly_close),
                    'valid_records': len(valid_idx),
                    'data_start': price_data['tradeDate'].min(),
                    'data_end': price_data['tradeDate'].max()
                }
            }
            
        except Exception as e:
            return {'status': 'error', 'error': str(e)}
    
    def screen_august_2024_golden_crosses(self, max_stocks=500):
        """
        筛选2024年8月黄金交叉的股票
        
        Args:
            max_stocks: 最大分析股票数量
        """
        print("🔍 开始筛选2024年8月黄金交叉A股")
        print("   🎯 目标: 十周线上穿百周线")
        print("   📅 时间: 2024年8月")
        print("=" * 80)
        
        # 获取股票文件
        stock_files_dict = self.get_stock_files()
        
        if not stock_files_dict:
            print("❌ 未找到股票数据文件")
            return []
        
        # 限制分析数量
        stock_codes = list(stock_files_dict.keys())[:max_stocks]
        
        print(f"📊 开始分析 {len(stock_codes)} 只A股...")
        
        self.results = []
        self.qualified_stocks = []
        
        for i, stock_code in enumerate(stock_codes, 1):
            print(f"📈 [{i}/{len(stock_codes)}] 分析: {stock_code}")
            
            # 加载股票完整数据
            stock_files = stock_files_dict[stock_code]
            stock_data = self.load_stock_complete_data(stock_files)
            
            if stock_data is None or len(stock_data) == 0:
                print(f"   ❌ 数据加载失败")
                continue
            
            print(f"   📊 数据: {len(stock_data)} 条记录")
            print(f"   📅 范围: {stock_data['tradeDate'].min().date()} - {stock_data['tradeDate'].max().date()}")
            
            # 分析8月黄金交叉
            analysis = self.calculate_ma_crossover_august_2024(stock_data)
            
            # 保存结果
            result = {
                'stock_code': stock_code,
                'analysis_time': datetime.now(),
                'analysis': analysis
            }
            self.results.append(result)
            
            # 检查是否符合条件
            if analysis['status'] == 'success':
                august_info = analysis['august_2024']
                current_status = analysis['current_status']
                historical = analysis['historical']
                
                print(f"   🌟 8月黄金交叉: {august_info['golden_count']} 次")
                print(f"   📈 当前状态: {current_status['position']}")
                print(f"   💰 最新价格: {current_status['latest_price']}")
                print(f"   📊 历史黄金交叉: {historical['total_golden_crosses']} 次")
                
                if august_info['has_august_golden']:
                    self.qualified_stocks.append(result)
                    print(f"   ✅ 符合条件 - 8月发生黄金交叉")
                    
                    # 显示具体日期
                    for date in august_info['golden_crosses']:
                        days_ago = (datetime.now() - date).days
                        print(f"      🌟 黄金交叉: {date.strftime('%Y-%m-%d')} ({days_ago}天前)")
                else:
                    print(f"   ❌ 8月无黄金交叉")
            else:
                print(f"   ❌ 分析失败: {analysis.get('status', 'unknown')}")
            
            print()
            
            # 控制分析速度
            if i % 50 == 0:
                print(f"   ⏸️ 中途统计: 已分析{i}只, 符合条件{len(self.qualified_stocks)}只")
                time.sleep(1)
        
        # 按8月黄金交叉日期排序（最晚的在前）
        self.qualified_stocks.sort(
            key=lambda x: max(x['analysis']['august_2024']['golden_crosses']) if x['analysis']['august_2024']['golden_crosses'] else pd.Timestamp('2024-08-01'),
            reverse=True
        )
        
        print(f"🎯 筛选完成:")
        print(f"   📊 总分析: {len(stock_codes)} 只A股")
        print(f"   ✅ 8月黄金交叉: {len(self.qualified_stocks)} 只")
        print(f"   📈 命中率: {len(self.qualified_stocks)/len(stock_codes)*100:.1f}%")
        
        return self.qualified_stocks
    
    def print_august_results(self, show_top=20):
        """打印8月黄金交叉结果"""
        if not self.qualified_stocks:
            print("❌ 未发现2024年8月黄金交叉的A股")
            return
        
        print(f"\n🏆 2024年8月黄金交叉A股榜单 (前{min(show_top, len(self.qualified_stocks))}名):")
        print("=" * 100)
        
        for i, stock in enumerate(self.qualified_stocks[:show_top], 1):
            analysis = stock['analysis']
            august_info = analysis['august_2024']
            current_status = analysis['current_status']
            historical = analysis['historical']
            data_info = analysis['data_info']
            
            # 尝试获取股票名称（从secShortName字段）
            stock_name = "未知"
            try:
                # 从第一个数据文件中获取股票名称
                if 'secShortName' in analysis or hasattr(analysis, 'secShortName'):
                    stock_name = "获取中..."
            except:
                pass
            
            print(f"\n🥇 第{i}名: {stock['stock_code']} ({stock_name})")
            print(f"    🌟 8月黄金交叉: {august_info['golden_count']} 次")
            
            # 显示8月黄金交叉日期
            for j, date in enumerate(august_info['golden_crosses'], 1):
                days_ago = (datetime.now() - date).days
                weekday = date.strftime('%A')  # 星期几
                print(f"    📅 第{j}次: {date.strftime('%Y-%m-%d')} ({weekday}, {days_ago}天前)")
            
            print(f"    📈 当前状态: {current_status['position']}")
            print(f"    💰 最新价格: {current_status['latest_price']}")
            print(f"    📊 MA状态: 10周MA={current_status['ma_short']}, 100周MA={current_status['ma_long']}")
            print(f"    📏 趋势强度: {current_status['ma_spread_pct']}%")
            print(f"    📚 历史统计: 总共{historical['total_golden_crosses']}次黄金交叉")
            print(f"    📆 数据跨度: {data_info['data_start'].strftime('%Y-%m')} - {data_info['data_end'].strftime('%Y-%m')} ({data_info['daily_records']}条记录)")
    
    def export_august_results(self, filename=None):
        """导出8月黄金交叉结果"""
        if filename is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"august_2024_golden_cross_a_stocks_{timestamp}.json"
        
        export_data = {
            'screening_info': {
                'timestamp': datetime.now().isoformat(),
                'strategy': '2024年8月十周线上穿百周线A股筛选',
                'target_month': '2024年8月',
                'data_source': 'MktEqud完整历史数据',
                'total_analyzed': len(self.results),
                'qualified_count': len(self.qualified_stocks),
                'hit_rate': f"{len(self.qualified_stocks)/len(self.results)*100:.1f}%" if self.results else "0%"
            },
            'august_golden_crosses': [
                {
                    'rank': i + 1,
                    'stock_code': stock['stock_code'],
                    'august_crosses': stock['analysis']['august_2024']['golden_count'],
                    'cross_dates': [d.strftime('%Y-%m-%d') for d in stock['analysis']['august_2024']['golden_crosses']],
                    'current_position': stock['analysis']['current_status']['position'],
                    'latest_price': stock['analysis']['current_status']['latest_price'],
                    'trend_strength': stock['analysis']['current_status']['ma_spread_pct'],
                    'historical_golden_crosses': stock['analysis']['historical']['total_golden_crosses'],
                    'data_records': stock['analysis']['data_info']['daily_records']
                }
                for i, stock in enumerate(self.qualified_stocks)
            ],
            'summary_statistics': {
                'total_qualified': len(self.qualified_stocks),
                'average_crosses_per_stock': round(sum(len(s['analysis']['august_2024']['golden_crosses']) for s in self.qualified_stocks) / max(1, len(self.qualified_stocks)), 2),
                'stocks_with_multiple_crosses': len([s for s in self.qualified_stocks if len(s['analysis']['august_2024']['golden_crosses']) > 1]),
                'current_bullish_count': len([s for s in self.qualified_stocks if s['analysis']['current_status']['position'] == 'bullish'])
            }
        }
        
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(export_data, f, ensure_ascii=False, indent=2, default=str)
            print(f"\n💾 结果已导出: {filename}")
            return filename
        except Exception as e:
            print(f"❌ 导出失败: {e}")
            return None

def main():
    """主函数"""
    print("🚀 2024年8月A股黄金交叉筛选器")
    print("=" * 80)
    print("🎯 目标: 筛选8月份十周线上穿百周线的A股")
    print("📡 数据源: 完整历史MktEqud数据")
    
    # 创建筛选器
    screener = AugustGoldenCrossAStockScreener()
    
    # 运行筛选
    qualified_stocks = screener.screen_august_2024_golden_crosses(max_stocks=200)  # 先分析200只进行测试
    
    # 显示结果
    screener.print_august_results(show_top=15)
    
    # 导出结果
    export_file = screener.export_august_results()
    
    print(f"\n🎉 2024年8月A股黄金交叉筛选完成!")
    print(f"✅ 发现 {len(qualified_stocks)} 只A股在8月发生黄金交叉")
    
    if export_file:
        print(f"📄 详细报告: {export_file}")
    
    return qualified_stocks

if __name__ == "__main__":
    results = main()