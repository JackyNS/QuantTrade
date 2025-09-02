#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
基于真实数据的十周线上穿百周线股票筛选器
使用本地UQER数据进行股票筛选
"""

import sys
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
import warnings
import json
import os
warnings.filterwarnings('ignore')

try:
    import talib
    print("✅ TA-Lib 可用")
except ImportError:
    print("❌ TA-Lib 不可用，将使用pandas计算")
    talib = None

class RealDataStockScreener:
    """基于真实数据的股票筛选器"""
    
    def __init__(self, data_root_path="/Users/jackstudio/QuantTrade"):
        """
        初始化筛选器
        
        Args:
            data_root_path: 数据根目录
        """
        self.data_root = Path(data_root_path)
        self.data_path = self.data_root / "data"
        self.results = []
        
        print(f"📁 数据根目录: {self.data_root}")
        
        # 寻找股票数据
        self.find_stock_data_files()
    
    def find_stock_data_files(self):
        """查找股票数据文件"""
        print("🔍 查找股票数据文件...")
        
        # 可能的股票数据位置
        possible_paths = [
            self.data_path / "MktEqud",
            self.data_path / "equity_data", 
            self.data_path / "stock_data",
            self.data_path,
        ]
        
        self.stock_files = []
        
        for path in possible_paths:
            if path.exists():
                print(f"   检查目录: {path}")
                # 查找.parquet文件
                parquet_files = list(path.glob("*.parquet"))
                if parquet_files:
                    self.stock_files.extend(parquet_files)
                    print(f"   发现 {len(parquet_files)} 个parquet文件")
                
                # 查找子目录中的文件
                for subdir in path.iterdir():
                    if subdir.is_dir():
                        sub_parquet = list(subdir.glob("*.parquet"))
                        if sub_parquet:
                            self.stock_files.extend(sub_parquet)
                            print(f"   {subdir.name}: {len(sub_parquet)} 个文件")
        
        print(f"✅ 总共找到 {len(self.stock_files)} 个数据文件")
        return len(self.stock_files) > 0
    
    def load_stock_data(self, file_path, limit_rows=None):
        """
        加载股票数据
        
        Args:
            file_path: 文件路径
            limit_rows: 限制行数
            
        Returns:
            DataFrame: 股票数据
        """
        try:
            # 读取parquet文件
            df = pd.read_parquet(file_path)
            
            if limit_rows:
                df = df.tail(limit_rows)  # 取最新的数据
            
            # 标准化列名
            if 'tradeDate' in df.columns:
                df['tradeDate'] = pd.to_datetime(df['tradeDate'])
            
            # 检查必要的列
            required_cols = ['closePrice', 'tradeDate']
            missing_cols = [col for col in required_cols if col not in df.columns]
            if missing_cols:
                print(f"   ⚠️ 缺少必要列: {missing_cols}")
                return None
            
            return df
            
        except Exception as e:
            print(f"   ❌ 加载文件失败: {e}")
            return None
    
    def calculate_ma_crossover(self, prices_data, short_period=10, long_period=100):
        """
        计算MA交叉信号
        
        Args:
            prices_data: 价格数据 (DataFrame)
            short_period: 短期MA周期
            long_period: 长期MA周期
            
        Returns:
            dict: 分析结果
        """
        try:
            # 确保有足够的数据
            if len(prices_data) < long_period * 7:  # 至少需要long_period周的日线数据
                return {'status': 'insufficient_data', 'data_length': len(prices_data)}
            
            # 按日期排序
            df = prices_data.sort_values('tradeDate').copy()
            
            # 转换为周线数据 (每周的最后一个交易日)
            df_indexed = df.set_index('tradeDate')
            weekly_close = df_indexed['closePrice'].resample('W').last().dropna()
            
            if len(weekly_close) < long_period:
                return {'status': 'insufficient_weekly_data', 'weekly_length': len(weekly_close)}
            
            # 计算移动平均线
            if talib and isinstance(weekly_close.values, np.ndarray):
                ma_short = talib.MA(weekly_close.values, timeperiod=short_period)
                ma_long = talib.MA(weekly_close.values, timeperiod=long_period)
            else:
                ma_short = weekly_close.rolling(short_period).mean().values
                ma_long = weekly_close.rolling(long_period).mean().values
            
            # 创建对齐的Series
            ma_short_series = pd.Series(ma_short, index=weekly_close.index)
            ma_long_series = pd.Series(ma_long, index=weekly_close.index)
            
            # 去除NaN值
            valid_idx = ma_short_series.dropna().index.intersection(ma_long_series.dropna().index)
            if len(valid_idx) < 2:
                return {'status': 'no_valid_data', 'valid_length': len(valid_idx)}
            
            ma_short_clean = ma_short_series.loc[valid_idx]
            ma_long_clean = ma_long_series.loc[valid_idx]
            
            # 计算交叉信号
            position = (ma_short_clean > ma_long_clean).astype(int)
            crossover = position.diff()
            
            # 统计信号
            golden_cross = crossover > 0
            death_cross = crossover < 0
            
            golden_dates = golden_cross[golden_cross].index.tolist()
            death_dates = death_cross[death_cross].index.tolist()
            
            # 当前状态
            current_position = position.iloc[-1] if len(position) > 0 else 0
            latest_price = weekly_close.iloc[-1] if len(weekly_close) > 0 else 0
            
            # 最近信号
            recent_crossovers = crossover[crossover != 0]
            latest_signal = None
            if len(recent_crossovers) > 0:
                latest_signal = {
                    'date': recent_crossovers.index[-1],
                    'type': 'golden_cross' if recent_crossovers.iloc[-1] > 0 else 'death_cross',
                    'signal_value': recent_crossovers.iloc[-1]
                }
            
            return {
                'status': 'success',
                'data_period': {
                    'start': weekly_close.index.min(),
                    'end': weekly_close.index.max(),
                    'weekly_count': len(weekly_close),
                    'valid_count': len(valid_idx)
                },
                'indicators': {
                    'ma_short_period': short_period,
                    'ma_long_period': long_period,
                    'current_position': 'bullish' if current_position > 0 else 'bearish',
                    'latest_price': latest_price
                },
                'signals': {
                    'golden_cross_count': len(golden_dates),
                    'death_cross_count': len(death_dates), 
                    'golden_cross_dates': golden_dates,
                    'death_cross_dates': death_dates,
                    'latest_signal': latest_signal
                }
            }
            
        except Exception as e:
            return {'status': 'error', 'error': str(e)}
    
    def screen_stocks(self, max_stocks=20, min_golden_cross=1):
        """
        筛选股票
        
        Args:
            max_stocks: 最大分析股票数
            min_golden_cross: 最少黄金交叉次数
            
        Returns:
            list: 符合条件的股票
        """
        print(f"🔍 开始筛选股票 (最多分析 {max_stocks} 只)")
        print(f"   筛选条件: 至少 {min_golden_cross} 次黄金交叉")
        print("=" * 60)
        
        self.results = []
        qualified_stocks = []
        
        # 随机选择一些文件进行分析
        import random
        files_to_analyze = random.sample(self.stock_files, min(max_stocks, len(self.stock_files)))
        
        for i, file_path in enumerate(files_to_analyze, 1):
            print(f"📊 [{i}/{len(files_to_analyze)}] 分析: {file_path.stem}")
            
            # 加载数据
            stock_data = self.load_stock_data(file_path, limit_rows=3000)  # 最近3000天的数据
            if stock_data is None:
                print(f"   ❌ 数据加载失败")
                continue
            
            print(f"   📈 数据范围: {len(stock_data)} 条记录")
            print(f"   📅 时间范围: {stock_data['tradeDate'].min().date()} - {stock_data['tradeDate'].max().date()}")
            
            # 分析MA交叉
            analysis = self.calculate_ma_crossover(stock_data)
            
            # 保存结果
            result = {
                'file_name': file_path.stem,
                'file_path': str(file_path),
                'analysis_time': datetime.now(),
                'analysis': analysis
            }
            self.results.append(result)
            
            # 检查是否符合条件
            if analysis['status'] == 'success':
                signals = analysis['signals']
                golden_count = signals['golden_cross_count']
                current_pos = analysis['indicators']['current_position']
                
                print(f"   📊 黄金交叉: {golden_count} 次")
                print(f"   📈 当前状态: {current_pos}")
                
                # 筛选条件
                meets_criteria = (
                    golden_count >= min_golden_cross and
                    current_pos == 'bullish'  # 当前多头
                )
                
                if meets_criteria:
                    qualified_stocks.append(result)
                    print(f"   ✅ 符合筛选条件")
                else:
                    print(f"   ❌ 不符合筛选条件")
                
                # 显示最近信号
                if signals['latest_signal']:
                    latest = signals['latest_signal']
                    signal_type = "🌟 黄金交叉" if latest['type'] == 'golden_cross' else "💀 死叉"
                    print(f"   {signal_type}: {latest['date'].strftime('%Y-%m-%d')}")
                
            else:
                print(f"   ❌ 分析失败: {analysis.get('status', 'unknown')}")
            
            print()  # 空行分隔
        
        print(f"🎯 筛选完成:")
        print(f"   总分析: {len(files_to_analyze)} 只股票")
        print(f"   符合条件: {len(qualified_stocks)} 只")
        print(f"   合格率: {len(qualified_stocks)/len(files_to_analyze)*100:.1f}%")
        
        # 排序 (按黄金交叉次数降序)
        qualified_stocks.sort(
            key=lambda x: x['analysis']['signals']['golden_cross_count'] if x['analysis']['status'] == 'success' else 0,
            reverse=True
        )
        
        return qualified_stocks
    
    def print_results(self, qualified_stocks, show_top=10):
        """打印筛选结果"""
        if not qualified_stocks:
            print("❌ 没有找到符合条件的股票")
            return
        
        print(f"\n🏆 符合条件的股票 (前{min(show_top, len(qualified_stocks))}只):")
        print("=" * 80)
        
        for i, stock in enumerate(qualified_stocks[:show_top], 1):
            analysis = stock['analysis']
            if analysis['status'] != 'success':
                continue
                
            signals = analysis['signals']
            indicators = analysis['indicators']
            
            print(f"{i}. 📊 {stock['file_name']}")
            print(f"   🌟 黄金交叉: {signals['golden_cross_count']} 次")
            print(f"   💀 死叉: {signals['death_cross_count']} 次") 
            print(f"   📈 当前状态: {indicators['current_position']}")
            print(f"   💰 最新价格: {indicators['latest_price']:.2f}")
            
            # 显示最近的交叉信号
            if signals['latest_signal']:
                latest = signals['latest_signal']
                signal_name = "黄金交叉" if latest['type'] == 'golden_cross' else "死叉"
                print(f"   🕒 最近信号: {signal_name} ({latest['date'].strftime('%Y-%m-%d')})")
            
            data_period = analysis['data_period']
            print(f"   📅 分析期间: {data_period['start'].strftime('%Y-%m-%d')} - {data_period['end'].strftime('%Y-%m-%d')}")
            print(f"   📊 有效周数: {data_period['valid_count']}")
            print()
    
    def export_results(self, filename=None):
        """导出筛选结果"""
        if filename is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"ma_crossover_screening_{timestamp}.json"
        
        export_data = {
            'screening_info': {
                'timestamp': datetime.now().isoformat(),
                'total_files_found': len(self.stock_files),
                'analyzed_count': len(self.results),
                'qualified_count': len([r for r in self.results 
                                       if r['analysis']['status'] == 'success' 
                                       and r['analysis']['signals']['golden_cross_count'] >= 1
                                       and r['analysis']['indicators']['current_position'] == 'bullish'])
            },
            'results': self.results
        }
        
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(export_data, f, ensure_ascii=False, indent=2, default=str)
            print(f"💾 结果已导出: {filename}")
            return filename
        except Exception as e:
            print(f"❌ 导出失败: {e}")
            return None

def main():
    """主函数"""
    print("🚀 基于真实数据的十周线上穿百周线股票筛选")
    print("=" * 70)
    
    # 创建筛选器
    screener = RealDataStockScreener()
    
    if len(screener.stock_files) == 0:
        print("❌ 未找到股票数据文件")
        print("💡 请确保 /Users/jackstudio/QuantTrade/data 目录下有股票数据")
        return
    
    # 运行筛选 
    qualified_stocks = screener.screen_stocks(max_stocks=30, min_golden_cross=1)
    
    # 显示结果
    screener.print_results(qualified_stocks, show_top=10)
    
    # 导出结果
    export_file = screener.export_results()
    
    print(f"\n🎉 筛选完成!")
    if export_file:
        print(f"📄 详细结果已保存到: {export_file}")
    
    return qualified_stocks

if __name__ == "__main__":
    results = main()