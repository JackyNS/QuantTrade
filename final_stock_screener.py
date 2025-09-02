#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
最终版十周线上穿百周线股票筛选器
使用样本数据进行完整的策略测试和股票筛选
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
    TALIB_AVAILABLE = True
except ImportError:
    print("❌ TA-Lib 不可用，将使用pandas计算")
    TALIB_AVAILABLE = False

class MACrossoverStockScreener:
    """十周线上穿百周线股票筛选器"""
    
    def __init__(self, data_path="sample_stock_data"):
        """
        初始化筛选器
        
        Args:
            data_path: 样本数据路径
        """
        self.data_path = Path(data_path)
        self.results = []
        self.qualified_stocks = []
        
        print(f"📁 数据路径: {self.data_path}")
        
        # 查找股票数据文件
        self.find_stock_files()
    
    def find_stock_files(self):
        """查找股票数据文件"""
        print("🔍 查找股票数据文件...")
        
        self.stock_files = []
        
        if self.data_path.exists():
            csv_files = list(self.data_path.glob("*_daily.csv"))
            self.stock_files = csv_files
            print(f"   ✅ 找到 {len(csv_files)} 个股票数据文件")
            
            for file in csv_files[:5]:  # 显示前5个文件
                print(f"      - {file.name}")
            
            if len(csv_files) > 5:
                print(f"      ... 还有 {len(csv_files) - 5} 个文件")
        else:
            print(f"   ❌ 数据目录不存在: {self.data_path}")
        
        return len(self.stock_files) > 0
    
    def load_stock_data(self, file_path):
        """
        加载股票数据
        
        Args:
            file_path: 文件路径
            
        Returns:
            DataFrame: 股票数据
        """
        try:
            df = pd.read_csv(file_path)
            
            # 转换日期列
            df['tradeDate'] = pd.to_datetime(df['tradeDate'])
            
            # 检查必要的列
            required_cols = ['tradeDate', 'closePrice']
            if not all(col in df.columns for col in required_cols):
                print(f"   ❌ 缺少必要列: {[col for col in required_cols if col not in df.columns]}")
                return None
            
            # 过滤有效数据
            df_clean = df.dropna(subset=['closePrice', 'tradeDate'])
            df_clean = df_clean[df_clean['closePrice'] > 0]
            df_clean = df_clean.sort_values('tradeDate')
            
            return df_clean
            
        except Exception as e:
            print(f"   ❌ 加载失败: {e}")
            return None
    
    def calculate_ma_crossover(self, price_data, short_period=10, long_period=100):
        """
        计算MA交叉信号
        
        Args:
            price_data: 价格数据
            short_period: 短期MA周期
            long_period: 长期MA周期
            
        Returns:
            dict: 分析结果
        """
        try:
            if len(price_data) < long_period * 7:
                return {'status': 'insufficient_data', 'data_length': len(price_data)}
            
            # 按日期排序
            df = price_data.sort_values('tradeDate').copy()
            
            # 转换为周线数据
            df_indexed = df.set_index('tradeDate')
            weekly_close = df_indexed['closePrice'].resample('W').last().dropna()
            
            if len(weekly_close) < long_period:
                return {
                    'status': 'insufficient_weekly_data', 
                    'weekly_length': len(weekly_close),
                    'required': long_period
                }
            
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
            
            # 去除NaN并找到有效数据
            valid_idx = ma_short_series.dropna().index.intersection(ma_long_series.dropna().index)
            if len(valid_idx) < 2:
                return {'status': 'no_valid_data', 'valid_length': len(valid_idx)}
            
            ma_short_clean = ma_short_series.loc[valid_idx]
            ma_long_clean = ma_long_series.loc[valid_idx]
            
            # 计算交叉信号
            position = (ma_short_clean > ma_long_clean).astype(int)
            crossover = position.diff()
            
            # 统计信号
            golden_cross_mask = crossover > 0
            death_cross_mask = crossover < 0
            
            golden_dates = golden_cross_mask[golden_cross_mask].index.tolist()
            death_dates = death_cross_mask[death_cross_mask].index.tolist()
            
            # 当前状态
            current_position = position.iloc[-1] if len(position) > 0 else 0
            latest_price = weekly_close.iloc[-1] if len(weekly_close) > 0 else 0
            latest_ma_short = ma_short_clean.iloc[-1] if len(ma_short_clean) > 0 else 0
            latest_ma_long = ma_long_clean.iloc[-1] if len(ma_long_clean) > 0 else 0
            
            # 最近信号
            recent_crossovers = crossover[crossover != 0]
            latest_signal = None
            if len(recent_crossovers) > 0:
                signal_date = recent_crossovers.index[-1]
                signal_value = recent_crossovers.iloc[-1]
                latest_signal = {
                    'date': signal_date,
                    'type': 'golden_cross' if signal_value > 0 else 'death_cross',
                    'signal_value': signal_value,
                    'days_ago': (datetime.now() - signal_date).days
                }
            
            # 计算一些额外的指标
            signal_frequency = len(golden_dates) + len(death_dates)
            signal_per_year = signal_frequency / (len(valid_idx) / 52) if len(valid_idx) > 52 else 0
            
            return {
                'status': 'success',
                'data_info': {
                    'daily_records': len(price_data),
                    'weekly_records': len(weekly_close),
                    'valid_records': len(valid_idx),
                    'analysis_period_weeks': len(valid_idx),
                    'analysis_period_years': round(len(valid_idx) / 52, 1)
                },
                'ma_values': {
                    'ma_short_latest': round(latest_ma_short, 2),
                    'ma_long_latest': round(latest_ma_long, 2),
                    'ma_spread': round(latest_ma_short - latest_ma_long, 2),
                    'ma_spread_pct': round((latest_ma_short - latest_ma_long) / latest_ma_long * 100, 2) if latest_ma_long > 0 else 0
                },
                'signals': {
                    'golden_cross_count': len(golden_dates),
                    'death_cross_count': len(death_dates),
                    'total_signals': len(golden_dates) + len(death_dates),
                    'signal_frequency': round(signal_per_year, 1),
                    'golden_cross_dates': golden_dates,
                    'death_cross_dates': death_dates,
                    'latest_signal': latest_signal
                },
                'current_status': {
                    'position': 'bullish' if current_position > 0 else 'bearish',
                    'latest_price': round(latest_price, 2),
                    'trend_strength': abs(latest_ma_short - latest_ma_long) / latest_ma_long if latest_ma_long > 0 else 0
                }
            }
            
        except Exception as e:
            return {'status': 'error', 'error': str(e), 'traceback': str(e)}
    
    def screen_stocks(self, min_golden_cross=1, require_current_bullish=True, min_analysis_years=2):
        """
        筛选股票
        
        Args:
            min_golden_cross: 最少黄金交叉次数
            require_current_bullish: 是否要求当前多头
            min_analysis_years: 最少分析年数
            
        Returns:
            list: 符合条件的股票
        """
        print(f"🔍 开始筛选股票")
        print(f"   筛选条件:")
        print(f"     - 最少黄金交叉次数: {min_golden_cross}")
        print(f"     - 要求当前多头: {'是' if require_current_bullish else '否'}")
        print(f"     - 最少分析年数: {min_analysis_years}")
        print("=" * 70)
        
        self.results = []
        self.qualified_stocks = []
        
        for i, file_path in enumerate(self.stock_files, 1):
            stock_code = file_path.stem.replace('_daily', '')
            print(f"📊 [{i}/{len(self.stock_files)}] 分析股票: {stock_code}")
            
            # 加载数据
            stock_data = self.load_stock_data(file_path)
            if stock_data is None:
                continue
            
            print(f"   📈 数据: {len(stock_data)} 条日线记录")
            print(f"   📅 时间范围: {stock_data['tradeDate'].min().date()} - {stock_data['tradeDate'].max().date()}")
            
            # 分析MA交叉
            analysis = self.calculate_ma_crossover(stock_data)
            
            # 保存结果
            result = {
                'stock_code': stock_code,
                'file_path': str(file_path),
                'analysis_time': datetime.now(),
                'analysis': analysis
            }
            self.results.append(result)
            
            # 检查是否符合条件
            if analysis['status'] == 'success':
                signals = analysis['signals']
                current_status = analysis['current_status']
                data_info = analysis['data_info']
                ma_values = analysis['ma_values']
                
                print(f"   📊 分析结果:")
                print(f"      🌟 黄金交叉: {signals['golden_cross_count']} 次")
                print(f"      💀 死叉: {signals['death_cross_count']} 次")
                print(f"      📈 当前状态: {current_status['position']}")
                print(f"      💰 最新价格: {current_status['latest_price']}")
                print(f"      📏 MA差价: {ma_values['ma_spread']} ({ma_values['ma_spread_pct']}%)")
                print(f"      📆 分析时长: {data_info['analysis_period_years']} 年")
                
                # 检查筛选条件
                meets_criteria = True
                reasons = []
                
                # 条件1: 黄金交叉次数
                if signals['golden_cross_count'] < min_golden_cross:
                    meets_criteria = False
                    reasons.append(f"黄金交叉次数不足({signals['golden_cross_count']}<{min_golden_cross})")
                
                # 条件2: 当前多头状态
                if require_current_bullish and current_status['position'] != 'bullish':
                    meets_criteria = False
                    reasons.append("当前非多头状态")
                
                # 条件3: 分析时长
                if data_info['analysis_period_years'] < min_analysis_years:
                    meets_criteria = False
                    reasons.append(f"分析时长不足({data_info['analysis_period_years']}<{min_analysis_years}年)")
                
                if meets_criteria:
                    self.qualified_stocks.append(result)
                    print(f"      ✅ 符合筛选条件")
                    
                    # 显示最近信号
                    if signals['latest_signal']:
                        latest = signals['latest_signal']
                        signal_type = "🌟 黄金交叉" if latest['type'] == 'golden_cross' else "💀 死叉"
                        print(f"      🕒 最近信号: {signal_type} ({latest['date'].strftime('%Y-%m-%d')}, {latest['days_ago']}天前)")
                else:
                    print(f"      ❌ 不符合条件: {'; '.join(reasons)}")
                
            else:
                status = analysis['status']
                print(f"   ❌ 分析失败: {status}")
                if 'error' in analysis:
                    print(f"      错误: {analysis['error']}")
            
            print()  # 空行分隔
        
        # 按黄金交叉次数和趋势强度排序
        self.qualified_stocks.sort(key=lambda x: (
            x['analysis']['signals']['golden_cross_count'],
            x['analysis']['current_status']['trend_strength']
        ), reverse=True)
        
        print(f"🎯 筛选完成:")
        print(f"   📊 总分析股票: {len(self.stock_files)}")
        print(f"   ✅ 符合条件股票: {len(self.qualified_stocks)}")
        print(f"   📈 合格率: {len(self.qualified_stocks)/len(self.stock_files)*100:.1f}%")
        
        return self.qualified_stocks
    
    def print_detailed_results(self, show_top=10):
        """打印详细的筛选结果"""
        if not self.qualified_stocks:
            print("\n❌ 没有找到符合条件的股票")
            return
        
        print(f"\n🏆 符合条件的股票排行榜 (前{min(show_top, len(self.qualified_stocks))}名):")
        print("=" * 90)
        
        for i, stock in enumerate(self.qualified_stocks[:show_top], 1):
            analysis = stock['analysis']
            if analysis['status'] != 'success':
                continue
            
            signals = analysis['signals']
            current_status = analysis['current_status']
            ma_values = analysis['ma_values']
            data_info = analysis['data_info']
            
            print(f"\n🥇 第{i}名: {stock['stock_code']}")
            print(f"    📊 交叉信号: 🌟{signals['golden_cross_count']}次黄金交叉, 💀{signals['death_cross_count']}次死叉")
            print(f"    📈 当前状态: {current_status['position']}")
            print(f"    💰 最新价格: {current_status['latest_price']}")
            print(f"    📏 移动平均: MA10={ma_values['ma_short_latest']}, MA100={ma_values['ma_long_latest']}")
            print(f"    📊 趋势强度: {ma_values['ma_spread_pct']}% (MA10-MA100差幅)")
            print(f"    📆 分析期间: {data_info['analysis_period_years']}年 ({data_info['valid_records']}周)")
            print(f"    🔄 信号频率: {signals['signal_frequency']}次/年")
            
            # 显示最近的信号
            if signals['latest_signal']:
                latest = signals['latest_signal']
                signal_icon = "🌟" if latest['type'] == 'golden_cross' else "💀"
                signal_name = "黄金交叉" if latest['type'] == 'golden_cross' else "死叉"
                print(f"    🕒 最近信号: {signal_icon} {signal_name} ({latest['date'].strftime('%Y-%m-%d')}, {latest['days_ago']}天前)")
            
            # 显示黄金交叉历史
            if len(signals['golden_cross_dates']) > 0:
                recent_golden = signals['golden_cross_dates'][-3:]  # 最近3次
                golden_str = ", ".join([d.strftime('%Y-%m') for d in recent_golden])
                print(f"    🌟 近期黄金交叉: {golden_str}")
    
    def export_results(self, filename=None):
        """导出筛选结果"""
        if filename is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"ma_crossover_screening_final_{timestamp}.json"
        
        export_data = {
            'screening_info': {
                'timestamp': datetime.now().isoformat(),
                'strategy': '十周线上穿百周线 (MA10/MA100交叉)',
                'total_stocks': len(self.stock_files),
                'analyzed_count': len(self.results),
                'qualified_count': len(self.qualified_stocks),
                'qualification_rate': f"{len(self.qualified_stocks)/len(self.stock_files)*100:.1f}%"
            },
            'qualified_stocks': [
                {
                    'rank': i + 1,
                    'stock_code': stock['stock_code'],
                    'golden_crosses': stock['analysis']['signals']['golden_cross_count'],
                    'current_position': stock['analysis']['current_status']['position'],
                    'latest_price': stock['analysis']['current_status']['latest_price'],
                    'trend_strength': stock['analysis']['current_status']['trend_strength'],
                    'analysis_years': stock['analysis']['data_info']['analysis_period_years'],
                    'latest_signal': stock['analysis']['signals']['latest_signal']
                }
                for i, stock in enumerate(self.qualified_stocks)
            ],
            'all_results': self.results
        }
        
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(export_data, f, ensure_ascii=False, indent=2, default=str)
            print(f"\n💾 筛选结果已导出: {filename}")
            return filename
        except Exception as e:
            print(f"\n❌ 导出失败: {e}")
            return None

def main():
    """主函数"""
    print("🚀 十周线上穿百周线股票筛选 - 最终版")
    print("=" * 70)
    
    # 创建筛选器
    screener = MACrossoverStockScreener()
    
    if len(screener.stock_files) == 0:
        print("❌ 未找到股票数据文件")
        print("💡 请先运行 create_sample_stock_data.py 生成样本数据")
        return []
    
    # 运行筛选
    qualified_stocks = screener.screen_stocks(
        min_golden_cross=1,           # 至少1次黄金交叉
        require_current_bullish=True, # 当前必须多头
        min_analysis_years=2          # 至少2年数据
    )
    
    # 显示详细结果
    screener.print_detailed_results(show_top=8)
    
    # 导出结果
    export_file = screener.export_results()
    
    print(f"\n🎉 股票筛选完成!")
    print(f"✅ 找到 {len(qualified_stocks)} 只符合十周线上穿百周线条件的股票")
    
    if export_file:
        print(f"📄 详细分析报告: {export_file}")
    
    return qualified_stocks

if __name__ == "__main__":
    results = main()