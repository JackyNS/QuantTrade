#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
8月份黄金交叉A股筛选器
从全A股中筛选出8月份发生十周线上穿百周线的股票
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

# 尝试导入UQER
try:
    import uqer
    print("✅ UQER API 可用")
    UQER_AVAILABLE = True
except ImportError:
    print("❌ UQER API 不可用")
    UQER_AVAILABLE = False

class AugustGoldenCrossScreener:
    """8月份黄金交叉筛选器"""
    
    def __init__(self):
        """初始化筛选器"""
        self.results = []
        self.qualified_stocks = []
        
        # 设置UQER token
        self.setup_uqer()
        
    def setup_uqer(self):
        """设置UQER连接"""
        try:
            if UQER_AVAILABLE:
                # 从环境变量或直接设置token
                uqer_token = "68b9922817ae6273137bda7acba81e293582ba347281dfcc056dcb245b23faf3"
                uqer.Client(token=uqer_token)
                print("✅ UQER连接成功")
                self.uqer_connected = True
            else:
                self.uqer_connected = False
        except Exception as e:
            print(f"❌ UQER连接失败: {e}")
            self.uqer_connected = False
    
    def get_all_a_stocks(self):
        """获取全A股股票列表"""
        print("📋 获取全A股股票列表...")
        
        if not self.uqer_connected:
            # 使用样本股票列表作为后备
            print("⚠️ UQER不可用，使用预设A股样本")
            return [
                '000001.XSHE',  # 平安银行
                '000002.XSHE',  # 万科A
                '000858.XSHE',  # 五粮液
                '600036.XSHG',  # 招商银行
                '600519.XSHG',  # 贵州茅台
                '002415.XSHE',  # 海康威视
                '000725.XSHE',  # 京东方A
                '600887.XSHG',  # 伊利股份
                '000063.XSHE',  # 中兴通讯
                '600276.XSHG',  # 恒瑞医药
                '600000.XSHG',  # 浦发银行
                '000001.XSHG',  # 上证指数 (这里会过滤掉)
                '300059.XSHE',  # 东方财富
                '000166.XSHE',  # 申万宏源
                '002594.XSHE',  # 比亚迪
                '600031.XSHG',  # 三一重工
                '600009.XSHG',  # 上海机场
                '000338.XSHE',  # 潍柴动力
                '002304.XSHE',  # 洋河股份
                '600104.XSHG'   # 上汽集团
            ]
        
        try:
            # 使用UQER获取A股列表
            df = uqer.DataAPI.EquGet(
                listStatusCD='L',  # 上市状态
                field='secID,ticker,secShortName,listDate',
                pandas=1
            )
            
            if df is not None and len(df) > 0:
                # 过滤A股（排除指数、港股等）
                a_stocks = df[
                    (df['secID'].str.contains('.XSHE|.XSHG')) &  # 深交所或上交所
                    (~df['secID'].str.contains('.ZICN|.INDX'))   # 排除指数
                ].copy()
                
                stock_list = a_stocks['secID'].tolist()
                print(f"✅ 获取到 {len(stock_list)} 只A股")
                return stock_list[:50]  # 限制为前50只进行测试
            else:
                print("⚠️ UQER返回数据为空，使用预设样本")
                return self.get_sample_stocks()
                
        except Exception as e:
            print(f"❌ 获取A股列表失败: {e}")
            return self.get_sample_stocks()
    
    def get_sample_stocks(self):
        """获取样本股票列表"""
        return [
            '000001.XSHE', '000002.XSHE', '000858.XSHE', '600036.XSHG', '600519.XSHG',
            '002415.XSHE', '000725.XSHE', '600887.XSHG', '000063.XSHE', '600276.XSHG',
            '600000.XSHG', '300059.XSHE', '000166.XSHE', '002594.XSHE', '600031.XSHG'
        ]
    
    def get_stock_data(self, stock_code, start_date='2023-01-01', end_date='2024-09-02'):
        """
        获取股票日线数据
        
        Args:
            stock_code: 股票代码 (如 '000001.XSHE')
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            DataFrame: 股票数据
        """
        try:
            if not self.uqer_connected:
                return None
            
            # 使用UQER获取日线数据
            df = uqer.DataAPI.MktEqudGet(
                secID=stock_code,
                beginDate=start_date.replace('-', ''),
                endDate=end_date.replace('-', ''),
                field='secID,tradeDate,openPrice,highestPrice,lowestPrice,closePrice,turnoverVol',
                pandas=1
            )
            
            if df is None or len(df) == 0:
                return None
            
            # 标准化列名
            df = df.rename(columns={
                'highestPrice': 'highPrice',
                'lowestPrice': 'lowPrice',
                'turnoverVol': 'volume'
            })
            
            # 转换日期格式
            df['tradeDate'] = pd.to_datetime(df['tradeDate'])
            
            # 过滤有效数据
            df = df.dropna(subset=['closePrice'])
            df = df[df['closePrice'] > 0]
            df = df.sort_values('tradeDate')
            
            return df
            
        except Exception as e:
            print(f"   ⚠️ 获取 {stock_code} 数据失败: {e}")
            return None
    
    def calculate_ma_crossover_august(self, price_data, short_period=10, long_period=100):
        """
        计算8月份的MA交叉信号
        
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
            
            # 筛选8月份的黄金交叉
            golden_cross_mask = crossover > 0
            golden_dates = golden_cross_mask[golden_cross_mask].index
            
            # 筛选8月份的黄金交叉 (2024年8月)
            august_2024_start = pd.Timestamp('2024-08-01')
            august_2024_end = pd.Timestamp('2024-08-31')
            
            august_golden_crosses = golden_dates[
                (golden_dates >= august_2024_start) & 
                (golden_dates <= august_2024_end)
            ]
            
            # 当前状态
            current_position = position.iloc[-1] if len(position) > 0 else 0
            latest_price = weekly_close.iloc[-1] if len(weekly_close) > 0 else 0
            
            # 最近的MA值
            latest_ma_short = ma_short_clean.iloc[-1] if len(ma_short_clean) > 0 else 0
            latest_ma_long = ma_long_clean.iloc[-1] if len(ma_long_clean) > 0 else 0
            
            return {
                'status': 'success',
                'august_golden_crosses': list(august_golden_crosses),
                'august_golden_count': len(august_golden_crosses),
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
                    'data_start': weekly_close.index.min(),
                    'data_end': weekly_close.index.max()
                }
            }
            
        except Exception as e:
            return {'status': 'error', 'error': str(e)}
    
    def screen_august_golden_cross(self):
        """筛选8月份黄金交叉的股票"""
        print("🔍 开始筛选8月份黄金交叉股票")
        print("   筛选条件: 2024年8月发生十周线上穿百周线")
        print("=" * 70)
        
        # 获取A股列表
        stock_list = self.get_all_a_stocks()
        
        self.results = []
        self.qualified_stocks = []
        
        print(f"📊 开始分析 {len(stock_list)} 只股票...")
        
        for i, stock_code in enumerate(stock_list, 1):
            print(f"📈 [{i}/{len(stock_list)}] 分析: {stock_code}")
            
            # 获取股票数据
            stock_data = self.get_stock_data(stock_code)
            
            if stock_data is None:
                print(f"   ❌ 数据获取失败")
                continue
            
            print(f"   📅 数据范围: {stock_data['tradeDate'].min().date()} - {stock_data['tradeDate'].max().date()}")
            print(f"   📊 记录数: {len(stock_data)} 条")
            
            # 分析8月份黄金交叉
            analysis = self.calculate_ma_crossover_august(stock_data)
            
            # 保存结果
            result = {
                'stock_code': stock_code,
                'analysis_time': datetime.now(),
                'analysis': analysis
            }
            self.results.append(result)
            
            # 检查是否符合条件
            if analysis['status'] == 'success':
                august_count = analysis['august_golden_count']
                current_status = analysis['current_status']
                
                print(f"   🌟 8月黄金交叉: {august_count} 次")
                print(f"   📈 当前状态: {current_status['position']}")
                print(f"   💰 最新价格: {current_status['latest_price']}")
                
                if august_count > 0:
                    self.qualified_stocks.append(result)
                    print(f"   ✅ 符合条件 - 8月发生黄金交叉")
                    
                    # 显示8月黄金交叉日期
                    for date in analysis['august_golden_crosses']:
                        print(f"      🌟 黄金交叉日期: {date.strftime('%Y-%m-%d')}")
                else:
                    print(f"   ❌ 8月无黄金交叉")
            else:
                print(f"   ❌ 分析失败: {analysis.get('status', 'unknown')}")
            
            print()  # 空行
        
        # 按黄金交叉日期排序（最近的在前）
        self.qualified_stocks.sort(key=lambda x: max(x['analysis']['august_golden_crosses']) if x['analysis']['august_golden_crosses'] else pd.Timestamp('1900-01-01'), reverse=True)
        
        print(f"🎯 筛选完成:")
        print(f"   📊 总分析: {len(stock_list)} 只股票")
        print(f"   ✅ 8月黄金交叉: {len(self.qualified_stocks)} 只")
        print(f"   📈 命中率: {len(self.qualified_stocks)/len(stock_list)*100:.1f}%")
        
        return self.qualified_stocks
    
    def print_august_results(self):
        """打印8月份黄金交叉结果"""
        if not self.qualified_stocks:
            print("❌ 未发现8月份黄金交叉的股票")
            return
        
        print(f"\n🏆 2024年8月黄金交叉股票榜单:")
        print("=" * 80)
        
        for i, stock in enumerate(self.qualified_stocks, 1):
            analysis = stock['analysis']
            current_status = analysis['current_status']
            
            print(f"\n🥇 第{i}名: {stock['stock_code']}")
            print(f"    🌟 8月黄金交叉次数: {analysis['august_golden_count']}")
            
            # 显示每次黄金交叉的日期
            for j, date in enumerate(analysis['august_golden_crosses'], 1):
                days_ago = (datetime.now() - date).days
                print(f"    📅 第{j}次: {date.strftime('%Y-%m-%d')} ({days_ago}天前)")
            
            print(f"    📈 当前状态: {current_status['position']}")
            print(f"    💰 最新价格: {current_status['latest_price']}")
            print(f"    📊 MA状态: 10周MA={current_status['ma_short']}, 100周MA={current_status['ma_long']}")
            print(f"    📏 趋势强度: {current_status['ma_spread_pct']}%")
            
            data_info = analysis['data_info']
            print(f"    📆 数据期间: {data_info['data_start'].strftime('%Y-%m')} - {data_info['data_end'].strftime('%Y-%m')}")
    
    def export_august_results(self, filename=None):
        """导出8月份黄金交叉结果"""
        if filename is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"august_golden_cross_a_stocks_{timestamp}.json"
        
        export_data = {
            'screening_info': {
                'timestamp': datetime.now().isoformat(),
                'strategy': '2024年8月十周线上穿百周线筛选',
                'target_month': '2024-08',
                'total_analyzed': len(self.results),
                'qualified_count': len(self.qualified_stocks),
                'hit_rate': f"{len(self.qualified_stocks)/len(self.results)*100:.1f}%" if self.results else "0%"
            },
            'august_golden_crosses': [
                {
                    'rank': i + 1,
                    'stock_code': stock['stock_code'],
                    'august_crosses': stock['analysis']['august_golden_count'],
                    'cross_dates': [d.strftime('%Y-%m-%d') for d in stock['analysis']['august_golden_crosses']],
                    'current_position': stock['analysis']['current_status']['position'],
                    'latest_price': stock['analysis']['current_status']['latest_price'],
                    'trend_strength': stock['analysis']['current_status']['ma_spread_pct']
                }
                for i, stock in enumerate(self.qualified_stocks)
            ],
            'all_results': len(self.results)
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
    print("=" * 70)
    print("🎯 目标: 筛选出8月份发生十周线上穿百周线的A股")
    
    # 创建筛选器
    screener = AugustGoldenCrossScreener()
    
    # 运行筛选
    qualified_stocks = screener.screen_august_golden_cross()
    
    # 显示结果
    screener.print_august_results()
    
    # 导出结果
    export_file = screener.export_august_results()
    
    print(f"\n🎉 8月黄金交叉筛选完成!")
    print(f"✅ 发现 {len(qualified_stocks)} 只股票在8月发生黄金交叉")
    
    if export_file:
        print(f"📄 详细报告: {export_file}")
    
    return qualified_stocks

if __name__ == "__main__":
    results = main()