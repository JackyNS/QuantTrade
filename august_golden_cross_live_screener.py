#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
2024年8月黄金交叉实时筛选器
直接通过UQER API获取A股数据并筛选8月份黄金交叉
"""

import sys
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
import warnings
import json
import time
warnings.filterwarnings('ignore')

try:
    import talib
    print("✅ TA-Lib 可用")
    TALIB_AVAILABLE = True
except ImportError:
    print("❌ TA-Lib 不可用，将使用pandas计算")
    TALIB_AVAILABLE = False

try:
    import uqer
    print("✅ UQER API 可用")
    UQER_AVAILABLE = True
except ImportError:
    print("❌ UQER API 不可用")
    UQER_AVAILABLE = False
    sys.exit(1)

class LiveAugustGoldenCrossScreener:
    """实时8月黄金交叉筛选器"""
    
    def __init__(self):
        """初始化筛选器"""
        self.setup_uqer()
        self.results = []
        self.qualified_stocks = []
        
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
    
    def get_a_stock_list(self):
        """获取A股列表"""
        print("📋 获取A股列表...")
        
        try:
            # 获取上市股票
            result = uqer.DataAPI.EquGet(
                listStatusCD='L',
                pandas=1
            )
            
            if result is None or len(result) == 0:
                # 使用备用股票列表
                return self.get_backup_stocks()
            
            # 处理CSV字符串或DataFrame
            if isinstance(result, str):
                # 转换CSV字符串为DataFrame
                from io import StringIO
                df = pd.read_csv(StringIO(result))
            elif isinstance(result, pd.DataFrame):
                df = result
            else:
                return self.get_backup_stocks()
            
            # 过滤A股
            a_stocks = df[
                df['secID'].str.contains('.XSHE|.XSHG', na=False)
            ].copy()
            
            # 排除指数
            a_stocks = a_stocks[
                ~a_stocks['secID'].str.contains('.ZICN|.INDX', na=False)
            ]
            
            stock_list = a_stocks['secID'].unique().tolist()
            print(f"✅ 获取到 {len(stock_list)} 只A股")
            return stock_list[:200]  # 扩大测试范围
                
        except Exception as e:
            print(f"⚠️ 获取股票列表失败: {e}, 使用备用列表")
            return self.get_backup_stocks()
    
    def get_backup_stocks(self):
        """备用A股列表"""
        return [
            # 主要蓝筹股
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
            
            # 金融股
            '600000.XSHG',  # 浦发银行
            '601166.XSHG',  # 兴业银行
            '601318.XSHG',  # 中国平安
            '601628.XSHG',  # 中国人寿
            '600030.XSHG',  # 中信证券
            
            # 科技股
            '300059.XSHE',  # 东方财富
            '000166.XSHE',  # 申万宏源
            '002241.XSHE',  # 歌尔股份
            '300015.XSHE',  # 爱尔眼科
            '688981.XSHG',  # 中芯国际
            
            # 新能源
            '002594.XSHE',  # 比亚迪
            '300750.XSHE',  # 宁德时代
            '002129.XSHE',  # 中环股份
            
            # 消费股
            '000568.XSHE',  # 泸州老窖
            '002304.XSHE',  # 洋河股份
            '600104.XSHG',  # 上汽集团
            '000338.XSHE',  # 潍柴动力
            '600031.XSHG',  # 三一重工
            '600009.XSHG',  # 上海机场
            
            # 医药股
            '000661.XSHE',  # 长春高新
            '002821.XSHE',  # 凯莱英
        ]
    
    def get_stock_data(self, stock_code, start_date='2020-01-01', end_date='2024-09-02'):
        """获取股票历史数据"""
        try:
            result = uqer.DataAPI.MktEqudGet(
                secID=stock_code,
                beginDate=start_date.replace('-', ''),
                endDate=end_date.replace('-', ''),
                pandas=1
            )
            
            if result is None or len(result) == 0:
                return None
            
            # 处理CSV字符串或DataFrame
            if isinstance(result, str):
                # 转换CSV字符串为DataFrame
                from io import StringIO
                df = pd.read_csv(StringIO(result))
            elif isinstance(result, pd.DataFrame):
                df = result.copy()
            else:
                return None
            
            # 数据处理
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
            return None
    
    def calculate_august_golden_cross(self, price_data, short_period=10, long_period=100):
        """计算8月黄金交叉"""
        try:
            if len(price_data) < long_period * 7:
                return {'status': 'insufficient_data', 'data_length': len(price_data)}
            
            # 转换为周线数据
            df_indexed = price_data.set_index('tradeDate')
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
                return {'status': 'no_valid_data'}
            
            ma_short_clean = ma_short_series.loc[valid_idx]
            ma_long_clean = ma_long_series.loc[valid_idx]
            
            # 计算交叉信号
            position = (ma_short_clean > ma_long_clean).astype(int)
            crossover = position.diff()
            
            # 找到黄金交叉
            golden_cross_mask = crossover > 0
            all_golden_dates = golden_cross_mask[golden_cross_mask].index
            
            # 筛选8月黄金交叉
            august_start = pd.Timestamp('2024-08-01')
            august_end = pd.Timestamp('2024-08-31')
            
            august_golden = all_golden_dates[
                (all_golden_dates >= august_start) & 
                (all_golden_dates <= august_end)
            ]
            
            # 当前状态
            current_position = position.iloc[-1] if len(position) > 0 else 0
            latest_price = weekly_close.iloc[-1] if len(weekly_close) > 0 else 0
            latest_ma_short = ma_short_clean.iloc[-1] if len(ma_short_clean) > 0 else 0
            latest_ma_long = ma_long_clean.iloc[-1] if len(ma_long_clean) > 0 else 0
            
            return {
                'status': 'success',
                'august_golden_crosses': list(august_golden),
                'august_golden_count': len(august_golden),
                'total_golden_crosses': len(all_golden_dates),
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
                    'data_start': price_data['tradeDate'].min(),
                    'data_end': price_data['tradeDate'].max()
                }
            }
            
        except Exception as e:
            return {'status': 'error', 'error': str(e)}
    
    def screen_august_golden_crosses(self):
        """筛选8月黄金交叉股票"""
        print("🔍 开始筛选2024年8月A股黄金交叉")
        print("   🎯 策略: 十周线上穿百周线")
        print("   📅 目标月份: 2024年8月")
        print("=" * 80)
        
        # 获取股票列表
        stock_list = self.get_a_stock_list()
        
        if not stock_list:
            print("❌ 无法获取股票列表")
            return []
        
        print(f"📊 开始分析 {len(stock_list)} 只A股...")
        
        self.results = []
        self.qualified_stocks = []
        
        for i, stock_code in enumerate(stock_list, 1):
            print(f"📈 [{i}/{len(stock_list)}] 分析: {stock_code}")
            
            # 获取股票数据
            stock_data = self.get_stock_data(stock_code)
            
            if stock_data is None or len(stock_data) == 0:
                print(f"   ❌ 数据获取失败")
                continue
            
            print(f"   📊 数据: {len(stock_data)} 条记录")
            print(f"   📅 时间: {stock_data['tradeDate'].min().date()} - {stock_data['tradeDate'].max().date()}")
            
            # 分析黄金交叉
            analysis = self.calculate_august_golden_cross(stock_data)
            
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
                total_golden = analysis['total_golden_crosses']
                
                print(f"   🌟 8月黄金交叉: {august_count} 次")
                print(f"   📈 当前状态: {current_status['position']}")
                print(f"   💰 最新价格: {current_status['latest_price']}")
                print(f"   📊 历史黄金交叉: {total_golden} 次")
                
                if august_count > 0:
                    self.qualified_stocks.append(result)
                    print(f"   ✅ 符合条件!")
                    
                    # 显示8月黄金交叉日期
                    for date in analysis['august_golden_crosses']:
                        days_ago = (datetime.now() - date).days
                        print(f"      🌟 黄金交叉: {date.strftime('%Y-%m-%d')} ({days_ago}天前)")
                else:
                    print(f"   ❌ 8月无黄金交叉")
                    
            else:
                print(f"   ❌ 分析失败: {analysis.get('status', 'unknown')}")
            
            print()
            
            # 控制API调用频率
            time.sleep(0.5)
        
        # 排序结果
        self.qualified_stocks.sort(
            key=lambda x: max(x['analysis']['august_golden_crosses']) if x['analysis']['august_golden_crosses'] else pd.Timestamp('2024-08-01'),
            reverse=True
        )
        
        print(f"🎯 筛选完成:")
        print(f"   📊 总分析: {len(stock_list)} 只A股")
        print(f"   ✅ 8月黄金交叉: {len(self.qualified_stocks)} 只")
        print(f"   📈 命中率: {len(self.qualified_stocks)/len(stock_list)*100:.1f}%")
        
        return self.qualified_stocks
    
    def print_results(self):
        """打印筛选结果"""
        if not self.qualified_stocks:
            print("❌ 未发现2024年8月黄金交叉的A股")
            return
        
        print(f"\n🏆 2024年8月A股黄金交叉榜单:")
        print("=" * 90)
        
        for i, stock in enumerate(self.qualified_stocks, 1):
            analysis = stock['analysis']
            current_status = analysis['current_status']
            
            print(f"\n🥇 第{i}名: {stock['stock_code']}")
            print(f"    🌟 8月黄金交叉: {analysis['august_golden_count']} 次")
            
            # 显示黄金交叉日期
            for j, date in enumerate(analysis['august_golden_crosses'], 1):
                days_ago = (datetime.now() - date).days
                weekday = date.strftime('%A')
                print(f"    📅 第{j}次: {date.strftime('%Y-%m-%d')} ({weekday}, {days_ago}天前)")
            
            print(f"    📈 当前状态: {current_status['position']}")
            print(f"    💰 最新价格: {current_status['latest_price']}")
            print(f"    📊 MA状态: 10周={current_status['ma_short']}, 100周={current_status['ma_long']}")
            print(f"    📏 趋势强度: {current_status['ma_spread_pct']}%")
            print(f"    📚 历史统计: 总共{analysis['total_golden_crosses']}次黄金交叉")
            
            data_info = analysis['data_info']
            print(f"    📆 数据范围: {data_info['data_start'].strftime('%Y-%m')} - {data_info['data_end'].strftime('%Y-%m')}")
    
    def export_results(self, filename=None):
        """导出结果"""
        if filename is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"august_2024_golden_cross_live_{timestamp}.json"
        
        export_data = {
            'screening_info': {
                'timestamp': datetime.now().isoformat(),
                'strategy': '2024年8月A股黄金交叉实时筛选',
                'method': '直接UQER API获取数据',
                'total_analyzed': len(self.results),
                'qualified_count': len(self.qualified_stocks),
                'hit_rate': f"{len(self.qualified_stocks)/len(self.results)*100:.1f}%" if self.results else "0%"
            },
            'qualified_stocks': [
                {
                    'rank': i + 1,
                    'stock_code': stock['stock_code'],
                    'august_crosses': stock['analysis']['august_golden_count'],
                    'cross_dates': [d.strftime('%Y-%m-%d') for d in stock['analysis']['august_golden_crosses']],
                    'current_position': stock['analysis']['current_status']['position'],
                    'latest_price': stock['analysis']['current_status']['latest_price'],
                    'trend_strength': stock['analysis']['current_status']['ma_spread_pct'],
                    'total_golden_crosses': stock['analysis']['total_golden_crosses']
                }
                for i, stock in enumerate(self.qualified_stocks)
            ]
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
    print("🚀 2024年8月A股黄金交叉实时筛选器")
    print("=" * 80)
    print("🎯 实时获取数据并筛选8月黄金交叉A股")
    
    # 创建筛选器
    screener = LiveAugustGoldenCrossScreener()
    
    # 运行筛选
    qualified_stocks = screener.screen_august_golden_crosses()
    
    # 显示结果
    screener.print_results()
    
    # 导出结果
    export_file = screener.export_results()
    
    print(f"\n🎉 实时筛选完成!")
    print(f"✅ 发现 {len(qualified_stocks)} 只A股在8月发生黄金交叉")
    
    if export_file:
        print(f"📄 详细报告: {export_file}")
    
    return qualified_stocks

if __name__ == "__main__":
    results = main()