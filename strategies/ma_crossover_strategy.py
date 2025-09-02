#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
十周线上穿百周线策略 - 黄金交叉策略
=================================

这是基于移动平均线交叉的经典技术分析策略：
- 10周移动平均线上穿100周移动平均线时买入
- 反之形成死叉时卖出
- 这个模式被称为"黄金交叉"，是市场转强的重要信号

策略特点:
✅ 经典技术分析指标
✅ 适用于中长期趋势跟踪
✅ 信号明确，执行简单
✅ 适合趋势性市场

作者: QuantTrader Team
版本: 1.0.0
日期: 2025-09-01
"""

import sys
import pandas as pd
import numpy as np
import warnings
from pathlib import Path
from datetime import datetime, timedelta

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# 导入核心模块
try:
    from core.strategy import BaseStrategy, TechnicalStrategy
    from core.data import create_data_manager_safe
    from core.utils import get_logger, validate_dataframe
    import talib
except ImportError as e:
    print(f"⚠️ 导入模块失败: {e}")
    print("正在尝试基础导入...")

warnings.filterwarnings('ignore')

class MACrossoverStrategy(TechnicalStrategy):
    """十周线上穿百周线策略"""
    
    def __init__(self, short_period=10, long_period=100, **kwargs):
        """
        初始化策略
        
        Args:
            short_period: 短期移动平均线周期 (默认10周)
            long_period: 长期移动平均线周期 (默认100周) 
            **kwargs: 其他参数
        """
        super().__init__(name="ma_crossover_10_100", **kwargs)
        
        # 策略参数
        self.short_period = short_period  # 10周
        self.long_period = long_period    # 100周
        
        # 策略状态
        self.indicators = {}
        self.signals = {}
        self.current_position = 0  # 0=空仓, 1=多头, -1=空头
        
        # 统计信息
        self.stats = {
            'total_signals': 0,
            'golden_cross_count': 0,  # 黄金交叉次数
            'death_cross_count': 0,   # 死叉次数
            'last_signal_date': None,
            'signal_accuracy': 0.0
        }
        
        # 创建日志器
        try:
            self.logger = get_logger(f"strategy.{self.name}")
            self.logger.info(f"初始化策略: {self.name}")
            self.logger.info(f"参数设置: 短期MA={short_period}周, 长期MA={long_period}周")
        except:
            print(f"✅ 策略初始化: {self.name}")
    
    def calculate_weekly_ma(self, daily_data, period):
        """
        将日线数据转换为周线，并计算移动平均线
        
        Args:
            daily_data: 日线价格数据
            period: 移动平均线周期
            
        Returns:
            pandas.Series: 周线移动平均线
        """
        try:
            # 确保数据是DataFrame且有必要的列
            if isinstance(daily_data, pd.Series):
                df = daily_data.to_frame('close')
            else:
                df = daily_data.copy()
                
            # 重新设置索引为日期（如果不是的话）
            if not isinstance(df.index, pd.DatetimeIndex):
                if 'tradeDate' in df.columns:
                    df['tradeDate'] = pd.to_datetime(df['tradeDate'])
                    df = df.set_index('tradeDate')
                else:
                    df.index = pd.to_datetime(df.index)
            
            # 确保有价格列
            price_col = None
            for col in ['closePrice', 'close', 'Close']:
                if col in df.columns:
                    price_col = col
                    break
            
            if price_col is None:
                raise ValueError("找不到价格数据列")
            
            # 转换为周线数据 (每周最后一个交易日的收盘价)
            weekly_data = df[price_col].resample('W').last().dropna()
            
            # 计算移动平均线
            if len(weekly_data) >= period:
                # 使用TA-Lib计算移动平均线（如果可用）
                try:
                    ma = talib.MA(weekly_data.values, timeperiod=period)
                    return pd.Series(ma, index=weekly_data.index, name=f'MA{period}')
                except:
                    # 备选方案：使用pandas计算
                    return weekly_data.rolling(window=period, min_periods=period).mean()
            else:
                self.logger.warning(f"数据不足，无法计算{period}周移动平均线")
                return pd.Series(dtype=float)
                
        except Exception as e:
            self.logger.error(f"计算周线移动平均线失败: {e}")
            return pd.Series(dtype=float)
    
    def calculate_indicators(self, data):
        """
        计算技术指标
        
        Args:
            data: 股票价格数据 (日线)
            
        Returns:
            dict: 包含各种技术指标的字典
        """
        try:
            self.indicators.clear()
            
            # 计算10周移动平均线
            ma10_weekly = self.calculate_weekly_ma(data, self.short_period)
            self.indicators['MA10_weekly'] = ma10_weekly
            
            # 计算100周移动平均线  
            ma100_weekly = self.calculate_weekly_ma(data, self.long_period)
            self.indicators['MA100_weekly'] = ma100_weekly
            
            # 计算交叉信号
            if len(ma10_weekly) > 0 and len(ma100_weekly) > 0:
                # 对齐两个序列的索引
                common_index = ma10_weekly.index.intersection(ma100_weekly.index)
                if len(common_index) > 1:
                    ma10_aligned = ma10_weekly.loc[common_index]
                    ma100_aligned = ma100_weekly.loc[common_index]
                    
                    # 计算交叉信号
                    self.indicators['position'] = (ma10_aligned > ma100_aligned).astype(int)
                    self.indicators['crossover'] = self.indicators['position'].diff()
                    
                    # 记录有效的指标数量
                    valid_signals = len(common_index)
                    self.logger.info(f"成功计算指标: MA10={len(ma10_weekly)}, MA100={len(ma100_weekly)}, 有效信号={valid_signals}")
                else:
                    self.logger.warning("移动平均线数据不足，无法计算交叉信号")
            
            return self.indicators
            
        except Exception as e:
            self.logger.error(f"计算技术指标失败: {e}")
            return {}
    
    def generate_signals(self, data):
        """
        生成交易信号
        
        Args:
            data: 股票价格数据
            
        Returns:
            dict: 包含交易信号的字典
        """
        try:
            self.signals.clear()
            
            # 先计算技术指标
            self.calculate_indicators(data)
            
            if 'crossover' not in self.indicators:
                self.logger.warning("无法生成信号：缺少交叉指标")
                return {}
            
            crossover = self.indicators['crossover']
            
            # 生成具体信号
            buy_signals = crossover > 0   # 黄金交叉：10周线上穿100周线
            sell_signals = crossover < 0  # 死叉：10周线下穿100周线
            
            self.signals['buy'] = buy_signals
            self.signals['sell'] = sell_signals
            self.signals['crossover'] = crossover
            
            # 统计信号
            self.stats['golden_cross_count'] = buy_signals.sum()
            self.stats['death_cross_count'] = sell_signals.sum()
            self.stats['total_signals'] = self.stats['golden_cross_count'] + self.stats['death_cross_count']
            
            if len(crossover) > 0:
                self.stats['last_signal_date'] = crossover.index[-1]
            
            self.logger.info(f"信号生成完成: 黄金交叉={self.stats['golden_cross_count']}, 死叉={self.stats['death_cross_count']}")
            
            return self.signals
            
        except Exception as e:
            self.logger.error(f"生成交易信号失败: {e}")
            return {}
    
    def analyze_stock(self, stock_code, start_date='2023-01-01', end_date=None):
        """
        分析单只股票
        
        Args:
            stock_code: 股票代码
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            dict: 分析结果
        """
        try:
            if end_date is None:
                end_date = datetime.now().strftime('%Y-%m-%d')
            
            self.logger.info(f"开始分析股票: {stock_code} ({start_date} to {end_date})")
            
            # 获取股票数据
            data_manager = create_data_manager_safe()
            stock_data = data_manager.get_stock_data(
                stock_code=stock_code,
                start_date=start_date,
                end_date=end_date
            )
            
            if stock_data is None or len(stock_data) == 0:
                self.logger.warning(f"股票 {stock_code} 数据为空")
                return {'status': 'no_data', 'stock_code': stock_code}
            
            # 生成信号
            signals = self.generate_signals(stock_data)
            
            if not signals:
                return {'status': 'no_signals', 'stock_code': stock_code}
            
            # 分析结果
            result = {
                'status': 'success',
                'stock_code': stock_code,
                'analysis_period': f"{start_date} to {end_date}",
                'data_points': len(stock_data),
                'indicators': self.indicators,
                'signals': signals,
                'statistics': self.stats.copy(),
                'current_status': self._get_current_status(),
                'latest_signal': self._get_latest_signal()
            }
            
            return result
            
        except Exception as e:
            self.logger.error(f"分析股票 {stock_code} 失败: {e}")
            return {'status': 'error', 'stock_code': stock_code, 'error': str(e)}
    
    def _get_current_status(self):
        """获取当前状态"""
        if 'position' not in self.indicators or len(self.indicators['position']) == 0:
            return "无数据"
        
        current_pos = self.indicators['position'].iloc[-1]
        if current_pos > 0:
            return "多头 (10周线在100周线之上)"
        else:
            return "空头 (10周线在100周线之下)"
    
    def _get_latest_signal(self):
        """获取最近的交叉信号"""
        if 'crossover' not in self.indicators:
            return "无信号"
        
        crossover = self.indicators['crossover']
        if len(crossover) == 0:
            return "无信号"
        
        # 找到最近的非零信号
        recent_signals = crossover[crossover != 0]
        if len(recent_signals) == 0:
            return "无交叉信号"
        
        latest_signal = recent_signals.iloc[-1]
        latest_date = recent_signals.index[-1]
        
        if latest_signal > 0:
            return f"黄金交叉 ({latest_date.strftime('%Y-%m-%d')})"
        else:
            return f"死叉 ({latest_date.strftime('%Y-%m-%d')})"
    
    def get_strategy_info(self):
        """获取策略信息"""
        return {
            'name': self.name,
            'description': "十周线上穿百周线策略 - 黄金交叉策略",
            'type': 'Technical Analysis',
            'timeframe': 'Weekly',
            'parameters': {
                'short_period': f"{self.short_period}周",
                'long_period': f"{self.long_period}周"
            },
            'signals': {
                'buy': "10周MA上穿100周MA (黄金交叉)",
                'sell': "10周MA下穿100周MA (死叉)"
            },
            '适用市场': ['A股', '港股', '美股'],
            '风险等级': '中等',
            '预期收益': '中长期趋势跟踪'
        }


class StockScreener:
    """股票筛选器 - 基于MA交叉策略"""
    
    def __init__(self, strategy=None):
        """
        初始化筛选器
        
        Args:
            strategy: MA交叉策略实例
        """
        self.strategy = strategy if strategy else MACrossoverStrategy()
        self.logger = get_logger("stock_screener")
        
        # 筛选结果
        self.screening_results = []
        self.qualified_stocks = []
        
    def screen_stocks(self, stock_list=None, start_date='2023-01-01', min_signals=1):
        """
        筛选符合条件的股票
        
        Args:
            stock_list: 股票代码列表 (None则使用默认列表)
            start_date: 分析起始日期
            min_signals: 最少信号数量要求
            
        Returns:
            list: 符合条件的股票列表
        """
        try:
            self.logger.info("开始股票筛选...")
            
            # 如果没有提供股票列表，使用测试股票
            if stock_list is None:
                stock_list = [
                    '000001',  # 平安银行
                    '000002',  # 万科A
                    '000858',  # 五粮液
                    '000001',  # 平安银行
                    '600036',  # 招商银行
                    '600519',  # 贵州茅台
                    '000858',  # 五粮液
                    '002415'   # 海康威视
                ]
                # 去重
                stock_list = list(set(stock_list))
            
            self.logger.info(f"筛选股票数量: {len(stock_list)}")
            
            # 清空之前的结果
            self.screening_results.clear()
            self.qualified_stocks.clear()
            
            # 逐一分析股票
            for i, stock_code in enumerate(stock_list, 1):
                self.logger.info(f"分析进度: {i}/{len(stock_list)} - {stock_code}")
                
                try:
                    # 分析股票
                    result = self.strategy.analyze_stock(
                        stock_code=stock_code,
                        start_date=start_date
                    )
                    
                    result['screening_rank'] = i
                    self.screening_results.append(result)
                    
                    # 检查是否符合筛选条件
                    if self._meets_criteria(result, min_signals):
                        self.qualified_stocks.append(result)
                        self.logger.info(f"✅ {stock_code}: 符合筛选条件")
                    else:
                        self.logger.info(f"❌ {stock_code}: 不符合筛选条件")
                        
                except Exception as e:
                    self.logger.error(f"分析股票 {stock_code} 时出错: {e}")
                    continue
            
            # 排序结果（按最近信号时间排序）
            self.qualified_stocks.sort(
                key=lambda x: x.get('statistics', {}).get('last_signal_date', datetime.min),
                reverse=True
            )
            
            self.logger.info(f"筛选完成: {len(self.qualified_stocks)}/{len(stock_list)} 只股票符合条件")
            
            return self.qualified_stocks
            
        except Exception as e:
            self.logger.error(f"股票筛选失败: {e}")
            return []
    
    def _meets_criteria(self, analysis_result, min_signals):
        """
        检查是否符合筛选条件
        
        Args:
            analysis_result: 股票分析结果
            min_signals: 最少信号数量要求
            
        Returns:
            bool: 是否符合条件
        """
        try:
            if analysis_result['status'] != 'success':
                return False
            
            stats = analysis_result.get('statistics', {})
            
            # 检查信号数量
            total_signals = stats.get('total_signals', 0)
            if total_signals < min_signals:
                return False
            
            # 检查是否有黄金交叉
            golden_cross_count = stats.get('golden_cross_count', 0)
            if golden_cross_count == 0:
                return False
            
            # 检查当前状态（可选：只选择当前多头的股票）
            current_status = analysis_result.get('current_status', '')
            if '多头' not in current_status:
                return False
                
            return True
            
        except Exception as e:
            self.logger.error(f"检查筛选条件时出错: {e}")
            return False
    
    def get_screening_summary(self):
        """获取筛选结果摘要"""
        return {
            'total_analyzed': len(self.screening_results),
            'qualified_count': len(self.qualified_stocks),
            'qualification_rate': f"{len(self.qualified_stocks)/max(1, len(self.screening_results))*100:.1f}%",
            'screening_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'strategy': self.strategy.get_strategy_info()
        }
    
    def export_results(self, filename=None):
        """导出筛选结果"""
        if filename is None:
            filename = f"ma_crossover_screening_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        export_data = {
            'screening_summary': self.get_screening_summary(),
            'qualified_stocks': self.qualified_stocks,
            'all_results': self.screening_results
        }
        
        try:
            import json
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(export_data, f, ensure_ascii=False, indent=2, default=str)
            
            self.logger.info(f"筛选结果已导出: {filename}")
            return filename
            
        except Exception as e:
            self.logger.error(f"导出结果失败: {e}")
            return None


# 主要执行函数
def run_ma_crossover_analysis():
    """运行MA交叉策略分析"""
    
    print("🚀 启动十周线上穿百周线策略分析")
    print("=" * 60)
    
    try:
        # 1. 创建策略实例
        print("📊 创建策略实例...")
        strategy = MACrossoverStrategy(short_period=10, long_period=100)
        
        # 2. 显示策略信息
        strategy_info = strategy.get_strategy_info()
        print(f"✅ 策略: {strategy_info['name']}")
        print(f"   描述: {strategy_info['description']}")
        print(f"   参数: 短期={strategy_info['parameters']['short_period']}, 长期={strategy_info['parameters']['long_period']}")
        
        # 3. 创建筛选器
        print("\n🔍 创建股票筛选器...")
        screener = StockScreener(strategy)
        
        # 4. 运行筛选
        print("\n📈 开始股票筛选...")
        qualified_stocks = screener.screen_stocks(
            start_date='2023-01-01',
            min_signals=1
        )
        
        # 5. 显示结果
        print(f"\n📋 筛选结果:")
        summary = screener.get_screening_summary()
        print(f"   分析股票: {summary['total_analyzed']} 只")
        print(f"   符合条件: {summary['qualified_count']} 只")  
        print(f"   合格率: {summary['qualification_rate']}")
        
        if qualified_stocks:
            print(f"\n✅ 符合条件的股票:")
            for i, stock in enumerate(qualified_stocks[:5], 1):  # 显示前5只
                print(f"   {i}. {stock['stock_code']}: {stock['latest_signal']}")
                print(f"      状态: {stock['current_status']}")
                print(f"      黄金交叉: {stock['statistics']['golden_cross_count']} 次")
        
        # 6. 导出结果
        export_file = screener.export_results()
        if export_file:
            print(f"\n💾 结果已导出: {export_file}")
        
        print(f"\n🎉 分析完成!")
        return qualified_stocks
        
    except Exception as e:
        print(f"❌ 策略分析失败: {e}")
        return []


if __name__ == "__main__":
    # 运行策略分析
    results = run_ma_crossover_analysis()