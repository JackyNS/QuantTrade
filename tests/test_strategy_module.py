#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
策略模块测试脚本 - 验证所有功能是否正常工作
=============================================

测试内容：
1. 技术指标计算
2. K线形态识别
3. 资金流分析
4. 市场情绪分析
5. 信号生成
6. 仓位管理
7. 策略优化
8. 综合策略运行

运行方式：
python test_strategy_module.py
"""

import sys
import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# 添加项目路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# =====================================
# 1. 生成测试数据
# =====================================

def generate_test_data(n_days=100, n_stocks=5):
    """生成测试用的股票数据"""
    print("\n" + "="*60)
    print("📊 生成测试数据...")
    print("="*60)
    
    np.random.seed(42)
    dates = pd.date_range(end=datetime.now(), periods=n_days)
    
    all_data = []
    
    for i in range(n_stocks):
        ticker = f"TEST{i:03d}"
        
        # 生成价格数据（随机游走）
        close = 10 + np.cumsum(np.random.randn(n_days) * 0.5) + np.arange(n_days) * 0.01
        close = np.maximum(close, 1)  # 确保价格为正
        
        # OHLC数据
        high = close * (1 + np.abs(np.random.randn(n_days) * 0.02))
        low = close * (1 - np.abs(np.random.randn(n_days) * 0.02))
        open_price = close + np.random.randn(n_days) * 0.1
        
        # 成交量
        volume = np.random.randint(1000000, 10000000, n_days)
        
        # 资金流数据
        main_net_flow = np.random.randn(n_days) * 1000000
        
        # 创建DataFrame
        stock_data = pd.DataFrame({
            'date': dates,
            'ticker': ticker,
            'open': open_price,
            'high': high,
            'low': low,
            'close': close,
            'volume': volume,
            'turnover': close * volume,
            'pct_change': pd.Series(close).pct_change() * 100,
            'main_net_flow': main_net_flow,
            'name': f'测试股票{i}'
        })
        
        all_data.append(stock_data)
    
    data = pd.concat(all_data, ignore_index=True)
    print(f"✅ 生成 {n_stocks} 只股票，{n_days} 天的数据")
    print(f"   数据形状: {data.shape}")
    print(f"   日期范围: {data['date'].min()} 至 {data['date'].max()}")
    
    return data

# =====================================
# 2. 测试技术指标
# =====================================

def test_technical_indicators(data):
    """测试技术指标计算"""
    print("\n" + "="*60)
    print("📈 测试技术指标模块...")
    print("="*60)
    
    try:
        from core.strategy.technical_indicators import TechnicalIndicators
        
        indicators = TechnicalIndicators()
        
        # 获取一只股票的数据
        test_stock = data[data['ticker'] == 'TEST000'].copy()
        
        print("\n1. 测试单个指标计算：")
        
        # 测试趋势指标
        sma = indicators.sma(test_stock['close'], 20)
        print(f"   ✅ SMA(20): 最新值 = {sma.iloc[-1]:.2f}")
        
        ema = indicators.ema(test_stock['close'], 12)
        print(f"   ✅ EMA(12): 最新值 = {ema.iloc[-1]:.2f}")
        
        # 测试MACD
        macd_result = indicators.macd(test_stock['close'])
        print(f"   ✅ MACD: Signal = {macd_result['signal'].iloc[-1]:.4f}")
        
        # 测试布林带
        bb = indicators.bollinger_bands(test_stock['close'])
        print(f"   ✅ 布林带: Upper = {bb['bb_upper'].iloc[-1]:.2f}, Lower = {bb['bb_lower'].iloc[-1]:.2f}")
        
        # 测试动量指标
        rsi = indicators.rsi(test_stock['close'])
        print(f"   ✅ RSI(14): 最新值 = {rsi.iloc[-1]:.2f}")
        
        # 测试成交量指标
        obv = indicators.obv(test_stock['close'], test_stock['volume'])
        print(f"   ✅ OBV: 最新值 = {obv.iloc[-1]:,.0f}")
        
        print("\n2. 测试批量指标计算：")
        all_indicators = indicators.calculate_all_indicators(test_stock)
        
        new_columns = [col for col in all_indicators.columns if col not in test_stock.columns]
        print(f"   ✅ 成功计算 {len(new_columns)} 个技术指标")
        print(f"   指标列表: {new_columns[:5]}...")
        
        print("\n✅ 技术指标模块测试通过！")
        return True
        
    except Exception as e:
        print(f"\n❌ 技术指标模块测试失败: {str(e)}")
        return False

# =====================================
# 3. 测试K线形态识别
# =====================================

def test_pattern_recognition(data):
    """测试K线形态识别"""
    print("\n" + "="*60)
    print("🕯️ 测试K线形态识别模块...")
    print("="*60)
    
    try:
        from core.strategy.pattern_recognition import PatternRecognizer
        
        recognizer = PatternRecognizer()
        
        # 获取测试数据
        test_stock = data[data['ticker'] == 'TEST000'].copy()
        
        print("\n1. 识别K线形态：")
        pattern_result = recognizer.recognize_all_patterns(test_stock)
        
        # 统计识别到的形态
        pattern_columns = [col for col in pattern_result.columns if col.startswith('pattern_')]
        patterns_found = {}
        
        for col in pattern_columns:
            count = (pattern_result[col] == 1).sum()
            if count > 0:
                patterns_found[col.replace('pattern_', '')] = count
        
        if patterns_found:
            print("   识别到的形态：")
            for pattern, count in patterns_found.items():
                print(f"   ✅ {pattern}: {count} 次")
        else:
            print("   ⚠️ 未识别到明显形态（正常情况，随机数据可能没有标准形态）")
        
        print(f"\n2. 形态强度评分：")
        if 'pattern_strength' in pattern_result.columns:
            avg_strength = pattern_result['pattern_strength'].mean()
            max_strength = pattern_result['pattern_strength'].max()
            print(f"   ✅ 平均强度: {avg_strength:.2f}")
            print(f"   ✅ 最大强度: {max_strength:.2f}")
        
        print("\n✅ K线形态识别模块测试通过！")
        return True
        
    except Exception as e:
        print(f"\n❌ K线形态识别模块测试失败: {str(e)}")
        return False

# =====================================
# 4. 测试资金流分析
# =====================================

def test_capital_flow_analysis(data):
    """测试资金流分析"""
    print("\n" + "="*60)
    print("💰 测试资金流分析模块...")
    print("="*60)
    
    try:
        from core.strategy.capital_flow_analysis import CapitalFlowAnalyzer
        
        analyzer = CapitalFlowAnalyzer()
        
        # 获取测试数据
        test_stock = data[data['ticker'] == 'TEST000'].copy()
        
        print("\n1. 分析资金流向：")
        flow_result = analyzer.analyze_money_flow(test_stock)
        
        if 'main_flow_ratio' in flow_result.columns:
            avg_ratio = flow_result['main_flow_ratio'].mean()
            print(f"   ✅ 平均主力资金流入比例: {avg_ratio:.4f}")
        
        if 'capital_flow_score' in flow_result.columns:
            avg_score = flow_result['capital_flow_score'].mean()
            max_score = flow_result['capital_flow_score'].max()
            min_score = flow_result['capital_flow_score'].min()
            print(f"   ✅ 资金流评分: 平均={avg_score:.1f}, 最高={max_score:.1f}, 最低={min_score:.1f}")
        
        if 'consecutive_inflow_days' in flow_result.columns:
            max_consecutive = flow_result['consecutive_inflow_days'].max()
            print(f"   ✅ 最大连续流入天数: {max_consecutive}")
        
        print("\n2. 检测异常资金流：")
        abnormal = analyzer.detect_abnormal_flow(test_stock)
        abnormal_count = abnormal['abnormal_flow'].sum() if 'abnormal_flow' in abnormal.columns else 0
        print(f"   ✅ 发现异常资金流动: {abnormal_count} 次")
        
        print("\n✅ 资金流分析模块测试通过！")
        return True
        
    except Exception as e:
        print(f"\n❌ 资金流分析模块测试失败: {str(e)}")
        return False

# =====================================
# 5. 测试市场情绪分析
# =====================================

def test_market_sentiment(data):
    """测试市场情绪分析"""
    print("\n" + "="*60)
    print("😨 测试市场情绪分析模块...")
    print("="*60)
    
    try:
        from core.strategy.market_sentiment import MarketSentimentAnalyzer
        
        analyzer = MarketSentimentAnalyzer()
        
        # 获取测试数据
        test_stock = data[data['ticker'] == 'TEST000'].copy()
        
        print("\n1. 分析市场情绪：")
        sentiment_result = analyzer.analyze_market_sentiment(test_stock)
        
        if 'fear_greed_index' in sentiment_result.columns:
            avg_fgi = sentiment_result['fear_greed_index'].mean()
            current_fgi = sentiment_result['fear_greed_index'].iloc[-1]
            print(f"   ✅ 恐惧贪婪指数: 当前={current_fgi:.1f}, 平均={avg_fgi:.1f}")
            
            if current_fgi < 30:
                print(f"      市场状态: 极度恐慌")
            elif current_fgi < 50:
                print(f"      市场状态: 恐慌")
            elif current_fgi < 70:
                print(f"      市场状态: 中性")
            else:
                print(f"      市场状态: 贪婪")
        
        print("\n2. 涨跌停分析：")
        limit_result = analyzer.analyze_limit_up(test_stock)
        
        limit_up_count = limit_result['is_limit_up'].sum() if 'is_limit_up' in limit_result.columns else 0
        limit_down_count = limit_result['is_limit_down'].sum() if 'is_limit_down' in limit_result.columns else 0
        
        print(f"   ✅ 涨停次数: {limit_up_count}")
        print(f"   ✅ 跌停次数: {limit_down_count}")
        
        print("\n✅ 市场情绪分析模块测试通过！")
        return True
        
    except Exception as e:
        print(f"\n❌ 市场情绪分析模块测试失败: {str(e)}")
        return False

# =====================================
# 6. 测试信号生成器
# =====================================

def test_signal_generator(data):
    """测试信号生成器"""
    print("\n" + "="*60)
    print("🎯 测试信号生成器模块...")
    print("="*60)
    
    try:
        from core.strategy.signal_generator import SignalGenerator
        from core.strategy.technical_indicators import TechnicalIndicators
        
        generator = SignalGenerator()
        indicators = TechnicalIndicators()
        
        # 准备数据
        test_stock = data[data['ticker'] == 'TEST000'].copy()
        
        # 计算技术指标
        technical_data = indicators.calculate_all_indicators(test_stock)
        
        print("\n1. 生成交易信号：")
        signals = generator.generate_signals(
            technical_data=technical_data,
            capital_data=None,  # 可选
            sentiment_data=None,  # 可选
            pattern_data=None  # 可选
        )
        
        # 统计信号
        if 'signal' in signals.columns:
            buy_signals = (signals['signal'] > 0).sum()
            sell_signals = (signals['signal'] < 0).sum()
            hold_signals = (signals['signal'] == 0).sum()
            
            print(f"   ✅ 买入信号: {buy_signals} 个")
            print(f"   ✅ 卖出信号: {sell_signals} 个")
            print(f"   ✅ 持有信号: {hold_signals} 个")
        
        print("\n2. 信号质量评估：")
        if 'confidence' in signals.columns:
            avg_confidence = signals[signals['signal'] != 0]['confidence'].mean()
            print(f"   ✅ 平均置信度: {avg_confidence:.1f}%")
        
        if 'signal_strength' in signals.columns:
            avg_strength = signals[signals['signal'] != 0]['signal_strength'].mean()
            print(f"   ✅ 平均信号强度: {avg_strength:.2f}")
        
        print("\n✅ 信号生成器模块测试通过！")
        return True
        
    except Exception as e:
        print(f"\n❌ 信号生成器模块测试失败: {str(e)}")
        return False

# =====================================
# 7. 测试仓位管理器
# =====================================

def test_position_manager():
    """测试仓位管理器"""
    print("\n" + "="*60)
    print("💼 测试仓位管理器模块...")
    print("="*60)
    
    try:
        from core.strategy.position_manager import PositionManager
        
        manager = PositionManager(initial_capital=1000000)
        
        print("\n1. 测试仓位计算：")
        
        # 测试不同方法的仓位计算
        position_info = manager.calculate_position_size(
            ticker='TEST000',
            signal_strength=1.5,
            confidence=0.8,
            price=10.0,
            volatility=0.02,
            atr=0.5
        )
        
        print(f"   ✅ 计算方法: {position_info['method']}")
        print(f"   ✅ 建议仓位: {position_info['size']:.2%}")
        print(f"   ✅ 建议数量: {position_info['quantity']} 股")
        print(f"   ✅ 所需资金: {position_info['value']:,.0f} 元")
        
        print("\n2. 测试开仓：")
        success = manager.open_position(
            ticker='TEST000',
            price=10.0,
            quantity=position_info['quantity'],
            signal_data={'signal_strength': 1.5, 'confidence': 0.8},
            timestamp=datetime.now()
        )
        
        if success:
            print(f"   ✅ 成功开仓")
            print(f"   ✅ 当前持仓数: {len(manager.positions)}")
            print(f"   ✅ 剩余资金: {manager.current_capital:,.0f}")
        
        print("\n3. 测试持仓更新：")
        # 模拟价格变动
        price_data = pd.DataFrame({
            'TEST000': {'close': 11.0}  # 涨10%
        }).T
        
        manager.update_positions(price_data, datetime.now())
        
        if not manager.positions.empty:
            position = manager.positions.iloc[0]
            print(f"   ✅ 当前价格: {position['current_price']:.2f}")
            print(f"   ✅ 浮动盈亏: {position['pnl']:,.0f} ({position['pnl_pct']:.2%})")
            print(f"   ✅ 止损价: {position['stop_loss']:.2f}")
            print(f"   ✅ 止盈价: {position['take_profit']:.2f}")
        
        print("\n4. 测试组合状态：")
        status = manager.get_portfolio_status()
        
        print(f"   ✅ 总资产: {status['capital']['total_value']:,.0f}")
        print(f"   ✅ 持仓价值: {status['positions']['total_value']:,.0f}")
        print(f"   ✅ 仓位占比: {status['positions']['exposure']:.2%}")
        print(f"   ✅ 总收益率: {status['performance']['total_pnl_pct']:.2%}")
        
        print("\n✅ 仓位管理器模块测试通过！")
        return True
        
    except Exception as e:
        print(f"\n❌ 仓位管理器模块测试失败: {str(e)}")
        return False

# =====================================
# 8. 测试策略优化器
# =====================================

def test_strategy_optimizer():
    """测试策略优化器"""
    print("\n" + "="*60)
    print("🔧 测试策略优化器模块...")
    print("="*60)
    
    try:
        from core.strategy.strategy_optimizer import StrategyOptimizer
        from core.strategy.base_strategy import BaseStrategy
        
        # 创建一个简单的测试策略
        class TestStrategy(BaseStrategy):
            def calculate_indicators(self, data):
                return data
            
            def generate_signals(self, data):
                signals = pd.DataFrame(index=data.index)
                signals['signal'] = np.random.choice([-1, 0, 1], len(data))
                return signals
        
        optimizer = StrategyOptimizer(TestStrategy)
        
        print("\n1. 测试参数优化配置：")
        print(f"   ✅ 优化方法: {optimizer.config['optimization']['method']}")
        print(f"   ✅ 优化指标: {optimizer.config['optimization']['metric']}")
        print(f"   ✅ 参数空间: {list(optimizer.config['param_space'].keys())}")
        
        print("\n2. 测试参数评估：")
        test_params = {
            'stop_loss': 0.05,
            'take_profit': 0.20,
            'signal_threshold': 0.6
        }
        
        # 生成测试数据
        test_data = pd.DataFrame({
            'date': pd.date_range(start='2024-01-01', periods=50),
            'close': 100 + np.cumsum(np.random.randn(50)),
            'open': 100 + np.cumsum(np.random.randn(50)),
            'high': 105 + np.cumsum(np.random.randn(50)),
            'low': 95 + np.cumsum(np.random.randn(50)),
            'volume': np.random.randint(1000000, 5000000, 50)
        })
        
        score = optimizer._evaluate_params(test_params, test_data, 1000000)
        print(f"   ✅ 参数评分: {score:.4f}")
        
        print("\n3. 测试优化报告：")
        # 模拟一些优化结果
        optimizer.best_params = test_params
        optimizer.best_score = score
        optimizer.optimization_history['scores'] = [0.1, 0.3, 0.5, score]
        
        report = optimizer.get_optimization_report()
        print(f"   ✅ 最佳参数: {report['best_params']}")
        print(f"   ✅ 最佳得分: {report['best_score']:.4f}")
        print(f"   ✅ 平均得分: {report['average_score']:.4f}")
        print(f"   ✅ 改进率: {report['improvement_rate']:.2%}")
        
        print("\n✅ 策略优化器模块测试通过！")
        return True
        
    except Exception as e:
        print(f"\n❌ 策略优化器模块测试失败: {str(e)}")
        return False

# =====================================
# 9. 综合测试
# =====================================

def test_complete_strategy():
    """测试完整策略流程"""
    print("\n" + "="*60)
    print("🚀 测试完整策略流程...")
    print("="*60)
    
    try:
        # 尝试导入所有模块
        from core.strategy.base_strategy import BaseStrategy
        from core.strategy.technical_indicators import TechnicalIndicators
        from core.strategy.capital_flow_analysis import CapitalFlowAnalyzer
        from core.strategy.market_sentiment import MarketSentimentAnalyzer
        from core.strategy.pattern_recognition import PatternRecognizer
        from core.strategy.signal_generator import SignalGenerator
        from core.strategy.position_manager import PositionManager
        
        print("\n✅ 所有策略模块导入成功！")
        
        # 创建综合策略类
        class CompleteTestStrategy(BaseStrategy):
            def __init__(self):
                super().__init__("CompleteTest")
                self.tech = TechnicalIndicators()
                self.capital = CapitalFlowAnalyzer()
                self.sentiment = MarketSentimentAnalyzer()
                self.pattern = PatternRecognizer()
                self.signal = SignalGenerator()
                self.position = PositionManager()
            
            def calculate_indicators(self, data):
                return self.tech.calculate_all_indicators(data)
            
            def generate_signals(self, data):
                # 简单信号生成
                signals = pd.DataFrame(index=data.index)
                signals['signal'] = 0
                
                # 基于RSI的简单信号
                if 'rsi' in data.columns:
                    signals.loc[data['rsi'] < 30, 'signal'] = 1
                    signals.loc[data['rsi'] > 70, 'signal'] = -1
                
                return signals
        
        # 创建策略实例
        strategy = CompleteTestStrategy()
        
        # 生成测试数据
        test_data = generate_test_data(n_days=50, n_stocks=1)
        stock_data = test_data[test_data['ticker'] == 'TEST000'].copy()
        
        # 运行策略
        print("\n运行完整策略流程：")
        results = strategy.run(stock_data)
        
        print(f"   ✅ 策略运行成功")
        print(f"   ✅ 生成信号数: {len(results['signals'])}")
        print(f"   ✅ 买入信号: {(results['signals']['signal'] > 0).sum()}")
        print(f"   ✅ 卖出信号: {(results['signals']['signal'] < 0).sum()}")
        
        print("\n✅ 完整策略测试通过！")
        return True
        
    except Exception as e:
        print(f"\n❌ 完整策略测试失败: {str(e)}")
        return False

# =====================================
# 主测试函数
# =====================================

def run_all_tests():
    """运行所有测试"""
    print("\n" + "="*80)
    print(" "*20 + "📋 策略模块完整性测试")
    print("="*80)
    print(f"测试时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*80)
    
    # 生成测试数据
    test_data = generate_test_data(n_days=100, n_stocks=5)
    
    # 测试结果记录
    test_results = {}
    
    # 运行各模块测试
    tests = [
        ("技术指标", lambda: test_technical_indicators(test_data)),
        ("K线形态识别", lambda: test_pattern_recognition(test_data)),
        ("资金流分析", lambda: test_capital_flow_analysis(test_data)),
        ("市场情绪分析", lambda: test_market_sentiment(test_data)),
        ("信号生成器", lambda: test_signal_generator(test_data)),
        ("仓位管理器", test_position_manager),
        ("策略优化器", test_strategy_optimizer),
        ("完整策略流程", test_complete_strategy)
    ]
    
    for test_name, test_func in tests:
        try:
            result = test_func()
            test_results[test_name] = "✅ 通过" if result else "❌ 失败"
        except Exception as e:
            test_results[test_name] = f"❌ 错误: {str(e)}"
    
    # 输出测试报告
    print("\n" + "="*80)
    print(" "*25 + "📊 测试报告")
    print("="*80)
    
    passed = 0
    failed = 0
    
    for test_name, result in test_results.items():
        print(f"{test_name:20s}: {result}")
        if "✅" in result:
            passed += 1
        else:
            failed += 1
    
    print("="*80)
    print(f"测试统计: {passed} 通过, {failed} 失败")
    
    if failed == 0:
        print("\n🎉 恭喜！所有策略模块测试通过！")
        print("您的策略系统已经可以正常工作了！")
    else:
        print(f"\n⚠️ 有 {failed} 个模块测试失败，请检查相关代码。")
    
    print("="*80)
    
    return passed, failed

# =====================================
# 快速测试函数（测试单个模块）
# =====================================

def quick_test(module_name="all"):
    """快速测试指定模块"""
    
    test_data = generate_test_data(n_days=50, n_stocks=2)
    
    module_tests = {
        "indicators": lambda: test_technical_indicators(test_data),
        "pattern": lambda: test_pattern_recognition(test_data),
        "capital": lambda: test_capital_flow_analysis(test_data),
        "sentiment": lambda: test_market_sentiment(test_data),
        "signal": lambda: test_signal_generator(test_data),
        "position": test_position_manager,
        "optimizer": test_strategy_optimizer,
        "complete": test_complete_strategy
    }
    
    if module_name == "all":
        run_all_tests()
    elif module_name in module_tests:
        print(f"\n测试 {module_name} 模块...")
        result = module_tests[module_name]()
        if result:
            print(f"\n✅ {module_name} 模块测试通过！")
        else:
            print(f"\n❌ {module_name} 模块测试失败！")
    else:
        print(f"未知模块: {module_name}")
        print(f"可用模块: {list(module_tests.keys())}")

# =====================================
# 程序入口
# =====================================

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="策略模块测试脚本")
    parser.add_argument(
        "--module",
        type=str,
        default="all",
        help="要测试的模块 (all/indicators/pattern/capital/sentiment/signal/position/optimizer/complete)"
    )
    parser.add_argument(
        "--quick",
        action="store_true",
        help="快速测试模式"
    )
    
    args = parser.parse_args()
    
    if args.quick:
        # 快速测试核心功能
        print("\n🚀 快速测试模式")
        quick_test("complete")
    else:
        # 完整测试
        if args.module:
            quick_test(args.module)
        else:
            run_all_tests()