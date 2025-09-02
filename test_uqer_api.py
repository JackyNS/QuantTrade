#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试UQER API调用
"""

import pandas as pd
import warnings
warnings.filterwarnings('ignore')

try:
    import uqer
    print("✅ UQER API 可用")
except ImportError:
    print("❌ UQER API 不可用")
    exit(1)

def test_uqer_api():
    """测试UQER API各种调用方式"""
    print("🔧 测试UQER API调用方式...")
    
    try:
        # 连接UQER
        uqer_token = "68b9922817ae6273137bda7acba81e293582ba347281dfcc056dcb245b23faf3"
        uqer.Client(token=uqer_token)
        print("✅ UQER连接成功")
        
        # 测试1: 获取股票列表
        print("\n📋 测试1: 获取股票列表")
        try:
            stocks = uqer.DataAPI.EquGet(listStatusCD='L', pandas=1)
            print(f"   结果类型: {type(stocks)}")
            if isinstance(stocks, str):
                # 转换CSV字符串为DataFrame
                from io import StringIO
                stocks_df = pd.read_csv(StringIO(stocks))
                print(f"   转换后获取到 {len(stocks_df)} 条记录")
                print(f"   列名: {list(stocks_df.columns)}")
                print(f"   前3行:")
                print(stocks_df.head(3))
            elif isinstance(stocks, pd.DataFrame):
                print(f"   获取到 {len(stocks)} 条记录")
                print(f"   列名: {list(stocks.columns)}")
                print(f"   前3行:")
                print(stocks.head(3))
            else:
                print(f"   返回值类型未知: {type(stocks)}")
        except Exception as e:
            print(f"   ❌ 失败: {e}")
        
        # 测试2: 获取单只股票数据
        print("\n📈 测试2: 获取平安银行数据")
        try:
            data = uqer.DataAPI.MktEqudGet(
                secID='000001.XSHE',
                beginDate='20240801',
                endDate='20240831', 
                pandas=1
            )
            print(f"   结果类型: {type(data)}")
            if isinstance(data, pd.DataFrame):
                print(f"   获取到 {len(data)} 条记录")
                if len(data) > 0:
                    print(f"   列名: {list(data.columns)}")
                    print(f"   前3行:")
                    print(data.head(3))
            else:
                print(f"   返回值: {data}")
        except Exception as e:
            print(f"   ❌ 失败: {e}")
            
        # 测试3: 不同的参数格式
        print("\n🔄 测试3: 不同参数格式")
        test_codes = ['000001.XSHE', '600519.XSHG', '000002.XSHE']
        
        for stock_code in test_codes:
            print(f"   测试 {stock_code}:")
            try:
                # 尝试最简单的调用
                result = uqer.DataAPI.MktEqudGet(
                    secID=stock_code,
                    beginDate='20240801',
                    endDate='20240901',
                    pandas=1
                )
                
                if result is not None and len(result) > 0:
                    print(f"      ✅ 成功: {len(result)} 条记录")
                    if 'closePrice' in result.columns:
                        latest_price = result['closePrice'].iloc[-1]
                        print(f"      💰 最新价格: {latest_price}")
                else:
                    print(f"      ❌ 无数据")
                    
            except Exception as e:
                print(f"      ❌ 异常: {e}")
                
    except Exception as e:
        print(f"❌ 连接失败: {e}")

if __name__ == "__main__":
    test_uqer_api()