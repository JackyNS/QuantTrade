#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
简单的优矿连接测试
"""
import uqer
from datetime import datetime, timedelta

# 设置优矿Token
token = "68b9922817ae6273137bda7acba81e293582ba347281dfcc056dcb245b23faf3"

def test_basic_connection():
    """测试基础连接"""
    print("🔍 测试优矿基础连接...")
    
    try:
        # 初始化客户端
        client = uqer.Client(token=token)
        print("✅ 优矿客户端初始化成功")
        print(f"   账号信息: {client}")
        return client
        
    except Exception as e:
        print(f"❌ 优矿连接失败: {e}")
        return None

def test_stock_data(client):
    """测试股票数据获取"""
    print("\n📊 测试股票数据获取...")
    
    try:
        # 获取最近的股票数据
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
        
        # 使用正确的API调用方式
        data = uqer.DataAPI.MktEqudGet(
            secID='',
            ticker='000001.XSHE,600000.XSHG',  # 测试2只股票
            beginDate=yesterday,
            endDate=yesterday,
            field='secID,ticker,tradeDate,closePrice,turnoverVol'
        )
        
        if data is not None and not data.empty:
            print(f"✅ 股票数据获取成功! 获得 {len(data)} 条记录")
            print("📄 数据样本:")
            print(data.head().to_string(index=False))
            return True
        else:
            print("⚠️ 获取到空数据")
            return False
        
    except Exception as e:
        print(f"❌ 股票数据获取失败: {e}")
        return False

def test_stock_list(client):
    """测试获取股票列表"""
    print("\n📋 测试获取股票列表...")
    
    try:
        # 获取股票基础信息
        stocks = uqer.DataAPI.EquGet(
            field='secID,ticker,secShortName,exchangeCD,listStatusCD'
        )
        
        if stocks is not None and not stocks.empty:
            # 过滤A股
            a_stocks = stocks[stocks['listStatusCD'] == 'L']
            print(f"✅ 股票列表获取成功! 共 {len(a_stocks)} 只A股")
            
            # 按交易所分类
            sh_stocks = a_stocks[a_stocks['exchangeCD'] == 'XSHG']
            sz_stocks = a_stocks[a_stocks['exchangeCD'] == 'XSHE']
            
            print(f"   📈 沪市股票: {len(sh_stocks)} 只")
            print(f"   📊 深市股票: {len(sz_stocks)} 只")
            
            print("\n📄 股票样本:")
            print(a_stocks.head(10)[['ticker', 'secShortName', 'exchangeCD']].to_string(index=False))
            
            return len(a_stocks)
        else:
            print("⚠️ 获取到空的股票列表")
            return 0
        
    except Exception as e:
        print(f"❌ 股票列表获取失败: {e}")
        return 0

def test_api_quota():
    """测试API配额"""
    print("\n💎 测试API配额...")
    
    try:
        # 获取用户配额信息
        quota = uqer.DataAPI.SysConfigGet()
        
        if quota is not None:
            print("✅ 配额信息获取成功!")
            print(f"   配额详情: {quota}")
            return True
        else:
            print("⚠️ 无法获取配额信息")
            return False
        
    except Exception as e:
        print(f"❌ 配额查询失败: {e}")
        return False

def main():
    """主测试函数"""
    print("=" * 60)
    print("🧪 优矿API简单连接测试")
    print("=" * 60)
    
    # 1. 测试基础连接
    client = test_basic_connection()
    if not client:
        print("\n❌ 基础连接失败，无法继续测试")
        return
    
    # 2. 测试股票数据获取
    data_success = test_stock_data(client)
    
    # 3. 测试股票列表
    stock_count = test_stock_list(client)
    
    # 4. 测试配额
    quota_success = test_api_quota()
    
    # 总结
    print("\n" + "=" * 60)
    print("📊 测试结果总结:")
    print(f"   连接状态: ✅ 成功")
    print(f"   数据获取: {'✅ 成功' if data_success else '❌ 失败'}")
    print(f"   股票列表: {'✅ 成功' if stock_count > 0 else '❌ 失败'} ({stock_count}只)")
    print(f"   配额查询: {'✅ 成功' if quota_success else '❌ 失败'}")
    
    if data_success and stock_count > 0:
        print("\n🎉 优矿API连接完全正常！可以开始大规模数据下载")
        print("\n📋 建议的下一步:")
        print("   1. 制定全面的数据下载计划")
        print("   2. 从2000年开始的历史数据下载")
        print("   3. 配置定时更新任务")
    else:
        print("\n⚠️ 部分功能存在问题，需要进一步调试")

if __name__ == "__main__":
    main()