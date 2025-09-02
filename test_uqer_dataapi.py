#!/usr/bin/env python3

import uqer

UQER_TOKEN = "68b9922817ae6273137bda7acba81e293582ba347281dfcc056dcb245b23faf3"

# 初始化客户端
client = uqer.Client(token=UQER_TOKEN)

print("检查 uqer.DataAPI:")
if hasattr(uqer, 'DataAPI'):
    print("✅ uqer.DataAPI 存在")
    
    # 获取所有可用的API方法
    dataapi_methods = [method for method in dir(uqer.DataAPI) 
                      if not method.startswith('_') and callable(getattr(uqer.DataAPI, method))]
    
    print(f"DataAPI 方法数量: {len(dataapi_methods)}")
    
    # 查找与目标关键词相关的API
    target_keywords = ["margin", "rank", "fdmt", "ee", "perf"]
    related_apis = []
    
    for method in dataapi_methods:
        method_lower = method.lower()
        for keyword in target_keywords:
            if keyword in method_lower:
                related_apis.append(method)
                break
    
    if related_apis:
        print(f"\n🎯 找到相关的API ({len(related_apis)}个):")
        for api in related_apis:
            print(f"  - {api}")
    
    # 测试已知可工作的API
    print("\n🧪 测试已知API:")
    try:
        result = uqer.DataAPI.EquGet()
        print(f"✅ uqer.DataAPI.EquGet(): {len(result)} 条记录")
    except Exception as e:
        print(f"❌ uqer.DataAPI.EquGet() 失败: {e}")
    
    # 搜索可能的API名称
    possible_names = [
        'EquMarginSecGet', 'getEquMarginSec', 'EquMarginSec', 'MarginSecGet',
        'MktRANKInstTrGet', 'getMktRANKInstTr', 'MktRANKInstTr', 'RANKInstTrGet',
        'FdmtEeGet', 'getFdmtEe', 'FdmtEe', 'PerfReportGet'
    ]
    
    print("\n🔍 搜索可能的API名称:")
    found_apis = []
    for name in possible_names:
        if hasattr(uqer.DataAPI, name):
            found_apis.append(name)
            print(f"✅ 找到: {name}")
    
    if not found_apis:
        print("❌ 没有找到目标API")
    
    # 显示部分API列表
    print(f"\n📋 DataAPI 方法列表 (前50个):")
    for i, method in enumerate(sorted(dataapi_methods)[:50], 1):
        print(f"{i:3d}. {method}")
    
    if len(dataapi_methods) > 50:
        print(f"... 还有 {len(dataapi_methods) - 50} 个方法")
        
else:
    print("❌ uqer.DataAPI 不存在")