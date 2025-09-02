#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
API探测工具 - 检查uqer客户端中可用的API函数
"""

import uqer
import inspect

UQER_TOKEN = "68b9922817ae6273137bda7acba81e293582ba347281dfcc056dcb245b23faf3"

def explore_uqer_apis():
    """探测uqer客户端中的所有可用API"""
    
    client = uqer.Client(token=UQER_TOKEN)
    
    # 获取客户端的所有方法
    methods = [method for method in dir(client) 
               if not method.startswith('_') and callable(getattr(client, method))]
    
    print(f"🔍 uqer.Client 中发现 {len(methods)} 个可用方法:")
    print("=" * 60)
    
    # 寻找可能相关的API
    target_keywords = ["margin", "rank", "perf", "fdmt", "ee"]
    
    found_apis = []
    for method in methods:
        method_lower = method.lower()
        for keyword in target_keywords:
            if keyword in method_lower:
                found_apis.append(method)
                break
    
    if found_apis:
        print("🎯 找到可能相关的API:")
        for api in found_apis:
            print(f"  - {api}")
    else:
        print("❌ 没有找到包含关键词的API")
    
    print("\n" + "=" * 60)
    print("📋 所有可用方法列表:")
    
    # 按字母顺序显示所有方法
    for i, method in enumerate(sorted(methods), 1):
        print(f"{i:3d}. {method}")
    
    return methods, found_apis

def test_specific_apis():
    """测试特定API是否存在并可调用"""
    
    client = uqer.Client(token=UQER_TOKEN)
    
    test_apis = [
        "getEquMarginSec",
        "getMktRANKInstTr", 
        "getFdmtEe",
        "EquMarginSecGet",
        "MktRANKInstTrGet",
        "getFdmtEeAllLatest"
    ]
    
    print("🧪 测试特定API是否存在:")
    print("=" * 40)
    
    for api_name in test_apis:
        if hasattr(client, api_name):
            print(f"✅ {api_name} - 存在")
            
            # 尝试获取函数签名
            try:
                func = getattr(client, api_name)
                sig = inspect.signature(func)
                print(f"   参数: {sig}")
            except Exception as e:
                print(f"   无法获取签名: {e}")
        else:
            print(f"❌ {api_name} - 不存在")

if __name__ == "__main__":
    print("🚀 开始探测uqer API...")
    
    methods, found_apis = explore_uqer_apis()
    
    print("\n" + "=" * 60)
    test_specific_apis()