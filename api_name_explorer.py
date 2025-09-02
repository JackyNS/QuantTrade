#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
API名称探索器 - 寻找正确的API名称
"""

import uqer
import re

def explore_api_names():
    """探索API名称"""
    
    token = "68b9922817ae6273137bda7acba81e293582ba347281dfcc056dcb245b23faf3"
    client = uqer.Client(token=token)
    
    print("🔍 **寻找相关API名称**")
    print("=" * 50)
    
    # 获取所有DataAPI的属性
    api_names = [attr for attr in dir(uqer.DataAPI) if not attr.startswith('_')]
    
    print(f"📊 找到 {len(api_names)} 个API")
    
    # 搜索关键词
    search_terms = {
        "因子": ["factor", "fancy", "equ"],
        "宏观": ["eco", "macro", "china"],
        "地域": ["region", "area", "sec"],
        "板块": ["sec", "type", "sector"],
        "行业": ["industry", "indu"],
        "融资融券": ["fst", "margin"]
    }
    
    for category, keywords in search_terms.items():
        print(f"\n🎯 **{category}相关API**:")
        found_apis = []
        
        for api_name in api_names:
            api_lower = api_name.lower()
            if any(keyword in api_lower for keyword in keywords):
                found_apis.append(api_name)
        
        if found_apis:
            for api in sorted(found_apis):
                print(f"  ✅ {api}")
        else:
            print(f"  ❌ 未找到相关API")
    
    # 特别搜索包含特定关键词的API
    print(f"\n🔍 **详细搜索结果**:")
    
    specific_searches = [
        ("Fancy", "精选因子"),
        ("Eco", "宏观数据"),
        ("Region", "地域"),
        ("SecType", "证券板块"),
        ("Industry", "行业"),
        ("Fst", "融资融券")
    ]
    
    for search_key, description in specific_searches:
        matches = [api for api in api_names if search_key.lower() in api.lower()]
        print(f"\n{description} ({search_key}): {len(matches)} 个API")
        for api in sorted(matches)[:10]:  # 只显示前10个
            print(f"  • {api}")
        if len(matches) > 10:
            print(f"  ... 还有 {len(matches) - 10} 个")

if __name__ == "__main__":
    explore_api_names()