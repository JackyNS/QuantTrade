#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试缺失API的正确调用方式
"""

import uqer
import inspect

UQER_TOKEN = "68b9922817ae6273137bda7acba81e293582ba347281dfcc056dcb245b23faf3"

def test_api_signatures():
    """测试API签名和调用方式"""
    client = uqer.Client(token=UQER_TOKEN)
    
    test_apis = ["EquMarginSecGet", "MktRANKInstTrGet", "FdmtEeGet"]
    
    for api_name in test_apis:
        print(f"🔍 测试 {api_name}:")
        print("-" * 40)
        
        try:
            api_func = getattr(uqer.DataAPI, api_name)
            
            # 获取函数签名
            try:
                sig = inspect.signature(api_func)
                print(f"函数签名: {api_name}{sig}")
                
                # 获取参数信息
                params = list(sig.parameters.keys())
                print(f"参数列表: {params}")
                
            except Exception as e:
                print(f"无法获取签名: {e}")
            
            # 尝试不同的调用方式
            print("\n测试调用方式:")
            
            # 1. 无参数调用
            try:
                result = api_func()
                print(f"✅ 无参数调用成功: {len(result)} 条记录")
                print(f"   前5行列名: {list(result.columns) if not result.empty else '无数据'}")
                if not result.empty:
                    print(f"   示例数据:")
                    print(result.head(2).to_string())
                continue
            except Exception as e:
                print(f"❌ 无参数调用失败: {e}")
            
            # 2. 尝试常见参数
            common_params = [
                {},
                {"beginDate": "2024-01-01", "endDate": "2024-12-31"},
                {"tradeDate": "2024-01-01"},
                {"reportDate": "2024-01-01"},
                {"publishDate": "2024-01-01"}
            ]
            
            for i, param_set in enumerate(common_params, 1):
                try:
                    if param_set:  # 跳过空字典
                        result = api_func(**param_set)
                        print(f"✅ 参数组合{i}成功: {param_set}")
                        print(f"   返回数据: {len(result)} 条记录")
                        if not result.empty:
                            print(f"   列名: {list(result.columns)}")
                        break
                except Exception as e:
                    if "无效的请求参数" in str(e):
                        print(f"⚠️  参数组合{i}无效: {param_set}")
                    else:
                        print(f"❌ 参数组合{i}失败: {param_set} - {e}")
                
        except Exception as e:
            print(f"❌ {api_name} 测试失败: {e}")
        
        print("\n" + "=" * 60 + "\n")

if __name__ == "__main__":
    test_api_signatures()