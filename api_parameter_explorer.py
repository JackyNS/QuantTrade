#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
API参数探索器 - 探索缺失API的正确参数
"""

import uqer
import inspect

def explore_api_parameters():
    """探索API参数"""
    
    token = "68b9922817ae6273137bda7acba81e293582ba347281dfcc056dcb245b23faf3"
    client = uqer.Client(token=token)
    
    apis_to_explore = ["MktRANKInstTrGet", "EquMarginSecGet"]
    
    for api_name in apis_to_explore:
        print(f"\n{'='*50}")
        print(f"🔍 探索API: {api_name}")
        print('='*50)
        
        if hasattr(uqer.DataAPI, api_name):
            api_func = getattr(uqer.DataAPI, api_name)
            
            # 获取函数签名
            try:
                sig = inspect.signature(api_func)
                print(f"📝 函数签名: {sig}")
                
                for param_name, param in sig.parameters.items():
                    print(f"  参数: {param_name}")
                    print(f"    默认值: {param.default}")
                    print(f"    注释: {param.annotation}")
                
            except Exception as e:
                print(f"❌ 无法获取函数签名: {e}")
            
            # 尝试获取帮助文档
            try:
                print(f"\n📖 帮助文档:")
                help_info = help(api_func)
                print(help_info)
            except Exception as e:
                print(f"❌ 无法获取帮助文档: {e}")
            
            # 尝试调用API获取字段信息
            print(f"\n🧪 尝试调用API获取字段信息...")
            try:
                if api_name == "MktRANKInstTrGet":
                    # 尝试不同的参数组合
                    test_params = [
                        {"beginDate": "20231231"},
                        {"tradeDate": "20231231"},  
                        {"endDate": "20231231"},
                        {"beginDate": "20230101", "endDate": "20231231"},
                        {},
                    ]
                    
                    for params in test_params:
                        try:
                            print(f"   尝试参数: {params}")
                            result = api_func(**params)
                            if hasattr(result, 'getData'):
                                df = result.getData()
                            else:
                                df = result
                            
                            if df is not None and not df.empty:
                                print(f"   ✅ 成功! 数据形状: {df.shape}")
                                print(f"   列名: {list(df.columns)}")
                                break
                        except Exception as e:
                            print(f"   ❌ 失败: {e}")
                            
                elif api_name == "EquMarginSecGet":
                    # 尝试不同的参数组合
                    test_params = [
                        {"beginDate": "20231231"},
                        {"publishDate": "20231231"},
                        {"tradeDate": "20231231"},
                        {"beginDate": "20230101", "endDate": "20231231"},
                        {"beginDate": "20231231", "endDate": "20231231"},
                    ]
                    
                    for params in test_params:
                        try:
                            print(f"   尝试参数: {params}")
                            result = api_func(**params)
                            if hasattr(result, 'getData'):
                                df = result.getData()
                            else:
                                df = result
                            
                            if df is not None and not df.empty:
                                print(f"   ✅ 成功! 数据形状: {df.shape}")
                                print(f"   列名: {list(df.columns)}")
                                break
                        except Exception as e:
                            print(f"   ❌ 失败: {e}")
                            
            except Exception as e:
                print(f"❌ API调用探索失败: {e}")
        else:
            print(f"❌ API不存在: {api_name}")

if __name__ == "__main__":
    explore_api_parameters()