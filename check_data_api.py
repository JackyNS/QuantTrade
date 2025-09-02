#!/usr/bin/env python3

import uqer

UQER_TOKEN = "68b9922817ae6273137bda7acba81e293582ba347281dfcc056dcb245b23faf3"

client = uqer.Client(token=UQER_TOKEN)

print("检查client.data:")
if hasattr(client, 'data'):
    data_obj = client.data
    print(f"data对象类型: {type(data_obj)}")
    
    data_attrs = [attr for attr in dir(data_obj) if not attr.startswith('_')]
    print(f"data对象属性: {data_attrs}")
    
    # 测试正确的API调用方式
    try:
        # 根据原始下载器的成功案例，应该是这样调用的
        df = client.data.EquGet()
        print(f"✅ client.data.EquGet() 成功: {len(df)} 条记录")
    except Exception as e:
        print(f"❌ client.data.EquGet() 失败: {e}")
    
    # 寻找缺失的API
    target_apis = [
        'EquMarginSecGet', 'getEquMarginSec', 'EquMarginSec',
        'MktRANKInstTrGet', 'getMktRANKInstTr', 'MktRANKInstTr',
        'FdmtEeGet', 'getFdmtEe', 'FdmtEe'
    ]
    
    print("\n检查目标API:")
    for api in target_apis:
        if hasattr(data_obj, api):
            print(f"✅ 找到: client.data.{api}")
            
            # 尝试调用
            try:
                func = getattr(data_obj, api)
                result = func()
                print(f"   成功调用，返回 {len(result)} 条记录")
            except Exception as e:
                print(f"   调用失败: {e}")
        else:
            print(f"❌ 未找到: {api}")
    
    print(f"\n所有data属性 ({len(data_attrs)}):")
    for i, attr in enumerate(data_attrs, 1):
        if 'margin' in attr.lower() or 'rank' in attr.lower() or 'fdmt' in attr.lower() or 'ee' in attr.lower():
            print(f"{i:3d}. {attr} ⭐")
        else:
            print(f"{i:3d}. {attr}")
            
else:
    print("❌ client.data 不存在")