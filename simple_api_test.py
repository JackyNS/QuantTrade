#!/usr/bin/env python3

import uqer

UQER_TOKEN = "68b9922817ae6273137bda7acba81e293582ba347281dfcc056dcb245b23faf3"

client = uqer.Client(token=UQER_TOKEN)

print("客户端类型:", type(client))
print("客户端属性数量:", len(dir(client)))

# 查看前20个属性
attrs = [attr for attr in dir(client) if not attr.startswith('_')]
print(f"前20个属性: {attrs[:20]}")

# 测试已知工作的API
try:
    df = client.EquGet()
    print(f"EquGet 工作正常，返回 {len(df)} 条记录")
except Exception as e:
    print(f"EquGet 失败: {e}")

# 检查是否有DataAPI对象
if hasattr(client, 'DataAPI'):
    data_api = client.DataAPI
    print("DataAPI 存在，类型:", type(data_api))
    
    # 查看DataAPI的属性
    data_attrs = [attr for attr in dir(data_api) if not attr.startswith('_')]
    print(f"DataAPI 属性数量: {len(data_attrs)}")
    
    # 寻找目标API
    for attr in data_attrs:
        if any(keyword in attr.lower() for keyword in ['margin', 'rank', 'fdmt']):
            print(f"找到相关API: {attr}")
else:
    print("没有DataAPI属性")