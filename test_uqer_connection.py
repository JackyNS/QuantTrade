#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
优矿连接测试脚本
==============

测试优矿API连接和基础功能
"""

import os
import sys
import json
from pathlib import Path

# 添加项目路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def test_uqer_import():
    """测试uqer包导入"""
    print("🔍 测试uqer包导入...")
    try:
        import uqer
        print(f"✅ uqer包导入成功，版本: {getattr(uqer, '__version__', '未知')}")
        return True
    except ImportError as e:
        print(f"❌ uqer包导入失败: {e}")
        return False

def get_uqer_token():
    """获取优矿Token"""
    # 从环境变量获取
    token = os.environ.get('UQER_TOKEN')
    if token:
        return token
    
    # 从配置文件获取
    config_files = ['config/uqer_config.json', 'uqer_config.json']
    for config_file in config_files:
        if Path(config_file).exists():
            try:
                with open(config_file, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                    token = config.get('token')
                    if token:
                        return token
            except:
                continue
    
    return None

def test_uqer_connection():
    """测试优矿API连接"""
    print("\n🔗 测试优矿API连接...")
    
    token = get_uqer_token()
    if not token:
        print("❌ 未找到优矿API Token")
        print("💡 配置方法：")
        print("   1. 设置环境变量: export UQER_TOKEN='your_token'")
        print("   2. 创建配置文件: uqer_config.json")
        print("   3. 注册地址: https://uqer.datayes.com/")
        return False
    
    try:
        import uqer
        
        # 设置Token
        client = uqer.Client(token=token)
        print("✅ 优矿客户端初始化成功")
        
        # 测试基础API调用
        print("📊 测试股票列表获取...")
        try:
            # 获取股票列表
            stock_list = client.getMktEqud(
                tradeDate='',
                field='ticker,secShortName',
                beginDate='2024-12-01',
                endDate='2024-12-01'
            )
            
            if not stock_list.empty:
                print(f"✅ 股票列表获取成功: {len(stock_list)} 只股票")
                print("📄 部分股票信息:")
                for i in range(min(5, len(stock_list))):
                    row = stock_list.iloc[i]
                    print(f"   {row['ticker']} - {row['secShortName']}")
                return True
            else:
                print("⚠️ 获取到空的股票列表")
                return False
                
        except Exception as e:
            print(f"❌ API调用失败: {e}")
            return False
        
    except Exception as e:
        print(f"❌ 优矿连接测试失败: {e}")
        return False

def test_data_download():
    """测试数据下载"""
    print("\n📥 测试数据下载...")
    
    try:
        from core.data.adapters.uqer_adapter import UqerAdapter
        
        token = get_uqer_token()
        config = {'token': token}
        
        adapter = UqerAdapter(config)
        print("✅ 优矿适配器初始化成功")
        
        # 测试连接
        if adapter.connect():
            print("✅ 优矿适配器连接成功")
            
            # 测试获取股票列表
            try:
                stock_list = adapter.get_stock_list()
                if stock_list:
                    print(f"✅ 获取股票列表成功: {len(stock_list)} 只")
                else:
                    print("⚠️ 股票列表为空")
                
                return True
                
            except Exception as e:
                print(f"❌ 获取股票列表失败: {e}")
                return False
        else:
            print("❌ 优矿适配器连接失败")
            return False
            
    except Exception as e:
        print(f"❌ 数据下载测试失败: {e}")
        return False

def test_enhanced_manager():
    """测试增强数据管理器"""
    print("\n🚀 测试增强数据管理器...")
    
    try:
        from core.data.enhanced_data_manager import EnhancedDataManager
        
        token = get_uqer_token()
        config = {
            'data_dir': './data',
            'uqer': {'token': token}
        }
        
        dm = EnhancedDataManager(config)
        print("✅ 增强数据管理器初始化成功")
        
        # 测试获取状态
        status = dm.get_download_status()
        print(f"📊 系统状态获取成功: {type(status)}")
        
        # 测试小批量下载
        print("📥 测试小批量数据下载...")
        test_symbols = ['000001.SZ', '000002.SZ']  # 测试2只股票
        
        try:
            result = dm.download_a_shares_data(
                symbols=test_symbols,
                start_date='2024-12-01',
                end_date='2024-12-03'  # 小范围日期
            )
            
            if result:
                print(f"✅ 小批量下载测试成功: {result}")
                return True
            else:
                print("⚠️ 下载结果为空")
                return False
                
        except Exception as e:
            print(f"❌ 小批量下载失败: {e}")
            return False
        
    except Exception as e:
        print(f"❌ 增强数据管理器测试失败: {e}")
        return False

def show_next_steps():
    """显示后续步骤"""
    print("""
🎯 后续步骤
==========

✅ 优矿连接测试通过后，您可以：

1. 📥 全量数据下载:
   python download_uqer_data.py

2. 🔄 每日更新测试:
   python daily_update_uqer.py

3. ⚙️ 配置定时任务:
   python setup_scheduler.py

4. 📊 监控和日志:
   - 查看 logs/ 目录的日志文件
   - 查看 reports/ 目录的更新报告

5. 📖 详细配置:
   - 查看 uqer_setup_guide.md

💡 重要提醒:
- 确保优矿API Token有效且有足够调用次数
- 首次全量下载可能需要较长时间
- 建议在非交易时间进行大批量下载
""")

def main():
    """主函数"""
    print("=" * 60)
    print("🧪 优矿API连接和功能测试")
    print("=" * 60)
    
    success_count = 0
    total_tests = 4
    
    # 测试1: uqer包导入
    if test_uqer_import():
        success_count += 1
    
    # 测试2: API连接
    if test_uqer_connection():
        success_count += 1
    
    # 测试3: 数据下载
    if test_data_download():
        success_count += 1
    
    # 测试4: 增强管理器
    if test_enhanced_manager():
        success_count += 1
    
    print("\n" + "=" * 60)
    print(f"📊 测试结果: {success_count}/{total_tests} 项测试通过")
    
    if success_count >= 3:
        print("🎉 优矿功能测试基本通过，可以开始使用！")
        show_next_steps()
    elif success_count >= 1:
        print("⚠️ 部分功能可用，建议检查配置")
    else:
        print("❌ 测试未通过，请检查配置和网络")
    
    print("=" * 60)
    
    return success_count >= 3

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)