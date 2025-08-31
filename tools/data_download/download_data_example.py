#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据下载使用示例
==============

演示如何使用新的统一数据架构下载和更新各类数据

使用方法:
python download_data_example.py
"""

from core.data.enhanced_data_manager import EnhancedDataManager
import logging

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    """主函数 - 演示各种数据下载方式"""
    
    # 配置数据管理器
    config = {
        'data_dir': './data',
        'cache': {
            'cache_dir': './data/cache',
            'max_memory_size': 100 * 1024 * 1024  # 100MB
        },
        # 如果您有API密钥，请在这里添加
        # 'uqer': {'token': 'your_uqer_token'},
        # 'tushare': {'token': 'your_tushare_token'}
    }
    
    print("🚀 启动统一数据管理器")
    print("=" * 50)
    
    try:
        with EnhancedDataManager(config) as dm:
            print("✅ 数据管理器初始化成功")
            
            # 1. 获取系统状态
            print("\n📊 获取系统状态...")
            status = dm.get_download_status()
            print(f"   数据目录: {status.get('data_dir', 'N/A')}")
            print(f"   缓存状态: {status.get('cache', {}).get('status', 'N/A')}")
            
            # 2. 下载A股数据示例
            print("\n📈 下载A股数据示例...")
            try:
                # 下载几只热门股票的数据
                symbols = ['000001.SZ', '000002.SZ', '600000.SH']  # 平安银行、万科A、浦发银行
                
                result = dm.download_a_shares_data(
                    symbols=symbols[:2],  # 先下载前2只测试
                    resume=True
                )
                print(f"   A股数据下载结果: {result}")
                
            except Exception as e:
                print(f"   ⚠️ A股数据下载需要配置数据源: {e}")
            
            # 3. 下载策略数据示例  
            print("\n💰 下载策略数据示例...")
            try:
                strategy_result = dm.download_strategy_data(
                    symbols=['000001.SZ'],
                    data_types=['capital_flow']  # 资金流向数据
                )
                print(f"   策略数据下载结果: {strategy_result}")
                
            except Exception as e:
                print(f"   ⚠️ 策略数据下载需要配置数据源: {e}")
            
            # 4. 计算技术指标示例
            print("\n📊 计算技术指标示例...")
            try:
                indicator_result = dm.download_indicators_data(
                    symbols=['000001.SZ'],
                    indicators=['SMA', 'RSI']  # 简单移动平均线、RSI指标
                )
                print(f"   技术指标计算结果: {indicator_result}")
                
            except Exception as e:
                print(f"   ⚠️ 技术指标计算需要价格数据: {e}")
            
            # 5. 获取已有数据
            print("\n📂 获取已有数据...")
            try:
                # 获取股票列表
                stock_list = dm.get_stock_list()
                if stock_list is not None:
                    print(f"   股票列表: {len(stock_list)} 只股票")
                else:
                    print("   股票列表: 暂无数据")
                
            except Exception as e:
                print(f"   获取股票列表失败: {e}")
            
            print("\n🎉 示例完成!")
            
    except Exception as e:
        print(f"❌ 数据管理器初始化失败: {e}")
        print("💡 请检查依赖是否安装: pip install pandas numpy")

def quick_start_guide():
    """快速开始指南"""
    print("""
🚀 快速开始指南
=============

1. 安装依赖:
   pip install pandas numpy scipy

2. 配置API密钥 (可选):
   - 优矿 (uqer): 注册获取token
   - Tushare: 注册获取token
   - Yahoo Finance: 免费使用

3. 基础使用:
   ```python
   from core.data.enhanced_data_manager import EnhancedDataManager
   
   config = {'data_dir': './data'}
   with EnhancedDataManager(config) as dm:
       # 下载数据
       dm.download_a_shares_data(['000001.SZ'])
   ```

4. 高级功能:
   - 智能缓存: 自动缓存下载的数据
   - 质量检查: 自动检查数据完整性
   - 断点续传: 支持大批量数据下载中断恢复
   - 多数据源: 自动切换数据源保证稳定性

📖 详细文档: core/data/UNIFIED_DATA_USAGE.md
""")

if __name__ == "__main__":
    print("=" * 60)
    print("🎯 统一数据架构 - 下载示例")
    print("=" * 60)
    
    # 显示快速指南
    quick_start_guide()
    
    # 运行示例
    main()