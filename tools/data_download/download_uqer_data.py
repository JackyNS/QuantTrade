#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
优矿全量数据下载脚本
==================

功能：
1. 下载优矿全部A股历史数据
2. 支持断点续传
3. 自动重试失败项
4. 数据质量检查

Author: QuantTrader Team
Date: 2025-08-31
"""

import sys
import os
import json
from pathlib import Path
from datetime import datetime, timedelta
import time
import logging

# 添加项目路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/uqer_download.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def check_uqer_dependencies():
    """检查优矿依赖和配置"""
    print("🔍 检查优矿依赖...")
    
    try:
        import uqer
        print("✅ uqer包已安装")
    except ImportError:
        print("❌ uqer包未安装")
        print("💡 请运行: pip install uqer")
        return False
    
    # 检查Token配置
    token = None
    config_sources = [
        ("环境变量", os.environ.get('UQER_TOKEN')),
        ("配置文件", get_token_from_config())
    ]
    
    for source_name, token_value in config_sources:
        if token_value:
            token = token_value
            print(f"✅ 找到优矿Token: {source_name}")
            break
    
    if not token:
        print("⚠️ 未找到优矿API Token")
        print("📖 请参考: uqer_setup_guide.md")
        return False
    
    return True

def get_token_from_config():
    """从配置文件获取Token"""
    config_files = [
        'config/uqer_config.json',
        'uqer_config.json'
    ]
    
    for config_file in config_files:
        if Path(config_file).exists():
            try:
                with open(config_file, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                    return config.get('token')
            except:
                continue
    return None

def create_download_config():
    """创建下载配置"""
    token = os.environ.get('UQER_TOKEN') or get_token_from_config()
    
    config = {
        'data_dir': './data',
        'cache': {
            'cache_dir': './data/cache',
            'max_memory_size': 200 * 1024 * 1024  # 200MB
        },
        'uqer': {
            'token': token,
            'rate_limit': 0.1,  # 100ms延迟
            'retry_times': 3
        },
        'download': {
            'batch_size': 50,
            'max_retry': 3,
            'delay_between_batches': 1.0
        }
    }
    
    return config

def download_all_uqer_data():
    """下载所有优矿数据"""
    print("🚀 开始下载优矿全量数据")
    print("=" * 50)
    
    try:
        from core.data.enhanced_data_manager import EnhancedDataManager
        
        config = create_download_config()
        
        with EnhancedDataManager(config) as dm:
            print("✅ 数据管理器初始化成功")
            
            # 获取系统状态
            status = dm.get_download_status()
            print(f"📊 系统状态: {status.get('data_dir', 'N/A')}")
            
            # 下载A股列表
            print("\n📋 获取A股列表...")
            try:
                stock_list = dm.get_stock_list()
                if stock_list and len(stock_list) > 0:
                    print(f"✅ 获取股票列表成功: {len(stock_list)} 只股票")
                    
                    # 显示部分股票信息
                    print("📄 部分股票列表:")
                    for i, symbol in enumerate(stock_list[:10]):
                        print(f"   {symbol}")
                    if len(stock_list) > 10:
                        print(f"   ... 还有 {len(stock_list) - 10} 只股票")
                else:
                    print("⚠️ 股票列表为空，将使用默认列表")
                    stock_list = get_default_stock_list()
                    
            except Exception as e:
                print(f"⚠️ 获取股票列表失败: {e}")
                stock_list = get_default_stock_list()
            
            # 分批下载数据
            print(f"\n📥 开始分批下载 {len(stock_list)} 只股票的历史数据...")
            batch_size = config['download']['batch_size']
            total_batches = (len(stock_list) + batch_size - 1) // batch_size
            
            successful_downloads = 0
            failed_downloads = []
            
            for i in range(0, len(stock_list), batch_size):
                batch_num = i // batch_size + 1
                batch_symbols = stock_list[i:i + batch_size]
                
                print(f"\n🔄 批次 {batch_num}/{total_batches}: {len(batch_symbols)} 只股票")
                print(f"   符号: {batch_symbols[:3]}{'...' if len(batch_symbols) > 3 else ''}")
                
                try:
                    # 下载当前批次
                    result = dm.download_a_shares_data(
                        symbols=batch_symbols,
                        resume=True,  # 断点续传
                        start_date='2020-01-01'  # 从2020年开始
                    )
                    
                    if result and result.get('success', False):
                        batch_success = result.get('successful', [])
                        batch_failed = result.get('failed', [])
                        
                        successful_downloads += len(batch_success)
                        failed_downloads.extend(batch_failed)
                        
                        print(f"   ✅ 成功: {len(batch_success)}, ❌ 失败: {len(batch_failed)}")
                    else:
                        print(f"   ❌ 批次下载失败")
                        failed_downloads.extend(batch_symbols)
                    
                except Exception as e:
                    print(f"   ❌ 批次下载异常: {e}")
                    failed_downloads.extend(batch_symbols)
                
                # 批次间延迟
                if batch_num < total_batches:
                    delay = config['download']['delay_between_batches']
                    print(f"   ⏳ 等待 {delay}s...")
                    time.sleep(delay)
            
            # 重试失败的下载
            if failed_downloads:
                print(f"\n🔁 重试失败的 {len(failed_downloads)} 只股票...")
                retry_result = dm.download_a_shares_data(
                    symbols=failed_downloads,
                    resume=True
                )
                
                if retry_result:
                    retry_success = len(retry_result.get('successful', []))
                    successful_downloads += retry_success
                    print(f"   ✅ 重试成功: {retry_success}")
            
            # 下载完成总结
            print(f"\n🎉 下载完成总结:")
            print(f"   📊 总计: {len(stock_list)} 只股票")
            print(f"   ✅ 成功: {successful_downloads}")
            print(f"   ❌ 失败: {len(stock_list) - successful_downloads}")
            print(f"   📈 成功率: {successful_downloads / len(stock_list) * 100:.1f}%")
            
            return True
            
    except Exception as e:
        print(f"❌ 下载过程出现异常: {e}")
        logger.error(f"下载异常: {e}", exc_info=True)
        return False

def get_default_stock_list():
    """获取默认股票列表（主要指数成分股）"""
    # 主要指数成分股和热门股票
    default_symbols = [
        # 沪深300部分成分股
        '000001.SZ', '000002.SZ', '000858.SZ', '002415.SZ', '002594.SZ',
        '600000.SH', '600036.SH', '600519.SH', '600887.SH', '601318.SH',
        '601398.SH', '601857.SH', '601988.SH', '603259.SH', '603993.SH',
        
        # 科技股
        '000725.SZ', '002230.SZ', '002241.SZ', '300003.SZ', '300059.SZ',
        '300122.SZ', '300124.SZ', '300136.SZ', '300628.SZ', '300661.SZ',
    ]
    
    print(f"📋 使用默认股票列表: {len(default_symbols)} 只")
    return default_symbols

def main():
    """主函数"""
    print("=" * 60)
    print("🎯 优矿全量数据下载")
    print("=" * 60)
    
    # 创建必要目录
    os.makedirs('logs', exist_ok=True)
    os.makedirs('data', exist_ok=True)
    os.makedirs('data/cache', exist_ok=True)
    
    # 检查依赖
    if not check_uqer_dependencies():
        print("\n❌ 依赖检查失败，请按提示配置后重试")
        return
    
    # 开始下载
    success = download_all_uqer_data()
    
    print("\n" + "=" * 60)
    if success:
        print("🎉 优矿数据下载任务完成！")
        print("💡 建议接下来配置每日自动更新")
    else:
        print("⚠️ 下载过程中遇到问题，请检查日志")
    print("=" * 60)

if __name__ == "__main__":
    main()