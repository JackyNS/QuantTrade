#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
大规模数据下载监控器
==================

实时监控三个后台下载任务的进度
"""

import pandas as pd
from pathlib import Path
from datetime import datetime
import time
import os

def get_file_info(file_path):
    """获取文件信息"""
    if file_path.exists():
        size_mb = file_path.stat().st_size / (1024*1024)
        mod_time = datetime.fromtimestamp(file_path.stat().st_mtime)
        return size_mb, mod_time
    return 0, None

def monitor_downloads():
    """监控下载进度"""
    
    print("📊 优矿大规模数据下载实时监控")
    print("=" * 60)
    print(f"⏰ 开始监控时间: {datetime.now()}")
    print()
    
    data_path = Path("/Users/jackstudio/QuantTrade/data")
    
    # 监控目录
    directories = {
        "adjustment": "复权因子数据",
        "dividend": "股票分红数据", 
        "limit_info": "涨跌停信息",
        "rank_list": "龙虎榜数据",
        "daily": "日行情数据",
        "capital_flow": "资金流向数据"
    }
    
    print("📁 监控目录状态:")
    for dir_name, desc in directories.items():
        dir_path = data_path / dir_name
        if dir_path.exists():
            files = list(dir_path.glob("*.csv"))
            total_size = sum(f.stat().st_size for f in files) / (1024*1024)
            print(f"   📂 {desc}: {len(files)} 文件, {total_size:.1f}MB")
        else:
            print(f"   📂 {desc}: 目录不存在")
    
    print()
    print("🔄 开始实时监控 (每30秒更新)...")
    print("   按 Ctrl+C 停止监控")
    print()
    
    last_sizes = {}
    
    try:
        while True:
            current_time = datetime.now().strftime("%H:%M:%S")
            print(f"⏰ {current_time} - 下载进度更新:")
            
            total_size = 0
            total_files = 0
            active_downloads = 0
            
            for dir_name, desc in directories.items():
                dir_path = data_path / dir_name
                if dir_path.exists():
                    files = list(dir_path.glob("*.csv"))
                    dir_size = sum(f.stat().st_size for f in files) / (1024*1024)
                    total_size += dir_size
                    total_files += len(files)
                    
                    # 检查是否有新增数据
                    if dir_name in last_sizes:
                        if dir_size > last_sizes[dir_name]:
                            growth = dir_size - last_sizes[dir_name]
                            print(f"   📈 {desc}: {len(files)} 文件, {dir_size:.1f}MB (+{growth:.1f}MB)")
                            active_downloads += 1
                        elif dir_size == last_sizes[dir_name] and dir_size > 0:
                            print(f"   📊 {desc}: {len(files)} 文件, {dir_size:.1f}MB (稳定)")
                        elif dir_size == 0:
                            print(f"   ⏳ {desc}: 等待下载...")
                    else:
                        if dir_size > 0:
                            print(f"   ✅ {desc}: {len(files)} 文件, {dir_size:.1f}MB (新增)")
                            active_downloads += 1
                        else:
                            print(f"   ⏳ {desc}: 等待下载...")
                    
                    last_sizes[dir_name] = dir_size
            
            print(f"   📊 总计: {total_files} 文件, {total_size:.1f}MB")
            print(f"   🔄 活跃下载: {active_downloads} 个任务")
            
            if active_downloads == 0 and total_size > 100:  # 如果有数据但无增长
                print("   🎯 可能所有下载已完成!")
            
            print("-" * 50)
            time.sleep(30)  # 30秒更新一次
            
    except KeyboardInterrupt:
        print("\n⏹️ 监控已停止")
        print(f"📊 最终统计: {total_files} 文件, {total_size:.1f}MB")

if __name__ == "__main__":
    monitor_downloads()