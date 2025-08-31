#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
下载进度监控脚本
"""

import os
import pandas as pd
from pathlib import Path
from datetime import datetime

def monitor_progress():
    """监控所有下载器进度"""
    print("📊 优矿数据下载进度监控")
    print("=" * 60)
    
    # 1. 优先级下载器进度
    priority_dir = Path("data/priority_download")
    if priority_dir.exists():
        csv_files = list(priority_dir.rglob("*.csv"))
        total_size = sum(f.stat().st_size for f in csv_files) / (1024*1024)  # MB
        
        print(f"🎯 优先级下载器 (7个核心接口):")
        print(f"   📁 文件数量: {len(csv_files)} 个")
        print(f"   💾 数据大小: {total_size:.1f} MB")
        
        # 按年份统计
        yearly_files = {}
        for file in csv_files:
            if "2022" in file.name:
                yearly_files["2022"] = yearly_files.get("2022", 0) + 1
            elif "2023" in file.name:
                yearly_files["2023"] = yearly_files.get("2023", 0) + 1
            elif "2024" in file.name:
                yearly_files["2024"] = yearly_files.get("2024", 0) + 1
        
        for year, count in yearly_files.items():
            print(f"   📅 {year}年: {count} 个文件")
    
    # 2. 智能下载器进度
    smart_dir = Path("data/smart_download")
    if smart_dir.exists():
        csv_files = list(smart_dir.rglob("*.csv"))
        total_size = sum(f.stat().st_size for f in csv_files) / (1024*1024)
        
        print(f"\n🧠 智能下载器 (日行情数据):")
        print(f"   📁 文件数量: {len(csv_files)} 个")
        print(f"   💾 数据大小: {total_size:.1f} MB")
    
    # 3. 原始下载器进度
    original_dir = Path("data/historical_download")
    if original_dir.exists():
        csv_files = list(original_dir.rglob("*.csv"))
        total_size = sum(f.stat().st_size for f in csv_files) / (1024*1024)
        
        print(f"\n📜 原始下载器 (2000-2002年):")
        print(f"   📁 文件数量: {len(csv_files)} 个") 
        print(f"   💾 数据大小: {total_size:.1f} MB")
    
    # 4. 总体统计
    all_dirs = ["data/priority_download", "data/smart_download", "data/historical_download"]
    total_files = 0
    total_size = 0
    
    for dir_path in all_dirs:
        path = Path(dir_path)
        if path.exists():
            files = list(path.rglob("*.csv"))
            total_files += len(files)
            total_size += sum(f.stat().st_size for f in files)
    
    total_size_mb = total_size / (1024*1024)
    total_size_gb = total_size / (1024*1024*1024)
    
    print(f"\n🎉 总体统计:")
    print(f"   📁 总文件数: {total_files:,} 个")
    print(f"   💾 总数据量: {total_size_mb:.1f} MB ({total_size_gb:.2f} GB)")
    
    # 5. 实时状态
    print(f"\n⏰ 更新时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

def show_latest_progress():
    """显示最新进度"""
    priority_log = Path("data/priority_download/priority_download.log")
    if priority_log.exists():
        print("\n📋 最新下载日志:")
        os.system(f"tail -10 {priority_log}")

if __name__ == "__main__":
    monitor_progress()
    show_latest_progress()