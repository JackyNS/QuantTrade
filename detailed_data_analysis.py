#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
详细数据分析工具
==============

深入分析本地数据的实际覆盖情况和质量
"""

import os
import sys
from pathlib import Path
from datetime import datetime
import pandas as pd
import numpy as np
from collections import defaultdict, Counter
import json

def analyze_all_data_files():
    """分析所有数据文件"""
    print("🔍 详细分析本地数据覆盖情况...")
    print("=" * 60)
    
    data_dir = Path('./data')
    if not data_dir.exists():
        print("❌ data目录不存在")
        return {}
    
    analysis_results = {
        'directories': {},
        'total_symbols': set(),
        'file_count_by_type': defaultdict(int),
        'size_statistics': {},
        'date_coverage': {},
        'data_quality': {}
    }
    
    # 遍历所有子目录
    for sub_dir in data_dir.iterdir():
        if sub_dir.is_dir():
            print(f"\n📂 分析目录: {sub_dir.name}")
            dir_analysis = analyze_directory_detailed(sub_dir)
            analysis_results['directories'][sub_dir.name] = dir_analysis
            analysis_results['total_symbols'].update(dir_analysis['unique_symbols'])
    
    # 转换set为list以便序列化
    analysis_results['total_symbols'] = sorted(list(analysis_results['total_symbols']))
    
    return analysis_results

def analyze_directory_detailed(directory):
    """详细分析单个目录"""
    dir_path = Path(directory)
    
    # 递归查找所有数据文件
    csv_files = list(dir_path.rglob('*.csv'))
    parquet_files = list(dir_path.rglob('*.parquet'))
    all_files = csv_files + parquet_files
    
    print(f"   📊 找到 {len(all_files)} 个数据文件")
    
    if not all_files:
        return {
            'file_count': 0,
            'unique_symbols': set(),
            'file_sizes': [],
            'subdirectories': {},
            'date_ranges': {},
            'data_samples': []
        }
    
    # 按子目录分组
    files_by_subdir = defaultdict(list)
    for file_path in all_files:
        relative_path = file_path.relative_to(dir_path)
        if len(relative_path.parts) > 1:
            subdir = relative_path.parts[0]
        else:
            subdir = '.'
        files_by_subdir[subdir].append(file_path)
    
    dir_analysis = {
        'file_count': len(all_files),
        'unique_symbols': set(),
        'file_sizes': [],
        'subdirectories': {},
        'date_ranges': {},
        'data_samples': []
    }
    
    # 分析每个子目录
    for subdir, files in files_by_subdir.items():
        print(f"      📁 {subdir}: {len(files)} 个文件")
        subdir_analysis = analyze_files_in_subdir(files, subdir)
        dir_analysis['subdirectories'][subdir] = subdir_analysis
        dir_analysis['unique_symbols'].update(subdir_analysis['symbols'])
        dir_analysis['file_sizes'].extend(subdir_analysis['file_sizes'])
    
    return dir_analysis

def analyze_files_in_subdir(files, subdir_name):
    """分析子目录中的文件"""
    symbols = set()
    file_sizes = []
    date_ranges = {}
    sample_data = []
    
    # 限制分析的文件数量，避免太慢
    files_to_analyze = files[:100] if len(files) > 100 else files
    
    for i, file_path in enumerate(files_to_analyze):
        try:
            # 基础文件信息
            file_size = file_path.stat().st_size / (1024 * 1024)  # MB
            file_sizes.append(file_size)
            
            # 从文件名提取股票代码
            symbol = extract_symbol_from_filename(file_path.name)
            if symbol:
                symbols.add(symbol)
            
            # 每10个文件读取一个样本分析数据内容
            if i % 10 == 0:
                try:
                    file_info = analyze_single_file(file_path)
                    if file_info:
                        sample_data.append(file_info)
                        if file_info.get('date_range'):
                            date_ranges[symbol] = file_info['date_range']
                except Exception as e:
                    print(f"         ⚠️ 读取文件失败 {file_path.name}: {e}")
        
        except Exception as e:
            print(f"         ⚠️ 分析文件失败 {file_path.name}: {e}")
    
    if len(files) > len(files_to_analyze):
        print(f"         📋 分析了前{len(files_to_analyze)}个文件，剩余{len(files) - len(files_to_analyze)}个文件")
    
    return {
        'file_count': len(files),
        'symbols': symbols,
        'unique_symbol_count': len(symbols),
        'file_sizes': file_sizes,
        'avg_file_size_mb': np.mean(file_sizes) if file_sizes else 0,
        'total_size_mb': sum(file_sizes),
        'date_ranges': date_ranges,
        'sample_data': sample_data[:5]  # 只保留前5个样本
    }

def extract_symbol_from_filename(filename):
    """从文件名提取股票代码"""
    # 移除扩展名
    name = filename.replace('.csv', '').replace('.parquet', '')
    
    # 常见的股票代码模式
    import re
    
    # 6位数字代码 (如 000001, 600000)
    match = re.search(r'\b(\d{6})\b', name)
    if match:
        code = match.group(1)
        # 根据代码判断交易所
        if code.startswith('0') or code.startswith('3'):
            return f"{code}.SZ"
        elif code.startswith('6') or code.startswith('9'):
            return f"{code}.SH"
        else:
            return code
    
    # 已经带有后缀的代码 (如 000001.SZ)
    match = re.search(r'\b(\d{6}\.(SZ|SH|XSHE|XSHG))\b', name)
    if match:
        return match.group(1)
    
    # 其他模式
    if name.isdigit() and len(name) == 6:
        return name
    
    return name

def analyze_single_file(file_path):
    """分析单个文件的详细内容"""
    try:
        if file_path.suffix == '.csv':
            # 先读取少量行数确定结构
            df_sample = pd.read_csv(file_path, nrows=100)
        elif file_path.suffix == '.parquet':
            df_sample = pd.read_parquet(file_path)
            if len(df_sample) > 100:
                df_sample = df_sample.head(100)
        else:
            return None
        
        if df_sample.empty:
            return None
        
        file_info = {
            'filename': file_path.name,
            'rows_sampled': len(df_sample),
            'columns': list(df_sample.columns),
            'column_count': len(df_sample.columns),
            'data_types': df_sample.dtypes.astype(str).to_dict(),
            'missing_data': df_sample.isnull().sum().to_dict(),
            'date_range': None
        }
        
        # 查找日期列并分析日期范围
        date_columns = [col for col in df_sample.columns 
                       if 'date' in col.lower() or 'time' in col.lower()]
        
        if date_columns:
            date_col = date_columns[0]
            try:
                dates = pd.to_datetime(df_sample[date_col], errors='coerce')
                valid_dates = dates.dropna()
                if not valid_dates.empty:
                    file_info['date_range'] = {
                        'start': valid_dates.min().strftime('%Y-%m-%d'),
                        'end': valid_dates.max().strftime('%Y-%m-%d'),
                        'total_days': (valid_dates.max() - valid_dates.min()).days + 1
                    }
            except:
                pass
        
        return file_info
    
    except Exception as e:
        return {'filename': file_path.name, 'error': str(e)}

def get_comprehensive_statistics(analysis_results):
    """获取综合统计信息"""
    print("\n" + "=" * 60)
    print("📊 综合数据统计")
    print("=" * 60)
    
    total_symbols = len(analysis_results['total_symbols'])
    total_files = sum(dir_info['file_count'] for dir_info in analysis_results['directories'].values())
    
    print(f"📁 总数据文件: {total_files:,} 个")
    print(f"📈 总股票数量: {total_symbols:,} 只")
    
    # 按目录显示详细信息
    for dir_name, dir_info in analysis_results['directories'].items():
        print(f"\n📂 {dir_name}:")
        print(f"   文件数: {dir_info['file_count']:,}")
        print(f"   股票数: {len(dir_info['unique_symbols'])}")
        
        if dir_info['subdirectories']:
            for subdir, subdir_info in dir_info['subdirectories'].items():
                if subdir != '.':
                    print(f"   📁 {subdir}:")
                    print(f"      文件: {subdir_info['file_count']:,}")
                    print(f"      股票: {subdir_info['unique_symbol_count']}")
                    print(f"      大小: {subdir_info['total_size_mb']:.1f} MB")
    
    # 显示股票代码样本
    if analysis_results['total_symbols']:
        print(f"\n📋 股票代码样本 (前20个):")
        for i, symbol in enumerate(analysis_results['total_symbols'][:20]):
            print(f"   {symbol}")
        
        if len(analysis_results['total_symbols']) > 20:
            print(f"   ... 还有 {len(analysis_results['total_symbols']) - 20} 个股票代码")
    
    return {
        'total_files': total_files,
        'total_symbols': total_symbols,
        'directories_analyzed': len(analysis_results['directories'])
    }

def save_detailed_analysis(analysis_results, statistics):
    """保存详细分析结果"""
    print("\n📝 保存详细分析结果...")
    
    # 创建报告目录
    reports_dir = Path('reports')
    reports_dir.mkdir(exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # 保存完整分析结果 (JSON)
    json_file = reports_dir / f'detailed_data_analysis_{timestamp}.json'
    
    # 转换set为list以便JSON序列化
    serializable_results = convert_sets_to_lists(analysis_results)
    
    with open(json_file, 'w', encoding='utf-8') as f:
        json.dump(serializable_results, f, ensure_ascii=False, indent=2, default=str)
    
    # 生成文本报告
    text_report = generate_text_report(analysis_results, statistics)
    text_file = reports_dir / f'detailed_data_analysis_{timestamp}.txt'
    
    with open(text_file, 'w', encoding='utf-8') as f:
        f.write(text_report)
    
    print(f"✅ JSON报告: {json_file}")
    print(f"✅ 文本报告: {text_file}")
    
    return json_file, text_file

def convert_sets_to_lists(obj):
    """递归转换set为list以便JSON序列化"""
    if isinstance(obj, set):
        return sorted(list(obj))
    elif isinstance(obj, dict):
        return {key: convert_sets_to_lists(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [convert_sets_to_lists(item) for item in obj]
    else:
        return obj

def generate_text_report(analysis_results, statistics):
    """生成文本格式的详细报告"""
    lines = []
    lines.append("=" * 80)
    lines.append("📊 本地数据详细分析报告")
    lines.append("=" * 80)
    lines.append(f"📅 分析时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("")
    
    # 概览统计
    lines.append("🎯 数据概览:")
    lines.append(f"   📁 总文件数: {statistics['total_files']:,}")
    lines.append(f"   📈 总股票数: {statistics['total_symbols']:,}")
    lines.append(f"   📂 目录数: {statistics['directories_analyzed']}")
    lines.append("")
    
    # 目录详情
    lines.append("📂 目录详情:")
    for dir_name, dir_info in analysis_results['directories'].items():
        lines.append(f"   {dir_name}:")
        lines.append(f"      📊 文件数: {dir_info['file_count']:,}")
        lines.append(f"      📈 股票数: {len(dir_info['unique_symbols'])}")
        
        if dir_info['subdirectories']:
            for subdir, subdir_info in dir_info['subdirectories'].items():
                if subdir != '.':
                    lines.append(f"      📁 {subdir}:")
                    lines.append(f"         文件: {subdir_info['file_count']:,}")
                    lines.append(f"         股票: {subdir_info['unique_symbol_count']}")
                    lines.append(f"         大小: {subdir_info['total_size_mb']:.1f} MB")
        lines.append("")
    
    # 股票代码列表
    lines.append("📋 完整股票列表:")
    symbols = analysis_results['total_symbols']
    if symbols:
        # 按交易所分组
        sz_symbols = [s for s in symbols if '.SZ' in s or (s.isdigit() and len(s) == 6 and (s.startswith('0') or s.startswith('3')))]
        sh_symbols = [s for s in symbols if '.SH' in s or (s.isdigit() and len(s) == 6 and s.startswith('6'))]
        other_symbols = [s for s in symbols if s not in sz_symbols and s not in sh_symbols]
        
        if sz_symbols:
            lines.append(f"   深市 ({len(sz_symbols)}只):")
            for i in range(0, len(sz_symbols), 10):
                batch = sz_symbols[i:i+10]
                lines.append(f"      {' '.join(batch)}")
        
        if sh_symbols:
            lines.append(f"   沪市 ({len(sh_symbols)}只):")
            for i in range(0, len(sh_symbols), 10):
                batch = sh_symbols[i:i+10]
                lines.append(f"      {' '.join(batch)}")
        
        if other_symbols:
            lines.append(f"   其他 ({len(other_symbols)}只):")
            for i in range(0, len(other_symbols), 10):
                batch = other_symbols[i:i+10]
                lines.append(f"      {' '.join(batch)}")
    
    lines.append("")
    lines.append("=" * 80)
    
    return "\n".join(lines)

def main():
    """主函数"""
    print("🔍 本地数据详细分析工具")
    print("=" * 60)
    
    # 执行详细分析
    analysis_results = analyze_all_data_files()
    
    # 生成统计信息
    statistics = get_comprehensive_statistics(analysis_results)
    
    # 保存分析结果
    json_file, text_file = save_detailed_analysis(analysis_results, statistics)
    
    print("\n" + "=" * 60)
    print("🎉 详细数据分析完成！")
    print("=" * 60)
    print(f"📊 发现 {statistics['total_symbols']} 只股票的数据")
    print(f"📁 总共 {statistics['total_files']} 个数据文件")
    print(f"📝 详细报告已保存到 reports/ 目录")
    
    return analysis_results, statistics

if __name__ == "__main__":
    main()