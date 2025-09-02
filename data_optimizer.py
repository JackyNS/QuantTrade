#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据优化整理工具 - 标准化、去重、建立索引
"""

import pandas as pd
from pathlib import Path
import json
import logging
from datetime import datetime
import hashlib
from collections import defaultdict
import sqlite3

class DataOptimizer:
    """数据优化整理器"""
    
    def __init__(self):
        self.data_dir = Path("data/final_comprehensive_download")
        self.optimized_dir = Path("data/optimized_data")
        self.optimized_dir.mkdir(exist_ok=True)
        self.setup_logging()
        
        # 字段标准化映射
        self.field_mappings = {
            # 基础信息标准化
            'secID': 'security_id',
            'ticker': 'stock_code', 
            'secShortName': 'stock_name',
            'exchangeCD': 'exchange',
            'tradeDate': 'trade_date',
            'publishDate': 'publish_date',
            'endDate': 'end_date',
            'beginDate': 'begin_date',
            
            # 财务数据标准化
            'revenue': 'total_revenue',
            'NIncomeAttrP': 'net_income',
            'TAssets': 'total_assets',
            'TEquityAttrP': 'total_equity',
            'basicEPS': 'basic_eps',
            'ROE': 'roe',
            
            # 市场数据标准化
            'openPrice': 'open_price',
            'highestPrice': 'high_price', 
            'lowestPrice': 'low_price',
            'closePrice': 'close_price',
            'turnoverVol': 'volume',
            'turnoverValue': 'turnover',
            'PE': 'pe_ratio',
            'PB': 'pb_ratio'
        }
        
    def setup_logging(self):
        """设置日志"""
        log_file = self.optimized_dir / "data_optimization.log"
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
    
    def standardize_field_names(self, df, source_info=""):
        """标准化字段命名"""
        original_columns = df.columns.tolist()
        standardized_df = df.copy()
        
        # 应用字段映射
        rename_map = {}
        for old_name, new_name in self.field_mappings.items():
            if old_name in standardized_df.columns:
                rename_map[old_name] = new_name
        
        if rename_map:
            standardized_df.rename(columns=rename_map, inplace=True)
            logging.info(f"📝 {source_info}: 标准化 {len(rename_map)} 个字段名")
        
        # 统一日期字段格式
        date_fields = [col for col in standardized_df.columns if 'date' in col.lower()]
        for date_field in date_fields:
            try:
                standardized_df[date_field] = pd.to_datetime(standardized_df[date_field]).dt.date
            except:
                pass
        
        return standardized_df, len(rename_map)
    
    def detect_duplicates(self, df, key_columns=None):
        """检测重复记录"""
        if df.empty:
            return df, 0, []
        
        # 如果没有指定关键列，尝试自动识别
        if key_columns is None:
            key_columns = []
            potential_keys = ['security_id', 'stock_code', 'trade_date', 'publish_date', 'end_date']
            for key in potential_keys:
                if key in df.columns:
                    key_columns.append(key)
        
        if not key_columns:
            # 使用所有列检测完全重复
            duplicates = df.duplicated()
            duplicate_rows = df[duplicates]
            cleaned_df = df.drop_duplicates()
        else:
            # 使用关键列检测业务重复
            duplicates = df.duplicated(subset=key_columns)
            duplicate_rows = df[duplicates]
            cleaned_df = df.drop_duplicates(subset=key_columns)
        
        duplicate_count = len(duplicate_rows)
        return cleaned_df, duplicate_count, key_columns
    
    def create_data_hash(self, df):
        """创建数据哈希用于完整性验证"""
        if df.empty:
            return ""
        
        # 创建基于数据内容的哈希
        data_string = df.to_csv(index=False)
        return hashlib.md5(data_string.encode()).hexdigest()
    
    def optimize_single_category(self, category):
        """优化单个数据类别"""
        logging.info(f"🔧 开始优化 {category} 类别...")
        
        category_dir = self.data_dir / category
        if not category_dir.exists():
            logging.warning(f"❌ 类别目录不存在: {category}")
            return {}
        
        optimized_category_dir = self.optimized_dir / category
        optimized_category_dir.mkdir(exist_ok=True)
        
        category_stats = {
            "apis_processed": 0,
            "files_processed": 0,
            "total_records_before": 0,
            "total_records_after": 0,
            "duplicates_removed": 0,
            "fields_standardized": 0
        }
        
        # 处理每个API目录
        for api_dir in category_dir.iterdir():
            if not api_dir.is_dir():
                continue
                
            api_name = api_dir.name
            optimized_api_dir = optimized_category_dir / api_name
            optimized_api_dir.mkdir(exist_ok=True)
            
            logging.info(f"  📁 处理API: {api_name}")
            
            # 收集所有CSV文件
            csv_files = list(api_dir.glob("*.csv"))
            if not csv_files:
                continue
            
            api_stats = {
                "files": len(csv_files),
                "records_before": 0,
                "records_after": 0,
                "duplicates": 0
            }
            
            # 处理每个CSV文件
            for csv_file in csv_files:
                try:
                    df = pd.read_csv(csv_file)
                    original_records = len(df)
                    api_stats["records_before"] += original_records
                    
                    if df.empty:
                        continue
                    
                    # 1. 标准化字段名
                    df, fields_renamed = self.standardize_field_names(df, f"{category}/{api_name}/{csv_file.name}")
                    category_stats["fields_standardized"] += fields_renamed
                    
                    # 2. 去重处理
                    df, duplicates_removed, key_columns = self.detect_duplicates(df)
                    api_stats["duplicates"] += duplicates_removed
                    api_stats["records_after"] += len(df)
                    
                    if duplicates_removed > 0:
                        logging.info(f"    🧹 {csv_file.name}: 移除 {duplicates_removed} 条重复记录")
                    
                    # 3. 保存优化后的数据
                    if not df.empty:
                        optimized_file = optimized_api_dir / csv_file.name
                        df.to_csv(optimized_file, index=False, encoding='utf-8-sig')
                    
                    # 4. 创建元数据
                    metadata = {
                        "original_file": str(csv_file.relative_to(self.data_dir)),
                        "records_before": original_records,
                        "records_after": len(df),
                        "duplicates_removed": duplicates_removed,
                        "key_columns": key_columns,
                        "fields_renamed": fields_renamed,
                        "data_hash": self.create_data_hash(df),
                        "optimization_time": datetime.now().isoformat()
                    }
                    
                    metadata_file = optimized_api_dir / f"{csv_file.stem}_metadata.json"
                    with open(metadata_file, 'w', encoding='utf-8') as f:
                        json.dump(metadata, f, indent=2, ensure_ascii=False)
                    
                except Exception as e:
                    logging.error(f"❌ 处理文件失败: {csv_file} - {e}")
            
            # 更新统计
            category_stats["apis_processed"] += 1
            category_stats["files_processed"] += api_stats["files"]
            category_stats["total_records_before"] += api_stats["records_before"]
            category_stats["total_records_after"] += api_stats["records_after"]
            category_stats["duplicates_removed"] += api_stats["duplicates"]
            
            logging.info(f"    ✅ {api_name}: {api_stats['files']} 文件, "
                        f"{api_stats['records_before']:,} → {api_stats['records_after']:,} 记录")
        
        logging.info(f"✅ {category} 优化完成: {category_stats['duplicates_removed']:,} 条重复记录移除")
        return category_stats
    
    def create_unified_index(self):
        """创建统一数据索引"""
        logging.info("📇 创建统一数据索引...")
        
        index_db_path = self.optimized_dir / "data_index.db"
        conn = sqlite3.connect(index_db_path)
        cursor = conn.cursor()
        
        # 创建主索引表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS data_index (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                category TEXT,
                api_name TEXT,
                file_name TEXT,
                file_path TEXT,
                record_count INTEGER,
                data_hash TEXT,
                last_updated TEXT,
                key_fields TEXT
            )
        ''')
        
        # 创建快速查询索引
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_category_api ON data_index(category, api_name)
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_last_updated ON data_index(last_updated)
        ''')
        
        # 扫描优化后的数据并建立索引
        categories = ["basic_info", "financial", "special_trading", "governance"]
        total_indexed = 0
        
        for category in categories:
            category_dir = self.optimized_dir / category
            if not category_dir.exists():
                continue
                
            for api_dir in category_dir.iterdir():
                if not api_dir.is_dir():
                    continue
                    
                api_name = api_dir.name
                
                for csv_file in api_dir.glob("*.csv"):
                    # 读取对应的元数据
                    metadata_file = api_dir / f"{csv_file.stem}_metadata.json"
                    metadata = {}
                    if metadata_file.exists():
                        with open(metadata_file, 'r', encoding='utf-8') as f:
                            metadata = json.load(f)
                    
                    # 插入索引记录
                    cursor.execute('''
                        INSERT OR REPLACE INTO data_index 
                        (category, api_name, file_name, file_path, record_count, data_hash, last_updated, key_fields)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        category,
                        api_name,
                        csv_file.name,
                        str(csv_file.relative_to(self.optimized_dir)),
                        metadata.get('records_after', 0),
                        metadata.get('data_hash', ''),
                        metadata.get('optimization_time', ''),
                        json.dumps(metadata.get('key_columns', []))
                    ))
                    
                    total_indexed += 1
        
        conn.commit()
        conn.close()
        
        logging.info(f"📇 索引创建完成: {total_indexed} 个文件已建立索引")
        return total_indexed
    
    def run_optimization(self):
        """执行完整的数据优化流程"""
        logging.info("🚀 开始数据优化整理...")
        
        start_time = datetime.now()
        categories = ["basic_info", "financial", "special_trading", "governance"]
        
        total_stats = {
            "categories_processed": 0,
            "apis_processed": 0,
            "files_processed": 0,
            "total_records_before": 0,
            "total_records_after": 0,
            "duplicates_removed": 0,
            "fields_standardized": 0
        }
        
        # 优化每个类别
        for category in categories:
            try:
                category_stats = self.optimize_single_category(category)
                
                # 累计统计
                for key in category_stats:
                    if key in total_stats:
                        total_stats[key] += category_stats[key]
                total_stats["categories_processed"] += 1
                
            except Exception as e:
                logging.error(f"❌ {category} 优化失败: {e}")
        
        # 创建统一索引
        try:
            indexed_files = self.create_unified_index()
        except Exception as e:
            logging.error(f"❌ 索引创建失败: {e}")
            indexed_files = 0
        
        # 生成优化报告
        end_time = datetime.now()
        duration = end_time - start_time
        
        optimization_report = {
            "optimization_time": end_time.isoformat(),
            "duration_seconds": duration.total_seconds(),
            "statistics": total_stats,
            "indexed_files": indexed_files,
            "compression_ratio": round((total_stats["total_records_before"] - total_stats["total_records_after"]) / total_stats["total_records_before"] * 100, 2) if total_stats["total_records_before"] > 0 else 0
        }
        
        # 保存报告
        report_file = self.optimized_dir / "optimization_report.json"
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(optimization_report, f, indent=2, ensure_ascii=False)
        
        # 输出摘要
        logging.info("🎊 数据优化整理完成!")
        logging.info("=" * 50)
        logging.info("📊 **优化结果摘要**")
        logging.info(f"  📁 处理类别: {total_stats['categories_processed']}")
        logging.info(f"  🔌 处理API: {total_stats['apis_processed']}")
        logging.info(f"  📄 处理文件: {total_stats['files_processed']}")
        logging.info(f"  📝 记录数变化: {total_stats['total_records_before']:,} → {total_stats['total_records_after']:,}")
        logging.info(f"  🧹 移除重复: {total_stats['duplicates_removed']:,} 条")
        logging.info(f"  📝 标准化字段: {total_stats['fields_standardized']} 个")
        logging.info(f"  📇 建立索引: {indexed_files} 个文件")
        logging.info(f"  📊 压缩比: {optimization_report['compression_ratio']}%")
        logging.info(f"  ⏱️ 处理时长: {duration}")
        
        return optimization_report

if __name__ == "__main__":
    optimizer = DataOptimizer()
    optimizer.run_optimization()