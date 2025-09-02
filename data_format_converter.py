#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据格式转换器 - 将优化后的CSV数据转换为Parquet格式
保持与历史数据格式的一致性
"""

import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from pathlib import Path
import json
import logging
import sqlite3
from datetime import datetime
import os

class DataFormatConverter:
    """数据格式转换器"""
    
    def __init__(self):
        self.optimized_dir = Path("data/optimized_data")
        self.parquet_dir = Path("data/optimized_parquet")
        self.parquet_dir.mkdir(exist_ok=True)
        self.setup_logging()
        
    def setup_logging(self):
        """设置日志"""
        log_file = self.parquet_dir / "format_conversion.log"
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
    
    def convert_csv_to_parquet(self, csv_file, parquet_file):
        """转换单个CSV文件为Parquet"""
        try:
            # 读取CSV文件
            df = pd.read_csv(csv_file)
            
            if df.empty:
                logging.warning(f"跳过空文件: {csv_file.name}")
                return False
            
            # 优化数据类型
            df = self.optimize_dtypes(df)
            
            # 保存为Parquet格式
            df.to_parquet(parquet_file, compression='snappy', index=False)
            
            # 计算压缩比
            csv_size = csv_file.stat().st_size
            parquet_size = parquet_file.stat().st_size
            compression_ratio = (1 - parquet_size / csv_size) * 100 if csv_size > 0 else 0
            
            logging.info(f"✅ {csv_file.name}: {len(df):,} 行, "
                        f"{csv_size/1024/1024:.2f}MB → {parquet_size/1024/1024:.2f}MB "
                        f"(压缩 {compression_ratio:.1f}%)")
            
            return True, len(df), csv_size, parquet_size
            
        except Exception as e:
            logging.error(f"❌ 转换失败 {csv_file.name}: {e}")
            return False, 0, 0, 0
    
    def optimize_dtypes(self, df):
        """优化数据类型以减少存储空间"""
        optimized_df = df.copy()
        
        for col in optimized_df.columns:
            col_data = optimized_df[col]
            
            # 跳过空列
            if col_data.isna().all():
                continue
            
            # 尝试转换日期列
            if 'date' in col.lower() and col_data.dtype == 'object':
                try:
                    optimized_df[col] = pd.to_datetime(col_data, errors='ignore')
                    continue
                except:
                    pass
            
            # 优化数值列
            if pd.api.types.is_numeric_dtype(col_data):
                # 整数优化
                if pd.api.types.is_integer_dtype(col_data):
                    col_min = col_data.min()
                    col_max = col_data.max()
                    
                    if col_min >= 0:  # 无符号整数
                        if col_max < 255:
                            optimized_df[col] = col_data.astype('uint8')
                        elif col_max < 65535:
                            optimized_df[col] = col_data.astype('uint16')
                        elif col_max < 4294967295:
                            optimized_df[col] = col_data.astype('uint32')
                    else:  # 有符号整数
                        if col_min >= -128 and col_max < 127:
                            optimized_df[col] = col_data.astype('int8')
                        elif col_min >= -32768 and col_max < 32767:
                            optimized_df[col] = col_data.astype('int16')
                        elif col_min >= -2147483648 and col_max < 2147483647:
                            optimized_df[col] = col_data.astype('int32')
                
                # 浮点数优化
                elif pd.api.types.is_float_dtype(col_data):
                    optimized_df[col] = col_data.astype('float32')
            
            # 字符串分类优化
            elif col_data.dtype == 'object':
                unique_count = col_data.nunique()
                total_count = len(col_data)
                
                # 如果唯一值少于总数的50%，转为分类类型
                if unique_count / total_count < 0.5:
                    optimized_df[col] = col_data.astype('category')
        
        return optimized_df
    
    def convert_category(self, category):
        """转换单个类别的所有数据"""
        logging.info(f"🔄 开始转换 {category} 类别...")
        
        csv_category_dir = self.optimized_dir / category
        parquet_category_dir = self.parquet_dir / category
        
        if not csv_category_dir.exists():
            logging.warning(f"❌ CSV类别目录不存在: {category}")
            return {}
        
        parquet_category_dir.mkdir(exist_ok=True)
        
        stats = {
            "apis_converted": 0,
            "files_converted": 0,
            "total_records": 0,
            "csv_size": 0,
            "parquet_size": 0,
            "failed_files": []
        }
        
        # 处理每个API目录
        for api_dir in csv_category_dir.iterdir():
            if not api_dir.is_dir():
                continue
                
            api_name = api_dir.name
            parquet_api_dir = parquet_category_dir / api_name
            parquet_api_dir.mkdir(exist_ok=True)
            
            logging.info(f"  📁 转换API: {api_name}")
            
            api_converted = False
            
            # 转换所有CSV文件
            for csv_file in api_dir.glob("*.csv"):
                parquet_file = parquet_api_dir / f"{csv_file.stem}.parquet"
                
                result = self.convert_csv_to_parquet(csv_file, parquet_file)
                
                if isinstance(result, tuple) and result[0]:  # 转换成功
                    success, records, csv_size, parquet_size = result
                    stats["files_converted"] += 1
                    stats["total_records"] += records
                    stats["csv_size"] += csv_size
                    stats["parquet_size"] += parquet_size
                    api_converted = True
                    
                    # 复制元数据文件
                    metadata_file = api_dir / f"{csv_file.stem}_metadata.json"
                    if metadata_file.exists():
                        parquet_metadata_file = parquet_api_dir / f"{csv_file.stem}_metadata.json"
                        # 更新元数据以反映格式变更
                        with open(metadata_file, 'r', encoding='utf-8') as f:
                            metadata = json.load(f)
                        
                        metadata["parquet_conversion"] = {
                            "converted_time": datetime.now().isoformat(),
                            "csv_size": csv_size,
                            "parquet_size": parquet_size,
                            "compression_ratio": (1 - parquet_size / csv_size) * 100 if csv_size > 0 else 0
                        }
                        
                        with open(parquet_metadata_file, 'w', encoding='utf-8') as f:
                            json.dump(metadata, f, indent=2, ensure_ascii=False)
                else:
                    stats["failed_files"].append(str(csv_file.relative_to(self.optimized_dir)))
            
            if api_converted:
                stats["apis_converted"] += 1
        
        compression_ratio = (1 - stats["parquet_size"] / stats["csv_size"]) * 100 if stats["csv_size"] > 0 else 0
        
        logging.info(f"✅ {category} 转换完成: {stats['files_converted']} 文件, "
                    f"{stats['csv_size']/1024/1024/1024:.2f}GB → "
                    f"{stats['parquet_size']/1024/1024/1024:.2f}GB "
                    f"(压缩 {compression_ratio:.1f}%)")
        
        return stats
    
    def create_parquet_index(self):
        """创建Parquet格式的数据索引"""
        logging.info("📇 创建Parquet格式数据索引...")
        
        index_db_path = self.parquet_dir / "data_index.db"
        conn = sqlite3.connect(index_db_path)
        cursor = conn.cursor()
        
        # 创建索引表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS data_index (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                category TEXT,
                api_name TEXT,
                file_name TEXT,
                file_path TEXT,
                record_count INTEGER,
                file_size_mb REAL,
                compression_ratio REAL,
                last_updated TEXT,
                key_fields TEXT,
                data_hash TEXT
            )
        ''')
        
        # 创建索引
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_category_api ON data_index(category, api_name)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_last_updated ON data_index(last_updated)')
        
        categories = ["basic_info", "financial", "special_trading", "governance"]
        total_indexed = 0
        
        for category in categories:
            category_dir = self.parquet_dir / category
            if not category_dir.exists():
                continue
                
            for api_dir in category_dir.iterdir():
                if not api_dir.is_dir():
                    continue
                    
                api_name = api_dir.name
                
                for parquet_file in api_dir.glob("*.parquet"):
                    # 读取记录数
                    try:
                        pf = pq.ParquetFile(parquet_file)
                        record_count = pf.metadata.num_rows
                        file_size_mb = parquet_file.stat().st_size / 1024 / 1024
                    except:
                        record_count = 0
                        file_size_mb = 0
                    
                    # 读取元数据
                    metadata_file = api_dir / f"{parquet_file.stem}_metadata.json"
                    compression_ratio = 0
                    key_fields = []
                    data_hash = ""
                    last_updated = ""
                    
                    if metadata_file.exists():
                        with open(metadata_file, 'r', encoding='utf-8') as f:
                            metadata = json.load(f)
                        
                        compression_ratio = metadata.get('parquet_conversion', {}).get('compression_ratio', 0)
                        key_fields = metadata.get('key_columns', [])
                        data_hash = metadata.get('data_hash', '')
                        last_updated = metadata.get('parquet_conversion', {}).get('converted_time', '')
                    
                    # 插入索引记录
                    cursor.execute('''
                        INSERT OR REPLACE INTO data_index 
                        (category, api_name, file_name, file_path, record_count, file_size_mb, 
                         compression_ratio, last_updated, key_fields, data_hash)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        category,
                        api_name,
                        parquet_file.name,
                        str(parquet_file.relative_to(self.parquet_dir)),
                        record_count,
                        file_size_mb,
                        compression_ratio,
                        last_updated,
                        json.dumps(key_fields),
                        data_hash
                    ))
                    
                    total_indexed += 1
        
        conn.commit()
        conn.close()
        
        logging.info(f"📇 Parquet索引创建完成: {total_indexed} 个文件")
        return total_indexed
    
    def run_conversion(self):
        """执行完整的格式转换"""
        logging.info("🚀 开始CSV到Parquet格式转换...")
        
        start_time = datetime.now()
        categories = ["basic_info", "financial", "special_trading", "governance"]
        
        total_stats = {
            "categories_converted": 0,
            "apis_converted": 0,
            "files_converted": 0,
            "total_records": 0,
            "csv_size": 0,
            "parquet_size": 0,
            "failed_files": []
        }
        
        # 转换每个类别
        for category in categories:
            try:
                category_stats = self.convert_category(category)
                
                # 累计统计
                for key in ['apis_converted', 'files_converted', 'total_records', 'csv_size', 'parquet_size']:
                    if key in category_stats:
                        total_stats[key] += category_stats[key]
                
                total_stats['failed_files'].extend(category_stats.get('failed_files', []))
                total_stats["categories_converted"] += 1
                
            except Exception as e:
                logging.error(f"❌ {category} 转换失败: {e}")
        
        # 创建Parquet索引
        try:
            indexed_files = self.create_parquet_index()
        except Exception as e:
            logging.error(f"❌ 索引创建失败: {e}")
            indexed_files = 0
        
        # 生成转换报告
        end_time = datetime.now()
        duration = end_time - start_time
        
        overall_compression = (1 - total_stats["parquet_size"] / total_stats["csv_size"]) * 100 if total_stats["csv_size"] > 0 else 0
        
        conversion_report = {
            "conversion_time": end_time.isoformat(),
            "duration_seconds": duration.total_seconds(),
            "statistics": total_stats,
            "indexed_files": indexed_files,
            "overall_compression_ratio": overall_compression,
            "storage_savings_gb": (total_stats["csv_size"] - total_stats["parquet_size"]) / 1024 / 1024 / 1024
        }
        
        # 保存报告
        report_file = self.parquet_dir / "conversion_report.json"
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(conversion_report, f, indent=2, ensure_ascii=False)
        
        # 输出摘要
        logging.info("🎊 数据格式转换完成!")
        logging.info("=" * 50)
        logging.info("📊 **转换结果摘要**")
        logging.info(f"  📁 转换类别: {total_stats['categories_converted']}")
        logging.info(f"  🔌 转换API: {total_stats['apis_converted']}")
        logging.info(f"  📄 转换文件: {total_stats['files_converted']}")
        logging.info(f"  📝 总记录数: {total_stats['total_records']:,}")
        logging.info(f"  💾 存储变化: {total_stats['csv_size']/1024/1024/1024:.2f}GB → {total_stats['parquet_size']/1024/1024/1024:.2f}GB")
        logging.info(f"  📊 总压缩比: {overall_compression:.1f}%")
        logging.info(f"  💰 节省存储: {conversion_report['storage_savings_gb']:.2f}GB")
        logging.info(f"  📇 建立索引: {indexed_files} 个文件")
        logging.info(f"  ⏱️ 转换时长: {duration}")
        
        if total_stats['failed_files']:
            logging.warning(f"⚠️ {len(total_stats['failed_files'])} 个文件转换失败")
        
        return conversion_report

if __name__ == "__main__":
    converter = DataFormatConverter()
    converter.run_conversion()