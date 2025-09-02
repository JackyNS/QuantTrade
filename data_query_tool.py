#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据查询工具 - 基于优化后的数据索引进行快速查询
"""

import sqlite3
import pandas as pd
from pathlib import Path
import json

class DataQueryTool:
    """数据查询工具"""
    
    def __init__(self):
        self.optimized_dir = Path("data/optimized_data")
        self.index_db_path = self.optimized_dir / "data_index.db"
        
    def query_data_catalog(self):
        """查询数据目录"""
        if not self.index_db_path.exists():
            print("❌ 数据索引不存在，请先运行数据优化")
            return
        
        conn = sqlite3.connect(self.index_db_path)
        cursor = conn.cursor()
        
        # 总体统计
        cursor.execute("SELECT COUNT(*), SUM(record_count) FROM data_index")
        total_files, total_records = cursor.fetchone()
        
        print("📊 **优化后数据目录**")
        print("=" * 50)
        print(f"📁 总文件数: {total_files:,}")
        print(f"📝 总记录数: {total_records:,}")
        
        # 按类别统计
        cursor.execute("""
            SELECT category, COUNT(*) as file_count, SUM(record_count) as record_count
            FROM data_index 
            GROUP BY category 
            ORDER BY record_count DESC
        """)
        
        print("\n📋 **按类别统计**:")
        for category, file_count, record_count in cursor.fetchall():
            print(f"  {category}: {file_count} 文件, {record_count:,} 记录")
        
        # 按API统计前10
        cursor.execute("""
            SELECT category, api_name, COUNT(*) as file_count, SUM(record_count) as record_count
            FROM data_index 
            GROUP BY category, api_name 
            ORDER BY record_count DESC
            LIMIT 10
        """)
        
        print("\n🔥 **记录数最多的前10个API**:")
        for category, api_name, file_count, record_count in cursor.fetchall():
            print(f"  {category}/{api_name}: {file_count} 文件, {record_count:,} 记录")
        
        conn.close()
    
    def search_apis(self, keyword=""):
        """搜索API"""
        if not self.index_db_path.exists():
            print("❌ 数据索引不存在")
            return []
        
        conn = sqlite3.connect(self.index_db_path)
        cursor = conn.cursor()
        
        if keyword:
            cursor.execute("""
                SELECT DISTINCT category, api_name, COUNT(*) as file_count, SUM(record_count) as record_count
                FROM data_index 
                WHERE api_name LIKE ? OR category LIKE ?
                GROUP BY category, api_name
                ORDER BY record_count DESC
            """, (f"%{keyword}%", f"%{keyword}%"))
            print(f"🔍 搜索结果 (关键词: '{keyword}'):")
        else:
            cursor.execute("""
                SELECT DISTINCT category, api_name, COUNT(*) as file_count, SUM(record_count) as record_count
                FROM data_index 
                GROUP BY category, api_name
                ORDER BY category, api_name
            """)
            print("📋 所有可用API:")
        
        results = cursor.fetchall()
        for category, api_name, file_count, record_count in results:
            print(f"  {category}/{api_name}: {file_count} 文件, {record_count:,} 记录")
        
        conn.close()
        return results
    
    def load_sample_data(self, category, api_name, max_rows=100):
        """加载样本数据"""
        if not self.index_db_path.exists():
            print("❌ 数据索引不存在")
            return None
        
        conn = sqlite3.connect(self.index_db_path)
        cursor = conn.cursor()
        
        # 查找文件
        cursor.execute("""
            SELECT file_path, record_count 
            FROM data_index 
            WHERE category = ? AND api_name = ?
            ORDER BY record_count DESC
            LIMIT 1
        """, (category, api_name))
        
        result = cursor.fetchone()
        if not result:
            print(f"❌ 未找到 {category}/{api_name}")
            conn.close()
            return None
        
        file_path, record_count = result
        full_path = self.optimized_dir / file_path
        
        if not full_path.exists():
            print(f"❌ 文件不存在: {full_path}")
            conn.close()
            return None
        
        try:
            # 加载样本数据
            df = pd.read_csv(full_path, nrows=max_rows)
            print(f"✅ 成功加载 {category}/{api_name}")
            print(f"📊 样本: {len(df)} 行 (总计 {record_count:,} 行)")
            print(f"📋 字段: {list(df.columns)}")
            
            if not df.empty:
                print("\n📄 **前5行数据预览**:")
                print(df.head().to_string())
            
            conn.close()
            return df
            
        except Exception as e:
            print(f"❌ 加载数据失败: {e}")
            conn.close()
            return None
    
    def get_data_quality_summary(self):
        """获取数据质量摘要"""
        quality_report_path = Path("data/final_comprehensive_download/data_quality_report.json")
        optimization_report_path = self.optimized_dir / "optimization_report.json"
        
        summary = {}
        
        # 读取质量报告
        if quality_report_path.exists():
            with open(quality_report_path, 'r', encoding='utf-8') as f:
                quality_data = json.load(f)
            summary["original_data"] = quality_data["summary"]
        
        # 读取优化报告
        if optimization_report_path.exists():
            with open(optimization_report_path, 'r', encoding='utf-8') as f:
                optimization_data = json.load(f)
            summary["optimization"] = optimization_data["statistics"]
            summary["compression_ratio"] = optimization_data["compression_ratio"]
        
        return summary
    
    def show_performance_comparison(self):
        """显示性能优化对比"""
        summary = self.get_data_quality_summary()
        
        if not summary:
            print("❌ 未找到性能对比数据")
            return
        
        print("🚀 **数据优化性能对比**")
        print("=" * 50)
        
        if "original_data" in summary:
            orig = summary["original_data"]
            print("📊 **原始数据**:")
            print(f"  📁 文件数: {orig.get('total_files', 0):,}")
            print(f"  💾 数据量: {orig.get('total_size_gb', 0)} GB")
            print(f"  📝 记录数: {orig.get('total_records', 0):,}")
        
        if "optimization" in summary:
            opt = summary["optimization"]
            print("\n✨ **优化后数据**:")
            print(f"  📁 处理文件: {opt.get('files_processed', 0):,}")
            print(f"  📝 记录数变化: {opt.get('total_records_before', 0):,} → {opt.get('total_records_after', 0):,}")
            print(f"  🧹 移除重复: {opt.get('duplicates_removed', 0):,} 条")
            print(f"  📝 标准化字段: {opt.get('fields_standardized', 0):,} 个")
            print(f"  📊 压缩比: {summary.get('compression_ratio', 0)}%")
        
        print("\n🎯 **优化效果**:")
        print("  ✅ 数据标准化完成")
        print("  ✅ 重复数据清理")
        print("  ✅ 快速索引建立") 
        print("  ✅ 查询性能优化")

def main():
    """主函数 - 演示数据查询功能"""
    tool = DataQueryTool()
    
    print("🔍 **数据查询工具演示**\n")
    
    # 1. 显示数据目录
    tool.query_data_catalog()
    
    # 2. 显示性能对比
    print("\n")
    tool.show_performance_comparison()
    
    # 3. 搜索示例
    print("\n")
    tool.search_apis("fdmt")
    
    # 4. 加载样本数据示例
    print("\n🧪 **样本数据加载演示**:")
    sample_df = tool.load_sample_data("financial", "fdmtindipsget")
    
    print("\n🎊 数据查询系统演示完成！")
    print("💡 提示: 现在可以通过索引快速查询和访问任何API数据")

if __name__ == "__main__":
    main()