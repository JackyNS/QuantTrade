#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
统一数据访问接口 - 整合所有CSV数据源的统一访问入口
"""

import pandas as pd
from pathlib import Path
import json
import logging
from datetime import datetime
from typing import Optional, List, Dict, Union

class UnifiedDataAccess:
    """统一数据访问接口"""
    
    def __init__(self):
        # 数据源优先级（数字越小优先级越高）
        self.data_sources = {
            "final_comprehensive_download": {
                "path": Path("data/final_comprehensive_download"),
                "description": "完整下载数据 (主数据源)",
                "priority": 1
            },
            "optimized_data": {
                "path": Path("data/optimized_data"),
                "description": "优化CSV数据",
                "priority": 2
            },
            "priority_download": {
                "path": Path("data/priority_download"),
                "description": "优先下载数据",
                "priority": 3
            },
            "historical_download": {
                "path": Path("data/historical_download"),
                "description": "历史基础数据",
                "priority": 4
            }
        }
        
        # 从分析报告加载数据映射
        self.load_data_mapping()
        self.setup_logging()
    
    def load_data_mapping(self):
        """加载数据映射配置"""
        report_file = Path("csv_data_comprehensive_report.json")
        if report_file.exists():
            with open(report_file, 'r', encoding='utf-8') as f:
                report = json.load(f)
            
            # 从报告中提取最佳数据源映射
            self.api_mapping = {}
            consolidation_plan = report.get("consolidation_plan", {})
            target_structure = consolidation_plan.get("target_structure", {})
            
            for category, apis in target_structure.items():
                if category not in self.api_mapping:
                    self.api_mapping[category] = {}
                
                for api_name, info in apis.items():
                    self.api_mapping[category][api_name] = {
                        "source_name": info["source_name"],
                        "source_path": Path(info["source_path"]),
                        "files": info["files"],
                        "size_mb": info["size_mb"]
                    }
        else:
            # 如果没有报告，默认使用final_comprehensive_download
            logging.warning("未找到数据分析报告，使用默认映射")
            self.api_mapping = {}
    
    def setup_logging(self):
        """设置日志"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
    
    def get_available_categories(self) -> List[str]:
        """获取所有可用的数据类别"""
        categories = set()
        
        for source_info in self.data_sources.values():
            source_path = source_info["path"]
            if source_path.exists():
                for category_dir in source_path.iterdir():
                    if category_dir.is_dir():
                        categories.add(category_dir.name)
        
        return sorted(list(categories))
    
    def get_available_apis(self, category: str) -> List[str]:
        """获取指定类别下的所有API"""
        apis = set()
        
        for source_info in self.data_sources.values():
            category_path = source_info["path"] / category
            if category_path.exists():
                for api_dir in category_path.iterdir():
                    if api_dir.is_dir() and list(api_dir.glob("*.csv")):
                        apis.add(api_dir.name)
        
        return sorted(list(apis))
    
    def get_available_files(self, category: str, api: str) -> List[Dict]:
        """获取指定API下的所有文件及其来源信息"""
        files_info = []
        
        for source_name, source_info in self.data_sources.items():
            api_path = source_info["path"] / category / api
            if api_path.exists():
                csv_files = list(api_path.glob("*.csv"))
                for csv_file in csv_files:
                    files_info.append({
                        "filename": csv_file.name,
                        "source": source_name,
                        "path": str(csv_file),
                        "size_mb": csv_file.stat().st_size / 1024 / 1024,
                        "priority": source_info["priority"]
                    })
        
        # 按优先级和文件名排序
        files_info.sort(key=lambda x: (x["priority"], x["filename"]))
        
        return files_info
    
    def find_best_data_source(self, category: str, api: str, filename: Optional[str] = None) -> Optional[Path]:
        """找到最佳的数据源路径"""
        
        # 1. 如果有映射配置，优先使用
        if category in self.api_mapping and api in self.api_mapping[category]:
            mapped_source = self.api_mapping[category][api]
            source_path = mapped_source["source_path"]
            
            if filename:
                file_path = source_path / filename
                if not file_path.suffix:
                    file_path = file_path.with_suffix('.csv')
                if file_path.exists():
                    return file_path
            else:
                # 返回目录下的第一个CSV文件
                csv_files = list(source_path.glob("*.csv"))
                if csv_files:
                    return csv_files[0]
        
        # 2. 按优先级查找
        for source_name, source_info in sorted(self.data_sources.items(), 
                                               key=lambda x: x[1]["priority"]):
            api_path = source_info["path"] / category / api
            
            if not api_path.exists():
                continue
            
            if filename:
                file_path = api_path / filename
                if not file_path.suffix:
                    file_path = file_path.with_suffix('.csv')
                if file_path.exists():
                    return file_path
            else:
                # 返回目录下的第一个CSV文件
                csv_files = list(api_path.glob("*.csv"))
                if csv_files:
                    return csv_files[0]
        
        return None
    
    def read_data(self, 
                  category: str, 
                  api: str, 
                  filename: Optional[str] = None,
                  max_rows: Optional[int] = None,
                  columns: Optional[List[str]] = None,
                  date_range: Optional[tuple] = None,
                  use_chunks: bool = False,
                  chunk_size: int = 10000) -> Optional[pd.DataFrame]:
        """读取数据的主要接口"""
        
        logging.info(f"📊 读取数据: {category}/{api}")
        if filename:
            logging.info(f"   指定文件: {filename}")
        
        # 找到最佳数据源
        file_path = self.find_best_data_source(category, api, filename)
        
        if not file_path:
            logging.error(f"❌ 未找到数据: {category}/{api}")
            if filename:
                logging.error(f"   文件: {filename}")
            return None
        
        logging.info(f"📖 数据源: {file_path}")
        
        try:
            # 获取文件信息
            file_size_mb = file_path.stat().st_size / 1024 / 1024
            logging.info(f"📊 文件大小: {file_size_mb:.2f} MB")
            
            # 选择读取策略
            if use_chunks or file_size_mb > 100:  # 大于100MB使用分块
                logging.info("📋 使用分块读取策略")
                df = self._read_large_file(file_path, max_rows, columns, chunk_size)
            else:
                logging.info("📋 使用标准读取策略")
                df = pd.read_csv(file_path, low_memory=False)
                
                if columns:
                    available_cols = [col for col in columns if col in df.columns]
                    if available_cols:
                        df = df[available_cols]
                
                if max_rows and len(df) > max_rows:
                    df = df.head(max_rows)
            
            if df is None or df.empty:
                logging.error("❌ 读取的数据为空")
                return None
            
            # 日期过滤
            if date_range:
                df = self._filter_by_date(df, date_range)
            
            logging.info(f"✅ 成功读取: {df.shape[0]:,} 行, {df.shape[1]} 列")
            return df
            
        except Exception as e:
            logging.error(f"❌ 读取数据失败: {e}")
            return None
    
    def _read_large_file(self, file_path: Path, max_rows: Optional[int], 
                        columns: Optional[List[str]], chunk_size: int) -> Optional[pd.DataFrame]:
        """读取大文件的分块策略"""
        chunk_list = []
        total_rows = 0
        
        try:
            for chunk in pd.read_csv(file_path, chunksize=chunk_size, low_memory=False):
                if columns:
                    available_cols = [col for col in columns if col in chunk.columns]
                    if available_cols:
                        chunk = chunk[available_cols]
                
                chunk_list.append(chunk)
                total_rows += len(chunk)
                
                if max_rows and total_rows >= max_rows:
                    break
            
            if not chunk_list:
                return None
            
            df = pd.concat(chunk_list, ignore_index=True)
            
            if max_rows and len(df) > max_rows:
                df = df.head(max_rows)
            
            return df
            
        except Exception as e:
            logging.error(f"❌ 分块读取失败: {e}")
            return None
    
    def _filter_by_date(self, df: pd.DataFrame, date_range: tuple) -> pd.DataFrame:
        """按日期范围过滤数据"""
        try:
            start_date, end_date = date_range
            
            # 寻找日期列
            date_columns = [col for col in df.columns if 'date' in col.lower()]
            
            if not date_columns:
                logging.warning("⚠️ 未找到日期列，跳过日期过滤")
                return df
            
            # 使用第一个日期列进行过滤
            date_col = date_columns[0]
            df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
            
            # 过滤
            mask = (df[date_col] >= start_date) & (df[date_col] <= end_date)
            filtered_df = df[mask]
            
            logging.info(f"📅 日期过滤: {len(df):,} → {len(filtered_df):,} 行")
            return filtered_df
            
        except Exception as e:
            logging.warning(f"⚠️ 日期过滤失败: {e}，返回原始数据")
            return df
    
    def get_data_info(self, category: str, api: str) -> Dict:
        """获取数据信息"""
        info = {
            "category": category,
            "api": api,
            "available_files": [],
            "total_files": 0,
            "total_size_mb": 0,
            "data_sources": [],
            "recommended_source": None
        }
        
        files_info = self.get_available_files(category, api)
        info["available_files"] = files_info
        info["total_files"] = len(files_info)
        info["total_size_mb"] = sum(f["size_mb"] for f in files_info)
        
        # 获取所有数据源
        sources = list(set(f["source"] for f in files_info))
        info["data_sources"] = sources
        
        # 推荐最高优先级的数据源
        if files_info:
            info["recommended_source"] = files_info[0]["source"]
        
        return info
    
    def show_data_catalog(self):
        """显示数据目录"""
        print("📚 **统一数据访问目录**")
        print("=" * 60)
        
        categories = self.get_available_categories()
        
        total_apis = 0
        total_sources = 0
        
        for category in categories:
            apis = self.get_available_apis(category)
            print(f"\n📂 **{category}** ({len(apis)} 个API)")
            
            for api in apis[:5]:  # 显示前5个API
                info = self.get_data_info(category, api)
                sources_str = ", ".join(info["data_sources"])
                print(f"  🔌 {api}: {info['total_files']} 文件, {info['total_size_mb']:.1f}MB")
                print(f"     📍 来源: {sources_str}")
                print(f"     ⭐ 推荐: {info['recommended_source']}")
            
            if len(apis) > 5:
                print(f"  ... 还有 {len(apis) - 5} 个API")
            
            total_apis += len(apis)
            total_sources += len(set(f["source"] for api in apis for f in self.get_available_files(category, api)))
        
        print(f"\n🎯 **总计**: {len(categories)} 个类别, {total_apis} 个API")
        print(f"📦 **数据源**: {len(self.data_sources)} 个目录")

def main():
    """主函数 - 演示统一数据访问功能"""
    accessor = UnifiedDataAccess()
    
    print("🚀 **统一数据访问系统演示**\n")
    
    # 1. 显示数据目录
    accessor.show_data_catalog()
    
    # 2. 读取样本数据
    print("\n" + "="*60)
    print("🧪 **样本数据读取演示**:")
    
    # 读取财务数据
    df = accessor.read_data(
        category="financial",
        api="fdmtindipsget",
        max_rows=5
    )
    
    if df is not None:
        print("\n📄 **数据预览**:")
        print(df.to_string())
    
    # 3. 获取数据信息
    print("\n" + "="*60)
    print("ℹ️ **数据信息查询演示**:")
    
    info = accessor.get_data_info("financial", "fdmtindipsget")
    print(f"📊 API信息: {info['category']}/{info['api']}")
    print(f"📁 文件数量: {info['total_files']}")
    print(f"💾 总大小: {info['total_size_mb']:.1f} MB")
    print(f"📦 数据源: {info['data_sources']}")
    print(f"⭐ 推荐源: {info['recommended_source']}")
    
    print("\n🎊 统一数据访问系统演示完成！")
    print("💡 提示: 系统会自动选择最佳数据源，确保数据的完整性和时效性")

if __name__ == "__main__":
    main()