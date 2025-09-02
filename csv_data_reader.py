#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CSV数据读取工具 - 纯CSV格式的数据访问接口
"""

import pandas as pd
from pathlib import Path
import json
from datetime import datetime
import logging

class CSVDataReader:
    """CSV数据读取器"""
    
    def __init__(self):
        # 主要数据源目录
        self.main_data_dir = Path("data/final_comprehensive_download")
        self.optimized_data_dir = Path("data/optimized_data")  
        self.setup_logging()
        
    def setup_logging(self):
        """设置日志"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
    
    def get_available_datasets(self):
        """获取所有可用的数据集"""
        datasets = {}
        
        # 扫描主要数据源
        if self.main_data_dir.exists():
            datasets["comprehensive"] = self._scan_directory(
                self.main_data_dir, "完整下载数据 (204GB)"
            )
        
        # 扫描优化数据源
        if self.optimized_data_dir.exists():
            datasets["optimized"] = self._scan_directory(
                self.optimized_data_dir, "优化数据 (5.5GB)"
            )
        
        return datasets
    
    def _scan_directory(self, directory, description):
        """扫描目录获取数据集信息"""
        info = {
            "description": description,
            "categories": {},
            "total_files": 0,
            "total_size_gb": 0
        }
        
        for category_dir in directory.iterdir():
            if not category_dir.is_dir():
                continue
                
            category_info = {
                "apis": {},
                "file_count": 0,
                "size_gb": 0
            }
            
            for api_dir in category_dir.iterdir():
                if not api_dir.is_dir():
                    continue
                    
                csv_files = list(api_dir.glob("*.csv"))
                if not csv_files:
                    continue
                    
                api_size = sum(f.stat().st_size for f in csv_files) / 1024 / 1024 / 1024
                
                category_info["apis"][api_dir.name] = {
                    "files": len(csv_files),
                    "size_gb": round(api_size, 2),
                    "files_list": [f.name for f in csv_files[:5]]  # 只显示前5个
                }
                category_info["file_count"] += len(csv_files)
                category_info["size_gb"] += api_size
            
            if category_info["file_count"] > 0:
                info["categories"][category_dir.name] = category_info
                info["total_files"] += category_info["file_count"]
                info["total_size_gb"] += category_info["size_gb"]
        
        return info
    
    def read_data(self, dataset="comprehensive", category=None, api=None, 
                  filename=None, max_rows=None, use_chunks=False, chunk_size=10000):
        """读取CSV数据"""
        
        logging.info(f"📊 查询参数: dataset={dataset}, category={category}, api={api}, file={filename}")
        
        # 确定数据目录
        if dataset == "comprehensive":
            base_dir = self.main_data_dir
        elif dataset == "optimized":
            base_dir = self.optimized_data_dir
        else:
            logging.error("❌ 无效的数据集名称，请使用 'comprehensive' 或 'optimized'")
            return None
        
        if not base_dir.exists():
            logging.error(f"❌ 数据目录不存在: {base_dir}")
            return None
        
        # 构建文件路径
        if not (category and api):
            logging.error("❌ 请提供category和api参数")
            return None
            
        api_dir = base_dir / category / api
        
        if not api_dir.exists():
            logging.error(f"❌ API目录不存在: {api_dir}")
            return None
            
        if filename:
            file_path = api_dir / filename
            if not file_path.suffix:
                file_path = file_path.with_suffix('.csv')
        else:
            # 列出API下的所有文件
            csv_files = list(api_dir.glob("*.csv"))
            if csv_files:
                file_path = csv_files[0]  # 取第一个文件
                logging.info(f"📁 找到 {len(csv_files)} 个文件，加载: {file_path.name}")
            else:
                logging.error(f"❌ 未找到CSV文件: {api_dir}")
                return None
        
        # 读取数据
        try:
            if not file_path.exists():
                logging.error(f"❌ 文件不存在: {file_path}")
                return None
            
            logging.info(f"📖 读取文件: {file_path}")
            
            # 获取文件信息
            file_size = file_path.stat().st_size / 1024 / 1024
            logging.info(f"📊 文件大小: {file_size:.2f} MB")
            
            # 根据文件大小选择读取策略
            if use_chunks or file_size > 500:  # 大于500MB使用分块读取
                logging.info(f"📋 使用分块读取 (chunk_size={chunk_size:,})")
                
                chunk_list = []
                total_rows = 0
                
                for i, chunk in enumerate(pd.read_csv(file_path, chunksize=chunk_size, low_memory=False)):
                    chunk_list.append(chunk)
                    total_rows += len(chunk)
                    
                    if max_rows and total_rows >= max_rows:
                        # 截取到指定行数
                        if len(chunk_list) > 1:
                            df = pd.concat(chunk_list, ignore_index=True)
                        else:
                            df = chunk_list[0]
                        df = df.head(max_rows)
                        logging.info(f"📋 已加载 {len(df):,} 行数据 (分块读取)")
                        break
                        
                    if i >= 10:  # 最多读取10个chunk
                        logging.info(f"📋 已读取10个chunk，继续合并...")
                        break
                
                if not chunk_list:
                    return None
                    
                if len(chunk_list) == 1:
                    df = chunk_list[0]
                else:
                    df = pd.concat(chunk_list, ignore_index=True)
                    
            else:
                # 普通读取
                df = pd.read_csv(file_path, low_memory=False)
                
                if max_rows and len(df) > max_rows:
                    df = df.head(max_rows)
                    logging.info(f"📋 已加载前 {max_rows:,} 行数据 (总计 {pd.read_csv(file_path, low_memory=False).shape[0]:,} 行)")
                else:
                    logging.info(f"📋 已加载全部 {len(df):,} 行数据")
            
            if not df.empty:
                logging.info(f"🔍 数据维度: {df.shape}")
                logging.info(f"📝 列名: {list(df.columns)[:10]}{'...' if len(df.columns) > 10 else ''}")
                
                # 显示数据预览
                print("\n📄 **数据预览**:")
                print(df.head().to_string())
                
                # 显示数据类型信息
                print(f"\n📊 **数据类型**:")
                for col, dtype in df.dtypes.head(10).items():
                    print(f"  {col}: {dtype}")
                if len(df.dtypes) > 10:
                    print(f"  ... 还有 {len(df.dtypes) - 10} 个字段")
            
            return df
            
        except Exception as e:
            logging.error(f"❌ 读取数据失败: {e}")
            return None
    
    def list_categories(self, dataset="comprehensive"):
        """列出可用的数据类别"""
        datasets = self.get_available_datasets()
        
        if dataset not in datasets:
            logging.error(f"❌ 数据集 '{dataset}' 不存在")
            return []
        
        return list(datasets[dataset]["categories"].keys())
    
    def list_apis(self, dataset="comprehensive", category=None):
        """列出指定类别下的API"""
        datasets = self.get_available_datasets()
        
        if dataset not in datasets:
            logging.error(f"❌ 数据集 '{dataset}' 不存在")
            return []
            
        if category not in datasets[dataset]["categories"]:
            logging.error(f"❌ 类别 '{category}' 不存在")
            return []
        
        return list(datasets[dataset]["categories"][category]["apis"].keys())
    
    def list_files(self, dataset="comprehensive", category=None, api=None):
        """列出指定API下的文件"""
        if dataset == "comprehensive":
            base_dir = self.main_data_dir
        elif dataset == "optimized":
            base_dir = self.optimized_data_dir
        else:
            logging.error("❌ 无效的数据集名称")
            return []
        
        if not (category and api):
            logging.error("❌ 请提供category和api参数")
            return []
            
        api_dir = base_dir / category / api
        
        if not api_dir.exists():
            return []
            
        return [f.name for f in api_dir.glob("*.csv")]
    
    def show_catalog(self):
        """显示数据目录"""
        print("📚 **CSV数据目录**")
        print("=" * 60)
        
        datasets = self.get_available_datasets()
        
        total_files = 0
        total_size = 0
        
        for dataset_name, dataset_info in datasets.items():
            print(f"\n📦 **{dataset_info['description']}** ({dataset_name})")
            print(f"  📁 总文件: {dataset_info['total_files']:,}")
            print(f"  💾 总大小: {dataset_info['total_size_gb']:.1f} GB")
            
            for category, category_info in dataset_info["categories"].items():
                print(f"\n  📂 {category} ({category_info['file_count']} 文件, {category_info['size_gb']:.1f} GB)")
                
                # 显示前几个API
                api_items = list(category_info["apis"].items())[:5]
                for api_name, api_info in api_items:
                    print(f"    🔌 {api_name}: {api_info['files']} 文件, {api_info['size_gb']:.1f} GB")
                
                if len(category_info["apis"]) > 5:
                    print(f"    ... 还有 {len(category_info['apis']) - 5} 个API")
            
            total_files += dataset_info['total_files']
            total_size += dataset_info['total_size_gb']
        
        print(f"\n🎯 **总计**: {total_files:,} 个文件, {total_size:.1f} GB")

def main():
    """主函数 - 演示CSV数据读取功能"""
    reader = CSVDataReader()
    
    print("🔍 **CSV数据读取工具演示**\n")
    
    # 1. 显示数据目录
    reader.show_catalog()
    
    # 2. 查询样本数据
    print("\n" + "="*60)
    print("🧪 **样本数据查询演示**:")
    
    sample_df = reader.read_data(
        dataset="comprehensive",
        category="financial", 
        api="fdmtindipsget",
        max_rows=10
    )
    
    print("\n🎊 CSV数据查询系统演示完成！")
    print("💡 提示: 使用CSV格式可以确保数据的原始完整性和兼容性")

if __name__ == "__main__":
    main()