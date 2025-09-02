#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据质量验证工具 - 验证已下载数据的完整性和质量
"""

import pandas as pd
from pathlib import Path
import json
import logging
from datetime import datetime
import os

class DataQualityValidator:
    """数据质量验证器"""
    
    def __init__(self):
        self.data_dir = Path("data/final_comprehensive_download")
        self.setup_logging()
        
    def setup_logging(self):
        """设置日志"""
        log_file = self.data_dir / "data_quality_validation.log"
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
    
    def scan_data_structure(self):
        """扫描数据结构"""
        logging.info("🔍 开始数据结构扫描...")
        
        categories = ["basic_info", "financial", "special_trading", "governance"]
        structure = {}
        total_files = 0
        total_size = 0
        
        for category in categories:
            category_dir = self.data_dir / category
            if not category_dir.exists():
                logging.warning(f"❌ 类别目录不存在: {category}")
                continue
                
            category_info = {
                "apis": {},
                "file_count": 0,
                "total_size": 0
            }
            
            # 扫描每个API目录
            for api_dir in category_dir.iterdir():
                if not api_dir.is_dir():
                    continue
                    
                api_info = {
                    "files": [],
                    "file_count": 0,
                    "total_size": 0,
                    "record_count": 0
                }
                
                # 扫描API目录中的CSV文件
                for csv_file in api_dir.glob("*.csv"):
                    file_size = csv_file.stat().st_size
                    api_info["files"].append({
                        "name": csv_file.name,
                        "size": file_size,
                        "size_mb": round(file_size / 1024 / 1024, 2)
                    })
                    api_info["file_count"] += 1
                    api_info["total_size"] += file_size
                    total_files += 1
                    total_size += file_size
                
                category_info["apis"][api_dir.name] = api_info
                category_info["file_count"] += api_info["file_count"]
                category_info["total_size"] += api_info["total_size"]
            
            structure[category] = category_info
            logging.info(f"✅ {category}: {category_info['file_count']} 文件, "
                        f"{round(category_info['total_size']/1024/1024/1024, 2)} GB")
        
        logging.info(f"📊 总计: {total_files} 文件, {round(total_size/1024/1024/1024, 2)} GB")
        return structure, total_files, total_size
    
    def validate_sample_data(self):
        """验证样本数据质量"""
        logging.info("🧪 开始样本数据验证...")
        
        validation_results = {
            "empty_files": [],
            "corrupted_files": [],
            "sample_validations": [],
            "total_records_estimated": 0
        }
        
        categories = ["basic_info", "financial", "special_trading", "governance"]
        sample_count = 0
        
        for category in categories:
            category_dir = self.data_dir / category
            if not category_dir.exists():
                continue
                
            # 从每个类别随机选择几个文件进行验证
            csv_files = list(category_dir.rglob("*.csv"))
            if not csv_files:
                continue
                
            # 选择前几个文件进行详细验证
            sample_files = csv_files[:min(3, len(csv_files))]
            
            for csv_file in sample_files:
                try:
                    df = pd.read_csv(csv_file)
                    sample_count += 1
                    
                    validation = {
                        "file": str(csv_file.relative_to(self.data_dir)),
                        "rows": len(df),
                        "columns": len(df.columns),
                        "file_size_mb": round(csv_file.stat().st_size / 1024 / 1024, 2),
                        "empty_ratio": df.isnull().sum().sum() / (len(df) * len(df.columns)) if len(df) > 0 else 1,
                        "column_names": list(df.columns)[:10],  # 前10个列名
                        "status": "valid"
                    }
                    
                    if len(df) == 0:
                        validation_results["empty_files"].append(str(csv_file.relative_to(self.data_dir)))
                        validation["status"] = "empty"
                    
                    validation_results["sample_validations"].append(validation)
                    validation_results["total_records_estimated"] += len(df) * (len(csv_files) / len(sample_files))
                    
                    if sample_count <= 5:  # 只显示前5个详细信息
                        logging.info(f"✅ {validation['file']}: {validation['rows']:,} 行, "
                                   f"{validation['columns']} 列, {validation['file_size_mb']} MB")
                    
                except Exception as e:
                    validation_results["corrupted_files"].append({
                        "file": str(csv_file.relative_to(self.data_dir)),
                        "error": str(e)
                    })
                    logging.error(f"❌ 文件验证失败: {csv_file.relative_to(self.data_dir)} - {e}")
        
        logging.info(f"📊 样本验证完成: {len(validation_results['sample_validations'])} 个有效文件")
        logging.info(f"📊 估计总记录数: {validation_results['total_records_estimated']:,.0f}")
        
        return validation_results
    
    def check_api_coverage(self):
        """检查API覆盖率"""
        logging.info("📋 检查API覆盖率...")
        
        # 读取进度文件
        progress_file = self.data_dir / "download_progress.json"
        if progress_file.exists():
            with open(progress_file, 'r', encoding='utf-8') as f:
                progress_data = json.load(f)
            
            completed_apis = progress_data.get("completed_apis", [])
            total_records = progress_data.get("statistics", {}).get("total_records", 0)
            
            logging.info(f"✅ API覆盖率: {len(completed_apis)} 个API完成")
            logging.info(f"📊 总记录数: {total_records:,}")
            
            # 按类别统计
            category_stats = {}
            for api in completed_apis:
                category = api.split('_')[0] if '_' in api else 'unknown'
                category_stats[category] = category_stats.get(category, 0) + 1
            
            for category, count in category_stats.items():
                logging.info(f"  - {category}: {count} 个API")
            
            return {
                "total_apis": len(completed_apis),
                "total_records": total_records,
                "category_stats": category_stats
            }
        else:
            logging.warning("❌ 未找到进度文件")
            return None
    
    def generate_quality_report(self):
        """生成数据质量报告"""
        logging.info("📝 生成数据质量报告...")
        
        # 收集所有验证结果
        structure, total_files, total_size = self.scan_data_structure()
        validation_results = self.validate_sample_data()
        api_coverage = self.check_api_coverage()
        
        # 生成报告
        report = {
            "generation_time": datetime.now().isoformat(),
            "summary": {
                "total_files": total_files,
                "total_size_gb": round(total_size / 1024 / 1024 / 1024, 2),
                "total_apis": api_coverage["total_apis"] if api_coverage else 0,
                "total_records": api_coverage["total_records"] if api_coverage else 0,
                "estimated_records_from_sample": int(validation_results["total_records_estimated"])
            },
            "data_structure": structure,
            "quality_validation": validation_results,
            "api_coverage": api_coverage,
            "issues": {
                "empty_files": len(validation_results["empty_files"]),
                "corrupted_files": len(validation_results["corrupted_files"])
            }
        }
        
        # 保存报告
        report_file = self.data_dir / "data_quality_report.json"
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        logging.info(f"📊 质量报告已保存: {report_file}")
        
        # 打印摘要
        logging.info("🎊 数据质量验证完成!")
        logging.info("=" * 50)
        logging.info("📊 **最终数据摘要**")
        logging.info(f"  📁 总文件数: {report['summary']['total_files']:,}")
        logging.info(f"  💾 总数据量: {report['summary']['total_size_gb']} GB")
        logging.info(f"  🔌 API数量: {report['summary']['total_apis']}")
        logging.info(f"  📝 总记录数: {report['summary']['total_records']:,}")
        
        if api_coverage:
            logging.info("\n📋 **API分类统计**:")
            for category, count in api_coverage["category_stats"].items():
                logging.info(f"  - {category}: {count} 个API")
        
        if validation_results["empty_files"]:
            logging.info(f"\n⚠️  **发现问题**: {len(validation_results['empty_files'])} 个空文件")
        if validation_results["corrupted_files"]:
            logging.info(f"⚠️  **发现问题**: {len(validation_results['corrupted_files'])} 个损坏文件")
        
        if not validation_results["empty_files"] and not validation_results["corrupted_files"]:
            logging.info("\n✅ **数据质量**: 优秀，无发现重大问题")
        
        return report

if __name__ == "__main__":
    validator = DataQualityValidator()
    validator.generate_quality_report()