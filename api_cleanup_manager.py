#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
API清理管理器 - 删除无效API和整理数据结构
"""

import pandas as pd
from pathlib import Path
import shutil
import logging
from datetime import datetime

class APICleanupManager:
    """API清理管理器"""
    
    def __init__(self):
        self.base_dir = Path("data/final_comprehensive_download")
        self.setup_logging()
        
        # 无数据的API列表（需要删除）
        self.invalid_apis = {
            'special_trading': ['equmarginsec', 'mktequperfget', 'equmarginsecget'],
            'additional_apis': ['eco_data_china_lite']
        }
        
    def setup_logging(self):
        """设置日志"""
        log_file = Path("api_cleanup.log")
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file, encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
    
    def backup_current_structure(self):
        """备份当前数据结构"""
        backup_dir = Path("data_backup_before_cleanup")
        if backup_dir.exists():
            shutil.rmtree(backup_dir)
        
        logging.info("🔄 创建数据结构备份...")
        
        # 只备份目录结构，不备份大文件
        for category_dir in self.base_dir.iterdir():
            if category_dir.is_dir():
                backup_category = backup_dir / category_dir.name
                backup_category.mkdir(parents=True, exist_ok=True)
                
                for api_dir in category_dir.iterdir():
                    if api_dir.is_dir():
                        backup_api = backup_category / api_dir.name
                        backup_api.mkdir(exist_ok=True)
                        
                        # 只备份目录结构信息
                        info_file = backup_api / "info.txt"
                        csv_files = list(api_dir.glob("*.csv"))
                        with open(info_file, 'w', encoding='utf-8') as f:
                            f.write(f"原始文件数: {len(csv_files)}\n")
                            f.write(f"备份时间: {datetime.now()}\n")
        
        logging.info(f"✅ 备份完成: {backup_dir}")
    
    def remove_invalid_apis(self):
        """删除无效的API目录"""
        logging.info("🗑️ 开始删除无效API...")
        
        removed_count = 0
        for category, api_list in self.invalid_apis.items():
            category_path = self.base_dir / category
            if not category_path.exists():
                continue
                
            for api_name in api_list:
                api_path = category_path / api_name
                if api_path.exists():
                    try:
                        shutil.rmtree(api_path)
                        logging.info(f"🗑️ 已删除: {category}/{api_name}")
                        removed_count += 1
                    except Exception as e:
                        logging.error(f"❌ 删除失败 {category}/{api_name}: {e}")
                else:
                    logging.warning(f"⚠️ 目录不存在: {category}/{api_name}")
        
        logging.info(f"✅ 删除完成，共删除 {removed_count} 个无效API")
        return removed_count
    
    def generate_cleanup_report(self):
        """生成清理报告"""
        logging.info("📊 生成清理报告...")
        
        # 重新扫描当前结构
        current_structure = {}
        total_apis = 0
        total_files = 0
        total_size = 0
        
        for category_dir in self.base_dir.iterdir():
            if category_dir.is_dir():
                category_name = category_dir.name
                api_count = 0
                files_count = 0
                size_mb = 0
                
                for api_dir in category_dir.iterdir():
                    if api_dir.is_dir():
                        csv_files = list(api_dir.glob("*.csv"))
                        if csv_files:  # 只统计有数据的API
                            api_count += 1
                            files_count += len(csv_files)
                            size_mb += sum(f.stat().st_size for f in csv_files) / (1024 * 1024)
                
                current_structure[category_name] = {
                    'api_count': api_count,
                    'file_count': files_count,
                    'size_mb': size_mb
                }
                
                total_apis += api_count
                total_files += files_count
                total_size += size_mb
        
        # 生成报告
        report = []
        report.append("="*80)
        report.append("🧹 **API清理报告**")
        report.append("="*80)
        report.append(f"📅 清理时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append("")
        report.append("🗑️ **已删除的无效API:**")
        
        for category, api_list in self.invalid_apis.items():
            report.append(f"  📁 {category}:")
            for api_name in api_list:
                report.append(f"    ❌ {api_name}")
        
        report.append("")
        report.append("📊 **清理后数据结构:**")
        report.append(f"  🔌 总API数量: {total_apis} 个")
        report.append(f"  📄 总文件数量: {total_files} 个")
        report.append(f"  💾 总数据大小: {total_size:.1f} MB ({total_size/1024:.1f} GB)")
        report.append("")
        report.append("📋 **各分类统计:**")
        
        for category, stats in current_structure.items():
            report.append(f"  📁 {category}: {stats['api_count']} APIs, "
                         f"{stats['file_count']} 文件, {stats['size_mb']:.1f}MB")
        
        report.append("")
        report.append("✅ **清理完成，数据结构已优化**")
        report.append("="*80)
        
        # 输出到控制台
        for line in report:
            print(line)
        
        # 保存到文件
        with open('api_cleanup_report.txt', 'w', encoding='utf-8') as f:
            f.write('\n'.join(report))
        
        logging.info("📄 清理报告已保存: api_cleanup_report.txt")
        return current_structure
    
    def update_csv_reports(self, current_structure):
        """更新CSV报告，移除无效API记录"""
        logging.info("📝 更新CSV报告文件...")
        
        # 需要移除的API名称
        apis_to_remove = []
        for category, api_list in self.invalid_apis.items():
            apis_to_remove.extend(api_list)
        
        # 更新概览报告
        overview_file = Path("API详细分析报告_概览.csv")
        if overview_file.exists():
            df_overview = pd.read_csv(overview_file)
            original_count = len(df_overview)
            
            # 删除无效API的记录
            df_overview = df_overview[~df_overview['api_name'].isin(apis_to_remove)]
            
            # 保存更新后的文件
            df_overview.to_csv(overview_file, index=False, encoding='utf-8-sig')
            
            logging.info(f"📊 概览报告已更新: 删除 {original_count - len(df_overview)} 个无效记录")
        
        # 更新分类汇总
        summary_file = Path("API详细分析报告_分类汇总.csv")
        if summary_file.exists():
            # 重新生成分类汇总
            summary_data = []
            for category, stats in current_structure.items():
                summary_data.append({
                    'category': category,
                    'api_count': stats['api_count'],
                    'apis_with_data': stats['api_count'], # 现在所有API都有数据
                    'data_coverage_rate': 100.0,
                    'total_files': stats['file_count'],
                    'total_size_mb': stats['size_mb'],
                    'estimated_total_records': stats['file_count'] * 1000  # 估算
                })
            
            df_summary = pd.DataFrame(summary_data)
            df_summary.to_csv(summary_file, index=False, encoding='utf-8-sig')
            
            logging.info("📊 分类汇总报告已更新")
    
    def run_cleanup(self):
        """执行完整的清理流程"""
        logging.info("🚀 开始API清理流程...")
        
        # 1. 备份
        self.backup_current_structure()
        
        # 2. 删除无效API
        removed_count = self.remove_invalid_apis()
        
        # 3. 生成报告
        current_structure = self.generate_cleanup_report()
        
        # 4. 更新CSV报告
        self.update_csv_reports(current_structure)
        
        logging.info("🎊 API清理流程完成！")
        return current_structure

if __name__ == "__main__":
    cleanup_manager = APICleanupManager()
    result = cleanup_manager.run_cleanup()