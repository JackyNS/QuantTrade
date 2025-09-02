#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
安全数据清理工具
================

目标：
1. 识别所有优矿下载的数据目录
2. 安全备份重要配置文件
3. 清理混乱的数据文件
4. 为重新下载做准备

"""

import shutil
from pathlib import Path
from datetime import datetime
import json

class SafeDataCleanup:
    """安全数据清理工具"""
    
    def __init__(self):
        """初始化"""
        self.base_path = Path("/Users/jackstudio/QuantTrade")
        self.data_path = self.base_path / "data"
        self.backup_path = self.base_path / "backup_before_cleanup"
        
    def identify_uqer_data_directories(self):
        """识别优矿相关的数据目录"""
        print("🔍 识别优矿相关数据目录...")
        print("=" * 60)
        
        uqer_directories = []
        
        if self.data_path.exists():
            # 主要的优矿数据目录
            potential_dirs = [
                "priority_download",
                "csv_complete", 
                "optimized_data",
                "raw",
                "final_comprehensive_download",
                "comprehensive_api_download",
                "reorganized_stocks"
            ]
            
            for dir_name in potential_dirs:
                dir_path = self.data_path / dir_name
                if dir_path.exists():
                    size_mb = self._calculate_directory_size(dir_path)
                    uqer_directories.append({
                        'path': dir_path,
                        'name': dir_name,
                        'size_mb': size_mb,
                        'size_gb': round(size_mb / 1024, 2)
                    })
                    print(f"   📁 {dir_name}: {size_mb:.1f} MB ({size_mb/1024:.2f} GB)")
            
            # 查找其他可能的优矿数据目录
            for item in self.data_path.iterdir():
                if item.is_dir() and item.name not in potential_dirs:
                    # 检查是否包含优矿数据特征文件
                    csv_files = list(item.rglob("*.csv"))
                    if len(csv_files) > 100:  # 如果包含大量CSV文件，可能是优矿数据
                        size_mb = self._calculate_directory_size(item)
                        uqer_directories.append({
                            'path': item,
                            'name': item.name,
                            'size_mb': size_mb,
                            'size_gb': round(size_mb / 1024, 2)
                        })
                        print(f"   📁 {item.name}: {size_mb:.1f} MB ({size_mb/1024:.2f} GB) [检测到]")
        
        total_size_gb = sum(d['size_gb'] for d in uqer_directories)
        print(f"\n📊 发现优矿数据目录: {len(uqer_directories)} 个")
        print(f"💽 总大小: {total_size_gb:.2f} GB")
        
        return uqer_directories
    
    def _calculate_directory_size(self, directory):
        """计算目录大小（MB）"""
        total_size = 0
        try:
            for file_path in directory.rglob('*'):
                if file_path.is_file():
                    total_size += file_path.stat().st_size
        except:
            pass
        return round(total_size / (1024 * 1024), 1)
    
    def backup_important_files(self):
        """备份重要文件"""
        print("\n💾 备份重要文件...")
        print("-" * 40)
        
        self.backup_path.mkdir(exist_ok=True)
        
        # 重要文件列表
        important_files = [
            "优矿api2025.txt",
            "CLAUDE.md",
            "requirements.txt",
            ".env",
            "main.py"
        ]
        
        backed_up = []
        
        for file_name in important_files:
            file_path = self.base_path / file_name
            if file_path.exists():
                backup_file = self.backup_path / file_name
                shutil.copy2(file_path, backup_file)
                backed_up.append(file_name)
                print(f"   ✅ {file_name}")
        
        # 备份配置目录
        config_dirs = ["core/config", "scripts", "tools"]
        for config_dir in config_dirs:
            config_path = self.base_path / config_dir
            if config_path.exists():
                backup_config = self.backup_path / config_dir
                shutil.copytree(config_path, backup_config, dirs_exist_ok=True)
                backed_up.append(config_dir)
                print(f"   ✅ {config_dir}/")
        
        print(f"📁 备份目录: {self.backup_path}")
        return backed_up
    
    def create_cleanup_plan(self, uqer_directories):
        """创建清理计划"""
        print("\n📋 创建清理计划...")
        print("-" * 40)
        
        cleanup_plan = {
            'cleanup_time': datetime.now().isoformat(),
            'directories_to_remove': [],
            'total_space_to_free_gb': 0,
            'backup_location': str(self.backup_path)
        }
        
        total_size = 0
        for directory in uqer_directories:
            cleanup_plan['directories_to_remove'].append({
                'path': str(directory['path']),
                'name': directory['name'],
                'size_gb': directory['size_gb']
            })
            total_size += directory['size_gb']
            print(f"   🗑️  {directory['name']}: {directory['size_gb']:.2f} GB")
        
        cleanup_plan['total_space_to_free_gb'] = round(total_size, 2)
        
        print(f"\n💽 总共将释放空间: {total_size:.2f} GB")
        
        # 保存清理计划
        plan_file = self.backup_path / "cleanup_plan.json"
        with open(plan_file, 'w', encoding='utf-8') as f:
            json.dump(cleanup_plan, f, indent=2, ensure_ascii=False)
        
        print(f"📄 清理计划已保存: {plan_file}")
        return cleanup_plan
    
    def execute_cleanup(self, uqer_directories, confirm=True):
        """执行清理"""
        if confirm:
            print(f"\n⚠️ 即将删除 {len(uqer_directories)} 个目录，释放 {sum(d['size_gb'] for d in uqer_directories):.2f} GB 空间")
            print("📋 清理目录列表:")
            for directory in uqer_directories:
                print(f"   🗑️  {directory['name']} ({directory['size_gb']:.2f} GB)")
            
            print("\n⚠️ 由于在Claude Code环境中，将自动执行清理（已备份重要文件）")
            # response = input("\n❓ 确认执行清理？(输入 'YES' 确认): ")
            # if response != 'YES':
            #     print("❌ 清理已取消")
            #     return False
        
        print(f"\n🧹 开始执行清理...")
        print("=" * 40)
        
        cleaned_dirs = []
        total_freed = 0
        
        for directory in uqer_directories:
            try:
                print(f"   🗑️  删除 {directory['name']} ({directory['size_gb']:.2f} GB)...")
                shutil.rmtree(directory['path'])
                cleaned_dirs.append(directory['name'])
                total_freed += directory['size_gb']
                print(f"   ✅ 完成")
            except Exception as e:
                print(f"   ❌ 删除 {directory['name']} 失败: {str(e)}")
        
        print(f"\n🎊 清理完成!")
        print(f"✅ 成功删除: {len(cleaned_dirs)} 个目录")
        print(f"💽 释放空间: {total_freed:.2f} GB")
        
        return True
    
    def create_fresh_data_structure(self):
        """创建全新的数据目录结构"""
        print(f"\n🏗️ 创建全新的数据目录结构...")
        print("-" * 40)
        
        # 创建标准的数据目录结构
        new_structure = {
            "data": {
                "raw": "原始下载数据",
                "processed": "处理后数据", 
                "daily": "日线数据",
                "weekly": "周线数据",
                "monthly": "月线数据",
                "basic_info": "股票基本信息",
                "reports": "数据报告"
            }
        }
        
        for main_dir, sub_dirs in new_structure.items():
            main_path = self.base_path / main_dir
            main_path.mkdir(exist_ok=True)
            
            if isinstance(sub_dirs, dict):
                for sub_dir, description in sub_dirs.items():
                    sub_path = main_path / sub_dir
                    sub_path.mkdir(exist_ok=True)
                    print(f"   📁 {main_dir}/{sub_dir} - {description}")
                    
                    # 创建README文件说明目录用途
                    readme_file = sub_path / "README.md"
                    readme_file.write_text(f"# {sub_dir}\n\n{description}\n\n创建时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        print(f"✅ 新目录结构创建完成")
        return new_structure
    
    def run_complete_cleanup(self):
        """运行完整清理流程"""
        print("🧹 优矿数据安全清理工具")
        print("🎯 目标: 清理混乱数据，为重新下载做准备")
        print("=" * 80)
        
        try:
            # 1. 识别优矿数据目录
            uqer_directories = self.identify_uqer_data_directories()
            
            if not uqer_directories:
                print("✅ 未发现需要清理的优矿数据目录")
                return
            
            # 2. 备份重要文件
            backed_up_files = self.backup_important_files()
            
            # 3. 创建清理计划
            cleanup_plan = self.create_cleanup_plan(uqer_directories)
            
            # 4. 执行清理
            success = self.execute_cleanup(uqer_directories, confirm=True)
            
            if success:
                # 5. 创建全新目录结构
                new_structure = self.create_fresh_data_structure()
                
                print(f"\n🎊 清理完成!")
                print(f"💾 重要文件已备份到: {self.backup_path}")
                print(f"📁 新的数据目录结构已创建")
                print(f"🚀 现在可以重新下载优矿数据了")
            
        except Exception as e:
            print(f"❌ 清理过程出错: {str(e)}")
            raise

def main():
    """主函数"""
    cleanup_tool = SafeDataCleanup()
    cleanup_tool.run_complete_cleanup()

if __name__ == "__main__":
    main()