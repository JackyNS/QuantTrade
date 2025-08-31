#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据备份脚本
============

备份重要数据和配置文件

Author: QuantTrader Team
Date: 2025-08-31
"""

from typing import Dict, List


import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import shutil
import zipfile
from pathlib import Path
from datetime import datetime
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BackupManager:
    """备份管理器"""
    
    def __init__(self, backup_dir: str = "./backups"):
        self.backup_dir = Path(backup_dir)
        self.backup_dir.mkdir(parents=True, exist_ok=True)
        
        # 定义需要备份的目录
        self.backup_targets = {
            'data': './data',
            'config': './config',
            'logs': './logs',
            'reports': './data/reports'
        }
    
    def create_backup(self, name: str = None) -> str:
        """创建备份"""
        if not name:
            name = f"backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        backup_path = self.backup_dir / f"{name}.zip"
        
        logger.info(f"开始创建备份: {name}")
        
        with zipfile.ZipFile(backup_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for target_name, target_path in self.backup_targets.items():
                if Path(target_path).exists():
                    self._add_to_zip(zipf, target_path, target_name)
                    logger.info(f"  已备份: {target_name}")
        
        # 创建备份元数据
        metadata = {
            'name': name,
            'created_at': datetime.now().isoformat(),
            'size': backup_path.stat().st_size,
            'targets': list(self.backup_targets.keys())
        }
        
        metadata_file = self.backup_dir / f"{name}.json"
        with open(metadata_file, 'w') as f:
            json.dump(metadata, f, indent=2)
        
        logger.info(f"备份完成: {backup_path}")
        logger.info(f"备份大小: {backup_path.stat().st_size / 1024 / 1024:.2f} MB")
        
        return str(backup_path)
    
    def _add_to_zip(self, zipf: zipfile.ZipFile, path: str, arcname: str):
        """添加文件或目录到ZIP"""
        path = Path(path)
        
        if path.is_file():
            zipf.write(path, arcname)
        elif path.is_dir():
            for item in path.rglob('*'):
                if item.is_file():
                    # 计算相对路径
                    rel_path = item.relative_to(path.parent)
                    zipf.write(item, rel_path)
    
    def restore_backup(self, backup_file: str, target_dir: str = "./restore"):
        """恢复备份"""
        backup_path = Path(backup_file)
        
        if not backup_path.exists():
            logger.error(f"备份文件不存在: {backup_file}")
            return False
        
        target_path = Path(target_dir)
        target_path.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"开始恢复备份: {backup_file}")
        
        try:
            with zipfile.ZipFile(backup_path, 'r') as zipf:
                zipf.extractall(target_path)
            
            logger.info(f"备份已恢复到: {target_path}")
            return True
            
        except Exception as e:
            logger.error(f"恢复失败: {e}")
            return False
    
    def list_backups(self) -> List[Dict]:
        """列出所有备份"""
        backups = []
        
        for json_file in self.backup_dir.glob("*.json"):
            with open(json_file, 'r') as f:
                metadata = json.load(f)
                backups.append(metadata)
        
        # 按时间排序
        backups.sort(key=lambda x: x['created_at'], reverse=True)
        
        return backups
    
    def clean_old_backups(self, keep_days: int = 30):
        """清理旧备份"""
        cutoff_date = datetime.now().timestamp() - (keep_days * 24 * 3600)
        
        removed_count = 0
        
        for backup_file in self.backup_dir.glob("*.zip"):
            if backup_file.stat().st_mtime < cutoff_date:
                # 删除备份文件和元数据
                backup_file.unlink()
                
                json_file = backup_file.with_suffix('.json')
                if json_file.exists():
                    json_file.unlink()
                
                removed_count += 1
                logger.info(f"删除旧备份: {backup_file.name}")
        
        if removed_count > 0:
            logger.info(f"共删除 {removed_count} 个旧备份")
        
        return removed_count
    
    def get_backup_info(self, backup_name: str) -> Dict:
        """获取备份信息"""
        metadata_file = self.backup_dir / f"{backup_name}.json"
        
        if metadata_file.exists():
            with open(metadata_file, 'r') as f:
                return json.load(f)
        
        return None

def main():
    backup_manager = BackupManager()
    
    # 创建备份
    backup_file = backup_manager.create_backup("daily_backup")
    
    # 列出所有备份
    backups = backup_manager.list_backups()
    print(f"\n现有备份数: {len(backups)}")
    
    for backup in backups[:5]:  # 显示最近5个
        print(f"  - {backup['name']}: {backup['size']/1024/1024:.2f} MB, {backup['created_at']}")
    
    # 清理旧备份
    removed = backup_manager.clean_old_backups(keep_days=30)
    
    if removed > 0:
        print(f"\n清理了 {removed} 个旧备份")

if __name__ == "__main__":
    main()