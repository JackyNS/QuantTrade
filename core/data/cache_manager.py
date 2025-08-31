#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
智能缓存管理器
=============

高性能、分层的数据缓存系统
"""

import os
import pickle
import json
import hashlib
import gzip
import shutil
from typing import Dict, List, Optional, Union, Any, Callable
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
import logging
import threading
from concurrent.futures import ThreadPoolExecutor
import sqlite3

logger = logging.getLogger(__name__)

class SmartCacheManager:
    """智能缓存管理器
    
    提供多层缓存策略：
    - 内存缓存 (最快)
    - 磁盘缓存 (持久化)
    - 压缩缓存 (节省空间)
    - SQLite缓存 (结构化数据)
    """
    
    def __init__(self, config: Optional[Dict] = None):
        """初始化缓存管理器
        
        Args:
            config: 配置参数
        """
        self.config = config or {}
        
        # 缓存配置
        self.cache_dir = Path(self.config.get('cache_dir', './cache'))
        self.max_memory_size = self.config.get('max_memory_size', 100 * 1024 * 1024)  # 100MB
        self.max_disk_size = self.config.get('max_disk_size', 1024 * 1024 * 1024)    # 1GB
        self.default_expire_hours = self.config.get('default_expire_hours', 24)
        self.compression_enabled = self.config.get('compression_enabled', True)
        self.cleanup_interval_hours = self.config.get('cleanup_interval_hours', 6)
        
        # 创建缓存目录
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        # 缓存子目录
        self.memory_cache_dir = self.cache_dir / 'memory'
        self.disk_cache_dir = self.cache_dir / 'disk'
        self.compressed_cache_dir = self.cache_dir / 'compressed'
        self.sqlite_cache_dir = self.cache_dir / 'sqlite'
        
        for dir_path in [self.memory_cache_dir, self.disk_cache_dir, 
                        self.compressed_cache_dir, self.sqlite_cache_dir]:
            dir_path.mkdir(parents=True, exist_ok=True)
        
        # 内存缓存
        self._memory_cache: Dict[str, Dict] = {}
        self._memory_usage = 0
        self._cache_lock = threading.RLock()
        
        # SQLite数据库连接
        self.db_path = self.sqlite_cache_dir / 'cache.db'
        self._init_sqlite_db()
        
        # 缓存统计
        self.stats = {
            'hits': 0,
            'misses': 0,
            'memory_hits': 0,
            'disk_hits': 0,
            'compressed_hits': 0,
            'sqlite_hits': 0
        }
        
        # 启动清理线程
        self._cleanup_thread = None
        self._start_cleanup_thread()
        
        logger.info(f"✅ 智能缓存管理器初始化完成，缓存目录: {self.cache_dir}")
    
    def _init_sqlite_db(self):
        """初始化SQLite数据库"""
        try:
            with sqlite3.connect(str(self.db_path)) as conn:
                conn.execute('''
                    CREATE TABLE IF NOT EXISTS cache_data (
                        key TEXT PRIMARY KEY,
                        data BLOB,
                        metadata TEXT,
                        created_time TIMESTAMP,
                        expire_time TIMESTAMP,
                        access_count INTEGER DEFAULT 0,
                        last_access TIMESTAMP
                    )
                ''')
                conn.execute('''
                    CREATE INDEX IF NOT EXISTS idx_expire_time ON cache_data(expire_time)
                ''')
                conn.execute('''
                    CREATE INDEX IF NOT EXISTS idx_last_access ON cache_data(last_access)
                ''')
                conn.commit()
                
            logger.info("✅ SQLite缓存数据库初始化完成")
            
        except Exception as e:
            logger.error(f"❌ SQLite数据库初始化失败: {str(e)}")
    
    def _generate_cache_key(self, 
                           data_type: str,
                           params: Dict[str, Any]) -> str:
        """生成缓存键
        
        Args:
            data_type: 数据类型
            params: 参数字典
            
        Returns:
            str: 缓存键
        """
        # 标准化参数
        sorted_params = sorted(params.items())
        param_str = json.dumps(sorted_params, sort_keys=True, default=str)
        
        # 生成哈希
        cache_key = f"{data_type}_{hashlib.md5(param_str.encode()).hexdigest()}"
        return cache_key
    
    def _get_cache_metadata(self, 
                           expire_hours: Optional[int] = None,
                           tags: Optional[List[str]] = None) -> Dict[str, Any]:
        """获取缓存元数据"""
        expire_hours = expire_hours or self.default_expire_hours
        expire_time = datetime.now() + timedelta(hours=expire_hours)
        
        return {
            'created_time': datetime.now(),
            'expire_time': expire_time,
            'tags': tags or [],
            'size_bytes': 0,
            'access_count': 0,
            'last_access': datetime.now()
        }
    
    def _serialize_data(self, data: Any) -> bytes:
        """序列化数据"""
        if isinstance(data, pd.DataFrame):
            # DataFrame使用pickle序列化
            return pickle.dumps(data)
        else:
            # 其他数据类型使用pickle
            return pickle.dumps(data)
    
    def _deserialize_data(self, data_bytes: bytes) -> Any:
        """反序列化数据"""
        return pickle.loads(data_bytes)
    
    def _compress_data(self, data_bytes: bytes) -> bytes:
        """压缩数据"""
        if self.compression_enabled:
            return gzip.compress(data_bytes)
        return data_bytes
    
    def _decompress_data(self, compressed_bytes: bytes) -> bytes:
        """解压缩数据"""
        if self.compression_enabled:
            try:
                return gzip.decompress(compressed_bytes)
            except:
                # 如果解压失败，可能是未压缩的数据
                return compressed_bytes
        return compressed_bytes
    
    def get(self, 
           data_type: str,
           params: Dict[str, Any],
           default: Any = None) -> Any:
        """获取缓存数据
        
        Args:
            data_type: 数据类型
            params: 参数字典
            default: 默认值
            
        Returns:
            Any: 缓存的数据，如果不存在返回default
        """
        cache_key = self._generate_cache_key(data_type, params)
        
        try:
            # 1. 尝试内存缓存
            result = self._get_from_memory(cache_key)
            if result is not None:
                self.stats['hits'] += 1
                self.stats['memory_hits'] += 1
                logger.debug(f"✅ 内存缓存命中: {cache_key}")
                return result
            
            # 2. 尝试磁盘缓存
            result = self._get_from_disk(cache_key)
            if result is not None:
                # 将数据加载到内存缓存
                self._put_to_memory(cache_key, result)
                self.stats['hits'] += 1
                self.stats['disk_hits'] += 1
                logger.debug(f"✅ 磁盘缓存命中: {cache_key}")
                return result
            
            # 3. 尝试压缩缓存
            result = self._get_from_compressed(cache_key)
            if result is not None:
                # 将数据加载到内存和磁盘缓存
                self._put_to_memory(cache_key, result)
                self._put_to_disk(cache_key, result)
                self.stats['hits'] += 1
                self.stats['compressed_hits'] += 1
                logger.debug(f"✅ 压缩缓存命中: {cache_key}")
                return result
            
            # 4. 尝试SQLite缓存
            result = self._get_from_sqlite(cache_key)
            if result is not None:
                # 将数据加载到上层缓存
                self._put_to_memory(cache_key, result)
                self._put_to_disk(cache_key, result)
                self.stats['hits'] += 1
                self.stats['sqlite_hits'] += 1
                logger.debug(f"✅ SQLite缓存命中: {cache_key}")
                return result
            
            # 所有缓存都未命中
            self.stats['misses'] += 1
            logger.debug(f"❌ 缓存未命中: {cache_key}")
            return default
            
        except Exception as e:
            logger.error(f"❌ 获取缓存失败: {str(e)}")
            return default
    
    def put(self, 
           data_type: str,
           params: Dict[str, Any],
           data: Any,
           expire_hours: Optional[int] = None,
           tags: Optional[List[str]] = None):
        """存储数据到缓存
        
        Args:
            data_type: 数据类型
            params: 参数字典
            data: 要缓存的数据
            expire_hours: 过期小时数
            tags: 标签列表
        """
        cache_key = self._generate_cache_key(data_type, params)
        metadata = self._get_cache_metadata(expire_hours, tags)
        
        try:
            # 计算数据大小
            serialized_data = self._serialize_data(data)
            data_size = len(serialized_data)
            metadata['size_bytes'] = data_size
            
            # 分层存储策略
            if data_size < 1024 * 1024:  # 小于1MB，存储到内存缓存
                self._put_to_memory(cache_key, data, metadata)
                
            if data_size < 10 * 1024 * 1024:  # 小于10MB，存储到磁盘缓存
                self._put_to_disk(cache_key, data, metadata)
            else:
                # 大数据使用压缩缓存
                self._put_to_compressed(cache_key, data, metadata)
            
            # 结构化数据存储到SQLite
            if isinstance(data, (pd.DataFrame, dict, list)):
                self._put_to_sqlite(cache_key, data, metadata)
            
            logger.debug(f"✅ 数据已缓存: {cache_key} ({data_size} bytes)")
            
        except Exception as e:
            logger.error(f"❌ 缓存存储失败: {str(e)}")
    
    def _get_from_memory(self, cache_key: str) -> Any:
        """从内存缓存获取数据"""
        with self._cache_lock:
            if cache_key in self._memory_cache:
                cache_item = self._memory_cache[cache_key]
                
                # 检查是否过期
                if datetime.now() > cache_item['metadata']['expire_time']:
                    del self._memory_cache[cache_key]
                    return None
                
                # 更新访问统计
                cache_item['metadata']['access_count'] += 1
                cache_item['metadata']['last_access'] = datetime.now()
                
                return cache_item['data']
            
            return None
    
    def _put_to_memory(self, 
                      cache_key: str, 
                      data: Any,
                      metadata: Optional[Dict] = None):
        """存储数据到内存缓存"""
        if metadata is None:
            metadata = self._get_cache_metadata()
        
        with self._cache_lock:
            # 检查内存使用量
            data_size = metadata.get('size_bytes', 0)
            if self._memory_usage + data_size > self.max_memory_size:
                self._evict_memory_cache(data_size)
            
            self._memory_cache[cache_key] = {
                'data': data,
                'metadata': metadata
            }
            self._memory_usage += data_size
    
    def _evict_memory_cache(self, required_size: int):
        """内存缓存淘汰算法 (LRU)"""
        with self._cache_lock:
            # 按最后访问时间排序
            items = list(self._memory_cache.items())
            items.sort(key=lambda x: x[1]['metadata']['last_access'])
            
            freed_size = 0
            for cache_key, cache_item in items:
                if freed_size >= required_size:
                    break
                
                item_size = cache_item['metadata'].get('size_bytes', 0)
                del self._memory_cache[cache_key]
                self._memory_usage -= item_size
                freed_size += item_size
                
                logger.debug(f"🗑️ 从内存缓存淘汰: {cache_key}")
    
    def _get_from_disk(self, cache_key: str) -> Any:
        """从磁盘缓存获取数据"""
        cache_file = self.disk_cache_dir / f"{cache_key}.pkl"
        metadata_file = self.disk_cache_dir / f"{cache_key}.meta"
        
        if cache_file.exists() and metadata_file.exists():
            try:
                # 读取元数据
                with open(metadata_file, 'r', encoding='utf-8') as f:
                    metadata = json.load(f, object_hook=self._datetime_parser)
                
                # 检查是否过期
                if datetime.now() > metadata['expire_time']:
                    cache_file.unlink(missing_ok=True)
                    metadata_file.unlink(missing_ok=True)
                    return None
                
                # 读取数据
                with open(cache_file, 'rb') as f:
                    data = pickle.load(f)
                
                # 更新访问统计
                metadata['access_count'] += 1
                metadata['last_access'] = datetime.now()
                
                with open(metadata_file, 'w', encoding='utf-8') as f:
                    json.dump(metadata, f, default=str, indent=2)
                
                return data
                
            except Exception as e:
                logger.warning(f"⚠️ 读取磁盘缓存失败: {str(e)}")
                cache_file.unlink(missing_ok=True)
                metadata_file.unlink(missing_ok=True)
        
        return None
    
    def _put_to_disk(self, 
                    cache_key: str,
                    data: Any,
                    metadata: Optional[Dict] = None):
        """存储数据到磁盘缓存"""
        if metadata is None:
            metadata = self._get_cache_metadata()
        
        cache_file = self.disk_cache_dir / f"{cache_key}.pkl"
        metadata_file = self.disk_cache_dir / f"{cache_key}.meta"
        
        try:
            # 写入数据
            with open(cache_file, 'wb') as f:
                pickle.dump(data, f)
            
            # 写入元数据
            with open(metadata_file, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, default=str, indent=2)
                
        except Exception as e:
            logger.error(f"❌ 磁盘缓存写入失败: {str(e)}")
            cache_file.unlink(missing_ok=True)
            metadata_file.unlink(missing_ok=True)
    
    def _get_from_compressed(self, cache_key: str) -> Any:
        """从压缩缓存获取数据"""
        cache_file = self.compressed_cache_dir / f"{cache_key}.pkl.gz"
        metadata_file = self.compressed_cache_dir / f"{cache_key}.meta"
        
        if cache_file.exists() and metadata_file.exists():
            try:
                # 读取元数据
                with open(metadata_file, 'r', encoding='utf-8') as f:
                    metadata = json.load(f, object_hook=self._datetime_parser)
                
                # 检查是否过期
                if datetime.now() > metadata['expire_time']:
                    cache_file.unlink(missing_ok=True)
                    metadata_file.unlink(missing_ok=True)
                    return None
                
                # 读取并解压数据
                with gzip.open(cache_file, 'rb') as f:
                    data = pickle.load(f)
                
                return data
                
            except Exception as e:
                logger.warning(f"⚠️ 读取压缩缓存失败: {str(e)}")
                cache_file.unlink(missing_ok=True)
                metadata_file.unlink(missing_ok=True)
        
        return None
    
    def _put_to_compressed(self, 
                          cache_key: str,
                          data: Any,
                          metadata: Optional[Dict] = None):
        """存储数据到压缩缓存"""
        if metadata is None:
            metadata = self._get_cache_metadata()
        
        cache_file = self.compressed_cache_dir / f"{cache_key}.pkl.gz"
        metadata_file = self.compressed_cache_dir / f"{cache_key}.meta"
        
        try:
            # 写入压缩数据
            with gzip.open(cache_file, 'wb') as f:
                pickle.dump(data, f)
            
            # 写入元数据
            with open(metadata_file, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, default=str, indent=2)
                
        except Exception as e:
            logger.error(f"❌ 压缩缓存写入失败: {str(e)}")
            cache_file.unlink(missing_ok=True)
            metadata_file.unlink(missing_ok=True)
    
    def _get_from_sqlite(self, cache_key: str) -> Any:
        """从SQLite缓存获取数据"""
        try:
            with sqlite3.connect(str(self.db_path)) as conn:
                cursor = conn.execute('''
                    SELECT data, metadata, expire_time, access_count 
                    FROM cache_data 
                    WHERE key = ? AND expire_time > ?
                ''', (cache_key, datetime.now()))
                
                row = cursor.fetchone()
                if row:
                    data_bytes, metadata_str, expire_time, access_count = row
                    
                    # 反序列化数据
                    data = pickle.loads(data_bytes)
                    
                    # 更新访问统计
                    conn.execute('''
                        UPDATE cache_data 
                        SET access_count = access_count + 1, last_access = ?
                        WHERE key = ?
                    ''', (datetime.now(), cache_key))
                    
                    conn.commit()
                    return data
                    
        except Exception as e:
            logger.warning(f"⚠️ 读取SQLite缓存失败: {str(e)}")
        
        return None
    
    def _put_to_sqlite(self, 
                      cache_key: str,
                      data: Any,
                      metadata: Optional[Dict] = None):
        """存储数据到SQLite缓存"""
        if metadata is None:
            metadata = self._get_cache_metadata()
        
        try:
            # 序列化数据
            data_bytes = pickle.dumps(data)
            metadata_str = json.dumps(metadata, default=str)
            
            with sqlite3.connect(str(self.db_path)) as conn:
                conn.execute('''
                    INSERT OR REPLACE INTO cache_data
                    (key, data, metadata, created_time, expire_time, access_count, last_access)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (
                    cache_key, data_bytes, metadata_str,
                    metadata['created_time'], metadata['expire_time'],
                    0, datetime.now()
                ))
                conn.commit()
                
        except Exception as e:
            logger.error(f"❌ SQLite缓存写入失败: {str(e)}")
    
    def _datetime_parser(self, dct: Dict) -> Dict:
        """JSON日期时间解析器"""
        for key, value in dct.items():
            if isinstance(value, str) and key.endswith('_time'):
                try:
                    dct[key] = datetime.fromisoformat(value.replace('Z', '+00:00'))
                except:
                    pass
        return dct
    
    def clear_cache(self, 
                   cache_type: Optional[str] = None,
                   tags: Optional[List[str]] = None):
        """清理缓存
        
        Args:
            cache_type: 缓存类型 ('memory', 'disk', 'compressed', 'sqlite', None为全部)
            tags: 标签过滤
        """
        logger.info(f"🧹 开始清理缓存 (类型: {cache_type or '全部'})")
        
        if cache_type is None or cache_type == 'memory':
            self._clear_memory_cache(tags)
        
        if cache_type is None or cache_type == 'disk':
            self._clear_disk_cache(tags)
        
        if cache_type is None or cache_type == 'compressed':
            self._clear_compressed_cache(tags)
        
        if cache_type is None or cache_type == 'sqlite':
            self._clear_sqlite_cache(tags)
        
        logger.info("✅ 缓存清理完成")
    
    def _clear_memory_cache(self, tags: Optional[List[str]] = None):
        """清理内存缓存"""
        with self._cache_lock:
            if tags is None:
                # 清理全部内存缓存
                self._memory_cache.clear()
                self._memory_usage = 0
            else:
                # 按标签清理
                keys_to_remove = []
                for key, item in self._memory_cache.items():
                    item_tags = item['metadata'].get('tags', [])
                    if any(tag in item_tags for tag in tags):
                        keys_to_remove.append(key)
                
                for key in keys_to_remove:
                    item = self._memory_cache.pop(key)
                    self._memory_usage -= item['metadata'].get('size_bytes', 0)
    
    def _clear_disk_cache(self, tags: Optional[List[str]] = None):
        """清理磁盘缓存"""
        if tags is None:
            # 清理全部磁盘缓存
            shutil.rmtree(self.disk_cache_dir, ignore_errors=True)
            self.disk_cache_dir.mkdir(parents=True, exist_ok=True)
        else:
            # 按标签清理
            for meta_file in self.disk_cache_dir.glob("*.meta"):
                try:
                    with open(meta_file, 'r', encoding='utf-8') as f:
                        metadata = json.load(f, object_hook=self._datetime_parser)
                    
                    item_tags = metadata.get('tags', [])
                    if any(tag in item_tags for tag in tags):
                        # 删除数据文件和元数据文件
                        cache_key = meta_file.stem
                        cache_file = self.disk_cache_dir / f"{cache_key}.pkl"
                        cache_file.unlink(missing_ok=True)
                        meta_file.unlink(missing_ok=True)
                        
                except Exception as e:
                    logger.warning(f"⚠️ 清理磁盘缓存失败: {str(e)}")
    
    def _clear_compressed_cache(self, tags: Optional[List[str]] = None):
        """清理压缩缓存"""
        if tags is None:
            # 清理全部压缩缓存
            shutil.rmtree(self.compressed_cache_dir, ignore_errors=True)
            self.compressed_cache_dir.mkdir(parents=True, exist_ok=True)
        else:
            # 按标签清理
            for meta_file in self.compressed_cache_dir.glob("*.meta"):
                try:
                    with open(meta_file, 'r', encoding='utf-8') as f:
                        metadata = json.load(f, object_hook=self._datetime_parser)
                    
                    item_tags = metadata.get('tags', [])
                    if any(tag in item_tags for tag in tags):
                        # 删除数据文件和元数据文件
                        cache_key = meta_file.stem
                        cache_file = self.compressed_cache_dir / f"{cache_key}.pkl.gz"
                        cache_file.unlink(missing_ok=True)
                        meta_file.unlink(missing_ok=True)
                        
                except Exception as e:
                    logger.warning(f"⚠️ 清理压缩缓存失败: {str(e)}")
    
    def _clear_sqlite_cache(self, tags: Optional[List[str]] = None):
        """清理SQLite缓存"""
        try:
            with sqlite3.connect(str(self.db_path)) as conn:
                if tags is None:
                    # 清理全部SQLite缓存
                    conn.execute('DELETE FROM cache_data')
                else:
                    # 按标签清理（需要解析metadata JSON）
                    cursor = conn.execute('SELECT key, metadata FROM cache_data')
                    keys_to_delete = []
                    
                    for key, metadata_str in cursor.fetchall():
                        try:
                            metadata = json.loads(metadata_str)
                            item_tags = metadata.get('tags', [])
                            if any(tag in item_tags for tag in tags):
                                keys_to_delete.append(key)
                        except:
                            continue
                    
                    for key in keys_to_delete:
                        conn.execute('DELETE FROM cache_data WHERE key = ?', (key,))
                
                conn.commit()
                
        except Exception as e:
            logger.error(f"❌ 清理SQLite缓存失败: {str(e)}")
    
    def cleanup_expired(self):
        """清理过期缓存"""
        logger.info("🧹 清理过期缓存...")
        
        current_time = datetime.now()
        
        # 清理内存过期数据
        with self._cache_lock:
            expired_keys = []
            for key, item in self._memory_cache.items():
                if current_time > item['metadata']['expire_time']:
                    expired_keys.append(key)
            
            for key in expired_keys:
                item = self._memory_cache.pop(key)
                self._memory_usage -= item['metadata'].get('size_bytes', 0)
        
        # 清理磁盘过期数据
        for meta_file in self.disk_cache_dir.glob("*.meta"):
            try:
                with open(meta_file, 'r', encoding='utf-8') as f:
                    metadata = json.load(f, object_hook=self._datetime_parser)
                
                if current_time > metadata['expire_time']:
                    cache_key = meta_file.stem
                    cache_file = self.disk_cache_dir / f"{cache_key}.pkl"
                    cache_file.unlink(missing_ok=True)
                    meta_file.unlink(missing_ok=True)
                    
            except Exception:
                # 如果元数据文件损坏，删除对应的缓存文件
                cache_key = meta_file.stem
                cache_file = self.disk_cache_dir / f"{cache_key}.pkl"
                cache_file.unlink(missing_ok=True)
                meta_file.unlink(missing_ok=True)
        
        # 清理压缩过期数据
        for meta_file in self.compressed_cache_dir.glob("*.meta"):
            try:
                with open(meta_file, 'r', encoding='utf-8') as f:
                    metadata = json.load(f, object_hook=self._datetime_parser)
                
                if current_time > metadata['expire_time']:
                    cache_key = meta_file.stem
                    cache_file = self.compressed_cache_dir / f"{cache_key}.pkl.gz"
                    cache_file.unlink(missing_ok=True)
                    meta_file.unlink(missing_ok=True)
                    
            except Exception:
                # 如果元数据文件损坏，删除对应的缓存文件
                cache_key = meta_file.stem
                cache_file = self.compressed_cache_dir / f"{cache_key}.pkl.gz"
                cache_file.unlink(missing_ok=True)
                meta_file.unlink(missing_ok=True)
        
        # 清理SQLite过期数据
        try:
            with sqlite3.connect(str(self.db_path)) as conn:
                conn.execute('DELETE FROM cache_data WHERE expire_time < ?', (current_time,))
                conn.commit()
                
                # 优化数据库
                conn.execute('VACUUM')
                
        except Exception as e:
            logger.error(f"❌ 清理SQLite过期数据失败: {str(e)}")
        
        logger.info("✅ 过期缓存清理完成")
    
    def _start_cleanup_thread(self):
        """启动清理线程"""
        if self._cleanup_thread is not None:
            return
        
        def cleanup_worker():
            import time
            while True:
                try:
                    time.sleep(self.cleanup_interval_hours * 3600)  # 转换为秒
                    self.cleanup_expired()
                except Exception as e:
                    logger.error(f"❌ 清理线程异常: {str(e)}")
        
        self._cleanup_thread = threading.Thread(target=cleanup_worker, daemon=True)
        self._cleanup_thread.start()
        
        logger.info("✅ 缓存清理线程已启动")
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """获取缓存统计信息"""
        # 计算缓存大小
        def get_dir_size(path: Path) -> int:
            return sum(f.stat().st_size for f in path.rglob('*') if f.is_file())
        
        memory_items = len(self._memory_cache)
        disk_size = get_dir_size(self.disk_cache_dir)
        compressed_size = get_dir_size(self.compressed_cache_dir)
        
        # SQLite大小
        sqlite_size = 0
        if self.db_path.exists():
            sqlite_size = self.db_path.stat().st_size
        
        hit_rate = 0
        if self.stats['hits'] + self.stats['misses'] > 0:
            hit_rate = self.stats['hits'] / (self.stats['hits'] + self.stats['misses'])
        
        return {
            'memory_cache': {
                'items': memory_items,
                'usage_bytes': self._memory_usage,
                'usage_mb': self._memory_usage / 1024 / 1024,
                'max_size_mb': self.max_memory_size / 1024 / 1024
            },
            'disk_cache': {
                'size_bytes': disk_size,
                'size_mb': disk_size / 1024 / 1024
            },
            'compressed_cache': {
                'size_bytes': compressed_size,
                'size_mb': compressed_size / 1024 / 1024
            },
            'sqlite_cache': {
                'size_bytes': sqlite_size,
                'size_mb': sqlite_size / 1024 / 1024
            },
            'statistics': {
                'total_hits': self.stats['hits'],
                'total_misses': self.stats['misses'],
                'hit_rate': hit_rate,
                'memory_hits': self.stats['memory_hits'],
                'disk_hits': self.stats['disk_hits'],
                'compressed_hits': self.stats['compressed_hits'],
                'sqlite_hits': self.stats['sqlite_hits']
            }
        }
    
    def __del__(self):
        """析构函数"""
        if hasattr(self, '_cleanup_thread') and self._cleanup_thread:
            self._cleanup_thread.join(timeout=1.0)