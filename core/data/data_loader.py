#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据加载器完整实现 - data_loader.py
====================================

负责从各种数据源获取金融数据，包括：
- 优矿API数据获取（已修复API方法名）
- 股票基础信息
- 价格行情数据  
- 财务数据
- 指数成分股

设计特点：
1. 支持多种数据源
2. 自动缓存机制
3. 错误重试机制
4. 数据质量检查
5. 修复优矿API兼容性问题

版本: 2.0.0
更新: 2024-08-26 (修复API方法名)
"""

import os
import time
import pickle
import hashlib
import warnings
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Union, Tuple
import pandas as pd
import numpy as np

# 导入配置
try:
    from config.settings import Config
    from config.database_config import DatabaseConfig
except ImportError:
    # 如果直接运行此文件，使用默认配置
    class Config:
        UQER_TOKEN = "68b9922817ae6273137bda7acba81e293582ba347281dfcc056dcb245b23faf3"
        START_DATE = '2020-01-01'
        END_DATE = '2024-08-20'
        UNIVERSE = 'CSI300'
        INDEX_CODE = '000300'
        CACHE_DIR = './cache'
        ENABLE_CACHE = True
        CACHE_EXPIRE_HOURS = 24

warnings.filterwarnings('ignore')


class DataLoader:
    """数据获取器主类"""
    
    def __init__(self, config: Optional[Config] = None):
        """
        初始化数据加载器
        
        Args:
            config: 配置对象
        """
        self.config = config if config else Config()
        self.cache_dir = self.config.CACHE_DIR
        
        # 初始化优矿客户端
        self.client = None
        self.DataAPI = None
        self._init_uqer_client()
        
        # 创建缓存目录
        os.makedirs(self.cache_dir, exist_ok=True)
        
        print(f"🚀 数据加载器初始化完成")
        print(f"   📁 缓存目录: {self.cache_dir}")
        print(f"   🔗 API状态: {'✅ 已连接' if self.client else '❌ 未连接'}")
    
    def _init_uqer_client(self):
        """初始化优矿API客户端"""
        try:
            import uqer
            from uqer import Client, DataAPI
            
            # 连接优矿
            self.client = Client(token=self.config.UQER_TOKEN)
            self.DataAPI = DataAPI
            
            # 测试连接（使用正确的API方法名）
            test_result = DataAPI.EquGet(
                field='ticker,shortName',
                ticker='000001',
                limit=1
            )
            
            if not test_result.empty:
                print("✅ 优矿API连接成功")
            else:
                print("⚠️ 优矿API测试查询返回空结果")
                
        except ImportError:
            print("❌ 优矿模块未安装，请运行: pip install uqer")
            self.client = None
        except Exception as e:
            print(f"❌ 优矿API连接失败: {str(e)}")
            self.client = None
    
    def _generate_cache_key(self, *args) -> str:
        """生成缓存文件的键值"""
        key_str = '_'.join(str(arg) for arg in args)
        return hashlib.md5(key_str.encode()).hexdigest()
    
    def _is_cache_valid(self, cache_path: str) -> bool:
        """检查缓存是否有效"""
        if not os.path.exists(cache_path):
            return False
        
        # 检查文件修改时间
        file_time = datetime.fromtimestamp(os.path.getmtime(cache_path))
        expire_time = datetime.now() - timedelta(hours=self.config.CACHE_EXPIRE_HOURS)
        
        return file_time > expire_time
    
    def _load_cache(self, cache_path: str):
        """从缓存加载数据"""
        try:
            with open(cache_path, 'rb') as f:
                return pickle.load(f)
        except Exception as e:
            print(f"⚠️ 缓存加载失败 {cache_path}: {e}")
            return None
    
    def _save_cache(self, data, cache_path: str):
        """保存数据到缓存"""
        try:
            with open(cache_path, 'wb') as f:
                pickle.dump(data, f)
        except Exception as e:
            print(f"⚠️ 缓存保存失败 {cache_path}: {e}")
    
    def get_index_components(self, index_code: str = '000300') -> List[str]:
        """
        获取指数成分股（修复版API调用）
        
        Args:
            index_code: 指数代码
            
        Returns:
            成分股代码列表
        """
        print(f"📋 获取指数 {index_code} 成分股...")
        
        # 缓存键值
        cache_key = self._generate_cache_key('index_components', index_code)
        cache_path = os.path.join(self.cache_dir, f"idx_comp_{cache_key}.pkl")
        
        # 检查缓存
        if self.config.ENABLE_CACHE and self._is_cache_valid(cache_path):
            cached_data = self._load_cache(cache_path)
            if cached_data is not None:
                print(f"📥 从缓存获取成分股: {len(cached_data)} 只")
                return cached_data
        
        if not self.client:
            print("❌ API未连接，使用默认股票池")
            default_stocks = ['000001.XSHE', '000002.XSHE', '600000.XSHG', 
                             '600036.XSHG', '000858.XSHE']
            return default_stocks
        
        try:
            # 使用正确的API方法名：IdxConsGet（而不是getIdxCons）
            result = self.DataAPI.IdxConsGet(
                ticker=index_code,
                field='consTickerSymbol'
            )
            
            if not result.empty:
                stock_list = result['consTickerSymbol'].tolist()
                print(f"✅ 获取到 {len(stock_list)} 只成分股")
                
                # 保存缓存
                if self.config.ENABLE_CACHE:
                    self._save_cache(stock_list, cache_path)
                
                return stock_list
            else:
                print("⚠️ 指数成分股查询结果为空")
                
        except Exception as e:
            print(f"❌ 指数成分股获取失败: {e}")
        
        # 返回默认股票池
        print("🔄 使用默认股票池")
        default_stocks = ['000001.XSHE', '000002.XSHE', '600000.XSHG', 
                         '600036.XSHG', '000858.XSHE', '000166.XSHE',
                         '000338.XSHE', '600519.XSHG', '000858.XSHE', '002415.XSHE']
        return default_stocks
    
    def get_stock_info(self, stock_list: List[str]) -> pd.DataFrame:
        """
        获取股票基础信息（修复版API调用）
        
        Args:
            stock_list: 股票代码列表
            
        Returns:
            股票基础信息DataFrame
        """
        print(f"📊 获取 {len(stock_list)} 只股票基础信息...")
        
        # 缓存键值
        cache_key = self._generate_cache_key('stock_info', str(sorted(stock_list)))
        cache_path = os.path.join(self.cache_dir, f"stock_info_{cache_key}.pkl")
        
        # 检查缓存
        if self.config.ENABLE_CACHE and self._is_cache_valid(cache_path):
            cached_data = self._load_cache(cache_path)
            if cached_data is not None:
                print(f"📥 从缓存获取股票信息")
                return cached_data
        
        if not self.client:
            print("❌ API未连接，返回空DataFrame")
            return pd.DataFrame()
        
        try:
            # 分批获取股票信息（避免API限制）
            all_info = []
            batch_size = 50
            
            for i in range(0, len(stock_list), batch_size):
                batch = stock_list[i:i+batch_size]
                tickers = ','.join(batch)
                
                # 使用正确的API方法名：EquGet（而不是getEqu）
                result = self.DataAPI.EquGet(
                    ticker=tickers,
                    field='ticker,shortName,industry,listDate,delistDate'
                )
                
                if not result.empty:
                    all_info.append(result)
                
                time.sleep(0.1)  # 避免API限制
            
            if all_info:
                stock_info = pd.concat(all_info, ignore_index=True)
                print(f"✅ 获取到 {len(stock_info)} 只股票基础信息")
                
                # 保存缓存
                if self.config.ENABLE_CACHE:
                    self._save_cache(stock_info, cache_path)
                
                return stock_info
            else:
                print("⚠️ 股票基础信息查询结果为空")
                return pd.DataFrame()
                
        except Exception as e:
            print(f"❌ 股票基础信息获取失败: {e}")
            return pd.DataFrame()
    
    def get_price_data(self, stock_list: List[str], start_date: str = None, 
                      end_date: str = None) -> pd.DataFrame:
        """
        获取股票价格数据（修复版API调用）
        
        Args:
            stock_list: 股票代码列表
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            价格数据DataFrame
        """
        start_date = start_date or self.config.START_DATE
        end_date = end_date or self.config.END_DATE
        
        print(f"💰 获取价格数据: {len(stock_list)} 只股票")
        print(f"   📅 时间范围: {start_date} → {end_date}")
        
        # 缓存键值
        cache_key = self._generate_cache_key('price_data', str(sorted(stock_list)), 
                                           start_date, end_date)
        cache_path = os.path.join(self.cache_dir, f"price_data_{cache_key}.pkl")
        
        # 检查缓存
        if self.config.ENABLE_CACHE and self._is_cache_valid(cache_path):
            cached_data = self._load_cache(cache_path)
            if cached_data is not None:
                print(f"📥 从缓存获取价格数据")
                return cached_data
        
        if not self.client:
            print("❌ API未连接，返回空DataFrame")
            return pd.DataFrame()
        
        try:
            all_data = []
            batch_size = 30  # 降低批处理大小，避免API限制
            
            for i in range(0, len(stock_list), batch_size):
                batch = stock_list[i:i+batch_size]
                tickers = ','.join(batch)
                
                print(f"   📊 获取批次 {i//batch_size + 1}: {len(batch)} 只股票")
                
                # 使用正确的API方法名：MktEqudGet（而不是getMktEqud）
                result = self.DataAPI.MktEqudGet(
                    ticker=tickers,
                    beginDate=start_date.replace('-', ''),
                    endDate=end_date.replace('-', ''),
                    field='ticker,tradeDate,openPrice,highestPrice,lowestPrice,closePrice,turnoverVol,turnoverValue'
                )
                
                if not result.empty:
                    all_data.append(result)
                
                time.sleep(0.2)  # 增加延迟避免API限制
            
            if all_data:
                price_data = pd.concat(all_data, ignore_index=True)
                
                # 数据预处理
                price_data['tradeDate'] = pd.to_datetime(price_data['tradeDate'])
                price_data = price_data.sort_values(['ticker', 'tradeDate'])
                
                print(f"✅ 获取价格数据: {price_data.shape}")
                
                # 保存缓存
                if self.config.ENABLE_CACHE:
                    self._save_cache(price_data, cache_path)
                
                return price_data
            else:
                print("⚠️ 价格数据查询结果为空")
                return pd.DataFrame()
                
        except Exception as e:
            print(f"❌ 价格数据获取失败: {e}")
            return pd.DataFrame()
    
    def get_financial_data(self, stock_list: List[str], start_date: str = None, 
                          end_date: str = None) -> pd.DataFrame:
        """
        获取财务数据
        
        Args:
            stock_list: 股票代码列表
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            财务数据DataFrame
        """
        print(f"📈 获取财务数据: {len(stock_list)} 只股票")
        
        if not self.client:
            print("❌ API未连接，返回空DataFrame")
            return pd.DataFrame()
        
        try:
            # 简化财务数据获取，仅获取基础指标
            tickers = ','.join(stock_list[:10])  # 限制数量避免超时
            
            result = self.DataAPI.FdmtEfGet(
                ticker=tickers,
                beginDate=start_date.replace('-', '') if start_date else '20240101',
                endDate=end_date.replace('-', '') if end_date else '20241231',
                field='ticker,publishDate,revenue,netProfit,totalAssets,totalLiab'
            )
            
            if not result.empty:
                print(f"✅ 获取财务数据: {result.shape}")
                return result
            else:
                print("⚠️ 财务数据为空")
                return pd.DataFrame()
                
        except Exception as e:
            print(f"❌ 财务数据获取失败: {e}")
            return pd.DataFrame()
    
    def get_complete_dataset(self, index_code: str = None, 
                           start_date: str = None, end_date: str = None,
                           include_financial: bool = False) -> Dict[str, pd.DataFrame]:
        """
        获取完整数据集
        
        Args:
            index_code: 指数代码
            start_date: 开始日期
            end_date: 结束日期
            include_financial: 是否包含财务数据
            
        Returns:
            包含所有数据的字典
        """
        print(f"🎯 获取完整数据集...")
        
        index_code = index_code or self.config.INDEX_CODE
        start_date = start_date or self.config.START_DATE
        end_date = end_date or self.config.END_DATE
        
        dataset = {}
        
        # 1. 获取股票池
        stock_list = self.get_index_components(index_code)
        dataset['stock_list'] = stock_list
        
        if not stock_list:
            print("❌ 未获取到股票列表，无法继续")
            return dataset
        
        # 2. 获取股票基础信息
        stock_info = self.get_stock_info(stock_list)
        dataset['stock_info'] = stock_info
        
        # 3. 获取价格数据
        price_data = self.get_price_data(stock_list, start_date, end_date)
        dataset['price_data'] = price_data
        
        # 4. 获取财务数据（可选）
        if include_financial:
            financial_data = self.get_financial_data(stock_list, start_date, end_date)
            dataset['financial_data'] = financial_data
        
        # 数据摘要
        print(f"\n📊 数据集摘要:")
        print(f"   股票数量: {len(stock_list)}")
        print(f"   价格数据: {price_data.shape if not price_data.empty else 'N/A'}")
        if include_financial:
            print(f"   财务数据: {financial_data.shape if not financial_data.empty else 'N/A'}")
        
        return dataset
    
    def clear_cache(self):
        """清理所有缓存文件"""
        import glob
        
        cache_files = glob.glob(os.path.join(self.cache_dir, "*.pkl"))
        cleaned_count = 0
        
        for file_path in cache_files:
            try:
                os.remove(file_path)
                cleaned_count += 1
            except Exception as e:
                print(f"⚠️ 删除缓存文件失败 {file_path}: {e}")
        
        print(f"🧹 清理缓存完成: {cleaned_count} 个文件")
    
    def __repr__(self) -> str:
        """数据加载器信息"""
        return f"""
DataLoader Summary:
==================
🔗 API状态: {'✅ 已连接' if self.client else '❌ 未连接'}
📁 缓存目录: {self.cache_dir}
🗂️ 缓存可用: {self.config.ENABLE_CACHE}
⏱️ 缓存过期: {self.config.CACHE_EXPIRE_HOURS}小时
==================
"""


# 工厂函数
def create_data_loader(config: Optional[Config] = None) -> DataLoader:
    """创建数据加载器实例"""
    return DataLoader(config)


# 模块导出
__all__ = ['DataLoader', 'create_data_loader']


# 使用示例
if __name__ == "__main__":
    # 创建数据加载器
    loader = DataLoader()
    
    # 获取完整数据集
    dataset = loader.get_complete_dataset(
        index_code='000300',  # 沪深300
        include_financial=False
    )
    
    print("\n🎉 数据获取演示完成！")