#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
新增API下载器 - 下载8个额外的重要数据接口
"""

import uqer
import pandas as pd
from pathlib import Path
import logging
from datetime import datetime, date
import time

class AdditionalAPIsDownloader:
    """新增API下载器"""
    
    def __init__(self, token):
        self.token = token
        self.target_dir = Path("data/final_comprehensive_download/additional_apis")
        self.target_dir.mkdir(exist_ok=True)
        self.setup_logging()
        
        # 需要下载的API配置 - 使用正确的API名称
        self.additional_apis = {
            "EquFancyFactorsLiteGet": {
                "dir_name": "equ_fancy_factors_lite",
                "description": "股精选精品因子数据",
                "date_pattern": "daily",
                "start_date": "20200101",
                "end_date": "20250901"
            },
            "EcoDataChinaLiteGet": {
                "dir_name": "eco_data_china_lite", 
                "description": "宏观行业-中国宏观重点指标",
                "date_pattern": "monthly",
                "start_date": "20200101", 
                "end_date": "20250901"
            },
            "SecTypeRegionGet": {
                "dir_name": "sec_type_region",
                "description": "地域分类",
                "date_pattern": "snapshot",
                "start_date": None,
                "end_date": None
            },
            "SecTypeRelGet": {
                "dir_name": "sec_type_rel",
                "description": "证券板块成分",
                "date_pattern": "snapshot",
                "start_date": None,
                "end_date": None
            },
            "SecTypeGet": {
                "dir_name": "sec_type",
                "description": "证券板块",
                "date_pattern": "snapshot", 
                "start_date": None,
                "end_date": None
            },
            "IndustryGet": {
                "dir_name": "industry",
                "description": "行业分类标准",
                "date_pattern": "snapshot",
                "start_date": None,
                "end_date": None
            },
            "FstTotalGet": {
                "dir_name": "fst_total",
                "description": "沪深融资融券每日汇总信息",
                "date_pattern": "daily",
                "start_date": "20200101",
                "end_date": "20250901"
            },
            "FstDetailGet": {
                "dir_name": "fst_detail", 
                "description": "沪深融资融券每日交易明细信息",
                "date_pattern": "daily",
                "start_date": "20200101",
                "end_date": "20250901"
            }
        }
    
    def setup_logging(self):
        """设置日志"""
        log_file = Path("additional_apis_download.log")
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file, encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
    
    def login_uqer(self):
        """登录优矿"""
        try:
            client = uqer.Client(token=self.token)
            logging.info("✅ 优矿登录成功")
            return client
        except Exception as e:
            logging.error(f"❌ 优矿登录失败: {e}")
            return None
    
    def check_api_existence(self):
        """检查API是否存在"""
        logging.info("🔍 检查新增API是否存在...")
        
        available_apis = []
        
        for api_name in self.additional_apis.keys():
            if hasattr(uqer.DataAPI, api_name):
                logging.info(f"✅ 找到API: {api_name}")
                available_apis.append(api_name)
            else:
                logging.error(f"❌ API不存在: {api_name}")
        
        return available_apis
    
    def generate_date_list(self, start_date, end_date, pattern):
        """生成日期列表"""
        if pattern == "snapshot":
            return [("", "snapshot")]
        
        date_list = []
        
        if pattern == "daily":
            # 按年生成，每年取4个季度末
            for year in range(2020, 2026):
                quarters = [
                    f"{year}0331",
                    f"{year}0630", 
                    f"{year}0930",
                    f"{year}1231"
                ]
                for i, quarter_date in enumerate(quarters, 1):
                    if year == 2025 and i > 3:  # 2025年只到Q3
                        break
                    date_list.append((quarter_date, f"{year}_Q{i}"))
                        
        elif pattern == "monthly":
            # 按年生成
            for year in range(2020, 2026):
                if year == 2025:
                    break
                date_list.append((f"{year}1231", f"year_{year}"))
        
        return date_list
    
    def download_snapshot_api(self, api_name, api_config):
        """下载快照型API（无日期参数）"""
        logging.info(f"📸 下载快照API: {api_name}")
        
        api_dir = self.target_dir / api_config["dir_name"]
        api_dir.mkdir(exist_ok=True)
        
        output_file = api_dir / "snapshot.csv"
        
        if output_file.exists():
            logging.info(f"⏭️ 文件已存在，跳过: snapshot.csv")
            return 1, 0
        
        try:
            api_func = getattr(uqer.DataAPI, api_name)
            result = api_func()
            
            if hasattr(result, 'getData') and callable(getattr(result, 'getData')):
                df = result.getData()
            else:
                df = result
            
            if df is None or (isinstance(df, pd.DataFrame) and df.empty):
                logging.warning(f"⚠️ 无数据: {api_name}")
                return 0, 0
            
            df.to_csv(output_file, index=False, encoding='utf-8')
            logging.info(f"✅ 成功: {len(df):,} 条记录")
            return 1, len(df)
            
        except Exception as e:
            logging.error(f"❌ 下载失败 {api_name}: {e}")
            return 0, 0
    
    def download_dated_api(self, api_name, api_config):
        """下载有日期参数的API"""
        logging.info(f"📅 下载日期型API: {api_name}")
        
        api_dir = self.target_dir / api_config["dir_name"]
        api_dir.mkdir(exist_ok=True)
        
        api_func = getattr(uqer.DataAPI, api_name)
        date_list = self.generate_date_list(
            api_config["start_date"], 
            api_config["end_date"], 
            api_config["date_pattern"]
        )
        
        success_count = 0
        total_records = 0
        
        for i, (trade_date, filename) in enumerate(date_list, 1):
            try:
                logging.info(f"📥 [{i}/{len(date_list)}] {api_name} - {filename}")
                
                output_file = api_dir / f"{filename}.csv"
                
                if output_file.exists():
                    logging.info(f"⏭️ 文件已存在，跳过: {filename}")
                    continue
                
                # 尝试不同的参数组合
                result = None
                param_combinations = [
                    {"tradeDate": trade_date} if trade_date else {},
                    {"beginDate": trade_date, "endDate": trade_date} if trade_date else {},
                    {"date": trade_date} if trade_date else {}
                ]
                
                for params in param_combinations:
                    try:
                        result = api_func(**params)
                        break
                    except:
                        continue
                
                if result is None:
                    # 无参数调用
                    result = api_func()
                
                if hasattr(result, 'getData') and callable(getattr(result, 'getData')):
                    df = result.getData()
                else:
                    df = result
                
                if df is None or (isinstance(df, pd.DataFrame) and df.empty):
                    logging.warning(f"⚠️ 无数据: {filename}")
                    continue
                
                df.to_csv(output_file, index=False, encoding='utf-8')
                success_count += 1
                total_records += len(df)
                logging.info(f"✅ 成功: {len(df):,} 条记录")
                
                time.sleep(0.3)
                
            except Exception as e:
                logging.error(f"❌ 下载失败 {filename}: {e}")
                continue
        
        return success_count, total_records
    
    def download_single_api(self, api_name, api_config):
        """下载单个API"""
        logging.info(f"🚀 开始下载 {api_name} ({api_config['description']})")
        
        if api_config["date_pattern"] == "snapshot":
            return self.download_snapshot_api(api_name, api_config)
        else:
            return self.download_dated_api(api_name, api_config)
    
    def run_download(self):
        """运行下载任务"""
        logging.info("🚀 开始下载新增的8个API...")
        
        start_time = datetime.now()
        
        # 登录
        client = self.login_uqer()
        if not client:
            return False
        
        # 检查API存在性
        available_apis = self.check_api_existence()
        if not available_apis:
            logging.error("❌ 没有可用的API")
            return False
        
        total_stats = {
            "apis_downloaded": 0,
            "files_downloaded": 0,
            "total_records": 0
        }
        
        # 逐个下载API
        for api_name in available_apis:
            if api_name in self.additional_apis:
                api_config = self.additional_apis[api_name]
                
                try:
                    success_files, records = self.download_single_api(api_name, api_config)
                    
                    total_stats["apis_downloaded"] += 1
                    total_stats["files_downloaded"] += success_files
                    total_stats["total_records"] += records
                    
                except Exception as e:
                    logging.error(f"❌ API下载失败 {api_name}: {e}")
        
        # 输出最终统计
        end_time = datetime.now()
        duration = end_time - start_time
        
        logging.info("🎊 新增API下载完成!")
        logging.info("=" * 50)
        logging.info(f"📊 下载统计:")
        logging.info(f"  API数量: {total_stats['apis_downloaded']}")
        logging.info(f"  文件数量: {total_stats['files_downloaded']}")
        logging.info(f"  记录总数: {total_stats['total_records']:,}")
        logging.info(f"  用时: {duration}")
        
        return total_stats["apis_downloaded"] > 0

if __name__ == "__main__":
    token = "68b9922817ae6273137bda7acba81e293582ba347281dfcc056dcb245b23faf3"
    downloader = AdditionalAPIsDownloader(token)
    downloader.run_download()