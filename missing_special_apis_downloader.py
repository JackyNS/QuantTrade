#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Special Trading缺失API下载器 - 完成最后2个API的下载
"""

import uqer
import pandas as pd
from pathlib import Path
import logging
from datetime import datetime, date
import time

class MissingSpecialAPIsDownloader:
    """Special Trading缺失API下载器"""
    
    def __init__(self, token):
        self.token = token
        self.target_dir = Path("data/final_comprehensive_download/special_trading")
        self.setup_logging()
        
        # 需要下载的API配置
        self.missing_apis = {
            "MktRANKInstTrGet": {
                "dir_name": "mktrankinsttrget",
                "description": "机构交易排名数据",
                "date_field": "beginDate",
                "is_quarterly": True,
                "start_year": 2000,
                "end_year": 2025
            },
            "EquMarginSecGet": {
                "dir_name": "equmarginsecget", 
                "description": "融资融券标的证券数据",
                "date_field": "publishDate",
                "is_quarterly": False,
                "start_year": 2000,
                "end_year": 2025
            }
        }
    
    def setup_logging(self):
        """设置日志"""
        log_file = Path("missing_special_apis_download.log")
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
        logging.info("🔍 检查API是否存在...")
        
        available_apis = []
        
        for api_name in self.missing_apis.keys():
            if hasattr(uqer.DataAPI, api_name):
                logging.info(f"✅ 找到API: {api_name}")
                available_apis.append(api_name)
            else:
                logging.error(f"❌ API不存在: {api_name}")
        
        return available_apis
    
    def generate_date_ranges(self, api_config):
        """生成日期范围"""
        ranges = []
        
        if api_config["is_quarterly"]:
            # 按季度生成
            for year in range(api_config["start_year"], api_config["end_year"] + 1):
                for quarter in [1, 2, 3, 4]:
                    if quarter == 1:
                        date_str = f"{year}0331"
                    elif quarter == 2:
                        date_str = f"{year}0630"
                    elif quarter == 3:
                        date_str = f"{year}0930"
                    else:
                        date_str = f"{year}1231"
                    ranges.append((date_str, f"{year}_Q{quarter}"))
        else:
            # 按年生成
            for year in range(api_config["start_year"], api_config["end_year"] + 1):
                ranges.append((f"{year}1231", f"year_{year}"))
        
        return ranges
    
    def download_single_api(self, api_name, api_config):
        """下载单个API数据"""
        logging.info(f"🚀 开始下载 {api_name} ({api_config['description']})")
        
        api_dir = self.target_dir / api_config["dir_name"]
        api_dir.mkdir(exist_ok=True)
        
        api_func = getattr(uqer.DataAPI, api_name)
        date_ranges = self.generate_date_ranges(api_config)
        
        success_count = 0
        total_records = 0
        
        for i, (end_date, filename) in enumerate(date_ranges, 1):
            try:
                logging.info(f"📥 [{i}/{len(date_ranges)}] {api_name} - {filename}")
                
                output_file = api_dir / f"{filename}.csv"
                
                # 如果文件已存在，跳过
                if output_file.exists():
                    logging.info(f"⏭️ 文件已存在，跳过: {filename}")
                    continue
                
                # 构建查询参数
                kwargs = {
                    api_config["date_field"]: "",
                    "endDate": end_date
                }
                
                # 调用API
                result = api_func(**kwargs)
                
                if hasattr(result, 'getData') and callable(getattr(result, 'getData')):
                    df = result.getData()
                else:
                    df = result
                
                if df is None or (isinstance(df, pd.DataFrame) and df.empty):
                    logging.warning(f"⚠️ 无数据: {filename}")
                    continue
                
                # 保存数据
                df.to_csv(output_file, index=False, encoding='utf-8')
                
                success_count += 1
                total_records += len(df)
                
                logging.info(f"✅ 成功: {len(df):,} 条记录")
                
                # 请求间隔
                time.sleep(0.5)
                
            except Exception as e:
                logging.error(f"❌ 下载失败 {filename}: {e}")
                continue
        
        logging.info(f"🎯 {api_name} 下载完成:")
        logging.info(f"   成功文件: {success_count}")
        logging.info(f"   总记录数: {total_records:,}")
        
        return success_count, total_records
    
    def run_download(self):
        """运行下载任务"""
        logging.info("🚀 开始下载Special Trading缺失API...")
        
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
            if api_name in self.missing_apis:
                api_config = self.missing_apis[api_name]
                
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
        
        logging.info("🎊 Special Trading缺失API下载完成!")
        logging.info("=" * 50)
        logging.info(f"📊 下载统计:")
        logging.info(f"  API数量: {total_stats['apis_downloaded']}")
        logging.info(f"  文件数量: {total_stats['files_downloaded']}")
        logging.info(f"  记录总数: {total_stats['total_records']:,}")
        logging.info(f"  用时: {duration}")
        
        return total_stats["apis_downloaded"] > 0

if __name__ == "__main__":
    token = "68b9922817ae6273137bda7acba81e293582ba347281dfcc056dcb245b23faf3"
    downloader = MissingSpecialAPIsDownloader(token)
    downloader.run_download()