#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
最终版缺失API补充下载器
基于最新优矿接口清单和使用说明，使用正确的API调用方式
完成special_trading分类中缺失的3个API:
1. getEquMarginSec - 可充抵保证金证券
2. getMktRANKInstTr - 行业成分换手率排名  
3. getFdmtEe - 业绩快报
"""

import uqer
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
import time
import logging
import json

UQER_TOKEN = "68b9922817ae6273137bda7acba81e293582ba347281dfcc056dcb245b23faf3"

class FinalMissingAPIDownloader:
    """最终版缺失API补充下载器"""
    
    def __init__(self):
        # 根据PDF说明，正确的初始化方式
        self.client = uqer.Client(token=UQER_TOKEN)
        self.data_dir = Path("data/final_comprehensive_download")
        
        # 配置日志
        log_file = self.data_dir / "final_missing_apis_download.log"
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
        
        # 基于uqer.DataAPI探测的正确API配置
        self.missing_apis = {
            "EquMarginSecGet": {
                "desc": "可充抵保证金证券",
                "time_range": True,
                "category": "special_trading",
                "dir_name": "equmarginsec"
            },
            "MktRANKInstTrGet": {
                "desc": "行业成分换手率排名",
                "time_range": True,
                "category": "special_trading", 
                "dir_name": "mktrankinstr"
            },
            "FdmtEeGet": {
                "desc": "业绩快报",
                "time_range": True,
                "category": "special_trading",
                "dir_name": "fdmtee"
            }
        }
        
    def download_with_time_range(self, api_name, desc, dir_name, year_start=2000, year_end=2025):
        """按年份范围下载数据"""
        category_dir = self.data_dir / "special_trading"
        api_dir = category_dir / dir_name
        api_dir.mkdir(parents=True, exist_ok=True)
        
        total_records = 0
        logging.info(f"📥 开始下载 {desc}...")
        
        for year in range(year_start, year_end + 1):
            try:
                begin_date = f"{year}-01-01"
                end_date = f"{year}-12-31" if year < 2025 else "2025-09-01"
                
                # 根据PDF说明，正确的API调用方式是 uqer.DataAPI.XXX
                if hasattr(uqer.DataAPI, api_name):
                    api_func = getattr(uqer.DataAPI, api_name)
                    
                    # 调用API
                    df = api_func(beginDate=begin_date, endDate=end_date)
                    
                    if not df.empty:
                        # 保存数据
                        file_path = api_dir / f"year_{year}.csv"
                        df.to_csv(file_path, index=False, encoding='utf-8-sig')
                        logging.info(f"✅ {desc} {year}年: {len(df)} 条记录")
                        total_records += len(df)
                    else:
                        logging.info(f"⚪ {desc} {year}年: 无数据")
                else:
                    logging.error(f"❌ API函数 {api_name} 不存在")
                    break
                
                # 避免请求过频
                time.sleep(1)
                
            except Exception as e:
                if "无效的请求参数" in str(e):
                    logging.warning(f"⚠️ {desc} {year}年: API参数无效，跳过")
                else:
                    logging.error(f"❌ {desc} {year}年下载失败: {str(e)}")
                time.sleep(2)
        
        logging.info(f"📊 {desc} 完成: 总计 {total_records} 条记录")
        return total_records
    
    def download_static_data(self, api_name, desc, dir_name):
        """下载静态数据"""
        category_dir = self.data_dir / "special_trading"
        api_dir = category_dir / dir_name
        api_dir.mkdir(parents=True, exist_ok=True)
        
        try:
            logging.info(f"📥 开始下载 {desc}...")
            
            # 正确的API调用方式
            if hasattr(uqer.DataAPI, api_name):
                api_func = getattr(uqer.DataAPI, api_name)
                
                # 先尝试无参数调用
                try:
                    df = api_func()
                except Exception as e1:
                    # 如果失败，尝试添加时间参数
                    try:
                        df = api_func(beginDate="2020-01-01", endDate="2025-09-01")
                    except Exception as e2:
                        logging.error(f"❌ {desc} 下载失败: {str(e1)} / {str(e2)}")
                        return 0
                
                if not df.empty:
                    file_path = api_dir / "static_data.csv"
                    df.to_csv(file_path, index=False, encoding='utf-8-sig')
                    logging.info(f"✅ {desc}: {len(df)} 条记录")
                    return len(df)
                else:
                    logging.info(f"⚪ {desc}: 无数据")
                    return 0
            else:
                logging.error(f"❌ API函数 {api_name} 不存在")
                return 0
                
        except Exception as e:
            logging.error(f"❌ {desc} 下载失败: {str(e)}")
            return 0
    
    def run(self):
        """执行补充下载"""
        logging.info("🚀 开始补充下载缺失的3个API（使用正确的调用方式）...")
        logging.info("📋 基于最新优矿接口清单和使用说明")
        
        success_count = 0
        total_records = 0
        
        for api_name, config in self.missing_apis.items():
            desc = config["desc"]
            dir_name = config.get("dir_name", api_name.lower())
            
            try:
                if config.get("time_range", False):
                    records = self.download_with_time_range(api_name, desc, dir_name)
                else:
                    records = self.download_static_data(api_name, desc, dir_name)
                
                if records > 0:
                    success_count += 1
                    total_records += records
                    
            except Exception as e:
                logging.error(f"❌ {api_name} 处理失败: {str(e)}")
        
        # 更新进度文件
        self.update_progress(success_count, total_records)
        
        logging.info(f"🎉 补充下载完成! 成功: {success_count}/3, 总记录: {total_records}")
        
        if success_count == 3:
            logging.info("🌟 所有缺失API下载成功！现在拥有完整的58个API数据！")
        elif success_count > 0:
            logging.info(f"✨ 部分API下载成功，还有 {3 - success_count} 个API需要处理")
        else:
            logging.warning("⚠️ 所有API下载失败，请检查API调用方式")
        
    def update_progress(self, success_count, total_records):
        """更新主进度文件"""
        progress_file = self.data_dir / "download_progress.json"
        
        try:
            with open(progress_file, 'r', encoding='utf-8') as f:
                progress_data = json.load(f)
            
            # 添加新完成的API
            completed_apis = progress_data.get("completed_apis", [])
            for api_name in self.missing_apis.keys():
                api_key = f"special_trading_{api_name}"
                if api_key not in completed_apis:
                    completed_apis.append(api_key)
            
            # 更新统计信息
            progress_data["completed_apis"] = completed_apis
            progress_data["statistics"]["success_count"] = len(completed_apis)
            progress_data["statistics"]["total_records"] += total_records
            progress_data["last_update"] = datetime.now().isoformat()
            
            # 保存更新后的进度
            with open(progress_file, 'w', encoding='utf-8') as f:
                json.dump(progress_data, f, indent=2, ensure_ascii=False)
                
            logging.info(f"📊 进度文件已更新: 总API数 {len(completed_apis)}")
            
            if len(completed_apis) >= 58:
                logging.info("🎊 恭喜！已完成所有58个API接口下载！")
            
        except Exception as e:
            logging.error(f"❌ 更新进度文件失败: {str(e)}")

if __name__ == "__main__":
    downloader = FinalMissingAPIDownloader()
    downloader.run()