#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
修正版缺失API下载器 - 基于API测试结果优化调用方式
"""

import uqer
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
import time
import logging
import json

UQER_TOKEN = "68b9922817ae6273137bda7acba81e293582ba347281dfcc056dcb245b23faf3"

class CorrectedMissingAPIDownloader:
    """修正版缺失API下载器"""
    
    def __init__(self):
        self.client = uqer.Client(token=UQER_TOKEN)
        self.data_dir = Path("data/final_comprehensive_download")
        
        # 配置日志
        log_file = self.data_dir / "corrected_missing_apis_download.log"
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
        
        # 基于测试结果的正确API配置
        self.api_configs = {
            "EquMarginSecGet": {
                "desc": "可充抵保证金证券",
                "call_method": "date_range_required",
                "dir_name": "equmarginsec",
                "start_year": 2010  # 从2010年开始尝试，margin trading开始时间
            },
            "MktRANKInstTrGet": {
                "desc": "行业成分换手率排名", 
                "call_method": "trade_date_required",
                "dir_name": "mktrankinstr",
                "start_year": 2005  # 从2005年开始尝试
            },
            "FdmtEeGet": {
                "desc": "业绩快报",
                "call_method": "no_params",
                "dir_name": "fdmtee",
                "start_year": 2000  # 无参数调用，获取全部数据
            }
        }
        
    def download_fdmt_ee(self):
        """下载业绩快报 - 无参数调用获取全部数据"""
        config = self.api_configs["FdmtEeGet"]
        desc = config["desc"]
        dir_name = config["dir_name"]
        
        category_dir = self.data_dir / "special_trading"
        api_dir = category_dir / dir_name
        api_dir.mkdir(parents=True, exist_ok=True)
        
        try:
            logging.info(f"📥 开始下载 {desc}（无参数获取全部数据）...")
            
            # 直接调用API获取所有数据
            df = uqer.DataAPI.FdmtEeGet()
            
            if not df.empty:
                # 保存完整数据
                file_path = api_dir / "all_data.csv"
                df.to_csv(file_path, index=False, encoding='utf-8-sig')
                
                # 按年份分组保存
                if 'publishDate' in df.columns:
                    df['year'] = pd.to_datetime(df['publishDate']).dt.year
                    for year in sorted(df['year'].unique()):
                        year_data = df[df['year'] == year].drop('year', axis=1)
                        if not year_data.empty:
                            year_file = api_dir / f"year_{year}.csv"
                            year_data.to_csv(year_file, index=False, encoding='utf-8-sig')
                            logging.info(f"✅ {desc} {year}年: {len(year_data)} 条记录")
                
                total_records = len(df)
                logging.info(f"📊 {desc} 完成: 总计 {total_records} 条记录")
                return total_records
            else:
                logging.info(f"⚪ {desc}: 无数据")
                return 0
                
        except Exception as e:
            logging.error(f"❌ {desc} 下载失败: {str(e)}")
            return 0
    
    def download_margin_sec(self):
        """下载可充抵保证金证券 - 需要beginDate和endDate"""
        config = self.api_configs["EquMarginSecGet"]
        desc = config["desc"]
        dir_name = config["dir_name"]
        start_year = config["start_year"]
        
        category_dir = self.data_dir / "special_trading"
        api_dir = category_dir / dir_name
        api_dir.mkdir(parents=True, exist_ok=True)
        
        total_records = 0
        logging.info(f"📥 开始下载 {desc}（从{start_year}年开始）...")
        
        # 从margin trading开始的年份尝试
        for year in range(start_year, 2025):
            try:
                begin_date = f"{year}-01-01"
                end_date = f"{year}-12-31" if year < 2024 else "2024-12-31"
                
                df = uqer.DataAPI.EquMarginSecGet(beginDate=begin_date, endDate=end_date)
                
                if not df.empty:
                    file_path = api_dir / f"year_{year}.csv"
                    df.to_csv(file_path, index=False, encoding='utf-8-sig')
                    logging.info(f"✅ {desc} {year}年: {len(df)} 条记录")
                    total_records += len(df)
                else:
                    logging.info(f"⚪ {desc} {year}年: 无数据")
                
                time.sleep(1)
                
            except Exception as e:
                if "无效的请求参数" in str(e):
                    logging.info(f"⚪ {desc} {year}年: 该年份无数据或服务不可用")
                else:
                    logging.error(f"❌ {desc} {year}年下载失败: {str(e)}")
                time.sleep(2)
        
        logging.info(f"📊 {desc} 完成: 总计 {total_records} 条记录")
        return total_records
    
    def download_rank_inst_tr(self):
        """下载行业成分换手率排名 - 需要tradeDate"""
        config = self.api_configs["MktRANKInstTrGet"]
        desc = config["desc"]
        dir_name = config["dir_name"]
        start_year = config["start_year"]
        
        category_dir = self.data_dir / "special_trading"
        api_dir = category_dir / dir_name
        api_dir.mkdir(parents=True, exist_ok=True)
        
        total_records = 0
        logging.info(f"📥 开始下载 {desc}（从{start_year}年开始，按月采样）...")
        
        # 从指定年份开始，每年取几个关键日期采样
        sample_dates = ["-01-01", "-03-31", "-06-30", "-09-30", "-12-31"]
        
        for year in range(start_year, 2025):
            year_records = 0
            for date_suffix in sample_dates:
                try:
                    trade_date = f"{year}{date_suffix}"
                    if year == 2024 and date_suffix in ["-09-30", "-12-31"]:
                        continue  # 跳过未来日期
                    
                    df = uqer.DataAPI.MktRANKInstTrGet(tradeDate=trade_date)
                    
                    if not df.empty:
                        date_file = api_dir / f"{trade_date.replace('-', '')}.csv"
                        df.to_csv(date_file, index=False, encoding='utf-8-sig')
                        year_records += len(df)
                    
                    time.sleep(0.5)
                    
                except Exception as e:
                    if "无效的请求参数" not in str(e):
                        logging.error(f"❌ {desc} {trade_date}下载失败: {str(e)}")
                    time.sleep(1)
            
            if year_records > 0:
                logging.info(f"✅ {desc} {year}年: {year_records} 条记录")
                total_records += year_records
            else:
                logging.info(f"⚪ {desc} {year}年: 无数据")
        
        logging.info(f"📊 {desc} 完成: 总计 {total_records} 条记录")
        return total_records
    
    def run(self):
        """执行修正版下载"""
        logging.info("🚀 开始修正版缺失API下载...")
        logging.info("📋 基于API测试结果优化调用方式")
        
        results = {}
        total_records = 0
        
        # 1. 下载业绩快报（最容易成功的）
        try:
            records = self.download_fdmt_ee()
            results["FdmtEeGet"] = records
            total_records += records
        except Exception as e:
            logging.error(f"❌ FdmtEeGet 处理失败: {str(e)}")
            results["FdmtEeGet"] = 0
        
        # 2. 下载可充抵保证金证券
        try:
            records = self.download_margin_sec()
            results["EquMarginSecGet"] = records
            total_records += records
        except Exception as e:
            logging.error(f"❌ EquMarginSecGet 处理失败: {str(e)}")
            results["EquMarginSecGet"] = 0
        
        # 3. 下载行业成分换手率排名
        try:
            records = self.download_rank_inst_tr()
            results["MktRANKInstTrGet"] = records
            total_records += records
        except Exception as e:
            logging.error(f"❌ MktRANKInstTrGet 处理失败: {str(e)}")
            results["MktRANKInstTrGet"] = 0
        
        # 更新进度文件
        success_count = sum(1 for r in results.values() if r > 0)
        self.update_progress(results, total_records)
        
        logging.info(f"🎉 修正版下载完成!")
        logging.info(f"📊 结果摘要: 成功 {success_count}/3 个API, 总记录 {total_records} 条")
        
        for api_name, records in results.items():
            if records > 0:
                logging.info(f"  ✅ {api_name}: {records:,} 条记录")
            else:
                logging.info(f"  ❌ {api_name}: 未获取到数据")
        
        if success_count == 3:
            logging.info("🌟 所有缺失API下载成功！现在拥有完整的58个API数据！")
        elif success_count > 0:
            logging.info(f"✨ 部分API下载成功，{3 - success_count} 个API可能无可用数据")
        else:
            logging.warning("⚠️ 所有API下载失败，可能服务不可用或参数需要进一步调整")
    
    def update_progress(self, results, total_records):
        """更新主进度文件"""
        progress_file = self.data_dir / "download_progress.json"
        
        try:
            with open(progress_file, 'r', encoding='utf-8') as f:
                progress_data = json.load(f)
            
            # 添加成功的API
            completed_apis = progress_data.get("completed_apis", [])
            for api_name, records in results.items():
                if records > 0:
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
    downloader = CorrectedMissingAPIDownloader()
    downloader.run()