#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MktRANKInstTrGet API下载器 - 下载机构交易排名数据
"""

import uqer
import pandas as pd
from pathlib import Path
import logging
from datetime import datetime, date
import time

class MktRANKInstTrDownloader:
    """机构交易排名数据下载器"""
    
    def __init__(self, token):
        self.token = token
        self.target_dir = Path("data/final_comprehensive_download/special_trading/mktrankinsttrget")
        self.target_dir.mkdir(exist_ok=True)
        self.setup_logging()
        
    def setup_logging(self):
        """设置日志"""
        log_file = Path("mktrankinstrtr_download.log")
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
    
    def generate_trade_dates(self):
        """生成交易日期列表"""
        # 基于A股交易日历，生成季度末日期
        trade_dates = []
        
        for year in range(2000, 2026):
            # 每年4个季度的最后交易日（大致）
            quarters = [
                f"{year}0331",  # Q1
                f"{year}0630",  # Q2 
                f"{year}0930",  # Q3
                f"{year}1231",  # Q4
            ]
            
            for i, quarter_date in enumerate(quarters, 1):
                # 只到2025年Q3
                if year == 2025 and i > 3:
                    break
                    
                trade_dates.append((quarter_date, f"{year}_Q{i}"))
        
        return trade_dates
    
    def download_mktrankinstrtr(self):
        """下载MktRANKInstTrGet数据"""
        logging.info("🚀 开始下载MktRANKInstTrGet数据...")
        
        # 登录
        client = self.login_uqer()
        if not client:
            return False
        
        api_func = uqer.DataAPI.MktRANKInstTrGet
        trade_dates = self.generate_trade_dates()
        
        success_count = 0
        total_records = 0
        
        logging.info(f"📋 计划下载 {len(trade_dates)} 个交易日的数据")
        
        for i, (trade_date, filename) in enumerate(trade_dates, 1):
            try:
                logging.info(f"📥 [{i}/{len(trade_dates)}] {trade_date} -> {filename}")
                
                output_file = self.target_dir / f"{filename}.csv"
                
                # 如果文件已存在，跳过
                if output_file.exists():
                    logging.info(f"⏭️ 文件已存在，跳过: {filename}")
                    continue
                
                # 调用API
                result = api_func(tradeDate=trade_date)
                
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
                time.sleep(0.3)
                
            except Exception as e:
                error_msg = str(e)
                if "无效的请求参数" in error_msg:
                    logging.warning(f"⚠️ 日期 {trade_date} 无数据或无效")
                else:
                    logging.error(f"❌ 下载失败 {filename}: {e}")
                continue
        
        logging.info("🎯 MktRANKInstTrGet下载完成:")
        logging.info(f"   成功文件: {success_count}")
        logging.info(f"   总记录数: {total_records:,}")
        
        return success_count > 0

if __name__ == "__main__":
    token = "68b9922817ae6273137bda7acba81e293582ba347281dfcc056dcb245b23faf3"
    downloader = MktRANKInstTrDownloader(token)
    downloader.download_mktrankinstrtr()