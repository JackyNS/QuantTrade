#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试版综合API下载器 - 验证API函数名和下载逻辑
"""

import uqer
import pandas as pd
from datetime import datetime
from pathlib import Path
import time
import logging
import json

UQER_TOKEN = "68b9922817ae6273137bda7acba81e293582ba347281dfcc056dcb245b23faf3"

class TestAPIDownloader:
    """测试API下载器"""
    
    def __init__(self):
        self.client = uqer.Client(token=UQER_TOKEN)
        self.data_dir = Path("data/test_api_download")
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        # 测试用的核心API接口（确认正确的函数名）
        self.test_apis = {
            # 基础信息
            "EquGet": {
                "desc": "股票基本信息",
                "params": {},
                "static": True
            },
            "MktIdxdGet": {
                "desc": "指数日行情",
                "params": {"beginDate": "20250101", "endDate": "20250831"},
                "time_range": True,
                "no_stock": True
            },
            # 龙虎榜（确认是否存在这个API）
            "MktRankListGet": {
                "desc": "交易公开信息龙虎榜",
                "params": {"beginDate": "20250801", "endDate": "20250831"},
                "time_range": True
            },
            # 大宗交易
            "MktBlockdGet": {
                "desc": "大宗交易",
                "params": {"beginDate": "20250801", "endDate": "20250831"},
                "time_range": True
            },
            # 融资融券
            "FstTotalGet": {
                "desc": "融资融券汇总",
                "params": {"beginDate": "20250801", "endDate": "20250831"},
                "time_range": True
            },
            # 股东户数
            "EquShareholderNumGet": {
                "desc": "股东户数",
                "params": {"endDate": "20250630"},
                "quarterly": True
            }
        }
        
        # 配置日志
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[logging.StreamHandler()]
        )
        
    def test_single_api(self, api_name, api_config):
        """测试单个API"""
        desc = api_config["desc"]
        logging.info(f"🔍 测试API: {desc} ({api_name})")
        
        try:
            # 检查API函数是否存在
            api_func = getattr(uqer.DataAPI, api_name, None)
            if not api_func:
                logging.error(f"❌ API函数 {api_name} 不存在")
                return False
            
            logging.info(f"✅ API函数 {api_name} 存在")
            
            # 尝试调用API
            params = api_config["params"]
            logging.info(f"📥 调用参数: {params}")
            
            data = api_func(**params)
            
            if data is not None and not data.empty:
                logging.info(f"✅ {desc}: {len(data)} 条记录")
                logging.info(f"📊 数据列: {list(data.columns)}")
                
                # 保存测试数据
                test_file = self.data_dir / f"{api_name.lower()}_test.csv"
                data.head(100).to_csv(test_file, index=False)
                logging.info(f"💾 测试数据已保存: {test_file}")
                return True
            else:
                logging.warning(f"⚠️ {desc}: 返回空数据")
                return False
                
        except Exception as e:
            logging.error(f"❌ {desc} 测试失败: {e}")
            return False
    
    def test_all_apis(self):
        """测试所有API"""
        logging.info("🚀 开始测试API接口...")
        
        results = {}
        for api_name, api_config in self.test_apis.items():
            success = self.test_single_api(api_name, api_config)
            results[api_name] = success
            time.sleep(1)  # API限制
            print("-" * 50)
        
        # 生成测试报告
        logging.info("\n📊 测试结果汇总:")
        success_count = sum(results.values())
        total_count = len(results)
        
        for api_name, success in results.items():
            status = "✅" if success else "❌"
            logging.info(f"{status} {api_name}: {self.test_apis[api_name]['desc']}")
        
        logging.info(f"\n🎯 成功率: {success_count}/{total_count} ({success_count/total_count*100:.1f}%)")
        
        return results

if __name__ == "__main__":
    downloader = TestAPIDownloader()
    results = downloader.test_all_apis()
    
    print("\n🎉 API测试完成!")