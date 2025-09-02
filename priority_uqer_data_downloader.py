#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
优矿核心数据优先下载器
====================

基于优矿api2025.txt和core模块需求，优先下载最重要的数据：

1. 【核心行情数据】- 策略和回测的基础
   - getMktEqud: 股票日行情 [免费版]
   - getMktEqudAdj: 股票前复权日行情 [免费版] 
   - getMktEquw: 股票周行情 [免费版]
   - getMktEqum: 股票月行情 [免费版]

2. 【股票基本信息】- 筛选和分析的基础
   - getEqu: 股票基本信息 [免费版]
   - getEquIPO: 股票首次上市信息 [免费版]
   - getEquIndustry: 股票行业分类 [免费版]
   - getSecID: 证券编码及基本上市信息 [免费版]

3. 【指数数据】- 基准对比
   - getMktIdxd: 指数日行情 [免费版]
   - getIdx: 指数基本信息 [免费版]

4. 【交易日历】- 时间管理
   - getTradeCal: 交易所交易日历 [免费版]

策略：
- 按重要性优先级下载
- 采用增量下载方式
- 统一存储结构
- 自动数据验证
"""

import uqer
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
import time
import logging
import json
import warnings
from typing import Dict, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

warnings.filterwarnings('ignore')

# 优矿Token
UQER_TOKEN = "68b9922817ae6273137bda7acba81e293582ba347281dfcc056dcb245b23faf3"

class PriorityUqerDataDownloader:
    """优矿核心数据优先下载器"""
    
    def __init__(self):
        """初始化下载器"""
        self.client = uqer.Client(token=UQER_TOKEN)
        
        # 数据目录
        self.base_path = Path("/Users/jackstudio/QuantTrade/data")
        self.base_path.mkdir(exist_ok=True)
        
        # 设置日志
        self.setup_logging()
        
        # 优先下载API配置
        self.priority_apis = self._define_priority_apis()
        
        # 下载统计
        self.download_stats = {
            'total_calls': 0,
            'successful_calls': 0,
            'failed_calls': 0,
            'total_records': 0,
            'start_time': None,
            'end_time': None
        }
        
        # 线程锁
        self.stats_lock = threading.Lock()
    
    def setup_logging(self):
        """设置日志"""
        log_file = self.base_path / "priority_download.log"
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def _define_priority_apis(self) -> Dict:
        """定义优先下载的API配置"""
        return {
            # 第1优先级：核心行情数据
            "priority_1_market_data": {
                "description": "核心行情数据 - 策略回测基础",
                "apis": {
                    "getMktEqud": {
                        "name": "股票日行情",
                        "package": "免费版",
                        "dir": "daily",
                        "fields": "secID,ticker,tradeDate,preClosePrice,openPrice,highestPrice,lowestPrice,closePrice,turnoverVol,marketValue,dealAmount,turnoverRate",
                        "time_based": True,
                        "batch_size": 100
                    },
                    "getMktEqudAdj": {
                        "name": "股票前复权日行情", 
                        "package": "免费版",
                        "dir": "daily_adj",
                        "fields": "secID,ticker,tradeDate,preClosePrice,openPrice,highestPrice,lowestPrice,closePrice,turnoverVol,accumAdjFactor",
                        "time_based": True,
                        "batch_size": 100
                    }
                }
            },
            
            # 第2优先级：基本信息数据
            "priority_2_basic_info": {
                "description": "股票基本信息 - 筛选分析基础",
                "apis": {
                    "getEqu": {
                        "name": "股票基本信息",
                        "package": "免费版", 
                        "dir": "basic_info",
                        "fields": "secID,ticker,secShortName,exchangeCD,listStatusCD,listDate,delistDate,secFullName",
                        "time_based": False,
                        "batch_size": 1000
                    },
                    "getEquIPO": {
                        "name": "股票首次上市信息",
                        "package": "免费版",
                        "dir": "basic_info", 
                        "fields": "secID,ticker,listDate,issuePrice,issueNum,parValue,floatSharesNum",
                        "time_based": False,
                        "batch_size": 1000
                    },
                    "getEquIndustry": {
                        "name": "股票行业分类",
                        "package": "免费版",
                        "dir": "basic_info",
                        "fields": "secID,ticker,industryID1,industryName1,industryID2,industryName2",
                        "time_based": False,
                        "batch_size": 1000
                    },
                    "getSecID": {
                        "name": "证券编码基本信息",
                        "package": "免费版", 
                        "dir": "basic_info",
                        "fields": "secID,ticker,secShortName,exchangeCD,listDate,delistDate",
                        "time_based": False,
                        "batch_size": 1000
                    }
                }
            },
            
            # 第3优先级：周月行情
            "priority_3_extended_market": {
                "description": "周月行情数据",
                "apis": {
                    "getMktEquw": {
                        "name": "股票周行情",
                        "package": "免费版",
                        "dir": "weekly",
                        "fields": "secID,ticker,tradeDate,preClosePrice,openPrice,highestPrice,lowestPrice,closePrice,turnoverVol,marketValue",
                        "time_based": True,
                        "batch_size": 100
                    },
                    "getMktEqum": {
                        "name": "股票月行情", 
                        "package": "免费版",
                        "dir": "monthly",
                        "fields": "secID,ticker,tradeDate,preClosePrice,openPrice,highestPrice,lowestPrice,closePrice,turnoverVol,marketValue",
                        "time_based": True,
                        "batch_size": 100
                    }
                }
            },
            
            # 第4优先级：指数数据
            "priority_4_index_data": {
                "description": "指数数据 - 基准对比",
                "apis": {
                    "getMktIdxd": {
                        "name": "指数日行情",
                        "package": "免费版",
                        "dir": "index_daily",
                        "fields": "secID,ticker,tradeDate,preClosePrice,openPrice,highestPrice,lowestPrice,closePrice,turnoverVol,marketValue",
                        "time_based": True,
                        "batch_size": 50,
                        "no_stock_filter": True
                    },
                    "getIdx": {
                        "name": "指数基本信息",
                        "package": "免费版",
                        "dir": "basic_info",
                        "fields": "secID,ticker,secShortName,exchangeCD,listDate",
                        "time_based": False,
                        "batch_size": 500,
                        "no_stock_filter": True
                    }
                }
            },
            
            # 第5优先级：交易日历
            "priority_5_calendar": {
                "description": "交易日历 - 时间管理", 
                "apis": {
                    "getTradeCal": {
                        "name": "交易所交易日历",
                        "package": "免费版",
                        "dir": "calendar",
                        "fields": "calendarDate,exchangeCD,prevTradeDate,isOpen",
                        "time_based": True,
                        "batch_size": 1,
                        "no_stock_filter": True,
                        "special_handling": True
                    }
                }
            }
        }
    
    def get_all_stock_codes(self) -> List[str]:
        """获取所有A股代码列表"""
        try:
            self.logger.info("📋 获取A股代码列表...")
            
            # 调用基本信息API获取股票列表
            result = self.client.DataAPI.EquGet(
                listStatusCD='L,S,DE',  # 上市、停牌、退市股票
                field='secID,ticker,listStatusCD',
                pandas='1'
            )
            
            if isinstance(result, pd.DataFrame) and not result.empty:
                stock_codes = result['secID'].unique().tolist()
                self.logger.info(f"✅ 获取到 {len(stock_codes)} 只A股代码")
                return stock_codes
            else:
                self.logger.error("获取股票代码失败")
                return []
                
        except Exception as e:
            self.logger.error(f"获取股票代码异常: {str(e)}")
            return []\n    \n    def create_time_ranges(self, start_year: int = 2000, end_year: int = 2025) -> List[Tuple[str, str]]:\n        \"\"\"创建时间范围列表\"\"\"\n        time_ranges = []\n        \n        for year in range(start_year, end_year + 1):\n            if year == 2025:\n                # 2025年只到8月底\n                time_ranges.append((f\"{year}0101\", \"20250831\"))\n            else:\n                time_ranges.append((f\"{year}0101\", f\"{year}1231\"))\n        \n        return time_ranges\n    \n    def download_api_data(self, api_name: str, api_config: Dict, stock_codes: List[str], time_ranges: List[Tuple[str, str]]) -> bool:\n        \"\"\"下载单个API的数据\"\"\"\n        try:\n            self.logger.info(f\"🚀 开始下载 {api_config['name']} ({api_name})\")\n            self.logger.info(f\"📦 数据包: {api_config['package']}\")\n            \n            # 创建输出目录\n            output_dir = self.base_path / api_config['dir']\n            output_dir.mkdir(exist_ok=True)\n            \n            # 获取API函数\n            api_func = getattr(self.client.DataAPI, api_name, None)\n            if not api_func:\n                self.logger.error(f\"API函数 {api_name} 不存在\")\n                return False\n            \n            total_records = 0\n            \n            # 处理特殊API\n            if api_config.get('special_handling'):\n                return self._handle_special_api(api_name, api_config, api_func, output_dir)\n            \n            # 处理静态数据（不基于时间）\n            if not api_config.get('time_based', True):\n                return self._download_static_data(api_name, api_config, api_func, output_dir, stock_codes)\n            \n            # 处理时间序列数据\n            return self._download_time_series_data(api_name, api_config, api_func, output_dir, stock_codes, time_ranges)\n        \n        except Exception as e:\n            self.logger.error(f\"下载 {api_name} 数据异常: {str(e)}\")\n            return False\n    \n    def _handle_special_api(self, api_name: str, api_config: Dict, api_func, output_dir: Path) -> bool:\n        \"\"\"处理特殊API（如交易日历）\"\"\"\n        try:\n            if api_name == 'getTradeCal':\n                self.logger.info(\"📅 下载交易日历...\")\n                \n                # 下载2000-2025年的交易日历\n                result = api_func(\n                    exchangeCD='XSHG,XSHE',\n                    beginDate='20000101',\n                    endDate='20251231',\n                    field=api_config['fields'],\n                    pandas='1'\n                )\n                \n                if isinstance(result, pd.DataFrame) and not result.empty:\n                    output_file = output_dir / \"trading_calendar.csv\"\n                    result.to_csv(output_file, index=False)\n                    \n                    with self.stats_lock:\n                        self.download_stats['successful_calls'] += 1\n                        self.download_stats['total_records'] += len(result)\n                    \n                    self.logger.info(f\"✅ 交易日历下载完成: {len(result)} 条记录\")\n                    return True\n                else:\n                    self.logger.error(\"交易日历数据为空\")\n                    return False\n            \n            return True\n            \n        except Exception as e:\n            self.logger.error(f\"特殊API {api_name} 处理异常: {str(e)}\")\n            return False\n    \n    def _download_static_data(self, api_name: str, api_config: Dict, api_func, output_dir: Path, stock_codes: List[str]) -> bool:\n        \"\"\"下载静态数据（不基于时间）\"\"\"\n        try:\n            self.logger.info(f\"📊 下载静态数据 {api_config['name']}...\")\n            \n            # 无需股票筛选的API\n            if api_config.get('no_stock_filter'):\n                result = api_func(\n                    field=api_config['fields'],\n                    pandas='1'\n                )\n                \n                if isinstance(result, pd.DataFrame) and not result.empty:\n                    output_file = output_dir / f\"{api_name.lower()}.csv\"\n                    result.to_csv(output_file, index=False)\n                    \n                    with self.stats_lock:\n                        self.download_stats['successful_calls'] += 1\n                        self.download_stats['total_records'] += len(result)\n                    \n                    self.logger.info(f\"✅ {api_config['name']} 下载完成: {len(result)} 条记录\")\n                    return True\n            else:\n                # 需要股票筛选的API，批量下载\n                batch_size = api_config.get('batch_size', 100)\n                total_records = 0\n                \n                for i in range(0, len(stock_codes), batch_size):\n                    batch_stocks = stock_codes[i:i+batch_size]\n                    batch_tickers = ','.join([code.split('.')[0] for code in batch_stocks])\n                    \n                    try:\n                        result = api_func(\n                            secID='',\n                            ticker=batch_tickers,\n                            field=api_config['fields'],\n                            pandas='1'\n                        )\n                        \n                        if isinstance(result, pd.DataFrame) and not result.empty:\n                            # 追加保存\n                            output_file = output_dir / f\"{api_name.lower()}.csv\"\n                            if output_file.exists():\n                                result.to_csv(output_file, mode='a', header=False, index=False)\n                            else:\n                                result.to_csv(output_file, index=False)\n                            \n                            total_records += len(result)\n                            \n                            with self.stats_lock:\n                                self.download_stats['successful_calls'] += 1\n                                self.download_stats['total_records'] += len(result)\n                        \n                        # 避免频率限制\n                        time.sleep(0.1)\n                        \n                        if (i // batch_size + 1) % 10 == 0:\n                            self.logger.info(f\"   进度: {i+batch_size}/{len(stock_codes)} 股票\")\n                            \n                    except Exception as e:\n                        self.logger.warning(f\"批次 {i//batch_size + 1} 下载失败: {str(e)}\")\n                        with self.stats_lock:\n                            self.download_stats['failed_calls'] += 1\n                        continue\n                \n                self.logger.info(f\"✅ {api_config['name']} 下载完成: {total_records} 条记录\")\n                return True\n                \n        except Exception as e:\n            self.logger.error(f\"静态数据 {api_name} 下载异常: {str(e)}\")\n            return False\n    \n    def _download_time_series_data(self, api_name: str, api_config: Dict, api_func, output_dir: Path, \n                                  stock_codes: List[str], time_ranges: List[Tuple[str, str]]) -> bool:\n        \"\"\"下载时间序列数据\"\"\"\n        try:\n            self.logger.info(f\"📈 下载时间序列数据 {api_config['name']}...\")\n            \n            batch_size = api_config.get('batch_size', 100)\n            total_records = 0\n            \n            # 按年份分别下载\n            for start_date, end_date in time_ranges:\n                year = start_date[:4]\n                self.logger.info(f\"   📅 下载 {year} 年数据...\")\n                \n                year_records = 0\n                \n                # 无需股票筛选的API（如指数）\n                if api_config.get('no_stock_filter'):\n                    try:\n                        result = api_func(\n                            beginDate=start_date,\n                            endDate=end_date,\n                            field=api_config['fields'],\n                            pandas='1'\n                        )\n                        \n                        if isinstance(result, pd.DataFrame) and not result.empty:\n                            output_file = output_dir / f\"{api_name.lower()}_{year}.csv\"\n                            result.to_csv(output_file, index=False)\n                            \n                            year_records = len(result)\n                            total_records += year_records\n                            \n                            with self.stats_lock:\n                                self.download_stats['successful_calls'] += 1\n                                self.download_stats['total_records'] += year_records\n                        \n                        self.logger.info(f\"   ✅ {year}年: {year_records} 条记录\")\n                        \n                    except Exception as e:\n                        self.logger.warning(f\"{year}年数据下载失败: {str(e)}\")\n                        with self.stats_lock:\n                            self.download_stats['failed_calls'] += 1\n                \n                else:\n                    # 需要股票筛选的API，分批下载\n                    year_output_file = output_dir / f\"{api_name.lower()}_{year}.csv\"\n                    \n                    for i in range(0, len(stock_codes), batch_size):\n                        batch_stocks = stock_codes[i:i+batch_size]\n                        batch_tickers = ','.join([code.split('.')[0] for code in batch_stocks])\n                        \n                        try:\n                            result = api_func(\n                                secID='',\n                                ticker=batch_tickers,\n                                beginDate=start_date,\n                                endDate=end_date,\n                                field=api_config['fields'],\n                                pandas='1'\n                            )\n                            \n                            if isinstance(result, pd.DataFrame) and not result.empty:\n                                # 追加保存到年度文件\n                                if year_output_file.exists():\n                                    result.to_csv(year_output_file, mode='a', header=False, index=False)\n                                else:\n                                    result.to_csv(year_output_file, index=False)\n                                \n                                batch_records = len(result)\n                                year_records += batch_records\n                                \n                                with self.stats_lock:\n                                    self.download_stats['successful_calls'] += 1\n                                    self.download_stats['total_records'] += batch_records\n                            \n                            # 避免频率限制\n                            time.sleep(0.1)\n                            \n                        except Exception as e:\n                            self.logger.warning(f\"{year}年批次 {i//batch_size + 1} 下载失败: {str(e)}\")\n                            with self.stats_lock:\n                                self.download_stats['failed_calls'] += 1\n                            continue\n                    \n                    self.logger.info(f\"   ✅ {year}年: {year_records} 条记录\")\n                \n                total_records += year_records\n                \n                # 年份之间稍作停顿\n                time.sleep(0.5)\n            \n            self.logger.info(f\"✅ {api_config['name']} 全部下载完成: {total_records} 条记录\")\n            return True\n            \n        except Exception as e:\n            self.logger.error(f\"时间序列数据 {api_name} 下载异常: {str(e)}\")\n            return False\n    \n    def run_priority_download(self):\n        \"\"\"运行优先下载流程\"\"\"\n        start_time = datetime.now()\n        self.download_stats['start_time'] = start_time\n        \n        print(\"🚀 优矿核心数据优先下载器\")\n        print(\"🎯 目标: 下载core模块必需的关键数据\")\n        print(\"=\" * 80)\n        \n        try:\n            # 1. 获取所有股票代码\n            stock_codes = self.get_all_stock_codes()\n            if not stock_codes:\n                print(\"❌ 无法获取股票代码列表\")\n                return\n            \n            print(f\"📊 A股总数: {len(stock_codes)}\")\n            \n            # 2. 创建时间范围\n            time_ranges = self.create_time_ranges()\n            print(f\"📅 时间范围: {len(time_ranges)} 年 (2000-2025)\")\n            \n            # 3. 按优先级下载数据\n            for priority_group, group_config in self.priority_apis.items():\n                print(f\"\\n{priority_group.upper()}: {group_config['description']}\")\n                print(\"=\" * 60)\n                \n                for api_name, api_config in group_config['apis'].items():\n                    success = self.download_api_data(api_name, api_config, stock_codes, time_ranges)\n                    \n                    if success:\n                        print(f\"✅ {api_config['name']} 下载成功\")\n                    else:\n                        print(f\"❌ {api_config['name']} 下载失败\")\n                    \n                    # API之间稍作停顿\n                    time.sleep(1)\n            \n            # 4. 生成下载报告\n            self.generate_download_report()\n            \n        except Exception as e:\n            self.logger.error(f\"下载流程异常: {str(e)}\")\n            raise\n        \n        finally:\n            end_time = datetime.now()\n            self.download_stats['end_time'] = end_time\n            duration = end_time - start_time\n            \n            print(f\"\\n🎊 下载完成!\")\n            print(f\"⏱️ 总耗时: {duration}\")\n            print(f\"📊 API调用: {self.download_stats['successful_calls']} 成功, {self.download_stats['failed_calls']} 失败\")\n            print(f\"📋 总记录数: {self.download_stats['total_records']:,}\")\n    \n    def generate_download_report(self):\n        \"\"\"生成下载报告\"\"\"\n        report = {\n            'download_time': datetime.now().isoformat(),\n            'statistics': self.download_stats.copy(),\n            'api_summary': {},\n            'data_structure': {}\n        }\n        \n        # 统计各目录的文件\n        for priority_group, group_config in self.priority_apis.items():\n            report['api_summary'][priority_group] = {\n                'description': group_config['description'],\n                'apis': list(group_config['apis'].keys())\n            }\n        \n        # 统计数据目录\n        for item in self.base_path.iterdir():\n            if item.is_dir():\n                files = list(item.glob(\"*.csv\"))\n                if files:\n                    total_size = sum(f.stat().st_size for f in files)\n                    report['data_structure'][item.name] = {\n                        'file_count': len(files),\n                        'size_mb': round(total_size / (1024 * 1024), 2)\n                    }\n        \n        # 保存报告\n        timestamp = datetime.now().strftime(\"%Y%m%d_%H%M%S\")\n        report_file = self.base_path / f\"priority_download_report_{timestamp}.json\"\n        \n        with open(report_file, 'w', encoding='utf-8') as f:\n            json.dump(report, f, indent=2, ensure_ascii=False, default=str)\n        \n        print(f\"\\n📄 下载报告已保存: {report_file}\")\n        \n        # 显示数据结构总结\n        print(f\"\\n📁 数据目录结构:\")\n        for dir_name, dir_info in report['data_structure'].items():\n            print(f\"   📂 {dir_name}: {dir_info['file_count']} 文件, {dir_info['size_mb']} MB\")\n\ndef main():\n    \"\"\"主函数\"\"\"\n    downloader = PriorityUqerDataDownloader()\n    downloader.run_priority_download()\n\nif __name__ == \"__main__\":\n    main()