#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
最终版综合API数据下载器 - 58个核心优矿API接口
=============================================

基于测试验证的正确API函数名，下载以下数据：
✅ 基础股票信息 (7个)
✅ 财务数据 (12个)  
✅ 特殊交易数据 (17个)
✅ 股东治理数据 (22个)

时间范围：2000年-2025年
智能处理：按上市时间筛选股票，避免空数据
"""

import uqer
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
import time
import logging
import json

UQER_TOKEN = "68b9922817ae6273137bda7acba81e293582ba347281dfcc056dcb245b23faf3"

class FinalComprehensiveDownloader:
    """最终版综合API数据下载器"""
    
    def __init__(self):
        self.client = uqer.Client(token=UQER_TOKEN)
        self.data_dir = Path("data/final_comprehensive_download")
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        # 全部58个API接口配置（基于测试验证的正确函数名）
        self.api_configs = self._define_verified_apis()
        
        # 配置日志
        log_file = self.data_dir / "final_download.log"
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
        
        # 进度追踪
        self.progress_file = self.data_dir / "download_progress.json"
        self.progress_data = self._load_progress()
        
    def _define_verified_apis(self):
        """定义经过验证的58个API接口配置"""
        return {
            # 1. 基础股票信息 (7个)
            "basic_info": {
                "priority": 1,
                "apis": {
                    "EquGet": {
                        "desc": "股票基本信息",
                        "params": {},
                        "static": True
                    },
                    "MktIdxdGet": {
                        "desc": "指数日行情", 
                        "params": {},
                        "time_range": True,
                        "no_stock": True
                    },
                    "EquIPOGet": {
                        "desc": "股票首次上市信息",
                        "params": {},
                        "static": True
                    },
                    "EquIndustryGet": {
                        "desc": "股票行业分类",
                        "params": {},
                        "static": True
                    },
                    "SecIDGet": {
                        "desc": "证券编码及基本信息",
                        "params": {},
                        "static": True
                    },
                    "EquDivGet": {
                        "desc": "股票分红信息",
                        "params": {},
                        "time_range": True
                    },
                    "EquSplitsGet": {
                        "desc": "股票拆股信息",
                        "params": {},
                        "time_range": True
                    }
                }
            },
            
            # 2. 财务数据 (12个)
            "financial": {
                "priority": 2,
                "apis": {
                    "FdmtBSAllLatestGet": {
                        "desc": "合并资产负债表(最新披露)",
                        "params": {},
                        "time_range": True
                    },
                    "FdmtBSBankAllLatestGet": {
                        "desc": "银行业资产负债表(最新披露)",
                        "params": {},
                        "time_range": True
                    },
                    "FdmtBSInduAllLatestGet": {
                        "desc": "一般工商业资产负债表(最新披露)",
                        "params": {},
                        "time_range": True
                    },
                    "FdmtISAllLatestGet": {
                        "desc": "合并利润表(最新披露)",
                        "params": {},
                        "time_range": True
                    },
                    "FdmtISBankAllLatestGet": {
                        "desc": "银行业利润表(最新披露)",
                        "params": {},
                        "time_range": True
                    },
                    "FdmtISInduAllLatestGet": {
                        "desc": "一般工商业利润表(最新披露)",
                        "params": {},
                        "time_range": True
                    },
                    "FdmtCFAllLatestGet": {
                        "desc": "合并现金流量表(最新披露)",
                        "params": {},
                        "time_range": True
                    },
                    "FdmtCFBankAllLatestGet": {
                        "desc": "银行业现金流量表(最新披露)",
                        "params": {},
                        "time_range": True
                    },
                    "FdmtCFInduAllLatestGet": {
                        "desc": "一般工商业现金流量表(最新披露)",
                        "params": {},
                        "time_range": True
                    },
                    "FdmtDerGet": {
                        "desc": "财务衍生数据",
                        "params": {},
                        "time_range": True
                    },
                    "FdmtIndiPSGet": {
                        "desc": "财务指标—每股指标",
                        "params": {},
                        "time_range": True
                    },
                    "FdmtIndiGrowthGet": {
                        "desc": "财务指标—成长能力",
                        "params": {},
                        "time_range": True
                    }
                }
            },
            
            # 3. 特殊交易数据 (17个)
            "special_trading": {
                "priority": 3,
                "apis": {
                    "MktRankListStocksGet": {
                        "desc": "沪深交易公开信息_股票(龙虎榜)",
                        "params": {},
                        "time_range": True
                    },
                    "MktRankListSalesGet": {
                        "desc": "沪深交易公开信息_营业部(龙虎榜)", 
                        "params": {},
                        "time_range": True
                    },
                    "MktLimitGet": {
                        "desc": "沪深涨跌停限制",
                        "params": {},
                        "time_range": True
                    },
                    "MktBlockdGet": {
                        "desc": "沪深大宗交易",
                        "params": {},
                        "time_range": True
                    },
                    "FstTotalGet": {
                        "desc": "沪深融资融券每日汇总信息",
                        "params": {},
                        "time_range": True
                    },
                    "FstDetailGet": {
                        "desc": "沪深融资融券每日交易明细信息",
                        "params": {},
                        "time_range": True
                    },
                    "VfsttargetGet": {
                        "desc": "融资融券标的证券",
                        "params": {},
                        "static": True
                    },
                    "EquMarginSecGet": {
                        "desc": "可充抵保证金证券",
                        "params": {},
                        "static": True
                    },
                    "EquIsActivityGet": {
                        "desc": "A股机构调研活动统计",
                        "params": {},
                        "time_range": True
                    },
                    "EquIsParticipantQaGet": {
                        "desc": "A股机构调研活动明细", 
                        "params": {},
                        "time_range": True
                    },
                    "MktIpoConTraddaysGet": {
                        "desc": "新股上市连板天数",
                        "params": {},
                        "time_range": True
                    },
                    "MktRankDivYieldGet": {
                        "desc": "沪深股息率排名",
                        "params": {},
                        "time_range": True
                    },
                    "MktRANKInstTrGet": {
                        "desc": "行业成分换手率排名",
                        "params": {},
                        "time_range": True,
                        "no_stock": True
                    },
                    "MktEquPerfGet": {
                        "desc": "每日股票最新表现",
                        "params": {},
                        "time_range": True
                    },
                    "MktEqudStatsGet": {
                        "desc": "股票市场统计",
                        "params": {},
                        "time_range": True,
                        "no_stock": True
                    },
                    "MktConsBondPremiumGet": {
                        "desc": "可转债折溢价信息", 
                        "params": {},
                        "time_range": True
                    },
                    "SecHaltGet": {
                        "desc": "沪深证券停复牌",
                        "params": {},
                        "time_range": True
                    }
                }
            },
            
            # 4. 股东治理数据 (22个)
            "governance": {
                "priority": 4,
                "apis": {
                    # 股东核心信息
                    "EquShareholderNumGet": {
                        "desc": "上市公司股东户数",
                        "params": {},
                        "quarterly": True
                    },
                    "EquShTenGet": {
                        "desc": "公司十大股东",
                        "params": {},
                        "quarterly": True
                    },
                    "EquFloatShTenGet": {
                        "desc": "公司十大流通股东",
                        "params": {},
                        "quarterly": True
                    },
                    "EquActualControllerGet": {
                        "desc": "上市公司实际控制人",
                        "params": {},
                        "static": True
                    },
                    "EquShareholdersMeetingGet": {
                        "desc": "股东大会召开信息",
                        "params": {},
                        "time_range": True
                    },
                    "EquOldShofferGet": {
                        "desc": "老股东公开发售明细",
                        "params": {},
                        "time_range": True
                    },
                    "EquMsChangesGet": {
                        "desc": "高管及相关人员持股变动",
                        "params": {},
                        "time_range": True
                    },
                    "EquChangePlanGet": {
                        "desc": "股东增减持计划",
                        "params": {},
                        "time_range": True
                    },
                    
                    # 高管治理信息
                    "EquManagersGet": {
                        "desc": "上市公司管理层",
                        "params": {},
                        "static": True
                    },
                    "EquExecsHoldingsGet": {
                        "desc": "公司高管持股薪酬明细",
                        "params": {},
                        "time_range": True
                    },
                    "EquRelatedTransactionGet": {
                        "desc": "上市公司关联交易",
                        "params": {},
                        "time_range": True
                    },
                    
                    # 股权质押与限售
                    "EquStockPledgeGet": {
                        "desc": "股票周质押信息",
                        "params": {},
                        "time_range": True
                    },
                    "EquPledgeGet": {
                        "desc": "A股公司股权质押",
                        "params": {},
                        "time_range": True
                    },
                    "EquSharesFloatGet": {
                        "desc": "限售股解禁",
                        "params": {},
                        "time_range": True
                    },
                    "EquIpoShareFloatGet": {
                        "desc": "首发限售解禁明细",
                        "params": {},
                        "time_range": True
                    },
                    "EquReformShareFloatGet": {
                        "desc": "股改限售解禁明细",
                        "params": {},
                        "time_range": True
                    },
                    
                    # 资本运作相关
                    "EquAllotGet": {
                        "desc": "股票配股信息",
                        "params": {},
                        "time_range": True
                    },
                    "EquSpoGet": {
                        "desc": "增发信息",
                        "params": {},
                        "time_range": True
                    },
                    "EquAllotmentSubscriptionResultsGet": {
                        "desc": "配股认购结果表",
                        "params": {},
                        "time_range": True
                    },
                    "EquSpoPubResultGet": {
                        "desc": "公开增发中签率及配售结果", 
                        "params": {},
                        "time_range": True
                    },
                    "EquSharesExcitGet": {
                        "desc": "股权激励基本资料",
                        "params": {},
                        "time_range": True
                    },
                    
                    # 特殊标记
                    "EquPartyNatureGet": {
                        "desc": "个股企业性质",
                        "params": {},
                        "static": True
                    }
                }
            }
        }
    
    def _load_progress(self):
        """加载下载进度"""
        if self.progress_file.exists():
            try:
                with open(self.progress_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except:
                pass
        
        return {
            "completed_apis": [],
            "failed_apis": [],
            "last_update": None,
            "statistics": {
                "success_count": 0,
                "failed_count": 0,
                "total_records": 0,
                "start_time": None,
                "estimated_completion": None
            }
        }
    
    def _save_progress(self):
        """保存下载进度"""
        self.progress_data["last_update"] = datetime.now().isoformat()
        with open(self.progress_file, 'w', encoding='utf-8') as f:
            json.dump(self.progress_data, f, indent=2, ensure_ascii=False)
    
    def get_stocks_with_listing_info(self):
        """获取股票及上市信息"""
        logging.info("📋 获取股票上市信息...")
        
        try:
            stocks = uqer.DataAPI.EquGet()
            
            if stocks is not None and not stocks.empty:
                # 过滤A股
                a_stocks = stocks[stocks['listStatusCD'] == 'L'].copy()
                
                # 处理上市日期
                a_stocks['listDate'] = pd.to_datetime(a_stocks['listDate'])
                a_stocks['listYear'] = a_stocks['listDate'].dt.year
                
                logging.info(f"✅ 获取股票信息成功: {len(a_stocks)} 只A股")
                return a_stocks
            
        except Exception as e:
            logging.error(f"❌ 获取股票信息失败: {e}")
            return None
    
    def get_stocks_for_year(self, stocks, year):
        """获取指定年份已上市的股票"""
        year_stocks = stocks[stocks['listYear'] <= year].copy()
        return year_stocks
    
    def download_single_api(self, api_name, api_config, category, stocks=None):
        """下载单个API数据"""
        desc = api_config["desc"]
        
        # 检查是否已完成
        api_key = f"{category}_{api_name}"
        if api_key in self.progress_data["completed_apis"]:
            logging.info(f"⏭️ {desc} 已完成，跳过")
            return True
        
        # 创建数据目录
        data_dir = self.data_dir / category / api_name.lower()
        data_dir.mkdir(parents=True, exist_ok=True)
        
        logging.info(f"📥 开始下载 {desc}...")
        
        try:
            # 获取API函数
            api_func = getattr(uqer.DataAPI, api_name, None)
            if not api_func:
                logging.error(f"❌ API函数 {api_name} 不存在")
                self.progress_data["failed_apis"].append(api_key)
                self._save_progress()
                return False
            
            if api_config.get("static"):
                return self._download_static_api(api_func, api_config, desc, data_dir, api_key)
            elif api_config.get("quarterly"):
                return self._download_quarterly_api(api_func, api_config, desc, data_dir, api_key, stocks)
            elif api_config.get("time_range"):
                return self._download_time_range_api(api_func, api_config, desc, data_dir, api_key, stocks)
            else:
                return self._download_other_api(api_func, api_config, desc, data_dir, api_key)
            
        except Exception as e:
            logging.error(f"❌ {desc} 下载失败: {e}")
            self.progress_data["failed_apis"].append(api_key)
            self._save_progress()
            return False
    
    def _download_static_api(self, api_func, api_config, desc, data_dir, api_key):
        """下载静态数据"""
        try:
            params = api_config["params"].copy()
            data = api_func(**params)
            
            if data is not None and not data.empty:
                file_path = data_dir / "static_data.csv"
                data.to_csv(file_path, index=False)
                
                logging.info(f"✅ {desc}: {len(data)} 条记录")
                self.progress_data["completed_apis"].append(api_key)
                self.progress_data["statistics"]["success_count"] += 1
                self.progress_data["statistics"]["total_records"] += len(data)
                self._save_progress()
                return True
            else:
                logging.warning(f"⚠️ {desc}: 无数据")
                return False
                
        except Exception as e:
            logging.error(f"❌ {desc} 静态数据下载失败: {e}")
            return False
    
    def _download_quarterly_api(self, api_func, api_config, desc, data_dir, api_key, stocks):
        """下载季度数据（如股东户数等）"""
        success_quarters = 0
        total_records = 0
        
        # 季度端点：每年4个季度
        quarters = [
            ("0331", "Q1"), ("0630", "Q2"), 
            ("0930", "Q3"), ("1231", "Q4")
        ]
        
        for year in range(2000, 2026):
            if year > datetime.now().year:
                break
                
            for quarter_end, quarter_name in quarters:
                quarter_file = data_dir / f"year_{year}_{quarter_name}.csv"
                if quarter_file.exists():
                    existing_data = pd.read_csv(quarter_file)
                    total_records += len(existing_data)
                    success_quarters += 1
                    continue
                
                try:
                    params = api_config["params"].copy()
                    params["endDate"] = f"{year}{quarter_end}"
                    
                    data = api_func(**params)
                    if data is not None and not data.empty:
                        data.to_csv(quarter_file, index=False)
                        total_records += len(data)
                        success_quarters += 1
                        logging.info(f"✅ {desc} {year}{quarter_name}: {len(data)} 条记录")
                    
                    time.sleep(0.3)  # API限制
                    
                except Exception as e:
                    logging.error(f"❌ {desc} {year}{quarter_name} 下载失败: {e}")
                    continue
        
        if success_quarters > 0:
            logging.info(f"📊 {desc} 完成: {success_quarters} 季度, 总计 {total_records} 条记录")
            self.progress_data["completed_apis"].append(api_key)
            self.progress_data["statistics"]["success_count"] += 1
            self.progress_data["statistics"]["total_records"] += total_records
            self._save_progress()
            return True
        else:
            return False
    
    def _download_time_range_api(self, api_func, api_config, desc, data_dir, api_key, stocks):
        """下载时间范围数据"""
        success_years = 0
        total_records = 0
        
        for download_year in range(2000, 2026):
            if download_year > datetime.now().year:
                break
                
            year_file = data_dir / f"year_{download_year}.csv"
            if year_file.exists():
                existing_data = pd.read_csv(year_file)
                total_records += len(existing_data)
                success_years += 1
                continue
            
            try:
                params = api_config["params"].copy()
                params["beginDate"] = f"{download_year}0101" 
                params["endDate"] = f"{download_year}1231"
                
                # 对于需要股票筛选的API，分批下载
                if not api_config.get("no_stock") and stocks is not None:
                    year_stocks = self.get_stocks_for_year(stocks, download_year)
                    if len(year_stocks) == 0:
                        continue
                    
                    # 分批处理，每批100只股票
                    year_data = []
                    batch_size = 100
                    batches = [year_stocks[i:i+batch_size] for i in range(0, len(year_stocks), batch_size)]
                    
                    for batch_stocks in batches:
                        batch_params = params.copy()
                        batch_params["ticker"] = ','.join(batch_stocks['ticker'].tolist())
                        
                        try:
                            batch_data = api_func(**batch_params)
                            if batch_data is not None and not batch_data.empty:
                                year_data.append(batch_data)
                        except:
                            continue
                        
                        time.sleep(0.2)  # API限制
                    
                    if year_data:
                        combined_data = pd.concat(year_data, ignore_index=True)
                        combined_data.to_csv(year_file, index=False)
                        total_records += len(combined_data)
                        success_years += 1
                        logging.info(f"✅ {desc} {download_year}年: {len(combined_data)} 条记录")
                else:
                    # 不需要股票筛选的数据
                    data = api_func(**params)
                    if data is not None and not data.empty:
                        data.to_csv(year_file, index=False)
                        total_records += len(data)
                        success_years += 1
                        logging.info(f"✅ {desc} {download_year}年: {len(data)} 条记录")
                
                time.sleep(0.3)  # API限制
                
            except Exception as e:
                logging.error(f"❌ {desc} {download_year}年 下载失败: {e}")
                continue
        
        if success_years > 0:
            logging.info(f"📊 {desc} 完成: {success_years}/26 年, 总计 {total_records} 条记录")
            self.progress_data["completed_apis"].append(api_key)
            self.progress_data["statistics"]["success_count"] += 1
            self.progress_data["statistics"]["total_records"] += total_records
            self._save_progress()
            return True
        else:
            return False
    
    def _download_other_api(self, api_func, api_config, desc, data_dir, api_key):
        """下载其他类型数据"""
        try:
            params = api_config["params"].copy()
            data = api_func(**params)
            
            if data is not None and not data.empty:
                file_path = data_dir / "other_data.csv"
                data.to_csv(file_path, index=False)
                
                logging.info(f"✅ {desc}: {len(data)} 条记录")
                self.progress_data["completed_apis"].append(api_key)
                self.progress_data["statistics"]["success_count"] += 1
                self.progress_data["statistics"]["total_records"] += len(data)
                self._save_progress()
                return True
            else:
                logging.warning(f"⚠️ {desc}: 无数据")
                return False
                
        except Exception as e:
            logging.error(f"❌ {desc} 其他数据下载失败: {e}")
            return False
    
    def execute_final_download(self):
        """执行最终下载"""
        start_time = datetime.now()
        self.progress_data["statistics"]["start_time"] = start_time.isoformat()
        
        total_apis = sum(len(cat["apis"]) for cat in self.api_configs.values())
        logging.info(f"🚀 开始执行最终版综合API数据下载...")
        logging.info(f"📊 总计 {total_apis} 个API接口")
        logging.info(f"⏰ 开始时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # 获取股票信息
        all_stocks = self.get_stocks_with_listing_info()
        if all_stocks is None:
            logging.error("❌ 无法获取股票信息，终止下载")
            return False
        
        # 按优先级下载
        for category_name, category_config in self.api_configs.items():
            priority = category_config["priority"]
            apis = category_config["apis"]
            
            logging.info(f"\n🎯 开始下载分类: {category_name} (优先级: {priority})")
            logging.info(f"📋 包含 {len(apis)} 个API接口")
            
            category_success = 0
            for api_name, api_config in apis.items():
                success = self.download_single_api(api_name, api_config, category_name, stocks=all_stocks)
                if success:
                    category_success += 1
                
                time.sleep(0.5)  # API限制
            
            logging.info(f"📊 分类 {category_name} 完成: {category_success}/{len(apis)} 个接口")
        
        # 生成最终报告
        self._generate_final_report(start_time)
        
        logging.info("🎉 最终版综合API数据下载完成!")
        return True
    
    def _generate_final_report(self, start_time):
        """生成最终下载报告"""
        end_time = datetime.now()
        duration = end_time - start_time
        
        total_apis = sum(len(cat["apis"]) for cat in self.api_configs.values())
        
        report = {
            "download_summary": {
                "start_time": start_time.isoformat(),
                "completion_time": end_time.isoformat(),
                "duration_hours": duration.total_seconds() / 3600,
                "date_range": "2000年-2025年",
                "total_apis": total_apis,
                "completed_apis": len(self.progress_data["completed_apis"]),
                "failed_apis": len(self.progress_data["failed_apis"]),
                "success_rate": len(self.progress_data["completed_apis"]) / total_apis,
                "total_records": self.progress_data["statistics"]["total_records"]
            },
            "api_categories": {
                "basic_info": 7,
                "financial": 12,
                "special_trading": 17, 
                "governance": 22
            },
            "completed_apis": self.progress_data["completed_apis"],
            "failed_apis": self.progress_data["failed_apis"]
        }
        
        report_file = self.data_dir / "final_download_report.json"
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        logging.info(f"📄 最终报告保存至: {report_file}")


if __name__ == "__main__":
    downloader = FinalComprehensiveDownloader()
    
    # 执行下载
    success = downloader.execute_final_download()
    
    if success:
        stats = downloader.progress_data["statistics"]
        print(f"\n🎉 最终版综合API数据下载任务完成!")
        print(f"✅ 成功: {stats['success_count']} 个接口")
        print(f"❌ 失败: {len(downloader.progress_data['failed_apis'])} 个接口") 
        print(f"📈 总记录数: {stats['total_records']:,} 条")
        if stats.get('start_time'):
            start_time = datetime.fromisoformat(stats['start_time'])
            duration = datetime.now() - start_time
            print(f"⏰ 总耗时: {duration.total_seconds()/3600:.1f} 小时")
    else:
        print("\n❌ 最终版综合API数据下载任务失败")