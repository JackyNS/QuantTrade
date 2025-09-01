#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
综合API数据下载器 - 58个优矿API接口全量下载
=====================================

覆盖股票投资分析的全部维度：
- 基础股票信息 (19个)
- 特殊交易数据 (17个) 
- 股东治理数据 (24个)

时间范围：2000年-2025年
智能处理：只下载已上市股票，避免空数据
"""

import uqer
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
import time
import logging
import json

UQER_TOKEN = "68b9922817ae6273137bda7acba81e293582ba347281dfcc056dcb245b23faf3"

class ComprehensiveAPIDownloader:
    """综合API数据下载器"""
    
    def __init__(self):
        self.client = uqer.Client(token=UQER_TOKEN)
        self.data_dir = Path("data/comprehensive_api_download")
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        # 所有58个API接口配置
        self.api_configs = self._define_all_apis()
        
        # 配置日志
        log_file = self.data_dir / "comprehensive_download.log"
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
        
    def _define_all_apis(self):
        """定义全部58个API接口配置"""
        return {
            # 1. 基础股票信息 (19个) - 已在综合下载计划中定义的基础接口
            "basic_stock_info": {
                "priority": 1,
                "apis": {
                    "MktIdxdGet": {
                        "desc": "指数日行情",
                        "params": {"field": "secID,ticker,tradeDate,preClosePrice,openPrice,highestPrice,lowestPrice,closePrice,turnoverVol,marketValue"},
                        "time_range": True,
                        "no_stock_filter": True  # 指数不需要股票筛选
                    },
                    "EquGet": {
                        "desc": "股票基本信息", 
                        "params": {"field": "secID,ticker,secShortName,exchangeCD,listStatusCD,listDate,delistDate"},
                        "time_range": False,
                        "static": True
                    },
                    "EquIPOGet": {
                        "desc": "股票首次上市信息",
                        "params": {"field": "secID,ticker,listDate,issuePrice,totalShares,floatShares"},
                        "time_range": False,
                        "static": True
                    },
                    "EquIndustryGet": {
                        "desc": "股票行业分类",
                        "params": {"field": "secID,ticker,industryID,industryName,industryLevel"},
                        "time_range": False,
                        "static": True
                    },
                    "SecIDGet": {
                        "desc": "证券编码及基本上市信息",
                        "params": {"field": "secID,ticker,secShortName,exchangeCD,listDate"},
                        "time_range": False,
                        "static": True
                    },
                    "EquDivGet": {
                        "desc": "股票分红信息",
                        "params": {"field": "secID,ticker,exDivDate,divCash,divCashAfterTax"},
                        "time_range": True
                    },
                    "EquSplitsGet": {
                        "desc": "股票拆股信息", 
                        "params": {"field": "secID,ticker,exSplitDate,splitRatio"},
                        "time_range": True
                    }
                }
            },
            
            # 2. 财务数据 (12个) - 核心财务指标
            "financial_data": {
                "priority": 2,
                "apis": {
                    "getFdmtBSAllLatest": {
                        "desc": "合并资产负债表(最新披露)",
                        "params": {"field": "secID,ticker,publishDate,endDate,totalAssets,totalLiab,totalEquity"},
                        "time_range": True
                    },
                    "getFdmtBSBankAllLatest": {
                        "desc": "银行业资产负债表(最新披露)",
                        "params": {"field": "secID,ticker,publishDate,endDate,totalAssets,totalLiab,totalEquity"},
                        "time_range": True
                    },
                    "getFdmtBSInduAllLatest": {
                        "desc": "一般工商业资产负债表(最新披露)",
                        "params": {"field": "secID,ticker,publishDate,endDate,totalAssets,totalLiab,totalEquity"},
                        "time_range": True
                    },
                    "getFdmtISAllLatest": {
                        "desc": "合并利润表(最新披露)",
                        "params": {"field": "secID,ticker,publishDate,endDate,revenue,netProfit,operatingProfit"},
                        "time_range": True
                    },
                    "getFdmtISBankAllLatest": {
                        "desc": "银行业利润表(最新披露)", 
                        "params": {"field": "secID,ticker,publishDate,endDate,revenue,netProfit,operatingProfit"},
                        "time_range": True
                    },
                    "getFdmtISInduAllLatest": {
                        "desc": "一般工商业利润表(最新披露)",
                        "params": {"field": "secID,ticker,publishDate,endDate,revenue,netProfit,operatingProfit"},
                        "time_range": True
                    },
                    "getFdmtCFAllLatest": {
                        "desc": "合并现金流量表(最新披露)",
                        "params": {"field": "secID,ticker,publishDate,endDate,netCashFlowsOper,netCashFlowsInvest,netCashFlowsFin"},
                        "time_range": True
                    },
                    "getFdmtCFBankAllLatest": {
                        "desc": "银行业现金流量表(最新披露)",
                        "params": {"field": "secID,ticker,publishDate,endDate,netCashFlowsOper,netCashFlowsInvest,netCashFlowsFin"},
                        "time_range": True
                    },
                    "getFdmtCFInduAllLatest": {
                        "desc": "一般工商业现金流量表(最新披露)",
                        "params": {"field": "secID,ticker,publishDate,endDate,netCashFlowsOper,netCashFlowsInvest,netCashFlowsFin"},
                        "time_range": True
                    },
                    "getFdmtDer": {
                        "desc": "财务衍生数据",
                        "params": {"field": "secID,ticker,publishDate,endDate,pe,pb,roe,roa"},
                        "time_range": True
                    },
                    "getFdmtIndiPS": {
                        "desc": "财务指标—每股指标",
                        "params": {"field": "secID,ticker,publishDate,endDate,eps,bps,cfps"},
                        "time_range": True
                    },
                    "getFdmtIndiGrowth": {
                        "desc": "财务指标—成长能力",
                        "params": {"field": "secID,ticker,publishDate,endDate,revenueGrowthRate,netProfitGrowthRate"},
                        "time_range": True
                    }
                }
            },
            
            # 3. 特殊交易数据 (17个) - 龙虎榜、涨跌停、大宗交易等
            "special_trading": {
                "priority": 3,
                "apis": {
                    "getMktRankListStocks": {
                        "desc": "沪深交易公开信息_股票(龙虎榜)",
                        "params": {"field": "secID,ticker,tradeDate,buyAmount,sellAmount,netBuyAmount"},
                        "time_range": True
                    },
                    "getMktRankListSales": {
                        "desc": "沪深交易公开信息_营业部(龙虎榜)",
                        "params": {"field": "secID,ticker,tradeDate,salesDeptName,buyAmount,sellAmount"},
                        "time_range": True
                    },
                    "getMktLimit": {
                        "desc": "沪深涨跌停限制",
                        "params": {"field": "secID,ticker,tradeDate,limitUpPrice,limitDownPrice"},
                        "time_range": True
                    },
                    "getMktBlockd": {
                        "desc": "沪深大宗交易",
                        "params": {"field": "secID,ticker,tradeDate,dealAmount,dealVol,dealPrice"},
                        "time_range": True
                    },
                    "getFstTotal": {
                        "desc": "沪深融资融券每日汇总信息",
                        "params": {"field": "secID,ticker,tradeDate,rzye,rqyl,rzrqye"},
                        "time_range": True
                    },
                    "getFstDetail": {
                        "desc": "沪深融资融券每日交易明细信息",
                        "params": {"field": "secID,ticker,tradeDate,rzmairu,rzmaichu,rzche"},
                        "time_range": True
                    },
                    "getVfsttarget": {
                        "desc": "融资融券标的证券",
                        "params": {"field": "secID,ticker,beginDate,endDate"},
                        "time_range": False,
                        "static": True
                    },
                    "getEquMarginSec": {
                        "desc": "可充抵保证金证券",
                        "params": {"field": "secID,ticker,adjustRate"},
                        "time_range": False,
                        "static": True
                    },
                    "getEquIsActivity": {
                        "desc": "A股机构调研活动统计",
                        "params": {"field": "secID,ticker,activityDate,activityType,participantNum"},
                        "time_range": True
                    },
                    "getEquIsParticipantQa": {
                        "desc": "A股机构调研活动明细",
                        "params": {"field": "secID,ticker,activityDate,participantName,participantType"},
                        "time_range": True
                    },
                    "getMktIpoConTraddays": {
                        "desc": "新股上市连板天数",
                        "params": {"field": "secID,ticker,listDate,conTradDays"},
                        "time_range": True
                    },
                    "getMktRankDivYield": {
                        "desc": "沪深股息率排名",
                        "params": {"field": "secID,ticker,tradeDate,divYield"},
                        "time_range": True
                    },
                    "getMktRANKInstTr": {
                        "desc": "行业成分换手率排名",
                        "params": {"field": "industryID,industryName,tradeDate,turnoverRate"},
                        "time_range": True,
                        "no_stock_filter": True  # 行业数据
                    },
                    "getMktEquPerf": {
                        "desc": "每日股票最新表现",
                        "params": {"field": "secID,ticker,tradeDate,pctChange,turnoverRate"},
                        "time_range": True
                    },
                    "getMktEqudStats": {
                        "desc": "股票市场统计",
                        "params": {"field": "tradeDate,totalStock,totalMarketValue"},
                        "time_range": True,
                        "no_stock_filter": True  # 市场统计
                    },
                    "getMktConsBondPremium": {
                        "desc": "可转债折溢价信息",
                        "params": {"field": "secID,ticker,tradeDate,conversionPremium"},
                        "time_range": True
                    },
                    "getSecHalt": {
                        "desc": "沪深证券停复牌",
                        "params": {"field": "secID,ticker,suspensionDate,resumptionDate,suspensionReason"},
                        "time_range": True
                    }
                }
            },
            
            # 4. 股东治理数据 (24个) - 股东户数、持股变动、质押等
            "shareholder_governance": {
                "priority": 4,
                "apis": {
                    # 股东核心信息
                    "getEquShareholderNum": {
                        "desc": "上市公司股东户数",
                        "params": {"field": "secID,ticker,endDate,shareholderNum"},
                        "time_range": True
                    },
                    "getEquShTen": {
                        "desc": "公司十大股东",
                        "params": {"field": "secID,ticker,endDate,shareholderName,shareholderRank,shareHolding"},
                        "time_range": True
                    },
                    "getEquFloatShTen": {
                        "desc": "公司十大流通股东",
                        "params": {"field": "secID,ticker,endDate,shareholderName,shareholderRank,shareHolding"},
                        "time_range": True
                    },
                    "getEquActualController": {
                        "desc": "上市公司实际控制人",
                        "params": {"field": "secID,ticker,controllerName,controllerType,shareRatio"},
                        "time_range": False,
                        "static": True
                    },
                    "getEquShareholdersMeeting": {
                        "desc": "股东大会召开信息",
                        "params": {"field": "secID,ticker,meetingDate,meetingType"},
                        "time_range": True
                    },
                    "getEquOldShoffer": {
                        "desc": "老股东公开发售明细",
                        "params": {"field": "secID,ticker,offerDate,offerPrice,offerShares"},
                        "time_range": True
                    },
                    "getEquMsChanges": {
                        "desc": "高管及相关人员持股变动",
                        "params": {"field": "secID,ticker,changeDate,managerName,changeShares"},
                        "time_range": True
                    },
                    "getEquChangePlan": {
                        "desc": "股东增减持计划",
                        "params": {"field": "secID,ticker,announcementDate,shareholderName,planType,planShares"},
                        "time_range": True
                    },
                    
                    # 高管治理信息
                    "getEquManagers": {
                        "desc": "上市公司管理层",
                        "params": {"field": "secID,ticker,managerName,position,gender,education"},
                        "time_range": False,
                        "static": True
                    },
                    "getEquExecsHoldings": {
                        "desc": "公司高管持股薪酬明细",
                        "params": {"field": "secID,ticker,endDate,managerName,holdingShares,compensation"},
                        "time_range": True
                    },
                    "getEquRelatedTransaction": {
                        "desc": "上市公司关联交易",
                        "params": {"field": "secID,ticker,announcementDate,relatedParty,transactionAmount"},
                        "time_range": True
                    },
                    
                    # 股权质押与限售
                    "getEquStockPledge": {
                        "desc": "股票周质押信息",
                        "params": {"field": "secID,ticker,endDate,pledgedShares,pledgedRatio"},
                        "time_range": True
                    },
                    "getEquPledge": {
                        "desc": "A股公司股权质押",
                        "params": {"field": "secID,ticker,pledgeDate,pledgedShares,pledgor"},
                        "time_range": True
                    },
                    "getEquSharesFloat": {
                        "desc": "限售股解禁",
                        "params": {"field": "secID,ticker,liftDate,liftShares,liftRatio"},
                        "time_range": True
                    },
                    "getEquIpoShareFloat": {
                        "desc": "首发限售解禁明细",
                        "params": {"field": "secID,ticker,liftDate,liftShares,shareholderName"},
                        "time_range": True
                    },
                    "getEquReformShareFloat": {
                        "desc": "股改限售解禁明细", 
                        "params": {"field": "secID,ticker,liftDate,liftShares,shareholderName"},
                        "time_range": True
                    },
                    
                    # 资本运作相关
                    "getEquAllot": {
                        "desc": "股票配股信息",
                        "params": {"field": "secID,ticker,exRightDate,allotRatio,allotPrice"},
                        "time_range": True
                    },
                    "getEquSpo": {
                        "desc": "增发信息",
                        "params": {"field": "secID,ticker,issueDate,issuePrice,issueShares"},
                        "time_range": True
                    },
                    "getEquAllotmentSubscriptionResults": {
                        "desc": "配股认购结果表",
                        "params": {"field": "secID,ticker,subscriptionDate,subscriptionRatio"},
                        "time_range": True
                    },
                    "getEquSpoPubResult": {
                        "desc": "公开增发中签率及配售结果",
                        "params": {"field": "secID,ticker,issueDate,winningRate,placementRatio"},
                        "time_range": True
                    },
                    "getEquSharesExcit": {
                        "desc": "股权激励基本资料",
                        "params": {"field": "secID,ticker,grantDate,grantPrice,grantShares"},
                        "time_range": True
                    },
                    
                    # 特殊标记
                    "getEquPartyNature": {
                        "desc": "个股企业性质",
                        "params": {"field": "secID,ticker,partyNature"},
                        "time_range": False,
                        "static": True
                    },
                    "getEquSalaryRange": {
                        "desc": "年薪区间",
                        "params": {"field": "secID,ticker,endDate,salaryRange,managerNum"},
                        "time_range": True
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
            "total_apis": 58,
            "statistics": {
                "success_count": 0,
                "failed_count": 0,
                "total_records": 0
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
            stocks = uqer.DataAPI.EquGet(
                field='secID,ticker,secShortName,exchangeCD,listStatusCD,listDate,delistDate'
            )
            
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
        # 筛选该年份已上市的股票
        year_stocks = stocks[stocks['listYear'] <= year].copy()
        return year_stocks
    
    def download_single_api(self, api_name, api_config, category, year=None, stocks=None):
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
                logging.error(f"❌ API {api_name} 不存在")
                return False
            
            if api_config.get("static"):
                # 静态数据，不需要年度循环
                return self._download_static_api(api_func, api_config, desc, data_dir, api_key)
            elif api_config.get("time_range"):
                # 需要时间范围的数据
                return self._download_time_range_api(api_func, api_config, desc, data_dir, api_key, year, stocks)
            else:
                # 其他类型数据
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
    
    def _download_time_range_api(self, api_func, api_config, desc, data_dir, api_key, year, stocks):
        """下载时间范围数据"""
        success_years = 0
        total_records = 0
        
        # 下载2000-2025年数据
        for download_year in range(2000, 2026):
            if download_year > datetime.now().year:
                break
                
            year_file = data_dir / f"year_{download_year}.csv"
            if year_file.exists():
                existing_data = pd.read_csv(year_file)
                total_records += len(existing_data)
                success_years += 1
                logging.info(f"📂 {desc} {download_year}年 已存在: {len(existing_data)} 条")
                continue
            
            try:
                params = api_config["params"].copy()
                params["beginDate"] = f"{download_year}0101" 
                params["endDate"] = f"{download_year}1231"
                
                # 处理股票筛选
                if not api_config.get("no_stock_filter") and stocks is not None:
                    year_stocks = self.get_stocks_for_year(stocks, download_year)
                    if len(year_stocks) == 0:
                        logging.info(f"⏭️ {desc} {download_year}年: 无已上市股票")
                        continue
                    
                    # 分批下载
                    year_data = []
                    batch_size = 100
                    batches = [year_stocks[i:i+batch_size] for i in range(0, len(year_stocks), batch_size)]
                    
                    for batch_idx, batch_stocks in enumerate(batches):
                        batch_params = params.copy()
                        batch_params["ticker"] = ','.join(batch_stocks['ticker'].tolist())
                        
                        batch_data = api_func(**batch_params)
                        if batch_data is not None and not batch_data.empty:
                            year_data.append(batch_data)
                        
                        time.sleep(0.2)  # API限制
                    
                    if year_data:
                        combined_data = pd.concat(year_data, ignore_index=True)
                        combined_data.to_csv(year_file, index=False)
                        total_records += len(combined_data)
                        success_years += 1
                        logging.info(f"✅ {desc} {download_year}年: {len(combined_data)} 条记录")
                    else:
                        logging.warning(f"⚠️ {desc} {download_year}年: 无数据")
                else:
                    # 不需要股票筛选的数据
                    data = api_func(**params)
                    if data is not None and not data.empty:
                        data.to_csv(year_file, index=False)
                        total_records += len(data)
                        success_years += 1
                        logging.info(f"✅ {desc} {download_year}年: {len(data)} 条记录")
                    else:
                        logging.warning(f"⚠️ {desc} {download_year}年: 无数据")
                
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
    
    def execute_comprehensive_download(self):
        """执行综合下载"""
        logging.info("🚀 开始执行综合API数据下载...")
        logging.info(f"📊 总计 {self.progress_data['total_apis']} 个API接口")
        
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
        self._generate_final_report()
        
        logging.info("🎉 综合API数据下载完成!")
        return True
    
    def _generate_final_report(self):
        """生成最终下载报告"""
        report = {
            "download_summary": {
                "completion_time": datetime.now().isoformat(),
                "date_range": "2000年-2025年",
                "total_apis": self.progress_data["total_apis"],
                "completed_apis": len(self.progress_data["completed_apis"]),
                "failed_apis": len(self.progress_data["failed_apis"]),
                "success_rate": len(self.progress_data["completed_apis"]) / self.progress_data["total_apis"],
                "total_records": self.progress_data["statistics"]["total_records"]
            },
            "api_categories": {
                "basic_stock_info": 7,
                "financial_data": 12,
                "special_trading": 17, 
                "shareholder_governance": 24
            },
            "completed_apis": self.progress_data["completed_apis"],
            "failed_apis": self.progress_data["failed_apis"]
        }
        
        report_file = self.data_dir / "final_download_report.json"
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        logging.info(f"📄 最终报告保存至: {report_file}")


if __name__ == "__main__":
    downloader = ComprehensiveAPIDownloader()
    
    # 执行下载
    success = downloader.execute_comprehensive_download()
    
    if success:
        print("\n🎉 综合API数据下载任务完成!")
        print(f"📊 成功: {downloader.progress_data['statistics']['success_count']} 个接口")
        print(f"❌ 失败: {len(downloader.progress_data['failed_apis'])} 个接口") 
        print(f"📈 总记录数: {downloader.progress_data['statistics']['total_records']} 条")
    else:
        print("\n❌ 综合API数据下载任务失败")