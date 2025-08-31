#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
全面数据下载计划
=============

基于263个优矿接口，制定从2000年至今的系统性数据下载方案
"""

import uqer
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
import time
import logging

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/comprehensive_download.log'),
        logging.StreamHandler()
    ]
)

# 优矿Token
UQER_TOKEN = "68b9922817ae6273137bda7acba81e293582ba347281dfcc056dcb245b23faf3"

class ComprehensiveDataDownloader:
    """全面数据下载器"""
    
    def __init__(self):
        """初始化下载器"""
        self.client = uqer.Client(token=UQER_TOKEN)
        self.data_dir = Path("data/comprehensive")
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.start_date = "20000101"
        self.end_date = datetime.now().strftime('%Y%m%d')
        
        # 核心数据接口分类
        self.core_apis = self._define_core_apis()
        
    def _define_core_apis(self):
        """定义核心数据接口"""
        return {
            # 1. 基础行情数据 (最高优先级)
            "market_data": {
                "priority": 1,
                "apis": {
                    "getMktEqud": "沪深股票日行情",
                    "getMktEquw": "股票周行情", 
                    "getMktEqum": "股票月行情",
                    "getMktIdxd": "指数日行情",
                    "getMktEqudAdj": "沪深股票前复权行情",
                    "getMktEquFlow": "个股日资金流向"
                }
            },
            
            # 2. 股票基础信息 (高优先级)
            "stock_basics": {
                "priority": 2,
                "apis": {
                    "getEqu": "股票基本信息",
                    "getEquIPO": "股票首次上市信息",
                    "getEquIndustry": "股票行业分类",
                    "getSecID": "证券编码及基本上市信息",
                    "getEquDiv": "股票分红信息",
                    "getEquSplits": "股票拆股信息"
                }
            },
            
            # 3. 财务数据 (高优先级)
            "financial_data": {
                "priority": 3,
                "apis": {
                    # 资产负债表
                    "getFdmtBSAllLatest": "合并资产负债表(最新披露)",
                    "getFdmtBSBankAllLatest": "银行业资产负债表(最新披露)",
                    "getFdmtBSInduAllLatest": "一般工商业资产负债表(最新披露)",
                    
                    # 利润表
                    "getFdmtISAllLatest": "合并利润表(最新披露)",
                    "getFdmtISBankAllLatest": "银行业利润表(最新披露)", 
                    "getFdmtISInduAllLatest": "一般工商业利润表(最新披露)",
                    
                    # 现金流量表
                    "getFdmtCFAllLatest": "合并现金流量表(最新披露)",
                    "getFdmtCFBankAllLatest": "银行业现金流量表(最新披露)",
                    "getFdmtCFInduAllLatest": "一般工商业现金流量表(最新披露)",
                    
                    # 财务指标
                    "getFdmtDer": "财务衍生数据",
                    "getFdmtIndiPS": "财务指标—每股指标",
                    "getFdmtIndiGrowth": "财务指标—成长能力",
                    "getFdmtIndiRtn": "财务指标—盈利能力",
                    "getFdmtIndiLqd": "财务指标—偿债能力"
                }
            },
            
            # 4. 技术因子数据 (中优先级)
            "factor_data": {
                "priority": 4,
                "apis": {
                    "getStockFactorsDateRange": "股票因子数据时间序列",
                    "getStockFactorsOneDay": "单日股票因子数据"
                }
            },
            
            # 5. 市场微观结构 (中优先级)  
            "market_structure": {
                "priority": 5,
                "apis": {
                    "getMktBlockd": "沪深大宗交易",
                    "getFstTotal": "沪深融资融券每日汇总",
                    "getFstDetail": "沪深融资融券每日明细",
                    "getMktLimit": "沪深涨跌停限制",
                    "getSecHalt": "沪深证券停复牌"
                }
            },
            
            # 6. 基金数据 (中优先级)
            "fund_data": {
                "priority": 6,
                "apis": {
                    "getFund": "基金基本信息",
                    "getFundNav": "基金历史净值",
                    "getFundHoldings": "基金持仓明细",
                    "getMktFundd": "基金日行情"
                }
            },
            
            # 7. 指数与行业数据 (中优先级)
            "index_industry": {
                "priority": 7,
                "apis": {
                    "getIdx": "指数基本信息", 
                    "getIdxCons": "指数成分构成",
                    "getIdxCloseWeight": "指数成分股权重",
                    "getMktIndustryFlow": "行业日资金流向",
                    "getMktIndustryEval": "行业估值信息"
                }
            },
            
            # 8. 公司治理与事件 (低优先级)
            "corporate_events": {
                "priority": 8,
                "apis": {
                    "getEquShTen": "公司十大股东",
                    "getEquFloatShTen": "公司十大流通股东",
                    "getEquManagers": "上市公司管理层",
                    "getEquSharesFloat": "限售股解禁",
                    "getEquSpo": "增发信息"
                }
            }
        }
    
    def get_stock_list(self):
        """获取股票列表"""
        logging.info("📋 获取A股股票列表...")
        
        try:
            stocks = uqer.DataAPI.EquGet(
                field='secID,ticker,secShortName,exchangeCD,listStatusCD,listDate,delistDate'
            )
            
            if stocks is not None and not stocks.empty:
                # 过滤A股上市股票
                a_stocks = stocks[stocks['listStatusCD'] == 'L'].copy()
                
                logging.info(f"✅ 获取到 {len(a_stocks)} 只A股")
                
                # 保存股票列表
                stock_list_file = self.data_dir / "stock_list.csv"
                a_stocks.to_csv(stock_list_file, index=False)
                logging.info(f"📁 股票列表保存至: {stock_list_file}")
                
                return a_stocks
            else:
                logging.error("❌ 获取股票列表失败")
                return None
                
        except Exception as e:
            logging.error(f"❌ 获取股票列表异常: {e}")
            return None
    
    def download_market_data(self):
        """下载核心行情数据"""
        logging.info("📊 开始下载核心行情数据...")
        
        # 获取股票列表
        stocks = self.get_stock_list()
        if stocks is None:
            return False
        
        # 创建行情数据目录
        market_dir = self.data_dir / "market_data"
        market_dir.mkdir(parents=True, exist_ok=True)
        
        # 下载日行情数据
        return self._download_daily_market_data(stocks, market_dir)
    
    def _download_daily_market_data(self, stocks, market_dir):
        """下载日行情数据"""
        logging.info("📈 下载股票日行情数据...")
        
        try:
            # 分批下载，每次100只股票
            batch_size = 100
            stock_batches = [stocks[i:i+batch_size] for i in range(0, len(stocks), batch_size)]
            
            success_count = 0
            failed_count = 0
            
            for batch_idx, batch_stocks in enumerate(stock_batches):
                logging.info(f"📦 处理第 {batch_idx+1}/{len(stock_batches)} 批股票...")
                
                # 构建ticker列表
                tickers = ','.join(batch_stocks['ticker'].tolist())
                
                try:
                    # 调用优矿API
                    data = uqer.DataAPI.MktEqudGet(
                        secID='',
                        ticker=tickers,
                        beginDate=self.start_date,
                        endDate=self.end_date,
                        field='secID,ticker,tradeDate,preClosePrice,openPrice,highestPrice,lowestPrice,closePrice,turnoverVol,negMarketValue,dealAmount,turnoverRate,marketValue'
                    )
                    
                    if data is not None and not data.empty:
                        # 保存数据
                        batch_file = market_dir / f"daily_batch_{batch_idx+1}.csv"
                        data.to_csv(batch_file, index=False)
                        
                        logging.info(f"✅ 批次 {batch_idx+1} 成功: {len(data)} 条记录")
                        success_count += len(batch_stocks)
                        
                    else:
                        logging.warning(f"⚠️ 批次 {batch_idx+1} 数据为空")
                        failed_count += len(batch_stocks)
                    
                    # 防止API限制，添加延迟
                    time.sleep(0.2)
                    
                except Exception as e:
                    logging.error(f"❌ 批次 {batch_idx+1} 下载失败: {e}")
                    failed_count += len(batch_stocks)
                    continue
            
            logging.info(f"📊 日行情下载完成: 成功 {success_count} 只, 失败 {failed_count} 只")
            return success_count > 0
            
        except Exception as e:
            logging.error(f"❌ 日行情下载异常: {e}")
            return False
    
    def download_financial_data(self):
        """下载财务数据"""
        logging.info("💰 开始下载财务数据...")
        
        # 创建财务数据目录
        financial_dir = self.data_dir / "financial_data"
        financial_dir.mkdir(parents=True, exist_ok=True)
        
        financial_apis = self.core_apis["financial_data"]["apis"]
        
        for api_name, description in financial_apis.items():
            logging.info(f"📋 下载 {description}...")
            
            try:
                # 调用API
                api_func = getattr(uqer.DataAPI, api_name)
                data = api_func(
                    publishDateBegin=self.start_date,
                    publishDateEnd=self.end_date
                )
                
                if data is not None and not data.empty:
                    # 保存数据
                    file_path = financial_dir / f"{api_name}.csv"
                    data.to_csv(file_path, index=False)
                    
                    logging.info(f"✅ {description} 成功: {len(data)} 条记录")
                else:
                    logging.warning(f"⚠️ {description} 数据为空")
                
                # API限制延迟
                time.sleep(0.5)
                
            except Exception as e:
                logging.error(f"❌ {description} 下载失败: {e}")
                continue
    
    def create_download_plan(self):
        """创建详细下载计划"""
        plan = {
            "download_plan": {
                "total_apis": sum(len(category["apis"]) for category in self.core_apis.values()),
                "estimated_time_hours": 24,  # 预估24小时
                "data_size_gb": 50,  # 预估50GB
                "date_range": f"{self.start_date} 到 {self.end_date}",
                "categories": {}
            }
        }
        
        for category, info in self.core_apis.items():
            plan["download_plan"]["categories"][category] = {
                "priority": info["priority"],
                "api_count": len(info["apis"]),
                "estimated_time_hours": len(info["apis"]) * 0.5,
                "apis": info["apis"]
            }
        
        return plan
    
    def execute_comprehensive_download(self):
        """执行全面数据下载"""
        logging.info("🚀 开始执行全面数据下载计划...")
        logging.info(f"📅 时间范围: {self.start_date} - {self.end_date}")
        
        # 创建下载计划
        plan = self.create_download_plan()
        logging.info(f"📊 下载计划: {plan['download_plan']['total_apis']} 个API接口")
        
        results = {}
        
        # 按优先级排序执行
        sorted_categories = sorted(
            self.core_apis.items(),
            key=lambda x: x[1]["priority"]
        )
        
        for category_name, category_info in sorted_categories:
            logging.info(f"🎯 执行分类: {category_name} (优先级: {category_info['priority']})")
            
            if category_name == "market_data":
                results[category_name] = self.download_market_data()
            elif category_name == "financial_data":
                results[category_name] = self.download_financial_data()
            else:
                # 其他分类的下载逻辑
                results[category_name] = self._download_category_data(category_name, category_info)
        
        # 生成下载报告
        self._generate_download_report(results)
        
        return results
    
    def _download_category_data(self, category_name, category_info):
        """下载特定分类数据"""
        logging.info(f"📁 开始下载 {category_name} 分类数据...")
        
        # 创建分类目录
        category_dir = self.data_dir / category_name
        category_dir.mkdir(parents=True, exist_ok=True)
        
        success_count = 0
        total_count = len(category_info["apis"])
        
        for api_name, description in category_info["apis"].items():
            try:
                logging.info(f"📋 下载 {description}...")
                
                # 根据API类型调用不同的下载方法
                success = self._download_single_api(api_name, description, category_dir)
                if success:
                    success_count += 1
                
                # API限制延迟
                time.sleep(0.3)
                
            except Exception as e:
                logging.error(f"❌ {description} 下载异常: {e}")
                continue
        
        logging.info(f"📊 {category_name} 完成: {success_count}/{total_count}")
        return success_count > 0
    
    def _download_single_api(self, api_name, description, save_dir):
        """下载单个API数据"""
        try:
            # 获取API函数
            api_func = getattr(uqer.DataAPI, api_name, None)
            if not api_func:
                logging.warning(f"⚠️ API {api_name} 不存在")
                return False
            
            # 根据API类型设置参数
            if "beginDate" in api_func.__code__.co_varnames:
                data = api_func(
                    beginDate=self.start_date,
                    endDate=self.end_date
                )
            else:
                data = api_func()
            
            if data is not None and not data.empty:
                # 保存数据
                file_path = save_dir / f"{api_name}.csv"
                data.to_csv(file_path, index=False)
                
                logging.info(f"✅ {description}: {len(data)} 条记录")
                return True
            else:
                logging.warning(f"⚠️ {description}: 数据为空")
                return False
                
        except Exception as e:
            logging.error(f"❌ {description} 下载失败: {e}")
            return False
    
    def _generate_download_report(self, results):
        """生成下载报告"""
        logging.info("📄 生成下载报告...")
        
        report = {
            "download_summary": {
                "start_time": datetime.now().isoformat(),
                "date_range": f"{self.start_date} - {self.end_date}",
                "categories": results,
                "success_categories": sum(1 for success in results.values() if success),
                "total_categories": len(results)
            }
        }
        
        # 保存报告
        report_file = self.data_dir / "download_report.json"
        import json
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        logging.info(f"📊 下载报告保存至: {report_file}")

if __name__ == "__main__":
    downloader = ComprehensiveDataDownloader()
    
    # 创建日志目录
    Path("logs").mkdir(exist_ok=True)
    
    # 执行下载
    results = downloader.execute_comprehensive_download()
    
    print("\n🎉 全面数据下载计划执行完成!")
    print(f"📊 结果: {results}")