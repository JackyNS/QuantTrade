#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
API增强管理器 - 补充各分类关键API接口
"""

import uqer
import pandas as pd
from pathlib import Path
import logging
from datetime import datetime, date
import time

class APIEnhancementManager:
    """API增强管理器"""
    
    def __init__(self, token):
        self.token = token
        self.base_dir = Path("data/final_comprehensive_download")
        self.setup_logging()
        
        # 各分类需要补充的重要API
        self.enhancement_apis = {
            "basic_info": [
                {
                    "api_name": "TradCalGet",
                    "dir_name": "tradcalget",
                    "description": "交易日历信息 - A股市场交易日历",
                    "date_pattern": "yearly"
                },
                {
                    "api_name": "EquIndustriesClassGet", 
                    "dir_name": "equindustriesclassget",
                    "description": "行业分类变更 - 股票行业分类历史变更记录",
                    "date_pattern": "yearly"
                },
                {
                    "api_name": "SecTypeGet",
                    "dir_name": "sectypeget_enhanced",
                    "description": "证券类型分类 - 完整的证券类型分类体系",
                    "date_pattern": "snapshot"
                }
            ],
            "financial": [
                {
                    "api_name": "FdmtIndiRTAllLatestGet",
                    "dir_name": "fdmtindirallatestget", 
                    "description": "实时财务指标 - 最新财务指标数据",
                    "date_pattern": "quarterly"
                },
                {
                    "api_name": "FdmtEfindicatorGet",
                    "dir_name": "fdmtefindicatorget",
                    "description": "财务效率指标 - 企业财务效率分析指标",
                    "date_pattern": "quarterly"
                },
                {
                    "api_name": "FdmtIndiPSGet",
                    "dir_name": "fdmtindipsget_enhanced",
                    "description": "每股指标数据 - 每股收益、净资产等指标",
                    "date_pattern": "quarterly"
                }
            ],
            "special_trading": [
                {
                    "api_name": "MktOptdGet",
                    "dir_name": "mktoptdget",
                    "description": "期权日行情 - 股票期权每日行情数据", 
                    "date_pattern": "yearly"
                },
                {
                    "api_name": "MktFunddGet",
                    "dir_name": "mktfunddget",
                    "description": "基金日行情 - 基金每日净值和行情数据",
                    "date_pattern": "yearly"
                },
                {
                    "api_name": "MktBonddGet",
                    "dir_name": "mktbonddget", 
                    "description": "债券日行情 - 债券每日价格和成交数据",
                    "date_pattern": "yearly"
                }
            ],
            "governance": [
                {
                    "api_name": "EquInformationGet",
                    "dir_name": "equinformationget",
                    "description": "公司信息变更 - 上市公司基本信息变更记录",
                    "date_pattern": "yearly"
                },
                {
                    "api_name": "EquReturnGet",
                    "dir_name": "equreturnget", 
                    "description": "股票回报率 - 历史股票回报率数据",
                    "date_pattern": "yearly"
                },
                {
                    "api_name": "EquEarEstGet",
                    "dir_name": "equearestget",
                    "description": "盈利预测数据 - 分析师盈利预测信息",
                    "date_pattern": "quarterly"
                }
            ],
            "additional_apis": [
                {
                    "api_name": "MktStockFactorsOneDayGet",
                    "dir_name": "mktstockfactorsonedayget",
                    "description": "单日因子数据 - 股票单日因子暴露数据",
                    "date_pattern": "yearly"
                },
                {
                    "api_name": "MktIdxFactorOneDayGet", 
                    "dir_name": "mktidxfactoronedayget",
                    "description": "指数因子数据 - 指数单日因子数据",
                    "date_pattern": "yearly"
                },
                {
                    "api_name": "ParFactorCovGet",
                    "dir_name": "parfactorcovget",
                    "description": "因子协方差矩阵 - 多因子模型协方差数据",
                    "date_pattern": "monthly"
                }
            ]
        }
    
    def setup_logging(self):
        """设置日志"""
        log_file = Path("api_enhancement.log")
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
    
    def check_api_availability(self):
        """检查新增API的可用性"""
        logging.info("🔍 检查新增API可用性...")
        
        available_apis = {}
        
        for category, api_list in self.enhancement_apis.items():
            available_apis[category] = []
            
            for api_info in api_list:
                api_name = api_info["api_name"]
                
                if hasattr(uqer.DataAPI, api_name):
                    logging.info(f"  ✅ {category}/{api_name}: 可用")
                    available_apis[category].append(api_info)
                else:
                    logging.warning(f"  ❌ {category}/{api_name}: 不可用")
        
        return available_apis
    
    def generate_date_ranges(self, pattern, start_year=2020, end_year=2025):
        """生成日期范围"""
        ranges = []
        
        if pattern == "snapshot":
            ranges.append(("", "snapshot"))
        elif pattern == "yearly":
            for year in range(start_year, end_year + 1):
                ranges.append((f"{year}1231", f"year_{year}"))
        elif pattern == "quarterly":
            for year in range(start_year, end_year + 1):
                for quarter in [1, 2, 3, 4]:
                    if year == 2025 and quarter > 3:
                        break
                    date_str = f"{year}{quarter*3:02d}31" if quarter < 4 else f"{year}1231"
                    ranges.append((date_str, f"{year}_Q{quarter}"))
        elif pattern == "monthly":
            for year in range(start_year, end_year + 1):
                ranges.append((f"{year}1231", f"year_{year}"))
        
        return ranges
    
    def download_new_api(self, category, api_info):
        """下载新增API数据"""
        api_name = api_info["api_name"]
        dir_name = api_info["dir_name"]
        description = api_info["description"]
        pattern = api_info["date_pattern"]
        
        logging.info(f"📥 下载 {category}/{api_name} ({description})")
        
        # 创建目录
        api_dir = self.base_dir / category / dir_name
        api_dir.mkdir(parents=True, exist_ok=True)
        
        # 获取API函数
        try:
            api_func = getattr(uqer.DataAPI, api_name)
        except AttributeError:
            logging.error(f"❌ API不存在: {api_name}")
            return 0, 0
        
        # 生成日期范围
        date_ranges = self.generate_date_ranges(pattern)
        
        success_count = 0
        total_records = 0
        
        for i, (date_param, filename) in enumerate(date_ranges[:5], 1):  # 限制下载数量
            try:
                logging.info(f"  📥 [{i}/5] {api_name} - {filename}")
                
                output_file = api_dir / f"{filename}.csv"
                
                if output_file.exists():
                    logging.info(f"  ⏭️ 文件已存在，跳过: {filename}")
                    continue
                
                # 尝试不同的参数组合
                result = None
                param_combinations = []
                
                if date_param:
                    param_combinations = [
                        {"tradeDate": date_param},
                        {"endDate": date_param}, 
                        {"beginDate": date_param, "endDate": date_param},
                        {"date": date_param}
                    ]
                else:
                    param_combinations = [{}]
                
                # 尝试调用API
                for params in param_combinations:
                    try:
                        result = api_func(**params)
                        break
                    except Exception as e:
                        if "无效的请求参数" in str(e):
                            continue
                        else:
                            raise
                
                if result is None:
                    # 无参数调用
                    result = api_func()
                
                # 处理结果
                if hasattr(result, 'getData') and callable(getattr(result, 'getData')):
                    df = result.getData()
                else:
                    df = result
                
                if df is None or (isinstance(df, pd.DataFrame) and df.empty):
                    logging.warning(f"  ⚠️ 无数据: {filename}")
                    continue
                
                # 保存数据
                df.to_csv(output_file, index=False, encoding='utf-8')
                success_count += 1
                total_records += len(df)
                
                logging.info(f"  ✅ 成功: {len(df):,} 条记录")
                
                # 请求间隔
                time.sleep(0.5)
                
            except Exception as e:
                logging.error(f"  ❌ 下载失败 {filename}: {str(e)[:100]}")
                continue
        
        return success_count, total_records
    
    def enhance_all_categories(self):
        """增强所有分类"""
        logging.info("🚀 开始API增强流程...")
        
        # 登录
        client = self.login_uqer()
        if not client:
            return False
        
        # 检查API可用性
        available_apis = self.check_api_availability()
        
        enhancement_stats = {
            "categories_enhanced": 0,
            "new_apis_added": 0,
            "files_downloaded": 0,
            "total_records": 0
        }
        
        # 逐个分类增强
        for category, api_list in available_apis.items():
            if not api_list:
                logging.warning(f"⚠️ {category}: 无可用的新增API")
                continue
                
            logging.info(f"📂 增强分类: {category}")
            
            category_success = 0
            for api_info in api_list:
                try:
                    files, records = self.download_new_api(category, api_info)
                    if files > 0:
                        category_success += 1
                        enhancement_stats["files_downloaded"] += files
                        enhancement_stats["total_records"] += records
                except Exception as e:
                    logging.error(f"❌ API增强失败 {api_info['api_name']}: {e}")
            
            if category_success > 0:
                enhancement_stats["categories_enhanced"] += 1
                enhancement_stats["new_apis_added"] += category_success
                logging.info(f"✅ {category}: 成功添加 {category_success} 个API")
        
        # 生成增强报告
        self.generate_enhancement_report(enhancement_stats)
        
        return enhancement_stats["new_apis_added"] > 0
    
    def generate_enhancement_report(self, stats):
        """生成增强报告"""
        logging.info("📊 生成API增强报告...")
        
        report = []
        report.append("="*80)
        report.append("🎯 **API增强报告**")
        report.append("="*80)
        report.append(f"📅 增强时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append("")
        report.append("📊 **增强统计:**")
        report.append(f"  📁 增强分类: {stats['categories_enhanced']} 个")
        report.append(f"  🔌 新增API: {stats['new_apis_added']} 个") 
        report.append(f"  📄 下载文件: {stats['files_downloaded']} 个")
        report.append(f"  📈 新增记录: {stats['total_records']:,} 条")
        report.append("")
        
        # 分类增强详情
        report.append("🏷️ **分类增强计划:**")
        for category, api_list in self.enhancement_apis.items():
            report.append(f"  📁 {category}:")
            for api_info in api_list:
                status = "✅" if hasattr(uqer.DataAPI, api_info["api_name"]) else "❌"
                report.append(f"    {status} {api_info['api_name']}: {api_info['description']}")
        
        report.append("")
        report.append("🎊 **API增强完成！数据库功能更加完善**")
        report.append("="*80)
        
        # 输出报告
        for line in report:
            print(line)
        
        # 保存报告
        with open('api_enhancement_report.txt', 'w', encoding='utf-8') as f:
            f.write('\n'.join(report))
        
        logging.info("📄 增强报告已保存: api_enhancement_report.txt")

if __name__ == "__main__":
    token = "68b9922817ae6273137bda7acba81e293582ba347281dfcc056dcb245b23faf3"
    enhancer = APIEnhancementManager(token)
    result = enhancer.enhance_all_categories()