#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CSV数据整合器 - 整合所有CSV数据目录并检查API完整性
"""

import pandas as pd
from pathlib import Path
import json
import logging
from datetime import datetime
from collections import defaultdict
import shutil

class CSVDataConsolidator:
    """CSV数据整合器"""
    
    def __init__(self):
        # 各个数据源目录
        self.data_sources = {
            "final_comprehensive_download": {
                "path": Path("data/final_comprehensive_download"),
                "description": "完整下载数据",
                "size_gb": 204,
                "priority": 1  # 最高优先级
            },
            "optimized_data": {
                "path": Path("data/optimized_data"),  
                "description": "优化CSV数据",
                "size_gb": 5.5,
                "priority": 2
            },
            "priority_download": {
                "path": Path("data/priority_download"),
                "description": "优先下载数据", 
                "size_gb": 3.0,
                "priority": 3
            },
            "historical_download": {
                "path": Path("data/historical_download"),
                "description": "历史基础数据",
                "size_gb": 1.4,
                "priority": 4
            },
            "smart_download": {
                "path": Path("data/smart_download"),
                "description": "智能下载数据",
                "size_gb": 1.3,
                "priority": 5
            }
        }
        
        # 已知的60+个API列表
        self.expected_apis = {
            "basic_info": [
                "equget", "secidget", "equindustryget", "equipoget", 
                "equdivget", "equsplitsget", "mktidxdget"
            ],
            "financial": [
                "fdmtbsalllatestget", "fdmtbsbankalllatestget", "fdmtbsindualllatestget",
                "fdmtisbankalllatestget", "fdmtisindualllatestget", "fdmtisalllatestget",
                "fdmtcfalllatestget", "fdmtcfbankalllatestget", "fdmtcfindualllatestget",
                "fdmtindipsget", "fdmtindigrowthget", "fdmtderget"
            ],
            "special_trading": [
                "mktlimitget", "mktblockdget", "fstdetailget", "fsttotalget",
                "mktconsbondpremiumget", "mktrankinstr", "mktrankdivyieldget",
                "equisactivityget", "equisparticipantqaget", "sechaltget",
                "mktequdstatsget", "mktipocontraddaysget", "mktranklistsalesget",
                "mktrankliststocksget", "vfsttargetget", "equmarginsecget",
                "mktrankinsttrget", "fdmtee"
            ],
            "governance": [
                "equshareholdernumget", "equshtenget", "equfloatshtenget",
                "equpledgeget", "equshareholdersmeetingget", "equsharesexcitget",
                "equsharesfloatget", "equexecsholdingsget", "equmschangesget",
                "equrelatedtransactionget", "equiposharefloatget", "equreformsharefloatget",
                "equactualcontrollerget", "equmanagersget", "equpartynatureget",
                "equallotget", "equallotmentsubscriptionresultsget", "equchangeplanget",
                "equoldshofferget", "equspoget", "equspopubresultget",
                "equstockpledgeget"
            ]
        }
        
        self.consolidated_dir = Path("data/consolidated_csv")
        self.setup_logging()
    
    def setup_logging(self):
        """设置日志"""
        log_file = Path("csv_consolidation.log")
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file, encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
    
    def scan_all_data_sources(self):
        """扫描所有数据源"""
        logging.info("🔍 扫描所有CSV数据源...")
        
        all_apis = defaultdict(lambda: defaultdict(list))
        source_stats = {}
        
        for source_name, source_info in self.data_sources.items():
            source_path = source_info["path"]
            
            if not source_path.exists():
                logging.warning(f"⚠️ 数据源不存在: {source_name} - {source_path}")
                continue
            
            logging.info(f"📂 扫描 {source_name}...")
            
            stats = {
                "categories": 0,
                "apis": 0,
                "files": 0,
                "size_mb": 0
            }
            
            # 扫描类别
            for category_dir in source_path.iterdir():
                if not category_dir.is_dir():
                    continue
                
                category_name = category_dir.name
                stats["categories"] += 1
                
                # 扫描API
                for api_dir in category_dir.iterdir():
                    if not api_dir.is_dir():
                        continue
                    
                    api_name = api_dir.name
                    csv_files = list(api_dir.glob("*.csv"))
                    
                    if csv_files:
                        stats["apis"] += 1
                        stats["files"] += len(csv_files)
                        
                        # 记录API信息
                        all_apis[category_name][api_name].append({
                            "source": source_name,
                            "path": api_dir,
                            "files": len(csv_files),
                            "file_list": [f.name for f in csv_files],
                            "size_mb": sum(f.stat().st_size for f in csv_files) / 1024 / 1024,
                            "priority": source_info["priority"]
                        })
                        
                        stats["size_mb"] += sum(f.stat().st_size for f in csv_files) / 1024 / 1024
            
            source_stats[source_name] = stats
            logging.info(f"  ✅ {stats['categories']} 类别, {stats['apis']} API, {stats['files']} 文件, {stats['size_mb']/1024:.1f}GB")
        
        return all_apis, source_stats
    
    def check_api_completeness(self, all_apis):
        """检查API完整性"""
        logging.info("🔍 检查60+个API下载完整性...")
        
        completeness_report = {
            "expected_total": 0,
            "found_total": 0,
            "categories": {},
            "missing_apis": [],
            "extra_apis": []
        }
        
        for category, expected_apis in self.expected_apis.items():
            found_apis = list(all_apis.get(category, {}).keys())
            
            missing = set(expected_apis) - set(found_apis)
            extra = set(found_apis) - set(expected_apis)
            
            category_report = {
                "expected": len(expected_apis),
                "found": len(found_apis),
                "missing": list(missing),
                "extra": list(extra),
                "completeness": (len(expected_apis) - len(missing)) / len(expected_apis) * 100
            }
            
            completeness_report["categories"][category] = category_report
            completeness_report["expected_total"] += len(expected_apis)
            completeness_report["found_total"] += len(found_apis) - len(extra)  # 只计算期望的API
            
            if missing:
                completeness_report["missing_apis"].extend([f"{category}/{api}" for api in missing])
            
            if extra:
                completeness_report["extra_apis"].extend([f"{category}/{api}" for api in extra])
            
            # 输出类别报告
            status = "✅" if len(missing) == 0 else "⚠️"
            logging.info(f"{status} {category}: {len(found_apis)}/{len(expected_apis)} ({category_report['completeness']:.1f}%)")
            
            if missing:
                logging.warning(f"   缺失API: {missing}")
            
            if extra:
                logging.info(f"   额外API: {extra}")
        
        # 总体完整性
        overall_completeness = completeness_report["found_total"] / completeness_report["expected_total"] * 100
        completeness_report["overall_completeness"] = overall_completeness
        
        logging.info("=" * 60)
        logging.info(f"📊 **API完整性总结**:")
        logging.info(f"  期望API总数: {completeness_report['expected_total']}")
        logging.info(f"  找到API总数: {completeness_report['found_total']}")
        logging.info(f"  总体完整性: {overall_completeness:.1f}%")
        
        if completeness_report["missing_apis"]:
            logging.warning(f"  缺失API: {len(completeness_report['missing_apis'])} 个")
        
        if completeness_report["extra_apis"]:
            logging.info(f"  额外API: {len(completeness_report['extra_apis'])} 个")
        
        return completeness_report
    
    def analyze_data_overlap(self, all_apis):
        """分析数据重叠和优先级"""
        logging.info("🔍 分析数据重叠和来源优先级...")
        
        overlap_report = {
            "multiple_sources": {},
            "single_source": {},
            "recommended_consolidation": {}
        }
        
        for category, apis in all_apis.items():
            for api_name, sources in apis.items():
                if len(sources) > 1:
                    # 多个来源，需要选择
                    sources_sorted = sorted(sources, key=lambda x: x["priority"])
                    best_source = sources_sorted[0]  # 优先级最高的
                    
                    overlap_report["multiple_sources"][f"{category}/{api_name}"] = {
                        "sources": [s["source"] for s in sources],
                        "recommended": best_source["source"],
                        "files": best_source["files"],
                        "size_mb": best_source["size_mb"]
                    }
                    
                    logging.info(f"🔄 {category}/{api_name}: {len(sources)} 个来源")
                    logging.info(f"   推荐: {best_source['source']} ({best_source['files']} 文件, {best_source['size_mb']:.1f}MB)")
                    
                else:
                    # 单一来源
                    source = sources[0]
                    overlap_report["single_source"][f"{category}/{api_name}"] = {
                        "source": source["source"],
                        "files": source["files"],
                        "size_mb": source["size_mb"]
                    }
        
        logging.info(f"📊 数据源重叠分析:")
        logging.info(f"  单一来源API: {len(overlap_report['single_source'])}")
        logging.info(f"  多来源API: {len(overlap_report['multiple_sources'])}")
        
        return overlap_report
    
    def create_consolidation_plan(self, all_apis, overlap_report):
        """创建数据整合计划"""
        logging.info("📋 创建数据整合计划...")
        
        consolidation_plan = {
            "target_structure": {},
            "copy_operations": [],
            "total_files": 0,
            "total_size_gb": 0
        }
        
        for category, apis in all_apis.items():
            consolidation_plan["target_structure"][category] = {}
            
            for api_name, sources in apis.items():
                # 选择最佳数据源
                if len(sources) > 1:
                    # 多来源选择优先级最高的
                    best_source = sorted(sources, key=lambda x: x["priority"])[0]
                else:
                    best_source = sources[0]
                
                source_path = best_source["path"]
                target_path = self.consolidated_dir / category / api_name
                
                consolidation_plan["target_structure"][category][api_name] = {
                    "source_path": str(source_path),
                    "target_path": str(target_path),
                    "files": best_source["files"],
                    "size_mb": best_source["size_mb"],
                    "source_name": best_source["source"]
                }
                
                consolidation_plan["copy_operations"].append({
                    "source": str(source_path),
                    "target": str(target_path),
                    "files": best_source["files"],
                    "size_mb": best_source["size_mb"]
                })
                
                consolidation_plan["total_files"] += best_source["files"]
                consolidation_plan["total_size_gb"] += best_source["size_mb"] / 1024
        
        logging.info(f"📊 整合计划统计:")
        logging.info(f"  目标文件数: {consolidation_plan['total_files']}")
        logging.info(f"  目标大小: {consolidation_plan['total_size_gb']:.1f} GB")
        logging.info(f"  复制操作: {len(consolidation_plan['copy_operations'])}")
        
        return consolidation_plan
    
    def execute_consolidation(self, consolidation_plan):
        """执行数据整合"""
        logging.info("🚀 开始执行数据整合...")
        
        # 创建目标目录
        if self.consolidated_dir.exists():
            logging.warning("目标目录已存在，是否继续？(这将覆盖现有数据)")
            return False
        
        self.consolidated_dir.mkdir(exist_ok=True)
        
        copied_files = 0
        copied_size = 0
        
        for i, operation in enumerate(consolidation_plan["copy_operations"], 1):
            source_path = Path(operation["source"])
            target_path = Path(operation["target"])
            
            logging.info(f"📁 [{i}/{len(consolidation_plan['copy_operations'])}] 复制 {source_path.name}")
            logging.info(f"   从: {source_path}")
            logging.info(f"   到: {target_path}")
            
            try:
                # 创建目标目录
                target_path.mkdir(parents=True, exist_ok=True)
                
                # 复制所有CSV文件
                for csv_file in source_path.glob("*.csv"):
                    target_file = target_path / csv_file.name
                    shutil.copy2(csv_file, target_file)
                    copied_files += 1
                    copied_size += csv_file.stat().st_size
                
                logging.info(f"   ✅ 完成 ({operation['files']} 文件)")
                
            except Exception as e:
                logging.error(f"   ❌ 失败: {e}")
                return False
        
        logging.info("🎊 数据整合完成!")
        logging.info(f"📊 整合结果:")
        logging.info(f"  复制文件: {copied_files}")
        logging.info(f"  复制大小: {copied_size/1024/1024/1024:.1f} GB")
        logging.info(f"  目标目录: {self.consolidated_dir}")
        
        return True
    
    def generate_comprehensive_report(self):
        """生成综合报告"""
        logging.info("📋 生成CSV数据综合分析报告...")
        
        # 1. 扫描所有数据源
        all_apis, source_stats = self.scan_all_data_sources()
        
        # 2. 检查API完整性
        completeness_report = self.check_api_completeness(all_apis)
        
        # 3. 分析数据重叠
        overlap_report = self.analyze_data_overlap(all_apis)
        
        # 4. 创建整合计划
        consolidation_plan = self.create_consolidation_plan(all_apis, overlap_report)
        
        # 5. 生成综合报告
        comprehensive_report = {
            "analysis_time": datetime.now().isoformat(),
            "data_sources": {
                "scanned": list(self.data_sources.keys()),
                "stats": source_stats
            },
            "api_completeness": completeness_report,
            "overlap_analysis": overlap_report,
            "consolidation_plan": consolidation_plan,
            "recommendations": []
        }
        
        # 生成建议
        if completeness_report["overall_completeness"] >= 95:
            comprehensive_report["recommendations"].append("✅ API下载完整性良好，数据收集基本完成")
        else:
            comprehensive_report["recommendations"].append(f"⚠️ 有 {len(completeness_report['missing_apis'])} 个API缺失，建议补充下载")
        
        if len(overlap_report["multiple_sources"]) > 0:
            comprehensive_report["recommendations"].append(f"🔄 发现 {len(overlap_report['multiple_sources'])} 个API有多个数据源，建议整合")
        
        comprehensive_report["recommendations"].append(f"💾 建议创建统一数据目录，整合后大小约 {consolidation_plan['total_size_gb']:.1f} GB")
        
        # 保存报告
        report_file = Path("csv_data_comprehensive_report.json")
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(comprehensive_report, f, indent=2, ensure_ascii=False)
        
        logging.info(f"📄 综合报告已保存: {report_file}")
        
        # 输出最终建议
        logging.info("🎯 **最终建议**:")
        for recommendation in comprehensive_report["recommendations"]:
            logging.info(recommendation)
        
        return comprehensive_report

if __name__ == "__main__":
    consolidator = CSVDataConsolidator()
    report = consolidator.generate_comprehensive_report()