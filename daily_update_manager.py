#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
每日数据更新管理器 - 自动化数据更新机制
"""

import uqer
import pandas as pd
from pathlib import Path
import logging
from datetime import datetime, date, timedelta
import time
import json
import sys
from typing import Dict, List, Tuple

class DailyUpdateManager:
    """每日数据更新管理器"""
    
    def __init__(self, token):
        self.token = token
        self.base_dir = Path("data/final_comprehensive_download")
        self.update_log_dir = Path("logs/daily_updates")
        self.update_log_dir.mkdir(parents=True, exist_ok=True)
        self.setup_logging()
        
        # 需要每日更新的API配置
        self.daily_update_apis = {
            "high_priority": [  # 高优先级：每日必更新
                {
                    "category": "basic_info",
                    "api_name": "MktIdxdGet",
                    "dir_name": "mktidxdget",
                    "description": "指数日行情",
                    "params": {"tradeDate": "TODAY"},
                    "update_frequency": "daily"
                },
                {
                    "category": "special_trading", 
                    "api_name": "MktLimitGet",
                    "dir_name": "mktlimitget",
                    "description": "涨跌停数据",
                    "params": {"tradeDate": "TODAY"},
                    "update_frequency": "daily"
                },
                {
                    "category": "special_trading",
                    "api_name": "FstDetailGet", 
                    "dir_name": "fstdetailget",
                    "description": "融资融券明细",
                    "params": {"endDate": "TODAY"},
                    "update_frequency": "daily"
                }
            ],
            "medium_priority": [  # 中优先级：每周更新
                {
                    "category": "governance",
                    "api_name": "EquShtEnGet",
                    "dir_name": "equshtenget", 
                    "description": "十大股东信息",
                    "params": {"endDate": "TODAY"},
                    "update_frequency": "weekly"
                },
                {
                    "category": "governance",
                    "api_name": "EquFloatShtEnGet",
                    "dir_name": "equfloatshtenget",
                    "description": "十大流通股东",
                    "params": {"endDate": "TODAY"}, 
                    "update_frequency": "weekly"
                }
            ],
            "low_priority": [  # 低优先级：每月更新
                {
                    "category": "financial",
                    "api_name": "FdmtBSAllLatestGet", 
                    "dir_name": "fdmtbsalllatestget",
                    "description": "资产负债表最新",
                    "params": {"endDate": "MONTH_END"},
                    "update_frequency": "monthly"
                },
                {
                    "category": "financial",
                    "api_name": "FdmtISAllLatestGet",
                    "dir_name": "fdmtisalllatestget", 
                    "description": "利润表最新",
                    "params": {"endDate": "MONTH_END"},
                    "update_frequency": "monthly"
                }
            ]
        }
    
    def setup_logging(self):
        """设置日志"""
        today = datetime.now().strftime('%Y%m%d')
        log_file = self.update_log_dir / f"daily_update_{today}.log"
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file, encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
    
    def get_target_date(self, date_param: str) -> str:
        """获取目标日期"""
        today = datetime.now()
        
        if date_param == "TODAY":
            return today.strftime('%Y%m%d')
        elif date_param == "YESTERDAY":
            return (today - timedelta(days=1)).strftime('%Y%m%d')
        elif date_param == "MONTH_END":
            # 上月最后一天
            first_day_this_month = today.replace(day=1)
            last_day_last_month = first_day_this_month - timedelta(days=1)
            return last_day_last_month.strftime('%Y%m%d')
        else:
            return today.strftime('%Y%m%d')
    
    def should_update_today(self, frequency: str) -> bool:
        """判断今天是否需要更新"""
        today = datetime.now()
        weekday = today.weekday()  # 0=Monday, 6=Sunday
        
        if frequency == "daily":
            return weekday < 5  # 工作日
        elif frequency == "weekly": 
            return weekday == 0  # 周一
        elif frequency == "monthly":
            return today.day == 1  # 每月1号
        
        return False
    
    def update_single_api(self, api_info: Dict) -> Tuple[bool, str]:
        """更新单个API数据"""
        api_name = api_info["api_name"]
        category = api_info["category"]
        dir_name = api_info["dir_name"]
        description = api_info["description"]
        params = api_info.get("params", {})
        
        logging.info(f"📥 更新 {category}/{api_name} ({description})")
        
        # 检查API是否存在
        if not hasattr(uqer.DataAPI, api_name):
            return False, f"API不存在: {api_name}"
        
        # 创建输出目录
        api_dir = self.base_dir / category / dir_name
        api_dir.mkdir(parents=True, exist_ok=True)
        
        try:
            api_func = getattr(uqer.DataAPI, api_name)
            
            # 处理日期参数
            processed_params = {}
            for key, value in params.items():
                if isinstance(value, str) and value.upper() in ["TODAY", "YESTERDAY", "MONTH_END"]:
                    processed_params[key] = self.get_target_date(value)
                else:
                    processed_params[key] = value
            
            # 生成文件名
            date_str = processed_params.get("tradeDate", 
                      processed_params.get("endDate", 
                      datetime.now().strftime('%Y%m%d')))
            output_file = api_dir / f"update_{date_str}.csv"
            
            # 如果今日文件已存在，跳过
            if output_file.exists():
                return True, f"文件已存在，跳过: {output_file.name}"
            
            # 调用API
            logging.info(f"  📡 调用API参数: {processed_params}")
            result = api_func(**processed_params)
            
            # 获取数据
            if hasattr(result, 'getData') and callable(getattr(result, 'getData')):
                df = result.getData()
            else:
                df = result
            
            if df is None or (isinstance(df, pd.DataFrame) and df.empty):
                return False, "API返回空数据"
            
            # 保存数据
            df.to_csv(output_file, index=False, encoding='utf-8')
            
            logging.info(f"  ✅ 更新成功: {len(df):,} 条记录 -> {output_file.name}")
            return True, f"成功更新 {len(df)} 条记录"
            
        except Exception as e:
            error_msg = f"更新失败: {str(e)[:100]}"
            logging.error(f"  ❌ {error_msg}")
            return False, error_msg
    
    def run_daily_update(self, priority: str = "all") -> Dict:
        """执行每日更新"""
        logging.info(f"🚀 开始每日数据更新 - 优先级: {priority}")
        
        # 登录优矿
        try:
            client = uqer.Client(token=self.token)
            logging.info("✅ 优矿登录成功")
        except Exception as e:
            logging.error(f"❌ 优矿登录失败: {e}")
            return {"status": "failed", "error": "登录失败"}
        
        update_stats = {
            "total_attempted": 0,
            "successful_updates": 0,
            "failed_updates": 0,
            "skipped_updates": 0,
            "details": []
        }
        
        # 选择要更新的API
        apis_to_update = []
        if priority == "all":
            apis_to_update.extend(self.daily_update_apis["high_priority"])
            apis_to_update.extend(self.daily_update_apis["medium_priority"])
            apis_to_update.extend(self.daily_update_apis["low_priority"])
        elif priority in self.daily_update_apis:
            apis_to_update = self.daily_update_apis[priority]
        
        # 执行更新
        for api_info in apis_to_update:
            frequency = api_info.get("update_frequency", "daily")
            
            # 检查是否需要今天更新
            if not self.should_update_today(frequency):
                logging.info(f"⏭️ 跳过 {api_info['api_name']} - 今日无需更新({frequency})")
                update_stats["skipped_updates"] += 1
                continue
            
            update_stats["total_attempted"] += 1
            
            success, message = self.update_single_api(api_info)
            
            detail = {
                "api_name": api_info["api_name"],
                "category": api_info["category"], 
                "success": success,
                "message": message,
                "timestamp": datetime.now().isoformat()
            }
            
            update_stats["details"].append(detail)
            
            if success:
                update_stats["successful_updates"] += 1
            else:
                update_stats["failed_updates"] += 1
            
            # 请求间隔
            time.sleep(1)
        
        # 生成更新报告
        self.generate_update_report(update_stats)
        
        return update_stats
    
    def generate_update_report(self, stats: Dict):
        """生成更新报告"""
        logging.info("📊 生成每日更新报告...")
        
        report = []
        report.append("="*70)
        report.append("📊 **每日数据更新报告**")
        report.append("="*70)
        report.append(f"📅 更新时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append("")
        report.append("📈 **更新统计:**")
        report.append(f"  🎯 尝试更新: {stats['total_attempted']} 个API")
        report.append(f"  ✅ 成功更新: {stats['successful_updates']} 个")
        report.append(f"  ❌ 失败更新: {stats['failed_updates']} 个")
        report.append(f"  ⏭️ 跳过更新: {stats['skipped_updates']} 个")
        report.append("")
        
        if stats["details"]:
            report.append("📋 **更新详情:**")
            for detail in stats["details"]:
                status_icon = "✅" if detail["success"] else "❌"
                report.append(f"  {status_icon} {detail['category']}/{detail['api_name']}: {detail['message']}")
        
        report.append("")
        
        # 成功率
        if stats["total_attempted"] > 0:
            success_rate = (stats["successful_updates"] / stats["total_attempted"]) * 100
            report.append(f"📊 **成功率: {success_rate:.1f}%**")
            
            if success_rate >= 80:
                report.append("🎊 **数据更新状态: 优秀**")
            elif success_rate >= 60:
                report.append("🟡 **数据更新状态: 良好**")
            else:
                report.append("🔴 **数据更新状态: 需要关注**")
        
        report.append("="*70)
        
        # 输出报告
        for line in report:
            print(line)
        
        # 保存报告
        today = datetime.now().strftime('%Y%m%d')
        report_file = self.update_log_dir / f"update_report_{today}.txt"
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write('\n'.join(report))
        
        logging.info(f"📄 更新报告已保存: {report_file}")
    
    def create_cron_script(self):
        """创建定时任务脚本"""
        script_content = f"""#!/bin/bash
# 每日数据更新脚本
# 添加到crontab: 0 9 * * 1-5 /Users/jackstudio/QuantTrade/daily_update.sh

cd /Users/jackstudio/QuantTrade
source .venv/bin/activate

export UQER_TOKEN="{self.token}"
python daily_update_manager.py --priority=high_priority

# 检查返回码
if [ $? -eq 0 ]; then
    echo "$(date): 每日更新成功" >> logs/daily_updates/cron.log
else
    echo "$(date): 每日更新失败" >> logs/daily_updates/cron.log
fi
"""
        
        script_file = Path("daily_update.sh")
        with open(script_file, 'w') as f:
            f.write(script_content)
        
        # 设置执行权限
        import stat
        script_file.chmod(script_file.stat().st_mode | stat.S_IEXEC)
        
        logging.info(f"✅ 定时脚本已创建: {script_file}")
        logging.info("📝 添加到crontab命令: crontab -e")
        logging.info("📝 cron表达式: 0 9 * * 1-5 /Users/jackstudio/QuantTrade/daily_update.sh")

def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='每日数据更新管理器')
    parser.add_argument('--priority', choices=['high_priority', 'medium_priority', 'low_priority', 'all'], 
                        default='high_priority', help='更新优先级')
    parser.add_argument('--create-cron', action='store_true', help='创建定时脚本')
    
    args = parser.parse_args()
    
    token = "68b9922817ae6273137bda7acba81e293582ba347281dfcc056dcb245b23faf3"
    manager = DailyUpdateManager(token)
    
    if args.create_cron:
        manager.create_cron_script()
        return
    
    # 执行更新
    result = manager.run_daily_update(priority=args.priority)
    
    # 返回状态码
    if result.get("status") == "failed":
        sys.exit(1)
    elif result["failed_updates"] > result["successful_updates"]:
        sys.exit(1)
    else:
        sys.exit(0)

if __name__ == "__main__":
    main()