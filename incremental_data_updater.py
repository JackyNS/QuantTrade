#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
增量数据更新器
补充2024-2025年数据到现有200+GB数据库
"""

import sys
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
import warnings
import json
import time
import os
from io import StringIO
warnings.filterwarnings('ignore')

try:
    import uqer
    print("✅ UQER API 可用")
    UQER_AVAILABLE = True
except ImportError:
    print("❌ UQER API 不可用")
    UQER_AVAILABLE = False
    sys.exit(1)

class IncrementalDataUpdater:
    """增量数据更新器"""
    
    def __init__(self):
        """初始化更新器"""
        self.setup_uqer()
        self.setup_paths()
        self.get_existing_stocks()
        self.stats = {
            'start_time': datetime.now(),
            'stocks_updated': 0,
            'records_added': 0,
            'api_calls': 0
        }
        
    def setup_uqer(self):
        """设置UQER连接"""
        try:
            uqer_token = "68b9922817ae6273137bda7acba81e293582ba347281dfcc056dcb245b23faf3"
            uqer.Client(token=uqer_token)
            print("✅ UQER极速版连接成功")
            self.uqer_connected = True
        except Exception as e:
            print(f"❌ UQER连接失败: {e}")
            self.uqer_connected = False
            sys.exit(1)
    
    def setup_paths(self):
        """设置路径"""
        self.base_path = Path("/Users/jackstudio/QuantTrade/data")
        self.existing_daily = self.base_path / "csv_complete/daily"
        self.update_path = self.base_path / "incremental_update"
        self.update_path.mkdir(exist_ok=True)
        
        print(f"📁 现有数据: {self.existing_daily}")
        print(f"📁 更新路径: {self.update_path}")
        
    def get_existing_stocks(self):
        """获取现有股票列表"""
        print("📋 分析现有股票...")
        
        # 从csv_complete获取股票列表
        if self.existing_daily.exists():
            stock_files = list(self.existing_daily.rglob("*.csv"))
            self.existing_stocks = []
            
            # 从文件名提取股票代码
            for file_path in stock_files[:100]:  # 取样本
                try:
                    df = pd.read_csv(file_path)
                    if 'secID' in df.columns:
                        stocks = df['secID'].unique()
                        self.existing_stocks.extend(stocks)
                except:
                    continue
            
            self.existing_stocks = list(set(self.existing_stocks))
            print(f"   📈 现有股票: {len(self.existing_stocks)} 只")
        else:
            print("   ❌ 未找到现有股票数据")
            self.existing_stocks = []
    
    def update_stock_data(self, stock_id, start_date="20240101", end_date="20250831"):
        """更新单只股票的2024-2025数据"""
        
        try:
            result = uqer.DataAPI.MktEqudGet(
                secID=stock_id,
                beginDate=start_date,
                endDate=end_date,
                pandas=1
            )
            
            self.stats['api_calls'] += 1
            
            if result is None:
                return False
            
            # 处理API返回数据
            if isinstance(result, str):
                df = pd.read_csv(StringIO(result))
            elif isinstance(result, pd.DataFrame):
                df = result.copy()
            else:
                return False
            
            if len(df) == 0:
                return False
            
            # 保存更新数据
            file_name = f"{stock_id.replace('.', '_')}_2024_2025.csv"
            file_path = self.update_path / file_name
            
            df.to_csv(file_path, index=False, encoding='utf-8')
            self.stats['records_added'] += len(df)
            
            # 检查是否包含2024年8月数据
            df['tradeDate'] = pd.to_datetime(df['tradeDate'])
            aug_2024 = df[(df['tradeDate'] >= '2024-08-01') & (df['tradeDate'] <= '2024-08-31')]
            
            print(f"   ✅ {stock_id}: {len(df)} 条记录, 2024年8月: {len(aug_2024)} 天")
            
            return len(aug_2024) > 0
            
        except Exception as e:
            print(f"   ❌ {stock_id}: 更新失败")
            return False
    
    def batch_update(self, max_stocks=100):
        """批量更新数据"""
        print(f"🔄 开始增量数据更新")
        print(f"📅 更新范围: 2024年1月1日 - 2025年8月31日") 
        print(f"📊 目标股票: {min(len(self.existing_stocks), max_stocks)} 只")
        print("=" * 80)
        
        stocks_with_aug_2024 = []
        
        # 更新前N只股票
        target_stocks = self.existing_stocks[:max_stocks] if max_stocks else self.existing_stocks
        
        for i, stock_id in enumerate(target_stocks, 1):
            print(f"📈 [{i}/{len(target_stocks)}] 更新: {stock_id}")
            
            has_aug_data = self.update_stock_data(stock_id)
            
            if has_aug_data:
                stocks_with_aug_2024.append(stock_id)
            
            self.stats['stocks_updated'] += 1
            
            # 进度报告
            if i % 25 == 0:
                progress = (i / len(target_stocks)) * 100
                print(f"   📊 进度: {i}/{len(target_stocks)} ({progress:.1f}%), 8月数据: {len(stocks_with_aug_2024)} 只")
            
            time.sleep(0.2)  # API限速
        
        self.create_update_summary(stocks_with_aug_2024)
    
    def create_update_summary(self, stocks_with_aug_2024):
        """创建更新总结"""
        end_time = datetime.now()
        duration = end_time - self.stats['start_time']
        
        summary = {
            'incremental_update_info': {
                'start_time': self.stats['start_time'].isoformat(),
                'end_time': end_time.isoformat(),
                'duration_minutes': round(duration.total_seconds() / 60, 2),
                'update_range': '2024年1月1日 - 2025年8月31日'
            },
            'statistics': {
                'stocks_updated': self.stats['stocks_updated'],
                'records_added': self.stats['records_added'],
                'api_calls': self.stats['api_calls'],
                'stocks_with_aug_2024': len(stocks_with_aug_2024)
            },
            'august_2024_stocks': stocks_with_aug_2024[:50],  # 前50只
            'next_steps': [
                '基于更新数据实现10周/100周MA金叉策略',
                '筛选2024年8月金叉股票',
                '生成投资建议报告'
            ]
        }
        
        # 保存总结
        summary_file = self.update_path / 'incremental_update_summary.json'
        with open(summary_file, 'w', encoding='utf-8') as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
        
        print(f"\\n🎊 增量数据更新完成!")
        print("=" * 80)
        print(f"📊 更新统计:")
        print(f"   📈 更新股票: {summary['statistics']['stocks_updated']}")
        print(f"   📋 新增记录: {summary['statistics']['records_added']:,}")
        print(f"   📞 API调用: {summary['statistics']['api_calls']}")
        print(f"   🎯 2024年8月数据: {summary['statistics']['stocks_with_aug_2024']} 只股票")
        print(f"   ⏱️ 用时: {summary['incremental_update_info']['duration_minutes']} 分钟")
        print(f"   📁 更新文件: {self.update_path}")
        print(f"   📄 详细报告: {summary_file}")
        
        # 显示部分2024年8月股票
        if stocks_with_aug_2024:
            print(f"\\n📈 2024年8月数据样本:")
            for stock in stocks_with_aug_2024[:10]:
                print(f"      {stock}")

def main():
    """主函数"""
    print("🔄 增量数据更新器")
    print("=" * 80)
    print("🎯 目标: 补充2024-2025年数据到现有200+GB数据库")
    print("📡 数据源: UQER极速版")
    print("🏗️ 策略: 仅获取缺失的时间段数据")
    
    updater = IncrementalDataUpdater()
    
    if not updater.existing_stocks:
        print("❌ 未找到现有股票数据")
        return
    
    # 开始增量更新
    updater.batch_update(max_stocks=100)

if __name__ == "__main__":
    main()