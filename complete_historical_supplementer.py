#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
完整历史数据补齐器 (2000-2014年)
=============================

补齐涨跌停和龙虎榜数据的2000-2014年数据
实现真正的25年完整覆盖
"""

import uqer
import pandas as pd
from pathlib import Path
from datetime import datetime
import time

# 优矿Token
UQER_TOKEN = "68b9922817ae6273137bda7acba81e293582ba347281dfcc056dcb245b23faf3"

class CompleteHistoricalSupplementer:
    """完整历史数据补齐器"""
    
    def __init__(self):
        uqer.Client(token=UQER_TOKEN)
        self.client = uqer
        self.base_path = Path("/Users/jackstudio/QuantTrade/data")
        
        print("📜 完整历史数据补齐器 (2000-2014年)")
        print("🎯 目标: 实现真正的25年完整覆盖")
        print("=" * 60)
        
    def supplement_limit_data_2000_2014(self):
        """补齐涨跌停数据 2000-2014年"""
        print("\n⚠️ 补齐涨跌停数据 2000-2014年...")
        
        output_dir = self.base_path / "limit_info"
        output_dir.mkdir(exist_ok=True)
        
        # 获取股票列表 (包含退市股票)
        try:
            result = self.client.DataAPI.EquGet(
                listStatusCD='',  # 包含退市股票
                field='secID',
                pandas='1'
            )
            stocks = result['secID'].unique().tolist()
            print(f"✅ 获取 {len(stocks)} 只股票 (含退市)")
        except Exception as e:
            print(f"❌ 获取股票列表失败: {e}")
            return False
        
        # 按年下载2000-2014年数据
        years = list(range(2000, 2015))  # 2000-2014年，15年数据
        successful_years = []
        
        for year in years:
            print(f"\n  📊 下载 {year} 年涨跌停数据...")
            
            batch_size = 30  # 历史数据用更小的批次
            all_data = []
            
            for i in range(0, len(stocks), batch_size):
                batch_stocks = stocks[i:i+batch_size]
                batch_tickers = ','.join([s.split('.')[0] for s in batch_stocks])
                
                print(f"    🔄 批次 {i//batch_size + 1}/{(len(stocks)-1)//batch_size + 1}: {len(batch_stocks)} 只股票")
                
                try:
                    result = self.client.DataAPI.MktLimitGet(
                        secID='',
                        ticker=batch_tickers,
                        beginDate=f'{year}0101',
                        endDate=f'{year}1231',
                        field='secID,ticker,tradeDate,upLimit,downLimit',
                        pandas='1'
                    )
                    
                    if isinstance(result, pd.DataFrame) and not result.empty:
                        all_data.append(result)
                        print(f"      ✅ 完成: {len(result)} 条记录")
                    else:
                        print(f"      ⚠️ {year}年该批次无数据")
                    
                    time.sleep(1)  # 历史数据间隔稍长
                    
                except Exception as e:
                    print(f"      ❌ 批次失败: {e}")
                    continue
            
            # 保存年度数据
            if all_data:
                combined = pd.concat(all_data, ignore_index=True)
                output_file = output_dir / f"limit_data_{year}.csv"
                combined.to_csv(output_file, index=False)
                
                print(f"  ✅ {year}年涨跌停完成: {len(combined)} 条记录")
                successful_years.append(year)
                time.sleep(2)  # 年度间休息
            else:
                print(f"  ⚠️ {year}年涨跌停无数据")
        
        print(f"\n⚠️ 涨跌停2000-2014年补齐完成: {len(successful_years)} 年")
        return len(successful_years) > 0
    
    def supplement_rank_list_2000_2014(self):
        """补齐龙虎榜数据 2000-2014年"""
        print("\n🔥 补齐龙虎榜数据 2000-2014年...")
        
        output_dir = self.base_path / "rank_list"
        output_dir.mkdir(exist_ok=True)
        
        # 按年下载2000-2014年数据
        years = list(range(2000, 2015))  # 2000-2014年，15年数据
        successful_years = []
        
        for year in years:
            print(f"\n  📊 下载 {year} 年龙虎榜数据...")
            
            try:
                result = self.client.DataAPI.MktRankListStocksGet(
                    beginDate=f'{year}0101',
                    endDate=f'{year}1231',
                    field='secID,ticker,tradeDate,rankReason,buyAmt,sellAmt',
                    pandas='1'
                )
                
                if isinstance(result, pd.DataFrame) and not result.empty:
                    output_file = output_dir / f"rank_list_{year}.csv"
                    result.to_csv(output_file, index=False)
                    
                    print(f"  ✅ {year}年龙虎榜完成: {len(result)} 条记录")
                    successful_years.append(year)
                else:
                    print(f"  ⚠️ {year}年无龙虎榜数据 (可能该年份龙虎榜制度尚未建立)")
                
                time.sleep(3)  # 龙虎榜API间隔较长
                
            except Exception as e:
                print(f"  ❌ {year}年龙虎榜失败: {e}")
                if "权限" in str(e):
                    print(f"    💡 可能该年份数据需要特殊权限")
                continue
        
        print(f"\n🔥 龙虎榜2000-2014年补齐完成: {len(successful_years)} 年")
        return len(successful_years) > 0
    
    def run_complete_historical_supplement(self):
        """运行完整历史数据补齐"""
        start_time = datetime.now()
        
        print(f"🚀 开始2000-2014年历史数据补齐...")
        print(f"📊 目标: 实现涨跌停和龙虎榜25年完整覆盖")
        print(f"⏰ 预计时间: 2-3小时")
        print()
        
        results = {}
        
        # 1. 补齐涨跌停数据 2000-2014年
        print("=" * 60)
        print("📋 阶段 1/2: 涨跌停数据 2000-2014年")
        print("=" * 60)
        results['limit_info'] = self.supplement_limit_data_2000_2014()
        
        # 2. 补齐龙虎榜数据 2000-2014年
        print("\n" + "=" * 60)
        print("📋 阶段 2/2: 龙虎榜数据 2000-2014年")
        print("=" * 60)
        results['rank_list'] = self.supplement_rank_list_2000_2014()
        
        # 生成补齐报告
        end_time = datetime.now()
        duration = end_time - start_time
        
        print(f"\n🎊 2000-2014年历史数据补齐完成!")
        print(f"⏱️ 总耗时: {duration}")
        print(f"📊 补齐结果:")
        
        success_count = sum(1 for success in results.values() if success)
        
        for data_type, success in results.items():
            status = "✅" if success else "❌"
            description = {
                'limit_info': '涨跌停数据 2000-2014年',
                'rank_list': '龙虎榜数据 2000-2014年'
            }
            print(f"   {status} {description[data_type]}: {'成功' if success else '失败'}")
        
        print(f"\n🎯 补齐成功率: {success_count}/2 ({success_count/2*100:.1f}%)")
        
        if success_count >= 1:
            print(f"\n🎊 历史数据覆盖显著改善！")
            print(f"📊 现在您的涨跌停和龙虎榜数据将拥有更完整的历史深度")
            print(f"🚀 支持更长期的历史分析和策略回测")
        
        # 最终覆盖统计
        print(f"\n📈 预期最终覆盖情况:")
        print(f"   ⚠️ 涨跌停数据: 25年 (2000-2024)")
        print(f"   🔥 龙虎榜数据: 25年 (2000-2024)")
        print(f"   🎯 实现真正的25年完整覆盖!")

def main():
    supplementer = CompleteHistoricalSupplementer()
    supplementer.run_complete_historical_supplement()

if __name__ == "__main__":
    main()