#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
增强版数据补齐器
=============

专门补齐年限不足的数据集:
1. 交易日历 2000-2009年
2. 资金流向 2015-2023年 
3. 涨跌停数据 2015-2019年
4. 龙虎榜数据 2015-2019年
"""

import uqer
import pandas as pd
from pathlib import Path
from datetime import datetime
import time

# 优矿Token
UQER_TOKEN = "68b9922817ae6273137bda7acba81e293582ba347281dfcc056dcb245b23faf3"

class EnhancedDataSupplementer:
    """增强版数据补齐器"""
    
    def __init__(self):
        uqer.Client(token=UQER_TOKEN)
        self.client = uqer
        self.base_path = Path("/Users/jackstudio/QuantTrade/data")
        
        print("📈 增强版数据补齐器")
        print("🎯 目标: 补齐年限不足的数据集")
        print("=" * 60)
        
    def supplement_trading_calendar(self):
        """补齐交易日历 2000-2009年"""
        print("\n📅 补齐交易日历 2000-2009年...")
        
        output_dir = self.base_path / "calendar"
        output_dir.mkdir(exist_ok=True)
        
        try:
            result = self.client.DataAPI.TradeCalGet(
                exchangeCD='XSHE',
                beginDate='20000101',
                endDate='20091231',
                field='calendarDate,exchangeCD,isOpen',
                pandas='1'
            )
            
            if isinstance(result, pd.DataFrame) and not result.empty:
                output_file = output_dir / "trading_calendar_2000_2009.csv"
                result.to_csv(output_file, index=False)
                
                print(f"✅ 交易日历2000-2009年完成: {len(result)} 条记录")
                return True
            else:
                print(f"⚠️ 交易日历2000-2009年无数据")
                return False
                
        except Exception as e:
            print(f"❌ 交易日历2000-2009年失败: {e}")
            return False
    
    def supplement_capital_flow(self):
        """补齐资金流向数据 2015-2023年"""
        print("\n💸 补齐资金流向数据 2015-2023年...")
        
        output_dir = self.base_path / "capital_flow"
        output_dir.mkdir(exist_ok=True)
        
        # 获取股票列表
        try:
            result = self.client.DataAPI.EquGet(
                listStatusCD='L',
                field='secID',
                pandas='1'
            )
            stocks = result['secID'].unique().tolist()
            print(f"✅ 获取 {len(stocks)} 只股票")
        except Exception as e:
            print(f"❌ 获取股票列表失败: {e}")
            return False
        
        # 按年下载资金流向数据
        years = [2023, 2022, 2021, 2020, 2019, 2018, 2017, 2016, 2015]
        successful_years = []
        
        for year in years:
            print(f"\n  📊 下载 {year} 年资金流向数据...")
            
            batch_size = 20  # 资金流向数据批次要小
            all_data = []
            
            for i in range(0, len(stocks), batch_size):
                batch_stocks = stocks[i:i+batch_size]
                batch_tickers = ','.join([s.split('.')[0] for s in batch_stocks])
                
                print(f"    🔄 批次 {i//batch_size + 1}/{(len(stocks)-1)//batch_size + 1}: {len(batch_stocks)} 只股票")
                
                try:
                    result = self.client.DataAPI.MktEquFlowGet(
                        secID='',
                        ticker=batch_tickers,
                        beginDate=f'{year}0101',
                        endDate=f'{year}1231',
                        field='secID,ticker,tradeDate,mainNetFlow,superNetFlow,largeNetFlow,mediumNetFlow,smallNetFlow',
                        pandas='1'
                    )
                    
                    if isinstance(result, pd.DataFrame) and not result.empty:
                        all_data.append(result)
                        print(f"      ✅ 完成: {len(result)} 条记录")
                    
                    time.sleep(1.5)  # 资金流向API限制较严
                    
                except Exception as e:
                    print(f"      ❌ 批次失败: {e}")
                    time.sleep(2)
                    continue
            
            # 保存年度数据
            if all_data:
                combined = pd.concat(all_data, ignore_index=True)
                output_file = output_dir / f"capital_flow_{year}.csv"
                combined.to_csv(output_file, index=False)
                
                print(f"  ✅ {year}年资金流向完成: {len(combined)} 条记录")
                successful_years.append(year)
                time.sleep(3)  # 年度间休息
            else:
                print(f"  ❌ {year}年无数据")
        
        print(f"\n💸 资金流向补齐完成: {len(successful_years)} 年")
        return len(successful_years) > 0
    
    def supplement_limit_data(self):
        """补齐涨跌停数据 2015-2019年"""
        print("\n⚠️ 补齐涨跌停数据 2015-2019年...")
        
        output_dir = self.base_path / "limit_info"
        output_dir.mkdir(exist_ok=True)
        
        # 获取股票列表
        try:
            result = self.client.DataAPI.EquGet(
                listStatusCD='',  # 包含退市股票
                field='secID',
                pandas='1'
            )
            stocks = result['secID'].unique().tolist()
            print(f"✅ 获取 {len(stocks)} 只股票")
        except Exception as e:
            print(f"❌ 获取股票列表失败: {e}")
            return False
        
        # 按年下载涨跌停数据
        years = [2019, 2018, 2017, 2016, 2015]
        successful_years = []
        
        for year in years:
            print(f"\n  📊 下载 {year} 年涨跌停数据...")
            
            batch_size = 50
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
                    
                    time.sleep(0.5)
                    
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
            else:
                print(f"  ❌ {year}年无数据")
        
        print(f"\n⚠️ 涨跌停补齐完成: {len(successful_years)} 年")
        return len(successful_years) > 0
    
    def supplement_rank_list(self):
        """补齐龙虎榜数据 2015-2019年"""
        print("\n🔥 补齐龙虎榜数据 2015-2019年...")
        
        output_dir = self.base_path / "rank_list"
        output_dir.mkdir(exist_ok=True)
        
        # 按年下载龙虎榜数据
        years = [2019, 2018, 2017, 2016, 2015]
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
                    print(f"  ⚠️ {year}年无龙虎榜数据")
                
                time.sleep(2)
                
            except Exception as e:
                print(f"  ❌ {year}年龙虎榜失败: {e}")
                continue
        
        print(f"\n🔥 龙虎榜补齐完成: {len(successful_years)} 年")
        return len(successful_years) > 0
    
    def run_supplement(self):
        """运行数据补齐"""
        start_time = datetime.now()
        
        print(f"🚀 开始数据年限补齐...")
        print(f"📊 目标: 补齐4个数据集的历史年限")
        print()
        
        results = {}
        
        # 1. 补齐交易日历
        results['calendar'] = self.supplement_trading_calendar()
        
        # 2. 补齐资金流向 (最耗时)
        results['capital_flow'] = self.supplement_capital_flow()
        
        # 3. 补齐涨跌停数据
        results['limit_info'] = self.supplement_limit_data()
        
        # 4. 补齐龙虎榜数据
        results['rank_list'] = self.supplement_rank_list()
        
        # 生成补齐报告
        end_time = datetime.now()
        duration = end_time - start_time
        
        print(f"\n🎊 数据年限补齐完成!")
        print(f"⏱️ 总耗时: {duration}")
        print(f"📊 补齐结果:")
        
        success_count = sum(1 for success in results.values() if success)
        
        for data_type, success in results.items():
            status = "✅" if success else "❌"
            print(f"   {status} {data_type}: {'成功' if success else '失败'}")
        
        print(f"\n🎯 总体成功率: {success_count}/4 ({success_count/4*100:.1f}%)")
        
        if success_count >= 3:
            print(f"🚀 数据年限显著改善！")
        elif success_count >= 2:
            print(f"📈 数据年限有所改善")
        else:
            print(f"⚠️ 需要进一步补齐")

def main():
    supplementer = EnhancedDataSupplementer()
    supplementer.run_supplement()

if __name__ == "__main__":
    main()