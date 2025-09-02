#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
智能个股数据重组器
=================

目标：
1. 从批次文件中提取所有股票数据
2. 按个股重组，考虑上市时间和退市时间
3. 生成完整的个股文件，替换不完整的现有文件
4. 建立合理的数据存储结构

策略：
- 扫描所有批次文件，收集每只股票的完整数据
- 获取股票基本信息（上市时间、退市时间）
- 按股票合并所有时间段的数据
- 生成统一格式的个股文件

"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import logging
import json
import time
import warnings
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

warnings.filterwarnings('ignore')

class IntelligentStockDataReorganizer:
    """智能个股数据重组器"""
    
    def __init__(self):
        """初始化"""
        self.base_path = Path("/Users/jackstudio/QuantTrade/data")
        self.batch_path = self.base_path / "priority_download/market_data/daily"
        self.output_path = self.base_path / "reorganized_stocks"
        self.output_path.mkdir(exist_ok=True)
        
        # 设置日志
        self.setup_logging()
        
        # 股票信息缓存
        self.stock_info_cache = {}
        self.stock_data_cache = defaultdict(list)
        
        # 进度追踪
        self.progress_lock = threading.Lock()
        self.processed_files = 0
        self.total_files = 0
        
    def setup_logging(self):
        """设置日志"""
        log_file = self.output_path / "reorganization.log"
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def scan_all_batch_files(self):
        """扫描所有批次文件，收集股票列表"""
        print("🔍 扫描所有批次文件，收集股票列表...")
        print("=" * 80)
        
        batch_files = list(self.batch_path.glob("*.csv"))
        self.total_files = len(batch_files)
        
        print(f"📊 发现批次文件: {len(batch_files)} 个")
        
        all_stocks = set()
        year_stats = defaultdict(set)
        
        for i, batch_file in enumerate(batch_files, 1):
            try:
                df = pd.read_csv(batch_file, nrows=0)  # 只读取列名
                if 'secID' not in df.columns:
                    continue
                
                # 读取完整数据
                df = pd.read_csv(batch_file)
                if 'secID' in df.columns:
                    batch_stocks = df['secID'].unique()
                    all_stocks.update(batch_stocks)
                    
                    # 按年份统计
                    year = batch_file.stem.split('_')[0]
                    year_stats[year].update(batch_stocks)
                
                if i % 50 == 0 or i <= 10:
                    print(f"   处理进度: {i}/{len(batch_files)} | 累计股票: {len(all_stocks)}")
                    
            except Exception as e:
                self.logger.warning(f"跳过文件 {batch_file.name}: {str(e)}")
                continue
        
        print(f"✅ 扫描完成")
        print(f"📈 发现股票总数: {len(all_stocks)}")
        print(f"📅 年份分布: {len(year_stats)} 年")
        
        # 显示年份统计
        print("\n📊 各年份股票数量:")
        for year in sorted(year_stats.keys()):
            print(f"   {year}: {len(year_stats[year])} 只")
        
        return sorted(list(all_stocks)), year_stats
    
    def get_stock_basic_info(self, stock_list):
        """获取股票基本信息（上市时间、退市时间等）"""
        print(f"\n📋 获取 {len(stock_list)} 只股票的基本信息...")
        print("-" * 60)
        
        # 尝试从现有的基本信息文件中读取
        basic_info_paths = [
            "final_comprehensive_download/basic_info",
            "optimized_data/basic_info",
            "raw/basic_info",
            "csv_complete/static"
        ]
        
        stock_info = {}
        
        for info_path in basic_info_paths:
            full_path = self.base_path / info_path
            if full_path.exists():
                print(f"🔍 检查路径: {info_path}")
                info_files = list(full_path.rglob("*.csv"))
                
                for info_file in info_files[:5]:  # 检查前5个文件
                    try:
                        df = pd.read_csv(info_file)
                        
                        # 查找相关列
                        secID_col = None
                        listDate_col = None
                        delistDate_col = None
                        
                        for col in df.columns:
                            col_lower = col.lower()
                            if 'secid' in col_lower:
                                secID_col = col
                            elif any(keyword in col_lower for keyword in ['listdate', 'list_date', '上市']):
                                listDate_col = col
                            elif any(keyword in col_lower for keyword in ['delistdate', 'delist_date', '退市']):
                                delistDate_col = col
                        
                        if secID_col and len(stock_info) < 1000:  # 限制获取数量
                            for _, row in df.iterrows():
                                stock_id = row[secID_col]
                                if stock_id in stock_list and stock_id not in stock_info:
                                    info = {'secID': stock_id}
                                    
                                    # 获取上市日期
                                    if listDate_col and pd.notna(row[listDate_col]):
                                        try:
                                            list_date = pd.to_datetime(row[listDate_col])
                                            if list_date.year >= 1990:
                                                info['listDate'] = list_date.strftime('%Y-%m-%d')
                                        except:
                                            pass
                                    
                                    # 获取退市日期
                                    if delistDate_col and pd.notna(row[delistDate_col]):
                                        try:
                                            delist_date = pd.to_datetime(row[delistDate_col])
                                            if delist_date.year >= 1990:
                                                info['delistDate'] = delist_date.strftime('%Y-%m-%d')
                                        except:
                                            pass
                                    
                                    stock_info[stock_id] = info
                        
                        if len(stock_info) >= 1000:
                            break
                            
                    except Exception as e:
                        continue
                
                if stock_info:
                    print(f"   ✅ 从 {info_file.name} 获取了 {len(stock_info)} 只股票信息")
                    break
        
        print(f"📊 获取股票基本信息: {len(stock_info)} 只")
        
        # 对于没有基本信息的股票，使用数据推断
        missing_stocks = set(stock_list) - set(stock_info.keys())
        print(f"⚠️ 缺失基本信息的股票: {len(missing_stocks)} 只（将从数据推断）")
        
        self.stock_info_cache = stock_info
        return stock_info
    
    def collect_stock_data_from_batches(self, stock_id, max_workers=4):
        """从所有批次文件中收集单只股票的数据"""
        batch_files = list(self.batch_path.glob("*.csv"))
        stock_data_pieces = []
        
        def process_batch_file(batch_file):
            """处理单个批次文件"""
            try:
                df = pd.read_csv(batch_file)
                if 'secID' in df.columns:
                    stock_data = df[df['secID'] == stock_id]
                    if len(stock_data) > 0:
                        return stock_data
            except Exception as e:
                pass
            return None
        
        # 多线程处理批次文件
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_file = {executor.submit(process_batch_file, batch_file): batch_file 
                             for batch_file in batch_files}
            
            for future in as_completed(future_to_file):
                result = future.result()
                if result is not None:
                    stock_data_pieces.append(result)
        
        if not stock_data_pieces:
            return None
        
        # 合并所有数据片段
        combined_data = pd.concat(stock_data_pieces, ignore_index=True)
        
        # 去重并排序
        if 'tradeDate' in combined_data.columns:
            combined_data['tradeDate'] = pd.to_datetime(combined_data['tradeDate'])
            combined_data = combined_data.drop_duplicates(subset=['tradeDate']).sort_values('tradeDate')
            combined_data['tradeDate'] = combined_data['tradeDate'].dt.strftime('%Y-%m-%d')
        
        return combined_data
    
    def determine_stock_time_range(self, stock_id, stock_data):
        """确定股票的合理时间范围"""
        if stock_data is None or len(stock_data) == 0:
            return None, None
        
        # 从数据中获取实际的时间范围
        dates = pd.to_datetime(stock_data['tradeDate'])
        actual_start = dates.min().strftime('%Y-%m-%d')
        actual_end = dates.max().strftime('%Y-%m-%d')
        
        # 如果有基本信息，使用基本信息校准
        expected_start = actual_start
        expected_end = actual_end
        
        if stock_id in self.stock_info_cache:
            info = self.stock_info_cache[stock_id]
            if 'listDate' in info:
                list_date = info['listDate']
                # 预期开始时间不应该早于上市时间
                if list_date > actual_start:
                    expected_start = list_date
            
            if 'delistDate' in info:
                delist_date = info['delistDate']
                # 预期结束时间不应该晚于退市时间
                if delist_date < actual_end:
                    expected_end = delist_date
        
        return expected_start, expected_end
    
    def reorganize_single_stock(self, stock_id):
        """重组单只股票的数据"""
        try:
            # 收集股票数据
            stock_data = self.collect_stock_data_from_batches(stock_id)
            
            if stock_data is None or len(stock_data) == 0:
                return {'status': 'no_data', 'stock_id': stock_id}
            
            # 确定时间范围
            expected_start, expected_end = self.determine_stock_time_range(stock_id, stock_data)
            
            # 生成输出文件名
            safe_stock_id = stock_id.replace('.', '_')
            output_file = self.output_path / f"{safe_stock_id}.csv"
            
            # 保存数据
            stock_data.to_csv(output_file, index=False)
            
            result = {
                'status': 'success',
                'stock_id': stock_id,
                'output_file': str(output_file),
                'records': len(stock_data),
                'date_range': f"{expected_start} - {expected_end}",
                'years': round((pd.to_datetime(expected_end) - pd.to_datetime(expected_start)).days / 365.25, 1)
            }
            
            # 更新进度
            with self.progress_lock:
                self.processed_files += 1
                if self.processed_files % 50 == 0 or self.processed_files <= 10:
                    print(f"✅ 进度 [{self.processed_files}/{len(self.target_stocks)}] {stock_id}: {result['records']} 条记录")
            
            return result
            
        except Exception as e:
            self.logger.error(f"处理股票 {stock_id} 失败: {str(e)}")
            return {'status': 'error', 'stock_id': stock_id, 'error': str(e)}
    
    def reorganize_all_stocks(self, stock_list, max_workers=8, batch_size=100):
        """重组所有股票数据"""
        print(f"\n🔄 开始重组 {len(stock_list)} 只股票的数据...")
        print("=" * 80)
        
        self.target_stocks = stock_list
        results = []
        
        # 分批处理
        for i in range(0, len(stock_list), batch_size):
            batch_stocks = stock_list[i:i+batch_size]
            batch_num = i // batch_size + 1
            total_batches = (len(stock_list) + batch_size - 1) // batch_size
            
            print(f"\n📦 处理批次 {batch_num}/{total_batches}: {len(batch_stocks)} 只股票")
            
            # 多线程处理当前批次
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_stock = {executor.submit(self.reorganize_single_stock, stock): stock 
                                 for stock in batch_stocks}
                
                batch_results = []
                for future in as_completed(future_to_stock):
                    result = future.result()
                    batch_results.append(result)
                    results.append(result)
            
            # 批次统计
            success_count = sum(1 for r in batch_results if r['status'] == 'success')
            print(f"   ✅ 批次完成: {success_count}/{len(batch_stocks)} 成功")
        
        return results
    
    def generate_reorganization_report(self, results):
        """生成重组报告"""
        print(f"\n📊 生成重组报告...")
        print("=" * 80)
        
        # 统计结果
        success_results = [r for r in results if r['status'] == 'success']
        error_results = [r for r in results if r['status'] == 'error']
        no_data_results = [r for r in results if r['status'] == 'no_data']
        
        total_records = sum(r.get('records', 0) for r in success_results)
        
        print(f"✅ 重组成功: {len(success_results)} 只股票")
        print(f"❌ 重组失败: {len(error_results)} 只股票")
        print(f"⚠️ 无数据: {len(no_data_results)} 只股票")
        print(f"📋 总记录数: {total_records:,} 条")
        
        # 显示成功案例样本
        if success_results:
            print(f"\n✅ 成功案例样本（前10只）:")
            for result in success_results[:10]:
                print(f"   {result['stock_id']}: {result['records']} 条记录, {result['date_range']}")
        
        # 显示失败案例
        if error_results:
            print(f"\n❌ 失败案例（前5只）:")
            for result in error_results[:5]:
                print(f"   {result['stock_id']}: {result.get('error', 'Unknown error')}")
        
        # 保存详细报告
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report = {
            'reorganization_time': datetime.now().isoformat(),
            'summary': {
                'total_stocks': len(results),
                'success_count': len(success_results),
                'error_count': len(error_results),
                'no_data_count': len(no_data_results),
                'total_records': total_records
            },
            'results': results
        }
        
        report_file = self.output_path / f"reorganization_report_{timestamp}.json"
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False, default=str)
        
        print(f"\n📄 详细报告已保存: {report_file}")
        
        return report
    
    def run_complete_reorganization(self):
        """运行完整的数据重组流程"""
        start_time = datetime.now()
        
        print("🚀 智能个股数据重组器")
        print("🎯 目标：从批次文件重组完整的个股数据")
        print("=" * 80)
        
        try:
            # 1. 扫描批次文件，收集股票列表
            all_stocks, year_stats = self.scan_all_batch_files()
            
            if not all_stocks:
                print("❌ 未发现任何股票数据")
                return
            
            # 2. 获取股票基本信息
            stock_info = self.get_stock_basic_info(all_stocks)
            
            # 3. 重组所有股票数据
            results = self.reorganize_all_stocks(all_stocks)
            
            # 4. 生成报告
            report = self.generate_reorganization_report(results)
            
            # 5. 总结
            end_time = datetime.now()
            duration = end_time - start_time
            
            print(f"\n🎊 重组完成！")
            print(f"⏱️ 总耗时: {duration}")
            print(f"📁 输出目录: {self.output_path}")
            print(f"✅ 成功重组: {report['summary']['success_count']} 只股票")
            
        except Exception as e:
            self.logger.error(f"重组过程发生错误: {str(e)}")
            raise

def main():
    """主函数"""
    reorganizer = IntelligentStockDataReorganizer()
    reorganizer.run_complete_reorganization()

if __name__ == "__main__":
    main()