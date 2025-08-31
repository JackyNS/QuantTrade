#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据质量深度检查工具
"""

import pandas as pd
import numpy as np
from pathlib import Path
import json
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

class DataQualityChecker:
    """数据质量检查器"""
    
    def __init__(self):
        self.data_root = Path("data")
        self.quality_report = {}
        
    def check_sample_data_quality(self):
        """检查样本数据质量"""
        print("🎯 深度数据质量检查...")
        
        quality_issues = {
            'priority_download': {},
            'smart_download': {},
            'historical_download': {}
        }
        
        # 1. 检查优先级下载器数据
        priority_daily = self.data_root / "priority_download/market_data/daily"
        if priority_daily.exists():
            sample_files = list(priority_daily.glob("**/2024_batch_001.csv"))[:3]
            for file_path in sample_files:
                issues = self._analyze_file_quality(file_path, "daily_market")
                if issues:
                    quality_issues['priority_download'][file_path.name] = issues
        
        # 2. 检查智能下载器数据  
        smart_2024 = self.data_root / "smart_download/year_2024"
        if smart_2024.exists():
            sample_files = list(smart_2024.glob("batch_001.csv"))
            for file_path in sample_files:
                issues = self._analyze_file_quality(file_path, "daily_market")
                if issues:
                    quality_issues['smart_download'][file_path.name] = issues
        
        # 3. 检查历史下载器数据
        hist_2024 = self.data_root / "historical_download/market_data/year_2024"
        if hist_2024.exists():
            sample_files = list(hist_2024.glob("batch_001.csv"))
            for file_path in sample_files:
                issues = self._analyze_file_quality(file_path, "daily_market")
                if issues:
                    quality_issues['historical_download'][file_path.name] = issues
        
        return quality_issues
    
    def _analyze_file_quality(self, file_path, data_type):
        """分析单个文件质量"""
        issues = []
        
        try:
            df = pd.read_csv(file_path)
            
            # 基础检查
            if df.empty:
                issues.append("文件为空")
                return issues
            
            # 1. 空值检查
            null_counts = df.isnull().sum()
            critical_nulls = null_counts[null_counts > 0]
            if len(critical_nulls) > 0:
                null_info = {}
                for col, count in critical_nulls.items():
                    null_info[col] = f"{count}/{len(df)} ({count/len(df)*100:.1f}%)"
                issues.append(f"空值: {null_info}")
            
            # 2. 重复记录检查
            if data_type == "daily_market":
                # 检查同一股票同一日期的重复
                if 'ticker' in df.columns and 'tradeDate' in df.columns:
                    duplicates = df.duplicated(['ticker', 'tradeDate']).sum()
                    if duplicates > 0:
                        issues.append(f"重复记录: {duplicates} 条")
            
            # 3. 价格异常检查
            price_cols = ['openPrice', 'highestPrice', 'lowestPrice', 'closePrice']
            for col in price_cols:
                if col in df.columns:
                    prices = df[col].dropna()
                    if len(prices) > 0:
                        # 负价格或零价格
                        invalid_prices = (prices <= 0).sum()
                        if invalid_prices > 0:
                            issues.append(f"{col}异常价格: {invalid_prices} 条 <= 0")
                        
                        # 极端价格（超过1000元或低于0.01元）
                        extreme_high = (prices > 1000).sum()
                        extreme_low = ((prices > 0) & (prices < 0.01)).sum()
                        if extreme_high > 0:
                            issues.append(f"{col}极高价格: {extreme_high} 条 > 1000")
                        if extreme_low > 0:
                            issues.append(f"{col}极低价格: {extreme_low} 条 < 0.01")
            
            # 4. 逻辑关系检查
            if all(col in df.columns for col in ['openPrice', 'highestPrice', 'lowestPrice', 'closePrice']):
                # 检查 high >= open, close, low 和 low <= open, close, high
                high_issues = ((df['highestPrice'] < df['openPrice']) | 
                              (df['highestPrice'] < df['closePrice']) |
                              (df['highestPrice'] < df['lowestPrice'])).sum()
                
                low_issues = ((df['lowestPrice'] > df['openPrice']) |
                             (df['lowestPrice'] > df['closePrice']) |
                             (df['lowestPrice'] > df['highestPrice'])).sum()
                
                if high_issues > 0:
                    issues.append(f"最高价逻辑错误: {high_issues} 条")
                if low_issues > 0:
                    issues.append(f"最低价逻辑错误: {low_issues} 条")
            
            # 5. 交易量检查
            if 'turnoverVol' in df.columns:
                volumes = df['turnoverVol'].dropna()
                if len(volumes) > 0:
                    negative_vol = (volumes < 0).sum()
                    if negative_vol > 0:
                        issues.append(f"负交易量: {negative_vol} 条")
            
            # 6. 日期格式检查
            if 'tradeDate' in df.columns:
                try:
                    pd.to_datetime(df['tradeDate'])
                except:
                    issues.append("日期格式错误")
            
            # 7. 数据完整性检查（记录数是否合理）
            if data_type == "daily_market":
                expected_records = self._estimate_expected_records(file_path, df)
                actual_records = len(df)
                if actual_records < expected_records * 0.8:  # 少于预期80%
                    issues.append(f"数据不完整: {actual_records}/{expected_records} 条")
                    
        except Exception as e:
            issues.append(f"读取失败: {str(e)}")
        
        return issues
    
    def _estimate_expected_records(self, file_path, df):
        """估算预期记录数"""
        # 根据文件路径判断年份
        path_str = str(file_path)
        year = None
        for y in range(2000, 2026):
            if str(y) in path_str:
                year = y
                break
        
        if year is None:
            return len(df)  # 无法估算则返回实际数量
        
        # 估算该年份的交易日数量
        trading_days = 250  # 平均每年交易日
        if year == 2025:
            # 2025年按当前日期计算
            days_passed = (datetime.now() - datetime(2025, 1, 1)).days
            trading_days = min(days_passed * 250 / 365, 250)
        
        # 估算股票数量（批次文件通常包含100只左右）
        unique_stocks = df['ticker'].nunique() if 'ticker' in df.columns else 100
        
        return int(trading_days * unique_stocks)
    
    def check_data_consistency(self):
        """检查数据一致性"""
        print("\n🔍 检查数据一致性...")
        
        consistency_issues = {}
        
        # 检查同一年份不同下载器的数据一致性
        test_years = [2020, 2023, 2024]
        
        for year in test_years:
            year_issues = []
            
            # 获取各下载器该年份数据
            files = {
                'historical': list((self.data_root / f"historical_download/market_data/year_{year}").glob("batch_001.csv")),
                'smart': list((self.data_root / f"smart_download/year_{year}").glob("batch_001.csv")),
                'priority': list((self.data_root / f"priority_download/market_data/daily").glob(f"{year}_batch_001.csv"))
            }
            
            # 比较数据
            dfs = {}
            for downloader, file_list in files.items():
                if file_list:
                    try:
                        df = pd.read_csv(file_list[0])
                        if not df.empty and 'ticker' in df.columns and 'closePrice' in df.columns:
                            dfs[downloader] = df
                    except:
                        continue
            
            # 检查相同股票相同日期的价格是否一致
            if len(dfs) >= 2:
                downloader_names = list(dfs.keys())
                for i, d1 in enumerate(downloader_names[:-1]):
                    for d2 in downloader_names[i+1:]:
                        df1, df2 = dfs[d1], dfs[d2]
                        
                        # 找出共同的股票和日期
                        if all(col in df1.columns and col in df2.columns for col in ['ticker', 'tradeDate', 'closePrice']):
                            common = pd.merge(df1[['ticker', 'tradeDate', 'closePrice']], 
                                            df2[['ticker', 'tradeDate', 'closePrice']], 
                                            on=['ticker', 'tradeDate'], 
                                            suffixes=('_1', '_2'))
                            
                            if len(common) > 0:
                                # 检查价格差异
                                price_diff = abs(common['closePrice_1'] - common['closePrice_2'])
                                inconsistent = (price_diff > 0.01).sum()  # 差异超过1分钱
                                
                                if inconsistent > 0:
                                    total_common = len(common)
                                    year_issues.append(f"{d1} vs {d2}: {inconsistent}/{total_common} 条价格不一致")
            
            if year_issues:
                consistency_issues[year] = year_issues
        
        return consistency_issues
    
    def check_temporal_continuity(self):
        """检查时间连续性"""
        print("\n📅 检查时间连续性...")
        
        continuity_issues = {}
        
        # 检查优先级数据的时间连续性
        daily_dir = self.data_root / "priority_download/market_data/daily"
        if daily_dir.exists():
            # 随机选择几个股票检查
            sample_files = list(daily_dir.glob("2024_batch_001.csv"))[:1]
            
            for file_path in sample_files:
                try:
                    df = pd.read_csv(file_path)
                    if 'ticker' in df.columns and 'tradeDate' in df.columns:
                        # 选择数据较多的股票
                        stock_counts = df['ticker'].value_counts()
                        top_stocks = stock_counts.head(3).index
                        
                        for stock in top_stocks:
                            stock_data = df[df['ticker'] == stock].copy()
                            stock_data['tradeDate'] = pd.to_datetime(stock_data['tradeDate'])
                            stock_data = stock_data.sort_values('tradeDate')
                            
                            # 检查日期间隔
                            date_diffs = stock_data['tradeDate'].diff().dt.days
                            
                            # 正常交易日间隔应该是1-3天（考虑周末）
                            large_gaps = date_diffs[date_diffs > 7]  # 超过一周的间隔
                            
                            if len(large_gaps) > 0:
                                continuity_issues[f"{file_path.name}_{stock}"] = f"{len(large_gaps)} 个大间隔"
                
                except Exception as e:
                    continuity_issues[file_path.name] = f"检查失败: {e}"
        
        return continuity_issues
    
    def generate_quality_report(self):
        """生成质量报告"""
        print("🚀 开始数据质量深度检查...\n")
        
        # 执行各项检查
        sample_quality = self.check_sample_data_quality()
        consistency = self.check_data_consistency()
        continuity = self.check_temporal_continuity()
        
        # 汇总报告
        report = {
            'check_time': datetime.now().isoformat(),
            'sample_quality_issues': sample_quality,
            'consistency_issues': consistency,
            'continuity_issues': continuity,
            'summary': self._generate_quality_summary(sample_quality, consistency, continuity)
        }
        
        # 保存报告
        report_file = self.data_root / 'data_quality_report.json'
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False, default=str)
        
        print(f"\n📋 质量报告已保存: {report_file}")
        return report
    
    def _generate_quality_summary(self, sample_quality, consistency, continuity):
        """生成质量摘要"""
        summary = {
            'overall_quality': 'good',
            'critical_issues': 0,
            'warning_issues': 0,
            'recommendations': []
        }
        
        # 统计问题
        for downloader, files in sample_quality.items():
            for filename, issues in files.items():
                for issue in issues:
                    if any(keyword in issue for keyword in ['异常价格', '逻辑错误', '重复记录']):
                        summary['critical_issues'] += 1
                    else:
                        summary['warning_issues'] += 1
        
        summary['critical_issues'] += len(consistency)
        summary['warning_issues'] += len(continuity)
        
        # 评估整体质量
        if summary['critical_issues'] > 5:
            summary['overall_quality'] = 'poor'
            summary['recommendations'].append('立即修复严重数据质量问题')
        elif summary['critical_issues'] > 0:
            summary['overall_quality'] = 'fair'
            summary['recommendations'].append('修复发现的数据质量问题')
        
        if summary['warning_issues'] > 10:
            summary['recommendations'].append('处理数据警告问题以提升质量')
        
        return summary
    
    def print_quality_summary(self, report):
        """打印质量摘要"""
        print("\n" + "="*60)
        print("📊 数据质量检查摘要")
        print("="*60)
        
        summary = report['summary']
        print(f"🎯 整体质量: {summary['overall_quality'].upper()}")
        print(f"❌ 严重问题: {summary['critical_issues']} 个")
        print(f"⚠️ 警告问题: {summary['warning_issues']} 个")
        
        if summary['critical_issues'] > 0:
            print(f"\n🔍 严重问题详情:")
            for downloader, files in report['sample_quality_issues'].items():
                for filename, issues in files.items():
                    critical = [i for i in issues if any(k in i for k in ['异常价格', '逻辑错误', '重复记录'])]
                    if critical:
                        print(f"   📁 {downloader}/{filename}:")
                        for issue in critical[:3]:  # 显示前3个
                            print(f"      • {issue}")
        
        if report['consistency_issues']:
            print(f"\n📊 数据一致性问题:")
            for year, issues in report['consistency_issues'].items():
                print(f"   {year}年: {len(issues)} 个问题")
                for issue in issues[:2]:
                    print(f"      • {issue}")
        
        if summary['recommendations']:
            print(f"\n💡 建议:")
            for rec in summary['recommendations']:
                print(f"   • {rec}")

def main():
    """主函数"""
    checker = DataQualityChecker()
    report = checker.generate_quality_report()
    checker.print_quality_summary(report)

if __name__ == "__main__":
    main()