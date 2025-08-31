#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据质量检查器
=============

全面的数据质量检查和验证系统
"""

from typing import Dict, List, Optional, Union, Any, Tuple
import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
import logging
from pathlib import Path
import json
from scipy import stats
import warnings

logger = logging.getLogger(__name__)

class DataQualityChecker:
    """数据质量检查器
    
    提供全面的数据质量检查功能：
    - 缺失数据检测
    - 异常值检测
    - 数据类型验证
    - 一致性检查
    - 完整性验证
    - 质量报告生成
    """
    
    def __init__(self, config: Optional[Dict] = None):
        """初始化数据质量检查器
        
        Args:
            config: 配置参数
        """
        self.config = config or {}
        
        # 默认阈值配置
        self.thresholds = {
            'missing_rate': 0.1,      # 缺失率阈值 10%
            'outlier_zscore': 3.0,    # Z-score异常值阈值
            'outlier_iqr': 1.5,       # IQR异常值阈值
            'price_change': 0.5,      # 价格变化阈值 50%
            'volume_change': 10.0,    # 成交量变化阈值 1000%
            'trading_days': 245       # 年交易日数量
        }
        
        # 更新用户配置的阈值
        if 'thresholds' in self.config:
            self.thresholds.update(self.config['thresholds'])
        
        # 检查结果存储
        self.check_results = {}
        
    def check_missing_data(self, 
                          df: pd.DataFrame,
                          critical_columns: Optional[List[str]] = None) -> Dict[str, Any]:
        """检查缺失数据
        
        Args:
            df: 待检查的DataFrame
            critical_columns: 关键列列表
            
        Returns:
            Dict: 检查结果
        """
        logger.info("🔍 检查缺失数据...")
        
        if df.empty:
            return {'error': '数据为空'}
        
        results = {
            'total_rows': len(df),
            'columns_checked': list(df.columns),
            'missing_summary': {},
            'critical_issues': [],
            'recommendations': []
        }
        
        # 计算每列缺失率
        missing_counts = df.isnull().sum()
        missing_rates = missing_counts / len(df)
        
        for col in df.columns:
            missing_count = missing_counts[col]
            missing_rate = missing_rates[col]
            
            results['missing_summary'][col] = {
                'missing_count': int(missing_count),
                'missing_rate': float(missing_rate),
                'is_critical': col in (critical_columns or []),
                'severity': self._get_missing_severity(missing_rate)
            }
            
            # 检查关键列
            if critical_columns and col in critical_columns:
                if missing_rate > 0:
                    results['critical_issues'].append(
                        f"关键列 '{col}' 存在 {missing_count} 个缺失值 ({missing_rate:.2%})"
                    )
            
            # 高缺失率警告
            if missing_rate > self.thresholds['missing_rate']:
                results['recommendations'].append(
                    f"列 '{col}' 缺失率过高 ({missing_rate:.2%})，建议检查数据源或进行插值处理"
                )
        
        # 整体评估
        total_missing = missing_counts.sum()
        total_cells = len(df) * len(df.columns)
        overall_missing_rate = total_missing / total_cells
        
        results['overall'] = {
            'total_missing': int(total_missing),
            'total_cells': int(total_cells),
            'missing_rate': float(overall_missing_rate),
            'quality_score': 1.0 - overall_missing_rate
        }
        
        logger.info(f"✅ 缺失数据检查完成，整体缺失率: {overall_missing_rate:.2%}")
        return results
    
    def check_outliers(self, 
                      df: pd.DataFrame,
                      numeric_columns: Optional[List[str]] = None,
                      method: str = 'both') -> Dict[str, Any]:
        """检查异常值
        
        Args:
            df: 待检查的DataFrame
            numeric_columns: 数值列列表
            method: 检测方法 ('zscore', 'iqr', 'both')
            
        Returns:
            Dict: 检查结果
        """
        logger.info("🔍 检查异常值...")
        
        if df.empty:
            return {'error': '数据为空'}
        
        # 自动识别数值列
        if numeric_columns is None:
            numeric_columns = df.select_dtypes(include=[np.number]).columns.tolist()
        
        results = {
            'columns_checked': numeric_columns,
            'outlier_summary': {},
            'outlier_details': {},
            'recommendations': []
        }
        
        for col in numeric_columns:
            if col not in df.columns:
                continue
                
            series = df[col].dropna()
            if series.empty:
                continue
            
            col_results = {
                'total_values': len(series),
                'zscore_outliers': 0,
                'iqr_outliers': 0,
                'outlier_indices': []
            }
            
            # Z-Score方法
            if method in ['zscore', 'both']:
                z_scores = np.abs(stats.zscore(series))
                zscore_outliers = series[z_scores > self.thresholds['outlier_zscore']]
                col_results['zscore_outliers'] = len(zscore_outliers)
                col_results['outlier_indices'].extend(zscore_outliers.index.tolist())
            
            # IQR方法
            if method in ['iqr', 'both']:
                Q1 = series.quantile(0.25)
                Q3 = series.quantile(0.75)
                IQR = Q3 - Q1
                lower_bound = Q1 - self.thresholds['outlier_iqr'] * IQR
                upper_bound = Q3 + self.thresholds['outlier_iqr'] * IQR
                
                iqr_outliers = series[(series < lower_bound) | (series > upper_bound)]
                col_results['iqr_outliers'] = len(iqr_outliers)
                col_results['outlier_indices'].extend(iqr_outliers.index.tolist())
            
            # 去重异常值索引
            col_results['outlier_indices'] = list(set(col_results['outlier_indices']))
            col_results['outlier_rate'] = len(col_results['outlier_indices']) / len(series)
            
            results['outlier_summary'][col] = col_results
            
            # 详细异常值信息
            if col_results['outlier_indices']:
                outlier_values = series.loc[col_results['outlier_indices']]
                results['outlier_details'][col] = {
                    'min_outlier': float(outlier_values.min()),
                    'max_outlier': float(outlier_values.max()),
                    'outlier_values': outlier_values.tolist()[:10]  # 最多显示10个
                }
            
            # 建议
            if col_results['outlier_rate'] > 0.05:  # 5%异常值阈值
                results['recommendations'].append(
                    f"列 '{col}' 异常值比例较高 ({col_results['outlier_rate']:.2%})，建议进一步分析"
                )
        
        logger.info(f"✅ 异常值检查完成，检查了 {len(numeric_columns)} 个数值列")
        return results
    
    def check_data_types(self, 
                        df: pd.DataFrame,
                        expected_types: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """检查数据类型
        
        Args:
            df: 待检查的DataFrame  
            expected_types: 期望的数据类型字典 {column: dtype}
            
        Returns:
            Dict: 检查结果
        """
        logger.info("🔍 检查数据类型...")
        
        if df.empty:
            return {'error': '数据为空'}
        
        results = {
            'type_summary': {},
            'type_issues': [],
            'recommendations': []
        }
        
        # 默认期望类型（针对金融数据）
        if expected_types is None:
            expected_types = {
                'date': 'datetime64',
                'symbol': 'object',
                'open': 'float64',
                'high': 'float64', 
                'low': 'float64',
                'close': 'float64',
                'volume': 'int64',
                'amount': 'float64'
            }
        
        for col in df.columns:
            current_type = str(df[col].dtype)
            expected_type = expected_types.get(col, 'auto')
            
            col_info = {
                'current_type': current_type,
                'expected_type': expected_type,
                'is_correct': True,
                'null_count': int(df[col].isnull().sum())
            }
            
            # 检查类型匹配
            if expected_type != 'auto':
                if expected_type == 'datetime64' and not pd.api.types.is_datetime64_any_dtype(df[col]):
                    col_info['is_correct'] = False
                    results['type_issues'].append(f"列 '{col}' 应为日期类型，当前为 {current_type}")
                elif expected_type.startswith('float') and not pd.api.types.is_numeric_dtype(df[col]):
                    col_info['is_correct'] = False  
                    results['type_issues'].append(f"列 '{col}' 应为浮点数类型，当前为 {current_type}")
                elif expected_type.startswith('int') and not pd.api.types.is_integer_dtype(df[col]):
                    col_info['is_correct'] = False
                    results['type_issues'].append(f"列 '{col}' 应为整数类型，当前为 {current_type}")
                elif expected_type == 'object' and df[col].dtype != 'object':
                    col_info['is_correct'] = False
                    results['type_issues'].append(f"列 '{col}' 应为字符串类型，当前为 {current_type}")
            
            results['type_summary'][col] = col_info
        
        # 生成建议
        for issue in results['type_issues']:
            results['recommendations'].append(f"数据类型转换: {issue}")
        
        logger.info(f"✅ 数据类型检查完成，发现 {len(results['type_issues'])} 个问题")
        return results
    
    def check_price_data_consistency(self, df: pd.DataFrame) -> Dict[str, Any]:
        """检查价格数据一致性
        
        Args:
            df: 价格数据DataFrame
            
        Returns:
            Dict: 检查结果
        """
        logger.info("🔍 检查价格数据一致性...")
        
        if df.empty:
            return {'error': '数据为空'}
        
        required_cols = ['open', 'high', 'low', 'close']
        missing_cols = [col for col in required_cols if col not in df.columns]
        
        if missing_cols:
            return {'error': f'缺少必需的价格列: {missing_cols}'}
        
        results = {
            'consistency_issues': [],
            'price_relationships': {},
            'extreme_changes': [],
            'recommendations': []
        }
        
        # 检查价格关系 (high >= low, high >= open, high >= close, low <= open, low <= close)
        invalid_high_low = df['high'] < df['low']
        invalid_high_open = df['high'] < df['open'] 
        invalid_high_close = df['high'] < df['close']
        invalid_low_open = df['low'] > df['open']
        invalid_low_close = df['low'] > df['close']
        
        issue_counts = {
            'high_less_than_low': invalid_high_low.sum(),
            'high_less_than_open': invalid_high_open.sum(),
            'high_less_than_close': invalid_high_close.sum(),
            'low_greater_than_open': invalid_low_open.sum(),
            'low_greater_than_close': invalid_low_close.sum()
        }
        
        results['price_relationships'] = issue_counts
        
        # 记录一致性问题
        for issue_type, count in issue_counts.items():
            if count > 0:
                results['consistency_issues'].append(f"{issue_type}: {count} 条记录")
        
        # 检查极端价格变化
        if 'symbol' in df.columns and len(df) > 1:
            df_sorted = df.sort_values(['symbol', 'date'] if 'date' in df.columns else ['symbol'])
            
            for symbol in df_sorted['symbol'].unique():
                symbol_data = df_sorted[df_sorted['symbol'] == symbol]
                if len(symbol_data) < 2:
                    continue
                
                # 计算价格变化率
                price_changes = symbol_data['close'].pct_change().abs()
                extreme_changes = price_changes > self.thresholds['price_change']
                
                if extreme_changes.any():
                    extreme_indices = symbol_data[extreme_changes].index.tolist()
                    results['extreme_changes'].extend([
                        {
                            'symbol': symbol,
                            'index': idx,
                            'change_rate': float(price_changes.loc[idx])
                        }
                        for idx in extreme_indices
                    ])
        
        # 生成建议
        if results['consistency_issues']:
            results['recommendations'].append("存在价格关系不一致的记录，建议检查数据源质量")
        
        if results['extreme_changes']:
            results['recommendations'].append(
                f"发现 {len(results['extreme_changes'])} 个极端价格变化，建议进一步核实"
            )
        
        logger.info(f"✅ 价格数据一致性检查完成")
        return results
    
    def check_completeness(self, 
                          df: pd.DataFrame,
                          date_column: str = 'date',
                          symbol_column: str = 'symbol') -> Dict[str, Any]:
        """检查数据完整性
        
        Args:
            df: 待检查的DataFrame
            date_column: 日期列名
            symbol_column: 股票代码列名
            
        Returns:
            Dict: 检查结果
        """
        logger.info("🔍 检查数据完整性...")
        
        if df.empty:
            return {'error': '数据为空'}
        
        if date_column not in df.columns:
            return {'error': f'缺少日期列: {date_column}'}
        
        if symbol_column not in df.columns:
            return {'error': f'缺少股票代码列: {symbol_column}'}
        
        results = {
            'date_range': {},
            'symbol_coverage': {},
            'missing_dates': {},
            'recommendations': []
        }
        
        # 日期范围分析
        df[date_column] = pd.to_datetime(df[date_column])
        min_date = df[date_column].min()
        max_date = df[date_column].max()
        date_range = (max_date - min_date).days
        
        results['date_range'] = {
            'start_date': min_date.strftime('%Y-%m-%d'),
            'end_date': max_date.strftime('%Y-%m-%d'),
            'total_days': date_range,
            'unique_dates': df[date_column].nunique()
        }
        
        # 股票覆盖分析
        symbols = df[symbol_column].unique()
        results['symbol_coverage'] = {
            'total_symbols': len(symbols),
            'symbols_list': symbols.tolist()[:20],  # 最多显示20个
            'avg_records_per_symbol': len(df) / len(symbols)
        }
        
        # 检查缺失的交易日
        trading_dates = pd.date_range(start=min_date, end=max_date, freq='D')
        trading_dates = trading_dates[trading_dates.weekday < 5]  # 去除周末
        
        existing_dates = set(df[date_column].dt.date)
        expected_dates = set(trading_dates.date)
        missing_dates = expected_dates - existing_dates
        
        if missing_dates:
            missing_list = sorted(list(missing_dates))[:10]  # 最多显示10个
            results['missing_dates'] = {
                'count': len(missing_dates),
                'sample_dates': [d.strftime('%Y-%m-%d') for d in missing_list]
            }
            
            results['recommendations'].append(
                f"发现 {len(missing_dates)} 个缺失的交易日，建议检查是否为节假日或数据源问题"
            )
        
        # 检查每个股票的数据完整性
        incomplete_symbols = []
        expected_records = results['date_range']['unique_dates']
        
        for symbol in symbols:
            symbol_records = len(df[df[symbol_column] == symbol])
            completeness = symbol_records / expected_records
            
            if completeness < 0.9:  # 完整度低于90%
                incomplete_symbols.append({
                    'symbol': symbol,
                    'records': symbol_records,
                    'expected': expected_records,
                    'completeness': completeness
                })
        
        if incomplete_symbols:
            results['incomplete_symbols'] = incomplete_symbols[:10]  # 最多显示10个
            results['recommendations'].append(
                f"{len(incomplete_symbols)} 个股票的数据不完整，建议补充缺失数据"
            )
        
        logger.info(f"✅ 数据完整性检查完成")
        return results
    
    def generate_quality_report(self, 
                               df: pd.DataFrame,
                               report_name: str = "数据质量报告") -> Dict[str, Any]:
        """生成综合质量报告
        
        Args:
            df: 待检查的DataFrame
            report_name: 报告名称
            
        Returns:
            Dict: 综合质量报告
        """
        logger.info("📊 生成数据质量报告...")
        
        report = {
            'report_name': report_name,
            'timestamp': datetime.now().isoformat(),
            'data_info': {
                'rows': len(df),
                'columns': len(df.columns),
                'memory_usage_mb': df.memory_usage(deep=True).sum() / 1024 / 1024
            },
            'quality_checks': {},
            'overall_score': 0.0,
            'recommendations': []
        }
        
        # 执行各项检查
        checks = [
            ('missing_data', self.check_missing_data),
            ('outliers', self.check_outliers), 
            ('data_types', self.check_data_types),
            ('completeness', self.check_completeness)
        ]
        
        # 如果是价格数据，添加一致性检查
        price_cols = ['open', 'high', 'low', 'close']
        if all(col in df.columns for col in price_cols):
            checks.append(('price_consistency', self.check_price_data_consistency))
        
        scores = []
        all_recommendations = []
        
        for check_name, check_func in checks:
            try:
                result = check_func(df)
                if 'error' not in result:
                    report['quality_checks'][check_name] = result
                    
                    # 计算得分
                    score = self._calculate_check_score(check_name, result)
                    scores.append(score)
                    
                    # 收集建议
                    if 'recommendations' in result:
                        all_recommendations.extend(result['recommendations'])
                else:
                    logger.warning(f"⚠️ {check_name} 检查失败: {result['error']}")
                    
            except Exception as e:
                logger.error(f"❌ {check_name} 检查出错: {str(e)}")
        
        # 计算整体得分
        if scores:
            report['overall_score'] = sum(scores) / len(scores)
        
        # 合并建议
        report['recommendations'] = list(set(all_recommendations))
        
        # 质量评级
        score = report['overall_score']
        if score >= 0.9:
            report['quality_grade'] = 'A (优秀)'
        elif score >= 0.8:
            report['quality_grade'] = 'B (良好)'
        elif score >= 0.7:
            report['quality_grade'] = 'C (一般)'
        elif score >= 0.6:
            report['quality_grade'] = 'D (较差)'
        else:
            report['quality_grade'] = 'F (极差)'
        
        logger.info(f"✅ 数据质量报告生成完成，整体得分: {score:.2f}")
        return report
    
    def _calculate_check_score(self, check_name: str, result: Dict[str, Any]) -> float:
        """计算单项检查得分"""
        if check_name == 'missing_data':
            if 'overall' in result:
                return result['overall'].get('quality_score', 0.8)
        elif check_name == 'outliers':
            # 基于异常值比例计算得分
            outlier_rates = []
            for col_result in result.get('outlier_summary', {}).values():
                outlier_rates.append(col_result.get('outlier_rate', 0))
            if outlier_rates:
                avg_outlier_rate = sum(outlier_rates) / len(outlier_rates)
                return max(0.0, 1.0 - avg_outlier_rate * 5)  # 异常值率*5作为扣分
        elif check_name == 'data_types':
            total_cols = len(result.get('type_summary', {}))
            issues = len(result.get('type_issues', []))
            if total_cols > 0:
                return max(0.0, (total_cols - issues) / total_cols)
        elif check_name == 'completeness':
            # 基于数据完整性评分
            incomplete_count = len(result.get('incomplete_symbols', []))
            total_symbols = result.get('symbol_coverage', {}).get('total_symbols', 1)
            return max(0.0, (total_symbols - incomplete_count) / total_symbols)
        elif check_name == 'price_consistency':
            # 基于一致性问题数量评分
            issues = len(result.get('consistency_issues', []))
            return max(0.0, 1.0 - issues * 0.1)
        
        return 0.8  # 默认得分
    
    def _get_missing_severity(self, missing_rate: float) -> str:
        """获取缺失数据严重程度"""
        if missing_rate == 0:
            return '无'
        elif missing_rate <= 0.05:
            return '轻微'
        elif missing_rate <= 0.15:
            return '中等'
        elif missing_rate <= 0.30:
            return '严重'
        else:
            return '极严重'
    
    def save_report(self, report: Dict[str, Any], file_path: Optional[str] = None) -> str:
        """保存质量报告到文件
        
        Args:
            report: 质量报告
            file_path: 保存路径
            
        Returns:
            str: 保存的文件路径
        """
        if file_path is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            file_path = f"data_quality_report_{timestamp}.json"
        
        # 确保目录存在
        Path(file_path).parent.mkdir(parents=True, exist_ok=True)
        
        # 保存JSON报告
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False, default=str)
        
        logger.info(f"✅ 质量报告已保存至: {file_path}")
        return file_path