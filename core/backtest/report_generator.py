#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
报告生成器 - report_generator.py
================================

生成专业的回测报告，支持多种格式输出。

主要功能：
1. 生成回测摘要
2. 计算和展示性能指标
3. 生成可视化图表
4. 导出多种格式（HTML、Excel、JSON、PDF）

版本: 1.0.0
更新: 2025-08-30
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any
from datetime import datetime
import logging
from dataclasses import dataclass
from pathlib import Path
import json

# 可视化相关
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.figure import Figure
import warnings
warnings.filterwarnings('ignore')


# ==========================================
# 📊 报告数据类
# ==========================================

@dataclass
class BacktestReport:
    """回测报告数据类"""
    summary: Dict                   # 摘要信息
    metrics: Dict                   # 性能指标
    equity_curve: pd.DataFrame      # 权益曲线
    transactions: pd.DataFrame      # 交易记录
    monthly_returns: pd.DataFrame   # 月度收益
    
    def to_dict(self) -> Dict:
        """转换为字典"""
        return {
            'summary': self.summary,
            'metrics': self.metrics,
            'equity_curve': self.equity_curve.to_dict() if not self.equity_curve.empty else {},
            'transactions': self.transactions.to_dict() if not self.transactions.empty else {},
            'monthly_returns': self.monthly_returns.to_dict() if not self.monthly_returns.empty else {}
        }


# ==========================================
# 📈 报告生成器主类
# ==========================================

class ReportGenerator:
    """报告生成器"""
    
    def __init__(self, results: Dict):
        """
        初始化报告生成器
        
        Args:
            results: 回测结果字典
        """
        self.results = results
        self.equity_curve = results.get('equity_curve', pd.DataFrame())
        self.transactions = results.get('transactions', pd.DataFrame())
        self.returns = results.get('returns', pd.Series())
        
        # 设置matplotlib样式
        try:
            plt.style.use('seaborn-v0_8')
        except:
            plt.style.use('default')  # 如果seaborn样式不可用，使用默认样式
        
        # 设置中文字体（如果需要）
        plt.rcParams['font.sans-serif'] = ['DejaVu Sans']
        plt.rcParams['axes.unicode_minus'] = False
        
        # 设置日志
        self.logger = logging.getLogger('ReportGenerator')
        self.logger.info("报告生成器初始化完成")
    
    # ==========================================
    # 📊 报告生成
    # ==========================================
    
    def generate(self) -> BacktestReport:
        """生成回测报告"""
        
        self.logger.info("开始生成回测报告...")
        
        # 生成摘要
        summary = self._generate_summary()
        
        # 计算性能指标
        metrics = self._calculate_metrics()
        
        # 计算月度收益
        monthly_returns = self._calculate_monthly_returns()
        
        # 创建报告对象
        report = BacktestReport(
            summary=summary,
            metrics=metrics,
            equity_curve=self.equity_curve,
            transactions=self.transactions,
            monthly_returns=monthly_returns
        )
        
        self.logger.info("回测报告生成完成")
        return report
    
    def _generate_summary(self) -> Dict:
        """生成摘要信息"""
        if self.equity_curve.empty:
            return {
                'status': 'No data available',
                'message': '无可用数据生成报告'
            }
        
        initial_value = self.equity_curve['total_value'].iloc[0]
        final_value = self.equity_curve['total_value'].iloc[-1]
        total_return = (final_value - initial_value) / initial_value
        
        # 计算交易天数
        trading_days = len(self.equity_curve)
        years = trading_days / 252
        
        summary = {
            'start_date': str(self.equity_curve.index[0].date()),
            'end_date': str(self.equity_curve.index[-1].date()),
            'trading_days': trading_days,
            'years': round(years, 2),
            'initial_capital': round(initial_value, 2),
            'final_value': round(final_value, 2),
            'total_return': round(total_return, 4),
            'annual_return': round((1 + total_return) ** (1/years) - 1, 4) if years > 0 else 0,
            'total_trades': len(self.transactions),
            'avg_trades_per_month': round(len(self.transactions) / (years * 12), 1) if years > 0 else 0
        }
        
        return summary
    
    def _calculate_metrics(self) -> Dict:
        """计算性能指标"""
        try:
            # 导入性能分析器
            from .performance_analyzer import PerformanceAnalyzer
            
            analyzer = PerformanceAnalyzer(self.results)
            metrics = analyzer.calculate_all_metrics()
            
            return metrics.to_dict()
        except Exception as e:
            self.logger.warning(f"性能指标计算失败: {e}")
            
            # 返回基础指标
            return {
                'total_return': 'N/A',
                'annual_return': 'N/A',
                'sharpe_ratio': 'N/A',
                'max_drawdown': 'N/A',
                'win_rate': 'N/A'
            }
    
    def _calculate_monthly_returns(self) -> pd.DataFrame:
        """计算月度收益"""
        if self.returns.empty:
            return pd.DataFrame()
        
        try:
            # 按月分组计算收益
            monthly = self.returns.resample('M').apply(
                lambda x: (1 + x).prod() - 1
            )
            
            # 创建月度收益矩阵
            monthly_matrix = pd.DataFrame(
                monthly.values,
                index=monthly.index,
                columns=['return']
            )
            
            monthly_matrix['year'] = monthly_matrix.index.year
            monthly_matrix['month'] = monthly_matrix.index.month
            
            # 转换为透视表
            pivot = monthly_matrix.pivot_table(
                values='return',
                index='year',
                columns='month',
                aggfunc='first'
            )
            
            # 添加年度汇总
            pivot['Year'] = pivot.mean(axis=1)
            
            return pivot
            
        except Exception as e:
            self.logger.warning(f"月度收益计算失败: {e}")
            return pd.DataFrame()
    
    # ==========================================
    # 📈 可视化
    # ==========================================
    
    def create_charts(self) -> Dict[str, Figure]:
        """创建图表"""
        charts = {}
        
        # 1. 权益曲线图
        charts['equity_curve'] = self._create_equity_curve_chart()
        
        # 2. 回撤图
        charts['drawdown'] = self._create_drawdown_chart()
        
        # 3. 月度收益热力图
        charts['monthly_returns'] = self._create_monthly_returns_heatmap()
        
        # 4. 收益分布图
        charts['returns_distribution'] = self._create_returns_distribution()
        
        return charts
    
    def _create_equity_curve_chart(self) -> Figure:
        """创建权益曲线图"""
        fig, ax = plt.subplots(figsize=(12, 6))
        
        if not self.equity_curve.empty:
            ax.plot(self.equity_curve.index, self.equity_curve['total_value'], 
                   label='Portfolio Value', linewidth=2)
            
            # 添加买卖点标记
            if not self.transactions.empty:
                buy_trades = self.transactions[self.transactions['side'] == 'BUY']
                sell_trades = self.transactions[self.transactions['side'] == 'SELL']
                
                for _, trade in buy_trades.iterrows():
                    ax.axvline(x=trade['timestamp'], color='g', alpha=0.3, linestyle='--')
                
                for _, trade in sell_trades.iterrows():
                    ax.axvline(x=trade['timestamp'], color='r', alpha=0.3, linestyle='--')
            
            ax.set_title('Portfolio Equity Curve', fontsize=14, fontweight='bold')
            ax.set_xlabel('Date')
            ax.set_ylabel('Portfolio Value')
            ax.legend()
            ax.grid(True, alpha=0.3)
            
            # 格式化x轴日期
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
            plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
        
        plt.tight_layout()
        return fig
    
    def _create_drawdown_chart(self) -> Figure:
        """创建回撤图"""
        fig, ax = plt.subplots(figsize=(12, 4))
        
        if not self.returns.empty:
            # 计算回撤
            cumulative = (1 + self.returns).cumprod()
            running_max = cumulative.expanding().max()
            drawdown = (cumulative - running_max) / running_max
            
            # 填充回撤区域
            ax.fill_between(drawdown.index, 0, drawdown.values, 
                           color='red', alpha=0.3, label='Drawdown')
            ax.plot(drawdown.index, drawdown.values, color='red', linewidth=1)
            
            ax.set_title('Drawdown Chart', fontsize=14, fontweight='bold')
            ax.set_xlabel('Date')
            ax.set_ylabel('Drawdown (%)')
            ax.legend()
            ax.grid(True, alpha=0.3)
            
            # 格式化y轴为百分比
            ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda y, _: '{:.0%}'.format(y)))
            
            # 格式化x轴日期
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
            plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
        
        plt.tight_layout()
        return fig
    
    def _create_monthly_returns_heatmap(self) -> Figure:
        """创建月度收益热力图"""
        fig, ax = plt.subplots(figsize=(12, 6))
        
        monthly_returns = self._calculate_monthly_returns()
        
        if not monthly_returns.empty:
            # 创建热力图数据
            import seaborn as sns
            
            # 移除年度汇总列用于热力图
            heatmap_data = monthly_returns.drop('Year', axis=1, errors='ignore')
            
            # 创建热力图
            sns.heatmap(heatmap_data, annot=True, fmt='.1%', 
                       cmap='RdYlGn', center=0, ax=ax,
                       cbar_kws={'label': 'Return (%)'})
            
            ax.set_title('Monthly Returns Heatmap', fontsize=14, fontweight='bold')
            ax.set_xlabel('Month')
            ax.set_ylabel('Year')
        
        plt.tight_layout()
        return fig
    
    def _create_returns_distribution(self) -> Figure:
        """创建收益分布图"""
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))
        
        if not self.returns.empty:
            # 直方图
            ax1.hist(self.returns, bins=50, alpha=0.7, color='blue', edgecolor='black')
            ax1.axvline(x=self.returns.mean(), color='red', linestyle='--', 
                       label=f'Mean: {self.returns.mean():.2%}')
            ax1.set_title('Returns Distribution', fontsize=12, fontweight='bold')
            ax1.set_xlabel('Daily Return')
            ax1.set_ylabel('Frequency')
            ax1.legend()
            ax1.grid(True, alpha=0.3)
            
            # Q-Q图
            from scipy import stats
            stats.probplot(self.returns, dist="norm", plot=ax2)
            ax2.set_title('Q-Q Plot', fontsize=12, fontweight='bold')
            ax2.grid(True, alpha=0.3)
        
        plt.tight_layout()
        return fig
    
    # ==========================================
    # 📄 报告导出
    # ==========================================
    
    def export_html(self, report: BacktestReport, filepath: str):
        """导出HTML报告"""
        path = Path(filepath)
        path.parent.mkdir(parents=True, exist_ok=True)
        
        # 生成HTML内容
        html_content = self._generate_html_content(report)
        
        # 保存文件
        with open(path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        self.logger.info(f"HTML报告已保存至: {filepath}")
    
    def _generate_html_content(self, report: BacktestReport) -> str:
        """生成HTML内容"""
        html = f"""
        <!DOCTYPE html>
        <html lang="zh-CN">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>量化策略回测报告</title>
            <style>
                * {{
                    margin: 0;
                    padding: 0;
                    box-sizing: border-box;
                }}
                body {{
                    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
                    line-height: 1.6;
                    color: #333;
                    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    min-height: 100vh;
                    padding: 20px;
                }}
                .container {{
                    max-width: 1200px;
                    margin: 0 auto;
                    background: white;
                    border-radius: 10px;
                    box-shadow: 0 20px 60px rgba(0,0,0,0.3);
                    overflow: hidden;
                }}
                .header {{
                    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    color: white;
                    padding: 40px;
                    text-align: center;
                }}
                .header h1 {{
                    font-size: 2.5em;
                    margin-bottom: 10px;
                }}
                .header p {{
                    font-size: 1.1em;
                    opacity: 0.9;
                }}
                .content {{
                    padding: 40px;
                }}
                .section {{
                    margin-bottom: 40px;
                }}
                .section h2 {{
                    color: #667eea;
                    margin-bottom: 20px;
                    padding-bottom: 10px;
                    border-bottom: 2px solid #667eea;
                }}
                table {{
                    width: 100%;
                    border-collapse: collapse;
                    margin-top: 20px;
                }}
                th {{
                    background: #f8f9fa;
                    padding: 12px;
                    text-align: left;
                    font-weight: 600;
                    color: #495057;
                    border-bottom: 2px solid #dee2e6;
                }}
                td {{
                    padding: 12px;
                    border-bottom: 1px solid #dee2e6;
                }}
                tr:hover {{
                    background: #f8f9fa;
                }}
                .metric-card {{
                    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    color: white;
                    padding: 20px;
                    border-radius: 10px;
                    margin: 10px;
                    display: inline-block;
                    min-width: 200px;
                    box-shadow: 0 5px 15px rgba(0,0,0,0.1);
                }}
                .metric-value {{
                    font-size: 2em;
                    font-weight: bold;
                }}
                .metric-label {{
                    font-size: 0.9em;
                    opacity: 0.9;
                    margin-top: 5px;
                }}
                .footer {{
                    background: #f8f9fa;
                    padding: 20px;
                    text-align: center;
                    color: #6c757d;
                }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>📈 量化策略回测报告</h1>
                    <p>生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                </div>
                
                <div class="content">
                    <div class="section">
                        <h2>📊 回测摘要</h2>
                        <table>
                            <tr>
                                <th>指标</th>
                                <th>数值</th>
                            </tr>
        """
        
        # 添加摘要数据
        for key, value in report.summary.items():
            if isinstance(value, float):
                if 'return' in key:
                    value_str = f"{value:.2%}"
                else:
                    value_str = f"{value:,.2f}"
            else:
                value_str = str(value)
            
            html += f"""
                            <tr>
                                <td><strong>{key.replace('_', ' ').title()}</strong></td>
                                <td>{value_str}</td>
                            </tr>
            """
        
        html += """
                        </table>
                    </div>
                    
                    <div class="section">
                        <h2>💡 关键指标</h2>
                        <div style="display: flex; flex-wrap: wrap; justify-content: space-around;">
        """
        
        # 添加关键指标卡片
        key_metrics = {
            'Total Return': report.metrics.get('total_return', 'N/A'),
            'Annual Return': report.metrics.get('annual_return', 'N/A'),
            'Sharpe Ratio': report.metrics.get('sharpe_ratio', 'N/A'),
            'Max Drawdown': report.metrics.get('max_drawdown', 'N/A'),
            'Win Rate': report.metrics.get('win_rate', 'N/A'),
            'Profit Factor': report.metrics.get('profit_factor', 'N/A')
        }
        
        for label, value in key_metrics.items():
            html += f"""
                            <div class="metric-card">
                                <div class="metric-value">{value}</div>
                                <div class="metric-label">{label}</div>
                            </div>
            """
        
        html += """
                        </div>
                    </div>
                    
                    <div class="section">
                        <h2>📈 详细指标</h2>
                        <table>
                            <tr>
                                <th>指标名称</th>
                                <th>数值</th>
                            </tr>
        """
        
        # 添加所有性能指标
        for key, value in report.metrics.items():
            html += f"""
                            <tr>
                                <td><strong>{key.replace('_', ' ').title()}</strong></td>
                                <td>{value}</td>
                            </tr>
            """
        
        html += """
                        </table>
                    </div>
                </div>
                
                <div class="footer">
                    <p>© 2025 量化交易框架 | Powered by Python</p>
                </div>
            </div>
        </body>
        </html>
        """
        
        return html
    
    def export_excel(self, report: BacktestReport, filepath: str):
        """导出Excel报告"""
        path = Path(filepath)
        path.parent.mkdir(parents=True, exist_ok=True)
        
        try:
            with pd.ExcelWriter(path, engine='openpyxl') as writer:
                # 1. 摘要表
                summary_df = pd.DataFrame([report.summary]).T
                summary_df.columns = ['Value']
                summary_df.index.name = 'Metric'
                summary_df.to_excel(writer, sheet_name='Summary')
                
                # 2. 性能指标
                metrics_df = pd.DataFrame([report.metrics]).T
                metrics_df.columns = ['Value']
                metrics_df.index.name = 'Metric'
                metrics_df.to_excel(writer, sheet_name='Performance Metrics')
                
                # 3. 权益曲线
                if not report.equity_curve.empty:
                    report.equity_curve.to_excel(writer, sheet_name='Equity Curve')
                
                # 4. 交易记录
                if not report.transactions.empty:
                    report.transactions.to_excel(writer, sheet_name='Transactions', index=False)
                
                # 5. 月度收益
                if not report.monthly_returns.empty:
                    report.monthly_returns.to_excel(writer, sheet_name='Monthly Returns')
                
                # 格式化Excel
                self._format_excel(writer)
            
            self.logger.info(f"Excel报告已保存至: {filepath}")
            
        except Exception as e:
            self.logger.error(f"Excel报告保存失败: {e}")
    
    def _format_excel(self, writer):
        """格式化Excel工作表"""
        try:
            # 获取工作簿
            workbook = writer.book
            
            # 定义格式
            header_format = workbook.add_format({
                'bold': True,
                'text_wrap': True,
                'valign': 'top',
                'fg_color': '#667eea',
                'font_color': 'white',
                'border': 1
            })
            
            # 应用格式到每个工作表
            for sheet_name in writer.sheets:
                worksheet = writer.sheets[sheet_name]
                
                # 设置列宽
                worksheet.set_column('A:A', 30)
                worksheet.set_column('B:Z', 15)
                
        except Exception as e:
            self.logger.warning(f"Excel格式化失败: {e}")
    
    def export_json(self, report: BacktestReport, filepath: str):
        """导出JSON报告"""
        path = Path(filepath)
        path.parent.mkdir(parents=True, exist_ok=True)
        
        try:
            # 转换为可序列化的格式
            json_data = {
                'metadata': {
                    'generated_at': datetime.now().isoformat(),
                    'version': '1.0.0',
                    'framework': 'QuantTrading'
                },
                'summary': report.summary,
                'metrics': report.metrics,
                'equity_curve': report.equity_curve.to_dict('records') if not report.equity_curve.empty else [],
                'transactions': report.transactions.to_dict('records') if not report.transactions.empty else [],
                'monthly_returns': report.monthly_returns.to_dict() if not report.monthly_returns.empty else {}
            }
            
            # 保存JSON
            with open(path, 'w', encoding='utf-8') as f:
                json.dump(json_data, f, indent=4, default=str, ensure_ascii=False)
            
            self.logger.info(f"JSON报告已保存至: {filepath}")
            
        except Exception as e:
            self.logger.error(f"JSON报告保存失败: {e}")
    
    def export_pdf(self, report: BacktestReport, filepath: str):
        """导出PDF报告（需要额外的依赖）"""
        try:
            from matplotlib.backends.backend_pdf import PdfPages
            
            path = Path(filepath)
            path.parent.mkdir(parents=True, exist_ok=True)
            
            # 创建PDF
            with PdfPages(path) as pdf:
                # 创建所有图表
                charts = self.create_charts()
                
                # 保存每个图表到PDF
                for title, fig in charts.items():
                    pdf.savefig(fig)
                    plt.close(fig)
                
                # 添加元数据
                d = pdf.infodict()
                d['Title'] = 'Backtest Report'
                d['Author'] = 'QuantTrading Framework'
                d['Subject'] = 'Strategy Backtest Results'
                d['Keywords'] = 'Quantitative Trading, Backtest, Performance'
                d['CreationDate'] = datetime.now()
            
            self.logger.info(f"PDF报告已保存至: {filepath}")
            
        except ImportError:
            self.logger.warning("PDF导出需要安装matplotlib，跳过PDF生成")
        except Exception as e:
            self.logger.error(f"PDF报告保存失败: {e}")
    
    # ==========================================
    # 📊 快速报告生成
    # ==========================================
    
    def generate_quick_report(self, output_dir: str = './reports'):
        """快速生成所有格式的报告"""
        # 生成报告
        report = self.generate()
        
        # 创建时间戳
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # 创建输出目录
        output_path = Path(output_dir) / timestamp
        output_path.mkdir(parents=True, exist_ok=True)
        
        # 导出各种格式
        formats = {
            'html': f"{output_path}/report.html",
            'excel': f"{output_path}/report.xlsx",
            'json': f"{output_path}/report.json"
        }
        
        results = {}
        
        for format_name, filepath in formats.items():
            try:
                if format_name == 'html':
                    self.export_html(report, filepath)
                elif format_name == 'excel':
                    self.export_excel(report, filepath)
                elif format_name == 'json':
                    self.export_json(report, filepath)
                
                results[format_name] = filepath
                print(f"✅ {format_name.upper()}报告: {filepath}")
                
            except Exception as e:
                self.logger.error(f"{format_name}报告生成失败: {e}")
                results[format_name] = None
        
        return results


# ==========================================
# 🔧 辅助函数
# ==========================================

def generate_quick_report(results: Dict, output_dir: str = './reports'):
    """
    快速生成报告的便捷函数
    
    Args:
        results: 回测结果
        output_dir: 输出目录
    
    Returns:
        生成的报告路径字典
    """
    generator = ReportGenerator(results)
    return generator.generate_quick_report(output_dir)


# ==========================================
# 📊 测试代码
# ==========================================

if __name__ == "__main__":
    print("报告生成器模块测试")
    print("=" * 60)
    
    # 创建测试数据
    dates = pd.date_range('2023-01-01', '2023-12-31', freq='D')
    
    # 模拟回测结果
    test_results = {
        'equity_curve': pd.DataFrame({
            'total_value': (1 + np.random.randn(len(dates)) * 0.02).cumprod() * 1000000
        }, index=dates),
        'returns': pd.Series(np.random.randn(len(dates)) * 0.02, index=dates),
        'transactions': pd.DataFrame({
            'timestamp': dates[::20],
            'symbol': '000001.SZ',
            'side': ['BUY', 'SELL'] * (len(dates[::20]) // 2),
            'price': np.random.uniform(50, 60, len(dates[::20])),
            'quantity': [1000] * len(dates[::20]),
            'commission': np.random.uniform(10, 50, len(dates[::20]))
        })
    }
    
    # 创建报告生成器
    generator = ReportGenerator(test_results)
    print("✅ 报告生成器创建成功")
    
    # 生成报告
    report = generator.generate()
    print("\n报告摘要:")
    for key, value in report.summary.items():
        print(f"  {key}: {value}")
    
    # 测试导出功能
    output_dir = Path('./test_reports')
    output_dir.mkdir(exist_ok=True)
    
    # 导出HTML
    try:
        generator.export_html(report, str(output_dir / 'test_report.html'))
        print("\n✅ HTML报告生成成功")
    except Exception as e:
        print(f"\n❌ HTML报告生成失败: {e}")
    
    # 导出Excel
    try:
        generator.export_excel(report, str(output_dir / 'test_report.xlsx'))
        print("✅ Excel报告生成成功")
    except Exception as e:
        print(f"❌ Excel报告生成失败: {e}")
    
    # 导出JSON
    try:
        generator.export_json(report, str(output_dir / 'test_report.json'))
        print("✅ JSON报告生成成功")
    except Exception as e:
        print(f"❌ JSON报告生成失败: {e}")
    
    print("\n✅ 报告生成器模块测试完成！")