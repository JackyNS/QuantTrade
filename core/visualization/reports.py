#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
报告生成器 - reports.py
=======================

专业的量化交易报告生成器，支持：
- 回测报告
- 策略分析报告
- 风险评估报告
- 月度/季度报告
- Excel导出

版本: 1.0.0
更新: 2025-08-29
"""

import warnings
from typing import Dict, List, Optional, Any, Union
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from pathlib import Path
import json

# 报告生成相关库
try:
    from jinja2 import Template
    JINJA2_AVAILABLE = True
except ImportError:
    JINJA2_AVAILABLE = False
    print("⚠️ Jinja2未安装，HTML报告功能受限")

warnings.filterwarnings('ignore')

class Reports:
    """
    专业报告生成器
    
    支持多种格式的量化交易报告生成
    """
    
    def __init__(self, config: Optional[Dict] = None):
        """
        初始化报告生成器
        
        Args:
            config: 配置字典
        """
        self.config = config or {}
        
        # 默认配置
        self.default_config = {
            'company_name': '量化交易系统',
            'author': 'QuantTrader',
            'output_dir': './results/reports',
            'template_dir': './templates',
            'formats': ['html', 'excel', 'json'],
            'include_charts': True
        }
        
        # 合并配置
        self.config = {**self.default_config, **self.config}
        
        # 创建输出目录
        self.output_dir = Path(self.config['output_dir'])
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        print("📋 报告生成器初始化完成")
    
    def generate_backtest_report(self, 
                                results: Dict[str, Any] = None,
                                backtest_results: Dict[str, Any] = None,
                                output_dir: str = "./results/reports",
                                **kwargs):
        """
        生成回测报告
        
        Args:
            results: 回测结果字典
            backtest_results: 回测结果（兼容参数）
            output_dir: 输出目录
            **kwargs: 其他参数
        
        Returns:
            str: 报告文件路径
        """
        # 参数兼容性处理
        if results is None and backtest_results is not None:
            results = backtest_results
        elif results is None:
            results = {}

        # 导入必要的库
        from pathlib import Path
        import json
        from datetime import datetime
        
        # 创建输出目录
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        # 提取数据
        metrics = results.get('metrics', {})
        trades = results.get('trades', None)
        
        # 生成HTML报告
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>回测报告</title>
            <meta charset="utf-8">
            <style>
                body {{ 
                    font-family: 'Segoe UI', Arial, sans-serif; 
                    margin: 0; 
                    padding: 20px; 
                    background: #f5f5f5; 
                }}
                .container {{ 
                    max-width: 1200px; 
                    margin: 0 auto; 
                }}
                .header {{ 
                    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); 
                    color: white; 
                    padding: 30px; 
                    border-radius: 10px; 
                    margin-bottom: 30px; 
                }}
                .metrics-grid {{ 
                    display: grid; 
                    grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); 
                    gap: 20px; 
                    margin-bottom: 30px; 
                }}
                .metric-card {{ 
                    background: white; 
                    padding: 20px; 
                    border-radius: 8px; 
                    box-shadow: 0 2px 4px rgba(0,0,0,0.1); 
                }}
                .metric-value {{ 
                    font-size: 24px; 
                    font-weight: bold; 
                    color: #333; 
                }}
                .metric-label {{ 
                    color: #666; 
                    margin-top: 5px; 
                }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>📊 策略回测报告</h1>
                    <p>生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                </div>
                
                <h2>📈 绩效指标</h2>
                <div class="metrics-grid">
                    <div class="metric-card">
                        <div class="metric-value">{metrics.get('total_return', 0):.2%}</div>
                        <div class="metric-label">总收益率</div>
                    </div>
                    <div class="metric-card">
                        <div class="metric-value">{metrics.get('annual_return', 0):.2%}</div>
                        <div class="metric-label">年化收益率</div>
                    </div>
                    <div class="metric-card">
                        <div class="metric-value">{metrics.get('sharpe_ratio', 0):.2f}</div>
                        <div class="metric-label">夏普比率</div>
                    </div>
                    <div class="metric-card">
                        <div class="metric-value">{metrics.get('max_drawdown', 0):.2%}</div>
                        <div class="metric-label">最大回撤</div>
                    </div>
                </div>
            </div>
        </body>
        </html>
        """
        
        # 保存HTML文件
        html_file = output_path / f"backtest_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
        with open(html_file, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        print(f"📊 回测报告已生成: {html_file}")
        return str(html_file)
    def generate_strategy_analysis(self,
                                  strategy_data: Dict[str, Any],
                                  format: str = "html") -> str:
        """
        生成策略分析报告
        
        Args:
            strategy_data: 策略数据
            format: 输出格式
            
        Returns:
            报告文件路径
        """
        print("📊 生成策略分析报告...")
        
        # 准备分析数据
        analysis_data = self._prepare_strategy_analysis(strategy_data)
        
        # 生成报告
        if format == "html":
            return self._generate_html_report(analysis_data, "strategy_analysis")
        elif format == "excel":
            return self._generate_excel_report(analysis_data, "strategy_analysis")
        else:
            return self._generate_json_report(analysis_data, "strategy_analysis")
    
    def generate_risk_report(self,
                           risk_metrics: Dict[str, float],
                           format: str = "html") -> str:
        """
        生成风险评估报告
        
        Args:
            risk_metrics: 风险指标字典
            format: 输出格式
            
        Returns:
            报告文件路径
        """
        print("📊 生成风险评估报告...")
        
        # 准备风险数据
        risk_data = self._prepare_risk_data(risk_metrics)
        
        # 生成报告
        if format == "html":
            return self._generate_html_report(risk_data, "risk_assessment")
        else:
            return self._generate_json_report(risk_data, "risk_assessment")
    
    def generate_monthly_report(self,
                              monthly_data: pd.DataFrame,
                              month: str,
                              format: str = "html") -> str:
        """
        生成月度报告
        
        Args:
            monthly_data: 月度数据
            month: 月份 (YYYY-MM)
            format: 输出格式
            
        Returns:
            报告文件路径
        """
        print(f"📊 生成{month}月度报告...")
        
        # 准备月度数据
        report_data = self._prepare_monthly_data(monthly_data, month)
        
        # 生成报告
        if format == "html":
            return self._generate_html_report(report_data, f"monthly_{month}")
        elif format == "excel":
            return self._generate_excel_report(report_data, f"monthly_{month}")
        else:
            return self._generate_json_report(report_data, f"monthly_{month}")
    
    def _prepare_backtest_data(self, 
                              results: Dict[str, Any],
                              strategy_name: str) -> Dict[str, Any]:
        """准备回测报告数据"""
        # 模拟数据（实际应从回测结果提取）
        return {
            'title': f'{strategy_name}回测报告',
            'generated_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'strategy_name': strategy_name,
            'summary': {
                '回测期间': results.get('period', '2024-01-01 至 2024-12-31'),
                '初始资金': results.get('initial_capital', 1000000),
                '最终资金': results.get('final_capital', 1150000),
                '总收益率': results.get('total_return', 0.15),
                '年化收益率': results.get('annual_return', 0.15),
                '夏普比率': results.get('sharpe_ratio', 1.85),
                '最大回撤': results.get('max_drawdown', -0.085),
                '胜率': results.get('win_rate', 0.58)
            },
            'trades': results.get('trades', []),
            'daily_returns': results.get('daily_returns', []),
            'metrics': results.get('metrics', {})
        }
    
    def _prepare_strategy_analysis(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """准备策略分析数据"""
        return {
            'title': '策略分析报告',
            'generated_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'strategy_type': data.get('type', '技术分析'),
            'parameters': data.get('parameters', {}),
            'performance': data.get('performance', {}),
            'signals': data.get('signals', []),
            'optimization': data.get('optimization', {})
        }
    
    def _prepare_risk_data(self, metrics: Dict[str, float]) -> Dict[str, Any]:
        """准备风险评估数据"""
        return {
            'title': '风险评估报告',
            'generated_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'risk_metrics': {
                'VaR_95': metrics.get('var_95', -0.032),
                'VaR_99': metrics.get('var_99', -0.045),
                'CVaR': metrics.get('cvar', -0.055),
                '最大回撤': metrics.get('max_drawdown', -0.085),
                '回撤持续时间': metrics.get('drawdown_duration', 45),
                'Beta': metrics.get('beta', 0.92),
                '下行风险': metrics.get('downside_risk', 0.015),
                '信息比率': metrics.get('information_ratio', 0.75)
            },
            'risk_level': self._assess_risk_level(metrics)
        }
    
    def _prepare_monthly_data(self, data: pd.DataFrame, month: str) -> Dict[str, Any]:
        """准备月度报告数据"""
        return {
            'title': f'{month}月度报告',
            'generated_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'month': month,
            'summary': {
                '交易次数': len(data),
                '盈利交易': len(data[data.get('pnl', 0) > 0]),
                '亏损交易': len(data[data.get('pnl', 0) < 0]),
                '总盈亏': data.get('pnl', pd.Series()).sum(),
                '平均收益': data.get('returns', pd.Series()).mean(),
                '收益标准差': data.get('returns', pd.Series()).std()
            },
            'daily_data': data.to_dict('records') if not data.empty else []
        }
    
    def _assess_risk_level(self, metrics: Dict[str, float]) -> str:
        """评估风险等级"""
        max_dd = abs(metrics.get('max_drawdown', 0))
        if max_dd < 0.05:
            return "低风险"
        elif max_dd < 0.10:
            return "中等风险"
        elif max_dd < 0.20:
            return "高风险"
        else:
            return "极高风险"
    
    def _generate_html_report(self, data: Dict[str, Any], report_type: str) -> str:
        """生成HTML报告"""
        # HTML模板
        html_template = """
        <!DOCTYPE html>
        <html lang="zh-CN">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>{{ title }}</title>
            <style>
                * { margin: 0; padding: 0; box-sizing: border-box; }
                body {
                    font-family: 'Microsoft YaHei', Arial, sans-serif;
                    line-height: 1.6;
                    color: #333;
                    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    min-height: 100vh;
                    padding: 20px;
                }
                .container {
                    max-width: 1200px;
                    margin: 0 auto;
                    background: white;
                    border-radius: 20px;
                    box-shadow: 0 20px 60px rgba(0,0,0,0.3);
                    overflow: hidden;
                }
                .header {
                    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    color: white;
                    padding: 40px;
                    text-align: center;
                }
                .header h1 {
                    font-size: 2.5em;
                    margin-bottom: 10px;
                    text-shadow: 2px 2px 4px rgba(0,0,0,0.2);
                }
                .header .subtitle {
                    font-size: 1.1em;
                    opacity: 0.9;
                }
                .content {
                    padding: 40px;
                }
                .section {
                    margin-bottom: 40px;
                    animation: fadeIn 0.6s ease-in;
                }
                .section h2 {
                    color: #667eea;
                    border-bottom: 3px solid #667eea;
                    padding-bottom: 10px;
                    margin-bottom: 20px;
                    font-size: 1.8em;
                }
                .metrics-grid {
                    display: grid;
                    grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
                    gap: 20px;
                    margin-top: 20px;
                }
                .metric-card {
                    background: linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%);
                    padding: 20px;
                    border-radius: 15px;
                    text-align: center;
                    transition: transform 0.3s ease, box-shadow 0.3s ease;
                }
                .metric-card:hover {
                    transform: translateY(-5px);
                    box-shadow: 0 10px 30px rgba(0,0,0,0.2);
                }
                .metric-label {
                    color: #666;
                    font-size: 0.9em;
                    margin-bottom: 8px;
                    text-transform: uppercase;
                    letter-spacing: 1px;
                }
                .metric-value {
                    font-size: 2em;
                    font-weight: bold;
                    color: #333;
                }
                .metric-value.positive { color: #4caf50; }
                .metric-value.negative { color: #f44336; }
                .table-container {
                    overflow-x: auto;
                    margin-top: 20px;
                }
                table {
                    width: 100%;
                    border-collapse: collapse;
                    background: white;
                    box-shadow: 0 2px 10px rgba(0,0,0,0.1);
                }
                th {
                    background: #667eea;
                    color: white;
                    padding: 12px;
                    text-align: left;
                    font-weight: 600;
                }
                td {
                    padding: 12px;
                    border-bottom: 1px solid #e0e0e0;
                }
                tr:hover {
                    background: #f5f5f5;
                }
                .footer {
                    background: #f8f9fa;
                    padding: 20px;
                    text-align: center;
                    color: #666;
                    border-top: 1px solid #e0e0e0;
                }
                .chart-placeholder {
                    background: linear-gradient(135deg, #e0e0e0 0%, #f5f5f5 100%);
                    height: 300px;
                    border-radius: 10px;
                    display: flex;
                    align-items: center;
                    justify-content: center;
                    color: #999;
                    margin: 20px 0;
                }
                @keyframes fadeIn {
                    from { opacity: 0; transform: translateY(20px); }
                    to { opacity: 1; transform: translateY(0); }
                }
                .badge {
                    display: inline-block;
                    padding: 4px 12px;
                    border-radius: 20px;
                    font-size: 0.85em;
                    font-weight: bold;
                    margin-left: 10px;
                }
                .badge.success { background: #4caf50; color: white; }
                .badge.warning { background: #ff9800; color: white; }
                .badge.danger { background: #f44336; color: white; }
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>{{ title }}</h1>
                    <div class="subtitle">
                        {{ company_name }} | 生成时间: {{ generated_time }}
                    </div>
                </div>
                
                <div class="content">
                    {% if summary %}
                    <div class="section">
                        <h2>📊 核心指标</h2>
                        <div class="metrics-grid">
                            {% for key, value in summary.items() %}
                            <div class="metric-card">
                                <div class="metric-label">{{ key }}</div>
                                <div class="metric-value {% if value > 0 %}positive{% elif value < 0 %}negative{% endif %}">
                                    {% if value is number %}
                                        {% if key.endswith('率') or key.endswith('比') %}
                                            {{ "%.2f%%"|format(value * 100) }}
                                        {% else %}
                                            {{ "{:,.2f}"|format(value) }}
                                        {% endif %}
                                    {% else %}
                                        {{ value }}
                                    {% endif %}
                                </div>
                            </div>
                            {% endfor %}
                        </div>
                    </div>
                    {% endif %}
                    
                    {% if risk_metrics %}
                    <div class="section">
                        <h2>⚠️ 风险指标</h2>
                        <div class="metrics-grid">
                            {% for key, value in risk_metrics.items() %}
                            <div class="metric-card">
                                <div class="metric-label">{{ key }}</div>
                                <div class="metric-value {% if value < 0 %}negative{% endif %}">
                                    {{ "{:.2%}"|format(value) if value < 1 else value }}
                                </div>
                            </div>
                            {% endfor %}
                        </div>
                        {% if risk_level %}
                        <p style="margin-top: 20px; text-align: center;">
                            风险等级: 
                            <span class="badge {% if '低' in risk_level %}success{% elif '高' in risk_level %}danger{% else %}warning{% endif %}">
                                {{ risk_level }}
                            </span>
                        </p>
                        {% endif %}
                    </div>
                    {% endif %}
                    
                    <div class="section">
                        <h2>📈 图表分析</h2>
                        <div class="chart-placeholder">
                            图表区域 - 可集成Plotly或其他图表
                        </div>
                    </div>
                    
                    {% if trades %}
                    <div class="section">
                        <h2>📝 交易记录</h2>
                        <div class="table-container">
                            <table>
                                <thead>
                                    <tr>
                                        <th>时间</th>
                                        <th>代码</th>
                                        <th>方向</th>
                                        <th>价格</th>
                                        <th>数量</th>
                                        <th>盈亏</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    {% for trade in trades[:10] %}
                                    <tr>
                                        <td>{{ trade.time }}</td>
                                        <td>{{ trade.symbol }}</td>
                                        <td>{{ trade.side }}</td>
                                        <td>{{ trade.price }}</td>
                                        <td>{{ trade.quantity }}</td>
                                        <td class="{% if trade.pnl > 0 %}positive{% else %}negative{% endif %}">
                                            {{ trade.pnl }}
                                        </td>
                                    </tr>
                                    {% endfor %}
                                </tbody>
                            </table>
                        </div>
                    </div>
                    {% endif %}
                </div>
                
                <div class="footer">
                    <p>© 2025 {{ company_name }} - 量化交易报告系统</p>
                    <p>报告生成者: {{ author }}</p>
                </div>
            </div>
        </body>
        </html>
        """
        
        # 渲染模板
        if JINJA2_AVAILABLE:
            template = Template(html_template)
            html_content = template.render(
                **data,
                company_name=self.config['company_name'],
                author=self.config['author']
            )
        else:
            # 简单替换
            html_content = html_template.replace('{{ title }}', data.get('title', '报告'))
            html_content = html_content.replace('{{ generated_time }}', data.get('generated_time', ''))
            html_content = html_content.replace('{{ company_name }}', self.config['company_name'])
            html_content = html_content.replace('{{ author }}', self.config['author'])
        
        # 保存文件
        filename = f"{report_type}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
        filepath = self.output_dir / filename
        
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        print(f"✅ HTML报告已生成: {filepath}")
        return str(filepath)
    
    def _generate_excel_report(self, data: Dict[str, Any], report_type: str) -> str:
        """生成Excel报告"""
        filename = f"{report_type}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        filepath = self.output_dir / filename
        
        try:
            with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
                # 摘要sheet
                if 'summary' in data:
                    summary_df = pd.DataFrame([data['summary']])
                    summary_df.to_excel(writer, sheet_name='摘要', index=False)
                
                # 指标sheet
                if 'metrics' in data:
                    metrics_df = pd.DataFrame([data['metrics']])
                    metrics_df.to_excel(writer, sheet_name='指标', index=False)
                
                # 交易记录sheet
                if 'trades' in data and data['trades']:
                    trades_df = pd.DataFrame(data['trades'])
                    trades_df.to_excel(writer, sheet_name='交易记录', index=False)
                
                # 日收益sheet
                if 'daily_returns' in data and data['daily_returns']:
                    returns_df = pd.DataFrame(data['daily_returns'])
                    returns_df.to_excel(writer, sheet_name='日收益', index=False)
            
            print(f"✅ Excel报告已生成: {filepath}")
            return str(filepath)
            
        except Exception as e:
            print(f"❌ Excel报告生成失败: {e}")
            return ""
    
    def _generate_json_report(self, data: Dict[str, Any], report_type: str) -> str:
        """生成JSON报告"""
        filename = f"{report_type}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        filepath = self.output_dir / filename
        
        # 转换不可序列化的对象
        def convert_to_serializable(obj):
            if isinstance(obj, (pd.DataFrame, pd.Series)):
                return obj.to_dict()
            elif isinstance(obj, np.ndarray):
                return obj.tolist()
            elif isinstance(obj, (np.integer, np.floating)):
                return float(obj)
            elif isinstance(obj, datetime):
                return obj.isoformat()
            return obj
        
        # 递归转换数据
        serializable_data = json.loads(
            json.dumps(data, default=convert_to_serializable)
        )
        
        # 保存JSON文件
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(serializable_data, f, ensure_ascii=False, indent=2)
        
        print(f"✅ JSON报告已生成: {filepath}")
        return str(filepath)
    
    def generate_comprehensive_report(self,
                                    all_data: Dict[str, Any],
                                    title: str = "综合分析报告") -> List[str]:
        """
        生成综合报告（多种格式）
        
        Args:
            all_data: 所有数据
            title: 报告标题
            
        Returns:
            生成的报告文件路径列表
        """
        print(f"📊 生成综合报告: {title}")
        
        report_paths = []
        
        # 准备综合数据
        comprehensive_data = {
            'title': title,
            'generated_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            **all_data
        }
        
        # 生成各种格式的报告
        for format in self.config['formats']:
            if format == 'html':
                path = self._generate_html_report(comprehensive_data, 'comprehensive')
            elif format == 'excel':
                path = self._generate_excel_report(comprehensive_data, 'comprehensive')
            elif format == 'json':
                path = self._generate_json_report(comprehensive_data, 'comprehensive')
            else:
                continue
            
            if path:
                report_paths.append(path)
        
        print(f"✅ 综合报告生成完成，共{len(report_paths)}个文件")
        return report_paths

# ==========================================
# 测试代码
# ==========================================
    def generate_risk_report(self, 
                            risk_metrics: Dict[str, float],
                            output_dir: str = "./results/reports",
                            **kwargs):
        """
        生成风险评估报告
        
        Args:
            risk_metrics: 风险指标字典
            output_dir: 输出目录
            **kwargs: 其他参数
        
        Returns:
            str: 报告文件路径
        """
        from pathlib import Path
        from datetime import datetime
        
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>风险评估报告</title>
            <meta charset="utf-8">
            <style>
                body {{ 
                    font-family: Arial, sans-serif; 
                    margin: 20px;
                    background: #f5f5f5;
                }}
                .header {{ 
                    background: #dc3545; 
                    color: white; 
                    padding: 20px;
                    border-radius: 10px;
                    margin-bottom: 20px;
                }}
                .risk-metric {{ 
                    margin: 15px 0; 
                    padding: 10px; 
                    background: white;
                    border-radius: 5px;
                    box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                }}
            </style>
        </head>
        <body>
            <div class="header">
                <h1>⚠️ 风险评估报告</h1>
                <p>生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            </div>
            
            <h2>风险指标</h2>
        """
        
        for metric_name, metric_value in risk_metrics.items():
            html_content += f"""
            <div class="risk-metric">
                <strong>{metric_name}:</strong> {metric_value:.4f}
            </div>
            """
        
        html_content += """
        </body>
        </html>
        """
        
        html_file = output_path / f"risk_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
        with open(html_file, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        print(f"📊 风险报告已生成: {html_file}")
        return str(html_file)


if __name__ == "__main__":
    print("\n" + "=" * 50)
    print("测试报告生成器")
    print("=" * 50)
    
    # 创建报告生成器
    reports = Reports()
    
    # 测试回测报告
    print("\n📊 测试回测报告...")
    backtest_results = {
        'period': '2024-01-01 至 2024-12-31',
        'initial_capital': 1000000,
        'final_capital': 1250000,
        'total_return': 0.25,
        'annual_return': 0.25,
        'sharpe_ratio': 1.85,
        'max_drawdown': -0.085,
        'win_rate': 0.62,
        'trades': [
            {'time': '2024-01-15', 'symbol': '000001', 'side': '买入', 
             'price': 10.5, 'quantity': 1000, 'pnl': 0},
            {'time': '2024-01-20', 'symbol': '000001', 'side': '卖出', 
             'price': 11.2, 'quantity': 1000, 'pnl': 700}
        ]
    }
    
    report_path = reports.generate_backtest_report(
        backtest_results,
        strategy_name="双均线策略",
        format="html"
    )
    
    # 测试风险报告
    print("\n📊 测试风险报告...")
    risk_metrics = {
        'var_95': -0.032,
        'var_99': -0.045,
        'max_drawdown': -0.085,
        'beta': 0.92,
        'downside_risk': 0.015
    }
    
    risk_report = reports.generate_risk_report(risk_metrics, format="html")
    
    # 测试综合报告
    print("\n📊 测试综合报告...")
    all_data = {
        'summary': backtest_results,
        'risk_metrics': risk_metrics,
        'strategy_info': {
            'name': '双均线策略',
            'type': '技术分析',
            'parameters': {'fast_period': 5, 'slow_period': 20}
        }
    }
    
    comprehensive_reports = reports.generate_comprehensive_report(
        all_data,
        title="2024年度量化策略综合报告"
    )
    
    print("\n✅ 报告生成器测试完成!")
    print(f"📁 报告保存路径: {reports.output_dir}")