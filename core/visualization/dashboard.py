#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
交互式面板 - dashboard.py
========================

专业的量化交易监控面板，提供实时数据展示和策略监控

版本: 1.0.0
更新: 2025-08-30
"""

import warnings
from typing import Dict, List, Optional, Any, Union
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import numpy as np

# 处理Dash依赖
try:
    import dash
    from dash import dcc, html, Input, Output, State
    import dash_bootstrap_components as dbc
    DASH_AVAILABLE = True
except ImportError:
    DASH_AVAILABLE = False
    dbc = None
    print("⚠️ Dash未安装，Web界面功能将不可用，但离线报告功能正常")

try:
    import plotly.graph_objects as go
    import plotly.express as px
    PLOTLY_AVAILABLE = True
except ImportError:
    PLOTLY_AVAILABLE = False
    print("⚠️ Plotly未安装，部分图表功能将不可用")

warnings.filterwarnings('ignore')

class Dashboard:
    """
    交互式监控面板
    
    提供实时数据展示、策略监控和报告生成功能
    """
    
    def __init__(self, config: Optional[Dict] = None):
        """
        初始化交互式面板
        
        Args:
            config: 配置字典
        """
        self.config = config or {}
        
        # 默认配置
        self.default_config = {
            'title': '量化交易监控面板',
            'theme': dbc.themes.BOOTSTRAP if (dbc and DASH_AVAILABLE) else None,
            'port': 8050,
            'host': '127.0.0.1',
            'debug': True,
            'update_interval': 5000,  # 5秒更新一次
            'colors': {
                'background': '#f8f9fa',
                'text': '#212529',
                'primary': '#007bff',
                'success': '#28a745',
                'danger': '#dc3545',
                'warning': '#ffc107'
            }
        }
        
        # 合并配置
        self.config = {**self.default_config, **self.config}
        
        # 数据存储
        self.data = {}
        self.strategies = {}
        self.positions = {}
        self.performance = {}
        self.alerts = []
        
        # Dash应用实例
        self.app = None
        
        # 如果Dash可用，初始化应用
        if DASH_AVAILABLE:
            self._init_dash_app()
        
        print("🖥️ Dashboard初始化完成")
    
    def _init_dash_app(self):
        """初始化Dash应用"""
        if not DASH_AVAILABLE:
            return
        
        # 创建Dash应用
        external_stylesheets = []
        if self.config['theme']:
            external_stylesheets.append(self.config['theme'])
        
        self.app = dash.Dash(
            __name__,
            external_stylesheets=external_stylesheets
        )
        
        # 设置布局
        self._setup_layout()
        
        # 设置回调
        self._setup_callbacks()
    
    def _setup_layout(self):
        """设置Dashboard布局"""
        if not self.app:
            return
        
        self.app.layout = html.Div([
            # 头部
            html.Div([
                html.H1(self.config['title'], className="text-center"),
                html.Hr()
            ], className="mb-4"),
            
            # 主要内容区
            html.Div([
                # 左侧面板
                html.Div([
                    html.H3("📊 实时数据"),
                    html.Div(id="live-data-display"),
                    html.Hr(),
                    html.H3("📈 策略状态"),
                    html.Div(id="strategy-status"),
                ], className="col-md-4"),
                
                # 中间图表区
                html.Div([
                    html.H3("📉 价格走势"),
                    dcc.Graph(id="price-chart"),
                    html.H3("📊 技术指标"),
                    dcc.Graph(id="indicator-chart"),
                ], className="col-md-8"),
            ], className="row"),
            
            # 自动更新组件
            dcc.Interval(
                id='interval-component',
                interval=self.config['update_interval'],
                n_intervals=0
            )
        ], className="container-fluid p-4")
    
    def _setup_callbacks(self):
        """设置回调函数"""
        if not self.app:
            return
        
        @self.app.callback(
            Output('live-data-display', 'children'),
            Input('interval-component', 'n_intervals')
        )
        def update_live_data(n):
            """更新实时数据显示"""
            if not self.data:
                return "等待数据..."
            
            return html.Div([
                html.P(f"最新价格: {self.data.get('current_price', 'N/A')}"),
                html.P(f"日涨跌: {self.data.get('daily_change', 'N/A')}"),
                html.P(f"成交量: {self.data.get('volume', 'N/A')}"),
            ])
    
    def update_data(self, data: Dict[str, Any]):
        """
        更新实时数据
        
        Args:
            data: 数据字典
        """
        self.data.update(data)
        self.data['last_update'] = datetime.now()
        return True
    
    def add_strategy(self, strategy_name: str, params: Dict[str, Any]):
        """
        添加策略
        
        Args:
            strategy_name: 策略名称
            params: 策略参数
        """
        self.strategies[strategy_name] = {
            'params': params,
            'status': 'active',
            'added_time': datetime.now()
        }
        return True
    
    def update_positions(self, positions: Dict[str, Any]):
        """
        更新持仓信息
        
        Args:
            positions: 持仓字典
        """
        self.positions.update(positions)
        self.positions['last_update'] = datetime.now()
        return True
    
    def update_performance(self, metrics: Dict[str, float]):
        """
        更新绩效指标
        
        Args:
            metrics: 绩效指标字典
        """
        self.performance.update(metrics)
        self.performance['last_update'] = datetime.now()
    
    def add_alert(self, alert_type: str, message: str, level: str = "info"):
        """
        添加警报
        
        Args:
            alert_type: 警报类型
            message: 警报消息
            level: 警报级别 (info, warning, danger)
        """
        self.alerts.append({
            'type': alert_type,
            'message': message,
            'level': level,
            'timestamp': datetime.now()
        })
        
        # 保留最近100条警报
        if len(self.alerts) > 100:
            self.alerts = self.alerts[-100:]
    
    def create_offline_report(self, output_dir: str = "./results/dashboard"):
        """
        创建离线HTML报告
        
        Args:
            output_dir: 输出目录
            
        Returns:
            str: 报告文件路径
        """
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        # 准备数据
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # 生成HTML内容
        html_content = f"""
        <!DOCTYPE html>
        <html lang="zh-CN">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>量化交易监控报告 - {current_time}</title>
            <style>
                * {{
                    margin: 0;
                    padding: 0;
                    box-sizing: border-box;
                }}
                body {{
                    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Microsoft YaHei', sans-serif;
                    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    min-height: 100vh;
                    padding: 20px;
                }}
                .container {{
                    max-width: 1400px;
                    margin: 0 auto;
                }}
                .header {{
                    background: white;
                    border-radius: 20px;
                    padding: 40px;
                    margin-bottom: 30px;
                    box-shadow: 0 20px 40px rgba(0,0,0,0.1);
                    text-align: center;
                }}
                .header h1 {{
                    color: #333;
                    font-size: 36px;
                    margin-bottom: 10px;
                }}
                .header p {{
                    color: #666;
                    font-size: 14px;
                }}
                .grid {{
                    display: grid;
                    grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
                    gap: 20px;
                    margin-bottom: 30px;
                }}
                .card {{
                    background: white;
                    border-radius: 15px;
                    padding: 25px;
                    box-shadow: 0 10px 25px rgba(0,0,0,0.08);
                    transition: transform 0.3s;
                }}
                .card:hover {{
                    transform: translateY(-5px);
                }}
                .card h3 {{
                    color: #333;
                    margin-bottom: 15px;
                    font-size: 18px;
                    display: flex;
                    align-items: center;
                }}
                .card h3::before {{
                    content: '📊';
                    margin-right: 10px;
                }}
                .metric {{
                    display: flex;
                    justify-content: space-between;
                    padding: 10px 0;
                    border-bottom: 1px solid #f0f0f0;
                }}
                .metric:last-child {{
                    border-bottom: none;
                }}
                .metric-label {{
                    color: #666;
                    font-size: 14px;
                }}
                .metric-value {{
                    font-weight: bold;
                    color: #333;
                    font-size: 14px;
                }}
                .status-active {{
                    color: #28a745;
                }}
                .status-inactive {{
                    color: #dc3545;
                }}
                .footer {{
                    text-align: center;
                    color: white;
                    margin-top: 40px;
                    padding: 20px;
                }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>📊 量化交易监控报告</h1>
                    <p>生成时间: {current_time}</p>
                </div>
                
                <div class="grid">
                    <div class="card">
                        <h3>实时数据</h3>
                        <div class="metric">
                            <span class="metric-label">最新价格</span>
                            <span class="metric-value">{self.data.get('current_price', 'N/A')}</span>
                        </div>
                        <div class="metric">
                            <span class="metric-label">日涨跌</span>
                            <span class="metric-value">{self.data.get('daily_change', 'N/A')}</span>
                        </div>
                        <div class="metric">
                            <span class="metric-label">成交量</span>
                            <span class="metric-value">{self.data.get('volume', 'N/A')}</span>
                        </div>
                        <div class="metric">
                            <span class="metric-label">RSI</span>
                            <span class="metric-value">{self.data.get('rsi', 'N/A')}</span>
                        </div>
                    </div>
                    
                    <div class="card">
                        <h3>策略状态</h3>
                        <div class="metric">
                            <span class="metric-label">运行策略数</span>
                            <span class="metric-value">{len(self.strategies)}</span>
                        </div>
                        <div class="metric">
                            <span class="metric-label">活跃策略</span>
                            <span class="metric-value status-active">{sum(1 for s in self.strategies.values() if s.get('status') == 'active')}</span>
                        </div>
                        <div class="metric">
                            <span class="metric-label">总持仓数</span>
                            <span class="metric-value">{len(self.positions)}</span>
                        </div>
                        <div class="metric">
                            <span class="metric-label">警报数</span>
                            <span class="metric-value">{len(self.alerts)}</span>
                        </div>
                    </div>
                    
                    <div class="card">
                        <h3>绩效指标</h3>
                        <div class="metric">
                            <span class="metric-label">总收益率</span>
                            <span class="metric-value">{self.performance.get('total_return', 'N/A')}</span>
                        </div>
                        <div class="metric">
                            <span class="metric-label">夏普比率</span>
                            <span class="metric-value">{self.performance.get('sharpe_ratio', 'N/A')}</span>
                        </div>
                        <div class="metric">
                            <span class="metric-label">最大回撤</span>
                            <span class="metric-value">{self.performance.get('max_drawdown', 'N/A')}</span>
                        </div>
                        <div class="metric">
                            <span class="metric-label">胜率</span>
                            <span class="metric-value">{self.performance.get('win_rate', 'N/A')}</span>
                        </div>
                    </div>
                </div>
                
                <div class="footer">
                    <p>© 2025 量化交易框架 | 报告自动生成</p>
                </div>
            </div>
        </body>
        </html>
        """
        
        # 保存文件
        html_file = output_path / f"dashboard_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
        with open(html_file, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        print(f"📊 离线报告已生成: {html_file}")
        return str(html_file)
    
    def launch(self, **kwargs):
        """
        启动Web界面
        
        Args:
            **kwargs: 传递给app.run_server的参数
        """
        if not DASH_AVAILABLE:
            print("❌ Dash未安装，无法启动Web界面")
            print("💡 请安装: pip install dash dash-bootstrap-components")
            print("📊 但您仍可以使用 create_offline_report() 生成静态报告")
            return False
        
        if not self.app:
            self._init_dash_app()
        
        print(f"🚀 启动Dashboard: http://{self.config['host']}:{self.config['port']}")
        
        # 合并默认参数和用户参数
        run_params = {
            'host': self.config['host'],
            'port': self.config['port'],
            'debug': self.config['debug']
        }
        run_params.update(kwargs)
        
        self.app.run_server(**run_params)
        return True

# ==========================================
# 简化版本的监控面板（不依赖Dash）
# ==========================================

class SimpleDashboard:
    """
    简化版监控面板（不需要Dash）
    
    提供基本的控制台监控功能
    """
    
    def __init__(self):
        """初始化简化面板"""
        self.strategies = {}
        self.metrics = {}
        self.signals = []
        
    def display_summary(self):
        """显示摘要信息"""
        print("\n" + "=" * 60)
        print("📊 策略监控面板")
        print("=" * 60)
        print(f"⏰ 更新时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"📈 运行策略: {len(self.strategies)}")
        print(f"🔔 待处理信号: {len(self.signals)}")
        
        if self.metrics:
            print("\n📊 关键指标:")
            for key, value in self.metrics.items():
                print(f"   {key}: {value}")
    
    def update_metrics(self, metrics: Dict[str, Any]):
        """更新指标"""
        self.metrics.update(metrics)
    
    def add_signal(self, signal: Dict[str, Any]):
        """添加信号"""
        self.signals.append(signal)
        if len(self.signals) > 100:  # 限制信号数量
            self.signals = self.signals[-100:]

# ==========================================
# 测试代码
# ==========================================

if __name__ == "__main__":
    print("\n" + "=" * 50)
    print("测试Dashboard模块")
    print("=" * 50)
    
    # 测试主Dashboard
    dashboard = Dashboard()
    
    # 测试数据更新
    test_data = {
        'current_price': 100.5,
        'daily_change': 0.025,
        'volume': 1000000,
        'rsi': 55.3
    }
    dashboard.update_data(test_data)
    
    # 测试策略添加
    dashboard.add_strategy('MA_Cross', {
        'fast': 5,
        'slow': 20
    })
    
    # 创建离线报告
    report_path = dashboard.create_offline_report()
    
    print("\n✅ Dashboard测试完成!")
    print(f"📊 报告已生成: {report_path}")