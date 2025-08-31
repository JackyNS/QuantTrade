#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
图表生成器 - charts.py
======================

专业的金融图表生成器，支持多种图表类型：
- K线图（Candlestick）
- 技术指标叠加
- 成交量图
- 收益曲线
- 回撤分析
- 相关性热力图
- 仓位分布图

版本: 1.0.0
更新: 2025-08-30
"""

import warnings
from typing import Dict, List, Optional, Any, Union, Tuple
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

# 图表库导入
try:
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots
    import plotly.express as px
    PLOTLY_AVAILABLE = True
except ImportError:
    PLOTLY_AVAILABLE = False
    print("⚠️ Plotly未安装，部分功能将不可用")

try:
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    from matplotlib.figure import Figure
    import seaborn as sns
    MATPLOTLIB_AVAILABLE = True
    sns.set_style("whitegrid")
except ImportError:
    MATPLOTLIB_AVAILABLE = False
    print("⚠️ Matplotlib/Seaborn未安装，部分功能将不可用")

warnings.filterwarnings('ignore')

class Charts:
    """
    专业图表生成器
    
    支持多种金融图表的生成和定制
    """
    
    def __init__(self, config: Optional[Dict] = None):
        """
        初始化图表生成器
        
        Args:
            config: 配置字典，包含样式、颜色等设置
        """
        self.config = config or {}
        
        # 默认配置
        self.default_config = {
            'theme': 'plotly_white',
            'colors': {
                'up': '#26a69a',      # 上涨颜色
                'down': '#ef5350',    # 下跌颜色
                'volume': '#1f77b4',  # 成交量颜色
                'ma5': '#ff9800',     # MA5颜色
                'ma10': '#9c27b0',    # MA10颜色
                'ma20': '#3f51b5',    # MA20颜色
                'macd': '#4caf50',    # MACD颜色
                'signal': '#ff5722',  # Signal颜色
                'histogram': '#607d8b' # 柱状图颜色
            },
            'figure_size': (14, 8),
            'font_size': 12,
            'show_grid': True
        }
        
        # 合并配置
        self.config = {**self.default_config, **self.config}
        
        print("📊 图表生成器初始化完成")
    
    def plot_candlestick(self, 
                        data: pd.DataFrame,
                        title: str = "K线图",
                        show_volume: bool = True,
                        indicators: Optional[List[str]] = None,
                        save_path: Optional[str] = None) -> Any:
        """
        绘制K线图
        
        Args:
            data: 包含OHLCV数据的DataFrame
            title: 图表标题
            show_volume: 是否显示成交量
            indicators: 要叠加的技术指标列表
            save_path: 保存路径
        
        Returns:
            图表对象
        """
        if not PLOTLY_AVAILABLE:
            print("❌ Plotly未安装，无法生成图表")
            return None
        
        # 创建子图
        rows = 2 if show_volume else 1
        row_heights = [0.7, 0.3] if show_volume else [1]
        
        fig = make_subplots(
            rows=rows, cols=1,
            shared_xaxes=True,
            vertical_spacing=0.03,
            row_heights=row_heights,
            subplot_titles=(title, "成交量") if show_volume else (title,)
        )
        
        # 添加K线图
        fig.add_trace(
            go.Candlestick(
                x=data.index,
                open=data['open'],
                high=data['high'],
                low=data['low'],
                close=data['close'],
                name='K线',
                increasing_line_color=self.config['colors']['up'],
                decreasing_line_color=self.config['colors']['down']
            ),
            row=1, col=1
        )
        
        # 添加技术指标
        if indicators:
            self._add_indicators(fig, data, indicators, 1, 1)
        
        # 添加成交量
        if show_volume and 'volume' in data.columns:
            colors = [self.config['colors']['up'] if data['close'].iloc[i] >= data['open'].iloc[i] 
                     else self.config['colors']['down'] 
                     for i in range(len(data))]
            
            fig.add_trace(
                go.Bar(
                    x=data.index,
                    y=data['volume'],
                    name='成交量',
                    marker_color=colors,
                    opacity=0.7
                ),
                row=2, col=1
            )
        
        # 更新布局
        fig.update_layout(
            template=self.config['theme'],
            height=600,
            showlegend=True,
            xaxis_rangeslider_visible=False
        )
        
        # 更新x轴
        fig.update_xaxes(title_text="日期", row=rows, col=1)
        fig.update_yaxes(title_text="价格", row=1, col=1)
        if show_volume:
            fig.update_yaxes(title_text="成交量", row=2, col=1)
        
        # 保存图表
        if save_path:
            fig.write_html(save_path)
            print(f"📊 图表已保存至: {save_path}")
        
        return fig
    
    def plot_returns(self, 
                     returns,
                     title: str = "收益率曲线",
                     benchmark_returns=None,
                     save_path: Optional[str] = None):
        """
        绘制收益率曲线
        
        Args:
            returns: 收益率序列
            title: 图表标题
            benchmark_returns: 基准收益率（可选）
            save_path: 保存路径
        
        Returns:
            图表对象
        """
        if not PLOTLY_AVAILABLE:
            print("❌ Plotly未安装，无法生成图表")
            return None
        
        import plotly.graph_objects as go
        
        # 创建图表
        fig = go.Figure()
        
        # 计算累计收益率
        cumulative_returns = (1 + returns).cumprod() - 1
        
        # 添加策略收益率曲线
        fig.add_trace(go.Scatter(
            x=returns.index if hasattr(returns, 'index') else list(range(len(returns))),
            y=cumulative_returns,
            mode='lines',
            name='策略收益',
            line=dict(color='#1f77b4', width=2)
        ))
        
        # 如果有基准收益率，添加基准曲线
        if benchmark_returns is not None:
            benchmark_cumulative = (1 + benchmark_returns).cumprod() - 1
            fig.add_trace(go.Scatter(
                x=benchmark_returns.index if hasattr(benchmark_returns, 'index') else list(range(len(benchmark_returns))),
                y=benchmark_cumulative,
                mode='lines',
                name='基准收益',
                line=dict(color='#ff7f0e', width=2, dash='dash')
            ))
        
        # 更新布局
        fig.update_layout(
            title=title,
            xaxis_title='时间',
            yaxis_title='累计收益率',
            template=self.config.get('theme', 'plotly_white'),
            height=500,
            showlegend=True,
            hovermode='x unified'
        )
        
        # 保存图表
        if save_path:
            fig.write_html(save_path)
            print(f"📊 图表已保存至: {save_path}")
        
        return fig
    
    def plot_drawdown(self, 
                     returns: pd.Series,
                     title: str = "回撤分析",
                     save_path: Optional[str] = None) -> Any:
        """
        绘制回撤图
        
        Args:
            returns: 收益率序列
            title: 图表标题
            save_path: 保存路径
        
        Returns:
            图表对象
        """
        if not PLOTLY_AVAILABLE:
            print("❌ Plotly未安装，无法生成图表")
            return None
        
        # 计算累计收益
        cum_returns = (1 + returns).cumprod()
        
        # 计算回撤
        running_max = cum_returns.cummax()
        drawdown = (cum_returns - running_max) / running_max
        
        # 创建图表
        fig = make_subplots(
            rows=2, cols=1,
            shared_xaxes=True,
            vertical_spacing=0.05,
            subplot_titles=("累计收益", "回撤"),
            row_heights=[0.6, 0.4]
        )
        
        # 添加累计收益曲线
        fig.add_trace(
            go.Scatter(
                x=returns.index,
                y=cum_returns,
                mode='lines',
                name='累计收益',
                line=dict(color='#1f77b4', width=2)
            ),
            row=1, col=1
        )
        
        # 添加回撤区域
        fig.add_trace(
            go.Scatter(
                x=drawdown.index,
                y=drawdown,
                mode='lines',
                name='回撤',
                fill='tozeroy',
                line=dict(color='#ef5350', width=1),
                fillcolor='rgba(239, 83, 80, 0.3)'
            ),
            row=2, col=1
        )
        
        # 更新布局
        fig.update_layout(
            title=title,
            template=self.config['theme'],
            height=600,
            showlegend=True
        )
        
        fig.update_xaxes(title_text="日期", row=2, col=1)
        fig.update_yaxes(title_text="累计收益", row=1, col=1)
        fig.update_yaxes(title_text="回撤率", row=2, col=1)
        
        # 保存图表
        if save_path:
            fig.write_html(save_path)
            print(f"📊 图表已保存至: {save_path}")
        
        return fig
    
    def _add_indicators(self, fig: Any, data: pd.DataFrame, 
                       indicators: List[str], row: int, col: int):
        """
        向图表添加技术指标
        
        Args:
            fig: 图表对象
            data: 数据DataFrame
            indicators: 指标列表
            row: 子图行号
            col: 子图列号
        """
        # 颜色映射
        color_map = {
            'ma5': self.config['colors']['ma5'],
            'ma10': self.config['colors']['ma10'],
            'ma20': self.config['colors']['ma20'],
            'ema12': '#ff5722',
            'ema26': '#3f51b5',
            'upper_band': '#9e9e9e',
            'lower_band': '#9e9e9e'
        }
        
        for indicator in indicators:
            if indicator in data.columns:
                fig.add_trace(
                    go.Scatter(
                        x=data.index,
                        y=data[indicator],
                        mode='lines',
                        name=indicator.upper(),
                        line=dict(
                            color=color_map.get(indicator, '#000000'),
                            width=1
                        )
                    ),
                    row=row, col=col
                )
    
    def create_subplot_layout(self,
                            specs: List[List[Dict]],
                            titles: List[str]) -> Any:
        """
        创建自定义子图布局
        
        Args:
            specs: 子图规格
            titles: 子图标题
            
        Returns:
            子图对象
        """
        if not PLOTLY_AVAILABLE:
            print("❌ Plotly未安装，无法创建子图")
            return None
        
        fig = make_subplots(
            rows=len(specs),
            cols=len(specs[0]) if specs else 1,
            subplot_titles=titles,
            specs=specs,
            vertical_spacing=0.08,
            horizontal_spacing=0.1
        )
        
        return fig
    
    def save_all_charts(self, 
                       data: pd.DataFrame,
                       output_dir: str = "./results/charts"):
        """
        生成并保存所有标准图表
        
        Args:
            data: 数据DataFrame
            output_dir: 输出目录
        """
        from pathlib import Path
        
        # 创建输出目录
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        print(f"📊 开始生成所有图表...")
        
        # 1. K线图
        if all(col in data.columns for col in ['open', 'high', 'low', 'close']):
            self.plot_candlestick(
                data,
                title="股票K线图",
                save_path=str(output_path / "candlestick.html")
            )
        
        # 2. 收益曲线
        if 'returns' in data.columns:
            self.plot_returns(
                data['returns'],
                title="策略收益曲线",
                save_path=str(output_path / "returns.html")
            )
            
            # 3. 回撤分析
            self.plot_drawdown(
                data['returns'],
                title="回撤分析",
                save_path=str(output_path / "drawdown.html")
            )
        
        print(f"✅ 所有图表已保存至: {output_path}")
    
    def plot_technical_indicators(self, 
                                 data: pd.DataFrame,
                                 indicators: List[str],
                                 title: str = "技术指标图",
                                 save_path: Optional[str] = None) -> Any:
        """
        绘制技术指标图
        
        Args:
            data: 包含指标数据的DataFrame
            indicators: 要绘制的指标列表
            title: 图表标题
            save_path: 保存路径
        
        Returns:
            图表对象
        """
        if not PLOTLY_AVAILABLE:
            print("❌ Plotly未安装，无法生成图表")
            return None
        
        from plotly.subplots import make_subplots
        
        # 创建子图
        fig = make_subplots(
            rows=len(indicators),
            cols=1,
            subplot_titles=indicators,
            vertical_spacing=0.05,
            row_heights=[1] * len(indicators)
        )
        
        # 为每个指标创建图表
        colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b']
        
        for i, indicator in enumerate(indicators, 1):
            if indicator in data.columns:
                fig.add_trace(
                    go.Scatter(
                        x=data.index,
                        y=data[indicator],
                        mode='lines',
                        name=indicator,
                        line=dict(color=colors[(i-1) % len(colors)], width=2)
                    ),
                    row=i, col=1
                )
        
        # 更新布局
        fig.update_layout(
            title=title,
            height=200 * len(indicators),
            showlegend=True,
            template=self.config.get('theme', 'plotly_white')
        )
        
        if save_path:
            fig.write_html(save_path)
            print(f"📊 图表已保存至: {save_path}")
        
        return fig
    
    def plot_volume(self, 
                   data: pd.DataFrame,
                   title: str = "成交量图",
                   save_path: Optional[str] = None) -> Any:
        """
        绘制成交量图
        
        Args:
            data: 包含volume数据的DataFrame
            title: 图表标题
            save_path: 保存路径
        
        Returns:
            图表对象
        """
        if not PLOTLY_AVAILABLE:
            print("❌ Plotly未安装，无法生成图表")
            return None
        
        if 'volume' not in data.columns:
            print("❌ 数据中没有volume列")
            return None
        
        # 计算涨跌颜色
        colors = []
        if 'close' in data.columns and 'open' in data.columns:
            colors = ['red' if data['close'].iloc[i] < data['open'].iloc[i] 
                     else 'green' for i in range(len(data))]
        else:
            colors = 'blue'
        
        fig = go.Figure()
        
        fig.add_trace(go.Bar(
            x=data.index,
            y=data['volume'],
            name='成交量',
            marker_color=colors,
            opacity=0.7
        ))
        
        fig.update_layout(
            title=title,
            xaxis_title='日期',
            yaxis_title='成交量',
            height=400,
            template=self.config.get('theme', 'plotly_white'),
            showlegend=False,
            hovermode='x unified'
        )
        
        if save_path:
            fig.write_html(save_path)
            print(f"📊 图表已保存至: {save_path}")
        
        return fig
    
    def plot_correlation_heatmap(self, 
                                data: pd.DataFrame,
                                title: str = "相关性热力图",
                                save_path: Optional[str] = None) -> Any:
        """
        绘制相关性热力图
        
        Args:
            data: 数据DataFrame
            title: 图表标题
            save_path: 保存路径
        
        Returns:
            图表对象
        """
        if not PLOTLY_AVAILABLE:
            print("❌ Plotly未安装，无法生成图表")
            return None
        
        # 计算相关性矩阵
        corr_matrix = data.select_dtypes(include=[np.number]).corr()
        
        fig = go.Figure(data=go.Heatmap(
            z=corr_matrix.values,
            x=corr_matrix.columns,
            y=corr_matrix.columns,
            colorscale='RdBu',
            zmid=0,
            text=corr_matrix.values.round(2),
            texttemplate='%{text}',
            textfont={"size": 10},
            colorbar=dict(title="相关系数"),
            hoverongaps=False
        ))
        
        fig.update_layout(
            title=title,
            height=600,
            width=800,
            xaxis={'side': 'bottom'},
            yaxis={'side': 'left'},
            template=self.config.get('theme', 'plotly_white')
        )
        
        if save_path:
            fig.write_html(save_path)
            print(f"📊 图表已保存至: {save_path}")
        
        return fig

# ==========================================
# 测试代码
# ==========================================

if __name__ == "__main__":
    print("\n" + "=" * 50)
    print("测试图表生成器")
    print("=" * 50)
    
    # 生成测试数据
    import pandas as pd
    import numpy as np
    from datetime import datetime, timedelta
    
    dates = pd.date_range(start='2024-01-01', end='2024-08-26', freq='D')
    n = len(dates)
    
    test_data = pd.DataFrame({
        'date': dates,
        'open': 100 + np.random.randn(n).cumsum(),
        'high': 102 + np.random.randn(n).cumsum(),
        'low': 98 + np.random.randn(n).cumsum(),
        'close': 100 + np.random.randn(n).cumsum(),
        'volume': np.random.randint(1000000, 10000000, n),
        'returns': np.random.randn(n) * 0.02
    })
    test_data.set_index('date', inplace=True)
    
    # 添加技术指标
    test_data['ma5'] = test_data['close'].rolling(5).mean()
    test_data['ma10'] = test_data['close'].rolling(10).mean()
    test_data['ma20'] = test_data['close'].rolling(20).mean()
    
    # 创建图表生成器
    charts = Charts()
    
    # 测试各种图表
    print("\n测试K线图...")
    fig1 = charts.plot_candlestick(test_data, indicators=['ma5', 'ma10', 'ma20'])
    
    print("\n测试收益率曲线...")
    fig2 = charts.plot_returns(test_data['returns'])
    
    print("\n测试回撤分析...")
    fig3 = charts.plot_drawdown(test_data['returns'])
    
    print("\n✅ 图表生成器测试完成!")