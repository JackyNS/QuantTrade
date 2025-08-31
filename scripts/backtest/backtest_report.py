#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
回测报告生成脚本
================

生成专业的回测报告

Author: QuantTrader Team
Date: 2025-08-31
"""

from typing import Dict

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
from pathlib import Path
import json

class ReportGenerator:
    """报告生成器"""
    
    def __init__(self, backtest_results: Dict):
        self.results = backtest_results
        self.report_dir = Path("./data/reports")
        self.report_dir.mkdir(parents=True, exist_ok=True)
        
    def generate_html_report(self) -> str:
        """生成HTML报告"""
        html_template = """
        <!DOCTYPE html>
        <html>
        <head>
            <title>回测报告</title>
            <style>
                body { font-family: Arial, sans-serif; margin: 20px; }
                h1 { color: #333; }
                table { border-collapse: collapse; width: 100%; }
                th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
                th { background-color: #f2f2f2; }
                .metric { font-weight: bold; }
                .positive { color: green; }
                .negative { color: red; }
            </style>
        </head>
        <body>
            <h1>策略回测报告</h1>
            <h2>报告生成时间: {timestamp}</h2>
            
            <h3>核心指标</h3>
            <table>
                <tr><td>总收益率</td><td class="metric {return_class}">{total_return:.2%}</td></tr>
                <tr><td>年化收益率</td><td class="metric">{annual_return:.2%}</td></tr>
                <tr><td>夏普比率</td><td class="metric">{sharpe_ratio:.2f}</td></tr>
                <tr><td>最大回撤</td><td class="metric negative">{max_drawdown:.2%}</td></tr>
                <tr><td>胜率</td><td class="metric">{win_rate:.2%}</td></tr>
            </table>
            
            <h3>详细统计</h3>
            <table>
                <tr><th>指标</th><th>数值</th></tr>
                {detailed_rows}
            </table>
        </body>
        </html>
        """
        
        # 准备数据
        metrics = self.results.get('metrics', {})
        
        # 生成详细行
        detailed_rows = ""
        for key, value in metrics.items():
            if isinstance(value, float):
                detailed_rows += f"<tr><td>{key}</td><td>{value:.4f}</td></tr>\n"
        
        # 填充模板
        html_content = html_template.format(
            timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            total_return=metrics.get('total_return', 0),
            annual_return=metrics.get('annual_return', 0),
            sharpe_ratio=metrics.get('sharpe_ratio', 0),
            max_drawdown=metrics.get('max_drawdown', 0),
            win_rate=metrics.get('win_rate', 0),
            return_class='positive' if metrics.get('total_return', 0) > 0 else 'negative',
            detailed_rows=detailed_rows
        )
        
        # 保存文件
        report_file = self.report_dir / f"backtest_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        print(f"报告已生成: {report_file}")
        return str(report_file)

def main():
    # 示例数据
    sample_results = {
        'metrics': {
            'total_return': 0.25,
            'annual_return': 0.15,
            'sharpe_ratio': 1.5,
            'max_drawdown': -0.12,
            'win_rate': 0.55
        }
    }
    
    generator = ReportGenerator(sample_results)
    generator.generate_html_report()

if __name__ == "__main__":
    main()