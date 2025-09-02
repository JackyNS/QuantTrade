# 🚀 QuantTrade Scripts

## 📖 概述

QuantTrade Scripts是高级业务脚本集合，提供完整的量化交易解决方案。与tools目录的开发工具不同，scripts专注于**核心业务逻辑**和**自动化任务执行**。

## 🏗️ 目录结构

### automation/ - 自动化系统
- `scheduler.py` - 任务调度器
- `alert_manager.py` - 预警管理
- `backup.py` - 数据备份
- `notification.py` - 通知系统

### reporting/ - 报告系统  
- `weekly_report.py` - 周报生成
- `monthly_report.py` - 月报生成
- `performance_dashboard.py` - 实时仪表盘

### analysis/ - 高级分析
- `market_analysis.py` - 市场分析
- `portfolio_analysis.py` - 投资组合分析
- `sector_analysis.py` - 行业分析

### optimization/ - 投资优化
- `optimize_allocation.py` - 资产配置优化
- `optimize_portfolio.py` - 投资组合优化
- `risk_optimization.py` - 风险优化

### monitoring/ - 监控系统
- `realtime_monitor.py` - 实时监控
- `performance_tracker.py` - 业绩追踪
- `system_monitor.py` - 系统监控

### backtest/ - 回测系统
- `backtest_engine.py` - 回测引擎
- `batch_backtest.py` - 批量回测
- `backtest_analysis.py` - 回测分析
- `backtest_report.py` - 回测报告

### trading/ - 交易系统
- `run_trading.py` - 交易执行
- `position_manager.py` - 仓位管理
- `trading_manager.py` - 交易管理

## 🚀 使用方法

### 统一入口
```bash
# 列出所有可用模块
python scripts/run_scripts.py --list

# 运行指定脚本
python scripts/run_scripts.py [模块] [脚本] [参数]

# 示例
python scripts/run_scripts.py reporting weekly
python scripts/run_scripts.py monitoring realtime --symbol=000001
python scripts/run_scripts.py backtest batch --start=2024-01-01
```

### 直接运行
```bash
# 直接运行脚本
python scripts/reporting/weekly_report.py
python scripts/automation/scheduler.py --mode=daily
python scripts/optimization/optimize_portfolio.py --strategy=momentum
```

## 🔧 与tools目录的区别

| 特性 | Scripts | Tools |
|------|---------|--------|
| **用途** | 核心业务逻辑执行 | 开发和维护工具 |
| **运行方式** | 生产环境自动化运行 | 开发时手动运行 |
| **复杂度** | 高级业务算法 | 简单工具脚本 |
| **依赖** | 完整的业务数据 | 基础开发环境 |
| **目标用户** | 投资经理、交易员 | 开发人员 |

## 📊 核心功能

### 🤖 自动化任务
- 定时策略执行
- 风险监控和预警
- 数据备份和维护
- 系统状态通知

### 📈 分析报告
- 策略业绩分析
- 市场趋势报告
- 风险评估报告
- 投资组合优化建议

### ⚡ 实时监控
- 实时行情监控
- 仓位变化追踪  
- 系统异常检测
- 业绩实时评估

### 🎯 策略优化
- 参数优化算法
- 资产配置优化
- 风险调整优化
- 多策略组合优化

## 🔒 安全注意事项

1. **生产环境配置**: 确保正确配置生产环境变量
2. **权限控制**: 脚本具有数据写入权限，请谨慎使用
3. **监控日志**: 定期检查脚本执行日志
4. **备份策略**: 重要脚本执行前确保数据备份

## 📝 开发规范

1. **统一入口**: 所有脚本支持通过`run_scripts.py`调用
2. **参数处理**: 使用argparse处理命令行参数
3. **错误处理**: 完善的异常处理和日志记录
4. **文档规范**: 详细的docstring和使用说明
5. **测试覆盖**: 核心逻辑需要单元测试

## 🆘 故障排除

### 常见问题
1. **导入错误**: 确保从项目根目录运行脚本
2. **配置缺失**: 检查必要的配置文件和环境变量
3. **权限不足**: 确保脚本有必要的文件读写权限
4. **依赖缺失**: 安装所需的Python包

### 获取帮助
```bash
# 查看脚本帮助
python scripts/run_scripts.py [模块] [脚本] --help

# 检查脚本状态
python tools/testing/strategy_validator.py
python tools/analysis/data_quality_checker.py
```

---

*QuantTrade Scripts - 专业量化交易业务脚本集合*
