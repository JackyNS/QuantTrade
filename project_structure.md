# 🏗️ QuantTrade 项目最新目录结构

**更新时间**: 2025-08-31  
**版本**: 1.0.0 (scripts模块已更新)

## 📁 根目录概览

```
QuantTrade/
├── 📁 core/                          # 🎯 核心框架模块
├── 📁 scripts/                       # 🚀 脚本模块 (新版本)
├── 📁 data/                          # 💾 数据存储目录
├── 📁 notebooks/                     # 📓 Jupyter分析笔记
├── 📁 tests/                         # 🧪 测试文件
├── 📁 logs/                          # 📝 日志文件
├── 📁 results/                       # 📊 结果输出
├── 📁 output/                        # 📤 输出文件
├── 📁 docs/                          # 📚 文档
├── 📁 config/                        # ⚙️ 配置目录
├── 📁 cache/                         # 💨 缓存目录
├── 📁 scripts_backup/                # 🗂️ 旧脚本备份
├── 🐍 main.py                        # 🚪 主程序入口
├── 📋 requirements.txt               # 📦 依赖包清单
├── 🔧 setup.py                      # 🛠️ 安装配置
├── 📖 README.md                     # 📖 项目说明
├── 📄 CLAUDE.md                     # 🤖 Claude助手说明
└── 📊 *.json, *.md                  # 📈 测试报告和文档
```

## 🎯 核心框架 (core/)

```
core/
├── 📁 config/                        # ✅ 配置管理
│   ├── settings.py                   # 基础配置类
│   ├── trading_config.py            # 交易配置
│   ├── database_config.py           # 数据库配置
│   └── __init__.py                  # 模块初始化
├── 📁 data/                         # 🔧 数据处理
│   ├── data_loader.py               # 数据获取器
│   ├── data_processor.py            # 数据预处理器
│   ├── feature_engineer.py          # 特征工程
│   ├── data_manager.py              # 数据管理器
│   ├── cache/                       # 数据缓存
│   └── __init__.py                  # 模块初始化
├── 📁 strategy/                     # 📋 策略模块
│   ├── base_strategy.py             # 策略基类
│   ├── technical_indicators.py      # 技术指标
│   ├── market_sentiment.py          # 市场情绪分析
│   ├── capital_flow_analysis.py     # 资金流向分析
│   ├── pattern_recognition.py       # 形态识别
│   ├── signal_generator.py          # 信号生成器
│   ├── position_manager.py          # 仓位管理
│   └── __init__.py                  # 模块初始化
├── 📁 backtest/                     # 📋 回测引擎
│   ├── backtest_engine.py           # 回测引擎核心
│   ├── performance_analyzer.py      # 性能分析器
│   ├── risk_manager.py              # 风险管理器
│   ├── report_generator.py          # 报告生成器
│   └── __init__.py                  # 模块初始化
├── 📁 visualization/                # 📋 可视化
│   ├── charts.py                    # 图表生成
│   ├── dashboard.py                 # 仪表板
│   ├── reports.py                   # 报告可视化
│   └── __init__.py                  # 模块初始化
├── 📁 screening/                    # 📋 股票筛选
│   ├── stock_screener.py            # 股票筛选器
│   ├── fundamental_filter.py        # 基本面筛选
│   ├── technical_filter.py          # 技术面筛选
│   ├── factor_ranker.py             # 因子排序
│   └── __init__.py                  # 模块初始化
├── 📁 utils/                       # 📋 工具模块
│   ├── logger.py                    # 日志系统
│   ├── decorators.py                # 装饰器
│   ├── validators.py                # 验证器
│   ├── helpers.py                   # 辅助函数
│   ├── exceptions.py                # 异常定义
│   └── __init__.py                  # 模块初始化
└── __init__.py                      # 核心模块初始化
```

## 🚀 脚本模块 (scripts/) - 新版本

```
scripts/                             # 📦 v1.0.0 (已替换scripts_new)
├── 📁 data/                         # 📊 数据管理 (10个文件)
│   ├── download_a_shares.py         # A股全市场数据下载
│   ├── download_strategy_data.py    # 策略数据下载
│   ├── download_indicators.py       # 技术指标下载
│   ├── data_validation.py           # 数据质量验证
│   ├── data_cleanup.py              # 数据清理
│   ├── strategy_data_manager.py     # 策略数据管理
│   ├── update_daily.py              # 日常数据更新
│   └── update_daily_wrapper.py      # 更新包装器
├── 📁 strategy/                     # 🎯 策略脚本 (5个文件)
│   ├── run_strategy.py              # 策略执行
│   ├── strategy_monitor.py          # 策略监控
│   ├── strategy_scanner.py          # 策略扫描
│   ├── strategy_validator.py        # 策略验证
│   └── __init__.py                  # 模块初始化
├── 📁 backtest/                     # 📈 回测脚本 (5个文件)
│   ├── run_backtest.py              # 回测执行
│   ├── backtest_analysis.py         # 回测分析
│   ├── backtest_report.py           # 回测报告
│   ├── batch_backtest.py            # 批量回测
│   └── __init__.py                  # 模块初始化
├── 📁 analysis/                     # 🔍 分析脚本 (5个文件)
│   ├── performance_analysis.py      # 性能分析
│   ├── market_analysis.py           # 市场分析
│   ├── portfolio_analysis.py        # 组合分析
│   ├── risk_analysis.py             # 风险分析
│   └── __init__.py                  # 模块初始化
├── 📁 monitoring/                   # 👁️ 监控脚本 (5个文件)
│   ├── realtime_monitor.py          # 实时监控
│   ├── alert_manager.py             # 告警管理
│   ├── daily_monitor.py             # 日常监控
│   ├── performance_tracker.py       # 性能跟踪
│   └── __init__.py                  # 模块初始化
├── 📁 reporting/                    # 📋 报告脚本 (4个文件)
│   ├── daily_report.py              # 日报生成
│   ├── weekly_report.py             # 周报生成
│   ├── monthly_report.py            # 月报生成
│   └── __init__.py                  # 模块初始化
├── 📁 optimization/                 # ⚡ 优化脚本 (4个文件)
│   ├── optimize_params.py           # 参数优化
│   ├── optimize_portfolio.py        # 组合优化
│   ├── optimize_allocation.py       # 配置优化
│   └── __init__.py                  # 模块初始化
├── 📁 screening/                    # 🔎 筛选脚本 (4个文件)
│   ├── run_screening.py             # 筛选执行
│   ├── screening_report.py          # 筛选报告
│   ├── screening_monitor.py         # 筛选监控
│   └── __init__.py                  # 模块初始化
├── 📁 utils/                       # 🛠️ 工具脚本 (4个文件)
│   ├── backup.py                    # 备份工具
│   ├── notification.py              # 通知系统
│   ├── scheduler.py                 # 任务调度
│   └── __init__.py                  # 模块初始化
└── __init__.py                      # 脚本模块初始化
```

## 💾 数据目录 (data/)

```
data/
├── 📁 raw/                          # 原始数据
├── 📁 processed/                    # 处理后数据
├── 📁 cache/                        # 数据缓存
├── 📁 metadata/                     # 元数据
├── 📁 strategy/                     # 策略数据
└── 📁 backtest/                     # 回测数据
```

## 📓 Jupyter笔记 (notebooks/)

```
notebooks/
├── 📁 analysis/                     # 分析笔记
├── 📁 development/                  # 开发笔记
├── 📁 experiments/                  # 实验笔记
├── 📁 fixes/                       # 修复笔记
├── 📁 testing/                      # 测试笔记
├── 📁 tutorials/                    # 教程笔记
└── 📁 visualization/                # 可视化笔记
```

## 📊 输出目录

```
output/                              # 输出文件
├── 📁 charts/                       # 图表输出
├── 📁 exports/                      # 数据导出
└── 📁 reports/                      # 报告输出

results/                             # 结果文件
├── 📁 charts/                       # 结果图表
├── 📁 dashboard/                    # 仪表板
└── 📁 reports/                      # 结果报告
```

## 📝 日志系统 (logs/)

```
logs/
├── 📁 system/                       # 系统日志
├── 📁 trading/                      # 交易日志
├── 📁 data/                         # 数据日志
└── 📁 error/                        # 错误日志
```

## 🗂️ 备份和测试文件

```
scripts_backup/                      # 🔒 旧scripts模块备份
test_*.py                           # 🧪 测试脚本
*.json                              # 📊 测试报告
scripts_migration_report.md         # 📋 迁移报告
migration_summary.md                # 📋 迁移总结
```

## 📈 项目统计

- **总Python文件**: ~100+ 个
- **核心模块**: 6个 (config, data, strategy, backtest, visualization, utils)
- **脚本模块**: 9个 (data, strategy, backtest, analysis, monitoring, reporting, optimization, screening, utils)
- **Jupyter笔记**: 7个分类目录
- **测试覆盖**: 语法测试100%通过

## 🎊 最新更新

- ✅ **scripts模块**: 已从scripts_new升级，包含46个重构的Python文件
- ✅ **测试完整**: 100%语法检查通过，集成测试通过
- ✅ **备份安全**: 原脚本已备份到scripts_backup/
- ✅ **文档完整**: 包含CLAUDE.md和完整的迁移文档

---

**项目状态**: 🚀 已准备就绪，核心框架完整，脚本模块已更新