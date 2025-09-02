# QuantTrade 数据目录结构说明

## 📁 完整数据目录结构

```
/Users/jackstudio/QuantTrade/data/
├── basic_info/              # 🏢 股票基本信息 [EquGet]
│   └── equget.csv          # 股票代码、名称、交易所、上市状态、上市时间等
├── daily/                   # 📈 日行情数据 [MktEqudGet] 
│   └── mktequdget_2024.csv # 开盘价、最高价、最低价、收盘价、成交量、市值
├── calendar/                # 📅 交易日历 [TradCalGet] - 待修复
│   └── (空)                # 交易日、休市日标记
├── capital_flow/            # 💰 资金流向数据 [MktEquFlowGet]
│   └── mktequflowget_2024.csv # 主力净流入、大单中单小单流向
├── limit_info/              # 📊 涨跌停信息 [MktLimitGet]
│   └── mktlimitget_2024.csv # 涨停价、跌停价、涨跌停状态
├── rank_list/               # 🔥 龙虎榜数据 [MktRankListStocksGet]
│   └── mktrankliststocksget_2024.csv # 异动股票、上榜原因、买卖金额
│
├── adjustment/              # 📉 复权因子 [MktAdjfGet] - 待下载
├── dividend/                # 💎 分红数据 [EquDivGet] - 待下载  
├── market_cap/              # 🏦 市值数据 [MktCapGet] - 待下载
├── financial/               # 💰 财务数据 [FdmtIncomeGet等] - 待下载
│
├── metadata/                # 📋 元数据信息
│   ├── all_stocks.json     # 所有股票列表
│   ├── available_stocks.json # 可用股票
│   ├── data_structure_info.json # 数据结构信息
│   └── download_progress.json # 下载进度
│
├── cache/                   # 🗂️ 缓存数据
│   ├── api/                # API响应缓存
│   ├── compressed/         # 压缩数据缓存
│   ├── disk/               # 磁盘缓存
│   ├── features/           # 特征工程缓存
│   ├── memory/             # 内存缓存
│   ├── models/             # 模型缓存
│   └── sqlite/             # SQLite数据库缓存
│
├── processed/               # 🔧 处理后数据
│   ├── factors/            # 因子数据
│   ├── features/           # 特征数据
│   ├── indicators/         # 技术指标
│   └── signals/            # 交易信号
│
├── backtest/                # 📊 回测相关
│   ├── reports/            # 回测报告
│   └── results/            # 回测结果
│
├── reports/                 # 📄 分析报告
│   └── weekly/             # 周度报告
│
├── raw/                     # 📥 原始数据存储
├── monthly/                 # 📊 月度数据
├── weekly/                  # 📊 周度数据
│
├── backup/                  # 💾 数据备份
│   └── original/           # 原始备份
│
├── mktequd_complete/        # 📈 完整行情数据 (历史)
│   ├── year_2000/          # 2000年数据
│   ├── year_2001/          # 2001年数据
│   └── ...                 # 其他年份
│
├── mktequd_daily/           # 📈 日度数据存储
├── realistic_complete/      # 📊 完整现实数据
│   ├── daily/              # 日度完整数据
│   ├── daily_adj/          # 日度复权数据
│   ├── weekly/             # 周度数据
│   ├── weekly_adj/         # 周度复权数据
│   ├── monthly/            # 月度数据
│   └── monthly_adj/        # 月度复权数据
│
└── test_api_download/       # 🧪 API测试数据
    ├── equget_test.csv     # 股票基本信息测试
    ├── mktblockdget_test.csv # 大宗交易测试
    └── ...                 # 其他测试数据
```

## 🎯 优矿API接口映射

### 🔥 核心必需数据 (已下载)

| 目录 | API接口 | 中文名称 | 数据内容 | 状态 |
|------|---------|----------|----------|------|
| `basic_info/` | `EquGet` | 股票基本信息 | 股票代码、名称、交易所、上市状态 | ✅ 完成 |
| `daily/` | `MktEqudGet` | 股票日行情 | OHLC价格、成交量、市值 | ✅ 完成 |
| `calendar/` | `TradCalGet` | 交易日历 | 交易日、休市日标记 | ❌ 待修复 |

### ⭐ 情绪分析数据 (已下载)

| 目录 | API接口 | 中文名称 | 数据内容 | 状态 |
|------|---------|----------|----------|------|
| `capital_flow/` | `MktEquFlowGet` | 个股资金流向 | 主力/大单/中单/小单流向 | ✅ 完成 |
| `limit_info/` | `MktLimitGet` | 涨跌停限制 | 涨停价、跌停价、涨跌停状态 | ✅ 完成 |
| `rank_list/` | `MktRankListStocksGet` | 龙虎榜数据 | 异动股票、上榜原因、买卖金额 | ✅ 完成 |

### 💰 财务技术数据 (待下载)

| 目录 | API接口 | 中文名称 | 数据内容 | 状态 |
|------|---------|----------|----------|------|
| `adjustment/` | `MktAdjfGet` | 复权因子 | 除权除息调整因子 | ⏳ 待下载 |
| `dividend/` | `EquDivGet` | 股票分红 | 分红派息、送股数据 | ⏳ 待下载 |
| `market_cap/` | `MktCapGet` | 市值数据 | 总市值、流通市值 | ⏳ 待下载 |
| `financial/` | `FdmtIncomeGet` | 利润表 | 营收、净利润、资产负债 | ⏳ 待下载 |
| `financial/` | `FdmtBSGet` | 资产负债表 | 总资产、总负债、净资产 | ⏳ 待下载 |
| `financial/` | `FdmtCashFlowGet` | 现金流量表 | 经营/投资/筹资现金流 | ⏳ 待下载 |

## 📊 数据完整性状态

### ✅ 已完成模块支持
- **DataManager**: 数据加载和管理 
- **BaseStrategy**: 策略开发框架
- **BacktestEngine**: 回测引擎 (需交易日历)
- **StockScreener**: 股票筛选 (基础筛选可用)
- **MarketSentimentAnalyzer**: 市场情绪分析
- **CapitalFlowAnalyzer**: 资金流向分析

### 🔧 待完善功能
- 交易日历修复 (BacktestEngine完整支持)
- 复权价格计算 (技术分析准确性)
- 基本面深度筛选 (财务数据支持)
- 完整时间范围扩展 (2000-2025年)

## 🚀 扩展计划

### 第一阶段 (当前)
- ✅ 核心交易数据
- ✅ 基础情绪数据
- 🔧 交易日历修复

### 第二阶段
- 复权因子和分红数据
- 财务三大报表
- 技术指标数据

### 第三阶段  
- 完整时间范围 (2000-2025)
- 全市场股票覆盖 (5507只)
- 行业和概念分类数据

---

**📝 说明**: 
- ✅ 表示已下载完成
- ❌ 表示下载失败，待修复  
- ⏳ 表示计划中，待下载
- 🔧 表示需要技术修复

**📊 当前数据覆盖**: 2024年1-8月，前100只A股测试数据