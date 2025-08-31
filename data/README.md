# 数据目录说明

## 目录结构

- **raw/**: 原始数据，从API下载的未处理数据
  - **daily/**: 股票日线数据 (parquet格式)
  - **minute/**: 分钟级别数据
  - **financial/**: 财务数据
  - **basic_info/**: 股票基本信息
  - **index/**: 指数数据

- **processed/**: 处理后的数据
  - **features/**: 技术指标和特征
  - **factors/**: 因子数据
  - **signals/**: 交易信号

- **cache/**: 临时缓存文件
  - **api/**: API请求缓存
  - **features/**: 特征计算缓存
  - **models/**: 模型缓存

- **metadata/**: 元数据和配置
  - 股票列表、交易日历等

- **backtest/**: 回测相关数据
  - **results/**: 回测结果
  - **reports/**: 回测报告

## 数据格式

- 日线数据: Parquet格式（推荐）或CSV
- 分钟数据: Parquet格式（按日期分区）
- 财务数据: Parquet格式

## 命名规范

- 股票数据: `{股票代码}.parquet` (如: 000001.parquet)
- 分钟数据: `{股票代码}_{日期}.parquet`
- 财务数据: `{股票代码}_financial.parquet`

## 更新时间

创建时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
