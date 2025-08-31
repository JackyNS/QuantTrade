
# 优化后数据使用示例

from data_usage_guide import OptimizedDataManager

# 初始化数据管理器
dm = OptimizedDataManager()

# 1. 加载日行情数据
# 加载2024年全年数据
daily_2024 = dm.load_daily_data(2024)

# 加载2024年1月数据
daily_jan = dm.load_daily_data(2024, month=1)

# 加载指定股票的2024年数据
stocks = ['000001', '000002', '600000']
daily_selected = dm.load_daily_data(2024, stocks=stocks)

# 2. 加载其他类型数据
weekly_2024 = dm.load_weekly_data(2024)
monthly_2024 = dm.load_monthly_data(2024)
adjusted_2024 = dm.load_adjusted_data(2024)
factors_2024 = dm.load_adjustment_factors(2024)

# 3. 加载资金流向数据
stock_flow_2024 = dm.load_stock_flow(2024)
industry_flow_2024 = dm.load_industry_flow(2024)

# 4. 获取股票列表
stocks_2024 = dm.get_stock_list(2024)
print(f"2024年股票数量: {len(stocks_2024)}")

# 5. 查看数据概览
summary = dm.get_data_summary()
print("数据概览:", summary)

# 6. 数据分析示例
if not daily_2024.empty:
    print("2024年日行情数据统计:")
    print(f"  记录总数: {len(daily_2024):,}")
    print(f"  股票数量: {daily_2024['ticker'].nunique()}")
    print(f"  日期范围: {daily_2024['tradeDate'].min()} ~ {daily_2024['tradeDate'].max()}")
    
    # 计算基础指标
    if 'closePrice' in daily_2024.columns:
        daily_2024['return'] = daily_2024.groupby('ticker')['closePrice'].pct_change()
        print(f"  平均日收益率: {daily_2024['return'].mean():.4f}")
