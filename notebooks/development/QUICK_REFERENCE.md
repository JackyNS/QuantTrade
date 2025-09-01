# ⚡ 技术指标开发快速参考

## 🚀 快速命令

### 创建新指标研发环境
```bash
# 命令行创建
python tools/indicator_generator.py --create "ADX" --category trend --complexity medium

# Python代码创建  
from tools.indicator_generator import IndicatorGenerator
gen = IndicatorGenerator()
gen.create_indicator_research("Stochastic RSI", "momentum", "medium")
```

### 查看开发进度
```bash
python tools/indicator_generator.py --list
```

---

## 📋 指标模板代码

### 基础指标模板
```python
def new_indicator(data: pd.Series, period: int = 14) -> pd.Series:
    """
    指标名称
    
    Args:
        data: 收盘价序列
        period: 计算周期
        
    Returns:
        pd.Series: 指标值
    """
    # 参数验证
    if period <= 0:
        raise ValueError("period must be positive")
    
    # 核心计算
    result = data.rolling(window=period).mean()
    
    return result
```

### 多输入指标模板
```python
def advanced_indicator(high: pd.Series, low: pd.Series, close: pd.Series, 
                      period: int = 14) -> pd.DataFrame:
    """多输入多输出指标"""
    
    # 计算中间值
    tp = (high + low + close) / 3
    
    # 返回多个结果
    return pd.DataFrame({
        'main': tp.rolling(period).mean(),
        'upper': tp.rolling(period).mean() + tp.rolling(period).std(),
        'lower': tp.rolling(period).mean() - tp.rolling(period).std()
    })
```

---

## 🧪 常用测试代码

### 快速测试
```python
# 创建测试数据
test_data = create_synthetic_data(252)

# 测试指标
result = new_indicator(test_data['close'])
print(f"✅ 指标计算成功: {result.shape}")

# 基本统计
print(result.describe())
```

### 性能测试
```python
import time

# 性能基准
times = []
for _ in range(100):
    start = time.time()
    new_indicator(test_data['close'])
    times.append(time.time() - start)

avg_time = np.mean(times)
print(f"平均执行时间: {avg_time:.4f}秒")
```

---

## 📊 常用计算公式

### 移动平均类
```python
# 简单移动平均
sma = data.rolling(window=period).mean()

# 指数移动平均  
ema = data.ewm(span=period).mean()

# 加权移动平均
weights = np.arange(1, period + 1)
wma = data.rolling(window=period).apply(lambda x: np.dot(x, weights) / weights.sum())
```

### 动量指标类
```python
# RSI
delta = data.diff()
gain = delta.where(delta > 0, 0).rolling(period).mean()
loss = -delta.where(delta < 0, 0).rolling(period).mean() 
rsi = 100 - (100 / (1 + gain / loss))

# 随机指标K值
lowest = low.rolling(period).min()
highest = high.rolling(period).max()
k = 100 * (close - lowest) / (highest - lowest)
```

### 波动率指标类
```python
# 真实波幅
tr1 = high - low
tr2 = abs(high - close.shift())
tr3 = abs(low - close.shift())
tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

# ATR
atr = tr.rolling(period).mean()

# 布林带
sma = close.rolling(period).mean()
std = close.rolling(period).std()
bb_upper = sma + (std * multiplier)
bb_lower = sma - (std * multiplier)
```

---

## 🎨 可视化代码片段

### 单指标图表
```python
fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8), sharex=True)

# 价格图
ax1.plot(data.index, data['close'], label='Close', linewidth=1)
ax1.set_title('Price Chart')
ax1.legend()

# 指标图
ax2.plot(data.index, indicator_result, label='Indicator', color='red')
ax2.set_title('Indicator')
ax2.legend()

plt.tight_layout()
plt.show()
```

### 双轴图表
```python
fig, ax1 = plt.subplots(figsize=(12, 6))
ax2 = ax1.twinx()

# 主轴 - 价格
ax1.plot(data.index, data['close'], 'b-', label='Price')
ax1.set_ylabel('Price', color='b')

# 副轴 - 指标
ax2.plot(data.index, indicator_result, 'r-', label='Indicator') 
ax2.set_ylabel('Indicator', color='r')

plt.title('Price vs Indicator')
plt.show()
```

---

## 🔧 实用工具函数

### 数据预处理
```python
def clean_data(data: pd.Series) -> pd.Series:
    """清理数据"""
    return data.dropna().replace([np.inf, -np.inf], np.nan).dropna()

def normalize_series(data: pd.Series, method='minmax') -> pd.Series:
    """标准化序列"""
    if method == 'minmax':
        return (data - data.min()) / (data.max() - data.min())
    elif method == 'zscore':
        return (data - data.mean()) / data.std()
    else:
        raise ValueError("method must be 'minmax' or 'zscore'")
```

### 信号处理
```python
def generate_signals(indicator: pd.Series, buy_threshold: float, 
                    sell_threshold: float) -> pd.Series:
    """生成交易信号"""
    signals = pd.Series(0, index=indicator.index)
    signals[indicator > buy_threshold] = 1   # 买入信号
    signals[indicator < sell_threshold] = -1  # 卖出信号
    return signals

def crossover(series1: pd.Series, series2: pd.Series) -> pd.Series:
    """检测金叉信号"""
    return (series1 > series2) & (series1.shift(1) <= series2.shift(1))

def crossunder(series1: pd.Series, series2: pd.Series) -> pd.Series:
    """检测死叉信号"""  
    return (series1 < series2) & (series1.shift(1) >= series2.shift(1))
```

---

## 📈 经典指标实现

### MACD (12行代码)
```python
def macd(close, fast=12, slow=26, signal=9):
    ema_fast = close.ewm(span=fast).mean()
    ema_slow = close.ewm(span=slow).mean()
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal).mean()
    histogram = macd_line - signal_line
    return pd.DataFrame({
        'macd': macd_line,
        'signal': signal_line, 
        'histogram': histogram
    })
```

### RSI (8行代码)
```python
def rsi(close, period=14):
    delta = close.diff()
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)
    avg_gain = gain.rolling(window=period).mean()
    avg_loss = loss.rolling(window=period).mean()
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))
```

### 布林带 (6行代码)
```python
def bollinger_bands(close, period=20, std_dev=2):
    sma = close.rolling(period).mean()
    std = close.rolling(period).std()
    return pd.DataFrame({
        'upper': sma + (std * std_dev),
        'middle': sma,
        'lower': sma - (std * std_dev)
    })
```

---

## ⚙️ 调试技巧

### 常见错误检查
```python
# 检查数据类型
assert isinstance(data, pd.Series), "Input must be pandas Series"

# 检查数据长度
assert len(data) >= period, f"Data length {len(data)} < period {period}"

# 检查数值有效性
assert not data.isna().all(), "All data is NaN"
assert not np.isinf(data).any(), "Data contains infinity values"

# 检查参数范围
assert 0 < period <= len(data), f"Invalid period: {period}"
```

### 性能优化检查
```python
# 避免循环，使用向量化
# ❌ 慢
result = pd.Series(index=data.index)
for i in range(len(data)):
    result.iloc[i] = data.iloc[max(0, i-period+1):i+1].mean()

# ✅ 快
result = data.rolling(window=period).mean()
```

---

## 📋 指标分类速查

| 类别 | 典型指标 | 主要用途 | 计算基础 |
|-----|---------|----------|----------|
| **趋势** | SMA, EMA, MACD | 方向识别 | 价格平滑 |
| **动量** | RSI, Stochastic | 超买超卖 | 价格变化率 |
| **波动** | ATR, Bollinger | 风险测量 | 价格分散度 |
| **成交量** | OBV, MFI | 资金流向 | 量价关系 |

---

## 🏃‍♂️ 开发速度提升

### 代码片段快捷键
- `ind` → 基础指标模板
- `test` → 快速测试代码
- `plot` → 基础图表代码
- `valid` → 验证测试代码

### 常用 Jupyter 快捷键
- `Shift + Enter` → 运行当前单元格
- `Ctrl + /` → 注释/取消注释
- `Tab` → 代码自动完成
- `Shift + Tab` → 查看函数文档

---

**更新时间**: 2025-09-01  
**版本**: v1.0  
**适用范围**: QuantTrade 技术指标开发

*💡 提示: 将此文档加入书签，开发时随时查阅*