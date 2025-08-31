量化交易框架 🚀
    
专业的Python量化交易框架，集成数据获取、策略开发、回测分析、风险管理于一体的完整解决方案。
✨ 核心特性
📊 统一数据接口
	•	🔗 集成优矿、Tushare、Yahoo Finance等多种数据源
	•	🚀 高效的数据缓存和管理机制
	•	🛡️ 完善的数据质量检查和清洗
	•	📈 丰富的技术指标和因子计算
🧠 智能策略框架
	•	🎯 模块化策略设计，支持多种策略类型
	•	🤖 机器学习策略：LightGBM、XGBoost、CatBoost
	•	📊 技术分析策略：移动平均、RSI、MACD等
	•	🔬 因子策略：多因子模型、IC分析、因子正交化
⚡ 高性能回测引擎
	•	🔄 事件驱动的回测架构
	•	📊 详细的绩效分析和风险指标
	•	🎨 可视化的回测报告
	•	🛡️ 完善的风险管理机制
🎨 交互式可视化
	•	📈 丰富的图表展示
	•	🌐 Web仪表板界面
	•	📊 实时策略监控
	•	📋 专业的研究报告
🔧 开发友好
	•	📓 Jupyter Notebook集成
	•	🧪 完整的单元测试
	•	📚 详细的API文档
	•	🔄 持续集成支持
🏗️ 架构设计
量化交易框架/
├── 📁 core/                    # 核心库
│   ├── config/                 # 配置管理
│   ├── data/                   # 数据模块
│   ├── strategy/               # 策略框架
│   ├── backtest/              # 回测引擎
│   ├── visualization/         # 可视化
│   └── utils/                 # 工具函数
├── 📁 notebooks/              # Jupyter笔记本
├── 📁 scripts/                # 执行脚本
├── 📁 tests/                  # 测试文件
├── 📁 docs/                   # 文档
├── main.py                    # 主程序入口
├── requirements.txt           # 依赖包
└── setup.py                   # 安装配置
🚀 快速开始
1. 环境要求
# Python版本要求
Python >= 3.8 (建议3.10+)

# 系统要求
Windows 10+ / macOS 10.15+ / Ubuntu 18.04+
2. 安装框架
方式一：从源码安装（推荐开发者）
# 克隆仓库
git clone https://github.com/your-username/quant-trading-framework.git
cd quant-trading-framework

# 创建虚拟环境
python -m venv quant_env
source quant_env/bin/activate  # Linux/macOS
# quant_env\Scripts\activate   # Windows

# 安装依赖
pip install -r requirements.txt

# 开发模式安装
pip install -e .
方式二：pip安装
# 基础安装
pip install quant-trading-framework

# 完整功能安装
pip install quant-trading-framework[all]

# 按需安装
pip install quant-trading-framework[ml,viz,web]
3. 安装TA-Lib（技术指标库）
# macOS (使用Homebrew)
brew install ta-lib
pip install TA-Lib

# Ubuntu/Debian
sudo apt-get install libta-lib-dev
pip install TA-Lib

# Windows
# 下载预编译wheel: https://www.lfd.uci.edu/~gohlke/pythonlibs/#ta-lib
pip install TA_Lib-0.4.28-cp310-cp310-win_amd64.whl
4. 配置API密钥
# 复制环境变量模板
cp .env.example .env

# 编辑.env文件，添加API密钥
UQER_TOKEN=your_uqer_token_here
TUSHARE_TOKEN=your_tushare_token_here
5. 验证安装
# 验证框架环境
python main.py validate

# 或使用命令行工具
quant-trading validate
💡 使用示例
基础数据获取
from core.data import create_data_manager

# 创建数据管理器
dm = create_data_manager()

# 获取股票数据
data = dm.get_price_data(
    symbols=['000001.SZ', '600000.SH'],
    start_date='2023-01-01',
    end_date='2024-08-26'
)

print(data.head())
策略开发
from core.strategy import BaseStrategy

class SimpleMAStrategy(BaseStrategy):
    """简单移动平均策略"""
    
    def __init__(self, short_window=5, long_window=20):
        super().__init__()
        self.short_window = short_window
        self.long_window = long_window
    
    def generate_signals(self, data):
        """生成交易信号"""
        data['ma_short'] = data['close'].rolling(self.short_window).mean()
        data['ma_long'] = data['close'].rolling(self.long_window).mean()
        
        # 金叉买入，死叉卖出
        data['signal'] = 0
        data.loc[data['ma_short'] > data['ma_long'], 'signal'] = 1
        data.loc[data['ma_short'] < data['ma_long'], 'signal'] = -1
        
        return data

# 使用策略
strategy = SimpleMAStrategy(short_window=5, long_window=20)
策略回测
from core.backtest import BacktestEngine

# 创建回测引擎
engine = BacktestEngine(
    initial_capital=1000000,  # 初始资金100万
    commission=0.002,         # 手续费0.2%
    benchmark='000300.SH'     # 基准：沪深300
)

# 运行回测
results = engine.run_backtest(
    strategy=strategy,
    start_date='2023-01-01',
    end_date='2024-08-26'
)

# 查看结果
print("回测结果:")
print(f"总收益率: {results['total_return']:.2%}")
print(f"年化收益率: {results['annual_return']:.2%}")
print(f"最大回撤: {results['max_drawdown']:.2%}")
print(f"夏普比率: {results['sharpe_ratio']:.2f}")
机器学习策略
from core.strategy import MLStrategy
from core.data import FeatureEngineer

# 特征工程
engineer = FeatureEngineer()
features = engineer.generate_all_features(data)

# ML策略
ml_strategy = MLStrategy(
    model_type='lightgbm',
    features=features.columns[:-1],  # 除目标变量外的所有特征
    target='future_return',
    lookback_period=252  # 训练窗口
)

# 训练和预测
ml_strategy.fit(features)
predictions = ml_strategy.predict(features)
Web界面启动
# 启动Web仪表板
python main.py web --port=8080

# 或使用命令行工具
quant-trading web --port=8080
访问 http://localhost:8080 查看Web界面。
📋 命令行工具
框架提供了丰富的命令行工具：
# 验证环境
quant-trading validate

# 更新数据
quant-trading update-data --start=2023-01-01 --end=2024-08-26

# 运行回测
quant-trading backtest --strategy=ml_strategy --start=2023-01-01

# 启动Web界面
quant-trading web --port=8080

# 模拟交易
quant-trading live --strategy=ml_strategy --dry-run

# 查看帮助
quant-trading --help
📊 支持的数据源
数据源
类型
覆盖范围
API限制
优矿(uqer)
股票、基金、期货
A股全市场
需注册
Tushare
股票、指数
A股全市场
免费有限制
Yahoo Finance
全球股票
全球市场
免费
AKShare
综合
A股、美股等
免费
🧠 支持的策略类型
技术分析策略
	•	📈 移动平均策略（MA、EMA）
	•	📊 相对强弱指标（RSI）
	•	🔄 MACD策略
	•	📊 布林带策略
	•	🎯 均值回归策略
机器学习策略
	•	🤖 梯度提升模型（LightGBM、XGBoost、CatBoost）
	•	🧠 神经网络策略（MLP、LSTM）
	•	🎯 支持向量机（SVM）
	•	🔄 随机森林（RandomForest）
	•	📊 逻辑回归策略
因子策略
	•	📊 多因子模型
	•	🔄 因子IC分析
	•	📈 因子正交化
	•	🎯 因子择时
	•	📊 行业轮动
📈 回测分析指标
收益指标
	•	总收益率、年化收益率
	•	月度、年度收益分布
	•	滚动收益率分析
	•	基准超额收益
风险指标
	•	最大回撤、平均回撤
	•	波动率、下行波动率
	•	VaR、CVaR风险度量
	•	贝塔、阿尔法系数
综合指标
	•	夏普比率、索提诺比率
	•	卡尔马比率、信息比率
	•	胜率、盈亏比
	•	换手率、交易成本
🎨 可视化功能
图表类型
	•	📈 净值曲线图
	•	📊 收益分布直方图
	•	🎯 回撤分析图
	•	📊 月度收益热力图
	•	🔄 滚动相关性图
Web仪表板
	•	📈 实时策略监控
	•	📊 组合分析界面
	•	🎯 风险管理面板
	•	📋 交易记录查看
	•	📊 绩效分析报告
🔧 配置管理
配置文件结构
# config/settings.py
class Config:
    # 时间设置
    START_DATE = '2020-01-01'
    END_DATE = '2024-08-26'
    
    # 资金设置
    INITIAL_CAPITAL = 1000000
    MAX_POSITION_SIZE = 0.05
    
    # 股票池设置
    UNIVERSE = 'CSI300'
    TOP_K_STOCKS = 30
    
    # 数据设置
    DATA_DIR = './data'
    CACHE_DIR = './data/cache'
环境变量
# .env文件
UQER_TOKEN=your_token
TUSHARE_TOKEN=your_token
LOG_LEVEL=INFO
ENVIRONMENT=development
DATABASE_URL=sqlite:///./data/trading.db
🧪 测试
# 运行所有测试
pytest

# 运行特定模块测试
pytest tests/test_data.py

# 生成测试覆盖率报告
pytest --cov=core --cov-report=html

# 运行性能测试
pytest tests/test_performance.py -v
📚 文档
	•	📖 用户指南
	•	🔧 开发指南
	•	📊 API参考
	•	🚀 部署指南
	•	📝 更新日志
🤝 贡献
我们欢迎任何形式的贡献！
贡献方式
	1	🐛 报告Bug
	2	💡 提出新功能建议
	3	📝 改进文档
	4	🔧 提交代码补丁
	5	⭐ 给项目加星
开发流程
# 1. Fork项目
# 2. 创建特性分支
git checkout -b feature/amazing-feature

# 3. 提交更改
git commit -m 'Add amazing feature'

# 4. 推送到分支
git push origin feature/amazing-feature

# 5. 创建Pull Request
代码规范
	•	使用Black进行代码格式化
	•	遵循PEP 8编码规范
	•	添加完整的单元测试
	•	更新相关文档
📄 许可证
本项目采用 MIT许可证。
🙏 致谢
感谢以下开源项目的支持：
	•	Pandas - 数据处理
	•	NumPy - 数值计算
	•	Scikit-learn - 机器学习
	•	Plotly - 交互式可视化
	•	TA-Lib - 技术指标
🆘 支持
获取帮助
	•	📖 查看文档
	•	🐛 提交[Issue](https://github.com/your-username
