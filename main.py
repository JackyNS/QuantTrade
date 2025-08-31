#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
量化交易框架 - 主程序入口
========================

这是量化交易框架的主程序入口文件，提供统一的启动和管理界面。

主要功能：
🎯 统一的命令行入口
🔧 环境初始化和配置加载
📊 数据流水线管理
🚀 策略执行和回测
📈 结果分析和报告生成
🌐 Web界面启动（可选）

使用方法：
```bash
# 基本使用
python main.py --help

# 配置验证
python main.py validate

# 数据更新
python main.py update-data --start=2023-01-01 --end=2024-08-26

# 策略回测
python main.py backtest --strategy=ml_strategy --start=2023-01-01 --end=2024-08-26

# 启动Web界面
python main.py web --port=8080

# 实时交易（谨慎使用）
python main.py live --strategy=ml_strategy --dry-run
```

作者：量化交易框架团队
版本：1.0.0
更新：2025-08-26
"""

import sys
import os
import argparse
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Dict, Any, List
import warnings

# 禁用警告信息
warnings.filterwarnings('ignore')

# 添加项目根目录到路径
PROJECT_ROOT = Path(__file__).parent.absolute()
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# ==========================================
# 导入框架模块
# ==========================================

def import_framework_modules():
    """导入框架模块，处理可能的导入错误"""
    modules = {}
    errors = []
    
    try:
        from core.config.settings import Config
        from core.config.trading_config import TradingConfig
        from core.config.database_config import DatabaseConfig
        modules['config'] = {'Config': Config, 'TradingConfig': TradingConfig, 'DatabaseConfig': DatabaseConfig}
        print("✅ 配置模块导入成功")
    except ImportError as e:
        errors.append(f"配置模块导入失败: {e}")
        
    try:
        from core.data import create_data_manager, get_module_status
        modules['data'] = {'create_data_manager': create_data_manager, 'get_module_status': get_module_status}
        print("✅ 数据模块导入成功")
    except ImportError as e:
        errors.append(f"数据模块导入失败: {e}")
        
    try:
        from core.strategy.base_strategy import BaseStrategy
        modules['strategy'] = {'BaseStrategy': BaseStrategy}
        print("✅ 策略模块导入成功")
    except ImportError as e:
        errors.append(f"策略模块导入失败: {e}")
        
    try:
        from core.backtest.backtest_engine import BacktestEngine
        modules['backtest'] = {'BacktestEngine': BacktestEngine}
        print("✅ 回测模块导入成功")
    except ImportError as e:
        errors.append(f"回测模块导入失败: {e}")
        
    try:
        from core.visualization.dashboard import launch_dashboard
        modules['visualization'] = {'launch_dashboard': launch_dashboard}
        print("✅ 可视化模块导入成功")
    except ImportError as e:
        errors.append(f"可视化模块导入失败: {e}")
        
    try:
        from core.utils.logger import setup_logger
        from core.utils.validators import validate_date_range, validate_strategy_config
        modules['utils'] = {
            'setup_logger': setup_logger,
            'validate_date_range': validate_date_range,
            'validate_strategy_config': validate_strategy_config
        }
        print("✅ 工具模块导入成功")
    except ImportError as e:
        errors.append(f"工具模块导入失败: {e}")
    
    return modules, errors

# ==========================================
# 核心应用类
# ==========================================

class QuantTradingApp:
    """量化交易框架主应用类"""
    
    def __init__(self, config_file: Optional[str] = None):
        """初始化应用"""
        self.config_file = config_file
        self.config = None
        self.trading_config = None
        self.database_config = None
        self.data_manager = None
        self.logger = None
        self.modules = {}
        self.errors = []
        
        # 初始化应用
        self._initialize()
    
    def _initialize(self):
        """初始化应用组件"""
        print("🚀 量化交易框架启动中...")
        print("=" * 50)
        
        # 导入模块
        self.modules, self.errors = import_framework_modules()
        
        if self.errors:
            print("⚠️ 模块导入警告：")
            for error in self.errors:
                print(f"  - {error}")
            print()
        
        # 初始化配置
        self._setup_config()
        
        # 设置日志
        self._setup_logging()
        
        # 初始化数据管理器
        self._setup_data_manager()
        
        print("✅ 量化交易框架初始化完成")
        print("=" * 50)
    
    def _setup_config(self):
        """设置配置"""
        if 'config' in self.modules:
            try:
                Config = self.modules['config']['Config']
                TradingConfig = self.modules['config']['TradingConfig']
                DatabaseConfig = self.modules['config']['DatabaseConfig']
                
                self.config = Config(config_file=self.config_file)
                self.trading_config = TradingConfig()
                self.database_config = DatabaseConfig()
                
                print("✅ 配置加载成功")
            except Exception as e:
                print(f"❌ 配置加载失败: {e}")
        else:
            print("⚠️ 配置模块不可用，使用默认配置")
    
    def _setup_logging(self):
        """设置日志"""
        if 'utils' in self.modules and 'setup_logger' in self.modules['utils']:
            try:
                setup_logger = self.modules['utils']['setup_logger']
                self.logger = setup_logger(
                    name='quant_trading',
                    level=getattr(self.config, 'LOG_LEVEL', 'INFO') if self.config else 'INFO'
                )
                print("✅ 日志系统初始化成功")
            except Exception as e:
                print(f"❌ 日志系统初始化失败: {e}")
                # 使用基础日志配置
                logging.basicConfig(
                    level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
                )
                self.logger = logging.getLogger('quant_trading')
        else:
            # 基础日志配置
            logging.basicConfig(
                level=logging.INFO,
                format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            self.logger = logging.getLogger('quant_trading')
            print("⚠️ 使用基础日志配置")
    
    def _setup_data_manager(self):
        """设置数据管理器"""
        if 'data' in self.modules and 'create_data_manager' in self.modules['data']:
            try:
                create_data_manager = self.modules['data']['create_data_manager']
                self.data_manager = create_data_manager(config=self.config)
                print("✅ 数据管理器初始化成功")
            except Exception as e:
                print(f"❌ 数据管理器初始化失败: {e}")
        else:
            print("⚠️ 数据模块不可用")
    
    def validate(self) -> bool:
        """验证框架配置和环境"""
        print("🔍 开始框架验证...")
        print("=" * 40)
        
        validation_results = []
        
        # 1. 配置验证
        if self.config:
            try:
                is_valid = self.config.validate_config()
                validation_results.append(("配置验证", is_valid))
                if is_valid:
                    print("✅ 配置验证通过")
                else:
                    print("❌ 配置验证失败")
            except Exception as e:
                print(f"❌ 配置验证错误: {e}")
                validation_results.append(("配置验证", False))
        else:
            print("❌ 配置未加载")
            validation_results.append(("配置验证", False))
        
        # 2. 数据模块验证
        if 'data' in self.modules and 'get_module_status' in self.modules['data']:
            try:
                get_module_status = self.modules['data']['get_module_status']
                status = get_module_status()
                is_ready = status.get('ready', False)
                validation_results.append(("数据模块", is_ready))
                if is_ready:
                    print("✅ 数据模块验证通过")
                else:
                    print("❌ 数据模块验证失败")
                    print(f"   状态: {status}")
            except Exception as e:
                print(f"❌ 数据模块验证错误: {e}")
                validation_results.append(("数据模块", False))
        else:
            print("❌ 数据模块不可用")
            validation_results.append(("数据模块", False))
        
        # 3. 环境依赖验证
        env_check = self._check_environment()
        validation_results.append(("环境依赖", env_check))
        
        # 4. 目录结构验证
        dir_check = self._check_directories()
        validation_results.append(("目录结构", dir_check))
        
        # 输出验证结果
        print("\n📋 验证结果汇总:")
        print("-" * 30)
        passed = 0
        total = len(validation_results)
        
        for test_name, result in validation_results:
            status = "✅ 通过" if result else "❌ 失败"
            print(f"  {test_name}: {status}")
            if result:
                passed += 1
        
        success_rate = (passed / total) * 100
        print(f"\n📊 总体验证结果: {passed}/{total} ({success_rate:.1f}%)")
        
        if success_rate >= 80:
            print("🎉 框架验证成功，可以开始使用!")
            return True
        else:
            print("⚠️ 框架验证失败，请检查上述问题")
            return False
    
    def _check_environment(self) -> bool:
        """检查环境依赖"""
        required_packages = [
            'pandas', 'numpy', 'scipy'
        ]
        
        optional_packages = [
            'uqer', 'talib', 'plotly', 'dash'
        ]
        
        missing_required = []
        missing_optional = []
        
        for package in required_packages:
            try:
                __import__(package)
            except ImportError:
                missing_required.append(package)
        
        for package in optional_packages:
            try:
                __import__(package)
            except ImportError:
                missing_optional.append(package)
        
        if missing_required:
            print(f"❌ 缺少必需依赖: {', '.join(missing_required)}")
            return False
        else:
            print("✅ 必需依赖检查通过")
        
        if missing_optional:
            print(f"⚠️ 缺少可选依赖: {', '.join(missing_optional)}")
            print("   部分功能可能不可用")
        
        return len(missing_required) == 0
    
    def _check_directories(self) -> bool:
        """检查目录结构"""
        required_dirs = [
            'core', 'core/config', 'core/data', 
            'data', 'data/cache', 'data/raw', 'data/processed',
            'logs'
        ]
        
        missing_dirs = []
        for dir_name in required_dirs:
            dir_path = PROJECT_ROOT / dir_name
            if not dir_path.exists():
                missing_dirs.append(dir_name)
        
        if missing_dirs:
            print(f"❌ 缺少目录: {', '.join(missing_dirs)}")
            
            # 尝试创建缺少的目录
            try:
                for dir_name in missing_dirs:
                    dir_path = PROJECT_ROOT / dir_name
                    dir_path.mkdir(parents=True, exist_ok=True)
                print("✅ 缺少的目录已自动创建")
                return True
            except Exception as e:
                print(f"❌ 目录创建失败: {e}")
                return False
        else:
            print("✅ 目录结构检查通过")
            return True
    
    def update_data(self, start_date: str, end_date: str, symbols: Optional[List[str]] = None):
        """更新数据"""
        if not self.data_manager:
            print("❌ 数据管理器不可用")
            return False
        
        try:
            print(f"📊 更新数据: {start_date} → {end_date}")
            if symbols:
                print(f"📈 股票代码: {symbols}")
            
            # 使用数据管理器更新数据
            result = self.data_manager.update_data(
                start_date=start_date,
                end_date=end_date,
                symbols=symbols
            )
            
            if result:
                print("✅ 数据更新成功")
                return True
            else:
                print("❌ 数据更新失败")
                return False
                
        except Exception as e:
            print(f"❌ 数据更新错误: {e}")
            if self.logger:
                self.logger.error(f"数据更新错误: {e}")
            return False
    
    def run_backtest(self, strategy_name: str, start_date: str, end_date: str, **kwargs):
        """运行回测"""
        if 'backtest' not in self.modules:
            print("❌ 回测模块不可用")
            return None
        
        try:
            print(f"🔄 开始回测: {strategy_name}")
            print(f"📅 时间范围: {start_date} → {end_date}")
            
            BacktestEngine = self.modules['backtest']['BacktestEngine']
            engine = BacktestEngine(
                config=self.config,
                trading_config=self.trading_config,
                data_manager=self.data_manager
            )
            
            results = engine.run_backtest(
                strategy_name=strategy_name,
                start_date=start_date,
                end_date=end_date,
                **kwargs
            )
            
            if results:
                print("✅ 回测完成")
                return results
            else:
                print("❌ 回测失败")
                return None
                
        except Exception as e:
            print(f"❌ 回测错误: {e}")
            if self.logger:
                self.logger.error(f"回测错误: {e}")
            return None
    
    def launch_web_interface(self, port: int = 8080, host: str = '127.0.0.1'):
        """启动Web界面"""
        if 'visualization' not in self.modules:
            print("❌ 可视化模块不可用")
            return False
        
        try:
            print(f"🌐 启动Web界面: http://{host}:{port}")
            
            launch_dashboard = self.modules['visualization']['launch_dashboard']
            
            launch_dashboard(
                app=self,
                host=host,
                port=port,
                debug=getattr(self.config, 'DEBUG', False) if self.config else False
            )
            
            return True
            
        except Exception as e:
            print(f"❌ Web界面启动错误: {e}")
            if self.logger:
                self.logger.error(f"Web界面启动错误: {e}")
            return False
    
    def run_live_trading(self, strategy_name: str, dry_run: bool = True):
        """运行实时交易"""
        print("⚠️ 实时交易功能正在开发中...")
        
        if dry_run:
            print("🧪 模拟模式运行")
        else:
            print("🚨 实盘模式 - 请谨慎操作!")
            
            # 安全检查
            confirm = input("确认要进行实盘交易吗? (输入 'YES' 确认): ")
            if confirm != 'YES':
                print("❌ 实盘交易已取消")
                return False
        
        try:
            # TODO: 实现实时交易逻辑
            print("🔄 实时交易逻辑正在开发中...")
            return True
            
        except Exception as e:
            print(f"❌ 实时交易错误: {e}")
            if self.logger:
                self.logger.error(f"实时交易错误: {e}")
            return False

# ==========================================
# 命令行参数解析
# ==========================================

def create_parser() -> argparse.ArgumentParser:
    """创建命令行参数解析器"""
    parser = argparse.ArgumentParser(
        description="量化交易框架 - 统一命令行工具",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  python main.py validate                                    # 验证框架环境
  python main.py update-data --start=2023-01-01 --end=2024-08-26
  python main.py backtest --strategy=ml_strategy --start=2023-01-01
  python main.py web --port=8080                            # 启动Web界面
  python main.py live --strategy=ml_strategy --dry-run      # 模拟交易
        """
    )
    
    parser.add_argument('--config', '-c', 
                       help='配置文件路径')
    parser.add_argument('--verbose', '-v', 
                       action='store_true',
                       help='详细输出')
    
    # 子命令
    subparsers = parser.add_subparsers(dest='command', help='可用命令')
    
    # 验证命令
    validate_parser = subparsers.add_parser('validate', help='验证框架配置和环境')
    
    # 数据更新命令
    data_parser = subparsers.add_parser('update-data', help='更新数据')
    data_parser.add_argument('--start', required=True, help='开始日期 (YYYY-MM-DD)')
    data_parser.add_argument('--end', required=True, help='结束日期 (YYYY-MM-DD)')
    data_parser.add_argument('--symbols', nargs='*', help='股票代码列表')
    
    # 回测命令
    backtest_parser = subparsers.add_parser('backtest', help='运行策略回测')
    backtest_parser.add_argument('--strategy', required=True, help='策略名称')
    backtest_parser.add_argument('--start', required=True, help='开始日期 (YYYY-MM-DD)')
    backtest_parser.add_argument('--end', help='结束日期 (YYYY-MM-DD)')
    backtest_parser.add_argument('--initial-capital', type=float, help='初始资金')
    backtest_parser.add_argument('--benchmark', help='基准指数')
    
    # Web界面命令
    web_parser = subparsers.add_parser('web', help='启动Web界面')
    web_parser.add_argument('--port', type=int, default=8080, help='端口号 (默认: 8080)')
    web_parser.add_argument('--host', default='127.0.0.1', help='主机地址 (默认: 127.0.0.1)')
    
    # 实时交易命令
    live_parser = subparsers.add_parser('live', help='实时交易')
    live_parser.add_argument('--strategy', required=True, help='策略名称')
    live_parser.add_argument('--dry-run', action='store_true', help='模拟模式')
    
    return parser

# ==========================================
# 主函数
# ==========================================

def main():
    """主函数"""
    parser = create_parser()
    args = parser.parse_args()
    
    # 设置详细输出
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    
    # 创建应用实例
    try:
        app = QuantTradingApp(config_file=args.config)
    except Exception as e:
        print(f"❌ 应用初始化失败: {e}")
        sys.exit(1)
    
    # 根据命令执行相应操作
    if args.command == 'validate':
        success = app.validate()
        sys.exit(0 if success else 1)
        
    elif args.command == 'update-data':
        success = app.update_data(
            start_date=args.start,
            end_date=args.end,
            symbols=args.symbols
        )
        sys.exit(0 if success else 1)
        
    elif args.command == 'backtest':
        end_date = args.end or datetime.now().strftime('%Y-%m-%d')
        results = app.run_backtest(
            strategy_name=args.strategy,
            start_date=args.start,
            end_date=end_date,
            initial_capital=args.initial_capital,
            benchmark=args.benchmark
        )
        sys.exit(0 if results else 1)
        
    elif args.command == 'web':
        success = app.launch_web_interface(
            port=args.port,
            host=args.host
        )
        sys.exit(0 if success else 1)
        
    elif args.command == 'live':
        success = app.run_live_trading(
            strategy_name=args.strategy,
            dry_run=args.dry_run
        )
        sys.exit(0 if success else 1)
        
    else:
        # 没有指定命令，显示帮助
        parser.print_help()
        
        # 显示快速启动信息
        print("\n🚀 快速开始:")
        print("  1. 验证环境: python main.py validate")
        print("  2. 更新数据: python main.py update-data --start=2023-01-01 --end=2024-08-26")
        print("  3. 运行回测: python main.py backtest --strategy=ml_strategy --start=2023-01-01")
        print("  4. 启动Web界面: python main.py web")
        print("\n📚 详细文档请查看 README.md")

# ==========================================
# 程序入口
# ==========================================

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⏹️ 程序被用户中断")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ 程序执行错误: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)