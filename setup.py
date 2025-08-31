#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
量化交易框架安装配置文件
=======================

使用方法:
    pip install -e .                    # 开发模式安装
    pip install .                       # 正常安装
    python setup.py sdist bdist_wheel   # 构建分发包
    python setup.py develop             # 开发模式安装

作者：量化交易框架团队
版本：1.0.0
"""

import os
import sys
from pathlib import Path
from setuptools import setup, find_packages

# 确保Python版本
if sys.version_info < (3, 8):
    sys.exit('错误: 需要Python 3.8或更高版本')

# 项目根目录
HERE = Path(__file__).parent.absolute()

# 读取长描述
def read_long_description():
    """读取README作为长描述"""
    readme_path = HERE / "README.md"
    if readme_path.exists():
        with open(readme_path, "r", encoding="utf-8") as f:
            return f.read()
    return "量化交易框架 - 专业的Python量化投资解决方案"

# 读取版本信息
def get_version():
    """从__init__.py获取版本信息"""
    version_file = HERE / "core" / "__init__.py"
    if version_file.exists():
        with open(version_file, "r", encoding="utf-8") as f:
            for line in f:
                if line.startswith("__version__"):
                    return line.split("=")[1].strip().strip('"\'')
    return "1.0.0"

# 读取requirements
def read_requirements(filename="requirements.txt"):
    """读取依赖列表"""
    req_path = HERE / filename
    if not req_path.exists():
        return []
    
    requirements = []
    with open(req_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            # 跳过注释和空行
            if line and not line.startswith("#") and not line.startswith("--"):
                # 处理内置模块和特殊情况
                if not any(skip in line.lower() for skip in [
                    "sqlite3", "asyncio", "concurrent.futures", "configparser",
                    "loguru", "multiprocessing", "threading", "queue", 
                    "pathlib", "os", "sys", "argparse", "subprocess", 
                    "tempfile", "shutil", "smtplib", "email", "hashlib"
                ]):
                    requirements.append(line)
    
    return requirements

# 定义依赖组
def get_extras_require():
    """定义可选依赖组"""
    return {
        # 基础功能组
        "basic": [
            "pandas>=2.0.0",
            "numpy>=1.24.0",
            "scipy>=1.10.0",
            "python-dateutil>=2.8.2",
            "pytz>=2023.3"
        ],
        
        # 数据源组
        "data": [
            "uqer>=3.1.0",
            "tushare>=1.2.89",
            "yfinance>=0.2.0",
            "akshare>=1.12.0",
            "requests>=2.31.0"
        ],
        
        # 技术分析组
        "technical": [
            "TA-Lib>=0.4.28",
            "ta>=0.10.2"
        ],
        
        # 机器学习组
        "ml": [
            "scikit-learn>=1.3.0",
            "lightgbm>=4.0.0",
            "xgboost>=1.7.0",
            "catboost>=1.2.0"
        ],
        
        # 深度学习组
        "dl": [
            "torch>=2.0.0",
            "tensorflow>=2.13.0"
        ],
        
        # 数据库组
        "database": [
            "SQLAlchemy>=2.0.0",
            "pymongo>=4.5.0",
            "redis>=4.6.0"
        ],
        
        # 可视化组
        "viz": [
            "matplotlib>=3.7.0",
            "seaborn>=0.12.0",
            "plotly>=5.15.0",
            "bokeh>=3.2.0"
        ],
        
        # Web界面组
        "web": [
            "dash>=2.14.0",
            "dash-bootstrap-components>=1.4.2",
            "streamlit>=1.25.0",
            "flask>=2.3.0"
        ],
        
        # 异步处理组
        "async": [
            "aiohttp>=3.8.5",
            "celery>=5.3.0"
        ],
        
        # 测试组
        "test": [
            "pytest>=7.4.0",
            "pytest-cov>=4.1.0",
            "pytest-mock>=3.11.1",
            "pytest-asyncio>=0.21.1"
        ],
        
        # 开发工具组
        "dev": [
            "black>=23.7.0",
            "flake8>=6.0.0",
            "mypy>=1.5.0",
            "isort>=5.12.0",
            "pre-commit>=3.3.3"
        ],
        
        # Jupyter组
        "jupyter": [
            "jupyter>=1.0.0",
            "jupyterlab>=4.0.0",
            "notebook>=7.0.0",
            "ipykernel>=6.25.0",
            "ipywidgets>=8.1.0"
        ],
        
        # 文档组
        "docs": [
            "sphinx>=7.1.0",
            "sphinx-rtd-theme>=1.3.0"
        ],
        
        # 监控组
        "monitor": [
            "psutil>=5.9.0",
            "prometheus-client>=0.17.1",
            "sentry-sdk>=1.32.0"
        ],
        
        # 完整安装
        "all": [
            # 这里会包含所有上述依赖
        ]
    }

# 获取完整依赖列表（用于all组）
def get_all_dependencies():
    """获取所有依赖"""
    extras = get_extras_require()
    all_deps = set()
    for deps in extras.values():
        if isinstance(deps, list):
            all_deps.update(deps)
    return list(all_deps)

# 更新all组
extras_require = get_extras_require()
extras_require["all"] = get_all_dependencies()

# 项目元信息
DESCRIPTION = "专业的Python量化交易框架"
LONG_DESCRIPTION = read_long_description()
VERSION = get_version()

# 分类器
CLASSIFIERS = [
    "Development Status :: 4 - Beta",
    "Intended Audience :: Financial and Insurance Industry",
    "Intended Audience :: Developers",
    "License :: OSI Approved :: MIT License",
    "Operating System :: OS Independent",
    "Programming Language :: Python",
    "Programming Language :: Python :: 3",
    "Programming Language :: Python :: 3.8",
    "Programming Language :: Python :: 3.9",
    "Programming Language :: Python :: 3.10",
    "Programming Language :: Python :: 3.11",
    "Programming Language :: Python :: 3.12",
    "Topic :: Office/Business :: Financial :: Investment",
    "Topic :: Scientific/Engineering :: Artificial Intelligence",
    "Topic :: Software Development :: Libraries :: Python Modules",
]

# 关键词
KEYWORDS = [
    "quantitative-trading", "algorithmic-trading", "backtesting",
    "machine-learning", "finance", "investment", "portfolio-optimization",
    "technical-analysis", "factor-model", "risk-management"
]

# 项目URL
PROJECT_URLS = {
    "Bug Reports": "https://github.com/your-username/quant-trading-framework/issues",
    "Source": "https://github.com/your-username/quant-trading-framework",
    "Documentation": "https://quant-trading-framework.readthedocs.io/",
}

# 控制台脚本
ENTRY_POINTS = {
    "console_scripts": [
        "quant-trading=main:main",
        "qt=main:main",
    ],
}

# 包数据
PACKAGE_DATA = {
    "core": [
        "config/*.yaml",
        "config/*.json",
        "templates/*.html",
        "static/*.*"
    ],
    "": [
        "README.md",
        "LICENSE",
        "CHANGELOG.md",
        "requirements*.txt"
    ]
}

# 数据文件
DATA_FILES = [
    ("config", ["config/default_config.yaml"]),
    ("templates", ["templates/report_template.html"]),
]

# 主要安装配置
setup(
    # 基本信息
    name="quant-trading-framework",
    version=VERSION,
    description=DESCRIPTION,
    long_description=LONG_DESCRIPTION,
    long_description_content_type="text/markdown",
    
    # 作者信息
    author="量化交易框架团队",
    author_email="dev@quant-framework.com",
    maintainer="量化交易框架团队",
    maintainer_email="dev@quant-framework.com",
    
    # 项目信息
    url="https://github.com/your-username/quant-trading-framework",
    project_urls=PROJECT_URLS,
    license="MIT",
    
    # 分类和关键词
    classifiers=CLASSIFIERS,
    keywords=" ".join(KEYWORDS),
    
    # Python版本要求
    python_requires=">=3.8",
    
    # 包发现
    packages=find_packages(
        exclude=["tests*", "docs*", "examples*", "notebooks*"]
    ),
    
    # 包数据
    package_data=PACKAGE_DATA,
    data_files=DATA_FILES,
    include_package_data=True,
    
    # 依赖
    install_requires=read_requirements("requirements.txt")[:20],  # 核心依赖
    extras_require=extras_require,
    
    # 控制台脚本
    entry_points=ENTRY_POINTS,
    
    # Zip安全
    zip_safe=False,
    
    # 测试配置
    test_suite="tests",
    tests_require=extras_require["test"],
    
    # 其他选项
    platforms=["any"],
    
    # 开发状态
    # development_status="4 - Beta"
)

# 安装后处理
def post_install():
    """安装后处理"""
    print("=" * 60)
    print("🎉 量化交易框架安装完成!")
    print("=" * 60)
    print()
    print("📋 快速开始:")
    print("  1. 验证安装: quant-trading validate")
    print("  2. 查看帮助: quant-trading --help")
    print("  3. 启动Web界面: quant-trading web")
    print()
    print("📚 文档地址: https://quant-trading-framework.readthedocs.io/")
    print("🐛 问题反馈: https://github.com/your-username/quant-trading-framework/issues")
    print()
    print("⚠️  重要提醒:")
    print("  - 请先安装TA-Lib系统依赖")
    print("  - 配置优矿API Token: export UQER_TOKEN=your_token")
    print("  - 首次使用请运行: quant-trading validate")
    print()
    print("🚀 开始您的量化交易之旅!")
    print("=" * 60)

if __name__ == "__main__":
    # 如果直接运行此文件，显示安装后信息
    post_install()