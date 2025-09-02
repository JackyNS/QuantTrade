# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a quantitative trading framework written in Python that integrates data acquisition, strategy development, backtesting, and risk management. The framework supports multiple data sources (uqer, Tushare, Yahoo Finance, AKShare) and implements both technical analysis and machine learning strategies.

## Core Architecture

The framework follows a modular architecture with clear separation of concerns:

- `core/` - Main framework modules
  - `data/` - Data acquisition, processing, and feature engineering
  - `strategy/` - Strategy implementations (technical, ML, factor-based)
  - `backtest/` - Backtesting engine and performance analysis
  - `visualization/` - Charts, reports, and dashboard
  - `screening/` - Stock screening and filtering
  - `config/` - Configuration management
  - `utils/` - Shared utilities and helpers
- `scripts/` - Legacy execution scripts
- `scripts_new/` - New modular scripts for analysis, reporting, monitoring
- `main.py` - Primary entry point for the framework

## Development Commands

### Environment Setup
```bash
# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # Linux/macOS

# Install dependencies
pip install -r requirements.txt

# Development mode installation
pip install -e .
```

### Testing
```bash
# Run all tests
pytest

# Run specific module tests
pytest tests/test_data.py

# Generate coverage report
pytest --cov=core --cov-report=html

# Run performance tests
pytest tests/test_performance.py -v
```

### Code Quality
```bash
# Format code
black .

# Lint code
flake8

# Type checking
mypy core/

# Sort imports
isort .
```

### Running the Framework
```bash
# Validate environment
python main.py validate

# Update data
python main.py update-data --start=2023-01-01 --end=2024-08-26

# Run backtest
python main.py backtest --strategy=ml_strategy --start=2023-01-01

# Start web interface
python main.py web --port=8080

# Live trading (dry run)
python main.py live --strategy=ml_strategy --dry-run
```

## Key Design Patterns

### Strategy Framework
- All strategies inherit from `BaseStrategy` in `core/strategy/base_strategy.py`
- Strategy implementations are modular and pluggable
- Support for technical analysis, machine learning, and factor-based strategies

### Data Management
- Unified data interface through `DataManager` in `core/data/data_manager.py`
- Multiple data source support with consistent API
- Automatic caching and data quality validation
- Feature engineering pipeline in `core/data/feature_engineer.py`

### Backtesting Engine
- Event-driven backtesting architecture in `core/backtest/backtest_engine.py`
- Comprehensive performance analysis and risk metrics
- Modular risk management system

### Configuration System
- Centralized configuration in `core/config/settings.py`
- Environment variable support through `.env` files
- Separate configs for trading, database, and framework settings

## Important Notes

- The framework requires Python 3.8+ (3.10+ recommended)
- TA-Lib must be installed separately for technical indicators
- API tokens required for data sources (UQER_TOKEN, TUSHARE_TOKEN)
- All file paths use absolute paths for consistency
- The codebase includes both legacy (`scripts/`) and new (`scripts_new/`) script implementations
- Web dashboard available through main.py or direct Dash/Streamlit interfaces

## Data Sources and APIs

The framework integrates multiple data sources:
- uqer (优矿) - Chinese stock data (requires registration)
- Tushare - Chinese market data (free with limitations)
- Yahoo Finance - Global stock data (free)
- AKShare - Comprehensive Chinese market data (free)

## Testing Strategy

- Unit tests for core modules in `tests/`
- Performance tests for backtesting engine
- Integration tests for data pipelines
- Mock data support for testing without API access