# 优矿数据下载配置指南

## 📋 前置条件

### 1. 注册优矿账号
- 访问：https://uqer.datayes.com/
- 注册账号并获取API Token
- 记录您的Token（格式类似：`1234567890abcdef...`）

### 2. 安装依赖
```bash
pip install uqer
```

## 🔧 配置方法

### 方法一：环境变量配置（推荐）
```bash
export UQER_TOKEN="your_token_here"
```

### 方法二：配置文件
创建 `config/uqer_config.json`:
```json
{
    "token": "your_token_here",
    "rate_limit": 0.1,
    "retry_times": 3
}
```

## 🚀 使用说明

配置完成后，系统将自动：
1. 下载全部A股历史数据
2. 每日更新增量数据
3. 自动重试失败的下载
4. 数据质量检查和清洗

详细使用见下方脚本。