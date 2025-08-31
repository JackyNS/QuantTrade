# 🚀 GitHub 仓库设置指南

## 📋 **手动创建GitHub仓库**

### 步骤1: 创建仓库
1. 访问 [https://github.com/new](https://github.com/new)
2. 填写仓库信息:
   - **Repository name**: `QuantTrade`
   - **Description**: `🚀 Enterprise-grade Quantitative Trading Platform | 企业级量化交易平台`
   - **Visibility**: 选择 Public (公开) 或 Private (私有)
   - **❌ 不要勾选** "Add a README file" (我们已有README)
   - **❌ 不要勾选** "Add .gitignore" (我们已有.gitignore)
3. 点击 **Create repository**

### 步骤2: 连接本地仓库
创建仓库后，GitHub会显示设置指令。请根据您的GitHub用户名运行：

```bash
# 添加远程仓库 (替换 YOUR_USERNAME)
git remote add origin https://github.com/YOUR_USERNAME/QuantTrade.git

# 设置主分支为 main
git branch -M main

# 推送代码
git push -u origin main
```

### 步骤3: 验证上传
访问您的GitHub仓库页面，应该能看到所有文件已成功上传。

## 🔧 **GitHub CLI 方式** (可选)

如果您想使用GitHub CLI自动创建：

### 安装GitHub CLI
```bash
# macOS
brew install gh

# Windows
winget install GitHub.cli
```

### 认证和创建
```bash
# 认证GitHub账号
gh auth login

# 自动创建仓库并推送
gh repo create QuantTrade --description "🚀 Enterprise-grade Quantitative Trading Platform" --public --source=. --remote=origin --push
```

## 📊 **仓库信息**

- **名称**: QuantTrade
- **描述**: 企业级量化交易平台
- **语言**: Python
- **大小**: ~50MB (代码文件)
- **文件**: 177个文件
- **特性**: 完整的量化交易开发框架

## 🎯 **仓库亮点**

### 📁 **项目结构**
- `core/` - 核心框架 (8个模块)
- `data/optimized/` - 优化数据 (0.9GB)
- `scripts/` - 执行脚本
- `notebooks/` - 开发笔记
- `tests/` - 测试用例

### ✨ **核心功能**
- 🎯 策略开发框架
- 📊 数据管理系统  
- 📈 回测分析引擎
- 📱 可视化仪表板
- 🔍 股票筛选工具

### 📈 **数据资产**
- 26年历史数据 (2000-2025)
- 5,347只A股全覆盖
- 1,600万+交易记录
- 86%存储优化

## 🔒 **安全注意**

`.gitignore` 已配置排除：
- ✅ 大型数据文件 (CSV/原始数据)
- ✅ 缓存和临时文件
- ✅ 日志文件
- ✅ 敏感配置 (API密钥等)
- ✅ 个人数据目录

**重要**: `uqer_config.json` 目前包含在仓库中，如需隐私保护请手动移除。