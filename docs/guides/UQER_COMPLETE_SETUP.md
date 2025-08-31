# 🎯 优矿数据完整配置指南

## 📋 系统已为您准备就绪

### ✅ 已创建的文件

1. **📖 配置指南**: `uqer_setup_guide.md`
2. **📥 全量下载**: `download_uqer_data.py`
3. **🔄 每日更新**: `daily_update_uqer.py`
4. **⚙️ 定时任务**: `setup_scheduler.py`
5. **🧪 连接测试**: `test_uqer_connection.py`

### 🚀 使用流程

#### 第一步：获取优矿API Token
1. 访问：https://uqer.datayes.com/
2. 注册账号并获取API Token
3. 记录您的Token

#### 第二步：配置Token
选择以下任一方式：

**方法A - 环境变量（推荐）**
```bash
export UQER_TOKEN="your_token_here"
```

**方法B - 配置文件**
创建 `uqer_config.json`:
```json
{
    "token": "your_token_here"
}
```

#### 第三步：测试连接
```bash
python test_uqer_connection.py
```

#### 第四步：下载数据
```bash
# 全量下载（首次使用）
python download_uqer_data.py

# 每日更新（日常使用）
python daily_update_uqer.py
```

#### 第五步：配置定时任务
```bash
python setup_scheduler.py
```

## 🎨 功能特色

### 📥 全量数据下载
- **智能批处理**: 自动分批下载，避免API限制
- **断点续传**: 支持中断后继续下载
- **自动重试**: 失败的股票自动重试
- **质量检查**: 自动数据清洗和验证

### 🔄 每日自动更新  
- **增量更新**: 只下载新数据，节省时间
- **智能检测**: 自动识别需要更新的股票
- **状态报告**: 生成详细的更新报告
- **错误处理**: 完善的异常处理机制

### ⚙️ 定时任务系统
- **多平台支持**: Linux/macOS/Windows
- **灵活配置**: crontab、任务计划程序、Python服务
- **自动启动**: 开机自启动选项
- **日志记录**: 完整的执行日志

### 📊 监控和报告
- **实时日志**: 详细的执行日志
- **更新报告**: 每日更新统计报告
- **错误追踪**: 失败原因分析
- **邮件通知**: 可选的邮件通知功能

## 📁 目录结构

```
QuantTrade/
├── data/                    # 数据存储目录
├── logs/                    # 日志文件
├── reports/                 # 更新报告
├── uqer_config.json        # 配置文件（需要创建）
├── download_uqer_data.py   # 全量下载脚本
├── daily_update_uqer.py    # 每日更新脚本
├── test_uqer_connection.py # 连接测试脚本
└── setup_scheduler.py      # 定时任务配置
```

## 💡 高级配置

### 自定义下载参数
```python
config = {
    'data_dir': './data',
    'uqer': {
        'token': 'your_token',
        'rate_limit': 0.1,      # API调用间隔
        'retry_times': 3        # 重试次数
    },
    'download': {
        'batch_size': 50,       # 批处理大小
        'start_date': '2020-01-01',  # 开始日期
        'max_retry': 3          # 最大重试次数
    }
}
```

### 邮件通知配置
```python
'notification': {
    'enable_email': True,
    'email_config': {
        'smtp_server': 'smtp.example.com',
        'sender_email': 'your_email@example.com',
        'recipients': ['recipient@example.com']
    }
}
```

## 🛡️ 安全建议

1. **Token保护**: 不要将Token提交到代码仓库
2. **权限控制**: 定时任务使用最小权限运行
3. **日志安全**: 定期清理敏感信息日志
4. **网络安全**: 使用HTTPS连接

## 📞 故障排除

### 常见问题

**问题1: Token无效**
- 检查Token是否正确
- 确认账号是否有权限
- 检查Token是否过期

**问题2: 下载失败**
- 检查网络连接
- 确认API调用次数是否足够
- 查看详细错误日志

**问题3: 定时任务不执行**
- 检查系统时间
- 确认任务配置正确
- 查看系统日志

### 日志位置
- 下载日志: `logs/uqer_download.log`
- 更新日志: `logs/daily_update_YYYYMMDD.log`
- 调度日志: `logs/scheduler.log`

## 🎉 完成！

现在您已经拥有了完整的优矿数据自动化下载和更新系统！

**立即开始：**
1. 配置您的优矿Token
2. 运行 `python test_uqer_connection.py` 测试连接
3. 执行全量下载或设置定时任务

🚀 **享受自动化的量化交易数据管理体验！**