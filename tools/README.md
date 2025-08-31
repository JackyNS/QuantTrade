# 🛠️ Tools Directory

## 📁 目录结构

### data_download/
数据下载相关工具和脚本
- 示例下载器
- 测试连接工具
- 专用下载器

### analysis/
数据分析和质量检查工具
- 数据质量检查
- 结构分析工具
- 优化工具

### testing/
现代化验证和测试工具
- 配置验证器 (config_validator.py)
- 策略验证器 (strategy_validator.py)
- 数据验证器 (data_validator.py)

## 📖 使用说明

这些工具主要用于开发和维护，不是核心业务逻辑的一部分。

### 运行工具
```bash
# 从项目根目录运行
python tools/analysis/data_quality_checker.py
python tools/data_download/simple_uqer_test.py

# 验证工具 (支持多种模式)
python tools/testing/config_validator.py [basic|advanced|environment|all]
python tools/testing/strategy_validator.py [quick|full|performance|all]  
python tools/testing/data_validator.py [basic|structure|quality|all]
```

### 注意事项
- 工具脚本可能依赖项目根目录的配置
- 建议从项目根目录运行工具脚本
- 部分工具可能需要特定的数据环境
