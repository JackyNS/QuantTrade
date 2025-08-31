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

## 📖 使用说明

这些工具主要用于开发和维护，不是核心业务逻辑的一部分。

### 运行工具
```bash
# 从项目根目录运行
python tools/analysis/data_quality_checker.py
python tools/data_download/simple_uqer_test.py
```

### 注意事项
- 工具脚本可能依赖项目根目录的配置
- 建议从项目根目录运行工具脚本
- 部分工具可能需要特定的数据环境
