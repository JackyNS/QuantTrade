# 📓 QuantTrade Notebooks 导航

## 🗂️ 目录结构

### 📚 [tutorials/](tutorials/) - 教程和使用指南
功能演示、安装指南、使用示例
- `talib_installation_guide.ipynb` - TA-Lib技术指标库完整安装指南

### 🔬 [development/](development/) - 开发和测试
功能测试、调试、原型开发
- `test_backtest_engine.ipynb` - 回测引擎功能测试
- `test_visualization.ipynb` - 可视化模块测试
- `test_utils_module.ipynb` - 工具模块测试
- `test_strategy_module.ipynb` - 策略模块测试
- `test_scripts_validation.ipynb` - 脚本验证测试

### 🔍 [research/](research/) - 研究和实验
策略研究、市场分析、新想法验证
- *使用research_template.ipynb模板开始新研究*

### 📊 [analysis/](analysis/) - 数据分析和报告
定期分析、业绩报告、市场洞察
- *使用analysis_template.ipynb模板创建分析报告*

### 💡 [examples/](examples/) - 完整示例和案例
端到端示例、最佳实践展示
- *使用example_template.ipynb模板创建完整示例*

### 📦 [archive/](archive/) - 历史版本和记录
历史记录和已废弃的notebook
- `2025-08-31_scripts_new_diagnosis.ipynb` - Scripts模块诊断记录

### 📄 [_templates/](_templates/) - Notebook模板
各类notebook的标准模板
- `research_template.ipynb` - 研究笔记模板
- `tutorial_template.ipynb` - 教程文档模板  
- `development_template.ipynb` - 开发测试模板
- `analysis_template.ipynb` - 数据分析模板
- `example_template.ipynb` - 完整示例模板

## 🚀 快速开始

### 创建新的Notebook
1. **研究笔记**: 复制 `_templates/research_template.ipynb`
2. **教程文档**: 复制 `_templates/tutorial_template.ipynb`
3. **开发测试**: 复制 `_templates/development_template.ipynb`
4. **数据分析**: 复制 `_templates/analysis_template.ipynb`
5. **完整示例**: 复制 `_templates/example_template.ipynb`

### 命名规范
- **研究笔记**: `YYYY-MM-DD_研究主题.ipynb`
- **教程文档**: `功能模块_tutorial.ipynb`
- **开发测试**: `test_模块名.ipynb`
- **数据分析**: `YYYY-MM_分析主题.ipynb`
- **完整示例**: `example_用例名.ipynb`

## 📋 最佳实践

### 📝 Notebook开发规范
1. **标题和说明**: 每个notebook都要有清晰的标题和目的说明
2. **环境设置**: 统一的导入和路径设置代码
3. **代码结构**: 使用markdown分段，逻辑清晰
4. **注释文档**: 重要代码块要有详细说明
5. **运行完整性**: 确保所有代码单元格都能正常执行

### 🔧 技术要求
- Python版本: 3.8+
- 核心依赖: pandas, numpy, matplotlib, seaborn
- 项目集成: 正确导入项目模块
- 路径设置: 使用相对路径引用项目根目录

### 📊 质量标准
- ✅ 所有代码单元格可执行
- ✅ 包含足够的markdown说明
- ✅ 图表和可视化清晰美观
- ✅ 结论和总结明确
- ✅ 可复现的结果

## 🛠️ 维护和管理

### 定期维护任务
- [ ] 检查notebook执行完整性
- [ ] 更新过时的依赖和代码
- [ ] 归档不再使用的notebook
- [ ] 更新模板和最佳实践

### 版本管理
- 重要notebook要定期备份
- 使用git管理代码变更
- 大型修改前创建分支
- 保留关键版本的历史记录

## 📚 相关资源

### 项目文档
- [项目README](../README.md)
- [核心模块文档](../core/)
- [工具文档](../tools/)
- [脚本文档](../scripts/)

### 开发指南
- [代码规范](../docs/coding_standards.md)
- [测试指南](../docs/testing_guide.md)
- [部署文档](../docs/deployment.md)

---
**最后更新**: 2025-09-01  
**维护者**: QuantTrade Team
