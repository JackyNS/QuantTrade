#!/bin/bash
# Git仓库清理脚本 - 移除已删除但仍被跟踪的文件

echo "🧹 开始清理Git仓库冗余文件..."

# 只删除真正不存在的文件（notebook文件）
echo "📝 清理已删除的notebook文件..."

# 检查文件是否存在，如果不存在才删除
if ! [ -f "notebooks/development/策略模块修复测试.ipynb" ]; then
    echo "  - 删除: notebooks/development/策略模块修复测试.ipynb"
    git rm "notebooks/development/策略模块修复测试.ipynb" 2>/dev/null || true
fi

if ! [ -f "notebooks/fixes/诊断和修复scripts_new模块.ipynb" ]; then
    echo "  - 删除: notebooks/fixes/诊断和修复scripts_new模块.ipynb"
    git rm "notebooks/fixes/诊断和修复scripts_new模块.ipynb" 2>/dev/null || true
fi

# 检查是否有变更需要提交
if [ -n "$(git status --porcelain)" ]; then
    echo "✅ 清理完成，准备提交..."
    git add .
    git commit -m "🗑️ Remove deleted notebook files from Git tracking

🧹 Cleaned up notebook files that were deleted but still tracked in Git:
- notebooks/development/策略模块修复测试.ipynb
- notebooks/fixes/诊断和修复scripts_new模块.ipynb

📊 Also added new Git redundancy checker tools

🤖 Generated with [Claude Code](https://claude.ai/code)

Co-Authored-By: Claude <noreply@anthropic.com>
"
    echo "🎉 Git仓库清理完成！"
else
    echo "ℹ️ 没有需要清理的文件"
fi