#!/bin/bash
# Git仓库清理脚本 - 移除已删除但仍被跟踪的文件

echo "🧹 开始清理Git仓库冗余文件..."

git rm ".gitignore"
git rm "data/.gitignore"
git rm ""notebooks/development/\347\255\226\347\225\245\346\250\241\345\235\227\344\277\256\345\244\215\346\265\213\350\257\225.ipynb""
git rm ""notebooks/fixes/\350\257\212\346\226\255\345\222\214\344\277\256\345\244\215scripts_new\346\250\241\345\235\227.ipynb""
git rm ""uqer\346\216\245\345\217\243\346\270\205\345\215\225.txt""

echo "✅ 清理完成，准备提交..."
git commit -m "🗑️ Remove redundant files from Git tracking

🧹 Cleaned up files that were moved/deleted but still tracked in Git
📊 Removed files: 5"

🤖 Generated with [Claude Code](https://claude.ai/code)

Co-Authored-By: Claude <noreply@anthropic.com>
"

echo "🎉 Git仓库清理完成！"
