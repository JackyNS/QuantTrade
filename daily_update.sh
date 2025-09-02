#!/bin/bash
# 每日数据更新脚本
# 添加到crontab: 0 9 * * 1-5 /Users/jackstudio/QuantTrade/daily_update.sh

cd /Users/jackstudio/QuantTrade
source .venv/bin/activate

export UQER_TOKEN="68b9922817ae6273137bda7acba81e293582ba347281dfcc056dcb245b23faf3"
python daily_update_manager.py --priority=high_priority

# 检查返回码
if [ $? -eq 0 ]; then
    echo "$(date): 每日更新成功" >> logs/daily_updates/cron.log
else
    echo "$(date): 每日更新失败" >> logs/daily_updates/cron.log
fi
