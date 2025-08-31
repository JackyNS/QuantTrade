#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
优矿数据每日自动更新脚本
====================

功能：
1. 每日增量更新股票数据
2. 智能检测需要更新的股票
3. 自动重试和错误处理
4. 生成更新报告

Author: QuantTrader Team
Date: 2025-08-31
"""

import sys
import os
import json
from pathlib import Path
from datetime import datetime, timedelta, date
import time
import logging
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# 添加项目路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# 配置日志
log_file = f"logs/daily_update_{date.today().strftime('%Y%m%d')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DailyUqerUpdater:
    """每日优矿数据更新器"""
    
    def __init__(self):
        self.config = self.load_config()
        self.data_manager = None
        self.update_stats = {
            'start_time': datetime.now(),
            'end_time': None,
            'total_symbols': 0,
            'updated_symbols': 0,
            'failed_symbols': [],
            'new_data_count': 0,
            'errors': []
        }
    
    def load_config(self):
        """加载配置"""
        # 从环境变量或配置文件获取Token
        token = os.environ.get('UQER_TOKEN')
        if not token:
            config_files = ['config/uqer_config.json', 'uqer_config.json']
            for config_file in config_files:
                if Path(config_file).exists():
                    try:
                        with open(config_file, 'r', encoding='utf-8') as f:
                            config_data = json.load(f)
                            token = config_data.get('token')
                            if token:
                                break
                    except:
                        continue
        
        config = {
            'data_dir': './data',
            'cache': {
                'cache_dir': './data/cache',
                'max_memory_size': 100 * 1024 * 1024  # 100MB
            },
            'uqer': {
                'token': token,
                'rate_limit': 0.2  # 200ms延迟，更保守
            },
            'update': {
                'batch_size': 30,  # 较小的批次大小
                'max_retry': 3,
                'delay_between_batches': 2.0,  # 批次间延迟
                'update_days': 5  # 更新最近5天的数据
            },
            'notification': {
                'enable_email': False,  # 邮件通知
                'email_config': {
                    'smtp_server': 'smtp.example.com',
                    'smtp_port': 587,
                    'sender_email': 'your_email@example.com',
                    'sender_password': 'your_password',
                    'recipients': ['recipient@example.com']
                }
            }
        }
        
        return config
    
    def get_symbols_to_update(self):
        """获取需要更新的股票列表"""
        logger.info("📋 获取需要更新的股票列表...")
        
        data_dir = Path(self.config['data_dir'])
        if not data_dir.exists():
            logger.warning("数据目录不存在，将获取全部股票列表")
            return self.get_all_symbols()
        
        # 检查现有数据文件的最后更新时间
        csv_files = list(data_dir.glob('*.csv'))
        update_threshold = datetime.now() - timedelta(days=self.config['update']['update_days'])
        
        symbols_to_update = []
        outdated_files = []
        
        for csv_file in csv_files:
            # 从文件名提取股票代码
            symbol = csv_file.stem
            
            # 检查文件修改时间
            file_mtime = datetime.fromtimestamp(csv_file.stat().st_mtime)
            
            if file_mtime < update_threshold:
                symbols_to_update.append(symbol)
                outdated_files.append((symbol, file_mtime))
        
        if outdated_files:
            logger.info(f"发现 {len(outdated_files)} 个需要更新的文件")
            for symbol, mtime in outdated_files[:5]:  # 显示前5个
                logger.info(f"   {symbol}: 上次更新 {mtime.strftime('%Y-%m-%d %H:%M:%S')}")
            if len(outdated_files) > 5:
                logger.info(f"   ... 还有 {len(outdated_files) - 5} 个文件")
        
        # 如果没有过期文件，获取所有股票进行检查
        if not symbols_to_update:
            logger.info("所有文件都是最新的，获取股票列表进行检查...")
            symbols_to_update = self.get_all_symbols()
        
        return symbols_to_update
    
    def get_all_symbols(self):
        """获取所有股票代码"""
        try:
            if self.data_manager:
                stock_list = self.data_manager.get_stock_list()
                if stock_list:
                    return stock_list
        except Exception as e:
            logger.warning(f"获取股票列表失败: {e}")
        
        # 返回默认列表
        return self.get_default_symbols()
    
    def get_default_symbols(self):
        """获取默认股票列表"""
        # 从现有文件中获取股票代码
        data_dir = Path(self.config['data_dir'])
        if data_dir.exists():
            csv_files = list(data_dir.glob('*.csv'))
            symbols = [f.stem for f in csv_files]
            if symbols:
                return symbols
        
        # 如果没有现有文件，返回主要股票
        return [
            '000001.SZ', '000002.SZ', '000858.SZ', '002415.SZ',
            '600000.SH', '600036.SH', '600519.SH', '601318.SH',
            '601398.SH', '603259.SH'
        ]
    
    def update_data(self):
        """执行数据更新"""
        logger.info("🚀 开始每日数据更新")
        logger.info("=" * 50)
        
        try:
            from core.data.enhanced_data_manager import EnhancedDataManager
            
            self.data_manager = EnhancedDataManager(self.config)
            logger.info("✅ 数据管理器初始化成功")
            
            # 获取需要更新的股票
            symbols_to_update = self.get_symbols_to_update()
            self.update_stats['total_symbols'] = len(symbols_to_update)
            
            logger.info(f"📊 需要更新 {len(symbols_to_update)} 只股票")
            
            # 计算更新日期范围
            end_date = datetime.now().strftime('%Y-%m-%d')
            start_date = (datetime.now() - timedelta(days=self.config['update']['update_days'])).strftime('%Y-%m-%d')
            
            logger.info(f"📅 更新日期范围: {start_date} 到 {end_date}")
            
            # 分批更新
            batch_size = self.config['update']['batch_size']
            total_batches = (len(symbols_to_update) + batch_size - 1) // batch_size
            
            for i in range(0, len(symbols_to_update), batch_size):
                batch_num = i // batch_size + 1
                batch_symbols = symbols_to_update[i:i + batch_size]
                
                logger.info(f"🔄 批次 {batch_num}/{total_batches}: {len(batch_symbols)} 只股票")
                
                try:
                    # 执行更新
                    result = self.data_manager.download_a_shares_data(
                        symbols=batch_symbols,
                        start_date=start_date,
                        end_date=end_date,
                        resume=True,
                        mode='update'  # 增量更新模式
                    )
                    
                    if result and result.get('success', False):
                        successful = result.get('successful', [])
                        failed = result.get('failed', [])
                        
                        self.update_stats['updated_symbols'] += len(successful)
                        self.update_stats['failed_symbols'].extend(failed)
                        
                        # 统计新增数据量
                        if 'new_data_count' in result:
                            self.update_stats['new_data_count'] += result['new_data_count']
                        
                        logger.info(f"   ✅ 成功: {len(successful)}, ❌ 失败: {len(failed)}")
                        
                        if failed:
                            logger.warning(f"   失败的股票: {failed[:3]}{'...' if len(failed) > 3 else ''}")
                    else:
                        logger.error(f"   ❌ 批次更新失败")
                        self.update_stats['failed_symbols'].extend(batch_symbols)
                
                except Exception as e:
                    error_msg = f"批次 {batch_num} 更新异常: {e}"
                    logger.error(error_msg)
                    self.update_stats['errors'].append(error_msg)
                    self.update_stats['failed_symbols'].extend(batch_symbols)
                
                # 批次间延迟
                if batch_num < total_batches:
                    delay = self.config['update']['delay_between_batches']
                    logger.info(f"   ⏳ 等待 {delay}s...")
                    time.sleep(delay)
            
            # 重试失败的更新
            if self.update_stats['failed_symbols']:
                logger.info(f"🔁 重试失败的 {len(self.update_stats['failed_symbols'])} 只股票...")
                self.retry_failed_updates()
            
            self.update_stats['end_time'] = datetime.now()
            
            return True
            
        except Exception as e:
            error_msg = f"数据更新过程异常: {e}"
            logger.error(error_msg, exc_info=True)
            self.update_stats['errors'].append(error_msg)
            self.update_stats['end_time'] = datetime.now()
            return False
    
    def retry_failed_updates(self):
        """重试失败的更新"""
        max_retry = self.config['update']['max_retry']
        failed_symbols = self.update_stats['failed_symbols'].copy()
        
        for retry_num in range(1, max_retry + 1):
            if not failed_symbols:
                break
            
            logger.info(f"🔁 第 {retry_num} 次重试，共 {len(failed_symbols)} 只股票")
            
            try:
                result = self.data_manager.download_a_shares_data(
                    symbols=failed_symbols,
                    resume=True,
                    mode='update'
                )
                
                if result:
                    successful = result.get('successful', [])
                    still_failed = result.get('failed', [])
                    
                    # 更新统计
                    self.update_stats['updated_symbols'] += len(successful)
                    
                    # 从失败列表中移除成功的
                    for symbol in successful:
                        if symbol in self.update_stats['failed_symbols']:
                            self.update_stats['failed_symbols'].remove(symbol)
                    
                    failed_symbols = still_failed
                    logger.info(f"   ✅ 重试成功: {len(successful)}, 仍失败: {len(still_failed)}")
                
            except Exception as e:
                logger.error(f"重试 {retry_num} 异常: {e}")
            
            if failed_symbols and retry_num < max_retry:
                time.sleep(5)  # 重试间延迟
    
    def generate_report(self):
        """生成更新报告"""
        stats = self.update_stats
        duration = (stats['end_time'] - stats['start_time']).total_seconds() if stats['end_time'] else 0
        
        success_rate = (stats['updated_symbols'] / stats['total_symbols'] * 100) if stats['total_symbols'] > 0 else 0
        
        report = f"""
📊 优矿数据每日更新报告
{'=' * 40}
📅 更新日期: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
⏱️ 耗时: {duration:.1f} 秒
📈 总计股票: {stats['total_symbols']}
✅ 成功更新: {stats['updated_symbols']}
❌ 失败数量: {len(stats['failed_symbols'])}
📊 成功率: {success_rate:.1f}%
📦 新增数据: {stats['new_data_count']} 条

{'失败股票列表:' if stats['failed_symbols'] else ''}
{chr(10).join(f'   {symbol}' for symbol in stats['failed_symbols'][:10])}
{'   ...' if len(stats['failed_symbols']) > 10 else ''}

{'错误信息:' if stats['errors'] else ''}
{chr(10).join(f'   {error}' for error in stats['errors'])}
{'=' * 40}
        """
        
        # 保存报告到文件
        report_file = f"reports/daily_update_{date.today().strftime('%Y%m%d')}.txt"
        os.makedirs('reports', exist_ok=True)
        
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(report)
        
        logger.info(f"📝 更新报告已保存: {report_file}")
        print(report)
        
        return report
    
    def send_notification(self, report):
        """发送通知"""
        if not self.config['notification']['enable_email']:
            return
        
        try:
            email_config = self.config['notification']['email_config']
            
            # 创建邮件
            msg = MIMEMultipart()
            msg['From'] = email_config['sender_email']
            msg['To'] = ', '.join(email_config['recipients'])
            msg['Subject'] = f"优矿数据更新报告 - {date.today().strftime('%Y-%m-%d')}"
            
            # 邮件正文
            msg.attach(MIMEText(report, 'plain', 'utf-8'))
            
            # 发送邮件
            with smtplib.SMTP(email_config['smtp_server'], email_config['smtp_port']) as server:
                server.starttls()
                server.login(email_config['sender_email'], email_config['sender_password'])
                server.send_message(msg)
            
            logger.info("📧 邮件通知发送成功")
            
        except Exception as e:
            logger.error(f"邮件发送失败: {e}")
    
    def run(self):
        """运行每日更新"""
        logger.info("🚀 启动每日优矿数据更新任务")
        
        # 创建必要目录
        os.makedirs('logs', exist_ok=True)
        os.makedirs('reports', exist_ok=True)
        
        # 检查Token
        if not self.config['uqer']['token']:
            logger.error("❌ 未配置优矿API Token")
            print("💡 请设置环境变量: export UQER_TOKEN='your_token'")
            return False
        
        # 执行更新
        success = self.update_data()
        
        # 生成报告
        report = self.generate_report()
        
        # 发送通知
        self.send_notification(report)
        
        if success:
            logger.info("🎉 每日更新任务完成")
        else:
            logger.error("⚠️ 更新任务完成但存在问题")
        
        return success

def main():
    """主函数"""
    updater = DailyUqerUpdater()
    return updater.run()

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)