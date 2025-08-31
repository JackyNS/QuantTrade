#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
简单数据下载示例
==============

演示如何使用统一数据架构下载数据的最简单方法
"""

import pandas as pd
import numpy as np
from pathlib import Path

def test_direct_component_usage():
    """直接使用组件进行数据下载测试"""
    print("🚀 直接组件使用测试")
    print("=" * 40)
    
    try:
        # 直接导入下载器，避免复杂初始化
        from core.data.downloaders.a_shares_downloader import ASharesDownloader
        
        print("✅ A股下载器导入成功")
        
        # 简单配置
        config = {
            'data_dir': './data',
            'batch_size': 10,
            'delay': 0.1,
            'max_retry': 2
        }
        
        # 创建下载器实例
        downloader = ASharesDownloader(config)
        print("✅ A股下载器创建成功")
        
        # 检查数据目录
        data_dir = Path('./data')
        if not data_dir.exists():
            data_dir.mkdir(parents=True, exist_ok=True)
            print("📁 创建数据目录")
        
        existing_files = list(data_dir.glob('*.csv'))
        print(f"📊 现有CSV文件数量: {len(existing_files)}")
        
        if len(existing_files) > 0:
            print("📄 部分现有文件:")
            for i, file in enumerate(existing_files[:5]):
                size = file.stat().st_size / 1024  # KB
                print(f"   {file.name} ({size:.1f} KB)")
            if len(existing_files) > 5:
                print(f"   ... 还有 {len(existing_files) - 5} 个文件")
        
        return True
        
    except Exception as e:
        print(f"❌ 测试失败: {e}")
        return False

def test_data_processing():
    """测试数据处理功能"""
    print("\n🔧 数据处理功能测试")
    print("=" * 40)
    
    try:
        from core.data.processors.data_processor import DataProcessor
        from core.data.processors.data_cleaner import DataCleaner
        from core.data.processors.data_transformer import DataTransformer
        
        print("✅ 处理器导入成功")
        
        # 创建示例数据
        sample_data = pd.DataFrame({
            'date': pd.date_range('2024-01-01', periods=10),
            'open': np.random.uniform(10, 20, 10),
            'high': np.random.uniform(15, 25, 10), 
            'low': np.random.uniform(8, 18, 10),
            'close': np.random.uniform(10, 20, 10),
            'volume': np.random.randint(1000, 10000, 10)
        })
        
        print(f"📊 创建示例数据: {sample_data.shape}")
        
        # 测试数据清洗
        cleaner = DataCleaner()
        cleaned_data = cleaner.clean_price_data(sample_data)
        print(f"🧹 数据清洗完成: {cleaned_data.shape}")
        
        # 测试数据转换
        transformer = DataTransformer()
        transformed_data = transformer.standardize_columns(sample_data)
        print(f"🔄 数据转换完成: {list(transformed_data.columns)}")
        
        # 测试统一处理器
        processor = DataProcessor()
        processed_data = processor.process_price_data(sample_data, symbol='TEST')
        print(f"⚙️ 统一处理完成: {processed_data.shape}")
        
        return True
        
    except Exception as e:
        print(f"❌ 数据处理测试失败: {e}")
        return False

def show_next_steps():
    """显示后续步骤"""
    print("""
🎯 后续使用步骤
==============

1. 📥 数据下载方式:

   方式一 - 使用统一管理器:
   ```python
   from core.data.enhanced_data_manager import EnhancedDataManager
   
   config = {'data_dir': './data'}
   dm = EnhancedDataManager(config)
   
   # 下载A股数据
   result = dm.download_a_shares_data(['000001.SZ', '000002.SZ'])
   ```

   方式二 - 直接使用下载器:
   ```python
   from core.data.downloaders.a_shares_downloader import ASharesDownloader
   
   downloader = ASharesDownloader(config)
   result = downloader.download_batch(['000001.SZ', '000002.SZ'])
   ```

2. 🔧 数据处理:
   ```python
   from core.data.processors import DataProcessor
   
   processor = DataProcessor()
   clean_data = processor.process_price_data(raw_data, 'symbol')
   ```

3. 📊 获取数据:
   ```python
   # 如果您已有数据文件，可以直接读取
   import pandas as pd
   data = pd.read_csv('./data/000001.SZ.csv')
   ```

4. ⚙️ 配置API密钥 (可选，用于自动下载):
   - Tushare: https://tushare.pro/
   - 优矿: https://uqer.datayes.com/
   
   配置示例:
   ```python
   config = {
       'tushare': {'token': 'your_token_here'},
       'uqer': {'token': 'your_token_here'}
   }
   ```

5. 📈 开始量化交易:
   有了数据后，您可以开始策略开发、回测等工作

📖 详细文档: core/data/UNIFIED_DATA_USAGE.md
""")

def main():
    """主函数"""
    print("🎯 简单数据下载示例")
    print("=" * 50)
    
    success_count = 0
    
    # 测试直接组件使用
    if test_direct_component_usage():
        success_count += 1
    
    # 测试数据处理
    if test_data_processing():
        success_count += 1
    
    print("\n" + "=" * 50)
    print(f"📊 测试结果: {success_count}/2 项测试通过")
    
    if success_count >= 1:
        print("🎉 基础功能可以正常使用！")
        show_next_steps()
    else:
        print("⚠️ 需要检查配置或依赖")
    
    print("=" * 50)

if __name__ == "__main__":
    main()