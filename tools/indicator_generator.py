#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
技术指标生成器 - 快速创建技术指标研发环境
"""

import os
import json
from datetime import datetime
from pathlib import Path
import shutil

class IndicatorGenerator:
    """技术指标开发生成器"""
    
    def __init__(self, project_root=None):
        """初始化生成器"""
        self.project_root = Path(project_root or Path.cwd())
        self.templates_dir = self.project_root / "notebooks" / "_templates"
        self.development_dir = self.project_root / "notebooks" / "development"
        
    def create_indicator_research(self, indicator_name, category="trend", 
                                complexity="medium", developer="Developer"):
        """
        创建技术指标研发环境
        
        Args:
            indicator_name: 指标名称 (如 "ADX", "Stochastic RSI")
            category: 指标类别 (trend/momentum/volatility/volume)
            complexity: 复杂度 (simple/medium/complex)
            developer: 开发者名称
        """
        
        # 创建安全的文件名
        safe_name = indicator_name.lower().replace(" ", "_").replace("/", "_")
        notebook_name = f"indicator_{safe_name}.ipynb"
        notebook_path = self.development_dir / notebook_name
        
        # 检查模板是否存在
        template_path = self.templates_dir / "indicator_development_template.ipynb"
        if not template_path.exists():
            raise FileNotFoundError(f"模板文件不存在: {template_path}")
        
        # 读取模板
        with open(template_path, 'r', encoding='utf-8') as f:
            template_content = f.read()
        
        # 替换占位符
        replacements = {
            '{indicator_name}': indicator_name,
            '{date}': datetime.now().strftime('%Y-%m-%d'),
            '{developer}': developer,
            '{category}': category,
            '{complexity}': complexity,
            '{completion_time}': "待完成",
            '{next_indicator}': "待定"
        }
        
        content = template_content
        for placeholder, value in replacements.items():
            content = content.replace(placeholder, value)
        
        # 确保目录存在
        notebook_path.parent.mkdir(parents=True, exist_ok=True)
        
        # 写入新文件
        with open(notebook_path, 'w', encoding='utf-8') as f:
            f.write(content)
        
        # 创建配套的研究配置文件
        config_path = notebook_path.with_suffix('.json')
        config = {
            "indicator_name": indicator_name,
            "category": category,
            "complexity": complexity,
            "developer": developer,
            "created_date": datetime.now().isoformat(),
            "status": "in_development",
            "priority": "medium",
            "parameters": {},
            "test_results": {},
            "performance_metrics": {},
            "notes": []
        }
        
        with open(config_path, 'w', encoding='utf-8') as f:
            json.dump(config, f, indent=2, ensure_ascii=False)
        
        print(f"✅ 技术指标研发环境创建成功:")
        print(f"   📓 Notebook: {notebook_path}")
        print(f"   ⚙️ 配置文件: {config_path}")
        print(f"   🎯 指标名称: {indicator_name}")
        print(f"   📂 类别: {category}")
        print(f"   🔧 复杂度: {complexity}")
        
        return notebook_path, config_path
    
    def list_indicators_in_development(self):
        """列出所有开发中的指标"""
        if not self.development_dir.exists():
            print("❌ 开发目录不存在")
            return []
        
        indicators = []
        for config_file in self.development_dir.glob("indicator_*.json"):
            try:
                with open(config_file, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                    indicators.append(config)
            except Exception as e:
                print(f"⚠️ 无法读取配置文件 {config_file}: {e}")
        
        if indicators:
            print(f"📊 发现 {len(indicators)} 个开发中的指标:")
            print("=" * 60)
            for i, indicator in enumerate(indicators, 1):
                status_emoji = {"in_development": "🔄", "completed": "✅", "paused": "⏸️"}.get(
                    indicator.get("status", "unknown"), "❓")
                
                print(f"{i:2d}. {status_emoji} {indicator['indicator_name']}")
                print(f"     类别: {indicator['category']} | "
                      f"复杂度: {indicator['complexity']} | "
                      f"开发者: {indicator['developer']}")
                print(f"     创建: {indicator['created_date'][:10]} | "
                      f"状态: {indicator.get('status', 'unknown')}")
                print()
        else:
            print("📭 没有发现开发中的指标")
        
        return indicators
    
    def update_indicator_status(self, indicator_name, status, notes=None):
        """更新指标开发状态"""
        safe_name = indicator_name.lower().replace(" ", "_").replace("/", "_")
        config_path = self.development_dir / f"indicator_{safe_name}.json"
        
        if not config_path.exists():
            print(f"❌ 找不到指标配置: {indicator_name}")
            return False
        
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            
            config['status'] = status
            config['last_updated'] = datetime.now().isoformat()
            
            if notes:
                if 'notes' not in config:
                    config['notes'] = []
                config['notes'].append({
                    'timestamp': datetime.now().isoformat(),
                    'note': notes
                })
            
            with open(config_path, 'w', encoding='utf-8') as f:
                json.dump(config, f, indent=2, ensure_ascii=False)
            
            print(f"✅ 指标状态已更新: {indicator_name} -> {status}")
            return True
            
        except Exception as e:
            print(f"❌ 更新失败: {e}")
            return False
    
    def create_batch_indicators(self, indicator_list):
        """批量创建指标研发环境"""
        created = []
        for indicator_info in indicator_list:
            try:
                if isinstance(indicator_info, str):
                    # 如果只提供名称，使用默认配置
                    notebook_path, config_path = self.create_indicator_research(indicator_info)
                elif isinstance(indicator_info, dict):
                    # 如果提供完整配置
                    notebook_path, config_path = self.create_indicator_research(**indicator_info)
                else:
                    print(f"⚠️ 跳过无效的指标配置: {indicator_info}")
                    continue
                
                created.append(notebook_path)
                
            except Exception as e:
                print(f"❌ 创建失败 {indicator_info}: {e}")
        
        print(f"\n🎉 批量创建完成: {len(created)} 个指标研发环境")
        return created

def main():
    """命令行入口"""
    import argparse
    
    parser = argparse.ArgumentParser(description="技术指标生成器")
    parser.add_argument("--create", "-c", help="创建指标研发环境")
    parser.add_argument("--category", default="trend", 
                       choices=["trend", "momentum", "volatility", "volume"],
                       help="指标类别")
    parser.add_argument("--complexity", default="medium",
                       choices=["simple", "medium", "complex"],
                       help="指标复杂度")
    parser.add_argument("--developer", default="Developer", help="开发者名称")
    parser.add_argument("--list", "-l", action="store_true", help="列出开发中的指标")
    parser.add_argument("--status", help="更新指标状态")
    parser.add_argument("--notes", help="添加备注")
    
    args = parser.parse_args()
    
    generator = IndicatorGenerator()
    
    if args.create:
        generator.create_indicator_research(
            indicator_name=args.create,
            category=args.category,
            complexity=args.complexity,
            developer=args.developer
        )
    elif args.list:
        generator.list_indicators_in_development()
    elif args.status:
        # 假设用户提供指标名称作为第一个参数
        indicator_name = args.create or input("请输入指标名称: ")
        generator.update_indicator_status(indicator_name, args.status, args.notes)
    else:
        print("请使用 --help 查看可用选项")

if __name__ == "__main__":
    main()