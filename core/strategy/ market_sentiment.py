# =====================================
# 5. market_sentiment.py - 市场情绪分析
# =====================================

class MarketSentimentAnalyzer:
    """
    市场情绪分析器 - 分析市场整体情绪和热度
    """
    
    def __init__(self):
        """初始化市场情绪分析器"""
        self.sentiment_params = {
            'panic_threshold': 30,      # 恐慌阈值
            'greed_threshold': 70,      # 贪婪阈值
            'neutral_zone': (40, 60),   # 中性区间
        }
    
    def analyze_market_sentiment(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        分析市场情绪
        
        Args:
            data: 市场数据
            
        Returns:
            包含情绪分析的DataFrame
        """
        result = data.copy()
        
        # 涨跌幅分析
        if 'pct_change' in data.columns:
            result['sentiment_momentum'] = self._calculate_momentum_sentiment(data['pct_change'])
        
        # 成交量情绪
        if 'volume' in data.columns:
            result['volume_sentiment'] = self._calculate_volume_sentiment(data['volume'])
        
        # 涨跌停分析
        result['limit_sentiment'] = self._analyze_limit_moves(data)
        
        # 市场宽度
        result['market_breadth'] = self._calculate_market_breadth(data)
        
        # 恐惧贪婪指数
        result['fear_greed_index'] = self._calculate_fear_greed_index(result)
        
        # 情绪信号
        result['sentiment_signal'] = self._generate_sentiment_signals(result)
        
        return result
    
    def _calculate_momentum_sentiment(self, pct_change: pd.Series) -> pd.Series:
        """基于涨跌幅计算动量情绪"""
        # 20日动量
        momentum_20 = pct_change.rolling(20).mean()
        
        # 标准化到0-100
        if momentum_20.std() > 0:
            sentiment = 50 + (momentum_20 - momentum_20.mean()) / momentum_20.std() * 20
        else:
            sentiment = pd.Series(50, index=pct_change.index)
        
        return sentiment.clip(0, 100)
    
    def _calculate_volume_sentiment(self, volume: pd.Series) -> pd.Series:
        """基于成交量计算情绪"""
        # 成交量相对强度
        volume_ma20 = volume.rolling(20).mean()
        volume_ratio = volume / volume_ma20
        
        # 转换为情绪值
        sentiment = 50 + (volume_ratio - 1) * 50
        
        return sentiment.clip(0, 100)
    
    def _analyze_limit_moves(self, data: pd.DataFrame) -> pd.Series:
        """分析涨跌停情况"""
        sentiment = pd.Series(50, index=data.index)
        
        if 'pct_change' in data.columns:
            # 涨停
            limit_up = data['pct_change'] >= 9.5
            sentiment[limit_up] = 100
            
            # 跌停
            limit_down = data['pct_change'] <= -9.5
            sentiment[limit_down] = 0
            
            # 大涨
            strong_up = (data['pct_change'] > 5) & ~limit_up
            sentiment[strong_up] = 80
            
            # 大跌
            strong_down = (data['pct_change'] < -5) & ~limit_down
            sentiment[strong_down] = 20
        
        return sentiment
    
    def _calculate_market_breadth(self, data: pd.DataFrame) -> pd.Series:
        """计算市场宽度指标"""
        breadth = pd.Series(50, index=data.index)
        
        # 这里简化处理，实际应该使用全市场数据
        if 'pct_change' in data.columns:
            # 使用滚动窗口计算上涨比例
            up_ratio = (data['pct_change'] > 0).rolling(20).mean()
            breadth = up_ratio * 100
        
        return breadth
    
    def _calculate_fear_greed_index(self, data: pd.DataFrame) -> pd.Series:
        """计算恐惧贪婪指数"""
        index = pd.Series(50, index=data.index)
        
        components = []
        weights = []
        
        # 动量情绪 (权重25%)
        if 'sentiment_momentum' in data.columns:
            components.append(data['sentiment_momentum'])
            weights.append(0.25)
        
        # 成交量情绪 (权重25%)
        if 'volume_sentiment' in data.columns:
            components.append(data['volume_sentiment'])
            weights.append(0.25)
        
        # 涨跌停情绪 (权重25%)
        if 'limit_sentiment' in data.columns:
            components.append(data['limit_sentiment'])
            weights.append(0.25)
        
        # 市场宽度 (权重25%)
        if 'market_breadth' in data.columns:
            components.append(data['market_breadth'])
            weights.append(0.25)
        
        # 加权计算
        if components:
            total_weight = sum(weights)
            weights = [w/total_weight for w in weights]
            
            for comp, weight in zip(components, weights):
                index += comp * weight - 50 * weight
        
        return index.clip(0, 100)
    
    def _generate_sentiment_signals(self, data: pd.DataFrame) -> pd.Series:
        """生成情绪信号"""
        signals = pd.Series(0, index=data.index)
        
        if 'fear_greed_index' not in data.columns:
            return signals
        
        fgi = data['fear_greed_index']
        
        # 极度恐慌 - 反向买入机会
        signals[fgi < self.sentiment_params['panic_threshold']] = 1
        
        # 极度贪婪 - 反向卖出机会
        signals[fgi > self.sentiment_params['greed_threshold']] = -1
        
        return signals
    
    def analyze_limit_up(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        涨停板分析
        
        Args:
            data: 包含价格数据
            
        Returns:
            涨停分析结果
        """
        result = data.copy()
        
        if 'pct_change' not in data.columns and 'close' in data.columns:
            result['pct_change'] = data['close'].pct_change() * 100
        
        # 涨停判断
        result['is_limit_up'] = result['pct_change'] >= 9.5
        result['is_limit_down'] = result['pct_change'] <= -9.5
        
        # 连板统计
        result['consecutive_limit_up'] = self._count_consecutive_limit(result['is_limit_up'])
        result['consecutive_limit_down'] = self._count_consecutive_limit(result['is_limit_down'])
        
        # 涨停强度
        if 'volume' in data.columns:
            result['limit_up_strength'] = result['is_limit_up'] * result['volume'] / result['volume'].rolling(20).mean()
        
        return result
    
    def _count_consecutive_limit(self, is_limit: pd.Series) -> pd.Series:
        """统计连续涨跌停天数"""
        consecutive = is_limit.astype(int)
        consecutive = consecutive.groupby((consecutive != consecutive.shift()).cumsum()).cumsum()
        consecutive[~is_limit] = 0
        return consecutive