from abc import ABC, abstractmethod
from typing import List

from .models import ResearchCandidate
from .context import StrategyContext

class BaseStrategy(ABC):
    @property
    @abstractmethod
    def metadata(self) -> dict:
        """
        策略元数据，为外部 AI 提供结构化上下文。
        例如:
        {
            "name": "value_v1",
            "version": "1.0",
            "description": "ROE+估值模型",
            "author": "system",
            "category": "value"
        }
        """
        pass

    @abstractmethod
    def run(self, context: StrategyContext) -> List[ResearchCandidate]:
        """
        根据策略规则评估市场截面，返回值得进一步投研的候选股票列表。
        不负责资金分配、不产生真实订单。
        """
        pass
