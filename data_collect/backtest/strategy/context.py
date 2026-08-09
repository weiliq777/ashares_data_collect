from dataclasses import dataclass
from typing import Optional
from datetime import date

@dataclass(frozen=True)
class StrategyContext:
    dataset_id: Optional[str]            # 回测冻结世界ID (仅回测时存在)
    snapshot_id: str                     # 研究截面ID (日常和回测都有)
    trade_date: date
    universe: "UniverseView"             # 股票池视图，提供过滤与行业信息，避免 List 污染
    features: "FeatureAccessor"          # 统一的特征读取接口 (按需读取)
