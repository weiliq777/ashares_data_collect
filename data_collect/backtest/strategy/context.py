from dataclasses import dataclass
from typing import Any, Optional
from datetime import date

@dataclass(frozen=True)
class StrategyContext:
    dataset_id: Optional[str]            # 回测冻结世界ID (仅回测时存在)
    snapshot_id: str                     # 研究截面ID (日常和回测都有)
    trade_date: date
    universe: Any                         # 股票池视图，由策略宿主提供
    features: Any                         # 特征读取接口，由策略宿主提供
