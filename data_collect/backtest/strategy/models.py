from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from datetime import date, datetime

@dataclass(frozen=True)
class ResearchCandidate:
    candidate_id: str                # 唯一候选标识，如 "20260809_value_v1_600519" (便于 AI 追踪与关联)
    ts_code: str                     # 股票代码，如 "600519.SH"
    strategy_name: str               # 策略名称，如 "value_v1"
    strategy_version: str            # 策略版本
    snapshot_date: date              # 候选所属截面日期
    generated_at: datetime           # 候选生成的时间戳，方便审计
    raw_score: float                 # 策略内原始评分。警告：不允许跨策略比较！(如 Value 的 90 不等同于 Growth 的 90)
    rank: Optional[int]              # 策略内排名
    reasons: List[str]               # 选股核心理由描述
    factor_detail: Dict[str, Any]    # 细分因子或附加特征，如 {"roe_score": 95.0, "industry": "白酒"}
    risk_tags: List[str]             # 风险标签，如 ["high_valuation", "cyclical"]
