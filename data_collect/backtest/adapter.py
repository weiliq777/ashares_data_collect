from typing import Any, Mapping
from datetime import date

from .engine import SignalIntent
from .models import Observation
from .strategy.base import BaseStrategy
from .strategy.context import StrategyContext

class ValueTrendCandidateAdapter:
    """
    Candidate Adapter 负责把纯粹的研究层 Candidate 转换为可执行的 Order Intent (SignalIntent)。
    它桥接了 Research 领域（不包含仓位、资金）和 Execution 领域（包含头寸状态、风险控制）。
    """
    
    def __init__(self, research_strategy: BaseStrategy):
        self.research_strategy = research_strategy
        self.strategy_id = research_strategy.metadata["name"]
        self.strategy_version = research_strategy.metadata["version"]
        self.parameters = research_strategy.metadata["parameters"]

    @property
    def required_observation_columns(self):
        return getattr(self.research_strategy, "required_observation_columns", None)
        
    def generate_signals(
        self,
        trade_date: date,
        observations: tuple[Observation, ...],
        positions: Mapping[str, Mapping[str, Any]],
    ) -> list[SignalIntent]:
        
        # 1. 构建无状态的研究上下文
        # 注意: 这里假设 observations 就是 universe/features 的平铺表示
        context = StrategyContext(
            dataset_id=None,
            snapshot_id=f"{trade_date.isoformat()}_snapshot",
            trade_date=trade_date,
            universe=observations,  # 简化：直接传入 observations
            features=observations   # 简化：直接传入 observations
        )
        
        # 2. 调用纯研究策略，获取值得买入/研究的候选池
        candidates = self.research_strategy.run(context)
        candidate_dict = {c.ts_code: c for c in candidates}
        
        signals: list[SignalIntent] = []
        
        # 3. 处理现有持仓的卖出逻辑 (Adapter 承担了由仓位状态引发的执行逻辑)
        for observation in observations:
            code = observation.ts_code
            if code in positions and positions[code].get("hold_shares", 0) > 0:
                # 针对持仓票，如果满足趋势破坏条件，产生 SELL Intent
                ma20 = observation.get("ma20")
                indicator_close = observation.get("adj_close_base")
                raw_close = observation.get("raw_close")
                
                if ma20 is not None and indicator_close is not None and indicator_close < ma20:
                    signals.append(SignalIntent(
                        ts_code=code,
                        direction="SELL",
                        indicator_price=indicator_close,
                        raw_reference_price=raw_close,
                        reason={"rule": "close_below_ma20", "ma20": ma20},
                    ))
                continue
            
            # 4. 对于未持仓票，如果它在候选池中，产生 BUY Intent
            if code in candidate_dict:
                candidate = candidate_dict[code]
                raw_close = observation.get("raw_close")
                stop_loss_pct = self.parameters.get("stop_loss_pct", 0.08)
                stop = raw_close * (1.0 - stop_loss_pct) if raw_close else None
                
                signals.append(SignalIntent(
                    ts_code=code,
                    direction="BUY",
                    indicator_price=observation.get("adj_close_base"),
                    raw_reference_price=raw_close,
                    stop_loss_price_raw=stop,
                    reason={
                        "rule": candidate.strategy_name,
                        "candidate_id": candidate.candidate_id,
                        "reasons": candidate.reasons,
                        "factor_detail": candidate.factor_detail
                    },
                    score=candidate.raw_score,
                ))
                
        return signals
