from typing import Any, List
from datetime import date, datetime

from .base import BaseStrategy
from .context import StrategyContext
from .models import ResearchCandidate

class ValueTrendStrategy(BaseStrategy):
    """
    第一版确定性示例策略：估值安全边际 + 均线趋势。
    从 engine.py 中剥离出来的纯粹的研究层策略。
    它不依赖任何仓位信息，只对整个市场切片(Universe)进行评估并输出候选。
    """
    strategy_id = "pure_value_trend_v1"
    strategy_version = "1.0.0"
    
    def __init__(
        self,
        pe_percentile_max: float = 0.30,
        volume_ratio_min: float = 1.0,
        stop_loss_pct: float = 0.08,
        allow_st: bool = False,
    ):
        if not 0 <= pe_percentile_max <= 1 or volume_ratio_min < 0 or stop_loss_pct <= 0:
            raise ValueError("策略参数无效")
        self.pe_percentile_max = pe_percentile_max
        self.volume_ratio_min = volume_ratio_min
        self.stop_loss_pct = stop_loss_pct
        self.allow_st = allow_st

    @property
    def metadata(self) -> dict:
        return {
            "name": self.strategy_id,
            "version": self.strategy_version,
            "description": "估值安全边际 + 均线趋势",
            "author": "system",
            "category": "value_trend",
            "parameters": {
                "pe_percentile_max": self.pe_percentile_max,
                "volume_ratio_min": self.volume_ratio_min,
                "stop_loss_pct": self.stop_loss_pct,
                "allow_st": self.allow_st,
            }
        }

    def run(self, context: StrategyContext) -> List[ResearchCandidate]:
        candidates: List[ResearchCandidate] = []
        trade_date = context.trade_date
        
        # We assume context.features provides an iterable of observations similar to dataset.observations_for
        for observation in context.features:
            code = observation.ts_code
            effective_announce_date = observation.get("effective_announce_date")
            if effective_announce_date is not None and effective_announce_date > trade_date:
                continue
            
            raw_close = observation.get("raw_close")
            indicator_close = observation.get("adj_close_base")
            ma20 = observation.get("ma20")
            ma60 = observation.get("ma60")
            
            if raw_close is None or raw_close <= 0 or indicator_close is None or indicator_close <= 0:
                continue

            if observation.get("is_listed_asof") is False:
                continue
            if observation.get("eligible_by_market") is False:
                continue
            if observation.get("is_st_asof") and not self.allow_st:
                continue
                
            pe_pct = observation.get("pe_pct_5y")
            is_pe_meaningful = observation.get("is_pe_meaningful")
            
            if pe_pct is None or is_pe_meaningful is not True or pe_pct > self.pe_percentile_max:
                continue
            if ma20 is None or ma60 is None or indicator_close <= ma20 or ma20 <= ma60:
                continue
                
            volume_ratio = observation.get("volume_ratio_20d")
            if volume_ratio is None or volume_ratio < self.volume_ratio_min:
                continue

            value_score = max(0.0, 1.0 - float(pe_pct))
            trend_score = max(0.0, float(indicator_close) / float(ma20) - 1.0)
            volume_score = max(0.0, min(float(volume_ratio), 5.0) - 1.0) * 0.05
            raw_score = round(value_score + trend_score + volume_score, 8)
            
            reasons = [
                f"PE 5y Pct {pe_pct:.2f} <= {self.pe_percentile_max}",
                f"Close {indicator_close:.2f} > MA20 {ma20:.2f} > MA60 {ma60:.2f}",
                f"Volume Ratio 20d {volume_ratio:.2f} >= {self.volume_ratio_min}"
            ]
            
            factor_detail = {
                "pe_pct_5y": pe_pct,
                "ma20": ma20,
                "ma60": ma60,
                "volume_ratio_20d": volume_ratio,
                "value_score": value_score,
                "trend_score": trend_score,
                "volume_score": volume_score,
                "raw_close": raw_close,
                "adj_close_base": indicator_close
            }
            
            candidates.append(ResearchCandidate(
                candidate_id=f"{trade_date.strftime('%Y%m%d')}_{self.strategy_id}_{code}",
                ts_code=code,
                strategy_name=self.strategy_id,
                strategy_version=self.strategy_version,
                snapshot_date=trade_date,
                generated_at=datetime.utcnow(),
                raw_score=raw_score,
                rank=None,
                reasons=reasons,
                factor_detail=factor_detail,
                risk_tags=[]
            ))
            
        # Sort candidates by raw_score descending and assign rank
        candidates.sort(key=lambda x: x.raw_score, reverse=True)
        for i, candidate in enumerate(candidates):
            object.__setattr__(candidate, 'rank', i + 1)
            
        return candidates
