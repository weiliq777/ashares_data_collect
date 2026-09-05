"""阶段三 Backtest 的纯 Python 领域模型、风控和日线撮合组件。"""

from .engine import (
    BacktestConfig, BacktestEngine, BaseStrategy,
    FundamentalValueGrowthTrendStrategy, ValueTrendStrategy,
)
from .models import (
    BacktestDataset,
    BacktestResult,
    DividendEvent,
    MarketBar,
    Observation,
    PositionLotState,
    SignalIntent,
)
from .risk import (
    FeeRule,
    LiquidityCheck,
    MarketRule,
    MarketTemperatureScaler,
    MonteCarloTester,
    PositionSizingResult,
    RiskPolicy,
    check_liquidity,
    open_position_stop_risk,
    size_position,
    size_target_position,
)
from .strategy.contract import (
    EvaluationConfig,
    ScenarioConfig,
    StrategyVersionSpec,
    backtest_fingerprint,
    canonical_hash,
)
from .strategy.quality_trend_rotation_v1 import QualityTrendRotationV1
from .validation import (
    assert_no_future_rows,
    assert_pit_cutoff_consistency,
    truncate_dataset_asof,
    validate_observation_cutoffs,
    verify_pit_cutoff_consistency,
)

__all__ = [
    "BacktestConfig", "BacktestEngine", "BaseStrategy", "ValueTrendStrategy",
    "FundamentalValueGrowthTrendStrategy",
    "BacktestDataset", "BacktestResult", "DividendEvent", "MarketBar",
    "Observation", "PositionLotState", "SignalIntent", "FeeRule", "LiquidityCheck", "MarketRule", "MarketTemperatureScaler",
    "MonteCarloTester",
    "PositionSizingResult", "RiskPolicy", "check_liquidity", "open_position_stop_risk", "size_position",
    "size_target_position", "StrategyVersionSpec", "ScenarioConfig", "EvaluationConfig",
    "canonical_hash", "backtest_fingerprint", "QualityTrendRotationV1",
    "assert_no_future_rows", "assert_pit_cutoff_consistency", "truncate_dataset_asof",
    "validate_observation_cutoffs", "verify_pit_cutoff_consistency",
]
