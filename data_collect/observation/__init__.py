"""Market Anomaly Observation（市场异常观察域）纯计算组件。"""

from data_collect.observation.contracts import (
    ANOMALY_TAGS,
    CONTRACT_VERSION,
    METHODOLOGY_ID,
    ObservationIdentity,
    ObservationPolicy,
    PRIMARY_STATES,
)
from data_collect.observation.hashing import hash_object, hash_rows
from data_collect.observation.outcome_metrics import compute_outcome
from data_collect.observation.sector_confirmation import aggregate_sector_rows, confirm_stock_sector
from data_collect.observation.stock_metrics import compute_stock_series
from data_collect.observation.stock_state_machine import calculate_scores, build_tags, select_primary_state

__all__ = [
    "ANOMALY_TAGS", "CONTRACT_VERSION", "METHODOLOGY_ID", "ObservationIdentity",
    "ObservationPolicy", "PRIMARY_STATES", "hash_object", "hash_rows",
    "compute_outcome", "aggregate_sector_rows", "confirm_stock_sector",
    "compute_stock_series", "calculate_scores", "build_tags", "select_primary_state",
]
