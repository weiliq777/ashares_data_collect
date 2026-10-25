"""Versioned identities and policies for reproducible factor research."""
from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date
from typing import Any

from data_collect.observation.hashing import hash_object

ALPHA_RESEARCH_VERSION = "alpha-research-v2"
FACTOR_SET_VERSION = "frozen-factor-set-v4"
DEBT_TO_ASSETS_FEATURE_CONTRACT_VERSION = "debt-to-assets-v1"
SOURCE_QUALITY_EVIDENCE_VERSION = "financial-source-quality-v1"
LABEL_VERSION = "frozen-forward-total-return-v3"
EVALUATION_VERSION = "cross-sectional-evaluation-v5"
MARKET_STAGE_VERSION = "benchmark-60-session-direction-v1"
SUPPORTED_RESEARCH_SCHEMAS = frozenset({"research-v2.0", "research-v2.1", "research-v2.2"})
HORIZONS = (20, 40, 60)


@dataclass(frozen=True, slots=True)
class FactorSpec:
    name: str
    category: str
    input_fields: tuple[str, ...]
    higher_is_better: int
    description: str

    def __post_init__(self) -> None:
        if self.higher_is_better not in {-1, 0, 1}:
            raise ValueError("higher_is_better must be -1, 0, or 1")


FACTORS = (
    FactorSpec("pe_percentile_5y", "valuation", ("pe_pct_5y", "is_pe_meaningful", "has_full_pe_5y_history"), -1, "Five-year PE historical percentile; lower is cheaper."),
    FactorSpec("pe_ttm", "valuation", ("pe_ttm", "is_pe_meaningful"), -1, "Trailing PE; positive meaningful values only."),
    FactorSpec("roe_ttm", "quality", (
        "roe_ttm", "roe_quality_status", "financial_feature_contract_version",
        "financial_data_available_timestamp", "financial_vintage_quality",
        "financial_component_lineage", "research_date",
    ), 1, "Parent-profit TTM ROE over average parent equity; uses its own quality status."),
    FactorSpec("debt_to_assets", "quality", (
        "debt_to_assets", "debt_to_assets_quality_status",
        "debt_to_assets_quality_reason", "debt_to_assets_data_available_timestamp",
        "debt_to_assets_source_observed_at", "debt_to_assets_vintage_quality",
        "debt_to_assets_source_record_key", "debt_to_assets_source_record_version",
        "debt_to_assets_source_payload_hash", "debt_to_assets_feature_contract_version",
        "debt_to_assets_source_quality_status", "debt_to_assets_report_date",
        "research_date",
    ), -1, "Balance-sheet liabilities / assets, percentage points, with independent PIT lineage."),
    FactorSpec("revenue_growth_yoy", "growth", (
        "revenue_growth_yoy", "growth_quality_status",
        "financial_feature_contract_version", "financial_data_available_timestamp",
        "financial_vintage_quality", "financial_component_lineage", "research_date",
    ), 1, "Same-report-period revenue growth; this is report YoY, not TTM growth."),
    FactorSpec("profit_growth_yoy", "growth", (
        "profit_growth_yoy", "growth_quality_status",
        "financial_feature_contract_version", "financial_data_available_timestamp",
        "financial_vintage_quality", "financial_component_lineage", "research_date",
    ), 1, "Same-report-period parent-profit growth; this is report YoY, not TTM growth."),
    FactorSpec("return_20d", "trend", (
        "return_20d", "price_feature_version", "price_adjustment_mode",
    ), 1, "20-session PIT total-return momentum from price_daily_v2 asof_close."),
    FactorSpec("return_60d", "trend", (
        "return_60d", "price_feature_version", "price_adjustment_mode",
    ), 1, "60-session PIT total-return momentum from price_daily_v2 asof_close."),
    FactorSpec("close_vs_ma60", "trend", (
        "adj_close_base", "ma60", "price_feature_version", "price_adjustment_mode",
    ), 1, "PIT adjusted close relative to the 60-session moving average."),
    FactorSpec("volume_ratio_20d", "trading_activity", ("volume_ratio_20d",), 0, "Frozen 20-session volume ratio; direction is left empirical."),
)
FACTOR_BY_NAME = {factor.name: factor for factor in FACTORS}
FACTOR_INPUT_FIELDS = frozenset(field for factor in FACTORS for field in factor.input_fields)
LABEL_BAR_FIELDS = frozenset({"raw_open", "raw_close", "quality_status", "is_suspended"})
LABEL_ACTION_FIELDS = frozenset({
    "ex_date", "implementation_status", "stock_div", "stock_bonus_rate",
    "stock_conversion_rate", "cash_div", "source_record_key", "source_record_version",
})


@dataclass(frozen=True, slots=True)
class AlphaResearchSpec:
    dataset_id: str
    signal_start: date
    signal_end: date
    horizons: tuple[int, ...] = HORIZONS
    group_count: int = 5
    bootstrap_repetitions: int = 1_000
    min_bootstrap_dates: int = 60
    page_size: int = 10_000
    seed: int = 20261016

    def __post_init__(self) -> None:
        if not self.dataset_id.strip():
            raise ValueError("dataset_id is required")
        if self.signal_start > self.signal_end:
            raise ValueError("signal_start must not exceed signal_end")
        if not self.horizons or any(horizon not in HORIZONS for horizon in self.horizons):
            raise ValueError(f"horizons must be selected from {HORIZONS}")
        if len(set(self.horizons)) != len(self.horizons):
            raise ValueError("horizons must not contain duplicates")
        object.__setattr__(self, "horizons", tuple(sorted(self.horizons)))
        if self.group_count != 5:
            raise ValueError("factor research currently fixes group_count at 5")
        if self.bootstrap_repetitions < 100:
            raise ValueError("bootstrap_repetitions must be at least 100")
        if self.min_bootstrap_dates < 2:
            raise ValueError("min_bootstrap_dates must be at least 2")
        if not 1 <= self.page_size <= 50_000:
            raise ValueError("page_size must be between 1 and 50000")

    def canonical(self) -> dict[str, Any]:
        value = asdict(self)
        value["signal_start"] = self.signal_start.isoformat()
        value["signal_end"] = self.signal_end.isoformat()
        value["horizons"] = list(self.horizons)
        return value


@dataclass(frozen=True, slots=True)
class ResearchRunIdentity:
    run_id: str
    dataset_id: str
    input_hash: str
    factor_version: str
    label_version: str
    evaluation_version: str
    code_hash: str
    parameter_hash: str

    @classmethod
    def build(
        cls,
        *,
        dataset_id: str,
        input_hash: str,
        spec: AlphaResearchSpec,
        selected_fields: tuple[str, ...],
        schema_versions: tuple[str, ...],
        code_hash: str,
    ) -> ResearchRunIdentity:
        parameters = {
            "spec": spec.canonical(),
            "selected_fields": sorted(selected_fields),
            "schema_versions": sorted(schema_versions),
            "research_version": ALPHA_RESEARCH_VERSION,
            "market_stage_version": MARKET_STAGE_VERSION,
            "code_hash": code_hash,
        }
        parameter_hash = hash_object(parameters)
        run_id = hash_object({
            "dataset_id": dataset_id,
            "input_hash": input_hash,
            "factor_version": FACTOR_SET_VERSION,
            "label_version": LABEL_VERSION,
            "evaluation_version": EVALUATION_VERSION,
            "code_hash": code_hash,
            "parameter_hash": parameter_hash,
        })
        return cls(
            run_id=run_id,
            dataset_id=dataset_id,
            input_hash=input_hash,
            factor_version=FACTOR_SET_VERSION,
            label_version=LABEL_VERSION,
            evaluation_version=EVALUATION_VERSION,
            code_hash=code_hash,
            parameter_hash=parameter_hash,
        )
