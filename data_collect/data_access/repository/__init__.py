"""领域仓储（SET-based SQL 的唯一合法归属，§8）。"""
from data_collect.data_access.repository.base import BaseRepository
from data_collect.data_access.repository.raw_repository import RawRepository
from data_collect.data_access.repository.security_repository import SecurityRepository
from data_collect.data_access.repository.market_repository import MarketRepository
from data_collect.data_access.repository.market_activity_repository import MarketActivityRepository
from data_collect.data_access.repository.market_activity_storage_repository import (
    MarketActivityStorageRepository,
)
from data_collect.data_access.repository.industry_membership_repository import (
    IndustryMembershipRepository,
)
from data_collect.data_access.repository.financial_repository import FinancialRepository
from data_collect.data_access.repository.pit_repository import PitRepository
from data_collect.data_access.repository.feature_repository import FeatureRepository
from data_collect.data_access.repository.snapshot_repository import SnapshotRepository
from data_collect.data_access.repository.dataset_repository import DatasetRepository
from data_collect.data_access.repository.market_coverage_repository import MarketCoverageRepository
from data_collect.data_access.repository.history_repository import HistoryRepository
from data_collect.data_access.repository.raw_standard_repository import RawStandardRepository
from data_collect.data_access.repository.financial_vip_repository import FinancialVipRepository
from data_collect.data_access.repository.quality_inspection_repository import QualityInspectionRepository
from data_collect.data_access.repository.financial_standard_repository import FinancialStandardRepository
from data_collect.data_access.repository.financial_projection_repository import (
    FinancialProjectionRepository,
    FinancialProjectionSelection,
)
from data_collect.data_access.repository.foundation_quality_repository import FoundationQualityRepository
from data_collect.data_access.repository.fina_indicator_repository import FinaIndicatorRepository
from data_collect.data_access.repository.snapshot_series_repository import SnapshotSeriesRepository
from data_collect.data_access.repository.phase2_raw_repository import Phase2RawRepository
from data_collect.data_access.repository.pit_validation_repository import PitValidationRepository
from data_collect.data_access.repository.research_watchlist_repository import ResearchWatchlistRepository
from data_collect.data_access.repository.backtest_run_repository import BacktestRunRepository
from data_collect.data_access.repository.backtest_rules_repository import BacktestRulesRepository
from data_collect.data_access.repository.foundation_contract_repository import FoundationContractRepository
from data_collect.data_access.repository.schema_migration_repository import SchemaMigrationRepository
from data_collect.data_access.repository.retention_archive_repository import RetentionArchiveRepository
from data_collect.data_access.repository.foundation_metadata_repository import FoundationMetadataRepository
from data_collect.data_access.repository.contract_product_repository import ContractProductRepository
from data_collect.data_access.repository.observation_repository import (
    ObservationEventRepository,
    ScenarioEvaluationRepository,
    SectorAnomalyObservationRepository,
    StockAnomalyObservationRepository,
    StockSectorConfirmationRepository,
)
from data_collect.data_access.repository.market_anomaly_outcome_repository import (
    MarketAnomalyOutcomeRepository,
)

__all__ = [
    "BaseRepository",
    "RawRepository",
    "SecurityRepository",
    "MarketRepository",
    "MarketActivityRepository",
    "MarketActivityStorageRepository",
    "IndustryMembershipRepository",
    "FinancialRepository",
    "PitRepository",
    "FeatureRepository",
    "SnapshotRepository",
    "DatasetRepository",
    "MarketCoverageRepository",
    "HistoryRepository",
    "RawStandardRepository",
    "FinancialVipRepository",
    "QualityInspectionRepository",
    "FinancialStandardRepository",
    "FinancialProjectionRepository",
    "FinancialProjectionSelection",
    "FoundationQualityRepository",
    "FinaIndicatorRepository",
    "SnapshotSeriesRepository",
    "Phase2RawRepository",
    "PitValidationRepository",
    "FoundationContractRepository",
    "SchemaMigrationRepository",
    "RetentionArchiveRepository",
    "FoundationMetadataRepository",
    "ContractProductRepository",
    "StockAnomalyObservationRepository",
    "SectorAnomalyObservationRepository",
    "StockSectorConfirmationRepository",
    "ObservationEventRepository",
    "MarketAnomalyOutcomeRepository",
    "ScenarioEvaluationRepository",
    "MarketActivityObservationRepository",
]


def __getattr__(name: str):
    """Load the Activity Observation adapter lazily to avoid import cycles."""
    if name == "MarketActivityObservationRepository":
        from data_collect.data_access.repository.market_activity_observation_repository import (
            MarketActivityObservationRepository,
        )
        return MarketActivityObservationRepository
    raise AttributeError(name)
