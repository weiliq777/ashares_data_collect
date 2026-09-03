"""默认 DatasetPolicy 注册表（迁移旧代码时统一使用）。

按 schema 分类给出默认 policy；精确表名可覆盖通配默认。
原则：Raw 只能 append；FROZEN 表只读；Feature/PIT/Standard 可 upsert/rebuild；
小维度表（ops.*）允许全表读；大表必须带 scope。
"""
from __future__ import annotations

from dataclasses import replace

from data_collect.data_access.dataset_policy import (
    DatasetPolicy,
    DatasetPolicyRegistry,
    ScanPolicy,
    WriteMode,
)


def build_default_registry() -> DatasetPolicyRegistry:
    """构建内置默认 policy 注册表（覆盖现有 A 股数据仓库各 schema）。"""
    r = DatasetPolicyRegistry()
    r.register_many(
        [
            # ---- Raw：只能 append，禁止 update/delete ----
            DatasetPolicy(
                name="raw.*",
                write_modes=frozenset([WriteMode.APPEND_VERSIONED]),
                update_allowed=False,
                delete_allowed=False,
                scan_policy=ScanPolicy.SCOPE_REQUIRED,
            ),
            # ---- Standard：可 upsert/rebuild，大表 ----
            DatasetPolicy(
                name="std.*",
                write_modes=frozenset([WriteMode.UPSERT, WriteMode.REBUILD, WriteMode.BULK_INSERT]),
                scan_policy=ScanPolicy.SCOPE_REQUIRED,
            ),
            # ---- PIT：可 upsert/rebuild ----
            DatasetPolicy(
                name="pit.*",
                write_modes=frozenset([WriteMode.UPSERT, WriteMode.REBUILD]),
                partition_key=("trade_date",),
                require_scope=True,
                scan_policy=ScanPolicy.SCOPE_REQUIRED,
            ),
            # ---- Feature：可 upsert/rebuild ----
            DatasetPolicy(
                name="feature.*",
                write_modes=frozenset([WriteMode.UPSERT, WriteMode.REBUILD]),
                partition_key=("trade_date",),
                require_scope=True,
                scan_policy=ScanPolicy.SCOPE_REQUIRED,
                direct_read_allowed=False,
            ),
            DatasetPolicy(
                name="feature.technical_daily_v1",
                write_modes=frozenset([WriteMode.UPSERT, WriteMode.REBUILD]),
                primary_key=("stock_code", "trade_date"),
                partition_key=("trade_date",),
                require_scope=True,
                scan_policy=ScanPolicy.SCOPE_REQUIRED,
            ),
            DatasetPolicy(
                name="feature.valuation_daily_v1",
                write_modes=frozenset([WriteMode.UPSERT, WriteMode.REBUILD]),
                primary_key=("ts_code", "trade_date"),
                partition_key=("trade_date",),
                require_scope=True,
                scan_policy=ScanPolicy.SCOPE_REQUIRED,
                direct_read_allowed=False,
            ),
            # ---- Mart contract views: application-facing, read-only projections ----
            DatasetPolicy(
                name="mart.*",
                write_modes=frozenset(),
                require_scope=True,
                scan_policy=ScanPolicy.SCOPE_REQUIRED,
            ),
            DatasetPolicy(
                name="mart.v_price_feature",
                write_modes=frozenset(),
                require_scope=True,
                scan_policy=ScanPolicy.SCOPE_REQUIRED,
            ),
            DatasetPolicy(
                name="foundation.*",
                write_modes=frozenset([WriteMode.PUBLISH_ONLY]),
                require_scope=True,
                scan_policy=ScanPolicy.SCOPE_REQUIRED,
            ),
            # ---- Contract（冻结快照/数据集）：只读/PUBLISH_ONLY ----
            DatasetPolicy(
                name="contract.*",
                write_modes=frozenset([WriteMode.PUBLISH_ONLY]),
                immutable_after_freeze=True,
                require_snapshot_id=True,
                scan_policy=ScanPolicy.SCOPE_REQUIRED,
            ),
            DatasetPolicy(
                name="contract.contract_version",
                write_modes=frozenset([WriteMode.PUBLISH_ONLY]),
                update_allowed=False,
                delete_allowed=False,
                require_scope=True,
                scan_policy=ScanPolicy.FULL_SCAN_ALLOWED,
            ),
            DatasetPolicy(
                name="contract.data_dictionary",
                write_modes=frozenset([WriteMode.PUBLISH_ONLY]),
                update_allowed=False,
                delete_allowed=False,
                require_scope=True,
                scan_policy=ScanPolicy.FULL_SCAN_ALLOWED,
            ),
            # ---- ops（维度/元数据/检查点）：小表，允许全表读 ----
            DatasetPolicy(
                name="ops.*",
                write_modes=frozenset([WriteMode.UPSERT, WriteMode.BULK_INSERT]),
                scan_policy=ScanPolicy.FULL_SCAN_ALLOWED,
            ),
            # ---- 交易日历：小表，允许全表读（供读范围，避免大表 MIN/MAX 全扫描）----
            DatasetPolicy(
                name="public.trading_calendar", write_modes=frozenset(),
                require_scope=True, scan_policy=ScanPolicy.FULL_SCAN_ALLOWED,
            ),
            # ---- 迁移源兼容策略（已切换；仅供终态迁移证据） ----
            DatasetPolicy(
                name="public.daily_kline", write_modes=frozenset(),
                require_scope=True, scan_policy=ScanPolicy.SCOPE_REQUIRED,
            ),
            DatasetPolicy(
                name="std.daily_kline",
                write_modes=frozenset([WriteMode.UPSERT, WriteMode.REBUILD, WriteMode.BULK_INSERT]),
                primary_key=("stock_code", "trade_date"),
                partition_key=("trade_date",), require_scope=True,
                scan_policy=ScanPolicy.SCOPE_REQUIRED,
            ),
            DatasetPolicy(
                name="divid_factors", scan_policy=ScanPolicy.SCOPE_REQUIRED,
            ),
            DatasetPolicy(
                name="fin_capital",
                write_modes=frozenset([WriteMode.UPSERT, WriteMode.BULK_INSERT]),
                require_scope=True, scan_policy=ScanPolicy.FULL_SCAN_ALLOWED,
            ),
            DatasetPolicy(
                name="股票全部信息汇总", scan_policy=ScanPolicy.FULL_SCAN_ALLOWED,
            ),
            DatasetPolicy(
                name="wave3_stocks",
                write_modes=frozenset([WriteMode.UPSERT, WriteMode.REBUILD, WriteMode.BULK_INSERT]),
                primary_key=("trade_date", "stock_code"), require_scope=True,
                scan_policy=ScanPolicy.SCOPE_REQUIRED,
            ),
            DatasetPolicy(
                name="etf_info", write_modes=frozenset([WriteMode.UPSERT]),
                primary_key=("code",), require_scope=True, scan_policy=ScanPolicy.SCOPE_REQUIRED,
            ),
            DatasetPolicy(
                name="etf_info_changelog", write_modes=frozenset([WriteMode.BULK_INSERT]),
                require_scope=True, scan_policy=ScanPolicy.SCOPE_REQUIRED,
            ),
            *[
                DatasetPolicy(
                    name=name,
                    write_modes=frozenset([WriteMode.BULK_INSERT, WriteMode.REBUILD]),
                    delete_allowed=name in {"fund_flow_daily", "etf_option_daily"},
                    require_scope=True,
                    scan_policy=ScanPolicy.SCOPE_REQUIRED,
                )
                for name in (
                    "block_trade", "fund_flow_daily", "margin_daily", "etf_divid_factors",
                    "etf_minute", "etf_option_daily", "a_share_minute", "index_minute",
                    "daily_kline_rebuild_validation",
                    "minutedata_tdx",
                )
            ],
            *[
                DatasetPolicy(
                    name=name,
                    write_modes=frozenset([WriteMode.UPSERT, WriteMode.BULK_INSERT]),
                    require_scope=True, scan_policy=ScanPolicy.SCOPE_REQUIRED,
                )
                for name in (
                    "fin_balance", "fin_income", "fin_cashflow", "fin_indicator",
                    "fin_holdernum", "fin_top10_holder", "fin_top10_flow_holder",
                )
            ],
            # ---- 估值原始表（public.valuation_daily）：大表 ----
            DatasetPolicy(
                name="public.valuation_daily", write_modes=frozenset(),
                require_scope=True, scan_policy=ScanPolicy.SCOPE_REQUIRED,
            ),
            DatasetPolicy(
                name="std.valuation_daily",
                write_modes=frozenset([WriteMode.UPSERT, WriteMode.REBUILD, WriteMode.BULK_INSERT]),
                primary_key=("stock_code", "trade_date", "source"),
                require_scope=True,
                scan_policy=ScanPolicy.SCOPE_REQUIRED,
            ),
            # ---- Observation（观察域）与可选 PIT 行业输入 ----
            DatasetPolicy(
                name="pit.industry_membership_history",
                write_modes=frozenset([WriteMode.UPSERT, WriteMode.BULK_INSERT, WriteMode.REBUILD]),
                primary_key=("source", "source_record_key", "record_version"),
                require_scope=True,
                scan_policy=ScanPolicy.SCOPE_REQUIRED,
            ),
            DatasetPolicy(
                name="pit.industry_membership_daily",
                write_modes=frozenset([WriteMode.UPSERT, WriteMode.BULK_INSERT, WriteMode.REBUILD]),
                primary_key=("trade_date", "ts_code", "industry_system", "industry_level"),
                partition_key=("trade_date",),
                require_scope=True,
                scan_policy=ScanPolicy.SCOPE_REQUIRED,
            ),
            DatasetPolicy(
                name="observation.snapshot",
                write_modes=frozenset([WriteMode.UPSERT, WriteMode.BULK_INSERT]),
                primary_key=("snapshot_id",),
                require_scope=True,
                scan_policy=ScanPolicy.SCOPE_REQUIRED,
                direct_read_allowed=False,
            ),
            DatasetPolicy(
                name="observation.stock_anomaly_daily",
                write_modes=frozenset([WriteMode.UPSERT, WriteMode.BULK_INSERT, WriteMode.REBUILD]),
                primary_key=("observation_id",),
                require_scope=True,
                require_snapshot_id=True,
                delete_allowed=True,
                scan_policy=ScanPolicy.SCOPE_REQUIRED,
                direct_read_allowed=False,
            ),
            DatasetPolicy(
                name="observation.stock_anomaly_tag",
                write_modes=frozenset([WriteMode.UPSERT, WriteMode.BULK_INSERT, WriteMode.REBUILD]),
                primary_key=("observation_id", "tag_code"),
                require_scope=True,
                require_snapshot_id=True,
                delete_allowed=True,
                scan_policy=ScanPolicy.SCOPE_REQUIRED,
                direct_read_allowed=False,
            ),
            DatasetPolicy(
                name="observation.sector_anomaly_daily",
                write_modes=frozenset([WriteMode.UPSERT, WriteMode.BULK_INSERT, WriteMode.REBUILD]),
                primary_key=("snapshot_id", "observation_date", "industry_system", "industry_level", "industry_code"),
                require_scope=True,
                require_snapshot_id=True,
                delete_allowed=True,
                scan_policy=ScanPolicy.SCOPE_REQUIRED,
                direct_read_allowed=False,
            ),
            DatasetPolicy(
                name="observation.stock_sector_confirmation",
                write_modes=frozenset([WriteMode.UPSERT, WriteMode.BULK_INSERT, WriteMode.REBUILD]),
                primary_key=("observation_id",),
                require_scope=True,
                require_snapshot_id=True,
                delete_allowed=True,
                scan_policy=ScanPolicy.SCOPE_REQUIRED,
                direct_read_allowed=False,
            ),
            DatasetPolicy(
                name="observation.event_link",
                write_modes=frozenset([WriteMode.UPSERT, WriteMode.BULK_INSERT]),
                primary_key=("observation_id", "source_record_id", "source_hash"),
                require_scope=True,
                require_snapshot_id=True,
                scan_policy=ScanPolicy.SCOPE_REQUIRED,
                direct_read_allowed=False,
            ),
            DatasetPolicy(
                name="evaluation.retrospective_event_link",
                write_modes=frozenset([WriteMode.UPSERT, WriteMode.BULK_INSERT]),
                primary_key=("observation_id", "source_record_id", "source_hash"),
                require_scope=True,
                require_snapshot_id=True,
                scan_policy=ScanPolicy.SCOPE_REQUIRED,
                direct_read_allowed=False,
            ),
            DatasetPolicy(
                name="evaluation.market_anomaly_outcome",
                write_modes=frozenset([WriteMode.UPSERT, WriteMode.BULK_INSERT]),
                primary_key=("observation_id", "evaluation_methodology_hash"),
                require_scope=True,
                require_snapshot_id=True,
                scan_policy=ScanPolicy.SCOPE_REQUIRED,
                direct_read_allowed=False,
            ),
            DatasetPolicy(
                name="evaluation.scenario_result",
                write_modes=frozenset([WriteMode.UPSERT, WriteMode.BULK_INSERT]),
                primary_key=("observation_id", "scenario_methodology_hash"),
                require_scope=True,
                require_snapshot_id=True,
                scan_policy=ScanPolicy.SCOPE_REQUIRED,
                direct_read_allowed=False,
            ),
            DatasetPolicy(
                name="public.adj_factor_daily", write_modes=frozenset(),
                require_scope=True, scan_policy=ScanPolicy.SCOPE_REQUIRED,
            ),
            DatasetPolicy(
                name="std.adj_factor_daily",
                write_modes=frozenset([WriteMode.UPSERT, WriteMode.REBUILD, WriteMode.BULK_INSERT]),
                primary_key=("stock_code", "trade_date", "source"),
                require_scope=True,
                scan_policy=ScanPolicy.SCOPE_REQUIRED,
            ),
            # Explicit, read-only migration sources.  These names are kept
            # only for a pre-cutover run; no application writer may use them.
            *[
                DatasetPolicy(
                    name=f"public.{name}", write_modes=frozenset(),
                    require_scope=True, scan_policy=ScanPolicy.SCOPE_REQUIRED,
                )
                for name in (
                    "financial_income_standard", "financial_cash_flow_standard",
                    "financial_balance_sheet_standard", "fina_indicator_standard",
                )
            ],
            # ---- Standard financial canonical targets (std schema) ----
            DatasetPolicy(
                name="std.financial_income_standard",
                write_modes=frozenset([WriteMode.UPSERT, WriteMode.REBUILD, WriteMode.BULK_INSERT]),
                # The surrogate financial_id is an identity column, not the
                # idempotency key.  Raw version projection conflicts on the
                # immutable source identity below.
                primary_key=("source", "source_record_key", "record_version"), require_scope=True,
                scan_policy=ScanPolicy.SCOPE_REQUIRED,
            ),
            DatasetPolicy(
                name="std.financial_cash_flow_standard",
                write_modes=frozenset([WriteMode.UPSERT, WriteMode.REBUILD, WriteMode.BULK_INSERT]),
                primary_key=("source", "source_record_key", "record_version"), require_scope=True,
                scan_policy=ScanPolicy.SCOPE_REQUIRED,
            ),
            DatasetPolicy(
                name="std.financial_balance_sheet_standard",
                write_modes=frozenset([WriteMode.UPSERT, WriteMode.REBUILD, WriteMode.BULK_INSERT]),
                primary_key=("source", "source_record_key", "record_version"), require_scope=True,
                scan_policy=ScanPolicy.SCOPE_REQUIRED,
            ),
            DatasetPolicy(
                name="std.fina_indicator_standard",
                write_modes=frozenset([WriteMode.UPSERT, WriteMode.REBUILD, WriteMode.BULK_INSERT]),
                # This table retains payload_hash in its deployed unique
                # index because the source identity can have multiple
                # immutable payload versions.
                primary_key=("source", "source_record_key", "record_version", "payload_hash"), require_scope=True,
                scan_policy=ScanPolicy.SCOPE_REQUIRED,
            ),
            DatasetPolicy(
                name="feature.feature_registry",
                write_modes=frozenset([WriteMode.BULK_INSERT]),
                primary_key=("feature_name", "feature_version"),
                update_allowed=False,
                delete_allowed=False,
                scan_policy=ScanPolicy.FULL_SCAN_ALLOWED,
                direct_read_allowed=True,
            ),
            DatasetPolicy(
                name="ops.source_registry",
                write_modes=frozenset([WriteMode.UPSERT]),
                primary_key=("source_id",),
                scan_policy=ScanPolicy.FULL_SCAN_ALLOWED,
            ),
            DatasetPolicy(
                name="ops.change_event",
                write_modes=frozenset([WriteMode.UPSERT, WriteMode.BULK_INSERT]),
                primary_key=("event_id",),
                require_scope=True,
                scan_policy=ScanPolicy.SCOPE_REQUIRED,
            ),
            DatasetPolicy(
                name="ops.run_manifest",
                write_modes=frozenset([WriteMode.UPSERT]),
                primary_key=("run_id",),
                require_scope=True,
                scan_policy=ScanPolicy.SCOPE_REQUIRED,
            ),
            DatasetPolicy(
                name="ops.data_release_event",
                write_modes=frozenset(),
                primary_key=("event_id",),
                require_scope=True,
                scan_policy=ScanPolicy.SCOPE_REQUIRED,
            ),
            DatasetPolicy(
                name="ops.data_quality_result",
                write_modes=frozenset([WriteMode.BULK_INSERT]),
                primary_key=("quality_result_id",),
                require_scope=True,
                scan_policy=ScanPolicy.SCOPE_REQUIRED,
            ),
            DatasetPolicy(
                name="contract.snapshot_manifest",
                write_modes=frozenset([WriteMode.UPSERT]),
                primary_key=("snapshot_id",),
                require_scope=True,
                scan_policy=ScanPolicy.SCOPE_REQUIRED,
                immutable_after_freeze=True,
            ),
            DatasetPolicy(
                name="contract.stock_snapshot_daily",
                write_modes=frozenset([WriteMode.PUBLISH_ONLY]),
                primary_key=("snapshot_id", "research_date", "ts_code"),
                partition_key=("research_date",),
                require_scope=True,
                require_snapshot_id=True,
                update_allowed=False,
                delete_allowed=False,
                scan_policy=ScanPolicy.SCOPE_REQUIRED,
                immutable_after_freeze=True,
            ),
            DatasetPolicy(
                name="contract.dataset_release",
                write_modes=frozenset([WriteMode.UPSERT]),
                primary_key=("dataset_id",),
                require_scope=True,
                scan_policy=ScanPolicy.SCOPE_REQUIRED,
                immutable_after_freeze=True,
            ),
            DatasetPolicy(
                name="ops.foundation_rebuild_run",
                write_modes=frozenset([WriteMode.UPSERT]),
                scan_policy=ScanPolicy.FULL_SCAN_ALLOWED,
            ),
            DatasetPolicy(
                name="ops.foundation_rebuild_checkpoint",
                write_modes=frozenset([WriteMode.UPSERT]),
                scan_policy=ScanPolicy.FULL_SCAN_ALLOWED,
            ),
            DatasetPolicy(
                name="ops.foundation_table_migration",
                write_modes=frozenset([WriteMode.UPSERT]),
                primary_key=("migration_id",),
                require_scope=True,
                scan_policy=ScanPolicy.FULL_SCAN_ALLOWED,
            ),
            DatasetPolicy(
                name="backtest.*",
                write_modes=frozenset([WriteMode.UPSERT, WriteMode.REBUILD, WriteMode.BULK_INSERT]),
                require_scope=True,
                scan_policy=ScanPolicy.SCOPE_REQUIRED,
            ),
            DatasetPolicy(
                name="backtest.app_strategy_version",
                write_modes=frozenset([WriteMode.BULK_INSERT]),
                update_allowed=False,
                delete_allowed=False,
                require_scope=True,
                scan_policy=ScanPolicy.FULL_SCAN_ALLOWED,
            ),
            DatasetPolicy(
                name="backtest.app_execution_params",
                write_modes=frozenset([WriteMode.BULK_INSERT]),
                update_allowed=False,
                delete_allowed=False,
                require_scope=True,
                scan_policy=ScanPolicy.FULL_SCAN_ALLOWED,
            ),
            DatasetPolicy(
                name="backtest.strategy_definition",
                write_modes=frozenset(),
                require_scope=True,
                scan_policy=ScanPolicy.SCOPE_REQUIRED,
            ),
            DatasetPolicy(
                name="portfolio.*",
                write_modes=frozenset([WriteMode.UPSERT, WriteMode.BULK_INSERT]),
                require_scope=True,
                scan_policy=ScanPolicy.SCOPE_REQUIRED,
            ),
            DatasetPolicy(
                name="research.*",
                scan_policy=ScanPolicy.SCOPE_REQUIRED,
            ),
            DatasetPolicy(
                name="research.snapshot_manifest",
                write_modes=frozenset([WriteMode.UPSERT]),
                primary_key=("snapshot_id",),
                require_scope=True,
                scan_policy=ScanPolicy.SCOPE_REQUIRED,
            ),
            DatasetPolicy(
                name="research.stock_snapshot_daily",
                write_modes=frozenset([WriteMode.REBUILD, WriteMode.BULK_INSERT]),
                require_scope=True,
                require_snapshot_id=True,
                delete_allowed=True,
                scan_policy=ScanPolicy.SCOPE_REQUIRED,
            ),
            DatasetPolicy(
                name="research.stock_snapshot_daily_partitioned",
                write_modes=frozenset([WriteMode.REBUILD, WriteMode.BULK_INSERT]),
                primary_key=("snapshot_id", "research_date", "ts_code"),
                partition_key=("research_date",),
                require_scope=True,
                # Physical migration windows intentionally contain every
                # snapshot for a date; this is not a business snapshot write.
                require_snapshot_id=False,
                delete_allowed=True,
                scan_policy=ScanPolicy.SCOPE_REQUIRED,
            ),
            DatasetPolicy(
                name="research.snapshot_lineage",
                write_modes=frozenset([WriteMode.REBUILD, WriteMode.BULK_INSERT]),
                require_scope=True,
                require_snapshot_id=True,
                delete_allowed=True,
                scan_policy=ScanPolicy.SCOPE_REQUIRED,
            ),
            DatasetPolicy(
                name="research.lineage_source_record",
                # Source records are content-addressed and append-only.  The
                # REBUILD mode is reserved for the named lineage-bundle
                # gateway, which may insert missing dictionary entries while
                # materializing one bounded snapshot/code slice.
                write_modes=frozenset([WriteMode.REBUILD, WriteMode.BULK_INSERT]),
                primary_key=("source_ref_id",),
                require_scope=True,
                require_snapshot_id=False,
                update_allowed=False,
                delete_allowed=False,
                scan_policy=ScanPolicy.SCOPE_REQUIRED,
            ),
            DatasetPolicy(
                name="research.snapshot_lineage_bundle",
                write_modes=frozenset([WriteMode.REBUILD, WriteMode.BULK_INSERT]),
                primary_key=("research_date", "snapshot_id", "ts_code"),
                partition_key=("research_date",),
                require_scope=True,
                require_snapshot_id=True,
                delete_allowed=True,
                scan_policy=ScanPolicy.SCOPE_REQUIRED,
            ),
            DatasetPolicy(
                name="research.snapshot_input",
                write_modes=frozenset([WriteMode.REBUILD, WriteMode.BULK_INSERT]),
                require_scope=True,
                require_snapshot_id=True,
                delete_allowed=True,
                scan_policy=ScanPolicy.SCOPE_REQUIRED,
            ),
            DatasetPolicy(
                name="research.watchlist",
                write_modes=frozenset([WriteMode.UPSERT]),
                primary_key=("watchlist_id",), require_scope=True,
                scan_policy=ScanPolicy.SCOPE_REQUIRED,
            ),
            DatasetPolicy(
                name="research.watchlist_member",
                write_modes=frozenset([WriteMode.BULK_INSERT]),
                require_scope=True, require_snapshot_id=True,
                scan_policy=ScanPolicy.SCOPE_REQUIRED,
            ),
            DatasetPolicy(
                name="research.evidence_reference",
                write_modes=frozenset([WriteMode.BULK_INSERT]),
                require_scope=True, require_snapshot_id=True,
                scan_policy=ScanPolicy.SCOPE_REQUIRED,
            ),
            DatasetPolicy(
                name="research.candidate_run",
                write_modes=frozenset([WriteMode.UPSERT, WriteMode.BULK_INSERT]),
                primary_key=("candidate_run_id",), require_scope=True,
                scan_policy=ScanPolicy.SCOPE_REQUIRED,
            ),
            DatasetPolicy(
                name="research.candidate",
                write_modes=frozenset([WriteMode.BULK_INSERT]),
                primary_key=("candidate_id",), require_scope=True,
                scan_policy=ScanPolicy.SCOPE_REQUIRED,
            ),
            DatasetPolicy(
                name="research.ai_audit",
                write_modes=frozenset([WriteMode.BULK_INSERT]),
                primary_key=("audit_id",), require_scope=True,
                scan_policy=ScanPolicy.SCOPE_REQUIRED,
            ),
            DatasetPolicy(
                name="research.human_decision",
                write_modes=frozenset([WriteMode.BULK_INSERT]),
                primary_key=("decision_id",), require_scope=True,
                scan_policy=ScanPolicy.SCOPE_REQUIRED,
            ),
            DatasetPolicy(
                name="portfolio.snapshot_binding",
                write_modes=frozenset([WriteMode.UPSERT, WriteMode.BULK_INSERT]),
                primary_key=("portfolio_id", "valuation_date"), require_scope=True,
                scan_policy=ScanPolicy.SCOPE_REQUIRED,
            ),
            DatasetPolicy(
                name="portfolio.order_intent",
                write_modes=frozenset([WriteMode.UPSERT, WriteMode.BULK_INSERT]),
                primary_key=("intent_id", "portfolio_id"), require_scope=True,
                scan_policy=ScanPolicy.SCOPE_REQUIRED,
            ),
            DatasetPolicy(
                name="portfolio.audit_event",
                write_modes=frozenset([WriteMode.BULK_INSERT]),
                primary_key=("event_id",), require_scope=True,
                scan_policy=ScanPolicy.SCOPE_REQUIRED,
            ),
            DatasetPolicy(
                name="backtest.market_rule_effective",
                write_modes=frozenset(),
                require_scope=True, scan_policy=ScanPolicy.SCOPE_REQUIRED,
            ),
            DatasetPolicy(
                name="information_schema.columns",
                scan_policy=ScanPolicy.FULL_SCAN_ALLOWED,
            ),
            DatasetPolicy(
                name="public.stock_basic_info", write_modes=frozenset(),
                require_scope=True, scan_policy=ScanPolicy.FULL_SCAN_ALLOWED,
            ),
            DatasetPolicy(
                name="std.stock_basic_info",
                write_modes=frozenset([WriteMode.UPSERT, WriteMode.REBUILD, WriteMode.BULK_INSERT]),
                primary_key=("stock_code",), require_scope=True,
                scan_policy=ScanPolicy.FULL_SCAN_ALLOWED,
            ),
            DatasetPolicy(
                name="tushare_financial_raw_version",
                write_modes=frozenset([WriteMode.APPEND_VERSIONED]),
                primary_key=("dataset", "business_key", "payload_hash"),
                update_allowed=False,
                delete_allowed=False,
                require_scope=True,
                scan_policy=ScanPolicy.SCOPE_REQUIRED,
            ),
            DatasetPolicy(
                name="fina_indicator_raw_version",
                write_modes=frozenset([WriteMode.APPEND_VERSIONED]),
                primary_key=("business_key", "payload_hash"),
                update_allowed=False,
                delete_allowed=False,
                require_scope=True,
                scan_policy=ScanPolicy.SCOPE_REQUIRED,
            ),
            DatasetPolicy(
                name="tushare_market_raw_version",
                write_modes=frozenset([WriteMode.APPEND_VERSIONED]),
                primary_key=("dataset", "business_key", "payload_hash"),
                update_allowed=False,
                delete_allowed=False,
                require_scope=True,
                scan_policy=ScanPolicy.SCOPE_REQUIRED,
            ),
            DatasetPolicy(
                name="tushare_daily_raw",
                write_modes=frozenset([WriteMode.APPEND_VERSIONED]),
                require_scope=True,
                scan_policy=ScanPolicy.SCOPE_REQUIRED,
            ),
            DatasetPolicy(
                name="tushare_daily_basic_raw",
                write_modes=frozenset([WriteMode.APPEND_VERSIONED]),
                require_scope=True,
                scan_policy=ScanPolicy.SCOPE_REQUIRED,
            ),
            DatasetPolicy(
                name="tushare_adj_factor_raw",
                write_modes=frozenset([WriteMode.APPEND_VERSIONED]),
                require_scope=True,
                scan_policy=ScanPolicy.SCOPE_REQUIRED,
            ),
            DatasetPolicy(
                name="tushare_trade_cal_raw",
                write_modes=frozenset([WriteMode.APPEND_VERSIONED]),
                require_scope=True,
                scan_policy=ScanPolicy.SCOPE_REQUIRED,
            ),
            DatasetPolicy(
                name="ops.tushare_history_checkpoint",
                write_modes=frozenset([WriteMode.UPSERT]),
                primary_key=("dataset", "checkpoint_key"),
                require_scope=True,
                scan_policy=ScanPolicy.SCOPE_REQUIRED,
            ),
            DatasetPolicy(
                name="ops.financial_ingest_anomaly",
                write_modes=frozenset([WriteMode.BULK_INSERT]),
                require_scope=True,
                scan_policy=ScanPolicy.FULL_SCAN_ALLOWED,
            ),
            DatasetPolicy(
                name="ops.financial_change_queue",
                write_modes=frozenset([WriteMode.UPSERT, WriteMode.BULK_INSERT]),
                primary_key=("dataset", "business_key", "payload_hash"),
                require_scope=True,
                scan_policy=ScanPolicy.SCOPE_REQUIRED,
            ),
            DatasetPolicy(
                name="ops.financial_version_cutover_watermark",
                scan_policy=ScanPolicy.FULL_SCAN_ALLOWED,
            ),
            DatasetPolicy(
                name="ops.market_data_quality_gate",
                write_modes=frozenset([WriteMode.UPSERT]),
                primary_key=("dataset", "trade_date", "batch_id"),
                require_scope=True,
                scan_policy=ScanPolicy.FULL_SCAN_ALLOWED,
            ),
            DatasetPolicy(
                name="ops.tushare_data_release",
                write_modes=frozenset([WriteMode.UPSERT]),
                primary_key=("release_tag",),
                require_scope=True,
                scan_policy=ScanPolicy.FULL_SCAN_ALLOWED,
            ),
            DatasetPolicy(
                name="tushare_stock_basic_raw_version",
                write_modes=frozenset([WriteMode.UPSERT]),
                primary_key=("business_key", "payload_hash"),
                require_scope=True,
                scan_policy=ScanPolicy.SCOPE_REQUIRED,
            ),
            # Snapshot input definitions use the fully qualified public name;
            # keep an explicit alias so fail-closed policy lookup does not
            # depend on search_path or an implicit schema fallback.
            DatasetPolicy(
                name="public.tushare_stock_basic_raw_version",
                write_modes=frozenset([WriteMode.UPSERT]),
                primary_key=("business_key", "payload_hash"),
                require_scope=True,
                scan_policy=ScanPolicy.SCOPE_REQUIRED,
            ),
            DatasetPolicy(
                name="tushare_stock_basic_raw",
                write_modes=frozenset([WriteMode.BULK_INSERT]),
                primary_key=("ts_code",), require_scope=True,
                update_allowed=False, delete_allowed=False,
                scan_policy=ScanPolicy.SCOPE_REQUIRED,
            ),
            DatasetPolicy(
                name="etf_daily_kline",
                write_modes=frozenset([WriteMode.UPSERT]),
                primary_key=("code", "trade_date"),
                require_scope=True,
                scan_policy=ScanPolicy.SCOPE_REQUIRED,
            ),
            DatasetPolicy(
                name="index_daily",
                write_modes=frozenset([WriteMode.UPSERT]),
                primary_key=("index_code", "trade_date"),
                require_scope=True,
                scan_policy=ScanPolicy.SCOPE_REQUIRED,
            ),
            DatasetPolicy(
                name="etf_nav",
                write_modes=frozenset([WriteMode.UPSERT]),
                primary_key=("code", "trade_date"),
                require_scope=True,
                scan_policy=ScanPolicy.SCOPE_REQUIRED,
            ),
            DatasetPolicy(
                name="limit_pool",
                write_modes=frozenset([WriteMode.REBUILD]),
                require_scope=True,
                scan_policy=ScanPolicy.SCOPE_REQUIRED,
            ),
            DatasetPolicy(
                name="limit_up_reason",
                write_modes=frozenset([WriteMode.REBUILD]),
                require_scope=True,
                scan_policy=ScanPolicy.SCOPE_REQUIRED,
            ),
            DatasetPolicy(
                name="public.stock_financial_indicator",
                write_modes=frozenset(), require_scope=True,
                scan_policy=ScanPolicy.SCOPE_REQUIRED,
            ),
            DatasetPolicy(
                name="std.stock_financial_indicator",
                write_modes=frozenset([WriteMode.UPSERT, WriteMode.REBUILD, WriteMode.BULK_INSERT]),
                primary_key=("stock_code", "report_date", "announce_date", "source"),
                require_scope=True, scan_policy=ScanPolicy.SCOPE_REQUIRED,
            ),
            DatasetPolicy(
                name="sector_stock",
                write_modes=frozenset([WriteMode.REBUILD]),
                require_scope=True,
                scan_policy=ScanPolicy.FULL_SCAN_ALLOWED,
            ),
            DatasetPolicy(
                name="sector_changelog",
                write_modes=frozenset([WriteMode.REBUILD]),
                require_scope=True,
                scan_policy=ScanPolicy.FULL_SCAN_ALLOWED,
            ),
            DatasetPolicy(
                name="index_weight",
                write_modes=frozenset([WriteMode.REBUILD]),
                require_scope=True,
                scan_policy=ScanPolicy.FULL_SCAN_ALLOWED,
            ),
            DatasetPolicy(
                name="index_weight_changelog",
                write_modes=frozenset([WriteMode.REBUILD]),
                require_scope=True,
                scan_policy=ScanPolicy.FULL_SCAN_ALLOWED,
            ),
            DatasetPolicy(
                name="instrument_info",
                write_modes=frozenset([WriteMode.UPSERT]),
                primary_key=("stock_code",),
                require_scope=True,
                scan_policy=ScanPolicy.FULL_SCAN_ALLOWED,
            ),
            DatasetPolicy(
                name="instrument_changelog",
                write_modes=frozenset([WriteMode.UPSERT]),
                primary_key=("stock_code", "changed_at", "field_name"),
                require_scope=True,
                scan_policy=ScanPolicy.FULL_SCAN_ALLOWED,
            ),
            DatasetPolicy(
                name="tushare_fina_indicator_raw",
                write_modes=frozenset([WriteMode.APPEND_VERSIONED]),
                require_scope=True,
                scan_policy=ScanPolicy.SCOPE_REQUIRED,
            ),
            DatasetPolicy(
                name="tushare_income_raw",
                write_modes=frozenset([WriteMode.APPEND_VERSIONED]),
                require_scope=True,
                scan_policy=ScanPolicy.SCOPE_REQUIRED,
            ),
            DatasetPolicy(
                name="tushare_balancesheet_raw",
                write_modes=frozenset([WriteMode.APPEND_VERSIONED]),
                require_scope=True,
                scan_policy=ScanPolicy.SCOPE_REQUIRED,
            ),
            DatasetPolicy(
                name="tushare_cashflow_raw",
                write_modes=frozenset([WriteMode.APPEND_VERSIONED]),
                require_scope=True,
                scan_policy=ScanPolicy.SCOPE_REQUIRED,
            ),
            # Phase-2 append-only Raw sources.  These exact policies keep
            # quality/recovery reads fail-closed without granting them a
            # write mode that the source contract does not define.
            *[
                DatasetPolicy(
                    name=table,
                    write_modes=frozenset([WriteMode.APPEND_VERSIONED]),
                    update_allowed=False,
                    delete_allowed=False,
                    require_scope=True,
                    scan_policy=ScanPolicy.SCOPE_REQUIRED,
                )
                for table in (
                    "tushare_stock_st_raw",
                    "tushare_namechange_raw",
                    "tushare_suspend_d_raw",
                    "tushare_stk_limit_raw",
                    "tushare_dividend_raw",
                    "tushare_index_daily_raw",
                )
            ],
        ]
    )
    # Schema defaults remain available for legacy read compatibility, but all
    # concrete write targets used by the migrated repositories are materialized
    # as table-level policies. QueryGuard rejects writes that only match a
    # wildcard, so a new table cannot silently inherit write permissions.
    concrete_targets = (
        "backtest.dataset_benchmark_bar", "backtest.dataset_build_batch_checkpoint",
        "backtest.dataset_build_checkpoint", "backtest.dataset_calendar",
        "backtest.dataset_dividend_event", "backtest.dataset_execution_bar",
        "backtest.dataset_manifest", "backtest.dataset_member",
        "backtest.dataset_rule_snapshot", "backtest.run_manifest",
        "backtest.dataset_validation", "backtest.order", "backtest.signal",
        "backtest.fill", "backtest.position_lot", "backtest.position_daily",
        "backtest.cash_ledger", "backtest.performance_daily", "backtest.metric",
        "feature.company_financial_v1", "feature.company_financial_v2",
        "feature.valuation_daily_v1", "feature.price_adjustment_factor_asof",
        "feature.price_daily_v1", "feature.price_daily_v2",
        "feature.shareholder_return_v1", "foundation.market_rule_effective",
        "foundation.tax_rule_effective",
        "ops.trading_day_build", "pit.financial_asof",
        "pit.security_tradeability_daily", "pit.universe_daily",
        "portfolio.account_manifest", "portfolio.cash_ledger",
        "portfolio.performance_daily", "portfolio.position_daily",
        "portfolio.position_lot", "portfolio.trade_fill",
        "std.dividend_event", "std.index_daily_tushare",
        "std.security_lifecycle", "std.security_status_daily",
        "std.trading_calendar",
    )
    partition_overrides = {
        "feature.company_financial_v1": ("report_date",),
        "feature.company_financial_v2": ("report_date",),
        "feature.shareholder_return_v1": ("asof_date",),
    }
    for name in concrete_targets:
        if name in r.names():
            continue
        inherited = r.get(name)
        if inherited is None:
            raise RuntimeError(f"缺少具体 DatasetPolicy: {name}")
        r.register(
            replace(
                inherited,
                name=name,
                partition_key=partition_overrides.get(name, inherited.partition_key),
            )
        )
    return r
