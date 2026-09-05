"""Foundation data-quality orchestration.

The job owns no database handle and contains no SQL.  Inspection semantics
belong to ``QualityInspectionRepository``; this module only assembles the
run context and combines its bounded reports.
"""
from __future__ import annotations

import argparse
import json
from uuid import uuid4

from data_collect.data_access import DataAccessKernel, RunContext
from data_collect.data_access.policy_catalog import build_default_registry
from data_collect.data_access.repository.quality_inspection_repository import QualityInspectionRepository


CHECKS = {
    "std.stock_basic_info": ("stock_code",),
    "tushare_stock_basic_raw": ("ts_code",),
    "std.trading_calendar": ("exchange", "trade_date", "source"),
    "tushare_daily_raw": ("ts_code", "trade_date"),
    "tushare_daily_basic_raw": ("ts_code", "trade_date"),
    "tushare_adj_factor_raw": ("ts_code", "trade_date"),
    "tushare_trade_cal_raw": ("exchange", "cal_date"),
    "tushare_market_raw_version": ("dataset", "business_key", "payload_hash"),
    # Canonical Standard relations after the physical cutover.  The
    # repository still accepts the old logical aliases for explicitly
    # requested compatibility checks, but the scheduled quality inventory
    # must never target removed public.* relations.
    "std.daily_kline": ("stock_code", "trade_date"),
    "std.valuation_daily": ("stock_code", "trade_date", "source"),
    "std.adj_factor_daily": ("stock_code", "trade_date", "source"),
    "std.stock_financial_indicator": ("stock_code", "report_date", "announce_date", "source"),
    "tushare_fina_indicator_raw": ("ts_code", "ann_date", "end_date"),
    "tushare_income_raw": ("ts_code", "ann_date", "end_date"),
    "tushare_balancesheet_raw": ("ts_code", "ann_date", "end_date"),
    "tushare_cashflow_raw": ("ts_code", "ann_date", "end_date"),
    "tushare_financial_raw_version": ("dataset", "business_key", "payload_hash"),
    "fina_indicator_raw_version": ("business_key", "payload_hash"),
    "std.fina_indicator_standard": ("source", "source_record_key", "record_version", "payload_hash"),
    "std.financial_income_standard": ("source", "source_record_key", "record_version"),
    "std.financial_balance_sheet_standard": ("source", "source_record_key", "record_version"),
    "std.financial_cash_flow_standard": ("source", "source_record_key", "record_version"),
    "tushare_stock_st_raw": ("ts_code", "trade_date", "type", "payload_hash"),
    "tushare_namechange_raw": ("ts_code", "payload_hash"),
    "tushare_suspend_d_raw": ("ts_code", "trade_date", "suspend_type", "payload_hash"),
    "tushare_stk_limit_raw": ("ts_code", "trade_date", "payload_hash"),
    "tushare_dividend_raw": ("ts_code", "payload_hash"),
    "tushare_index_daily_raw": ("ts_code", "trade_date", "payload_hash"),
    "std.security_lifecycle": ("ts_code", "effective_from", "effective_to", "source"),
    "std.security_status_daily": ("trade_date", "ts_code"),
    "std.dividend_event": ("source", "source_record_key", "record_version"),
    "std.index_daily_tushare": ("ts_code", "trade_date", "source"),
    "pit.universe_daily": ("trade_date", "ts_code"),
    "pit.security_tradeability_daily": ("trade_date", "ts_code"),
    "pit.financial_asof": ("decision_date", "ts_code"),
    "feature.price_daily_v1": ("ts_code", "trade_date"),
    "feature.price_adjustment_factor_asof": ("ts_code", "trade_date"),
    "feature.price_daily_v2": ("ts_code", "trade_date"),
    "feature.valuation_daily_v1": ("ts_code", "trade_date"),
    "feature.company_financial_v1": ("ts_code", "report_date", "source_record_key", "record_version"),
    "feature.company_financial_v2": ("ts_code", "report_date", "data_available_timestamp", "source_fingerprint"),
    "feature.shareholder_return_v1": ("ts_code", "asof_date"),
    "feature.feature_registry": ("feature_name", "feature_version"),
}


def _service(label: str):
    kernel = DataAccessKernel(policy_registry=build_default_registry())
    context = RunContext(
        run_id=f"quality-{label}-{uuid4().hex}",
        job_name="tushare_data_quality",
        code_version="unknown",
        config_hash="unknown",
        pipeline_version="data-access-v1",
    )
    return kernel, QualityInspectionRepository(kernel), context


def inspect_table(table: str, keys: tuple[str, ...]) -> dict:
    kernel, repository, context = _service(f"table-{table}")
    try:
        return repository.inspect_table(table, keys, context)
    finally:
        kernel.close()


def inspect_phase2_checkpoint_completeness() -> dict:
    kernel, repository, context = _service("phase2-checkpoints")
    try:
        return repository.phase2_checkpoint_completeness(context)
    finally:
        kernel.close()


def inspect_phase2_invariants(cutoff=None) -> dict:
    kernel, repository, context = _service("phase2-invariants")
    try:
        return repository.phase2_invariants(context, cutoff)
    finally:
        kernel.close()


def run() -> dict:
    kernel, repository, context = _service("run")
    try:
        result = {"checkpoints": repository.checkpoint_summary(context)}
        for table, keys in CHECKS.items():
            try:
                result[table] = repository.inspect_table(table, keys, context)
            except Exception as exc:
                result[table] = {"error": str(exc)}
        result["phase2_invariants"] = repository.phase2_invariants(context)
        result["phase2_checkpoint_completeness"] = repository.phase2_checkpoint_completeness(context)
        return result
    finally:
        kernel.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Inspect Tushare Raw/Standard quality")
    parser.parse_args()
    print(json.dumps(run(), ensure_ascii=False, indent=2))
