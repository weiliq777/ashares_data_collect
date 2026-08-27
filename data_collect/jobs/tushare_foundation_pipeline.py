"""Orchestrate resumable Foundation rebuilds through the Access Framework."""
from __future__ import annotations

import argparse
import json
import uuid
from contextlib import contextmanager
from datetime import date, timedelta

from data_collect.data_access import DataAccessKernel, RunContext
from data_collect.data_access.policy_catalog import build_default_registry
from data_collect.data_access.repository.financial_repository import FinancialRepository
from data_collect.jobs.tushare_fina_indicator_version import _run_unlocked as run_indicator_standard
from data_collect.jobs.tushare_financial_raw_to_standard import _run_unlocked as run_statement_standard
from data_collect.jobs.tushare_foundation_features import (
    DEFAULT_FEATURE_BATCH_STOCKS,
    create_run,
    create_smoke_target,
    create_valuation_smoke_target,
    rebuild_company_financial_v2,
    rebuild_valuation_features,
)
from data_collect.jobs.tushare_foundation_quality import inspect_foundation_v2
from data_collect.jobs.tushare_raw_to_standard import rebuild_valuation


FIXED_SMOKE_CODES = ("600519.SH", "000858.SZ", "000001.SZ")
WARMUP_START = date(2016, 8, 1)
OUTPUT_START = date(2021, 8, 2)


def _pipeline_access(run_id: str | None = None):
    context = RunContext(
        run_id=run_id or uuid.uuid4().hex,
        job_name="tushare_foundation_pipeline",
        code_version="unknown",
        config_hash="unknown",
        pipeline_version="feature-contract-v2.0",
    )
    kernel = DataAccessKernel(policy_registry=build_default_registry())
    return kernel, FinancialRepository(kernel), context


@contextmanager
def _run_lock(mode: str):
    kernel, repository, _context = _pipeline_access()
    try:
        with repository.foundation_lock(mode):
            yield
    finally:
        kernel.close()


def assert_migration_applied() -> None:
    required = {
        ("std", "financial_income_standard", "net_profit_consolidated"),
        ("std", "valuation_daily", "dv_ttm_vendor"),
        ("feature", "valuation_daily_v1", "dv_ttm_percentile_5y"),
        ("feature", "company_financial_v2", "feature_contract_version"),
        ("ops", "foundation_rebuild_run", "run_id"),
    }
    kernel, repository, context = _pipeline_access()
    try:
        repository.assert_foundation_schema(required, context)
    finally:
        kernel.close()


def latest_completed_trade_date() -> date:
    kernel, repository, context = _pipeline_access()
    try:
        return repository.latest_completed_trade_date(context)
    finally:
        kernel.close()


def select_smoke_codes(limit: int = 10) -> list[str]:
    kernel, repository, context = _pipeline_access()
    try:
        return repository.select_smoke_codes(context, FIXED_SMOKE_CODES, limit)
    finally:
        kernel.close()


def _mark_run(run_id: str, status: str, error: str | None = None):
    kernel, repository, context = _pipeline_access(run_id)
    try:
        return repository.mark_run(run_id, status, context, error)
    finally:
        kernel.close()


def resume_run(run_id: str, mode: str, output_end: date) -> None:
    from data_collect.jobs.research_snapshot import _git_metadata

    kernel, repository, context = _pipeline_access(run_id)
    try:
        repository.resume_run(run_id, mode, output_end, _git_metadata(), context)
    finally:
        kernel.close()


def _standard_partition(
    run_id: str, codes: list[str], end_date: date, *, include_valuation: bool = False,
) -> dict:
    key = f"{codes[0]}..{codes[-1]}"
    kernel, repository, context = _pipeline_access(run_id)
    try:
        if repository.checkpoint_status(run_id, "standard_v2", key, context) == "COMPLETED":
            return {"skipped": True, "partition": key}
    finally:
        kernel.close()
    try:
        statements = run_statement_standard("all", 2000, codes, WARMUP_START, end_date)
        indicator = run_indicator_standard(False, True, 2000, codes, WARMUP_START, end_date)
        valuation = rebuild_valuation(codes, WARMUP_START, end_date) if include_valuation else (0, 0)
        written = (
            sum(item["inserted"] for item in statements.values())
            + int(indicator.get("standard_conversion", 0))
            + int(valuation[1])
        )
        kernel, repository, context = _pipeline_access(run_id)
        try:
            repository.save_checkpoint(run_id, "standard_v2", key, "COMPLETED", context, written, written)
        finally:
            kernel.close()
        return {"partition": key, "written": written}
    except Exception as exc:
        kernel, repository, context = _pipeline_access(run_id)
        try:
            repository.save_checkpoint(run_id, "standard_v2", key, "FAILED", context, error=str(exc)[:1000])
        finally:
            kernel.close()
        raise


def _valuation_standard_partitions(run_id: str, start_date: date, end_date: date) -> dict:
    current = start_date.replace(day=1)
    partitions = written = 0
    while current <= end_date:
        next_month = (
            current.replace(year=current.year + 1, month=1)
            if current.month == 12 else current.replace(month=current.month + 1)
        )
        window_start = max(start_date, current)
        window_end = min(end_date, next_month - timedelta(days=1))
        key = f"{window_start.isoformat()}..{window_end.isoformat()}"
        kernel, repository, context = _pipeline_access(run_id)
        try:
            done = repository.checkpoint_status(run_id, "valuation_standard_v2", key, context) == "COMPLETED"
        finally:
            kernel.close()
        if done:
            partitions += 1
            current = next_month
            continue
        try:
            attempted, affected = rebuild_valuation(None, window_start, window_end)
            kernel, repository, context = _pipeline_access(run_id)
            try:
                repository.save_checkpoint(run_id, "valuation_standard_v2", key, "COMPLETED", context, attempted, affected)
            finally:
                kernel.close()
            partitions += 1
            written += affected
        except Exception as exc:
            kernel, repository, context = _pipeline_access(run_id)
            try:
                repository.save_checkpoint(run_id, "valuation_standard_v2", key, "FAILED", context, error=str(exc)[:1000])
            finally:
                kernel.close()
            raise
        current = next_month
    return {"partitions": partitions, "written": written}


def run_smoke(output_end: date | None = None) -> dict:
    assert_migration_applied()
    output_end = output_end or latest_completed_trade_date()
    codes = select_smoke_codes()
    if not codes:
        raise RuntimeError("no smoke securities available")
    with _run_lock("SMOKE"):
        run_id = create_run("SMOKE", WARMUP_START, OUTPUT_START, output_end, {"codes": codes})
        try:
            standard = _standard_partition(run_id, codes, output_end, include_valuation=True)
            company_target = create_smoke_target(run_id)
            valuation_target = create_valuation_smoke_target(run_id)
            company = rebuild_company_financial_v2(run_id, WARMUP_START, OUTPUT_START, output_end, codes, company_target, 5)
            valuation = rebuild_valuation_features(run_id, WARMUP_START, OUTPUT_START, output_end, codes, valuation_target)
            quality = inspect_foundation_v2(max(OUTPUT_START, output_end - timedelta(days=730)), output_end, codes, company_target, valuation_target)
            if not quality["passed"]:
                raise RuntimeError(f"foundation smoke quality failed: {quality['failures']}")
            _mark_run(run_id, "VALIDATED")
            return {"run_id": run_id, "codes": codes, "output_end": output_end.isoformat(), "company_target": company_target, "valuation_target": valuation_target, "standard": standard, "company": company, "valuation": valuation, "quality": quality}
        except Exception as exc:
            _mark_run(run_id, "FAILED", str(exc)[:2000])
            raise


def run_production_derived(output_end: date | None = None, batch_stocks: int = DEFAULT_FEATURE_BATCH_STOCKS, run_id: str | None = None) -> dict:
    assert_migration_applied()
    output_end = output_end or latest_completed_trade_date()
    kernel, repository, context = _pipeline_access(run_id)
    try:
        codes = repository.active_stock_codes(context)
    finally:
        kernel.close()
    with _run_lock("PRODUCTION"):
        if run_id:
            resume_run(run_id, "PRODUCTION", output_end)
        else:
            run_id = create_run("PRODUCTION", WARMUP_START, OUTPUT_START, output_end, {"batch_stocks": batch_stocks})
        try:
            standard = [_standard_partition(run_id, codes[offset:offset + batch_stocks], output_end) for offset in range(0, len(codes), batch_stocks)]
            valuation_standard = _valuation_standard_partitions(run_id, WARMUP_START, output_end)
            warmup = rebuild_company_financial_v2(None, WARMUP_START, WARMUP_START, OUTPUT_START - timedelta(days=1), target_table="feature.company_financial_v2", batch_stocks=batch_stocks)
            company = rebuild_company_financial_v2(run_id, WARMUP_START, OUTPUT_START, output_end, target_table="feature.company_financial_v2", batch_stocks=batch_stocks)
            valuation = rebuild_valuation_features(run_id, WARMUP_START, OUTPUT_START, output_end, target_table="feature.valuation_daily_v1")
            quality = inspect_foundation_v2(OUTPUT_START, output_end, None, "feature.company_financial_v2", "feature.valuation_daily_v1")
            if not quality["passed"]:
                raise RuntimeError(f"production foundation quality failed: {quality['failures']}")
            _mark_run(run_id, "VALIDATED")
            return {"run_id": run_id, "standard_partitions": len(standard), "valuation_standard": valuation_standard, "warmup": warmup, "company": company, "valuation": valuation, "quality": quality}
        except Exception as exc:
            _mark_run(run_id, "FAILED", str(exc)[:2000])
            raise


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=("smoke", "production-derived"))
    parser.add_argument("--output-end")
    parser.add_argument("--batch-stocks", type=int, default=DEFAULT_FEATURE_BATCH_STOCKS)
    parser.add_argument("--run-id")
    args = parser.parse_args()
    end = date.fromisoformat(args.output_end) if args.output_end else None
    result = run_smoke(end) if args.mode == "smoke" else run_production_derived(end, args.batch_stocks, args.run_id)
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
