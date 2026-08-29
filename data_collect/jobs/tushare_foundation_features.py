"""Build Feature Contract v2 in bounded, resumable stock partitions."""
from __future__ import annotations

import argparse
import bisect
import hashlib
import json
import logging
import os
import re
import uuid
from collections import defaultdict, deque
from datetime import date, datetime, timedelta
from typing import Any, Iterable
from zoneinfo import ZoneInfo

from data_collect.features.financial_v2 import (
    DIVIDEND_PERCENTILE_MIN_OBSERVATIONS,
    VALUATION_WINDOW_COMPLETENESS_TOLERANCE_DAYS,
    VALUATION_WINDOW_YEARS,
    annual_window_stats,
    finite,
    ocf_to_profit,
    period_kind,
    prior_fiscal_year_end,
    prior_same_period,
    prior_ytd_period,
    quarter_bridge,
    roe_ttm,
    safe_cagr,
    safe_growth,
    safe_margin,
    ttm_bridge,
)
from data_collect.data_access import DataAccessKernel, RunContext
from data_collect.data_access.policy_catalog import build_default_registry
from data_collect.data_access.repository.financial_repository import FinancialRepository


logger = logging.getLogger(__name__)
CONTRACT_VERSION = "feature-contract-v2.0"
TARGET_TABLE = "feature.company_financial_v2"
VALUATION_TARGET_TABLE = "feature.valuation_daily_v1"
VALUATION_CODE_BATCH_SIZE = max(
    1, int(os.getenv("ASHARES_VALUATION_CODE_BATCH_SIZE", "1"))
)
DEFAULT_FEATURE_BATCH_STOCKS = 5
MAX_FEATURE_ROWS_PER_TRANSACTION = 20_000
MAX_VALUATION_ROWS_PER_TRANSACTION = 20_000
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*$")


def _financial_access(run_id: str | None = None):
    context = RunContext(
        run_id=run_id or uuid.uuid4().hex,
        job_name="tushare_foundation_features",
        code_version="unknown",
        config_hash="unknown",
        pipeline_version=CONTRACT_VERSION,
    )
    kernel = DataAccessKernel(policy_registry=build_default_registry())
    return kernel, FinancialRepository(kernel), context


def _target(value: str) -> tuple[str, str]:
    if not _IDENTIFIER.fullmatch(value):
        raise ValueError(f"非法目标表: {value}")
    return tuple(value.split(".", 1))  # type: ignore[return-value]


def create_run(
    mode: str,
    warmup_start: date,
    output_start: date,
    output_end: date,
    parameters: dict[str, Any] | None = None,
) -> str:
    from data_collect.jobs.research_snapshot import _git_metadata

    run_id = str(uuid.uuid4())
    metadata = _git_metadata()
    kernel, repository, context = _financial_access(run_id)
    try:
        repository.create_run(
            run_id, mode, warmup_start, output_start, output_end,
            metadata, parameters, context,
        )
    finally:
        kernel.close()
    return run_id


def _checkpoint_status(run_id: str, stage: str, partition_key: str) -> str | None:
    kernel, repository, context = _financial_access(run_id)
    try:
        return repository.checkpoint_status(run_id, stage, partition_key, context)
    finally:
        kernel.close()


def _checkpoint(
    _unused,
    run_id: str,
    stage: str,
    partition_key: str,
    status: str,
    attempted: int = 0,
    written: int = 0,
    result_hash: str | None = None,
    error: str | None = None,
):
    kernel, repository, context = _financial_access(run_id)
    try:
        return repository.save_checkpoint(
            run_id, stage, partition_key, status, context,
            attempted, written, result_hash, error,
        )
    finally:
        kernel.close()


def _load_events(codes: list[str], warmup_start: date, output_end: date) -> list[dict[str, Any]]:
    kernel, repository, context = _financial_access()
    try:
        return repository.load_events(codes, warmup_start, output_end, context)
    finally:
        kernel.close()


def _latest(
    state: dict[str, dict[tuple, dict[str, Any]]],
    dataset: str,
    report_date: date,
    comp_type: str | None = None,
    required_kind: str | None = None,
    *,
    allow_missing_end_type: bool = False,
    require_comp_type_match: bool = False,
) -> dict[str, Any] | None:
    candidates = [
        row for row in state[dataset].values()
        if row["report_date"] == report_date
        and (
            not require_comp_type_match or row.get("comp_type") == comp_type
        )
        and (
            required_kind is None
            or (
                period_kind(row["report_date"], row.get("end_type")) == required_kind
                and (allow_missing_end_type or row.get("end_type") is not None)
            )
        )
    ]
    if not candidates:
        return None
    return max(
        candidates,
        key=lambda row: (
            row["data_available_timestamp"], row.get("record_version") or 0,
            row.get("payload_hash") or "",
        ),
    )


def _value(row: dict[str, Any] | None, field: str):
    return finite((row or {}).get("values", {}).get(field))


def _bridge(
    state,
    dataset: str,
    field: str,
    report_date: date,
    kind: str,
    comp_type: str | None,
):
    previous_ytd_date = prior_ytd_period(report_date, kind)
    previous_kind = {"H1": "Q1", "Q3": "H1", "FY": "Q3"}.get(kind)
    current = _latest(
        state, dataset, report_date, comp_type, kind,
        require_comp_type_match=True,
    )
    previous_ytd = (
        _latest(
            state, dataset, previous_ytd_date, comp_type, previous_kind,
            require_comp_type_match=True,
        )
        if previous_ytd_date and previous_kind else None
    )
    prior_fy = _latest(
        state, dataset, prior_fiscal_year_end(report_date), comp_type, "FY",
        require_comp_type_match=True,
    )
    prior_same = _latest(
        state, dataset, prior_same_period(report_date), comp_type, kind,
        require_comp_type_match=True,
    )
    return (
        quarter_bridge(_value(current, field), _value(previous_ytd, field), kind),
        ttm_bridge(_value(current, field), _value(prior_fy, field), _value(prior_same, field), kind),
        [row for row in (current, previous_ytd, prior_fy, prior_same) if row],
    )


def _annual_rows(state, dataset: str, comp_type: str | None, report_date: date):
    result = {}
    for row in state[dataset].values():
        period = row["report_date"]
        if period > report_date or period.month != 12 or period.day != 31:
            continue
        if row.get("end_type") is None or period_kind(period, row.get("end_type")) != "FY":
            continue
        if row.get("comp_type") != comp_type:
            continue
        previous = result.get(period)
        if previous is None or (
            row["data_available_timestamp"], row.get("record_version") or 0
        ) > (
            previous["data_available_timestamp"], previous.get("record_version") or 0
        ):
            result[period] = row
    return result


def _dependent_report_dates(state, changed: dict[str, Any]) -> set[date]:
    """Return already-visible income periods affected by one source event.

    Restatements can change later quarter bridges, next-year TTM, ROE and
    rolling annual facts.  Emitting a new version for those periods is what
    makes the Feature table genuinely point-in-time instead of merely
    announcement-date filtered.  The explicit contract is same-fiscal-year
    downstream YTD bridges plus the following year's same-period TTM bridge:
    the bridge uses raw YTD source rows, so a Q1 source change is not a direct
    dependency of the following year's H1/Q3/FY source bridges.
    """
    report_date = changed["report_date"]
    dataset = changed["dataset"]
    visible_income_dates = {
        row["report_date"] for row in state["income"].values()
    }
    affected = {report_date}
    if dataset == "indicator":
        return affected & visible_income_dates

    kind = period_kind(report_date, changed.get("end_type"))
    if dataset in {"income", "cashflow"}:
        # Every later YTD period in the same fiscal year uses this period as
        # part of its quarter bridge.  Keeping only one next quarter here
        # leaves Q3/FY stale after a late Q1 restatement and leaves FY stale
        # after a late H1 restatement.
        affected.update(
            period
            for period in visible_income_dates
            if period.year == report_date.year and period >= report_date
        )
        # The changed source period is also the prior-same-period component of
        # the following year's TTM bridge.
        try:
            affected.add(report_date.replace(year=report_date.year + 1))
        except ValueError:
            # Keep the helper fail-closed if malformed source data appears.
            pass
    elif dataset == "balance":
        affected.add(report_date.replace(year=report_date.year + 1))

    if kind == "FY":
        # Annual income/cash/balance facts feed five-year windows. Balance at
        # year N can also be the opening equity for ROE in year N+1, hence the
        # conservative six-year dependency horizon.
        horizon = date(report_date.year + 6, 12, 31)
        affected.update(
            period for period in visible_income_dates
            if report_date <= period <= horizon
        )
    return affected & visible_income_dates


def _lineage(rows: Iterable[dict[str, Any]]) -> tuple[dict[str, Any], str, datetime | None]:
    unique = {}
    observed = []
    for row in rows:
        key = f"{row['dataset']}:{row.get('source_record_key')}:{row.get('record_version')}"
        unique[key] = {
            "dataset": row["dataset"],
            "source_record_key": row.get("source_record_key"),
            "record_version": row.get("record_version"),
            "payload_hash": row.get("payload_hash"),
            "vintage_quality": row.get("vintage_quality"),
        }
        if row.get("source_observed_at") is not None:
            observed.append(row["source_observed_at"])
    payload = json.dumps(unique, sort_keys=True, ensure_ascii=False, default=str)
    return unique, hashlib.sha256(payload.encode("utf-8")).hexdigest(), max(observed) if observed else None


def _pit_evidence_level(rows: Iterable[dict[str, Any]]) -> str:
    qualities = {
        row.get("vintage_quality") for row in rows if row.get("vintage_quality")
    }
    if not qualities:
        return "UNKNOWN"
    if qualities == {"WAREHOUSE_OBSERVED_ONLY"}:
        return "WAREHOUSE_OBSERVED_ONLY"
    if qualities == {"SOURCE_ANNOUNCEMENT_ASSUMED"}:
        return "SOURCE_ANNOUNCEMENT_ASSUMED"
    return "MIXED_SOURCE_AND_WAREHOUSE_OBSERVED"


def _build_feature(state, ts_code: str, report_date: date, event_time) -> dict[str, Any] | None:
    expected_kind = period_kind(report_date, None)
    if expected_kind is None:
        return None
    income = _latest(state, "income", report_date, required_kind=expected_kind)
    if income is None:
        return None
    comp_type = income.get("comp_type")
    kind = period_kind(report_date, income.get("end_type"))
    if kind is None:
        return None
    cashflow = _latest(
        state, "cashflow", report_date, comp_type, kind,
        require_comp_type_match=True,
    )
    balance = _latest(
        state, "balance", report_date, comp_type, kind,
        require_comp_type_match=True,
    )
    indicator = _latest(
        state, "indicator", report_date, comp_type, required_kind=kind,
        allow_missing_end_type=True,
        require_comp_type_match=True,
    )

    fields = {
        "revenue": ("income", "revenue"),
        "oper_cost": ("income", "oper_cost"),
        "total_cogs": ("income", "total_cogs"),
        "consolidated": ("income", "net_profit_consolidated"),
        "parent": ("income", "net_profit_parent"),
        "ocf": ("cashflow", "operating_cashflow_ytd"),
    }
    bridges = {}
    lineage_rows: list[dict[str, Any]] = [row for row in (income, cashflow, balance, indicator) if row]
    for name, (dataset, field) in fields.items():
        q_result, ttm_result, rows = _bridge(
            state, dataset, field, report_date, kind, comp_type
        )
        bridges[name] = (q_result, ttm_result)
        lineage_rows.extend(rows)

    prior_balance = _latest(
        state, "balance", prior_same_period(report_date), comp_type, kind,
        require_comp_type_match=True,
    )
    if prior_balance:
        lineage_rows.append(prior_balance)
    parent_equity_begin = _value(prior_balance, "parent_equity_end")
    parent_equity_end = _value(balance, "parent_equity_end")
    roe_value, roe_reason = roe_ttm(
        bridges["parent"][1].value, parent_equity_begin, parent_equity_end
    )
    ocf_ratio, ocf_reason = ocf_to_profit(
        bridges["ocf"][1].value, bridges["consolidated"][1].value
    )
    if comp_type is None:
        ocf_ratio, ocf_reason = None, "UNKNOWN_COMP_TYPE"
    elif str(comp_type) != "1":
        # Operating cash flow has a materially different economic meaning for
        # banks, insurers and brokers. Preserve the source facts/TTM values,
        # but do not expose an apparently comparable quality ratio.
        ocf_ratio, ocf_reason = None, "NOT_APPLICABLE"

    required = bridges["consolidated"][1].required
    actual = min(
        bridges[name][1].actual for name in ("consolidated", "parent", "ocf")
    )
    complete = all(
        bridges[name][1].complete for name in ("consolidated", "parent", "ocf")
    )

    prior_income = _latest(
        state, "income", prior_same_period(report_date), comp_type, kind,
        require_comp_type_match=True,
    )
    if prior_income:
        lineage_rows.append(prior_income)
    revenue_growth = safe_growth(_value(income, "revenue"), _value(prior_income, "revenue"))
    profit_growth = safe_growth(
        _value(income, "net_profit_parent"), _value(prior_income, "net_profit_parent")
    )
    annual_income = _annual_rows(state, "income", comp_type, report_date)
    annual_cash = _annual_rows(state, "cashflow", comp_type, report_date)
    annual_balance = _annual_rows(state, "balance", comp_type, report_date)
    lineage_floor = date(report_date.year - 6, 1, 1)
    lineage_rows.extend(
        row
        for annual in (annual_income, annual_cash, annual_balance)
        for period, row in annual.items()
        if period >= lineage_floor
    )
    annual_profit_values = [
        (period, _value(row, "net_profit_parent"))
        for period, row in annual_income.items()
    ]
    annual_cash_values = [
        (period, _value(row, "operating_cashflow_ytd"))
        for period, row in annual_cash.items()
    ]
    annual_roes = []
    for period, row in annual_income.items():
        end_balance = annual_balance.get(period)
        begin_balance = annual_balance.get(date(period.year - 1, 12, 31))
        value, _ = roe_ttm(
            _value(row, "net_profit_parent"),
            _value(begin_balance, "parent_equity_end"),
            _value(end_balance, "parent_equity_end"),
        )
        annual_roes.append((period, value))
    stats = annual_window_stats(annual_profit_values, annual_cash_values, annual_roes)
    years = sorted(annual_income)
    revenue_cagr = profit_cagr = None
    if years:
        latest_year = years[-1]
        five_year_prior = date(latest_year.year - 5, 12, 31)
        if five_year_prior in annual_income:
            revenue_cagr = safe_cagr(
                _value(annual_income[latest_year], "revenue"),
                _value(annual_income[five_year_prior], "revenue"), 5,
            )
            profit_cagr = safe_cagr(
                _value(annual_income[latest_year], "net_profit_parent"),
                _value(annual_income[five_year_prior], "net_profit_parent"), 5,
            )

    oper_cost_ttm = bridges["oper_cost"][1].value
    gross_margin = None
    if bridges["revenue"][1].value is not None and oper_cost_ttm is not None:
        gross_margin = safe_margin(
            bridges["revenue"][1].value - oper_cost_ttm,
            bridges["revenue"][1].value,
        )
    net_margin = safe_margin(
        bridges["consolidated"][1].value, bridges["revenue"][1].value
    )
    cashflow_status = "VALID" if complete and ocf_reason == "VALID" else ocf_reason
    growth_status = (
        "VALID" if revenue_growth is not None and profit_growth is not None
        else "INSUFFICIENT_HISTORY"
    )
    profit_recovery = None
    if stats["profit_window_complete"]:
        profit_recovery = bool(
            stats["profit_loss_event_5y"]
            and bridges["parent"][1].value is not None
            and bridges["parent"][1].value > 0
        )
    lineage, fingerprint, observed_at = _lineage(lineage_rows)
    total_assets, total_liab = _value(balance, "total_assets"), _value(balance, "total_liab")
    debt_to_assets = safe_margin(total_liab, total_assets)

    return {
        "ts_code": ts_code,
        "report_date": report_date,
        "effective_announce_date": event_time.astimezone(
            ZoneInfo("Asia/Shanghai")
        ).date(),
        "data_available_timestamp": event_time,
        "source_observed_at": observed_at,
        "report_type": income.get("report_type"),
        "comp_type": comp_type,
        "end_type": income.get("end_type"),
        "revenue_ytd": _value(income, "revenue"),
        "revenue_q": bridges["revenue"][0].value,
        "revenue_ttm": bridges["revenue"][1].value,
        "oper_cost_ytd": _value(income, "oper_cost"),
        "oper_cost_q": bridges["oper_cost"][0].value,
        "oper_cost_ttm": bridges["oper_cost"][1].value,
        "total_cogs_ytd": _value(income, "total_cogs"),
        "total_cogs_q": bridges["total_cogs"][0].value,
        "total_cogs_ttm": bridges["total_cogs"][1].value,
        "net_profit_consolidated_ytd": _value(income, "net_profit_consolidated"),
        "net_profit_consolidated_q": bridges["consolidated"][0].value,
        "net_profit_consolidated_ttm": bridges["consolidated"][1].value,
        "net_profit_parent_ytd": _value(income, "net_profit_parent"),
        "net_profit_parent_q": bridges["parent"][0].value,
        "net_profit_parent_ttm": bridges["parent"][1].value,
        "operating_cashflow_ytd": _value(cashflow, "operating_cashflow_ytd"),
        "operating_cashflow_q": bridges["ocf"][0].value,
        "operating_cashflow_ttm": bridges["ocf"][1].value,
        "parent_equity_begin": parent_equity_begin,
        "parent_equity_end": parent_equity_end,
        "total_equity_end": _value(balance, "total_equity_end"),
        "roe_reported": _value(indicator, "roe_reported"),
        "roe_annualized": _value(indicator, "roe_annualized"),
        "roe_quarterly": _value(indicator, "roe_quarterly"),
        "roe_ttm": roe_value,
        "debt_to_assets": debt_to_assets,
        "gross_margin_ttm": gross_margin,
        "net_margin_consolidated_ttm": net_margin,
        "ocf_to_net_profit_consolidated_ttm": ocf_ratio,
        "revenue_growth_yoy": revenue_growth,
        "profit_growth_yoy": profit_growth,
        "revenue_cagr_5y": revenue_cagr,
        "profit_cagr_5y": profit_cagr,
        "ttm_basis": bridges["consolidated"][1].basis,
        "ttm_required_component_count": required,
        "ttm_component_count": actual,
        "ttm_complete": complete,
        "roe_quality_reason": roe_reason,
        "ocf_quality_reason": ocf_reason,
        "cashflow_quality_status": cashflow_status,
        "roe_quality_status": "VALID" if roe_reason == "VALID" else roe_reason,
        "growth_quality_status": growth_status,
        **stats,
        "profit_recovery_flag": profit_recovery,
        "recovery_basis_period": f"{report_date.isoformat()}_TTM",
        "profit_metric_basis": "NET_PROFIT_PARENT",
        "component_lineage": lineage,
        "source_fingerprint": fingerprint,
        "pit_evidence_level": _pit_evidence_level(lineage_rows),
        "quality_status": "VALID" if complete else "INCOMPLETE_TTM",
        "feature_contract_version": CONTRACT_VERSION,
    }


def calculate_features(events: list[dict[str, Any]], output_start: date, output_end: date):
    output = []
    by_code: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in events:
        by_code[row["ts_code"]].append(row)
    for ts_code, rows in by_code.items():
        state: dict[str, dict[tuple, dict[str, Any]]] = defaultdict(dict)
        by_time: dict[Any, list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            by_time[row["data_available_timestamp"]].append(row)
        for event_time in sorted(by_time):
            affected_dates = set()
            # A single source announcement date can contain several reports
            # and several statements (for example prior FY plus current Q1).
            # Load the complete timestamp batch before calculating anything;
            # otherwise iteration order can emit an incomplete TTM version
            # and never revisit it after the companion fact is visible at the
            # same PIT timestamp.
            for row in by_time[event_time]:
                row["dataset"] = row["dataset"]
                key = (
                    row["report_date"], row.get("report_type"), row.get("comp_type"),
                    row.get("end_type"), row.get("source_record_key"),
                )
                state[row["dataset"]][key] = row
            for row in by_time[event_time]:
                affected_dates.update(_dependent_report_dates(state, row))
            for report_date in sorted(affected_dates):
                if report_date < output_start or report_date > output_end:
                    continue
                result = _build_feature(state, ts_code, report_date, event_time)
                if result:
                    output.append(result)
    return output


def _stock_codes(start: date, end: date, requested: list[str] | None = None) -> list[str]:
    kernel, repository, context = _financial_access()
    try:
        return repository.stock_codes(start, end, context, requested)
    finally:
        kernel.close()


def _write_partition(target_table: str, codes: list[str], rows: list[dict[str, Any]], output_start, output_end, run_id: str | None = None):
    if len(rows) > MAX_FEATURE_ROWS_PER_TRANSACTION:
        raise RuntimeError(
            f"Feature 分区行数 {len(rows)} 超过单事务上限 "
            f"{MAX_FEATURE_ROWS_PER_TRANSACTION}，请进一步减小 batch_stocks"
        )
    kernel, repository, context = _financial_access(run_id)
    try:
        return repository.publish_feature_partition(
            target, rows, codes, output_start, output_end, context,
        )
    finally:
        kernel.close()


def rebuild_company_financial_v2(
    run_id: str | None,
    warmup_start: date,
    output_start: date,
    output_end: date,
    requested_codes: list[str] | None = None,
    target_table: str = TARGET_TABLE,
    batch_stocks: int = DEFAULT_FEATURE_BATCH_STOCKS,
) -> dict[str, int]:
    if batch_stocks <= 0:
        raise ValueError("batch_stocks 必须为正整数")
    _target(target_table)
    kernel, repository, context = _financial_access(run_id)
    codes = repository.stock_codes(warmup_start, output_end, context, requested_codes)
    completed_keys = (
        repository.completed_checkpoint_keys(run_id, "company_financial_v2", context)
        if run_id else set()
    )
    attempted = written = completed = 0
    for offset in range(0, len(codes), batch_stocks):
        batch = codes[offset : offset + batch_stocks]
        partition_key = f"{batch[0]}..{batch[-1]}"
        if run_id and partition_key in completed_keys:
            completed += 1
            continue
        if run_id:
            repository.save_checkpoint(
                run_id, "company_financial_v2", partition_key, "RUNNING", context
            )
        try:
            events = _load_events(batch, warmup_start, output_end)
            rows = calculate_features(events, output_start, output_end)
            result_hash = hashlib.sha256(
                json.dumps(
                    rows, ensure_ascii=False, sort_keys=True,
                    separators=(",", ":"), default=str, allow_nan=False,
                ).encode("utf-8")
            ).hexdigest()
            count = repository.publish_feature_partition(
                target_table, rows, batch, output_start, output_end, context
            )
            if run_id:
                repository.save_checkpoint(
                    run_id, "company_financial_v2", partition_key, "COMPLETED", context,
                    len(rows), count, result_hash,
                )
            attempted += len(rows)
            written += count
            completed += 1
            logger.info(
                "company_financial_v2 partition=%s rows=%s", partition_key, count
            )
        except Exception as exc:
            if run_id:
                repository.save_checkpoint(
                    run_id, "company_financial_v2", partition_key, "FAILED", context,
                    error=str(exc)[:1000],
                )
            raise
    kernel.close()
    return {"stocks": len(codes), "partitions": completed, "attempted": attempted, "written": written}


def create_smoke_target(run_id: str) -> str:
    suffix = run_id.replace("-", "")[:12]
    table = f"company_financial_v2_smoke_{suffix}"
    kernel, repository, context = _financial_access(run_id)
    try:
        return repository.create_feature_target(
            "feature.company_financial_v2", f"feature.{table}", context
        )
    finally:
        kernel.close()


def create_valuation_smoke_target(run_id: str) -> str:
    suffix = run_id.replace("-", "")[:12]
    table = f"valuation_daily_v1_smoke_{suffix}"
    kernel, repository, context = _financial_access(run_id)
    try:
        return repository.create_feature_target(
            "feature.valuation_daily_v1", f"feature.{table}", context
        )
    finally:
        kernel.close()


def _percentile(sorted_values: list[float], value: float | None, *, allow_zero=False):
    value = finite(value)
    if value is None or not sorted_values or value < 0 or (value == 0 and not allow_zero):
        return None
    return bisect.bisect_right(sorted_values, value) / len(sorted_values)


def _valuation_input(code: str, warmup_start: date, output_end: date):
    kernel, repository, context = _financial_access()
    try:
        return repository.load_valuation_input(code, warmup_start, output_end, context)
    finally:
        kernel.close()


def calculate_valuation_features(rows, output_start: date, output_end: date):
    q3, q5 = deque(), deque()
    pe3: list[float] = []
    pe5: list[float] = []
    pb3: list[float] = []
    pb5: list[float] = []
    dv5: list[float] = []
    pe_dates3, pe_dates5, pb_dates3, pb_dates5, dv_dates5 = (
        deque(), deque(), deque(), deque(), deque()
    )
    output = []

    def remove(item, pe_values, pb_values, dv_values, pe_dates, pb_dates, dv_dates):
        item_date, pe, pb, dv = item
        for value, values, dates in (
            (pe, pe_values, pe_dates), (pb, pb_values, pb_dates), (dv, dv_values, dv_dates),
        ):
            if value is None:
                continue
            index = bisect.bisect_left(values, value)
            if index < len(values) and values[index] == value:
                values.pop(index)
            if dates and dates[0] == item_date:
                dates.popleft()

    for row in rows:
        trade_date = row["trade_date"]
        cutoff3 = trade_date - timedelta(days=365 * 3)
        cutoff5 = trade_date - timedelta(days=365 * VALUATION_WINDOW_YEARS)
        while q3 and q3[0][0] < cutoff3:
            remove(q3.popleft(), pe3, pb3, [], pe_dates3, pb_dates3, deque())
        while q5 and q5[0][0] < cutoff5:
            remove(q5.popleft(), pe5, pb5, dv5, pe_dates5, pb_dates5, dv_dates5)
        pe, pb = finite(row.get("pe_ttm")), finite(row.get("pb"))
        dv = finite(row.get("dv_ttm_vendor"))
        pe = pe if pe is not None and pe > 0 else None
        pb = pb if pb is not None and pb > 0 else None
        dv = dv if dv is not None and dv >= 0 else None
        item = (trade_date, pe, pb, dv)
        q3.append(item); q5.append(item)
        for value, values3, values5, dates3, dates5 in (
            (pe, pe3, pe5, pe_dates3, pe_dates5),
            (pb, pb3, pb5, pb_dates3, pb_dates5),
        ):
            if value is not None:
                bisect.insort(values3, value); bisect.insort(values5, value)
                dates3.append(trade_date); dates5.append(trade_date)
        if dv is not None:
            bisect.insort(dv5, dv)
            dv_dates5.append(trade_date)
        if trade_date < output_start or trade_date > output_end:
            continue
        full_pe3 = bool(len(pe3) >= 600 and pe_dates3 and (trade_date - pe_dates3[0]).days >= 365 * 3 - 31)
        full_pe5 = bool(
            len(pe5) >= 1000 and pe_dates5
            and (trade_date - pe_dates5[0]).days
                >= 365 * VALUATION_WINDOW_YEARS
                   - VALUATION_WINDOW_COMPLETENESS_TOLERANCE_DAYS
        )
        full_pb3 = bool(len(pb3) >= 600 and pb_dates3 and (trade_date - pb_dates3[0]).days >= 365 * 3 - 31)
        full_pb5 = bool(
            len(pb5) >= 1000 and pb_dates5
            and (trade_date - pb_dates5[0]).days
                >= 365 * VALUATION_WINDOW_YEARS
                   - VALUATION_WINDOW_COMPLETENESS_TOLERANCE_DAYS
        )
        dv_complete = bool(
            dv_dates5
            and (trade_date - dv_dates5[0]).days
                >= 365 * VALUATION_WINDOW_YEARS
                   - VALUATION_WINDOW_COMPLETENESS_TOLERANCE_DAYS
        )
        output.append({
            "ts_code": row["stock_code"], "trade_date": trade_date,
            "pe_ttm": finite(row.get("pe_ttm")), "pb": finite(row.get("pb")),
            "ps_ttm": finite(row.get("ps_ttm")), "dv_ratio": finite(row.get("dv_ratio")),
            "total_mv": finite(row.get("total_mv")), "circ_mv": finite(row.get("circ_mv")),
            "pe_pct_3y": _percentile(pe3, pe) if full_pe3 else None,
            "pe_pct_5y": _percentile(pe5, pe) if full_pe5 else None,
            "pb_pct_3y": _percentile(pb3, pb) if full_pb3 else None,
            "pb_pct_5y": _percentile(pb5, pb) if full_pb5 else None,
            "is_pe_meaningful": pe is not None, "valuation_observation_count": len(pe5),
            "pe_window_start_date_3y": pe_dates3[0] if pe_dates3 else None,
            "pe_window_start_date_5y": pe_dates5[0] if pe_dates5 else None,
            "pe_observation_count_3y": len(pe3), "pe_observation_count_5y": len(pe5),
            "has_full_pe_3y_history": full_pe3, "has_full_pe_5y_history": full_pe5,
            "quality_status": "VALID" if full_pe5 else "WARNING",
            "source_asof_date": trade_date,
            "pe_ttm_vendor": finite(row.get("pe_ttm_vendor")),
            "ps_ttm_vendor": finite(row.get("ps_ttm_vendor")),
            "pb_vendor": finite(row.get("pb_vendor")),
            "dv_ttm": dv, "dv_ttm_vendor": dv,
            "dv_ttm_source": row.get("dv_ttm_source"),
            "dv_ttm_percentile_5y": (
                _percentile(dv5, dv, allow_zero=True)
                if len(dv5) >= DIVIDEND_PERCENTILE_MIN_OBSERVATIONS else None
            ),
            "dv_window_start": dv_dates5[0] if dv_dates5 else None,
            "dv_window_end": trade_date, "dv_observation_days": len(dv5),
            "dv_window_complete": dv_complete,
            "source_observed_at": row.get("source_observed_at"),
            "data_available_timestamp": row.get("data_available_timestamp"),
            "feature_contract_version": CONTRACT_VERSION,
        })
    return output


def _write_valuation_partition(target_table: str, code: str, rows, output_start, output_end, run_id: str | None = None):
    kernel, repository, context = _financial_access(run_id)
    try:
        return repository.publish_valuation_partition(
            target_table, code, rows, output_start, output_end, context
        )
    finally:
        kernel.close()


def rebuild_valuation_features(
    run_id: str | None,
    warmup_start: date,
    output_start: date,
    output_end: date,
    requested_codes: list[str] | None = None,
    target_table: str = VALUATION_TARGET_TABLE,
):
    _target(target_table)
    kernel, repository, context = _financial_access(run_id)
    codes = repository.valuation_codes(
        warmup_start, output_end, context, requested_codes,
        # Select every security with at least one source observation in the
        # output interval.  Selecting only rows on output_end would silently
        # drop securities that stopped trading during the interval, leaving
        # valid Standard rows without a corresponding Feature row.
        selection_start=output_start,
    )
    completed_keys = (
        repository.completed_checkpoint_keys(run_id, "valuation_daily_v2", context)
        if run_id else set()
    )
    total = completed = 0
    total_codes = len(codes)
    logger.info(
        "valuation rebuild started target=%s codes=%s warmup=%s output=%s..%s batch=%s",
        target_table, total_codes, warmup_start, output_start, output_end,
        VALUATION_CODE_BATCH_SIZE,
    )
    for offset in range(0, len(codes), VALUATION_CODE_BATCH_SIZE):
        batch = codes[offset : offset + VALUATION_CODE_BATCH_SIZE]
        input_rows = defaultdict(list)
        for row in repository.load_valuation_inputs(batch, warmup_start, output_end, context):
            input_rows[row["stock_code"]].append(row)

        prepared = []
        for code in batch:
            if (
                run_id is not None and code in completed_keys
            ):
                completed += 1
                continue
            rows = calculate_valuation_features(
                input_rows.get(code, []), output_start, output_end
            )
            digest = hashlib.sha256(
                json.dumps(rows, sort_keys=True, default=str).encode("utf-8")
            ).hexdigest()
            prepared.append((code, rows, digest))

        if not prepared:
            logger.info(
                "valuation rebuild progress target=%s completed=%s/%s skipped_batch=%s",
                target_table, completed, total_codes, len(batch),
            )
            continue
        try:
            publish_batch = []
            publish_rows = 0
            for item in prepared:
                item_rows = len(item[1])
                if item_rows > MAX_VALUATION_ROWS_PER_TRANSACTION:
                    raise RuntimeError(
                        f"valuation partition {item[0]} rows={item_rows} exceeds "
                        f"transaction budget={MAX_VALUATION_ROWS_PER_TRANSACTION}"
                    )
                if publish_batch and publish_rows + item_rows > MAX_VALUATION_ROWS_PER_TRANSACTION:
                    written = repository.publish_valuation_partitions(
                        target_table,
                        ((code, rows) for code, rows, _ in publish_batch),
                        output_start,
                        output_end,
                        context,
                        max_rows=MAX_VALUATION_ROWS_PER_TRANSACTION,
                    )
                    for code, rows, digest in publish_batch:
                        if run_id is not None:
                            repository.save_checkpoint(
                                run_id, "valuation_daily_v2", code, "COMPLETED", context,
                                len(rows), len(rows), digest,
                            )
                        completed += 1
                    total += written
                    publish_batch = []
                    publish_rows = 0
                publish_batch.append(item)
                publish_rows += item_rows
            if publish_batch:
                written = repository.publish_valuation_partitions(
                    target_table,
                    ((code, rows) for code, rows, _ in publish_batch),
                    output_start,
                    output_end,
                    context,
                    max_rows=MAX_VALUATION_ROWS_PER_TRANSACTION,
                )
                for code, rows, digest in publish_batch:
                    if run_id is not None:
                        repository.save_checkpoint(
                            run_id, "valuation_daily_v2", code, "COMPLETED", context,
                            len(rows), len(rows), digest,
                        )
                    completed += 1
                total += written
            logger.info(
                "valuation rebuild progress target=%s completed=%s/%s written=%s",
                target_table, completed, total_codes, total,
            )
        except Exception as exc:
            if run_id is not None:
                for code, _, _ in prepared:
                    repository.save_checkpoint(
                        run_id, "valuation_daily_v2", code, "FAILED", context,
                        error=str(exc)[:1000],
                    )
            raise
    kernel.close()
    logger.info(
        "valuation rebuild finished target=%s codes=%s completed=%s written=%s",
        target_table, total_codes, completed, total,
    )
    return {"stocks": len(codes), "partitions": completed, "written": total}


def main():
    parser = argparse.ArgumentParser(description="分片构建通用财务 Feature v2")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--warmup-start", default="2016-08-01")
    parser.add_argument("--output-start", default="2021-08-02")
    parser.add_argument("--output-end", required=True)
    parser.add_argument("--codes", nargs="*")
    parser.add_argument("--target-table", default=TARGET_TABLE)
    parser.add_argument("--batch-stocks", type=int, default=DEFAULT_FEATURE_BATCH_STOCKS)
    args = parser.parse_args()
    result = rebuild_company_financial_v2(
        args.run_id, date.fromisoformat(args.warmup_start),
        date.fromisoformat(args.output_start), date.fromisoformat(args.output_end),
        args.codes, args.target_table, args.batch_stocks,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
