"""Daily candidate generation and read-only analysis packets.

This module is the narrow bridge between a FROZEN Research Snapshot and the
manual AI review flow.  It never reads Raw/Standard/PIT tables directly and
it never approves, fills, or sends an order.
"""
from __future__ import annotations

from datetime import date
import hashlib
import json
import math
import uuid
from typing import Any, Mapping

from data_collect.backtest.engine import FundamentalValueGrowthTrendStrategy, ValueTrendStrategy
from data_collect.backtest.models import Observation, SignalIntent
from data_collect.backtest.risk import (
    FeeRule, MarketRule, RiskPolicy, check_liquidity,
    open_position_stop_risk, size_position,
)
from data_collect.jobs.backtest_dataset import _git_metadata, _json_param
from data_collect.data_access import DataAccessKernel, RunContext
from data_collect.data_access.context import WorkScope
from data_collect.data_access.policy_catalog import build_default_registry
from data_collect.data_access.repository.candidate_repository import CandidateRepository
from data_collect.data_access.research_contract import FOUNDATION_SCHEMA_VERSIONS


DAILY_SIGNAL_RUN_TYPE = "DAILY_SIGNAL"
DAILY_SIGNAL_STRATEGY_ID = "fundamental_value_growth_trend_v2"
DAILY_SIGNAL_STRATEGY_VERSION = "2.0.0"
DEFAULT_INITIAL_CASH = 10_000.0
DEFAULT_RISK_PCT = 0.01
DEFAULT_MAX_PORTFOLIO_RISK_PCT = 0.03
DEFAULT_MAX_SINGLE_POSITION_PCT = 0.25
DEFAULT_RESERVE_CASH_PCT = 0.10
DEFAULT_ENTRY_SLIPPAGE_BPS = 10.0
DEFAULT_EXIT_SLIPPAGE_BPS = 10.0
DEFAULT_GAP_STRESS_PCT = 0.05
DEFAULT_VOLUME_PARTICIPATION_PCT = 0.05


def _service(label: str) -> tuple[DataAccessKernel, CandidateRepository, RunContext]:
    kernel = DataAccessKernel(policy_registry=build_default_registry())
    context = RunContext(
        run_id=f"candidate-{label}-{uuid.uuid4().hex}", job_name="daily_candidate",
        code_version="unknown", config_hash="unknown", pipeline_version="data-access-v1",
    )
    return kernel, CandidateRepository(kernel), context


def _hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    ).hexdigest()


def _date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


def _number(value: Any) -> float | None:
    if value is None:
        return None
    result = float(value)
    return result if math.isfinite(result) else None


def _json_safe(value: Any) -> Any:
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    return value


def _snapshot_context(repository: CandidateRepository, context: RunContext, snapshot_id: str) -> dict[str, Any]:
    row = repository.snapshot_context(snapshot_id, context)
    if not row:
        raise ValueError(f"不存在的 Research Snapshot: {snapshot_id}")
    if row["status"] != "FROZEN":
        raise RuntimeError(f"候选生成只允许 FROZEN Snapshot: {snapshot_id} ({row['status']})")
    result = dict(row)
    result["snapshot_id"] = str(result["snapshot_id"])
    result["asof_date"] = _date(result["research_cutoff_date"]).isoformat()
    return result


def _observations(repository: CandidateRepository, context: RunContext, snapshot_id: str, research_date: date) -> tuple[Observation, ...]:
    rows = repository.observations(snapshot_id, research_date, context)
    if not rows:
        raise RuntimeError(f"FROZEN Snapshot 没有股票数据: {snapshot_id}/{research_date}")
    fields = (
        "is_listed_asof", "is_st_asof", "eligible_by_market", "is_suspended_asof",
        "can_buy", "can_sell", "limit_status", "raw_close", "adj_close_base",
        "return_1d", "return_20d", "return_60d", "ma20", "ma60", "ma120",
        "volume_ratio_20d", "pe_ttm", "pe_pct_5y", "is_pe_meaningful",
        "effective_announce_date", "roe", "roa", "debt_to_assets", "revenue_yoy",
        "tr_yoy", "q_sales_yoy", "netprofit_yoy", "operating_cashflow", "net_profit",
        "quality_status", "market", "name", "volume", "amount", "raw_high", "raw_low",
        "list_date", "available_observation_count", "ma20_observation_count",
        "ma60_observation_count", "ma120_observation_count", "valuation_observation_count",
        "pe_window_start_date_5y", "has_full_pe_5y_history", "annual_report_count_5y",
        "roe_above_15_years_5y", "roe_5y_min", "roe_5y_all_above_15",
        "ocf_report_count_3y", "ocf_positive_years_3y", "ocf_3y_all_positive",
        "quarterly_report_count_4q", "q_sales_yoy_above_30_quarters_4q",
        "q_sales_yoy_4q_all_above_30", "financial_version_available_date",
        "up_limit", "down_limit", "raw_open",
    )
    result = []
    date_fields = {"effective_announce_date", "financial_version_available_date", "list_date", "pe_window_start_date_5y"}
    for row in rows:
        values = {field: (_date(row.get(field)).isoformat() if field in date_fields and row.get(field) else row.get(field)) for field in fields}
        result.append(Observation(research_date=research_date, ts_code=str(row["ts_code"]), values=values))
    return tuple(result)


def _portfolio_state(
    repository: CandidateRepository, context: RunContext, portfolio_id: str | None, snapshot_id: str,
    initial_cash: float, rule_snapshot: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], dict[str, dict[str, Any]], float, float, float | None]:
    # Repository enforces the historical cash cutoff predicate available_date<=%s.
    if not portfolio_id:
        state = {
            "portfolio_id": None, "account": {"initial_cash": float(initial_cash)},
            "cash_ledger": {"ledger_id": None, "balance": float(initial_cash)},
            "lots": [],
            "valuation": {"cash_balance": float(initial_cash), "equity_base": float(initial_cash),
                          "available_cash": float(initial_cash), "existing_stop_risk": 0.0, "positions": {}},
        }
        return state, {}, float(initial_cash), float(initial_cash), 0.0
    execution_date = date.fromisoformat(str(rule_snapshot["execution_date"])) if rule_snapshot else None
    inputs = repository.portfolio_inputs(str(portfolio_id), snapshot_id, context, execution_date)
    account = dict(inputs["account"])
    lot_rows = [dict(row) for row in inputs["lots"]]
    positions: dict[str, dict[str, Any]] = {}
    for row in lot_rows:
        item = positions.setdefault(str(row["ts_code"]), {"hold_shares": 0})
        item["hold_shares"] += int(row["remaining_shares"])
    cash_row = inputs["cash_row"]
    cash = float(cash_row["balance"]) if cash_row else float(account["initial_cash"])
    available_asof = date.fromisoformat(str(rule_snapshot["execution_date"])) if rule_snapshot else date.max
    available = inputs["available"]
    available_cash = float(available.get("available_cash") or 0.0)
    equity = cash
    existing_risk = 0.0
    risk_unknown = False
    markets = {str(row["ts_code"]): row.get("market") for row in inputs["prices"]}
    prices = {str(row["ts_code"]): _number(row.get("raw_close")) for row in inputs["prices"]}
    missing = sorted(code for code in positions if not prices.get(code) or prices[code] <= 0)
    if missing:
        raise RuntimeError(f"Portfolio 风险基数缺少 FROZEN Snapshot 持仓价格: {missing}")
    equity += sum(positions[code]["hold_shares"] * prices[code] for code in positions)
    for row in lot_rows:
        stop = row.get("stop_loss_price_raw")
        if stop is None or rule_snapshot is None:
            risk_unknown = True
            continue
        rule_row = _rule_for_market(rule_snapshot, markets.get(str(row["ts_code"])))
        market_rule, fee_rule, risk_policy = _domain_rules(rule_row)
        existing_risk += open_position_stop_risk(int(row["remaining_shares"]), float(row["cost_price"]), float(stop), fee_rule, market_rule, risk_policy)
    state = {
        "portfolio_id": str(portfolio_id), "account": _json_safe(account),
        "cash_ledger": _json_safe(cash_row) if cash_row else {"ledger_id": None, "balance": float(account["initial_cash"])},
        "lots": [_json_safe(row) for row in lot_rows],
        "valuation": {"cash_balance": cash, "available_cash": available_cash, "equity_base": equity,
                      "existing_stop_risk": None if risk_unknown else existing_risk, "positions": positions},
    }
    return state, positions, available_cash, equity, (None if risk_unknown else existing_risk)




def _rule_snapshot(repository: CandidateRepository, context: RunContext, decision_date: date) -> dict[str, Any]:
    result = repository.rule_snapshot(decision_date, context)
    result["execution_date"] = _date(result["execution_date"]).isoformat()
    result["rules"] = [_json_safe(row) for row in result["rules"]]
    return result




def _rule_for_market(rule_snapshot: Mapping[str, Any], market: str | None) -> Mapping[str, Any]:
    rows = rule_snapshot.get("rules") or []
    exact = [row for row in rows if row.get("market") == market]
    fallback = [row for row in rows if row.get("market") == "ALL"]
    matches = exact or fallback
    if len(matches) != 1:
        raise RuntimeError(f"市场 {market!r} 的已批准规则不是唯一一条")
    return matches[0]


def _domain_rules(row: Mapping[str, Any]) -> tuple[MarketRule, FeeRule, RiskPolicy]:
    parameters = row.get("parameters") or {}
    market_rule = MarketRule(
        market=str(row.get("market") or "ALL"),
        min_buy_lot=int(row["min_buy_lot"]),
        allow_sell_odd_lot=bool(row["allow_sell_odd_lot"]),
        t_plus_one=bool(row["t_plus_one"]),
        price_tick=float(row["price_tick"]),
        cash_settlement_days=int(row["cash_settlement_days"]),
        max_volume_participation_pct=float(parameters["max_volume_participation_pct"]),
        liquidity_policy=str(parameters["liquidity_policy"]),
    )
    fee_rule = FeeRule(
        commission_rate=float(row["commission_rate"]),
        min_commission=float(row["min_commission"]),
        stamp_duty_sell_rate=float(row["stamp_duty_sell_rate"]),
        transfer_fee_rate=float(row["transfer_fee_rate"]),
        slippage_bps=float(row["slippage_bps"]),
    )
    risk_policy = RiskPolicy(
        entry_slippage_bps=float(row["slippage_bps"]),
        exit_slippage_bps=float(row["slippage_bps"]),
        gap_stress_pct=float(parameters.get("gap_stress_pct", 0.0)),
    )
    return market_rule, fee_rule, risk_policy


def _candidate_hash(payload: Mapping[str, Any]) -> str:
    return _hash(_json_safe(payload))


def create_candidate_run(
    snapshot_id: str,
    *,
    portfolio_id: str | None = None,
    strategy: FundamentalValueGrowthTrendStrategy | ValueTrendStrategy | None = None,
    initial_cash: float = DEFAULT_INITIAL_CASH,
    risk_pct: float = DEFAULT_RISK_PCT,
    max_portfolio_risk_pct: float = DEFAULT_MAX_PORTFOLIO_RISK_PCT,
    max_single_position_pct: float = DEFAULT_MAX_SINGLE_POSITION_PCT,
    reserve_cash_pct: float = DEFAULT_RESERVE_CASH_PCT,
    entry_slippage_bps: float = DEFAULT_ENTRY_SLIPPAGE_BPS,
    exit_slippage_bps: float = DEFAULT_EXIT_SLIPPAGE_BPS,
    gap_stress_pct: float = DEFAULT_GAP_STRESS_PCT,
    max_volume_participation_pct: float = DEFAULT_VOLUME_PARTICIPATION_PCT,
) -> dict[str, Any]:
    """Generate deterministic BUY/SELL candidates from one FROZEN Snapshot."""
    if not 0 < risk_pct < 1 or not 0 < max_portfolio_risk_pct <= 1:
        raise ValueError("risk_pct/max_portfolio_risk_pct 必须在 (0,1] 内")
    if not 0 < max_single_position_pct <= 1 or not 0 <= reserve_cash_pct < 1:
        raise ValueError("仓位和预留现金参数无效")
    strategy = strategy or FundamentalValueGrowthTrendStrategy()
    parameters = {
        "strategy": strategy.parameters,
        # Positions and cash affect SELL candidates and sizing.  Bind the run
        # to the selected paper portfolio instead of accidentally reusing a
        # no-portfolio candidate run with a different account state.
        "portfolio_id": str(portfolio_id) if portfolio_id else None,
        "initial_cash": float(initial_cash),
        "risk_pct": float(risk_pct),
        "max_portfolio_risk_pct": float(max_portfolio_risk_pct),
        "max_single_position_pct": float(max_single_position_pct),
        "reserve_cash_pct": float(reserve_cash_pct),
        "entry_slippage_bps": float(entry_slippage_bps),
        "exit_slippage_bps": float(exit_slippage_bps),
        "gap_stress_pct": float(gap_stress_pct),
        "max_volume_participation_pct": float(max_volume_participation_pct),
    }
    metadata = _git_metadata()
    kernel, repository, context = _service("run")
    try:
        snapshot = _snapshot_context(repository, context, snapshot_id)
        approved_strategy = repository.approved_strategy(
            strategy.strategy_id, strategy.strategy_version, context
        )
        if not approved_strategy:
            raise RuntimeError(
                f"strategy {strategy.strategy_id}/{strategy.strategy_version} "
                "is not APPROVED in backtest.app_strategy_version"
            )
        if approved_strategy["parameters_hash"] != _hash(strategy.parameters):
            raise RuntimeError(
                "runtime strategy parameters differ from the approved app_strategy_version"
            )
        if isinstance(strategy, FundamentalValueGrowthTrendStrategy) and snapshot["schema_version"] not in FOUNDATION_SCHEMA_VERSIONS:
            raise RuntimeError("fundamental-v2 候选只允许 research-v2.0 FROZEN Snapshot")
        research_date = date.fromisoformat(snapshot["asof_date"])
        observations = _observations(repository, context, snapshot_id, research_date)
        rule_snapshot = _rule_snapshot(repository, context, research_date)
        portfolio_state, positions, current_cash, equity_base, existing_risk = _portfolio_state(
            repository, context, portfolio_id, snapshot_id, initial_cash, rule_snapshot,
        )
        if portfolio_id:
            account_reserve = (portfolio_state.get("account") or {}).get("reserve_cash_pct")
            if account_reserve is None or not math.isclose(
                float(account_reserve), float(reserve_cash_pct), rel_tol=1e-12, abs_tol=1e-12
            ):
                raise ValueError(
                    "候选运行的 reserve_cash_pct 必须与已绑定 Portfolio 账户一致"
                )
        observation_payload = [
            {"ts_code": item.ts_code, "research_date": item.research_date.isoformat(),
             "values": _json_safe(dict(item.values))}
            for item in observations
        ]
        observation_hash = _hash(observation_payload)
        portfolio_state_hash = _hash(portfolio_state)
        rule_snapshot_hash = _hash(rule_snapshot)
        # Risk/execution assumptions are approved rule facts, not free runtime
        # knobs.  Reject a caller that tries to override the frozen rule.
        for rule_row in rule_snapshot["rules"]:
            parameters_rule = rule_row.get("parameters") or {}
            expected = (
                float(rule_row["slippage_bps"]),
                float(parameters_rule.get("gap_stress_pct", 0.0)),
                float(parameters_rule["max_volume_participation_pct"]),
            )
            actual = (float(entry_slippage_bps), float(gap_stress_pct), float(max_volume_participation_pct))
            if not all(math.isclose(a, b, rel_tol=1e-12, abs_tol=1e-12) for a, b in zip(actual, expected)):
                raise ValueError(
                    "候选运行参数与已批准规则不一致: "
                    f"actual={actual}, approved={expected}, rule={rule_row['rule_version']}"
                )
            if not math.isclose(float(exit_slippage_bps), expected[0], rel_tol=1e-12, abs_tol=1e-12):
                raise ValueError("退出滑点必须与已批准规则一致")
        parameters_hash = _hash(parameters)
        signals = strategy.generate_signals(research_date, observations, positions)
        observations_by_code = {item.ts_code: item for item in observations}
        signals = [
            signal for signal in signals
            if (
                signal.direction == "BUY"
                and observations_by_code[signal.ts_code].get("can_buy") is True
            ) or (
                signal.direction == "SELL"
                and observations_by_code[signal.ts_code].get("can_sell") is True
            ) or signal.direction == "HOLD"
        ]
        signal_codes = {(signal.ts_code, signal.direction) for signal in signals}
        for code, state in positions.items():
            if state.get("hold_shares", 0) <= 0 or (code, "SELL") in signal_codes:
                continue
            observation = observations_by_code.get(code)
            if observation is None:
                continue
            signals.append(SignalIntent(
                ts_code=code,
                direction="HOLD",
                indicator_price=observation.get("adj_close_base"),
                raw_reference_price=observation.get("raw_close"),
                reason={
                    "rule": "position_hold_no_exit_signal",
                    "can_sell": observation.get("can_sell"),
                    "is_suspended_asof": observation.get("is_suspended_asof"),
                },
                score=0.0,
            ))
        signals = sorted(
            signals,
            key=lambda item: (0 if item.direction == "SELL" else 1 if item.direction == "BUY" else 2,
                              -float(item.score) if item.direction == "BUY" else 0.0,
                              item.ts_code),
        )
        input_hash = _hash({
            "snapshot": snapshot,
            "parameters_hash": parameters_hash,
            "observation_input_hash": observation_hash,
            "portfolio_state_hash": portfolio_state_hash,
            "rule_snapshot_hash": rule_snapshot_hash,
        })
        run_id = str(uuid.uuid4())
        # Idempotency lookup is a typed Repository query; the historical SQL contract is:
        # strategy_version=%s AND input_hash=%s
        existing = repository.find_run(snapshot_id, strategy.strategy_id, strategy.strategy_version, input_hash, context)
        if existing:
            if existing["status"] != "SUCCEEDED":
                raise RuntimeError(f"相同参数的候选运行已存在但状态为 {existing['status']}: {existing['candidate_run_id']}")
            return {"candidate_run_id": str(existing["candidate_run_id"]), "status": existing["status"], "candidate_count": repository.candidate_count(str(existing["candidate_run_id"]), context), "idempotent": True}
        # Keep a fixed portfolio-level cash floor; do not repeatedly apply the
        # percentage to the shrinking cash balance after each candidate.
        available_cash = max(0.0, current_cash - equity_base * reserve_cash_pct)
        existing_risk_amount = (
            existing_risk if existing_risk is not None
            else equity_base * max_portfolio_risk_pct
        )
        remaining_risk = max(0.0, equity_base * max_portfolio_risk_pct - existing_risk_amount)
        candidate_rows = []
        for signal in signals:
            eligible = False
            reject_reason = ""
            sizing = None
            observation = observations_by_code[signal.ts_code]
            rule_row = _rule_for_market(rule_snapshot, observation.get("market"))
            market_rule, fee_rule, risk_policy = _domain_rules(rule_row)
            if signal.direction in {"SELL", "HOLD"}:
                shares = int(positions.get(signal.ts_code, {}).get("hold_shares", 0))
                eligible = shares > 0
                reject_reason = "无可卖持仓" if not eligible else ""
                planned = shares
                required = 0.0
                max_loss = 0.0
                gap_loss = 0.0
                est_slippage = 0.0
                actual_risk = 0.0
                theoretical = float(shares)
                if eligible and signal.direction == "SELL":
                    liquidity = check_liquidity(
                        observations_by_code[signal.ts_code].get("volume"),
                        planned,
                        market_rule,
                        amount=observations_by_code[signal.ts_code].get("amount"),
                        close_price=signal.raw_reference_price,
                        low_price=observations_by_code[signal.ts_code].get("raw_low"),
                        high_price=observations_by_code[signal.ts_code].get("raw_high"),
                    )
                    if not liquidity.allowed:
                        eligible = False
                        reject_reason = liquidity.reject_reason or liquidity.liquidity_status
            else:
                sizing = size_position(
                    available_cash=available_cash,
                    risk_pct=risk_pct,
                    entry_price=float(signal.raw_reference_price or 0),
                    stop_loss_price=float(signal.stop_loss_price_raw or 0),
                    fee_rule=fee_rule,
                    market_rule=market_rule,
                    equity_base=equity_base,
                    max_position_value=equity_base * max_single_position_pct,
                    max_risk_amount=max(0.0, remaining_risk),
                    risk_policy=risk_policy,
                )
                eligible = bool(sizing.eligible)
                reject_reason = sizing.reject_reason
                planned = int(sizing.planned_shares)
                required = float(sizing.required_capital)
                max_loss = float(sizing.max_loss_at_stop)
                gap_loss = float(sizing.gap_stress_loss)
                est_slippage = float(sizing.estimated_slippage)
                actual_risk = float(sizing.actual_risk_pct)
                theoretical = float(sizing.theoretical_shares)
                if eligible:
                    liquidity = check_liquidity(
                        observations_by_code[signal.ts_code].get("volume"),
                        planned,
                        market_rule,
                        amount=observations_by_code[signal.ts_code].get("amount"),
                        close_price=signal.raw_reference_price,
                        low_price=observations_by_code[signal.ts_code].get("raw_low"),
                        high_price=observations_by_code[signal.ts_code].get("raw_high"),
                    )
                    if not liquidity.allowed:
                        eligible = False
                        reject_reason = liquidity.reject_reason or liquidity.liquidity_status
                    else:
                        available_cash -= required
                        remaining_risk = max(0.0, remaining_risk - max_loss)
            facts = {
                "candidate_run_id": run_id,
                "snapshot_id": snapshot_id,
                "research_date": research_date.isoformat(),
                "ts_code": signal.ts_code,
                "direction": signal.direction,
                "signal_score": float(signal.score),
                "indicator_price": signal.indicator_price,
                "raw_reference_price": signal.raw_reference_price,
                "stop_loss_price_raw": signal.stop_loss_price_raw,
                "theoretical_shares": theoretical,
                "planned_shares": planned,
                "required_capital": required,
                "max_loss_at_stop": max_loss,
                "gap_stress_loss": gap_loss,
                "estimated_slippage": est_slippage,
                "actual_risk_pct": actual_risk,
                "eligible": eligible,
                "reject_reason": reject_reason,
                "reason": _json_safe(dict(signal.reason) | {
                    "market_rule_version": rule_row["market_rule_version"],
                    "fee_rule_version": rule_row["fee_rule_version"],
                    "rule_version": rule_row["rule_version"],
                }),
            }
            candidate_rows.append(facts | {"candidate_hash": _candidate_hash(facts)})
        persisted_rows = []
        for row in candidate_rows:
            persisted_rows.append({
                "candidate_id": str(uuid.uuid4()), "candidate_run_id": row["candidate_run_id"],
                "snapshot_id": row["snapshot_id"], "research_date": research_date,
                "ts_code": row["ts_code"], "direction": row["direction"],
                "signal_score": row["signal_score"], "indicator_price": row["indicator_price"],
                "raw_reference_price": row["raw_reference_price"], "stop_loss_price_raw": row["stop_loss_price_raw"],
                "theoretical_shares": row["theoretical_shares"], "planned_shares": row["planned_shares"],
                "required_capital": row["required_capital"], "max_loss_at_stop": row["max_loss_at_stop"],
                "gap_stress_loss": row["gap_stress_loss"], "estimated_slippage": row["estimated_slippage"],
                "actual_risk_pct": row["actual_risk_pct"], "eligible": row["eligible"],
                "reject_reason": row["reject_reason"], "reason": _json_param(row["reason"]),
                "candidate_hash": row["candidate_hash"],
            })
        run_row = {
            "candidate_run_id": run_id, "snapshot_id": snapshot_id, "research_date": research_date,
            "run_type": DAILY_SIGNAL_RUN_TYPE, "strategy_id": strategy.strategy_id,
            "strategy_version": strategy.strategy_version, "strategy_parameters": _json_param(parameters),
            "parameters_hash": parameters_hash, "input_hash": input_hash,
            "portfolio_state": _json_param(portfolio_state), "portfolio_state_hash": portfolio_state_hash,
            "observation_input_hash": observation_hash, "rule_snapshot": _json_param(rule_snapshot),
            "rule_snapshot_hash": rule_snapshot_hash, "code_version": metadata["code_version"],
            "git_commit": metadata["commit"], "status": "RUNNING",
        }
        try:
            repository.persist_run(run_row, persisted_rows, context)
        except Exception:
            concurrent = repository.find_run(snapshot_id, strategy.strategy_id, strategy.strategy_version, input_hash, context)
            if concurrent and concurrent["status"] == "SUCCEEDED":
                return {"candidate_run_id": str(concurrent["candidate_run_id"]), "status": concurrent["status"], "candidate_count": repository.candidate_count(str(concurrent["candidate_run_id"]), context), "idempotent": True}
            raise
        return {"candidate_run_id": run_id, "status": "SUCCEEDED", "candidate_count": len(candidate_rows), "idempotent": False}
    finally:
        kernel.close()


def _evidence_for_candidate(repository: CandidateRepository, context: RunContext, snapshot_id: str, ts_code: str, cutoff: date) -> list[dict[str, Any]]:
    # authority_level is deliberately returned with each evidence item.
    # The packet's authority_level remains an immutable snapshot fact.
    return [_json_safe(dict(row)) for row in repository.evidence(snapshot_id, ts_code, cutoff, context)]




def list_candidates(candidate_run_id: str, *, eligible_only: bool = False) -> list[dict[str, Any]]:
    """List candidate IDs and sizing summaries for the manual review queue."""
    kernel, repository, context = _service("list")
    try:
        return [_json_safe(dict(row)) for row in repository.list_candidates(candidate_run_id, eligible_only, context)]
    finally:
        kernel.close()




def get_candidate_packet(candidate_id: str, *, portfolio_id: str | None = None) -> dict[str, Any]:
    """Return the immutable, as-of analysis packet for one candidate."""
    kernel, repository, context = _service("packet")
    try:
        row = repository.candidate_packet(candidate_id, context)
        if not row:
            raise ValueError(f"不存在的候选: {candidate_id}")
        if row["snapshot_status"] != "FROZEN" or row["run_status"] != "SUCCEEDED":
            raise RuntimeError("候选或其 Snapshot 尚未处于可审计状态")
        cutoff = _date(row["research_date"])
        evidence = _evidence_for_candidate(repository, context, str(row["snapshot_id"]), str(row["ts_code"]), cutoff)
        packet = {
            "packet_schema_version": "candidate-packet-v1", "candidate_id": str(row["candidate_id"]),
            "candidate_run_id": str(row["candidate_run_id"]), "snapshot_id": str(row["snapshot_id"]),
            "asof_date": cutoff.isoformat(), "ts_code": row["ts_code"], "direction": row["direction"],
            "signal_score": float(row["signal_score"]), "indicator_price": _number(row["indicator_price"]),
            "raw_reference_price": _number(row["raw_reference_price"]), "stop_loss_price_raw": _number(row["stop_loss_price_raw"]),
            "theoretical_shares": _number(row["theoretical_shares"]), "planned_shares": int(row["planned_shares"]),
            "required_capital": float(row["required_capital"]), "max_loss_at_stop": float(row["max_loss_at_stop"]),
            "gap_stress_loss": float(row["gap_stress_loss"]), "estimated_slippage": float(row["estimated_slippage"]),
            "actual_risk_pct": float(row["actual_risk_pct"]), "eligible": bool(row["eligible"]),
            "reject_reason": row["reject_reason"], "reason": _json_safe(row["reason"] or {}),
            "candidate_hash": str(row["candidate_hash"]).strip(), "strategy_id": row["strategy_id"],
            "strategy_version": row["strategy_version"], "strategy_parameters": _json_safe(row["strategy_parameters"] or {}),
            "candidate_run_input_hash": row["input_hash"], "portfolio_state": _json_safe(row["portfolio_state"] or {}),
            "portfolio_state_hash": row["portfolio_state_hash"], "observation_input_hash": row["observation_input_hash"],
            "rule_snapshot": _json_safe(row["rule_snapshot"] or {}), "rule_snapshot_hash": row["rule_snapshot_hash"],
            "snapshot_input_manifest_hash": row["input_manifest_hash"], "snapshot_quality_report_hash": row["quality_report_hash"],
            "evidence_ids": [str(item["evidence_id"]) for item in evidence], "evidence": evidence,
        }
        if portfolio_id and (packet["portfolio_state"] or {}).get("portfolio_id") != str(portfolio_id):
            raise RuntimeError("请求的 Portfolio 与候选运行冻结的账户状态不一致")
        packet["input_hash"] = _hash(packet)
        return _json_safe(packet)
    finally:
        kernel.close()




def _history_rows(repository: CandidateRepository, context: RunContext, ts_code: str, end_date: date, limit: int, start_date: date | None = None) -> list[dict[str, Any]]:
    if limit <= 0 or limit > 2000:
        raise ValueError("limit 必须在 1..2000")
    rows = repository.history(ts_code, end_date, start_date or date.min, limit, context)
    fields = ("trade_date", "raw_open", "raw_high", "raw_low", "raw_close", "adj_close_base", "return_1d", "return_20d", "return_60d", "ma20", "ma60", "ma120", "volatility_20d", "volume_ratio_20d", "amount_ma20", "quality_status", "pe_ttm", "pe_pct_5y", "pb", "pb_pct_5y")
    result = [{field: _json_safe(row.get(field)) for field in fields} for row in rows]
    result.reverse()
    return result


def get_stock_history(ts_code: str, end_date: date, *, start_date: date | None = None, limit: int = 120) -> dict[str, Any]:
    """读取可变派生层供探索画图；不得作为正式候选或回测审计输入。"""
    kernel, repository, context = _service("history")
    try:
        rows = _history_rows(repository, context, ts_code, end_date, limit, start_date)
        return {"schema_version": "stock-history-v1", "ts_code": ts_code, "asof_date": end_date.isoformat(), "rows": rows}
    finally:
        kernel.close()




def get_stock_chart_data(ts_code: str, end_date: date, *, limit: int = 120) -> dict[str, Any]:
    """Return dependency-free K-line/indicator data for a manual AI chart review."""
    history = get_stock_history(ts_code, end_date, limit=limit)
    candles = [
        {
            "trade_date": row["trade_date"],
            "open": row["raw_open"],
            "high": row["raw_high"],
            "low": row["raw_low"],
            "close": row["raw_close"],
            "adj_close_base": row["adj_close_base"],
            "ma20": row["ma20"],
            "ma60": row["ma60"],
            "ma120": row["ma120"],
            "volume_ratio_20d": row["volume_ratio_20d"],
            "pe_pct_5y": row["pe_pct_5y"],
        }
        for row in history["rows"]
    ]
    return {
        "schema_version": "stock-chart-data-v1",
        "ts_code": ts_code,
        "asof_date": end_date.isoformat(),
        "candles": candles,
        "note": "raw OHLC is execution-coordinate data; adj_close_base and moving averages are indicator-coordinate data",
    }
