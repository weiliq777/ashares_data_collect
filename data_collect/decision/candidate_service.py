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
from data_collect.utils.db import get_connection, require_psycopg2


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


def _snapshot_context(cur, snapshot_id: str) -> dict[str, Any]:
    cur.execute(
        """SELECT snapshot_id, status, research_cutoff_date, data_baseline_tag,
                  schema_version, code_version, git_commit, git_dirty,
                  diff_hash, quality_report_hash, input_manifest_hash
             FROM research.snapshot_manifest
            WHERE snapshot_id=%s
            FOR SHARE""",
        (snapshot_id,),
    )
    row = cur.fetchone()
    if not row:
        raise ValueError(f"不存在的 Research Snapshot: {snapshot_id}")
    if row[1] != "FROZEN":
        raise RuntimeError(f"候选生成只允许 FROZEN Snapshot: {snapshot_id} ({row[1]})")
    return {
        "snapshot_id": str(row[0]),
        "status": row[1],
        "asof_date": _date(row[2]).isoformat(),
        "data_baseline_tag": row[3],
        "schema_version": row[4],
        "code_version": row[5],
        "git_commit": row[6],
        "git_dirty": bool(row[7]),
        "diff_hash": row[8],
        "quality_report_hash": row[9],
        "input_manifest_hash": row[10],
    }


def _observations(cur, snapshot_id: str, research_date: date) -> tuple[Observation, ...]:
    cur.execute(
        """SELECT s.ts_code, s.is_listed_asof, s.is_st_asof, s.eligible_by_market,
                  is_suspended_asof, can_buy, can_sell, limit_status, raw_close,
                  adj_close_base, return_1d, return_20d, return_60d, ma20, ma60,
                  ma120, volume_ratio_20d, pe_ttm, pe_pct_5y, is_pe_meaningful,
                  effective_announce_date, roe, roa, debt_to_assets, revenue_yoy,
                  tr_yoy, q_sales_yoy, netprofit_yoy, operating_cashflow,
                  net_profit, quality_status, market, name, raw_volume, raw_amount,
                  raw_high, raw_low, list_date, available_observation_count,
                  ma20_observation_count, ma60_observation_count,
                  ma120_observation_count, valuation_observation_count,
                  pe_window_start_date_5y, has_full_pe_5y_history,
                  annual_report_count_5y, roe_above_15_years_5y, roe_5y_min,
                  roe_5y_all_above_15, ocf_report_count_3y,
                  ocf_positive_years_3y, ocf_3y_all_positive,
                  quarterly_report_count_4q, q_sales_yoy_above_30_quarters_4q,
                  q_sales_yoy_4q_all_above_30, financial_version_available_date,
                  up_limit, down_limit, raw_open
             FROM research.stock_snapshot_daily s
            WHERE s.snapshot_id=%s AND s.research_date=%s
            ORDER BY ts_code""",
        (snapshot_id, research_date),
    )
    rows = cur.fetchall()
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
        "ma60_observation_count", "ma120_observation_count",
        "valuation_observation_count", "pe_window_start_date_5y",
        "has_full_pe_5y_history", "annual_report_count_5y",
        "roe_above_15_years_5y", "roe_5y_min", "roe_5y_all_above_15",
        "ocf_report_count_3y", "ocf_positive_years_3y", "ocf_3y_all_positive",
        "quarterly_report_count_4q", "q_sales_yoy_above_30_quarters_4q",
        "q_sales_yoy_4q_all_above_30", "financial_version_available_date",
        "up_limit", "down_limit", "raw_open",
    )
    result = []
    for row in rows:
        values = {}
        for index, field in enumerate(fields, start=1):
            value = row[index]
            values[field] = (
                _date(value).isoformat()
                if field in {"effective_announce_date", "financial_version_available_date", "list_date", "pe_window_start_date_5y"} and value
                else value
            )
        result.append(Observation(research_date=research_date, ts_code=str(row[0]), values=values))
    return tuple(result)


def _portfolio_state(
    cur, portfolio_id: str | None, snapshot_id: str, initial_cash: float,
    rule_snapshot: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], dict[str, dict[str, Any]], float, float, float | None]:
    if not portfolio_id:
        state = {
            "portfolio_id": None,
            "account": {"initial_cash": float(initial_cash)},
            "cash_ledger": {"ledger_id": None, "balance": float(initial_cash)},
            "lots": [],
            "valuation": {
                "cash_balance": float(initial_cash), "equity_base": float(initial_cash),
                "available_cash": float(initial_cash),
                "existing_stop_risk": 0.0, "positions": {},
            },
        }
        return state, {}, float(initial_cash), float(initial_cash), 0.0
    cur.execute(
        """SELECT initial_cash, reserve_cash_pct, min_buy_lot,
                  allow_sell_odd_lot, t_plus_one, price_tick,
                  commission_rate, min_commission, stamp_duty_sell_rate,
                  transfer_fee_rate, slippage_bps, cash_settlement_days,
                  source_snapshot_id, portfolio_version
             FROM portfolio.account_manifest
            WHERE portfolio_id=%s AND status='ACTIVE' FOR SHARE""",
        (portfolio_id,),
    )
    account = cur.fetchone()
    if not account:
        raise ValueError(f"不存在或未激活的 Portfolio: {portfolio_id}")
    cur.execute(
        """SELECT lot_id, ts_code, open_date, available_sell_date,
                  open_shares, remaining_shares, cost_price, stop_loss_price_raw
             FROM portfolio.position_lot
            WHERE portfolio_id=%s AND remaining_shares > 0
            ORDER BY ts_code, lot_id""",
        (portfolio_id,),
    )
    lot_rows = cur.fetchall()
    positions: dict[str, dict[str, Any]] = {}
    for _, code, _, _, _, shares, _, _ in lot_rows:
        item = positions.setdefault(str(code), {"hold_shares": 0})
        item["hold_shares"] += int(shares)
    cur.execute(
        """SELECT ledger_id, trade_date, event_type, balance, available_date,
                  reference_id, created_at
             FROM portfolio.cash_ledger
            WHERE portfolio_id=%s ORDER BY created_at DESC, ledger_id DESC LIMIT 1""",
        (portfolio_id,),
    )
    cash_row = cur.fetchone()
    cash = float(cash_row[3]) if cash_row else float(account[0])
    available_asof = date.fromisoformat(rule_snapshot["execution_date"]) if rule_snapshot else date.max
    cur.execute(
        """SELECT COALESCE(SUM(amount),0),
                  COALESCE(SUM(amount) FILTER (WHERE available_date<=%s),0)
             FROM portfolio.cash_ledger
            WHERE portfolio_id=%s""",
        (available_asof, portfolio_id),
    )
    _total_ledger_cash, available_cash = (float(value) for value in cur.fetchone())
    equity = cash
    existing_risk = 0.0
    risk_unknown = False
    markets: dict[str, str | None] = {}
    if positions:
        cur.execute(
            """SELECT ts_code, raw_close, market
                 FROM research.stock_snapshot_daily
                WHERE snapshot_id=%s AND research_date=(
                    SELECT research_cutoff_date FROM research.snapshot_manifest
                     WHERE snapshot_id=%s
                ) AND ts_code = ANY(%s)""",
            (snapshot_id, snapshot_id, list(positions)),
        )
        price_rows = cur.fetchall()
        prices = {str(row[0]): _number(row[1]) for row in price_rows}
        markets = {str(row[0]): row[2] for row in price_rows}
        missing = sorted(code for code in positions if not prices.get(code) or prices[code] <= 0)
        if missing:
            raise RuntimeError(f"Portfolio 风险基数缺少 FROZEN Snapshot 持仓价格: {missing}")
        equity += sum(positions[code]["hold_shares"] * prices[code] for code in positions)
    for _, code, _, _, _, shares, cost, stop in lot_rows:
        if stop is None or rule_snapshot is None:
            risk_unknown = True
            continue
        rule_row = _rule_for_market(rule_snapshot, markets.get(str(code)))
        market_rule, fee_rule, risk_policy = _domain_rules(rule_row)
        existing_risk += open_position_stop_risk(
            int(shares), float(cost), float(stop), fee_rule, market_rule, risk_policy
        )
    account_fields = (
        "initial_cash", "reserve_cash_pct", "min_buy_lot", "allow_sell_odd_lot",
        "t_plus_one", "price_tick", "commission_rate", "min_commission",
        "stamp_duty_sell_rate", "transfer_fee_rate", "slippage_bps",
        "cash_settlement_days", "source_snapshot_id", "portfolio_version",
    )
    lot_fields = (
        "lot_id", "ts_code", "open_date", "available_sell_date", "open_shares",
        "remaining_shares", "cost_price", "stop_loss_price_raw",
    )
    ledger_fields = (
        "ledger_id", "trade_date", "event_type", "balance", "available_date",
        "reference_id", "created_at",
    )
    state = {
        "portfolio_id": str(portfolio_id),
        "account": _json_safe(dict(zip(account_fields, account))),
        "cash_ledger": _json_safe(dict(zip(ledger_fields, cash_row))) if cash_row else {
            "ledger_id": None, "balance": float(account[0]),
        },
        "lots": [_json_safe(dict(zip(lot_fields, row))) for row in lot_rows],
        "valuation": {
            "cash_balance": cash, "available_cash": available_cash, "equity_base": equity,
            "existing_stop_risk": None if risk_unknown else existing_risk,
            "positions": positions,
        },
    }
    return state, positions, available_cash, equity, (None if risk_unknown else existing_risk)


def _rule_snapshot(cur, decision_date: date) -> dict[str, Any]:
    cur.execute(
        """SELECT MIN(trade_date) FROM trading_calendar
            WHERE exchange='SSE' AND is_open IS TRUE AND trade_date > %s""",
        (decision_date,),
    )
    execution_date = cur.fetchone()[0]
    if execution_date is None:
        raise RuntimeError(f"缺少 {decision_date} 之后的交易日历，无法绑定执行规则")
    cur.execute(
        """SELECT rule_version, market_rule_version, fee_rule_version,
                  effective_from, effective_to, market, min_buy_lot,
                  allow_sell_odd_lot, t_plus_one, price_tick,
                  cash_settlement_days, limit_pct, commission_rate,
                  min_commission, stamp_duty_sell_rate, transfer_fee_rate,
                  slippage_bps, parameters, approval_status, approved_by,
                  approved_at, approval_hash
             FROM backtest.market_rule_effective
            WHERE effective_from <= %s
              AND (effective_to IS NULL OR effective_to >= %s)
              AND approval_status='APPROVED'
            ORDER BY CASE WHEN market='ALL' THEN 1 ELSE 0 END, market, rule_version""",
        (execution_date, execution_date),
    )
    fields = (
        "rule_version", "market_rule_version", "fee_rule_version",
        "effective_from", "effective_to", "market", "min_buy_lot",
        "allow_sell_odd_lot", "t_plus_one", "price_tick",
        "cash_settlement_days", "limit_pct", "commission_rate",
        "min_commission", "stamp_duty_sell_rate", "transfer_fee_rate",
        "slippage_bps", "parameters", "approval_status", "approved_by",
        "approved_at", "approval_hash",
    )
    rows = [_json_safe(dict(zip(fields, row))) for row in cur.fetchall()]
    if not rows:
        raise RuntimeError(f"{execution_date} 没有已批准的市场/费用规则")
    return {"decision_date": decision_date.isoformat(),
            "execution_date": execution_date.isoformat(), "rules": rows}


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
    with get_connection() as conn, conn.cursor() as cur:
        conn.set_session(isolation_level="REPEATABLE READ", readonly=False, autocommit=False)
        snapshot = _snapshot_context(cur, snapshot_id)
        if isinstance(strategy, FundamentalValueGrowthTrendStrategy) and snapshot["schema_version"] != "research-v1.2":
            raise RuntimeError("正式 fundamental-v2 候选只允许 research-v1.2 FROZEN Snapshot")
        research_date = date.fromisoformat(snapshot["asof_date"])
        observations = _observations(cur, snapshot_id, research_date)
        rule_snapshot = _rule_snapshot(cur, research_date)
        portfolio_state, positions, current_cash, equity_base, existing_risk = _portfolio_state(
            cur, portfolio_id, snapshot_id, initial_cash, rule_snapshot,
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
        cur.execute(
            """SELECT candidate_run_id, status FROM research.candidate_run
                WHERE snapshot_id=%s AND run_type=%s AND strategy_id=%s
                  AND strategy_version=%s AND input_hash=%s""",
            (snapshot_id, DAILY_SIGNAL_RUN_TYPE, strategy.strategy_id,
             strategy.strategy_version, input_hash),
        )
        existing = cur.fetchone()
        if existing:
            if existing[1] != "SUCCEEDED":
                raise RuntimeError(f"相同参数的候选运行已存在但状态为 {existing[1]}: {existing[0]}")
            cur.execute(
                "SELECT COUNT(*) FROM research.candidate WHERE candidate_run_id=%s",
                (existing[0],),
            )
            return {"candidate_run_id": str(existing[0]), "status": existing[1], "candidate_count": cur.fetchone()[0], "idempotent": True}
        cur.execute(
            """INSERT INTO research.candidate_run
               (candidate_run_id,snapshot_id,research_date,run_type,strategy_id,
                strategy_version,strategy_parameters,parameters_hash,input_hash,
                portfolio_state,portfolio_state_hash,observation_input_hash,
                rule_snapshot,rule_snapshot_hash,code_version,git_commit,status)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'RUNNING')
               ON CONFLICT (snapshot_id,run_type,strategy_id,strategy_version,input_hash)
               DO NOTHING
               RETURNING candidate_run_id""",
            (run_id, snapshot_id, research_date, DAILY_SIGNAL_RUN_TYPE,
             strategy.strategy_id, strategy.strategy_version, _json_param(parameters),
             parameters_hash, input_hash, _json_param(portfolio_state),
             portfolio_state_hash, observation_hash, _json_param(rule_snapshot),
             rule_snapshot_hash, metadata["code_version"], metadata["commit"]),
        )
        inserted_run = cur.fetchone()
        if inserted_run is None:
            cur.execute(
                """SELECT candidate_run_id, status FROM research.candidate_run
                    WHERE snapshot_id=%s AND run_type=%s AND strategy_id=%s
                      AND strategy_version=%s AND input_hash=%s
                    FOR SHARE""",
                (snapshot_id, DAILY_SIGNAL_RUN_TYPE, strategy.strategy_id,
                 strategy.strategy_version, input_hash),
            )
            concurrent = cur.fetchone()
            if not concurrent or concurrent[1] != "SUCCEEDED":
                raise RuntimeError("相同参数的候选运行正在执行或已失败，请稍后重试")
            cur.execute(
                "SELECT COUNT(*) FROM research.candidate WHERE candidate_run_id=%s",
                (concurrent[0],),
            )
            return {"candidate_run_id": str(concurrent[0]), "status": concurrent[1], "candidate_count": cur.fetchone()[0], "idempotent": True}
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
        psycopg2, _ = require_psycopg2()
        for row in candidate_rows:
            cur.execute(
                """INSERT INTO research.candidate
                   (candidate_id,candidate_run_id,snapshot_id,research_date,ts_code,direction,
                    signal_score,indicator_price,raw_reference_price,stop_loss_price_raw,
                    theoretical_shares,planned_shares,required_capital,max_loss_at_stop,
                    gap_stress_loss,estimated_slippage,actual_risk_pct,eligible,reject_reason,
                    reason,candidate_hash)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (str(uuid.uuid4()), row["candidate_run_id"], row["snapshot_id"], research_date,
                 row["ts_code"], row["direction"], row["signal_score"], row["indicator_price"],
                 row["raw_reference_price"], row["stop_loss_price_raw"], row["theoretical_shares"],
                 row["planned_shares"], row["required_capital"], row["max_loss_at_stop"],
                 row["gap_stress_loss"], row["estimated_slippage"], row["actual_risk_pct"],
                 row["eligible"], row["reject_reason"], _json_param(row["reason"]), row["candidate_hash"]),
            )
        cur.execute(
            """UPDATE research.candidate_run
                  SET status='SUCCEEDED', completed_at=CURRENT_TIMESTAMP
                WHERE candidate_run_id=%s""",
            (run_id,),
        )
        conn.commit()
    return {"candidate_run_id": run_id, "status": "SUCCEEDED", "candidate_count": len(candidate_rows), "idempotent": False}


def _evidence_for_candidate(cur, snapshot_id: str, ts_code: str, cutoff: date) -> list[dict[str, Any]]:
    cur.execute(
        """SELECT evidence_id, source_system, source_type, source_locator,
                  document_index, document_id, source_url, content_hash,
                  published_at, effective_date, claim_role, authority_level,
                  locator, note
             FROM research.evidence_reference
            WHERE snapshot_id=%s AND ts_code=%s
              AND (published_at IS NULL OR published_at::date <= %s)
              AND (effective_date IS NULL OR effective_date <= %s)
            ORDER BY claim_role, published_at DESC NULLS LAST, evidence_id""",
        (snapshot_id, ts_code, cutoff, cutoff),
    )
    fields = (
        "evidence_id", "source_system", "source_type", "source_locator",
        "document_index", "document_id", "source_url", "content_hash",
        "published_at", "effective_date", "claim_role", "authority_level",
        "locator", "note",
    )
    result = []
    for row in cur.fetchall():
        item = {field: _json_safe(row[index]) for index, field in enumerate(fields)}
        result.append(item)
    return result


def list_candidates(candidate_run_id: str, *, eligible_only: bool = False) -> list[dict[str, Any]]:
    """List candidate IDs and sizing summaries for the manual review queue."""
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """SELECT candidate_id,ts_code,direction,signal_score,planned_shares,
                      required_capital,max_loss_at_stop,gap_stress_loss,
                      actual_risk_pct,eligible,reject_reason,candidate_hash
                 FROM research.candidate
                WHERE candidate_run_id=%s
                  AND (%s IS TRUE OR eligible IS TRUE)
                ORDER BY CASE WHEN direction='SELL' THEN 0 ELSE 1 END,
                         signal_score DESC, ts_code""",
            (candidate_run_id, not eligible_only),
        )
        fields = (
            "candidate_id", "ts_code", "direction", "signal_score", "planned_shares",
            "required_capital", "max_loss_at_stop", "gap_stress_loss", "actual_risk_pct",
            "eligible", "reject_reason", "candidate_hash",
        )
        return [_json_safe(dict(zip(fields, row))) for row in cur.fetchall()]


def get_candidate_packet(candidate_id: str, *, portfolio_id: str | None = None) -> dict[str, Any]:
    """Return the immutable, as-of analysis packet for one candidate."""
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """SELECT c.candidate_id,c.candidate_run_id,c.snapshot_id,c.research_date,
                      c.ts_code,c.direction,c.signal_score,c.indicator_price,
                      c.raw_reference_price,c.stop_loss_price_raw,c.theoretical_shares,
                      c.planned_shares,c.required_capital,c.max_loss_at_stop,
                      c.gap_stress_loss,c.estimated_slippage,c.actual_risk_pct,c.eligible,
                      c.reject_reason,c.reason,c.candidate_hash,r.strategy_id,
                      r.strategy_version,r.strategy_parameters,r.input_hash,
                      r.portfolio_state,r.portfolio_state_hash,
                      r.observation_input_hash,r.rule_snapshot,r.rule_snapshot_hash,
                      r.status,m.status,m.input_manifest_hash,m.quality_report_hash
                 FROM research.candidate c
                 JOIN research.candidate_run r ON r.candidate_run_id=c.candidate_run_id
                 JOIN research.snapshot_manifest m ON m.snapshot_id=c.snapshot_id
                WHERE c.candidate_id=%s""",
            (candidate_id,),
        )
        row = cur.fetchone()
        if not row:
            raise ValueError(f"不存在的候选: {candidate_id}")
        if row[31] != "FROZEN" or row[30] != "SUCCEEDED":
            raise RuntimeError("候选或其 Snapshot 尚未处于可审计状态")
        cutoff = _date(row[3])
        evidence = _evidence_for_candidate(cur, str(row[2]), str(row[4]), cutoff)
        packet = {
            "packet_schema_version": "candidate-packet-v1",
            "candidate_id": str(row[0]),
            "candidate_run_id": str(row[1]),
            "snapshot_id": str(row[2]),
            "asof_date": cutoff.isoformat(),
            "ts_code": row[4],
            "direction": row[5],
            "signal_score": float(row[6]),
            "indicator_price": _number(row[7]),
            "raw_reference_price": _number(row[8]),
            "stop_loss_price_raw": _number(row[9]),
            "theoretical_shares": _number(row[10]),
            "planned_shares": int(row[11]),
            "required_capital": float(row[12]),
            "max_loss_at_stop": float(row[13]),
            "gap_stress_loss": float(row[14]),
            "estimated_slippage": float(row[15]),
            "actual_risk_pct": float(row[16]),
            "eligible": bool(row[17]),
            "reject_reason": row[18],
            "reason": _json_safe(row[19] or {}),
            "candidate_hash": str(row[20]).strip(),
            "strategy_id": row[21],
            "strategy_version": row[22],
            "strategy_parameters": _json_safe(row[23] or {}),
            "candidate_run_input_hash": row[24],
            "portfolio_state": _json_safe(row[25] or {}),
            "portfolio_state_hash": row[26],
            "observation_input_hash": row[27],
            "rule_snapshot": _json_safe(row[28] or {}),
            "rule_snapshot_hash": row[29],
            "snapshot_input_manifest_hash": row[32],
            "snapshot_quality_report_hash": row[33],
            "evidence_ids": [str(item["evidence_id"]) for item in evidence],
            "evidence": evidence,
        }
        if portfolio_id:
            frozen_portfolio_id = (packet["portfolio_state"] or {}).get("portfolio_id")
            if frozen_portfolio_id != str(portfolio_id):
                raise RuntimeError("请求的 Portfolio 与候选运行冻结的账户状态不一致")
        packet["input_hash"] = _hash(packet)
        return _json_safe(packet)


def _history_rows(cur, ts_code: str, end_date: date, limit: int, start_date: date | None = None) -> list[dict[str, Any]]:
    if limit <= 0 or limit > 2000:
        raise ValueError("limit 必须在 1..2000")
    start_date = start_date or date.min
    cur.execute(
        """SELECT p.trade_date,b.open,b.high,b.low,p.raw_close,
                      p.asof_close AS adj_close_base,p.return_1d,p.return_20d,p.return_60d,
                      p.ma20,p.ma60,p.ma120,p.volatility_20d,p.volume_ratio_20d,
                      p.amount_ma20,p.quality_status,v.pe_ttm,v.pe_pct_5y,
                      v.pb,v.pb_pct_5y
                 FROM feature.price_daily_v2 p
            LEFT JOIN feature.valuation_daily_v1 v
                   ON v.ts_code=p.ts_code AND v.trade_date=p.trade_date
            LEFT JOIN LATERAL (
                SELECT open, high, low
                  FROM daily_kline
                 WHERE stock_code=p.ts_code AND trade_date=p.trade_date
            ) b ON TRUE
                WHERE p.ts_code=%s AND p.trade_date BETWEEN %s AND %s
                ORDER BY p.trade_date DESC LIMIT %s""",
        (ts_code, start_date, end_date, limit),
    )
    fields = ("trade_date", "raw_open", "raw_high", "raw_low", "raw_close", "adj_close_base", "return_1d", "return_20d", "return_60d", "ma20", "ma60", "ma120", "volatility_20d", "volume_ratio_20d", "amount_ma20", "quality_status", "pe_ttm", "pe_pct_5y", "pb", "pb_pct_5y")
    rows = [{field: _json_safe(row[index]) for index, field in enumerate(fields)} for row in cur.fetchall()]
    rows.reverse()
    return rows


def get_stock_history(ts_code: str, end_date: date, *, start_date: date | None = None, limit: int = 120) -> dict[str, Any]:
    """读取可变派生层供探索画图；不得作为正式候选或回测审计输入。"""
    with get_connection() as conn, conn.cursor() as cur:
        rows = _history_rows(cur, ts_code, end_date, limit, start_date)
    return {"schema_version": "stock-history-v1", "ts_code": ts_code, "asof_date": end_date.isoformat(), "rows": rows}


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
