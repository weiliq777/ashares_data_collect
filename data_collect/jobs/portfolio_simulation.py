"""Portfolio v1：人工批准的模拟组合持仓、成交和每日估值。

本模块不生成买卖信号、不访问券商、不发送订单。它只接受人工批准的
``order_intent``，然后在数据库事务内写入模拟成交和账本；估值输入必须
明确绑定一个 FROZEN Research Snapshot。
"""
from __future__ import annotations

import argparse
import hashlib
import json
from datetime import date, datetime
from pathlib import Path
from typing import Any, Mapping
import uuid

from data_collect.backtest.risk import FeeRule
from data_collect.jobs.backtest_dataset import _git_metadata, _json_param
from data_collect.portfolio.models import PortfolioConfig
from data_collect.utils.db import get_connection


def _round_price(price: float, tick: float) -> float:
    if price <= 0:
        raise ValueError("价格必须为正")
    rounded = round(round(price / tick) * tick, 8)
    if abs(price - rounded) > 1e-8:
        raise ValueError(f"价格 {price} 不符合最小价位 {tick}")
    return rounded


def _fee_rule(row) -> FeeRule:
    return FeeRule(
        commission_rate=float(row[10]),
        min_commission=float(row[11]),
        stamp_duty_sell_rate=float(row[12]),
        transfer_fee_rate=float(row[13]),
        slippage_bps=float(row[14]),
    )


def _valuation_input_hash(
    snapshot_id: str,
    valuation_date: date,
    cash: float,
    lots,
    prices: Mapping[str, float],
) -> str:
    """Hash the exact portfolio state and frozen prices used for one valuation day."""
    payload = {
        "snapshot_id": str(snapshot_id),
        "valuation_date": valuation_date.isoformat(),
        "cash": round(float(cash), 8),
        "positions": sorted(
            (
                str(row[3]),
                str(row[0]),
                int(row[1]),
                round(float(row[2]), 8),
            )
            for row in lots
        ),
        "prices": sorted((str(code), round(float(price), 8)) for code, price in prices.items()),
    }
    return hashlib.sha256(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _account(cur, portfolio_id: str, for_update: bool = False):
    sql = """SELECT portfolio_id, portfolio_name, portfolio_version, source_snapshot_id,
                     source_watchlist_id, initial_cash, min_buy_lot, allow_sell_odd_lot,
                     t_plus_one, reserve_cash_pct, price_tick, commission_rate, min_commission,
                     stamp_duty_sell_rate, transfer_fee_rate, slippage_bps,
                     cash_settlement_days, status
                FROM portfolio.account_manifest
               WHERE portfolio_id=%s"""
    sql += " FOR UPDATE" if for_update else ""
    cur.execute(sql, (portfolio_id,))
    row = cur.fetchone()
    if not row:
        raise ValueError(f"不存在的 Portfolio: {portfolio_id}")
    return row


def create_portfolio(
    portfolio_name: str,
    portfolio_version: str,
    source_snapshot_id: str,
    effective_date: date,
    config: PortfolioConfig | None = None,
    source_watchlist_id: str | None = None,
    code_archive_path: str | None = None,
) -> str:
    """创建并激活模拟组合；激活前必须引用 FROZEN 快照/ACTIVE 观察池。"""
    config = config or PortfolioConfig()
    metadata = _git_metadata()
    if metadata["dirty"] and not code_archive_path:
        raise RuntimeError("创建正式模拟组合需要干净 Git，或提供存在的 code_archive_path")
    if code_archive_path and not Path(code_archive_path).exists():
        raise FileNotFoundError(code_archive_path)
    portfolio_id = str(uuid.uuid4())
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """SELECT status, research_cutoff_date
                 FROM research.snapshot_manifest WHERE snapshot_id=%s""",
            (source_snapshot_id,),
        )
        snapshot = cur.fetchone()
        if not snapshot or snapshot[0] != "FROZEN":
            raise ValueError(f"Portfolio 只能引用 FROZEN Research Snapshot: {source_snapshot_id}")
        if effective_date > snapshot[1]:
            raise ValueError(
                f"Portfolio effective_date 不能晚于绑定 Snapshot 截止日: {effective_date} > {snapshot[1]}"
            )
        if source_watchlist_id:
            cur.execute(
                """SELECT snapshot_id, status FROM research.watchlist WHERE watchlist_id=%s""",
                (source_watchlist_id,),
            )
            watchlist = cur.fetchone()
            if not watchlist or str(watchlist[0]) != str(source_snapshot_id) or watchlist[1] != "ACTIVE":
                raise ValueError("Portfolio 必须引用同一 FROZEN Snapshot 下的 ACTIVE 观察池")
        cur.execute(
            """INSERT INTO portfolio.account_manifest
              (portfolio_id,portfolio_name,portfolio_version,source_snapshot_id,source_watchlist_id,
               initial_cash,min_buy_lot,allow_sell_odd_lot,t_plus_one,reserve_cash_pct,price_tick,
               commission_rate,min_commission,stamp_duty_sell_rate,transfer_fee_rate,slippage_bps,
               cash_settlement_days,
               status,code_version,git_commit,code_archive_path)
              VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'DRAFT',%s,%s,%s)""",
            (portfolio_id, portfolio_name, portfolio_version, source_snapshot_id, source_watchlist_id,
             config.initial_cash, config.min_buy_lot, config.allow_sell_odd_lot, config.t_plus_one,
             config.reserve_cash_pct, config.price_tick, config.fee_rule.commission_rate,
             config.fee_rule.min_commission, config.fee_rule.stamp_duty_sell_rate,
             config.fee_rule.transfer_fee_rate, config.fee_rule.slippage_bps,
             config.cash_settlement_days,
             metadata["code_version"], metadata["commit"], code_archive_path),
        )
        cur.execute(
            """UPDATE portfolio.account_manifest SET status='ACTIVE' WHERE portfolio_id=%s""",
            (portfolio_id,),
        )
        cur.execute(
            """INSERT INTO portfolio.snapshot_binding
                      (portfolio_id, valuation_date, snapshot_id)
               VALUES (%s,%s,%s)
               ON CONFLICT (portfolio_id, valuation_date) DO NOTHING""",
            (portfolio_id, effective_date, source_snapshot_id),
        )
        cur.execute(
            """INSERT INTO portfolio.cash_ledger
              (portfolio_id,trade_date,event_type,amount,balance,available_date,description)
              VALUES (%s,%s,'INIT',%s,%s,%s,%s)""",
            (portfolio_id, effective_date, config.initial_cash, config.initial_cash,
             effective_date, "模拟组合初始现金"),
        )
        cur.execute(
            """INSERT INTO portfolio.audit_event
              (portfolio_id,event_date,event_type,payload) VALUES (%s,%s,'PORTFOLIO_ACTIVATED',%s)""",
            (portfolio_id, effective_date, _json_param({
                "snapshot_id": source_snapshot_id,
                "watchlist_id": source_watchlist_id,
                "code_version": metadata["code_version"],
            })),
        )
        conn.commit()
    return portfolio_id


def create_intent(
    portfolio_id: str,
    snapshot_id: str,
    ts_code: str,
    direction: str,
    decision_date: date,
    intended_execution_date: date,
    requested_shares: int,
    reference_price: float | None = None,
    stop_loss_price_raw: float | None = None,
    watchlist_id: str | None = None,
    source_type: str = "MANUAL",
    reason: Mapping[str, Any] | None = None,
) -> str:
    intent_id = str(uuid.uuid4())
    with get_connection() as conn, conn.cursor() as cur:
        _account(cur, portfolio_id)
        cur.execute(
            """INSERT INTO portfolio.order_intent
              (intent_id,portfolio_id,snapshot_id,watchlist_id,ts_code,decision_date,
               intended_execution_date,direction,requested_shares,reference_price,
               stop_loss_price_raw,source_type,reason)
              VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (intent_id, portfolio_id, snapshot_id, watchlist_id, ts_code, decision_date,
             intended_execution_date, direction, requested_shares, reference_price,
             stop_loss_price_raw, source_type, _json_param(dict(reason or {}))),
        )
        cur.execute(
            """INSERT INTO portfolio.audit_event
              (portfolio_id,event_date,event_type,intent_id,payload)
              VALUES (%s,%s,'INTENT_CREATED',%s,%s)""",
            (portfolio_id, decision_date, intent_id, _json_param({"source_type": source_type})),
        )
        conn.commit()
    return intent_id


def import_backtest_order(
    portfolio_id: str,
    backtest_order_id: str,
    *,
    actor: str = "backtest-import",
) -> str:
    """把已完成的 Backtest 订单导入为待人工审批 Intent。

    这是唯一允许的 Backtest -> Portfolio 入口：只复制订单审计元数据，
    永远不批准、不成交、不访问券商接口。相同 Portfolio/订单重复导入时
    返回原 Intent，并校验 source hash 保持一致。
    """
    with get_connection() as conn, conn.cursor() as cur:
        account = _account(cur, portfolio_id, for_update=True)
        account_snapshot, account_watchlist, account_status = account[3], account[4], account[17]
        if account_status != "ACTIVE":
            raise ValueError("只有 ACTIVE Portfolio 才能导入 Backtest 订单")

        cur.execute(
            """SELECT o.order_id, o.run_id, r.dataset_id, o.signal_id,
                      o.ts_code, o.decision_date, o.order_date, o.expire_date,
                      o.direction, o.planned_shares, o.order_shares,
                      o.requested_shares, o.actual_shares, o.reference_price,
                      o.stop_loss_price_raw, o.status, o.liquidity_status,
                      o.liquidity_reject_reason, o.risk_policy_version,
                      o.theoretical_shares, o.required_capital, o.max_loss_at_stop,
                      o.gap_stress_loss, o.buy_fees, o.estimated_sell_fees,
                      o.estimated_slippage, o.actual_risk_pct,
                      o.order_audit_hash, r.status, d.status, d.dataset_version
                 FROM backtest.order o
                 JOIN backtest.run_manifest r ON r.run_id=o.run_id
                 JOIN backtest.dataset_manifest d ON d.dataset_id=r.dataset_id
                WHERE o.order_id=%s
                FOR SHARE OF o""",
            (backtest_order_id,),
        )
        row = cur.fetchone()
        if not row:
            raise ValueError(f"不存在的 Backtest 订单: {backtest_order_id}")
        (
            order_id, run_id, dataset_id, signal_id, ts_code, decision_date,
            order_date, expire_date, direction, planned_shares, order_shares,
            requested_shares, actual_shares, reference_price, stop_loss, order_status,
            liquidity_status, liquidity_reason, risk_version, theoretical_shares,
            required_capital, max_loss_at_stop, gap_stress_loss, buy_fees,
            estimated_sell_fees, estimated_slippage, actual_risk_pct,
            order_audit_hash, run_status, dataset_status, dataset_version,
        ) = row
        if run_status != "SUCCEEDED" or dataset_status != "FROZEN" or order_status != "FILLED":
            raise ValueError("只能导入 SUCCEEDED/FROZEN 数据集中的 FILLED Backtest 订单")
        if account_snapshot is None:
            raise ValueError("Portfolio 未绑定 Research Snapshot")

        cur.execute(
            """SELECT sm.research_cutoff_date
                 FROM research.snapshot_manifest sm
                WHERE sm.snapshot_id=%s AND sm.status='FROZEN'""",
            (account_snapshot,),
        )
        snapshot = cur.fetchone()
        if not snapshot:
            raise ValueError("Portfolio 绑定的 Research Snapshot 不是 FROZEN")
        if decision_date != snapshot[0]:
            raise ValueError(
                "Portfolio v1 只接受与绑定 Snapshot cutoff 相同决策日的订单"
            )
        cur.execute(
            """SELECT research_snapshot_id
                 FROM backtest.dataset_member
                WHERE dataset_id=%s AND research_date=%s""",
            (dataset_id, decision_date),
        )
        member = cur.fetchone()
        if not member or str(member[0]) != str(account_snapshot):
            raise ValueError("Backtest 订单决策日没有绑定 Portfolio Snapshot 的 Dataset 成员")
        if account_watchlist:
            cur.execute(
                """SELECT 1 FROM research.watchlist_member
                    WHERE watchlist_id=%s AND snapshot_id=%s AND ts_code=%s""",
                (account_watchlist, account_snapshot, ts_code),
            )
            if not cur.fetchone():
                raise ValueError("Backtest 订单标的不在 Portfolio ACTIVE 观察池")

        if not order_audit_hash or len(str(order_audit_hash).strip()) != 64:
            raise ValueError("Backtest order 缺少数据库生成的 order_audit_hash")
        # PostgreSQL is the canonical authority. The order trigger hashes the
        # exact persisted order payload plus its bound Dataset version, so a
        # direct SQL insert cannot substitute a Python-only hash.
        source_order_hash = str(order_audit_hash).strip()

        cur.execute(
            """SELECT intent_id, source_order_hash
                 FROM portfolio.order_intent
                WHERE portfolio_id=%s AND source_backtest_order_id=%s
                FOR UPDATE""",
            (portfolio_id, backtest_order_id),
        )
        existing = cur.fetchone()
        if existing:
            if existing[1] != source_order_hash:
                raise ValueError("同一 Backtest 订单的 source hash 已变化，禁止覆盖原 Intent")
            conn.commit()
            return str(existing[0])

        intent_id = str(uuid.uuid4())
        requested = int(actual_shares or order_shares)
        if requested <= 0:
            raise ValueError("FILLED Backtest 订单缺少有效实际成交股数")
        reason = {
            "air_gap": "BACKTEST_TO_PENDING_APPROVAL",
            "actor": actor,
            "source_order_hash": source_order_hash,
            "risk_policy_version": risk_version,
            "liquidity_status": liquidity_status,
        }
        cur.execute(
            """INSERT INTO portfolio.order_intent
              (intent_id,portfolio_id,snapshot_id,watchlist_id,ts_code,decision_date,
               intended_execution_date,direction,requested_shares,reference_price,
               stop_loss_price_raw,source_type,status,reason,
               source_backtest_run_id,source_backtest_order_id,source_dataset_id,
               source_signal_id,source_order_hash)
              VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'BACKTEST','PENDING_APPROVAL',%s,%s,%s,%s,%s,%s)""",
            (intent_id, portfolio_id, account_snapshot, account_watchlist, ts_code,
             decision_date, order_date, direction, requested, reference_price, stop_loss,
             _json_param(reason), run_id, order_id, dataset_id, signal_id, source_order_hash),
        )
        cur.execute(
            """INSERT INTO portfolio.audit_event
              (portfolio_id,event_date,event_type,intent_id,payload)
              VALUES (%s,%s,'BACKTEST_ORDER_IMPORTED_PENDING_APPROVAL',%s,%s)""",
            (portfolio_id, decision_date, intent_id, _json_param({
                "source_backtest_order_id": str(order_id),
                "source_backtest_run_id": str(run_id),
                "source_dataset_id": str(dataset_id),
                "source_order_hash": source_order_hash,
            })),
        )
        conn.commit()
        return intent_id


def set_intent_status(portfolio_id: str, intent_id: str, status: str, actor: str | None = None, reason: str | None = None):
    if status not in {"APPROVED", "REJECTED", "CANCELLED", "EXPIRED"}:
        raise ValueError(f"不支持的人工意图状态: {status}")
    with get_connection() as conn, conn.cursor() as cur:
        _account(cur, portfolio_id)
        cur.execute(
            """UPDATE portfolio.order_intent
                  SET status=%s, approved_by=CASE WHEN %s='APPROVED' THEN %s ELSE approved_by END
                WHERE intent_id=%s AND portfolio_id=%s
            RETURNING decision_date""",
            (status, status, actor, intent_id, portfolio_id),
        )
        row = cur.fetchone()
        if not row:
            raise ValueError("Intent 不存在或状态流转不允许")
        cur.execute(
            """INSERT INTO portfolio.audit_event
              (portfolio_id,event_date,event_type,intent_id,payload)
              VALUES (%s,%s,%s,%s,%s)""",
            (portfolio_id, row[0], f"INTENT_{status}", intent_id,
             _json_param({"actor": actor, "reason": reason})),
        )
        conn.commit()


def record_manual_fill(
    portfolio_id: str,
    intent_id: str,
    fill_date: date,
    fill_price: float,
    next_trade_date: date | None = None,
    cash_available_date: date | None = None,
    reference_price: float | None = None,
) -> str:
    """原子写入一笔人工批准的模拟成交及其账本。禁止部分成交和自动下单。"""
    with get_connection() as conn, conn.cursor() as cur:
        account = _account(cur, portfolio_id, for_update=True)
        (
            _portfolio_id, _name, _version, _snapshot, _watchlist, _initial_cash,
            min_lot, allow_odd, t_plus_one, reserve_pct, tick, _commission_rate,
            _min_commission, _stamp_rate, _transfer_rate, _slippage_bps,
            cash_settlement_days, account_status,
        ) = account
        if account_status != "ACTIVE":
            raise ValueError("Portfolio 不是 ACTIVE 状态")
        cur.execute(
            """SELECT status, ts_code, direction, intended_execution_date, requested_shares,
                      reference_price, stop_loss_price_raw
                 FROM portfolio.order_intent
                WHERE intent_id=%s AND portfolio_id=%s FOR UPDATE""",
            (intent_id, portfolio_id),
        )
        intent = cur.fetchone()
        if not intent or intent[0] != "APPROVED":
            raise ValueError("只有 APPROVED intent 可以模拟成交")
        status, ts_code, direction, intended_date, requested, intent_reference, stop = intent
        if fill_date < intended_date:
            raise ValueError("成交日期不能早于计划成交日期")
        if fill_price <= 0:
            raise ValueError("成交价必须为正")
        fill_price = _round_price(float(fill_price), float(tick))
        reference_price = reference_price if reference_price is not None else intent_reference
        if direction == "BUY" and requested % min_lot != 0:
            raise ValueError("Portfolio BUY 必须按整手")
        if direction == "SELL" and not allow_odd and requested % min_lot != 0:
            raise ValueError("Portfolio SELL 不允许零股")
        if direction == "BUY" and stop is not None and fill_price <= float(stop):
            raise ValueError("成交价不高于止损价")
        if direction == "BUY" and t_plus_one and next_trade_date is None:
            raise ValueError("T+1 买入必须提供 next_trade_date")
        if direction == "BUY" and t_plus_one and next_trade_date is not None and next_trade_date <= fill_date:
            raise ValueError("next_trade_date must be later than fill_date for T+1")
        cur.execute(
            """SELECT lot_id, ts_code, available_sell_date, remaining_shares, cost_price
                 FROM portfolio.position_lot
                WHERE portfolio_id=%s AND ts_code=%s AND remaining_shares>0
                ORDER BY open_date, lot_id FOR UPDATE""",
            (portfolio_id, ts_code),
        )
        lots = cur.fetchall()
        cur.execute(
            """SELECT COALESCE(SUM(amount),0),
                      COALESCE(SUM(amount) FILTER (WHERE available_date<=%s),0)
                 FROM portfolio.cash_ledger WHERE portfolio_id=%s""",
            (fill_date, portfolio_id),
        )
        total_cash, available_cash = (float(value) for value in cur.fetchone())
        # Reserve cash is a fixed portfolio-level floor. Prefer the latest
        # immutable daily valuation; before the first valuation, use the
        # account's initial equity rather than repeatedly taxing cash.
        cur.execute(
            """SELECT total_equity
                 FROM portfolio.performance_daily
                WHERE portfolio_id=%s AND trade_date<=%s
                ORDER BY trade_date DESC
                LIMIT 1""",
            (portfolio_id, fill_date),
        )
        equity_row = cur.fetchone()
        equity_base = float(equity_row[0]) if equity_row else float(_initial_cash)
        fee = FeeRule(
            commission_rate=float(_commission_rate), min_commission=float(_min_commission),
            stamp_duty_sell_rate=float(_stamp_rate), transfer_fee_rate=float(_transfer_rate),
            slippage_bps=float(_slippage_bps),
        )
        value = float(fill_price) * int(requested)
        commission = fee.commission(value)
        transfer = value * fee.transfer_fee_rate
        stamp = value * fee.stamp_duty_sell_rate if direction == "SELL" else 0.0
        if direction == "BUY":
            if lots:
                raise ValueError("Portfolio v1 禁止已有持仓加仓")
            total_cost = value + commission + transfer
            cash_budget = max(0.0, available_cash - equity_base * float(reserve_pct))
            if total_cost > cash_budget + 1e-9:
                raise ValueError("买入后现金不足或突破预留现金")
            available_sell_date = next_trade_date if t_plus_one else fill_date
            if available_sell_date is None:
                raise ValueError("缺少 T+1 可卖日期")
            cash_amount = -total_cost
        else:
            if sum(int(row[3]) for row in lots) < int(requested):
                raise ValueError("可卖持仓不足")
            if any(row[2] > fill_date for row in lots):
                raise ValueError("存在尚不可卖的 T+1 持仓")
            if cash_available_date is None and cash_settlement_days:
                raise ValueError("卖出资金结算日必须显式提供")
            if cash_available_date is None:
                cash_available_date = fill_date
            if cash_settlement_days and cash_available_date <= fill_date:
                raise ValueError("cash_available_date must be later than fill_date when settlement is deferred")
            if cash_available_date < fill_date:
                raise ValueError("卖出资金可用日不能早于成交日")
            proceeds = value - commission - stamp - transfer
            cash_amount = proceeds
        fill_id = str(uuid.uuid4())
        slippage_cost = abs(float(fill_price) - float(reference_price)) * int(requested) if reference_price else 0.0
        cur.execute(
            """INSERT INTO portfolio.trade_fill
              (fill_id,portfolio_id,intent_id,ts_code,fill_date,direction,fill_price,fill_shares,
               commission,stamp_duty,transfer_fee,slippage_cost)
              VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (fill_id, portfolio_id, intent_id, ts_code, fill_date, direction, fill_price,
             requested, commission, stamp, transfer, slippage_cost),
        )
        if direction == "BUY":
            lot_id = str(uuid.uuid4())
            cur.execute(
                """INSERT INTO portfolio.position_lot
                  (lot_id,portfolio_id,ts_code,open_fill_id,open_date,available_sell_date,
                   open_shares,remaining_shares,cost_price,stop_loss_price_raw)
                  VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (lot_id, portfolio_id, ts_code, fill_id, fill_date, available_sell_date,
                 requested, requested, fill_price, stop),
            )
            available_date = fill_date
        else:
            remaining = int(requested)
            for lot_id, _code, _sell_date, lot_shares, _cost in lots:
                if remaining <= 0:
                    break
                sold = min(remaining, int(lot_shares))
                remaining -= sold
                cur.execute(
                    """UPDATE portfolio.position_lot
                          SET remaining_shares=%s,
                              close_date=CASE WHEN %s=0 THEN %s ELSE close_date END,
                              close_fill_id=CASE WHEN %s=0 THEN %s ELSE close_fill_id END
                        WHERE lot_id=%s""",
                    (int(lot_shares) - sold, int(lot_shares) - sold, fill_date,
                     int(lot_shares) - sold, fill_id, lot_id),
                )
            available_date = cash_available_date
        new_balance = total_cash + cash_amount
        if new_balance < -1e-8:
            raise ValueError("成交后现金余额不能为负")
        cur.execute(
            """INSERT INTO portfolio.cash_ledger
              (portfolio_id,trade_date,event_type,amount,balance,available_date,reference_id,description)
              VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
            (portfolio_id, fill_date, direction, cash_amount, new_balance, available_date,
             fill_id, "人工批准的模拟成交"),
        )
        cur.execute(
            """UPDATE portfolio.order_intent SET status='EXECUTED' WHERE intent_id=%s""",
            (intent_id,),
        )
        cur.execute(
            """INSERT INTO portfolio.audit_event
              (portfolio_id,event_date,event_type,intent_id,fill_id,payload)
              VALUES (%s,%s,'FILL_RECORDED',%s,%s,%s)""",
            (portfolio_id, fill_date, intent_id, fill_id,
             _json_param({"direction": direction, "shares": requested, "paper_only": True})),
        )
        conn.commit()
    return fill_id


def mark_to_market(
    portfolio_id: str,
    valuation_date: date,
    snapshot_id: str,
    prices: Mapping[str, float],
) -> dict[str, Any]:
    """使用指定 FROZEN Snapshot 的原始收盘价生成组合每日估值。"""
    with get_connection() as conn, conn.cursor() as cur:
        account = _account(cur, portfolio_id, for_update=True)
        if account[17] not in {"ACTIVE", "ARCHIVED"}:
            raise ValueError("只有 ACTIVE/ARCHIVED Portfolio 可以估值")
        if not snapshot_id:
            raise ValueError("估值 Snapshot 必须与 Portfolio 绑定的 Snapshot 一致")
        cur.execute("SELECT status, research_cutoff_date FROM research.snapshot_manifest WHERE snapshot_id=%s", (snapshot_id,))
        snapshot = cur.fetchone()
        cur.execute(
            """SELECT snapshot_id FROM portfolio.snapshot_binding
                WHERE portfolio_id=%s AND valuation_date=%s AND snapshot_id=%s""",
            (portfolio_id, valuation_date, snapshot_id),
        )
        if cur.fetchone() is None:
            raise ValueError("估值 Snapshot 尚未绑定到 Portfolio 当日")
        if not snapshot or snapshot[0] != "FROZEN" or snapshot[1] != valuation_date:
            raise ValueError("Portfolio v1 估值日必须严格对应 FROZEN Snapshot 截止日")
        cur.execute(
            "SELECT MAX(fill_date) FROM portfolio.trade_fill WHERE portfolio_id=%s",
            (portfolio_id,),
        )
        latest_fill_date = cur.fetchone()[0]
        if latest_fill_date is not None and latest_fill_date > valuation_date:
            raise ValueError(
                f"不能在未来成交已写入后回补过去估值: latest_fill={latest_fill_date}, valuation={valuation_date}"
            )
        cur.execute(
            "SELECT MAX(trade_date) FROM portfolio.performance_daily WHERE portfolio_id=%s",
            (portfolio_id,),
        )
        latest_valuation_date = cur.fetchone()[0]
        if latest_valuation_date is not None and latest_valuation_date > valuation_date:
            raise ValueError(
                f"估值日期必须单调前进，不能覆盖已有较晚估值: latest={latest_valuation_date}, valuation={valuation_date}"
            )
        cur.execute(
            """SELECT ts_code, remaining_shares, cost_price, lot_id
                 FROM portfolio.position_lot
                WHERE portfolio_id=%s AND remaining_shares>0
                ORDER BY ts_code, lot_id""",
            (portfolio_id,),
        )
        lots = cur.fetchall()
        held_codes = sorted({row[0] for row in lots})
        cur.execute(
            """SELECT ts_code, raw_close
                 FROM research.stock_snapshot_daily
                WHERE snapshot_id=%s AND research_date=%s AND ts_code = ANY(%s)""",
            (snapshot_id, valuation_date, held_codes),
        )
        snapshot_prices = {row[0]: row[1] for row in cur.fetchall()}
        for code in held_codes:
            if code not in snapshot_prices or snapshot_prices[code] is None or float(snapshot_prices[code]) <= 0:
                raise ValueError(f"FROZEN Snapshot 缺少持仓原始收盘价: {code}")
            if code not in prices or prices[code] <= 0:
                raise ValueError(f"估值缺少持仓原始收盘价: {code}")
            if abs(float(prices[code]) - float(snapshot_prices[code])) > 1e-8:
                raise ValueError(f"估值价格与 FROZEN Snapshot 原始收盘价不一致: {code}")
        prices = {code: float(snapshot_prices[code]) for code in held_codes}
        price_hash = hashlib.sha256(
            json.dumps(sorted(prices.items()), separators=(",", ":")).encode()
        ).hexdigest()
        missing = sorted({row[0] for row in lots if row[0] not in prices or prices[row[0]] <= 0})
        if missing:
            raise ValueError(f"估值缺少持仓原始收盘价: {missing}")
        cur.execute(
            """SELECT COALESCE(SUM(amount),0) FROM portfolio.cash_ledger WHERE portfolio_id=%s""",
            (portfolio_id,),
        )
        cash = float(cur.fetchone()[0])
        position_value = 0.0
        position_rows = []
        for code in sorted({row[0] for row in lots}):
            code_lots = [row for row in lots if row[0] == code]
            shares = sum(int(row[1]) for row in code_lots)
            avg = sum(int(row[1]) * float(row[2]) for row in code_lots) / shares
            price = float(prices[code])
            value = shares * price
            position_value += value
            position_rows.append((portfolio_id, valuation_date, code, shares, avg, price, value,
                                  value - shares * avg, "VALID"))
        total_equity = cash + position_value
        valuation_input_hash = _valuation_input_hash(
            snapshot_id, valuation_date, cash, lots, prices
        )
        cur.execute(
            """SELECT source_snapshot_id, valuation_input_hash, cash_balance,
                          position_value, total_equity, daily_return, drawdown
                 FROM portfolio.performance_daily
                WHERE portfolio_id=%s AND trade_date=%s
                FOR UPDATE""",
            (portfolio_id, valuation_date),
        )
        existing_performance = cur.fetchone()
        if existing_performance:
            cur.execute(
                """SELECT COUNT(*) FROM portfolio.position_daily
                    WHERE portfolio_id=%s AND trade_date=%s""",
                (portfolio_id, valuation_date),
            )
            existing_position_count = cur.fetchone()[0]
            if (
                str(existing_performance[0]) != str(snapshot_id)
                or existing_performance[1] != valuation_input_hash
                or existing_position_count != len(position_rows)
            ):
                raise ValueError(
                    "同一组合/日期已有不同估值输入，禁止覆盖；请创建新的 Portfolio 版本"
                )
            conn.commit()
            return {
                "portfolio_id": portfolio_id,
                "trade_date": valuation_date,
                "cash_balance": float(existing_performance[2]),
                "position_value": float(existing_performance[3]),
                "total_equity": float(existing_performance[4]),
                "daily_return": float(existing_performance[5]) if existing_performance[5] is not None else None,
                "drawdown": float(existing_performance[6]) if existing_performance[6] is not None else None,
                "price_hash": price_hash,
                "valuation_input_hash": valuation_input_hash,
                "idempotent": True,
            }
        cur.execute(
            """SELECT total_equity FROM portfolio.performance_daily
                WHERE portfolio_id=%s AND trade_date<%s ORDER BY trade_date DESC LIMIT 1""",
            (portfolio_id, valuation_date),
        )
        previous = cur.fetchone()
        previous_equity = float(previous[0]) if previous else float(account[5])
        daily_return = total_equity / previous_equity - 1 if previous_equity else 0.0
        cur.execute(
            """SELECT COALESCE(MAX(total_equity),%s) FROM portfolio.performance_daily
                WHERE portfolio_id=%s AND trade_date<%s""",
            (float(account[5]), portfolio_id, valuation_date),
        )
        peak = float(cur.fetchone()[0])
        drawdown = (peak - total_equity) / peak if peak else 0.0
        cur.executemany(
            """INSERT INTO portfolio.position_daily
              (portfolio_id,source_snapshot_id,trade_date,ts_code,hold_shares,average_cost,raw_close,
               market_value,unrealized_pnl,valuation_input_hash,valuation_status)
              VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            [
                (row[0], snapshot_id, row[1], row[2], row[3], row[4], row[5], row[6], row[7],
                 valuation_input_hash, row[8])
                for row in position_rows
            ],
        )
        cur.execute(
            """INSERT INTO portfolio.performance_daily
              (portfolio_id,source_snapshot_id,trade_date,cash_balance,position_value,total_equity,daily_return,
               cumulative_return,drawdown,turnover,valuation_input_hash,valuation_status)
              VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'VALID')""",
            (portfolio_id, snapshot_id, valuation_date, cash, position_value, total_equity, daily_return,
             total_equity / float(account[5]) - 1, drawdown, 0.0, valuation_input_hash),
        )
        cur.execute(
            """INSERT INTO portfolio.audit_event
              (portfolio_id,event_date,event_type,payload)
              VALUES (%s,%s,'MARK_TO_MARKET',%s)""",
            (portfolio_id, valuation_date, _json_param({
                "snapshot_id": snapshot_id, "price_hash": price_hash,
                "valuation_input_hash": valuation_input_hash,
                "position_count": len(position_rows),
            })),
        )
        conn.commit()
    return {
        "portfolio_id": portfolio_id,
        "trade_date": valuation_date,
        "cash_balance": cash,
        "position_value": position_value,
        "total_equity": total_equity,
        "daily_return": daily_return,
        "drawdown": drawdown,
        "price_hash": price_hash,
        "valuation_input_hash": valuation_input_hash,
        "idempotent": False,
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description="人工确认 Portfolio 模拟组合")
    parser.add_argument("--portfolio-id", required=True)
    parser.add_argument("--snapshot-id")
    parser.add_argument("--valuation-date")
    parser.add_argument("--import-backtest-order-id", help="只导入为 BACKTEST/PENDING_APPROVAL，不批准、不成交")
    parser.add_argument("--actor", default="backtest-import")
    parser.add_argument("--price", action="append", default=[], help="TS_CODE=PRICE，可重复")
    args = parser.parse_args(argv)
    if args.import_backtest_order_id:
        print(json.dumps(import_backtest_order(
            args.portfolio_id, args.import_backtest_order_id, actor=args.actor
        ), ensure_ascii=False, default=str))
        return
    if not args.snapshot_id or not args.valuation_date:
        parser.error("估值模式必须同时提供 --snapshot-id 和 --valuation-date")
    prices = {}
    for item in args.price:
        code, value = item.split("=", 1)
        prices[code] = float(value)
    print(json.dumps(mark_to_market(
        args.portfolio_id, date.fromisoformat(args.valuation_date), args.snapshot_id, prices
    ), ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
