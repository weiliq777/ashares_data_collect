"""Typed Repository boundary for paper portfolio lifecycle and valuation."""
from __future__ import annotations

import json
import uuid
from datetime import date
from typing import Any, Mapping, Sequence

from data_collect.data_access.context import RunContext, WorkScope
from data_collect.data_access.repository.base import BaseRepository


class PortfolioRepository(BaseRepository):
    def _scope(self, portfolio_id: str, **dimensions: Any) -> WorkScope:
        return WorkScope(partition=portfolio_id, dimensions={"portfolio_id": portfolio_id, **dimensions})

    def account(self, portfolio_id: str, context: RunContext) -> dict[str, Any]:
        rows = self.kernel.read._repository_query(
            context, "portfolio.account_manifest",
            """SELECT portfolio_id,portfolio_name,portfolio_version,source_snapshot_id,source_watchlist_id,
                              initial_cash,min_buy_lot,allow_sell_odd_lot,t_plus_one,reserve_cash_pct,price_tick,
                              commission_rate,min_commission,stamp_duty_sell_rate,transfer_fee_rate,slippage_bps,
                              cash_settlement_days,status FROM portfolio.account_manifest WHERE portfolio_id=%s""",
            (portfolio_id,), self._scope(portfolio_id), 2,
        )
        if not rows:
            raise ValueError(f"不存在的 Portfolio: {portfolio_id}")
        return rows[0]

    def snapshot(self, snapshot_id: str, context: RunContext) -> dict[str, Any] | None:
        rows = self.kernel.read._repository_query(
            context, "research.snapshot_manifest",
            "SELECT status,research_cutoff_date FROM research.snapshot_manifest WHERE snapshot_id=%s",
            (snapshot_id,), WorkScope(partition=f"snapshot:{snapshot_id}"), 2,
        )
        return rows[0] if rows else None

    def watchlist(self, watchlist_id: str, context: RunContext) -> dict[str, Any] | None:
        rows = self.kernel.read._repository_query(
            context, "research.watchlist",
            "SELECT snapshot_id,status FROM research.watchlist WHERE watchlist_id=%s",
            (watchlist_id,), WorkScope(partition=f"watchlist:{watchlist_id}"), 2,
        )
        return rows[0] if rows else None

    def create_portfolio(self, row: dict[str, Any], context: RunContext) -> int:
        return self.kernel.write.bulk_insert(context, self._scope(str(row["portfolio_id"])), "portfolio.account_manifest", [row])

    def activate_portfolio(self, portfolio_id: str, context: RunContext) -> int:
        return self.kernel.write.upsert(
            context, self._scope(portfolio_id), "portfolio.account_manifest",
            [{"portfolio_id": portfolio_id, "status": "ACTIVE"}], key=("portfolio_id",), update_columns=("status",), skip_noop=True,
        )

    def bind_snapshot(self, portfolio_id: str, valuation_date: date, snapshot_id: str, context: RunContext) -> int:
        return self.kernel.write.upsert(
            context, self._scope(portfolio_id, date_from=valuation_date, date_to=valuation_date), "portfolio.snapshot_binding",
            [{"portfolio_id": portfolio_id, "valuation_date": valuation_date, "snapshot_id": snapshot_id}],
            key=("portfolio_id", "valuation_date"), update_columns=(), skip_noop=True,
        )

    def append_cash(self, row: dict[str, Any], context: RunContext) -> int:
        return self.kernel.write.bulk_insert(context, self._scope(str(row["portfolio_id"])), "portfolio.cash_ledger", [row])

    def append_audit(self, row: dict[str, Any], context: RunContext) -> int:
        return self.kernel.write.bulk_insert(context, self._scope(str(row["portfolio_id"])), "portfolio.audit_event", [row])

    def create_intent(self, row: dict[str, Any], context: RunContext) -> int:
        return self.kernel.write.bulk_insert(context, self._scope(str(row["portfolio_id"])), "portfolio.order_intent", [row])

    def backtest_order(self, order_id: str, context: RunContext) -> dict[str, Any] | None:
        rows = self.kernel.read._repository_query(
            context, "backtest.order",
            """SELECT o.order_id,o.run_id,r.dataset_id,o.signal_id,o.ts_code,o.decision_date,o.order_date,o.expire_date,
                              o.direction,o.planned_shares,o.order_shares,o.requested_shares,o.actual_shares,o.reference_price,
                              o.stop_loss_price_raw,o.status,o.liquidity_status,o.liquidity_reject_reason,o.risk_policy_version,
                              o.theoretical_shares,o.required_capital,o.max_loss_at_stop,o.gap_stress_loss,o.buy_fees,
                              o.estimated_sell_fees,o.estimated_slippage,o.actual_risk_pct,o.order_audit_hash,r.status AS run_status,
                              d.status AS dataset_status,d.dataset_version FROM backtest.order o
                              JOIN backtest.run_manifest r ON r.run_id=o.run_id JOIN backtest.dataset_manifest d ON d.dataset_id=r.dataset_id
                             WHERE o.order_id=%s""",
            (order_id,), WorkScope(partition=f"backtest-order:{order_id}"), 2,
        )
        return rows[0] if rows else None

    def dataset_member_snapshot(self, dataset_id: str, decision_date: date, context: RunContext) -> str | None:
        rows = self.kernel.read._repository_query(
            context, "backtest.dataset_member",
            """WITH components AS (
                   SELECT component_dataset_id AS dataset_id,effective_from,effective_to
                     FROM backtest.dataset_component
                    WHERE composite_dataset_id=%s
                   UNION ALL
                   SELECT dataset_id,warmup_start,evaluation_end
                     FROM backtest.dataset_manifest
                    WHERE dataset_id=%s AND dataset_kind='MATERIALIZED'
               )
               SELECT m.research_snapshot_id
                 FROM components c
                 JOIN backtest.dataset_member m ON m.dataset_id=c.dataset_id
                  AND m.research_date BETWEEN c.effective_from AND c.effective_to
                WHERE m.research_date=%s""",
            (dataset_id, dataset_id, decision_date),
            WorkScope(partition=dataset_id, date_from=decision_date, date_to=decision_date),
            2,
        )
        return str(rows[0]["research_snapshot_id"]) if rows else None

    def watchlist_contains(self, watchlist_id: str, snapshot_id: str, ts_code: str, context: RunContext) -> bool:
        rows = self.kernel.read._repository_query(context, "research.watchlist_member", "SELECT 1 AS present FROM research.watchlist_member WHERE watchlist_id=%s AND snapshot_id=%s AND ts_code=%s", (watchlist_id, snapshot_id, ts_code), WorkScope(partition=f"watchlist:{watchlist_id}"), 2)
        return bool(rows)

    def existing_import(self, portfolio_id: str, order_id: str, context: RunContext) -> dict[str, Any] | None:
        rows = self.kernel.read._repository_query(context, "portfolio.order_intent", "SELECT intent_id,source_order_hash FROM portfolio.order_intent WHERE portfolio_id=%s AND source_backtest_order_id=%s", (portfolio_id, order_id), self._scope(portfolio_id), 2)
        return rows[0] if rows else None

    def update_intent_status(self, portfolio_id: str, intent_id: str, status: str, actor: str | None, context: RunContext) -> dict[str, Any] | None:
        row = {"intent_id": intent_id, "portfolio_id": portfolio_id, "status": status}
        if status == "APPROVED":
            row["approved_by"] = actor
        self.kernel.write.upsert(context, self._scope(portfolio_id), "portfolio.order_intent", [row], key=("intent_id", "portfolio_id"), update_columns=tuple(key for key in row if key not in {"intent_id", "portfolio_id"}), skip_noop=True)
        rows = self.kernel.read._repository_query(context, "portfolio.order_intent", "SELECT decision_date FROM portfolio.order_intent WHERE intent_id=%s AND portfolio_id=%s", (intent_id, portfolio_id), self._scope(portfolio_id), 2)
        return rows[0] if rows else None

    def intent(self, portfolio_id: str, intent_id: str, context: RunContext) -> dict[str, Any] | None:
        rows = self.kernel.read._repository_query(context, "portfolio.order_intent", "SELECT status,ts_code,direction,intended_execution_date,requested_shares,reference_price,stop_loss_price_raw FROM portfolio.order_intent WHERE intent_id=%s AND portfolio_id=%s", (intent_id, portfolio_id), self._scope(portfolio_id), 2)
        return rows[0] if rows else None

    def lots(self, portfolio_id: str, ts_code: str, context: RunContext) -> list[dict[str, Any]]:
        return self.kernel.read._repository_query(context, "portfolio.position_lot", "SELECT lot_id,ts_code,available_sell_date,remaining_shares,cost_price FROM portfolio.position_lot WHERE portfolio_id=%s AND ts_code=%s AND remaining_shares>0 ORDER BY open_date,lot_id", (portfolio_id, ts_code), self._scope(portfolio_id), 100_000)

    def cash_totals(self, portfolio_id: str, asof: date, context: RunContext) -> tuple[float, float]:
        rows = self.kernel.read._repository_query(context, "portfolio.cash_ledger", "SELECT COALESCE(SUM(amount),0) AS total_cash,COALESCE(SUM(amount) FILTER (WHERE available_date<=%s),0) AS available_cash FROM portfolio.cash_ledger WHERE portfolio_id=%s", (asof, portfolio_id), self._scope(portfolio_id, date_to=asof), 2)
        row = rows[0] if rows else {"total_cash": 0, "available_cash": 0}
        return float(row["total_cash"]), float(row["available_cash"])

    def latest_equity(self, portfolio_id: str, asof: date, context: RunContext) -> float | None:
        rows = self.kernel.read._repository_query(context, "portfolio.performance_daily", "SELECT total_equity FROM portfolio.performance_daily WHERE portfolio_id=%s AND trade_date<=%s ORDER BY trade_date DESC LIMIT 1", (portfolio_id, asof), self._scope(portfolio_id, date_to=asof), 2)
        return float(rows[0]["total_equity"]) if rows else None

    def append_fill(self, row: dict[str, Any], context: RunContext) -> int:
        return self.kernel.write.bulk_insert(context, self._scope(str(row["portfolio_id"])), "portfolio.trade_fill", [row])

    def append_lot(self, row: dict[str, Any], context: RunContext) -> int:
        return self.kernel.write.bulk_insert(context, self._scope(str(row["portfolio_id"])), "portfolio.position_lot", [row])

    def update_lot(self, lot_id: str, fields: dict[str, Any], portfolio_id: str, context: RunContext) -> int:
        return self.kernel.write.upsert(context, self._scope(portfolio_id), "portfolio.position_lot", [{"lot_id": lot_id, "portfolio_id": portfolio_id, **fields}], key=("lot_id",), update_columns=tuple(fields), skip_noop=True)

    def update_intent_executed(self, intent_id: str, portfolio_id: str, context: RunContext) -> int:
        return self.kernel.write.upsert(context, self._scope(portfolio_id), "portfolio.order_intent", [{"intent_id": intent_id, "portfolio_id": portfolio_id, "status": "EXECUTED"}], key=("intent_id", "portfolio_id"), update_columns=("status",), skip_noop=True)

    def snapshot_prices(self, snapshot_id: str, valuation_date: date, codes: Sequence[str], context: RunContext) -> dict[str, float]:
        if not codes:
            return {}
        rows = self.kernel.read._repository_query(context, "research.stock_snapshot_daily", "SELECT ts_code,raw_close FROM research.stock_snapshot_daily WHERE snapshot_id=%s AND research_date=%s AND ts_code=ANY(%s)", (snapshot_id, valuation_date, list(codes)), WorkScope(partition=f"snapshot:{snapshot_id}", date_from=valuation_date, date_to=valuation_date), 100_000)
        return {row["ts_code"]: float(row["raw_close"]) for row in rows if row["raw_close"] is not None}

    def binding_exists(self, portfolio_id: str, valuation_date: date, snapshot_id: str, context: RunContext) -> bool:
        rows = self.kernel.read._repository_query(context, "portfolio.snapshot_binding", "SELECT 1 AS present FROM portfolio.snapshot_binding WHERE portfolio_id=%s AND valuation_date=%s AND snapshot_id=%s", (portfolio_id, valuation_date, snapshot_id), self._scope(portfolio_id, date_from=valuation_date, date_to=valuation_date), 2)
        return bool(rows)

    def valuation_state(self, portfolio_id: str, valuation_date: date, context: RunContext) -> dict[str, Any]:
        fills = self.kernel.read._repository_query(context, "portfolio.trade_fill", "SELECT MAX(fill_date) AS latest_fill FROM portfolio.trade_fill WHERE portfolio_id=%s", (portfolio_id,), self._scope(portfolio_id), 2)
        valuations = self.kernel.read._repository_query(context, "portfolio.performance_daily", "SELECT MAX(trade_date) AS latest_valuation FROM portfolio.performance_daily WHERE portfolio_id=%s", (portfolio_id,), self._scope(portfolio_id), 2)
        lots = self.kernel.read._repository_query(context, "portfolio.position_lot", "SELECT ts_code,remaining_shares,cost_price,lot_id FROM portfolio.position_lot WHERE portfolio_id=%s AND remaining_shares>0 ORDER BY ts_code,lot_id", (portfolio_id,), self._scope(portfolio_id), 100_000)
        cash_rows = self.kernel.read._repository_query(context, "portfolio.cash_ledger", "SELECT COALESCE(SUM(amount),0) AS cash FROM portfolio.cash_ledger WHERE portfolio_id=%s", (portfolio_id,), self._scope(portfolio_id), 2)
        return {"latest_fill": fills[0]["latest_fill"] if fills else None, "latest_valuation": valuations[0]["latest_valuation"] if valuations else None, "lots": lots, "cash": float(cash_rows[0]["cash"]) if cash_rows else 0.0}

    def existing_valuation(self, portfolio_id: str, valuation_date: date, context: RunContext) -> dict[str, Any] | None:
        rows = self.kernel.read._repository_query(context, "portfolio.performance_daily", "SELECT source_snapshot_id,valuation_input_hash,cash_balance,position_value,total_equity,daily_return,drawdown FROM portfolio.performance_daily WHERE portfolio_id=%s AND trade_date=%s", (portfolio_id, valuation_date), self._scope(portfolio_id, date_from=valuation_date, date_to=valuation_date), 2)
        return rows[0] if rows else None

    def position_count(self, portfolio_id: str, valuation_date: date, context: RunContext) -> int:
        rows = self.kernel.read._repository_query(context, "portfolio.position_daily", "SELECT COUNT(*) AS value FROM portfolio.position_daily WHERE portfolio_id=%s AND trade_date=%s", (portfolio_id, valuation_date), self._scope(portfolio_id, date_from=valuation_date, date_to=valuation_date), 2)
        return int(rows[0]["value"]) if rows else 0

    def previous_equity(self, portfolio_id: str, valuation_date: date, default: float, context: RunContext) -> float:
        rows = self.kernel.read._repository_query(context, "portfolio.performance_daily", "SELECT total_equity FROM portfolio.performance_daily WHERE portfolio_id=%s AND trade_date<%s ORDER BY trade_date DESC LIMIT 1", (portfolio_id, valuation_date), self._scope(portfolio_id, date_to=valuation_date), 2)
        return float(rows[0]["total_equity"]) if rows else float(default)

    def peak_equity(self, portfolio_id: str, valuation_date: date, default: float, context: RunContext) -> float:
        rows = self.kernel.read._repository_query(context, "portfolio.performance_daily", "SELECT COALESCE(MAX(total_equity),%s) AS value FROM portfolio.performance_daily WHERE portfolio_id=%s AND trade_date<%s", (float(default), portfolio_id, valuation_date), self._scope(portfolio_id, date_to=valuation_date), 2)
        return float(rows[0]["value"]) if rows else float(default)

    def append_position_daily(self, rows: Sequence[dict[str, Any]], context: RunContext) -> int:
        return self.kernel.write.bulk_insert(context, self._scope(str(rows[0]["portfolio_id"])) if rows else WorkScope(partition="portfolio:empty"), "portfolio.position_daily", list(rows))

    def append_performance(self, row: dict[str, Any], context: RunContext) -> int:
        return self.kernel.write.bulk_insert(context, self._scope(str(row["portfolio_id"])), "portfolio.performance_daily", [row])
