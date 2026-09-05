"""Typed persistence for formal Backtest Runs."""
from __future__ import annotations

import json
from typing import Any

from data_collect.data_access.context import RunContext, WorkScope
from data_collect.data_access.repository.base import BaseRepository


class BacktestRunRepository(BaseRepository):
    def _scope(self, key: str, dataset_id: str | None = None) -> WorkScope:
        dimensions = {"run_id": key}
        if dataset_id:
            dimensions["dataset_id"] = dataset_id
        return WorkScope(partition=key, dimensions=dimensions)

    def strategy(self, strategy_id: str, strategy_version: str, context: RunContext):
        rows = self.kernel.read._repository_query(
            context, "backtest.app_strategy_version",
            """SELECT strategy_name, strategy_version, parameters_hash, code_version
                 FROM backtest.app_strategy_version
                WHERE strategy_id=%s AND strategy_version=%s
                  AND approval_status='APPROVED'""",
            (strategy_id, strategy_version), self._scope(strategy_id), 2,
        )
        return rows[0] if rows else None

    def strategy_record(self, strategy_id: str, context: RunContext):
        """Return immutable registration metadata, including approval state."""
        rows = self.kernel.read._repository_query(
            context, "backtest.app_strategy_version",
            """SELECT strategy_id, strategy_name, strategy_version,
                             parameters_hash, approval_status, approved_by,
                             approved_at, evidence_hash, code_version
                        FROM backtest.app_strategy_version
                       WHERE strategy_id=%s
                       ORDER BY strategy_version DESC
                       LIMIT 2""",
            (strategy_id,), self._scope(strategy_id), 2,
        )
        return rows[0] if rows else None

    def register_strategy(self, row: dict[str, Any], context: RunContext) -> int:
        normalized = {
            key: row[key]
            for key in (
                "strategy_id", "strategy_version", "strategy_name", "parameters",
                "parameters_hash", "approval_status", "approved_by", "approved_at",
                "evidence_hash", "code_version", "effective_from", "effective_to",
            )
            if key in row
        }
        normalized.setdefault("approval_status", "DRAFT")
        return self.kernel.write.bulk_insert(
            context, self._scope(str(row["strategy_id"])), "backtest.app_strategy_version", [normalized]
        )

    def dataset_rules(self, dataset_id: str, context: RunContext) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        datasets = self.kernel.read._repository_query(
            context, "backtest.dataset_manifest",
            """SELECT status, input_manifest_hash, market_rule_version,
                      fee_rule_version, benchmark_code, warmup_start,
                      evaluation_start, evaluation_end
                 FROM backtest.dataset_manifest WHERE dataset_id=%s""",
            (dataset_id,), self._scope(dataset_id), 2,
        )
        rules = self.kernel.read._repository_query(
            context, "backtest.dataset_rule_snapshot",
            """SELECT rule_version, market_rule_version, fee_rule_version, effective_from, effective_to,
                      market, min_buy_lot, allow_sell_odd_lot, t_plus_one, price_tick,
                      cash_settlement_days, commission_rate, min_commission, stamp_duty_sell_rate,
                      transfer_fee_rate, slippage_bps, parameters, rule_hash
                 FROM backtest.dataset_rule_snapshot WHERE dataset_id=%s
                 ORDER BY effective_from, market, rule_version""",
            (dataset_id,), self._scope(dataset_id), 100_000,
        )
        if not datasets:
            raise RuntimeError("正式回测只能使用存在的 Dataset")
        return datasets[0], rules

    def create_run(self, row: dict[str, Any], context: RunContext) -> int:
        return self.kernel.write.bulk_insert(
            context, self._scope(str(row["run_id"]), str(row["dataset_id"])), "backtest.run_manifest", [row]
        )

    def persist_result(self, run_id: str, result, context: RunContext) -> None:
        scope = self._scope(run_id)

        def rows_for(items, fields):
            return [{field: item.get(field) for field in fields} for item in items]

        signals = [{
            "signal_id": row["signal_id"], "run_id": run_id, "research_snapshot_id": row.get("research_snapshot_id"),
            "ts_code": row["ts_code"], "signal_date": row["signal_date"], "decision_time": row.get("decision_time"),
            "intended_execution_date": row["intended_execution_date"], "direction": row["direction"],
            "indicator_price": row.get("indicator_price"), "raw_reference_price": row.get("raw_reference_price"),
            "stop_loss_price_raw": row.get("stop_loss_price_raw"), "signal_score": row.get("signal_score", 0.0),
            "reason": json.dumps(row.get("reason", {}), ensure_ascii=False, default=str), "status": row.get("status", "CREATED"),
        } for row in result.signals]
        orders = [{
            "order_id": row["order_id"], "run_id": run_id, "signal_id": row.get("signal_id"), "ts_code": row["ts_code"],
            "decision_date": row["decision_date"], "order_date": row["order_date"], "expire_date": row["expire_date"],
            "direction": row["direction"], "planned_shares": row.get("planned_shares", row["order_shares"]),
            "order_shares": row["order_shares"], "requested_shares": row.get("requested_shares", row.get("planned_shares", row["order_shares"])),
            "actual_shares": row.get("actual_shares", 0), "volume_capacity_shares": row.get("volume_capacity_shares"),
            "liquidity_status": row.get("liquidity_status", "NOT_CHECKED"), "liquidity_reject_reason": row.get("liquidity_reject_reason"),
            "liquidity_reference_date": row.get("liquidity_reference_date"), "risk_policy_version": row.get("risk_policy_version"),
            "theoretical_shares": row.get("theoretical_shares"), "required_capital": row.get("required_capital"),
            "max_loss_at_stop": row.get("max_loss_at_stop"), "gap_stress_loss": row.get("gap_stress_loss"),
            "buy_fees": row.get("buy_fees"), "estimated_sell_fees": row.get("estimated_sell_fees"),
            "estimated_slippage": row.get("estimated_slippage"), "actual_risk_pct": row.get("actual_risk_pct"),
            "reference_price": row.get("reference_price"), "stop_loss_price_raw": row["stop_loss_price_raw"],
            "status": row["status"], "reject_reason": row.get("reject_reason"),
        } for row in result.orders]
        fills = [{
            "fill_id": row["fill_id"], "run_id": run_id, "order_id": row["order_id"], "ts_code": row["ts_code"],
            "fill_date": row["fill_date"], "actual_fill_time": row.get("actual_fill_time"), "direction": row["direction"],
            "fill_price": row["fill_price"], "fill_shares": row["fill_shares"], "commission": row["commission"],
            "stamp_duty": row["stamp_duty"], "transfer_fee": row["transfer_fee"], "slippage_cost": row.get("slippage_cost", 0),
        } for row in result.fills]
        lots = [{"lot_id": row["lot_id"], "run_id": run_id, "ts_code": row["ts_code"], "open_fill_id": row["open_fill_id"],
                 "open_date": row["open_date"], "available_sell_date": row["available_sell_date"], "open_shares": row["open_shares"],
                 "remaining_shares": row["remaining_shares"], "cost_price": row["cost_price"], "stop_loss_price_raw": row.get("stop_loss_price_raw"),
                 "close_date": row.get("close_date"), "close_fill_id": row.get("close_fill_id"), "opening_event_type": row.get("opening_event_type", "FILL"),
                 "opening_event_key": row.get("opening_event_key"), "accrued_cash_dividend_per_share": row.get("accrued_cash_dividend_per_share", 0.0)} for row in result.position_lots]
        positions = [{"run_id": run_id, "trade_date": row["trade_date"], "ts_code": row["ts_code"], "hold_shares": row["hold_shares"],
                      "average_cost": row.get("average_cost"), "raw_close": row.get("raw_close"), "market_value": row.get("market_value"),
                      "unrealized_pnl": row.get("unrealized_pnl"), "valuation_status": row.get("valuation_status", "VALID")} for row in result.position_daily]
        cash = [{"run_id": run_id, "trade_date": row["trade_date"], "event_type": row["event_type"], "amount": row["amount"],
                 "balance": row["balance"], "available_date": row["available_date"], "reference_id": row.get("reference_id"), "description": row.get("description")} for row in result.cash_ledger]
        performance = [{"run_id": run_id, "trade_date": row["trade_date"], "cash_balance": row["cash_balance"], "position_value": row["position_value"],
                        "cash_receivable": row.get("cash_receivable", 0), "stock_receivable_value": row.get("stock_receivable_value", 0),
                        "receivable_total": row.get("receivable_total", 0), "total_equity": row["total_equity"], "daily_return": row.get("daily_return"),
                        "cumulative_return": row.get("cumulative_return"), "drawdown": row.get("drawdown"), "turnover": row.get("turnover", 0),
                        "valuation_status": row.get("valuation_status", "VALID")} for row in result.performance_daily]
        metrics = [{"run_id": run_id, "metric_name": name, "metric_value": float(value), "metric_text": None, "parameters": "{}"} for name, value in result.metrics.items()]
        for audit_name, payload in getattr(result, "audit_metadata", {}).items():
            if audit_name in {"monte_carlo", "end_of_data_liquidation"}:
                metrics.append({"run_id": run_id, "metric_name": f"{audit_name}_metadata", "metric_value": None,
                                "metric_text": str(payload.get("method", "")), "parameters": json.dumps(payload, ensure_ascii=False, default=str)})
        for target, values in (("backtest.signal", signals), ("backtest.order", orders), ("backtest.fill", fills),
                               ("backtest.position_lot", lots), ("backtest.position_daily", positions),
                               ("backtest.cash_ledger", cash), ("backtest.performance_daily", performance), ("backtest.metric", metrics)):
            if values:
                self.kernel.write.bulk_insert(context, scope, target, values)
        self.kernel.write.upsert(context, scope, "backtest.run_manifest", [{"run_id": run_id, "status": "SUCCEEDED"}],
                                 key=("run_id",), update_columns=("status",), skip_noop=True)

    def mark_failed(self, run_id: str, reason: str, context: RunContext) -> None:
        self.kernel.write.upsert(context, self._scope(run_id), "backtest.run_manifest",
                                 [{"run_id": run_id, "status": "FAILED", "failure_reason": reason[:4000]}],
                                 key=("run_id",), update_columns=("status", "failure_reason"), skip_noop=True)
