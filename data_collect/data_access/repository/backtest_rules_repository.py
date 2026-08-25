"""Append-only persistence for approved market, tax and execution rules."""
from __future__ import annotations

import json
from typing import Any

from data_collect.data_access.context import RunContext, WorkScope
from data_collect.data_access.repository.base import BaseRepository


class BacktestRulesRepository(BaseRepository):
    def insert_approved(
        self,
        rows: list[dict[str, Any]],
        approval_hash: str,
        metadata: dict[str, Any],
        content_fields: tuple[str, ...],
        fingerprint,
        context: RunContext,
    ) -> int:
        """Publish one append-only rule row across the split contracts."""
        del content_fields, fingerprint  # retained in the caller contract for compatibility
        inserted = 0
        approved_by = str(metadata["approved_by"]).strip()
        approved_at = str(metadata["approved_at"])
        for row in rows:
            scope = WorkScope(
                date_from=row["effective_from"],
                date_to=row.get("effective_to") or row["effective_from"],
                partition=f"rule:{row['market']}:{row['rule_version']}",
            )
            overlap = self.kernel.read._repository_query(
                context,
                "foundation.market_rule_effective",
                """SELECT rule_version FROM foundation.market_rule_effective
                    WHERE market=%s AND market_rule_version=%s
                      AND NOT (rule_version=%s AND effective_from=%s)
                      AND effective_from <= COALESCE(%s, DATE '9999-12-31')
                      AND COALESCE(effective_to, DATE '9999-12-31') >= %s LIMIT 1""",
                (
                    row["market"], row["market_rule_version"], row["rule_version"],
                    row["effective_from"], row["effective_to"], row["effective_from"],
                ),
                scope,
                2,
            )
            if overlap:
                raise ValueError(
                    f"数据库中已有重叠市场规则区间，禁止追加歧义规则: "
                    f"market={row['market']}, rule={row['rule_version']}"
                )

            parameters = dict(row["parameters"])
            common = {
                "approval_status": "APPROVED",
                "approved_by": approved_by,
                "approved_at": approved_at,
                "evidence_hash": approval_hash,
            }
            market_payload = {
                "rule_version": row["rule_version"],
                "market_rule_version": row["market_rule_version"],
                "effective_from": row["effective_from"],
                "effective_to": row["effective_to"],
                "market": row["market"],
                "min_buy_lot": row["min_buy_lot"],
                "allow_sell_odd_lot": row["allow_sell_odd_lot"],
                "t_plus_one": row["t_plus_one"],
                "price_tick": row["price_tick"],
                "cash_settlement_days": row["cash_settlement_days"],
                "limit_pct": row["limit_pct"],
                **common,
            }
            tax_payload = {
                "tax_rule_version": row["fee_rule_version"],
                "effective_from": row["effective_from"],
                "effective_to": row["effective_to"],
                "market": row["market"],
                "stamp_duty_sell_rate": row["stamp_duty_sell_rate"],
                "transfer_fee_rate": row["transfer_fee_rate"],
                **common,
            }
            execution_payload = {
                "execution_params_version": f"{row['fee_rule_version']}:{row['effective_from']}",
                "fee_rule_version": row["fee_rule_version"],
                "market": row["market"],
                "effective_from": row["effective_from"],
                "effective_to": row["effective_to"],
                "commission_rate": row["commission_rate"],
                "min_commission": row["min_commission"],
                "slippage_bps": row["slippage_bps"],
                "gap_stress_pct": float(parameters.get("gap_stress_pct", 0.0)),
                "max_volume_participation_pct": float(parameters.get("max_volume_participation_pct", 1.0)),
                "liquidity_policy": str(parameters.get("liquidity_policy", "reject_full_order")),
                "execution_model": str(parameters.get("execution_model", "bar_open_close")),
                "parameters": json.dumps(parameters, ensure_ascii=False, default=str),
                **common,
            }

            def publish_split_rules() -> int:
                count = self.kernel.publish.snapshot(
                    context, scope, "foundation.market_rule_effective", [market_payload]
                )
                count += self.kernel.publish.snapshot(
                    context, scope, "foundation.tax_rule_effective", [tax_payload]
                )
                _, execution_count = self.kernel.write.bulk_insert_ignore(
                    context, scope, "backtest.app_execution_params", [execution_payload]
                )
                return count + execution_count

            inserted += self.atomic(context, scope, publish_split_rules, estimated_rows=3)
        return inserted
