"""Typed Repository for AI-audit, human-gate, and paper-intent persistence."""
from __future__ import annotations

from datetime import date
from typing import Any, Mapping, Sequence

from data_collect.data_access.context import RunContext, WorkScope
from data_collect.data_access.repository.base import BaseRepository


class AuditRepository(BaseRepository):
    def _scope(self, partition: str, *, date_from: date | None = None, date_to: date | None = None) -> WorkScope:
        return WorkScope(partition=partition, date_from=date_from, date_to=date_to)

    def candidate_hash(self, candidate_id: str, context: RunContext) -> str | None:
        rows = self.kernel.read._repository_query(context, "research.candidate", "SELECT candidate_hash FROM research.candidate WHERE candidate_id=%s", (candidate_id,), self._scope(f"candidate:{candidate_id}"), 2)
        return str(rows[0]["candidate_hash"]).strip() if rows else None

    def existing_ai_audit(self, candidate_id: str, normalized: Mapping[str, Any], context: RunContext) -> dict[str, Any] | None:
        rows = self.kernel.read._repository_query(
            context, "research.ai_audit",
            """SELECT audit_id,output_hash FROM research.ai_audit
                        WHERE candidate_id=%s AND input_hash=%s AND skill_name=%s
                          AND skill_version=%s AND model_version=%s AND prompt_version=%s""",
            (candidate_id, normalized["input_hash"], normalized["skill_name"], normalized["skill_version"], normalized["model_version"], normalized["prompt_version"]),
            self._scope(f"candidate:{candidate_id}"), 2,
        )
        if not rows:
            return None
        row = rows[0]
        return tuple(row[key] for key in ("status", "source_type", "snapshot_id", "ts_code", "direction", "decision_date", "intended_execution_date", "requested_shares", "stop_loss_price_raw", "source_candidate_id", "reason", "strategy_parameters", "eligible", "candidate_hash"))

    def append_ai_audit(self, row: Mapping[str, Any], context: RunContext) -> int:
        return self.kernel.write.bulk_insert(context, self._scope(f"candidate:{row['candidate_id']}"), "research.ai_audit", [dict(row)])

    def decision_context(self, audit_id: str, candidate_id: str, context: RunContext) -> dict[str, Any] | None:
        rows = self.kernel.read._repository_query(
            context, "research.ai_audit",
            """SELECT a.action,a.risk_level,a.candidate_id,a.portfolio_id,a.output_hash,
                              c.eligible,c.direction,c.snapshot_id,c.ts_code,a.evidence_ids
                         FROM research.ai_audit a JOIN research.candidate c ON c.candidate_id=a.candidate_id
                        WHERE a.audit_id=%s""",
            (audit_id,), self._scope(f"candidate:{candidate_id}"), 2,
        )
        row = rows[0] if rows else None
        return row if row and str(row["candidate_id"]) == str(candidate_id) else None

    def evidence_for_ids(self, evidence_ids: Sequence[str], snapshot_id: str, ts_code: str, context: RunContext) -> list[dict[str, Any]]:
        return self.kernel.read._repository_query(
            context, "research.evidence_reference",
            """SELECT e.claim_role,e.source_system,e.authority_level,e.content_hash,
                              e.published_at,e.effective_date,m.research_cutoff_date
                         FROM research.evidence_reference e
                         JOIN research.snapshot_manifest m ON m.snapshot_id=e.snapshot_id
                        WHERE e.evidence_id=ANY(%s::uuid[]) AND e.snapshot_id=%s AND e.ts_code=%s""",
            (list(evidence_ids), snapshot_id, ts_code), self._scope(f"snapshot:{snapshot_id}"), 100_000,
        )

    def existing_decision(self, candidate_id: str, portfolio_id: str | None, context: RunContext) -> dict[str, Any] | None:
        rows = self.kernel.read._repository_query(
            context, "research.human_decision",
            """SELECT decision_id,decision FROM research.human_decision
                        WHERE candidate_id=%s AND portfolio_id IS NOT DISTINCT FROM %s""",
            (candidate_id, portfolio_id), self._scope(f"candidate:{candidate_id}"), 2,
        )
        return rows[0] if rows else None

    def append_decision(self, row: Mapping[str, Any], context: RunContext) -> int:
        return self.kernel.write.bulk_insert(context, self._scope(f"candidate:{row['candidate_id']}"), "research.human_decision", [dict(row)])

    def snapshot(self, snapshot_id: str, context: RunContext) -> dict[str, Any] | None:
        rows = self.kernel.read._repository_query(context, "research.snapshot_manifest", "SELECT research_cutoff_date,status FROM research.snapshot_manifest WHERE snapshot_id=%s", (snapshot_id,), self._scope(f"snapshot:{snapshot_id}"), 2)
        return rows[0] if rows else None

    def binding(self, portfolio_id: str, valuation_date: date, context: RunContext) -> dict[str, Any] | None:
        rows = self.kernel.read._repository_query(context, "portfolio.snapshot_binding", "SELECT snapshot_id,candidate_run_id FROM portfolio.snapshot_binding WHERE portfolio_id=%s AND valuation_date=%s", (portfolio_id, valuation_date), self._scope(f"portfolio:{portfolio_id}", date_from=valuation_date, date_to=valuation_date), 2)
        return rows[0] if rows else None

    def append_binding(self, row: Mapping[str, Any], context: RunContext) -> int:
        return self.kernel.write.upsert(context, self._scope(str(row["portfolio_id"])), "portfolio.snapshot_binding", [dict(row)], key=("portfolio_id", "valuation_date"), update_columns=(), skip_noop=True)

    def daily_signal_row(self, candidate_id: str, audit_id: str, portfolio_id: str, context: RunContext) -> dict[str, Any] | None:
        rows = self.kernel.read._repository_query(
            context, "research.human_decision",
            """SELECT d.decision,d.actor,a.action,a.output_hash,c.snapshot_id,c.candidate_run_id,
                              c.research_date,c.ts_code,c.direction,c.planned_shares,c.raw_reference_price,
                              c.stop_loss_price_raw,c.candidate_hash,c.reason,c.eligible,r.status
                         FROM research.human_decision d JOIN research.ai_audit a ON a.audit_id=d.audit_id
                         JOIN research.candidate c ON c.candidate_id=d.candidate_id
                         JOIN research.candidate_run r ON r.candidate_run_id=c.candidate_run_id
                        WHERE d.candidate_id=%s AND d.audit_id=%s AND d.portfolio_id=%s""",
            (candidate_id, audit_id, portfolio_id), self._scope(f"candidate:{candidate_id}"), 2,
        )
        return rows[0] if rows else None

    def next_trade_date(self, decision_date: date, context: RunContext) -> date | None:
        rows = self.kernel.read._repository_query(context, "std.trading_calendar", "SELECT MIN(trade_date) AS trade_date FROM std.trading_calendar WHERE exchange='SSE' AND is_open IS TRUE AND trade_date>%s", (decision_date,), WorkScope(), 2)
        return rows[0]["trade_date"] if rows else None

    def existing_intent(self, portfolio_id: str, candidate_id: str, context: RunContext) -> dict[str, Any] | None:
        rows = self.kernel.read._repository_query(context, "portfolio.order_intent", "SELECT source_candidate_id,intent_id FROM portfolio.order_intent WHERE portfolio_id=%s AND source_candidate_id=%s", (portfolio_id, candidate_id), self._scope(f"portfolio:{portfolio_id}"), 2)
        return rows[0] if rows else None

    def persist_daily_intent(self, intent_row: Mapping[str, Any], audit_row: Mapping[str, Any], context: RunContext) -> None:
        scope = self._scope(str(intent_row["portfolio_id"]))
        def persist():
            self.kernel.write.bulk_insert(context, scope, "portfolio.order_intent", [dict(intent_row)])
            self.kernel.write.bulk_insert(context, scope, "portfolio.audit_event", [dict(audit_row)])
        self.atomic(context, scope, persist, estimated_rows=2)

    def execution_intent(self, portfolio_id: str, intent_id: str, context: RunContext) -> tuple[Any, ...] | None:
        rows = self.kernel.read._repository_query(
            context, "portfolio.order_intent",
            """SELECT i.status,i.source_type,i.snapshot_id,i.ts_code,i.direction,i.decision_date,
                              i.intended_execution_date,i.requested_shares,i.stop_loss_price_raw,
                              i.source_candidate_id,i.reason,r.strategy_parameters,c.eligible,c.candidate_hash
                         FROM portfolio.order_intent i JOIN research.candidate c ON c.candidate_id=i.source_candidate_id
                         JOIN research.candidate_run r ON r.candidate_run_id=c.candidate_run_id
                        WHERE i.intent_id=%s AND i.portfolio_id=%s""",
            (intent_id, portfolio_id), self._scope(f"portfolio:{portfolio_id}"), 2,
        )
        if not rows:
            return None
        row = rows[0]
        return tuple(row[key] for key in ("status", "source_type", "snapshot_id", "ts_code", "direction", "decision_date", "intended_execution_date", "requested_shares", "stop_loss_price_raw", "source_candidate_id", "reason", "strategy_parameters", "eligible", "candidate_hash"))

    def market_raw_quote(self, ts_code: str, trade_date: date, context: RunContext) -> tuple[Any, ...] | None:
        rows = self.kernel.read._repository_query(context, "tushare_market_raw_version", """SELECT record_id,payload,payload_hash,fetched_at,batch_id FROM tushare_market_raw_version WHERE dataset='daily' AND ts_code=%s AND trade_date=%s ORDER BY fetched_at DESC,record_id DESC LIMIT 1""", (ts_code, trade_date), self._scope(f"market:{ts_code}", date_from=trade_date, date_to=trade_date), 2)
        if not rows:
            return None
        row = rows[0]
        return tuple(row[key] for key in ("record_id", "payload", "payload_hash", "fetched_at", "batch_id"))

    def snapshot_market(self, snapshot_id: str, ts_code: str, context: RunContext) -> tuple[Any, ...] | None:
        rows = self.kernel.read._repository_query(context, "research.stock_snapshot_daily", "SELECT market FROM research.stock_snapshot_daily WHERE snapshot_id=%s AND ts_code=%s", (snapshot_id, ts_code), self._scope(f"snapshot:{snapshot_id}"), 2)
        return (rows[0]["market"],) if rows else None

    def execution_checkpoints(self, checkpoint_key: str, context: RunContext) -> dict[str, tuple[Any, Any]]:
        rows = self.kernel.read._repository_query(context, "ops.tushare_history_checkpoint", "SELECT dataset,status,updated_at FROM ops.tushare_history_checkpoint WHERE dataset IN ('suspend_d_incremental','stk_limit_incremental') AND checkpoint_key=%s", (checkpoint_key,), self._scope(f"checkpoint:{checkpoint_key}"), 10)
        return {str(row["dataset"]): (row["status"], row["updated_at"]) for row in rows}

    def limit_raw(self, ts_code: str, trade_date: date, context: RunContext) -> tuple[Any, ...] | None:
        rows = self.kernel.read._repository_query(context, "tushare_stk_limit_raw", """SELECT record_id,payload,payload_hash,fetched_at,batch_id FROM tushare_stk_limit_raw WHERE ts_code=%s AND trade_date=%s ORDER BY fetched_at DESC,record_id DESC LIMIT 1""", (ts_code, trade_date), self._scope(f"limit:{ts_code}", date_from=trade_date, date_to=trade_date), 2)
        if not rows:
            return None
        row = rows[0]
        return tuple(row[key] for key in ("record_id", "payload", "payload_hash", "fetched_at", "batch_id"))

    def suspend_raw(self, ts_code: str, trade_date: date, context: RunContext) -> tuple[Any, ...] | None:
        rows = self.kernel.read._repository_query(context, "tushare_suspend_d_raw", """SELECT record_id,payload_hash,fetched_at,batch_id FROM tushare_suspend_d_raw WHERE ts_code=%s AND trade_date=%s AND suspend_type='S' ORDER BY fetched_at DESC,record_id DESC LIMIT 1""", (ts_code, trade_date), self._scope(f"suspend:{ts_code}", date_from=trade_date, date_to=trade_date), 2)
        if not rows:
            return None
        row = rows[0]
        return tuple(row[key] for key in ("record_id", "payload_hash", "fetched_at", "batch_id"))

    def update_pending_intent(self, portfolio_id: str, intent_id: str, requested_shares: int, reference_price: float, reason: Mapping[str, Any], context: RunContext) -> int:
        return self.kernel.write.upsert(context, self._scope(f"portfolio:{portfolio_id}"), "portfolio.order_intent", [{"intent_id": intent_id, "portfolio_id": portfolio_id, "requested_shares": requested_shares, "reference_price": reference_price, "reason": reason}], key=("intent_id", "portfolio_id"), update_columns=("requested_shares", "reference_price", "reason"), skip_noop=False)
