"""Typed Repository for frozen daily-candidate inputs and persistence."""
from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any, Mapping, Sequence

from data_collect.data_access.context import RunContext, WorkScope
from data_collect.data_access.repository.base import BaseRepository


class CandidateRepository(BaseRepository):
    def _scope(self, partition: str, *, date_from: date | None = None, date_to: date | None = None) -> WorkScope:
        return WorkScope(partition=partition, date_from=date_from, date_to=date_to)

    def snapshot_context(self, snapshot_id: str, context: RunContext) -> dict[str, Any] | None:
        rows = self.kernel.read._repository_query(
            context, "research.snapshot_manifest",
            """SELECT snapshot_id,status,research_cutoff_date,data_baseline_tag,
                              schema_version,code_version,git_commit,git_dirty,
                              diff_hash,quality_report_hash,input_manifest_hash
                         FROM research.snapshot_manifest
                        WHERE snapshot_id=%s""",
            (snapshot_id,), self._scope(f"snapshot:{snapshot_id}"), 2,
        )
        return rows[0] if rows else None

    def approved_strategy(self, strategy_id: str, strategy_version: str, context: RunContext) -> dict[str, Any] | None:
        rows = self.kernel.read._repository_query(
            context, "backtest.app_strategy_version",
            """SELECT strategy_id, strategy_version, parameters,
                             parameters_hash, approved_by, approved_at,
                             evidence_hash, code_version
                        FROM backtest.app_strategy_version
                       WHERE strategy_id=%s AND strategy_version=%s
                         AND approval_status='APPROVED'""",
            (strategy_id, strategy_version),
            self._scope(f"strategy:{strategy_id}:{strategy_version}"), 2,
        )
        return rows[0] if rows else None

    def observations(self, snapshot_id: str, research_date: date, context: RunContext) -> list[dict[str, Any]]:
        return self.kernel.read._repository_query(
            context, "research.stock_snapshot_daily",
            """SELECT s.ts_code,s.is_listed_asof,s.is_st_asof,s.eligible_by_market,
                              s.is_suspended_asof,s.can_buy,s.can_sell,s.limit_status,s.raw_close,
                              s.adj_close_base,s.return_1d,s.return_20d,s.return_60d,s.ma20,s.ma60,
                              s.ma120,s.volume_ratio_20d,s.pe_ttm,s.pe_pct_5y,s.is_pe_meaningful,
                              s.effective_announce_date,s.roe,s.roa,s.debt_to_assets,s.revenue_yoy,
                              s.tr_yoy,s.q_sales_yoy,s.netprofit_yoy,s.operating_cashflow,
                              s.net_profit,s.quality_status,s.market,s.name,s.raw_volume AS volume,
                              s.raw_amount AS amount,s.raw_high,s.raw_low,s.list_date,
                              s.available_observation_count,s.ma20_observation_count,
                              s.ma60_observation_count,s.ma120_observation_count,
                              s.valuation_observation_count,s.pe_window_start_date_5y,
                              s.has_full_pe_5y_history,s.annual_report_count_5y,
                              s.roe_above_15_years_5y,s.roe_5y_min,s.roe_5y_all_above_15,
                              s.ocf_report_count_3y,s.ocf_positive_years_3y,s.ocf_3y_all_positive,
                              s.quarterly_report_count_4q,s.q_sales_yoy_above_30_quarters_4q,
                              s.q_sales_yoy_4q_all_above_30,s.financial_version_available_date,
                              s.up_limit,s.down_limit,s.raw_open
                         FROM research.stock_snapshot_daily s
                        WHERE s.snapshot_id=%s AND s.research_date=%s
                        ORDER BY s.ts_code""",
            (snapshot_id, research_date),
            self._scope(f"snapshot:{snapshot_id}", date_from=research_date, date_to=research_date),
            100_000,
        )

    def portfolio_inputs(self, portfolio_id: str, snapshot_id: str, context: RunContext, execution_date: date | None = None) -> dict[str, Any]:
        scope = self._scope(f"portfolio:{portfolio_id}")
        account = self.kernel.read._repository_query(
            context, "portfolio.account_manifest",
            """SELECT initial_cash,reserve_cash_pct,min_buy_lot,allow_sell_odd_lot,
                              t_plus_one,price_tick,commission_rate,min_commission,
                              stamp_duty_sell_rate,transfer_fee_rate,slippage_bps,
                              cash_settlement_days,source_snapshot_id,portfolio_version
                         FROM portfolio.account_manifest
                        WHERE portfolio_id=%s AND status='ACTIVE'""",
            (portfolio_id,), scope, 2,
        )
        if not account:
            raise ValueError(f"不存在或未激活的 Portfolio: {portfolio_id}")
        lots = self.kernel.read._repository_query(
            context, "portfolio.position_lot",
            """SELECT lot_id,ts_code,open_date,available_sell_date,open_shares,
                              remaining_shares,cost_price,stop_loss_price_raw
                         FROM portfolio.position_lot
                        WHERE portfolio_id=%s AND remaining_shares>0
                        ORDER BY ts_code,lot_id""",
            (portfolio_id,), scope, 100_000,
        )
        cash_rows = self.kernel.read._repository_query(
            context, "portfolio.cash_ledger",
            """SELECT ledger_id,trade_date,event_type,balance,available_date,
                              reference_id,created_at
                         FROM portfolio.cash_ledger
                        WHERE portfolio_id=%s
                        ORDER BY created_at DESC,ledger_id DESC LIMIT 1""",
            (portfolio_id,), scope, 2,
        )
        available_rows = self.kernel.read._repository_query(
            context, "portfolio.cash_ledger",
            """SELECT COALESCE(SUM(amount),0) AS total_cash,
                              COALESCE(SUM(amount) FILTER (WHERE available_date<=%s),0) AS available_cash
                         FROM portfolio.cash_ledger
                        WHERE portfolio_id=%s""",
            (execution_date or date.max, portfolio_id), scope, 2,
        )
        positions = sorted({str(row["ts_code"]) for row in lots})
        prices: list[dict[str, Any]] = []
        if positions:
            prices = self.kernel.read._repository_query(
                context, "research.stock_snapshot_daily",
                """SELECT ts_code,raw_close,market
                             FROM research.stock_snapshot_daily
                            WHERE snapshot_id=%s
                              AND research_date=(SELECT research_cutoff_date
                                                   FROM research.snapshot_manifest
                                                  WHERE snapshot_id=%s)
                              AND ts_code=ANY(%s)""",
                (snapshot_id, snapshot_id, positions), self._scope(f"snapshot:{snapshot_id}"), 100_000,
            )
        return {
            "account": account[0], "lots": lots, "cash_row": cash_rows[0] if cash_rows else None,
            "available": available_rows[0] if available_rows else {"total_cash": 0, "available_cash": 0},
            "prices": prices,
        }

    def rule_snapshot(self, decision_date: date, context: RunContext) -> dict[str, Any]:
        calendar = self.kernel.read._repository_query(
            context, "std.trading_calendar",
            """SELECT MIN(trade_date) AS execution_date FROM std.trading_calendar
                         WHERE exchange='SSE' AND is_open IS TRUE AND trade_date>%s""",
            (decision_date,), WorkScope(), 2,
        )
        execution_date = calendar[0]["execution_date"] if calendar else None
        if execution_date is None:
            raise RuntimeError(f"缺少 {decision_date} 之后的交易日历，无法绑定执行规则")
        rows = self.kernel.read._repository_query(
            context, "foundation.market_rule_effective",
            """SELECT m.rule_version,m.market_rule_version,e.fee_rule_version,m.effective_from,
                              m.effective_to,m.market,m.min_buy_lot,m.allow_sell_odd_lot,m.t_plus_one,
                              m.price_tick,m.cash_settlement_days,m.limit_pct,e.commission_rate,
                              e.min_commission,t.stamp_duty_sell_rate,t.transfer_fee_rate,
                              e.slippage_bps,e.parameters,m.approval_status,m.approved_by,m.approved_at,
                              e.evidence_hash AS approval_hash
                         FROM foundation.market_rule_effective m
                         JOIN backtest.app_execution_params e
                           ON e.market=m.market AND e.effective_from=m.effective_from
                          AND (e.effective_to IS NULL OR e.effective_to=m.effective_to)
                         JOIN foundation.tax_rule_effective t
                           ON t.market=m.market AND t.tax_rule_version=e.fee_rule_version
                          AND t.effective_from=m.effective_from
                        WHERE m.effective_from<=%s
                          AND (m.effective_to IS NULL OR m.effective_to>=%s)
                          AND m.approval_status='APPROVED'
                          AND e.approval_status='APPROVED'
                          AND t.approval_status='APPROVED'
                        ORDER BY CASE WHEN m.market='ALL' THEN 1 ELSE 0 END,m.market,m.rule_version""",
            (execution_date, execution_date), self._scope(f"rules:{execution_date}", date_from=execution_date, date_to=execution_date), 1000,
        )
        if not rows:
            raise RuntimeError(f"{execution_date} 没有已批准的市场/费用规则")
        return {"decision_date": decision_date.isoformat(), "execution_date": execution_date, "rules": rows}

    def find_run(self, snapshot_id: str, strategy_id: str, strategy_version: str, input_hash: str, context: RunContext) -> dict[str, Any] | None:
        rows = self.kernel.read._repository_query(
            context, "research.candidate_run",
            """SELECT candidate_run_id,status FROM research.candidate_run
                        WHERE snapshot_id=%s AND run_type=%s AND strategy_id=%s
                          AND strategy_version=%s AND input_hash=%s""",
            (snapshot_id, "DAILY_SIGNAL", strategy_id, strategy_version, input_hash),
            self._scope(f"candidate-run:{snapshot_id}"), 2,
        )
        return rows[0] if rows else None

    def candidate_count(self, candidate_run_id: str, context: RunContext) -> int:
        rows = self.kernel.read._repository_query(
            context, "research.candidate", "SELECT COUNT(*) AS value FROM research.candidate WHERE candidate_run_id=%s",
            (candidate_run_id,), self._scope(f"candidate-run:{candidate_run_id}"), 2,
        )
        return int(rows[0]["value"]) if rows else 0

    def persist_run(self, run_row: Mapping[str, Any], candidate_rows: Sequence[Mapping[str, Any]], context: RunContext) -> None:
        scope = self._scope(f"candidate-run:{run_row['candidate_run_id']}")

        def persist():
            self.kernel.write.bulk_insert(context, scope, "research.candidate_run", [dict(run_row)])
            if candidate_rows:
                self.kernel.write.bulk_insert(context, scope, "research.candidate", list(candidate_rows))
            self.kernel.write.upsert(
                context, scope, "research.candidate_run",
                [{"candidate_run_id": run_row["candidate_run_id"], "status": "SUCCEEDED", "completed_at": datetime.now(timezone.utc)}],
                key=("candidate_run_id",), update_columns=("status", "completed_at"), skip_noop=False,
            )

        self.atomic(context, scope, persist, estimated_rows=len(candidate_rows) + 2)

    def evidence(self, snapshot_id: str, ts_code: str, cutoff: date, context: RunContext) -> list[dict[str, Any]]:
        return self.kernel.read._repository_query(
            context, "research.evidence_reference",
            """SELECT evidence_id,source_system,source_type,source_locator,document_index,
                              document_id,source_url,content_hash,published_at,effective_date,
                              claim_role,authority_level,locator,note
                         FROM research.evidence_reference
                        WHERE snapshot_id=%s AND ts_code=%s
                          AND (published_at IS NULL OR published_at::date<=%s)
                          AND (effective_date IS NULL OR effective_date<=%s)
                        ORDER BY claim_role,published_at DESC NULLS LAST,evidence_id""",
            (snapshot_id, ts_code, cutoff, cutoff), self._scope(f"snapshot:{snapshot_id}", date_from=cutoff, date_to=cutoff), 100_000,
        )

    def candidate_packet(self, candidate_id: str, context: RunContext) -> dict[str, Any] | None:
        rows = self.kernel.read._repository_query(
            context, "research.candidate",
            """SELECT c.candidate_id,c.candidate_run_id,c.snapshot_id,c.research_date,c.ts_code,
                              c.direction,c.signal_score,c.indicator_price,c.raw_reference_price,
                              c.stop_loss_price_raw,c.theoretical_shares,c.planned_shares,
                              c.required_capital,c.max_loss_at_stop,c.gap_stress_loss,
                              c.estimated_slippage,c.actual_risk_pct,c.eligible,c.reject_reason,
                              c.reason,c.candidate_hash,r.strategy_id,r.strategy_version,
                              r.strategy_parameters,r.input_hash,r.portfolio_state,
                              r.portfolio_state_hash,r.observation_input_hash,r.rule_snapshot,
                              r.rule_snapshot_hash,r.status AS run_status,m.status AS snapshot_status,
                              m.input_manifest_hash,m.quality_report_hash
                         FROM research.candidate c
                         JOIN research.candidate_run r ON r.candidate_run_id=c.candidate_run_id
                         JOIN research.snapshot_manifest m ON m.snapshot_id=c.snapshot_id
                        WHERE c.candidate_id=%s""",
            (candidate_id,), self._scope(f"candidate:{candidate_id}"), 2,
        )
        return rows[0] if rows else None

    def list_candidates(self, candidate_run_id: str, eligible_only: bool, context: RunContext) -> list[dict[str, Any]]:
        return self.kernel.read._repository_query(
            context, "research.candidate",
            """SELECT candidate_id,ts_code,direction,signal_score,planned_shares,required_capital,
                              max_loss_at_stop,gap_stress_loss,actual_risk_pct,eligible,reject_reason,candidate_hash
                         FROM research.candidate
                        WHERE candidate_run_id=%s AND (%s IS TRUE OR eligible IS TRUE)
                        ORDER BY CASE WHEN direction='SELL' THEN 0 ELSE 1 END,signal_score DESC,ts_code""",
            (candidate_run_id, not eligible_only), self._scope(f"candidate-run:{candidate_run_id}"), 100_000,
        )

    def history(self, ts_code: str, end_date: date, start_date: date, limit: int, context: RunContext) -> list[dict[str, Any]]:
        return self.kernel.read._repository_query(
            context, "mart.v_price_feature",
            """SELECT trade_date,raw_open,raw_high,raw_low,raw_close,
                              asof_close AS adj_close_base,return_1d,return_20d,
                              return_60d,ma20,ma60,ma120,volatility_20d,
                              volume_ratio_20d,amount_ma20,price_quality_status AS quality_status,
                              pe_ttm,pe_pct_5y,pb,pb_pct_5y
                         FROM mart.v_price_feature
                        WHERE ts_code=%s AND trade_date BETWEEN %s AND %s
                        ORDER BY trade_date DESC LIMIT %s""",
            (ts_code, start_date, end_date, limit), self._scope(f"history:{ts_code}", date_from=start_date, date_to=end_date), limit,
        )
