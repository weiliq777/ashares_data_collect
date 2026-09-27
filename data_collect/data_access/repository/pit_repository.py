"""PitRepository：时点层领域仓储（股票池 / 可交易性）。

迁移自 tushare_phase2_standard.py 的 rebuild_universe / rebuild_tradeability。
SET-based SQL（多表 JOIN + INSERT...SELECT...ON CONFLICT）的唯一合法归属就在这里。
"""
from __future__ import annotations

from datetime import timedelta

from data_collect.data_access.context import Operation, RunContext, SqlPlan, WorkScope
from data_collect.data_access.repository.base import BaseRepository

_UNIVERSE_REBUILD_SQL = """
DELETE FROM pit.universe_daily WHERE trade_date BETWEEN %s AND %s;

INSERT INTO pit.universe_daily
    (trade_date, ts_code, is_listed_asof, is_st_asof, is_suspended_asof,
     has_price_history, days_since_listing, eligible_by_market, exclusion_reason,
     quality_status, source_asof_date)
SELECT s.trade_date, s.ts_code, s.is_listed, s.is_st, s.is_suspended,
       (d.stock_code IS NOT NULL),
       CASE WHEN b.list_date IS NULL THEN NULL ELSE s.trade_date - b.list_date END,
       (b.market IN ('主板','创业板','科创板','北交所')),
       NULLIF(concat_ws(',',
           CASE WHEN b.market NOT IN ('主板','创业板','科创板','北交所') OR b.market IS NULL THEN 'MARKET' END,
           CASE WHEN s.is_st THEN 'ST' END,
           CASE WHEN NOT s.is_listed THEN 'NOT_LISTED' END
       ), ''),
       s.quality_status, s.trade_date
FROM std.security_status_daily s
LEFT JOIN std.stock_basic_info b ON b.stock_code = s.ts_code
LEFT JOIN std.daily_kline d ON d.stock_code = s.ts_code AND d.trade_date = s.trade_date
WHERE s.trade_date BETWEEN %s AND %s
ON CONFLICT (trade_date, ts_code) DO UPDATE SET
  is_listed_asof=EXCLUDED.is_listed_asof, is_st_asof=EXCLUDED.is_st_asof,
  is_suspended_asof=EXCLUDED.is_suspended_asof, has_price_history=EXCLUDED.has_price_history,
  days_since_listing=EXCLUDED.days_since_listing, eligible_by_market=EXCLUDED.eligible_by_market,
  exclusion_reason=EXCLUDED.exclusion_reason, quality_status=EXCLUDED.quality_status,
  source_asof_date=EXCLUDED.source_asof_date, calculated_at=CURRENT_TIMESTAMP
"""

_TRADEABILITY_REBUILD_SQL = """
DELETE FROM pit.security_tradeability_daily WHERE trade_date BETWEEN %s AND %s;

INSERT INTO pit.security_tradeability_daily
    (trade_date, ts_code, is_tradeable, can_buy, can_sell, is_suspended,
     limit_status, blocked_reason, quality_status, source_asof_date)
SELECT trade_date, ts_code, is_tradeable,
       (is_tradeable AND limit_status <> 'UP_LIMIT'),
       (is_tradeable AND limit_status <> 'DOWN_LIMIT'),
       is_suspended, limit_status,
       NULLIF(concat_ws(',',
         CASE WHEN NOT is_tradeable THEN 'NOT_TRADEABLE' END,
         CASE WHEN is_tradeable AND limit_status = 'UP_LIMIT'
              THEN 'BUY_BLOCKED_UP_LIMIT' END,
         CASE WHEN is_tradeable AND limit_status = 'DOWN_LIMIT'
              THEN 'SELL_BLOCKED_DOWN_LIMIT' END
       ), ''),
       quality_status, trade_date
FROM std.security_status_daily
WHERE trade_date BETWEEN %s AND %s
ON CONFLICT (trade_date, ts_code) DO UPDATE SET
  is_tradeable=EXCLUDED.is_tradeable, can_buy=EXCLUDED.can_buy,
  can_sell=EXCLUDED.can_sell, is_suspended=EXCLUDED.is_suspended,
  limit_status=EXCLUDED.limit_status, blocked_reason=EXCLUDED.blocked_reason,
  quality_status=EXCLUDED.quality_status, source_asof_date=EXCLUDED.source_asof_date,
  calculated_at=CURRENT_TIMESTAMP
"""

# A single trading day can still contain several thousand securities.  Keep
# the delete and INSERT...SELECT scoped to a code batch as well as to the day;
# this avoids turning a bounded date window into a statement-timeout hotspot on
# a cold PostgreSQL/WSL cache.
_UNIVERSE_REBUILD_CODE_SQL = _UNIVERSE_REBUILD_SQL.replace(
    "DELETE FROM pit.universe_daily WHERE trade_date BETWEEN %s AND %s;",
    "DELETE FROM pit.universe_daily WHERE trade_date BETWEEN %s AND %s "
    "AND ts_code = ANY(%s);",
).replace(
    "WHERE s.trade_date BETWEEN %s AND %s\nON CONFLICT",
    "WHERE s.trade_date BETWEEN %s AND %s AND s.ts_code = ANY(%s)\nON CONFLICT",
)
_TRADEABILITY_REBUILD_CODE_SQL = _TRADEABILITY_REBUILD_SQL.replace(
    "DELETE FROM pit.security_tradeability_daily WHERE trade_date BETWEEN %s AND %s;",
    "DELETE FROM pit.security_tradeability_daily WHERE trade_date BETWEEN %s AND %s "
    "AND ts_code = ANY(%s);",
).replace(
    "WHERE trade_date BETWEEN %s AND %s\nON CONFLICT",
    "WHERE trade_date BETWEEN %s AND %s AND ts_code = ANY(%s)\nON CONFLICT",
)


class PitRepository(BaseRepository):
    CODE_BATCH_SIZE = 500

    @staticmethod
    def _daily_scopes(scope: WorkScope):
        """Yield one-date scopes so JOIN-heavy PIT rebuilds stay short."""
        current = scope.date_from
        while current <= scope.date_to:
            yield WorkScope(
                date_from=current,
                date_to=current,
                codes=scope.codes,
                dimensions=scope.dimensions,
                partition=scope.partition,
            )
            current += timedelta(days=1)

    def rebuild_universe(self, scope: WorkScope, context: RunContext) -> int:
        """重建股票池（物理标的池）。scoped merge（DELETE 范围 + INSERT...ON CONFLICT）。"""
        self._require_date_range(scope, "rebuild_universe")
        total = 0
        for day_scope in self._daily_scopes(scope):
            for code_batch in self._code_batches(day_scope, "pit.universe_daily", context):
                batch_scope = self._batch_scope(day_scope, code_batch)
                plan = SqlPlan(
                    operation=Operation.REBUILD,
                    target="pit.universe_daily",
                    scope=batch_scope,
                    expected_mode="SET_BASED",
                    estimated_rows=self.CODE_BATCH_SIZE,
                    allow_insert_select=True,
                    allow_delete=True,
                )
                if code_batch is None:
                    params = (
                        day_scope.date_from, day_scope.date_to,
                        day_scope.date_from, day_scope.date_to,
                    )
                    sql = _UNIVERSE_REBUILD_SQL
                else:
                    params = (
                        day_scope.date_from, day_scope.date_to, list(code_batch),
                        day_scope.date_from, day_scope.date_to, list(code_batch),
                    )
                    sql = _UNIVERSE_REBUILD_CODE_SQL
                total += self.kernel.rebuild.run_scoped_merge(
                    sql=sql, plan=plan, context=context, params=params
                )
        return total

    def rebuild_tradeability(self, scope: WorkScope, context: RunContext) -> int:
        """重建可交易性（can_buy/can_sell/涨跌停状态）。scoped merge。"""
        self._require_date_range(scope, "rebuild_tradeability")
        total = 0
        for day_scope in self._daily_scopes(scope):
            for code_batch in self._code_batches(
                day_scope, "pit.security_tradeability_daily", context
            ):
                batch_scope = self._batch_scope(day_scope, code_batch)
                plan = SqlPlan(
                    operation=Operation.REBUILD,
                    target="pit.security_tradeability_daily",
                    scope=batch_scope,
                    expected_mode="SET_BASED",
                    estimated_rows=self.CODE_BATCH_SIZE,
                    allow_insert_select=True,
                    allow_delete=True,
                )
                if code_batch is None:
                    params = (
                        day_scope.date_from, day_scope.date_to,
                        day_scope.date_from, day_scope.date_to,
                    )
                    sql = _TRADEABILITY_REBUILD_SQL
                else:
                    params = (
                        day_scope.date_from, day_scope.date_to, list(code_batch),
                        day_scope.date_from, day_scope.date_to, list(code_batch),
                    )
                    sql = _TRADEABILITY_REBUILD_CODE_SQL
                total += self.kernel.rebuild.run_scoped_merge(
                    sql=sql, plan=plan, context=context, params=params
                )
        return total

    def _code_batches(
        self,
        day_scope: WorkScope,
        target: str,
        context: RunContext,
    ):
        """Return bounded code batches, retaining stale target rows for deletion."""
        if day_scope.codes is not None:
            if not day_scope.codes:
                return
            codes = set(day_scope.codes)
        else:
            read_scope = WorkScope(
                date_from=day_scope.date_from,
                date_to=day_scope.date_to,
                partition=f"pit-code-set:{target}",
            )
            source_rows = self.kernel.read.query(
                context=context,
                source="std.security_status_daily",
                columns=["ts_code"],
                where={"trade_date__eq": day_scope.date_from},
                limit=10_000,
                scope=read_scope,
            )
            target_rows = self.kernel.read.query(
                context=context,
                source=target,
                columns=["ts_code"],
                where={"trade_date__eq": day_scope.date_from},
                limit=10_000,
                scope=read_scope,
            )
            codes = {str(row[0]) for row in source_rows}
            codes.update(str(row[0]) for row in target_rows)
        ordered = sorted(codes)
        for offset in range(0, len(ordered), self.CODE_BATCH_SIZE):
            yield tuple(ordered[offset : offset + self.CODE_BATCH_SIZE])
        if not ordered:
            # An empty source day must still clear stale target rows.
            yield None

    @staticmethod
    def _batch_scope(day_scope: WorkScope, code_batch: tuple[str, ...] | None) -> WorkScope:
        return WorkScope(
            date_from=day_scope.date_from,
            date_to=day_scope.date_to,
            codes=code_batch,
            dimensions=day_scope.dimensions,
            partition=day_scope.partition,
        )

    @staticmethod
    def _require_date_range(scope: WorkScope, fn: str) -> None:
        if not isinstance(scope, WorkScope):
            raise TypeError(f"{fn} 必须携带 WorkScope")
        if scope.date_from is None or scope.date_to is None:
            raise ValueError(f"{fn} 需要 scope.date_from/date_to")

    def rebuild_financial_asof(self, batch_days: int = 30, context: RunContext | None = None) -> int:
        """重建财务 PIT（Repository → rebuild.run_table_replace）。

        SET-based ``INSERT...SELECT`` 不能在 SQL 执行中途安全 commit；由
        statement_timeout 熔断，staging 填充成功后再做短事务原子 swap。
        ``batch_days`` 仅保留调用兼容参数，事务切分不再由 Repository 自己实现。
        """
        if batch_days <= 0:
            raise ValueError("batch_days 必须为正整数")
        if not isinstance(context, RunContext):
            raise TypeError("rebuild_financial_asof 必须携带 RunContext")
        from datetime import date as _date

        bounds = self.kernel.read.scalar(
            context=context,
            source="std.daily_kline",
            aggregate="MIN(trade_date), MAX(trade_date)",
            where={"trade_date__gte": _date(1990, 1, 1)},
            scope=WorkScope(date_from=_date(1990, 1, 1)),
        )
        if not bounds or not bounds[0] or not bounds[1]:
            raise RuntimeError("daily_kline 没有可用于构建 financial_asof 的行情日期")
        scope = WorkScope(date_from=bounds[0], date_to=bounds[1])
        plan = SqlPlan(
            operation=Operation.REBUILD,
            target="pit.financial_asof",
            scope=scope,
            expected_mode="SET_BASED",
            estimated_rows=0,
            allow_insert_select=True,
        )
        return self.kernel.rebuild.run_table_replace(
            fill_sql=_FINANCIAL_ASOF_INSERT_SQL,
            plan=plan,
            context=context,
            params=(bounds[0], bounds[1]),
        )


# 迁移自 tushare_phase2_standard.rebuild_financial_asof 的 insert_sql（{staging} 占位）
_FINANCIAL_ASOF_INSERT_SQL = """
INSERT INTO pit."{staging}"
    (decision_date, ts_code, report_date, effective_announce_date,
     version_available_date, ann_date,
     f_ann_date, report_type, comp_type, record_version, source_record_key,
     roe, roa, debt_to_assets, revenue_yoy, netprofit_yoy, or_yoy, tr_yoy,
     q_sales_yoy, payload_hash, vintage_quality, source_observed_at,
     quality_status)
SELECT f.trade_date, f.ts_code, f.report_date, f.effective_announce_date,
    f.version_available_date, f.ann_date, f.f_ann_date, f.report_type,
    f.comp_type, f.record_version,
    f.source_record_key, f.roe, f.roa, f.debt_to_assets, f.or_yoy,
    f.netprofit_yoy, f.or_yoy, f.tr_yoy, f.q_sales_yoy, f.payload_hash,
    f.vintage_quality, f.source_observed_at,
    f.quality_status
FROM (
    SELECT d.trade_date, i.ts_code, i.report_date, i.effective_announce_date,
           i.version_available_date,
           i.ann_date, i.f_ann_date, i.report_type, i.comp_type, i.record_version,
           i.source_record_key, i.roe, i.roa, i.debt_to_assets, i.or_yoy,
           i.tr_yoy, i.q_sales_yoy, i.netprofit_yoy, i.payload_hash,
           i.vintage_quality, i.source_observed_at,
           i.quality_status, i.fetched_at
    FROM (SELECT DISTINCT stock_code, trade_date FROM std.daily_kline) d
    JOIN LATERAL (
        SELECT i.*
        FROM std.fina_indicator_standard i
        WHERE i.ts_code = d.stock_code
          AND i.effective_announce_date IS NOT NULL
          AND i.effective_announce_date <= d.trade_date
          AND i.version_available_date IS NOT NULL
          AND i.version_available_date <= d.trade_date
        ORDER BY i.report_date DESC, i.version_available_date DESC,
                 i.effective_announce_date DESC,
                 i.record_version DESC, i.fetched_at DESC
        LIMIT 1
    ) i ON TRUE
) f
WHERE f.trade_date BETWEEN %s AND %s
"""
