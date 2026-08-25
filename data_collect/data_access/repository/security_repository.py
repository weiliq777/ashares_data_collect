"""SecurityRepository：证券状态（上市/退市/ST/停牌）领域仓储。

这是迁移样板：把 tushare_phase2_standard.py 里的 SET-based 重建 SQL 迁到这里
（SET-based SQL 的唯一合法归属），通过 Kernel 的受控 run_scoped_merge 执行，
强制携带 SqlPlan。业务层只调 Repository 方法，看不见 SQL。
"""
from __future__ import annotations

from data_collect.data_access.context import Operation, RunContext, SqlPlan, WorkScope
from data_collect.data_access.repository.base import BaseRepository

# 迁移自 tushare_phase2_standard.rebuild_status_daily 的完整 SET-based SQL。
# 语义：先 DELETE 该日期范围内的旧状态，再 INSERT...SELECT...ON CONFLICT 重建。
# （这是"scoped merge"：替换 trade_date 范围内的 security_status_daily。）
_STATUS_DAILY_REBUILD_SQL = """
DELETE FROM std.security_status_daily WHERE trade_date BETWEEN %s AND %s;

INSERT INTO std.security_status_daily
    (trade_date, ts_code, is_listed, is_st, is_suspended, is_tradeable,
     limit_status, up_limit, down_limit, close_price, status_reason,
     source, quality_status)
WITH dates AS (
    SELECT cal_date AS trade_date
    FROM tushare_trade_cal_raw
    WHERE exchange = 'SSE' AND (payload->>'is_open')::int = 1
      AND cal_date BETWEEN %s AND %s
), universe AS (
    SELECT d.trade_date, b.stock_code, b.market, b.list_date, b.delist_date
    FROM dates d
    JOIN std.stock_basic_info b
      ON COALESCE(b.list_date, DATE '1900-01-01') <= d.trade_date
     AND (b.delist_date IS NULL OR b.delist_date >= d.trade_date)
), limits AS (
    SELECT DISTINCT ON (ts_code, trade_date)
           ts_code, trade_date,
           (payload->>'up_limit')::double precision AS up_limit,
           (payload->>'down_limit')::double precision AS down_limit,
           fetched_at, record_id
    FROM tushare_stk_limit_raw
    WHERE trade_date BETWEEN %s AND %s
    ORDER BY ts_code, trade_date, fetched_at DESC, record_id DESC
), flags AS (
    SELECT u.*,
           EXISTS (
               SELECT 1 FROM tushare_stock_st_raw st
               WHERE st.ts_code = u.stock_code AND st.trade_date = u.trade_date
           ) AS is_st,
           EXISTS (
               SELECT 1 FROM tushare_suspend_d_raw sd
               WHERE sd.ts_code = u.stock_code AND sd.trade_date = u.trade_date
                 AND sd.suspend_type = 'S'
           ) AS is_suspended,
           d.close AS close_price,
           l.up_limit,
           l.down_limit
    FROM universe u
    LEFT JOIN std.daily_kline d
      ON d.stock_code = u.stock_code AND d.trade_date = u.trade_date
    LEFT JOIN limits l
      ON l.ts_code = u.stock_code AND l.trade_date = u.trade_date
)
SELECT trade_date, stock_code, TRUE, is_st, is_suspended,
       (close_price IS NOT NULL AND close_price > 0 AND NOT is_suspended) AS is_tradeable,
       CASE
         WHEN close_price IS NULL OR close_price <= 0 THEN 'NO_PRICE'
         WHEN up_limit IS NOT NULL AND close_price >= up_limit - 0.0001 THEN 'UP_LIMIT'
         WHEN down_limit IS NOT NULL AND close_price <= down_limit + 0.0001 THEN 'DOWN_LIMIT'
         ELSE 'NONE'
       END,
       up_limit, down_limit, close_price,
       NULLIF(concat_ws(',',
           CASE WHEN is_st THEN 'ST' END,
           CASE WHEN is_suspended THEN 'SUSPENDED' END,
           CASE WHEN close_price IS NULL OR close_price <= 0 THEN 'NO_PRICE' END,
           CASE WHEN up_limit IS NOT NULL AND close_price >= up_limit - 0.0001 THEN 'UP_LIMIT' END,
           CASE WHEN down_limit IS NOT NULL AND close_price <= down_limit + 0.0001 THEN 'DOWN_LIMIT' END
       ), '') AS status_reason,
       'tushare',
       CASE WHEN close_price IS NULL THEN 'MISSING_SOURCE'
            WHEN close_price <= 0 THEN 'WARNING'
            WHEN is_suspended OR is_st THEN 'WARNING' ELSE 'VALID' END
FROM flags
ON CONFLICT (trade_date, ts_code) DO UPDATE SET
   is_listed=EXCLUDED.is_listed, is_st=EXCLUDED.is_st,
   is_suspended=EXCLUDED.is_suspended, is_tradeable=EXCLUDED.is_tradeable,
   limit_status=EXCLUDED.limit_status, up_limit=EXCLUDED.up_limit,
   down_limit=EXCLUDED.down_limit, close_price=EXCLUDED.close_price,
   status_reason=EXCLUDED.status_reason, quality_status=EXCLUDED.quality_status,
   fetched_at=CURRENT_TIMESTAMP
"""


class SecurityRepository(BaseRepository):
    def stock_codes(self, context: RunContext) -> list[str]:
        rows = self.kernel.read.query(
            context=context,
            source="std.stock_basic_info",
            columns=["stock_code"],
            scope=WorkScope(partition="stock-codes"),
        )
        return [str(row[0]) for row in rows]

    def sync_sector_members(self, new_data: dict[str, set[str]], trade_date, context: RunContext) -> tuple[int, int, int]:
        rows = self.kernel.read._repository_query(
            context=context,
            source="sector_stock",
            sql="SELECT sector_name, stock_code FROM sector_stock",
            scope=WorkScope(partition="sector-members"),
        )
        existing = {}
        for row in rows:
            existing.setdefault(row["sector_name"], set()).add(row["stock_code"])
        inserts, deletes, changelogs = [], [], []
        for sector in set(new_data) | set(existing):
            added = set(new_data.get(sector, set())) - existing.get(sector, set())
            removed = existing.get(sector, set()) - set(new_data.get(sector, set()))
            inserts.extend((sector, code, trade_date) for code in added)
            deletes.extend((sector, code) for code in removed)
            changelogs.extend((sector, code, trade_date, "add") for code in added)
            changelogs.extend((sector, code, trade_date, "remove") for code in removed)
        if not inserts and not deletes:
            return 0, 0, 0
        delete_sql = ""
        params: list = []
        if deletes:
            delete_sql = "DELETE FROM sector_stock WHERE (sector_name, stock_code) IN (" + ",".join(["(%s,%s)"] * len(deletes)) + ");"
            params.extend(value for row in deletes for value in row)
        if inserts:
            insert_sql = "INSERT INTO sector_stock (sector_name, stock_code, update_date) VALUES " + ",".join(["(%s,%s,%s)"] * len(inserts)) + " ON CONFLICT DO NOTHING;"
            params.extend(value for row in inserts for value in row)
        else:
            insert_sql = "INSERT INTO sector_stock (sector_name, stock_code, update_date) SELECT NULL, NULL, NULL WHERE FALSE;"
        if changelogs:
            change_sql = "INSERT INTO sector_changelog (sector_name, stock_code, changed_at, action) VALUES " + ",".join(["(%s,%s,%s,%s)"] * len(changelogs)) + " ON CONFLICT DO NOTHING;"
            params.extend(value for row in changelogs for value in row)
        else:
            change_sql = ""
        scope = WorkScope(partition=f"sector:{trade_date}")
        plan = SqlPlan(Operation.REBUILD, "sector_stock", scope, expected_mode="SET_BASED", allow_delete=True)
        self.kernel.rebuild.run_scoped_merge(delete_sql + insert_sql + change_sql, plan, context, params=tuple(params))
        return len(inserts), len(deletes), len(changelogs)

    def sync_index_weights(self, new_data: dict[str, dict[str, float]], trade_date, context: RunContext) -> tuple[int, int, int]:
        rows = self.kernel.read._repository_query(
            context=context,
            source="index_weight",
            sql="SELECT index_code, stock_code, weight FROM index_weight",
            scope=WorkScope(partition="index-weights"),
        )
        existing = {}
        for row in rows:
            existing.setdefault(row["index_code"], {})[row["stock_code"]] = row["weight"]
        inserts, deletes, changelogs = [], [], []
        for index_code in set(new_data) | set(existing):
            current, previous = new_data.get(index_code, {}), existing.get(index_code, {})
            for code in set(current) | set(previous):
                new_weight, old_weight = current.get(code), previous.get(code)
                if new_weight is not None and (old_weight is None or abs(new_weight - old_weight) > 1e-6):
                    inserts.append((index_code, code, new_weight, trade_date))
                    changelogs.append((index_code, code, trade_date, old_weight, new_weight))
                elif new_weight is None and old_weight is not None:
                    deletes.append((index_code, code))
                    changelogs.append((index_code, code, trade_date, old_weight, None))
        if not inserts and not deletes:
            return 0, 0, 0
        delete_sql = ""
        params: list = []
        if deletes:
            delete_sql = "DELETE FROM index_weight WHERE (index_code, stock_code) IN (" + ",".join(["(%s,%s)"] * len(deletes)) + ");"
            params.extend(value for row in deletes for value in row)
        if inserts:
            insert_sql = "INSERT INTO index_weight (index_code, stock_code, weight, update_date) VALUES " + ",".join(["(%s,%s,%s,%s)"] * len(inserts)) + " ON CONFLICT (index_code, stock_code) DO UPDATE SET weight=EXCLUDED.weight, update_date=EXCLUDED.update_date;"
            params.extend(value for row in inserts for value in row)
        else:
            insert_sql = "INSERT INTO index_weight (index_code, stock_code, weight, update_date) SELECT NULL, NULL, NULL, NULL WHERE FALSE;"
        if changelogs:
            change_sql = "INSERT INTO index_weight_changelog (index_code, stock_code, changed_at, old_weight, new_weight) VALUES " + ",".join(["(%s,%s,%s,%s,%s)"] * len(changelogs)) + " ON CONFLICT DO NOTHING;"
            params.extend(value for row in changelogs for value in row)
        else:
            change_sql = ""
        scope = WorkScope(partition=f"index-weight:{trade_date}")
        plan = SqlPlan(Operation.REBUILD, "index_weight", scope, expected_mode="SET_BASED", allow_delete=True)
        self.kernel.rebuild.run_scoped_merge(delete_sql + insert_sql + change_sql, plan, context, params=tuple(params))
        return len(inserts), len(deletes), len(changelogs)

    def sync_instruments(self, details: dict[str, dict], changed_at, context: RunContext) -> tuple[int, int, int]:
        codes = tuple(details)
        if not codes:
            return 0, 0, 0
        rows = self.kernel.read._repository_query(
            context=context,
            source="instrument_info",
            sql="SELECT * FROM instrument_info WHERE stock_code=ANY(%s)",
            params=(list(codes),),
            scope=WorkScope(codes=codes, partition="instrument-info"),
        )
        existing = {row.pop("stock_code"): row for row in rows}
        inserts, updates, changelogs = [], [], []
        for code, detail in details.items():
            old = existing.get(code)
            if old is None:
                inserts.append({"stock_code": code, **detail})
                continue
            changed = {
                field: (str(old.get(field)), str(value))
                for field, value in detail.items()
                if str(old.get(field)) != str(value)
            }
            if changed:
                updates.append({"stock_code": code, **detail})
                changelogs.extend({
                    "stock_code": code, "changed_at": changed_at, "field_name": field,
                    "old_value": old_value, "new_value": new_value,
                } for field, (old_value, new_value) in changed.items())
        scope = WorkScope(codes=codes, date_from=changed_at, date_to=changed_at)
        repo_rows = inserts + updates
        if repo_rows:
            all_columns = tuple(sorted({column for row in repo_rows for column in row}))
            repo_rows = [
                {column: row.get(column) for column in all_columns}
                for row in repo_rows
            ]
            update_columns = tuple(column for column in all_columns if column != "stock_code")
            self.kernel.write.upsert(
                context=context, scope=scope, target="instrument_info", rows=repo_rows,
                key=("stock_code",), update_columns=update_columns,
            )
        if changelogs:
            self.kernel.write.upsert(
                context=context, scope=scope, target="instrument_changelog", rows=changelogs,
                key=("stock_code", "changed_at", "field_name"),
                update_columns=("old_value", "new_value"),
            )
        return len(inserts), len(updates), len(changelogs)

    def rebuild_status_daily(self, scope: WorkScope, context: RunContext) -> int:
        """重建某日期范围的证券状态（scoped merge）。

        强制 scope 带 date_from/date_to（QueryGuard 会因无范围 DELETE 拒绝）。
        """
        if not isinstance(scope, WorkScope):
            raise TypeError("rebuild_status_daily 必须携带 WorkScope")
        if scope.date_from is None or scope.date_to is None:
            raise ValueError("rebuild_status_daily 需要 scope.date_from/date_to")

        plan = SqlPlan(
            operation=Operation.REBUILD,
            target="std.security_status_daily",
            scope=scope,
            expected_mode="SET_BASED",
            estimated_rows=0,
            allow_insert_select=True,
            allow_delete=True,  # SQL 含 scoped DELETE
        )
        # params 顺序：DELETE(2) + dates(2) + limits(2)
        params = (
            scope.date_from, scope.date_to,
            scope.date_from, scope.date_to,
            scope.date_from, scope.date_to,
        )
        return self.kernel.rebuild.run_scoped_merge(
            sql=_STATUS_DAILY_REBUILD_SQL, plan=plan, context=context, params=params
        )

    def rebuild_dividend(self, context: RunContext) -> int:
        """重建分红事件（纯 upsert，无 TRUNCATE——原实现 TRUNCATE 后 ON CONFLICT 冗余且危险）。"""
        plan = SqlPlan(
            operation=Operation.REBUILD,
            target="std.dividend_event",
            scope=WorkScope(),
            expected_mode="SET_BASED",
            estimated_rows=100_000,
            allow_insert_select=True,
        )
        return self.kernel.rebuild.run_insert_select(
            sql=_DIVIDEND_REBUILD_SQL, plan=plan, context=context
        )

    def rebuild_lifecycle(self, context: RunContext) -> int:
        """重建证券生命周期（名称历史，全量快照重建）。

        用 staging + 原子 swap（TABLE_REPLACE），消除原实现的 TRUNCATE
        （TRUNCATE 无法回滚、锁全表，文档 §5.2 禁止）。
        """
        plan = SqlPlan(
            operation=Operation.REBUILD,
            target="std.security_lifecycle",
            scope=WorkScope(),
            expected_mode="SET_BASED",
            estimated_rows=50_000,
            allow_insert_select=True,
        )
        return self.kernel.rebuild.run_table_replace(
            fill_sql=_LIFECYCLE_FILL_SQL, plan=plan, context=context
        )

    def load_trade_date_range(self, context: RunContext) -> tuple | None:
        """读交易日范围（从交易日历，避免对派生大表做 MIN/MAX 全扫描，文档 §10.3）。"""
        from datetime import date as _date

        row = self.kernel.read.scalar(
            context=context,
            source="std.trading_calendar",
            aggregate="MIN(trade_date), MAX(trade_date)",
            where={"exchange__eq": "SSE", "is_open__eq": True},
        )
        if not row or not row[0] or not row[1]:
            return None
        first = row[0] if isinstance(row[0], _date) else _date.fromisoformat(str(row[0]))
        last = row[1] if isinstance(row[1], _date) else _date.fromisoformat(str(row[1]))
        return first, last


# 迁移自 tushare_phase2_standard.rebuild_dividend（去掉 TRUNCATE，纯 ON CONFLICT upsert）
_DIVIDEND_REBUILD_SQL = """
INSERT INTO std.dividend_event
    (ts_code, report_date, announce_date, implementation_status, stock_div,
     stock_bonus_rate, stock_conversion_rate, cash_div, cash_div_tax,
     record_date, ex_date, pay_date, div_list_date, implementation_announce_date,
     source, source_record_key, record_version, business_key, payload_hash, quality_status)
SELECT ts_code, end_date, ann_date, div_proc,
       NULLIF(payload->>'stk_div', '')::double precision,
       NULLIF(payload->>'stk_bo_rate', '')::double precision,
       NULLIF(payload->>'stk_co_rate', '')::double precision,
       NULLIF(payload->>'cash_div', '')::double precision,
       NULLIF(payload->>'cash_div_tax', '')::double precision,
       record_date, ex_date,
       to_date(NULLIF(payload->>'pay_date', ''), 'YYYYMMDD'),
       to_date(NULLIF(payload->>'div_listdate', ''), 'YYYYMMDD'),
       to_date(NULLIF(payload->>'imp_ann_date', ''), 'YYYYMMDD'),
       source, business_key, record_id, business_key, payload_hash,
       CASE WHEN div_proc = '实施' AND ex_date IS NULL THEN 'WARNING' ELSE 'VALID' END
FROM (
  SELECT DISTINCT ON (ts_code, end_date, div_proc)
         record_id, ts_code, end_date, ann_date, div_proc, record_date, ex_date,
         payload, source, payload_hash,
         ts_code || ':' || COALESCE(end_date::text,'') || ':' ||
         COALESCE(div_proc,'') AS business_key
  FROM tushare_dividend_raw
  ORDER BY ts_code, end_date, div_proc,
           to_date(NULLIF(payload->>'imp_ann_date',''),'YYYYMMDD') DESC NULLS LAST,
           ann_date DESC NULLS LAST, fetched_at DESC, record_id DESC
) d
ON CONFLICT (source, source_record_key, record_version) DO UPDATE SET
  report_date=EXCLUDED.report_date, announce_date=EXCLUDED.announce_date,
  implementation_status=EXCLUDED.implementation_status, stock_div=EXCLUDED.stock_div,
  cash_div=EXCLUDED.cash_div, cash_div_tax=EXCLUDED.cash_div_tax,
  record_date=EXCLUDED.record_date, ex_date=EXCLUDED.ex_date,
  pay_date=EXCLUDED.pay_date, div_list_date=EXCLUDED.div_list_date,
  implementation_announce_date=EXCLUDED.implementation_announce_date,
  business_key=EXCLUDED.business_key, payload_hash=EXCLUDED.payload_hash,
  quality_status=EXCLUDED.quality_status, fetched_at=CURRENT_TIMESTAMP
"""


# 迁移自 tushare_phase2_standard.rebuild_lifecycle（TRUNCATE → staging+swap，INSERT 目标改为 {staging}）
_LIFECYCLE_FILL_SQL = """
INSERT INTO {staging}
    (ts_code, symbol, name, exchange, market, list_status, list_date, delist_date,
     effective_from, effective_to, source, source_record_key, record_version,
     source_payload_hash, quality_status)
WITH ranked AS (
    SELECT DISTINCT ON (n.ts_code, n.start_date)
           n.ts_code, n.start_date, n.end_date, n.payload->>'name' AS name,
           n.payload_hash, n.source, n.fetched_at, n.record_id
    FROM tushare_namechange_raw n
    WHERE n.start_date IS NOT NULL
    ORDER BY n.ts_code, n.start_date, n.fetched_at DESC, n.record_id DESC
), chained AS (
    SELECT r.*, LEAD(r.start_date) OVER (PARTITION BY r.ts_code ORDER BY r.start_date) AS next_start
    FROM ranked r
), named AS (
    SELECT c.ts_code, b.symbol, c.name, split_part(c.ts_code, '.', 2) AS exchange,
           b.market, b.list_status, b.list_date, b.delist_date,
           GREATEST(c.start_date, COALESCE(b.list_date, c.start_date)) AS effective_from,
           NULLIF(LEAST(COALESCE(c.end_date, DATE '9999-12-31'),
                        COALESCE(c.next_start - 1, DATE '9999-12-31'),
                        COALESCE(b.delist_date, DATE '9999-12-31')),
                  DATE '9999-12-31') AS effective_to,
           c.source, c.ts_code || ':namechange:' || c.payload_hash AS source_record_key,
           c.record_id AS record_version, c.payload_hash AS source_payload_hash,
           CASE WHEN c.next_start IS NULL AND c.end_date IS NULL THEN 'VALID' ELSE 'VALID' END AS quality_status,
           c.fetched_at
    FROM chained c
    LEFT JOIN std.stock_basic_info b ON b.stock_code = c.ts_code
), fallback AS (
    SELECT b.stock_code AS ts_code, b.symbol, b.name, split_part(b.stock_code, '.', 2),
           b.market, b.list_status, b.list_date, b.delist_date,
           COALESCE(b.list_date, DATE '1990-01-01'), b.delist_date,
           b.source, b.stock_code || ':stock_basic',
           1::bigint,
           CASE WHEN r.payload IS NULL THEN NULL
                ELSE encode(digest(convert_to(r.payload::text, 'UTF8'), 'sha256'), 'hex') END,
           CASE WHEN b.list_date IS NULL THEN 'WARNING' ELSE 'VALID' END, b.fetched_at
    FROM std.stock_basic_info b
    LEFT JOIN tushare_stock_basic_raw r ON r.ts_code = b.stock_code
    WHERE NOT EXISTS (
        SELECT 1 FROM tushare_namechange_raw n
        WHERE n.ts_code=b.stock_code AND n.start_date IS NOT NULL
    )
), all_rows AS (
    SELECT * FROM named WHERE effective_from <= COALESCE(effective_to, DATE '9999-12-31')
    UNION ALL SELECT * FROM fallback
)
SELECT ts_code, symbol, name, exchange, market, list_status, list_date, delist_date,
       effective_from, effective_to, source, source_record_key, record_version,
       source_payload_hash, quality_status
FROM all_rows
"""
