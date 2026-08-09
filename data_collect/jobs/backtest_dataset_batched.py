"""Resource-safe, resumable Backtest Dataset materialization.

The legacy builder keeps one repeatable-read transaction open while deleting and
materializing millions of rows.  This module keeps the same source selection,
unit conversion, quality rules, and hashes, but commits one bounded trading-day
batch at a time.  A failed batch is safe to repeat; completed batches are
skipped only when they were produced by the current code version.
"""
from __future__ import annotations

import hashlib
import json
import math
from datetime import date
from typing import Any

from data_collect.jobs.backtest_dataset import (
    ADAPTER_VERSION,
    FEATURE_VERSION,
    HASH_ALGORITHM,
    _canonical,
    _check_baseline,
    _execution_limit_safety,
    _git_metadata,
    _json_param,
    _manifest,
    _materialized_hashes,
    _quality_report_hash,
    _set_checkpoint,
    _IncrementalRowHasher,
    hash_rows,
    require_psycopg2,
)
from data_collect.ops.market_coverage import assert_market_coverage_range_publishable
from data_collect.utils.db import get_connection


EXECUTION_INSERT = """
INSERT INTO backtest.dataset_execution_bar
  (dataset_id,trade_date,ts_code,market,raw_open,raw_high,raw_low,raw_close,
   volume,amount,up_limit,down_limit,is_suspended,can_buy,can_sell,
   limit_status,source_record_key,source_record_version,source_payload_hash,
   limit_source_record_id,limit_source_payload_hash,
   suspend_source_record_id,suspend_source_payload_hash,quality_status)
VALUES %s
"""


def _execution_sql() -> str:
    return """
        SELECT m.research_date, s.ts_code, s.market,
               NULLIF(COALESCE(v.payload, r.payload)->>'open', '')::double precision,
               NULLIF(COALESCE(v.payload, r.payload)->>'high', '')::double precision,
               NULLIF(COALESCE(v.payload, r.payload)->>'low', '')::double precision,
               NULLIF(COALESCE(v.payload, r.payload)->>'close', '')::double precision,
               NULLIF(COALESCE(v.payload, r.payload)->>'vol', '')::double precision * 100,
               NULLIF(COALESCE(v.payload, r.payload)->>'amount', '')::double precision * 1000,
               NULLIF(lim.payload->>'up_limit', '')::double precision,
               NULLIF(lim.payload->>'down_limit', '')::double precision,
               (COALESCE(s.is_suspended_asof, FALSE) OR sus.record_id IS NOT NULL),
               (COALESCE(s.can_buy, FALSE)
                AND NOT (COALESCE(s.is_suspended_asof, FALSE) OR sus.record_id IS NOT NULL)),
               (COALESCE(s.can_sell, FALSE)
                AND NOT (COALESCE(s.is_suspended_asof, FALSE) OR sus.record_id IS NOT NULL)),
               (COALESCE(v.payload, r.payload) IS NOT NULL AND lim.record_id IS NOT NULL),
               s.limit_status, COALESCE(v.payload, r.payload),
               v.record_id, v.payload_hash,
               lim.record_id, lim.payload_hash,
               sus.record_id, sus.payload_hash
          FROM backtest.dataset_member m
          JOIN research.stock_snapshot_daily s
            ON s.snapshot_id=m.research_snapshot_id
           AND s.research_date=m.research_date
          LEFT JOIN LATERAL (
              SELECT record_id, payload, payload_hash
                FROM tushare_market_raw_version mv
               WHERE mv.dataset='daily'
                 AND mv.business_key=s.ts_code || ':' || to_char(m.research_date,'YYYYMMDD')
                 AND mv.record_id <= %s
               ORDER BY mv.fetched_at DESC, mv.record_id DESC LIMIT 1
          ) v ON TRUE
          LEFT JOIN LATERAL (
              SELECT legacy.payload
                FROM tushare_daily_raw legacy
               WHERE v.record_id IS NULL
                 AND legacy.ts_code=s.ts_code
                 AND legacy.trade_date=m.research_date
                 AND legacy.source='tushare'
                 AND legacy.fetched_at <= %s::timestamp
               ORDER BY legacy.fetched_at DESC
               LIMIT 1
          ) r ON TRUE
          LEFT JOIN LATERAL (
              SELECT record_id, payload, payload_hash
                FROM tushare_stk_limit_raw raw_limit
               WHERE raw_limit.ts_code=s.ts_code
                 AND raw_limit.trade_date=m.research_date
                 AND raw_limit.record_id <= %s
               ORDER BY raw_limit.fetched_at DESC, raw_limit.record_id DESC LIMIT 1
          ) lim ON TRUE
          LEFT JOIN LATERAL (
              SELECT record_id, payload_hash
                FROM tushare_suspend_d_raw raw_suspend
               WHERE raw_suspend.ts_code=s.ts_code
                 AND raw_suspend.trade_date=m.research_date
                 AND raw_suspend.record_id <= %s
               ORDER BY raw_suspend.fetched_at DESC, raw_suspend.record_id DESC LIMIT 1
          ) sus ON TRUE
         WHERE m.dataset_id=%s
           AND m.research_date BETWEEN %s AND %s
         ORDER BY m.research_date, s.ts_code
    """


def _transform_row(row: tuple[Any, ...], dataset_id: str) -> tuple[tuple[Any, ...], str]:
    (
        day, code, market, opn, high, low, close, volume, amount, up, down,
        suspended, can_buy, can_sell, status_source_present, limit_status,
        payload, version, payload_hash, limit_version, limit_hash,
        suspend_version, suspend_hash,
    ) = row
    up, down, can_buy, can_sell, limit_unknown = _execution_limit_safety(
        up, down, suspended, can_buy, can_sell
    )
    computed_hash = payload_hash
    if computed_hash is None and payload is not None:
        computed_hash = hashlib.sha256(
            json.dumps(
                _canonical(payload), ensure_ascii=False, sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()
    has_ohlc = all(value is not None and float(value) > 0 for value in (opn, high, low, close))
    geometry_valid = (
        has_ohlc
        and float(low) <= min(float(opn), float(close))
        <= max(float(opn), float(close)) <= float(high)
    )
    has_activity = (
        volume is not None and math.isfinite(float(volume)) and float(volume) > 0
        and amount is not None and math.isfinite(float(amount)) and float(amount) > 0
    )
    amount_volume_valid = True
    if has_activity and has_ohlc:
        average_price = float(amount) / float(volume)
        amount_volume_valid = (
            math.isfinite(average_price)
            and float(low) * 0.5 <= average_price <= float(high) * 1.5
        )
    if not status_source_present:
        quality = "WARNING" if suspended or (not can_buy and not can_sell) else "MISSING_SOURCE"
    elif suspended and geometry_valid and computed_hash is not None:
        quality = "WARNING"
    elif geometry_valid and computed_hash is not None and not limit_unknown and has_activity and amount_volume_valid:
        quality = "VALID"
    elif geometry_valid and computed_hash is not None and (limit_unknown or not has_activity):
        quality = "WARNING"
    elif geometry_valid and computed_hash is not None and not amount_volume_valid:
        quality = "INVALID"
    elif has_ohlc and not geometry_valid:
        quality = "INVALID"
    elif suspended:
        quality = "WARNING"
    else:
        quality = "MISSING_SOURCE"
    values = (
        dataset_id, day, code, market, opn, high, low, close, volume, amount, up, down,
        suspended, can_buy, can_sell, limit_status, f"{code}:{day}", version,
        computed_hash, limit_version, limit_hash, suspend_version, suspend_hash, quality,
    )
    return values, quality


def _batch_checkpoint(cur, dataset_id: str, batch_start: date) -> tuple[Any, ...] | None:
    cur.execute(
        """SELECT status, batch_end, row_count, batch_hash, code_version
             FROM backtest.dataset_build_batch_checkpoint
            WHERE dataset_id=%s AND stage='BUILD' AND batch_start=%s""",
        (dataset_id, batch_start),
    )
    return cur.fetchone()


def _set_batch_checkpoint(
    dataset_id: str,
    batch_start: date,
    batch_end: date,
    status: str,
    *,
    row_count: int = 0,
    batch_hash: str | None = None,
    code_version: str | None = None,
    details: dict[str, Any] | None = None,
    error_message: str | None = None,
) -> None:
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """INSERT INTO backtest.dataset_build_batch_checkpoint
                (dataset_id,stage,batch_start,batch_end,status,row_count,batch_hash,
                 code_version,details,error_message,started_at,heartbeat_at,completed_at)
              VALUES (%s,'BUILD',%s,%s,%s,%s,%s,%s,%s,%s,
                      CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,
                      CASE WHEN %s='COMPLETED' THEN CURRENT_TIMESTAMP ELSE NULL END)
              ON CONFLICT (dataset_id,stage,batch_start) DO UPDATE SET
                batch_end=EXCLUDED.batch_end, status=EXCLUDED.status,
                row_count=EXCLUDED.row_count, batch_hash=EXCLUDED.batch_hash,
                code_version=EXCLUDED.code_version, details=EXCLUDED.details,
                error_message=EXCLUDED.error_message, heartbeat_at=CURRENT_TIMESTAMP,
                completed_at=EXCLUDED.completed_at""",
            (dataset_id, batch_start, batch_end, status, row_count, batch_hash,
             code_version, _json_param(details or {}), error_message, status),
        )
        conn.commit()


def _trading_batches(conn, dataset_id: str, batch_days: int) -> list[tuple[date, date, list[date]]]:
    with conn.cursor() as cur:
        cur.execute(
            """SELECT research_date FROM backtest.dataset_member
                WHERE dataset_id=%s ORDER BY research_date""", (dataset_id,)
        )
        dates = [row[0] for row in cur.fetchall()]
    if not dates:
        raise RuntimeError("Dataset 没有 dataset_member，不能构建执行行情")
    return [
        (chunk[0], chunk[-1], chunk)
        for offset in range(0, len(dates), batch_days)
        for chunk in [dates[offset:offset + batch_days]]
    ]


def _clear_small_children(dataset_id: str) -> None:
    with get_connection() as conn, conn.cursor() as cur:
        for table in (
            "backtest.dataset_calendar",
            "backtest.dataset_dividend_event",
            "backtest.dataset_rule_snapshot",
            "backtest.dataset_benchmark_bar",
        ):
            cur.execute(f"DELETE FROM {table} WHERE dataset_id=%s", (dataset_id,))
        conn.commit()


def _prepare_manifest(dataset_id: str) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    with get_connection() as conn, conn.cursor() as cur:
        manifest = _manifest(conn, dataset_id, for_update=True)
        if manifest["status"] not in {"DRAFT", "FAILED", "BUILDING"}:
            raise RuntimeError(f"只有 DRAFT/FAILED/BUILDING Dataset 可分批构建: {manifest['status']}")
        baseline = _check_baseline(
            conn, manifest["baseline"], manifest["evaluation_end"],
            expected_quality_report_hash=manifest.get("baseline_quality_report_hash"),
            expected_source_manifest_hash=manifest.get("baseline_source_manifest_hash"),
        )
        assert_market_coverage_range_publishable(
            manifest["warmup_start"], manifest["evaluation_end"], connection=conn
        )
        source = baseline["source_manifest"]
        append_only = source.get("append_only_sources", {})
        boundaries = {
            "daily": source.get("market_versions", {}).get("daily", {}).get("max_record_id"),
            "legacy_daily": source.get("legacy_market_raw", {}).get("tushare_daily_raw", {}).get("max_fetched_at"),
            "limit": append_only.get("tushare_stk_limit_raw", {}).get("max_record_id"),
            "suspend": append_only.get("tushare_suspend_d_raw", {}).get("max_record_id"),
            "dividend": append_only.get("tushare_dividend_raw", {}).get("max_record_id"),
            "trade_cal": source.get("market_versions", {}).get("trade_cal", {}).get("max_record_id"),
            "legacy_trade_cal": source.get("legacy_market_raw", {}).get("tushare_trade_cal_raw", {}).get("max_fetched_at"),
            "index": append_only.get("tushare_index_daily_raw", {}).get("max_record_id"),
        }
        missing = [key for key, value in boundaries.items() if value is None]
        if missing:
            raise RuntimeError("Baseline 缺少 Dataset 所需 Raw 高水位: " + ", ".join(missing))
        cur.execute(
            """UPDATE backtest.dataset_manifest
                  SET status='BUILDING', failure_reason=NULL, failed_at=NULL
                WHERE dataset_id=%s""", (dataset_id,)
        )
        conn.commit()
        return manifest, boundaries, _git_metadata()


def _build_execution_batch(
    dataset_id: str,
    batch_start: date,
    batch_end: date,
    boundaries: dict[str, Any],
    code_version: str,
) -> dict[str, Any]:
    _set_batch_checkpoint(
        dataset_id, batch_start, batch_end, "RUNNING", code_version=code_version,
        details={"mode": "date_batch", "batch_start": batch_start.isoformat(), "batch_end": batch_end.isoformat()},
    )
    try:
        with get_connection() as conn, conn.cursor() as cur:
            cur.execute("SET LOCAL work_mem = '64MB'")
            cur.execute(
                """DELETE FROM backtest.dataset_execution_bar
                    WHERE dataset_id=%s AND trade_date BETWEEN %s AND %s""",
                (dataset_id, batch_start, batch_end),
            )
            named = conn.cursor(name=f"dataset_batch_{dataset_id.replace('-', '')}_{batch_start:%Y%m%d}")
            named.itersize = 5000
            named.execute(
                _execution_sql(),
                (boundaries["daily"], boundaries["legacy_daily"], boundaries["limit"],
                 boundaries["suspend"], dataset_id, batch_start, batch_end),
            )
            insert_rows: list[tuple[Any, ...]] = []
            batch_hasher = _IncrementalRowHasher()
            per_day: dict[date, dict[str, Any]] = {}
            total = 0
            execute_values = require_psycopg2()[1]
            while True:
                source_rows = named.fetchmany(5000)
                if not source_rows:
                    break
                for source_row in source_rows:
                    values, quality = _transform_row(source_row, dataset_id)
                    insert_rows.append(values)
                    batch_hasher.update(values[1:])
                    total += 1
                    stats = per_day.setdefault(
                        values[1], {"hasher": _IncrementalRowHasher(), "count": 0,
                                    "missing": 0, "invalid": 0, "warning": 0},
                    )
                    stats["hasher"].update(values[1:])
                    stats["count"] += 1
                    if quality == "MISSING_SOURCE":
                        stats["missing"] += 1
                    elif quality == "INVALID":
                        stats["invalid"] += 1
                    elif quality == "WARNING":
                        stats["warning"] += 1
                    if len(insert_rows) >= 5000:
                        execute_values(cur, EXECUTION_INSERT, insert_rows, page_size=2000)
                        insert_rows.clear()
            named.close()
            if insert_rows:
                execute_values(cur, EXECUTION_INSERT, insert_rows, page_size=2000)

            for day, stats in per_day.items():
                cur.execute(
                    """SELECT snapshot_input_hash,
                              (SELECT COUNT(*) FROM research.stock_snapshot_daily s
                                WHERE s.snapshot_id=m.research_snapshot_id
                                  AND s.research_date=m.research_date)
                         FROM backtest.dataset_member m
                        WHERE m.dataset_id=%s AND m.research_date=%s""",
                    (dataset_id, day),
                )
                member = cur.fetchone()
                if not member:
                    raise RuntimeError(f"执行批次出现未绑定 Dataset 成员: {day}")
                cur.execute(
                    """UPDATE backtest.dataset_member
                          SET observation_row_count=%s, execution_row_count=%s,
                              execution_hash=%s, quality_status=%s
                        WHERE dataset_id=%s AND research_date=%s""",
                    (member[1], stats["count"], stats["hasher"].hexdigest(),
                     ("INVALID" if stats["invalid"] else
                      "MISSING_SOURCE" if stats["missing"] else
                      "WARNING" if stats["warning"] else "VALID"), dataset_id, day),
                )
            cur.execute(
                """UPDATE backtest.dataset_build_batch_checkpoint
                      SET status='COMPLETED', row_count=%s, batch_hash=%s,
                          code_version=%s, details=%s, error_message=NULL,
                          heartbeat_at=CURRENT_TIMESTAMP, completed_at=CURRENT_TIMESTAMP
                    WHERE dataset_id=%s AND stage='BUILD' AND batch_start=%s""",
                (total, batch_hasher.hexdigest(), code_version,
                 _json_param({"dates": len(per_day), "rows": total}),
                 dataset_id, batch_start),
            )
            conn.commit()
            return {"batch_start": batch_start, "batch_end": batch_end,
                    "rows": total, "hash": batch_hasher.hexdigest()}
    except Exception as exc:
        _set_batch_checkpoint(
            dataset_id, batch_start, batch_end, "FAILED", code_version=code_version,
            error_message=str(exc)[:4000],
        )
        raise


def _rebuild_auxiliary(dataset_id: str, manifest: dict[str, Any], boundaries: dict[str, Any]) -> None:
    """Build small calendar/corporate-action/rule/benchmark children separately."""
    with get_connection() as conn, conn.cursor() as cur:
        for table in (
            "backtest.dataset_calendar",
            "backtest.dataset_dividend_event",
            "backtest.dataset_rule_snapshot",
            "backtest.dataset_benchmark_bar",
        ):
            cur.execute(f"DELETE FROM {table} WHERE dataset_id=%s", (dataset_id,))
        cur.execute(
            """WITH days AS (
                   SELECT generate_series(%s::date,%s::date,interval '1 day')::date AS trade_date
               )
               SELECT d.trade_date,
                      COALESCE(v.payload->>'exchange',r.payload->>'exchange','SSE'),
                      COALESCE(NULLIF(v.payload->>'is_open','')::integer,
                               NULLIF(r.payload->>'is_open','')::integer)=1,
                      to_date(NULLIF(COALESCE(v.payload->>'pretrade_date',r.payload->>'pretrade_date'),''),'YYYYMMDD'),
                      v.record_id,
                      COALESCE(v.payload_hash, encode(digest(convert_to(r.payload::text,'UTF8'),'sha256'),'hex'))
                 FROM days d
                 LEFT JOIN LATERAL (
                    SELECT record_id,payload,payload_hash FROM tushare_market_raw_version mv
                     WHERE mv.dataset='trade_cal' AND mv.exchange='SSE' AND mv.cal_date=d.trade_date
                       AND mv.record_id <= %s
                     ORDER BY mv.fetched_at DESC,mv.record_id DESC LIMIT 1
                 ) v ON TRUE
                 LEFT JOIN LATERAL (
                    SELECT payload FROM tushare_trade_cal_raw legacy
                     WHERE v.record_id IS NULL AND legacy.exchange='SSE'
                       AND legacy.cal_date=d.trade_date AND legacy.fetched_at <= %s::timestamp
                     ORDER BY legacy.fetched_at DESC LIMIT 1
                 ) r ON TRUE
                WHERE COALESCE(v.payload,r.payload) IS NOT NULL ORDER BY d.trade_date""",
            (manifest["warmup_start"], manifest["evaluation_end"],
             boundaries["trade_cal"], boundaries["legacy_trade_cal"]),
        )
        calendar_rows = cur.fetchall()
        expected = (manifest["evaluation_end"] - manifest["warmup_start"]).days + 1
        if len(calendar_rows) != expected:
            raise RuntimeError(f"交易日历覆盖不完整: rows={len(calendar_rows)}, expected={expected}")
        execute_values = require_psycopg2()[1]
        execute_values(cur,
            """INSERT INTO backtest.dataset_calendar
              (dataset_id,trade_date,exchange,is_open,pretrade_date,source_record_key,
               source_record_version,source_payload_hash) VALUES %s""",
            [(dataset_id, day, exchange, is_open, pretrade, f"{exchange}:{day}", version, source_hash)
             for day, exchange, is_open, pretrade, version, source_hash in calendar_rows], page_size=1000)

        cur.execute(
            """WITH candidates AS (
                   SELECT record_id,ts_code,end_date AS report_date,ann_date AS announce_date,
                          div_proc AS implementation_status,
                          NULLIF(payload->>'stk_div','')::double precision AS stock_div,
                          NULLIF(payload->>'stk_bo_rate','')::double precision AS stock_bonus_rate,
                          NULLIF(payload->>'stk_co_rate','')::double precision AS stock_conversion_rate,
                          NULLIF(payload->>'cash_div','')::double precision AS cash_div,
                          NULLIF(payload->>'cash_div_tax','')::double precision AS cash_div_tax,
                          record_date,ex_date,
                          to_date(NULLIF(payload->>'pay_date',''),'YYYYMMDD') AS pay_date,
                          to_date(NULLIF(payload->>'div_listdate',''),'YYYYMMDD') AS div_list_date,
                          to_date(NULLIF(payload->>'imp_ann_date',''),'YYYYMMDD') AS implementation_announce_date,
                          ts_code||':'||COALESCE(end_date::text,'')||':'||COALESCE(div_proc,'')||':'||
                          COALESCE(record_date::text,'')||':'||COALESCE(ex_date::text,'')||':'||
                          COALESCE(payload->>'imp_ann_date','') AS event_key,
                          payload_hash,fetched_at
                     FROM tushare_dividend_raw
                    WHERE record_id <= %s AND div_proc='实施' AND ex_date IS NOT NULL
                      AND ex_date <= %s
                      AND COALESCE(to_date(NULLIF(payload->>'pay_date',''),'YYYYMMDD'),ex_date) >= %s
                      AND COALESCE(to_date(NULLIF(payload->>'imp_ann_date',''),'YYYYMMDD'),ann_date,end_date,ex_date) <= ex_date
               ), chosen AS (
                   SELECT DISTINCT ON (event_key) * FROM candidates
                    ORDER BY event_key,fetched_at DESC,record_id DESC
               )
               SELECT ts_code,report_date,announce_date,implementation_status,stock_div,
                      stock_bonus_rate,stock_conversion_rate,cash_div,cash_div_tax,record_date,ex_date,
                      pay_date,div_list_date,implementation_announce_date,event_key,record_id,payload_hash
                 FROM chosen ORDER BY ex_date,ts_code,event_key,record_id""",
            (boundaries["dividend"], manifest["evaluation_end"], manifest["warmup_start"]),
        )
        dividend_rows = cur.fetchall()
        execute_values(cur,
            """INSERT INTO backtest.dataset_dividend_event
              (dataset_id,ts_code,report_date,announce_date,implementation_status,stock_div,
               stock_bonus_rate,stock_conversion_rate,cash_div,cash_div_tax,record_date,ex_date,
               pay_date,div_list_date,implementation_announce_date,source_record_key,
               source_record_version,source_payload_hash) VALUES %s""",
            [(dataset_id, *row) for row in dividend_rows], page_size=1000)

        cur.execute(
            """SELECT rule_version,market_rule_version,fee_rule_version,effective_from,effective_to,
                      market,min_buy_lot,allow_sell_odd_lot,t_plus_one,price_tick,cash_settlement_days,
                      limit_pct,commission_rate,min_commission,stamp_duty_sell_rate,transfer_fee_rate,
                      slippage_bps,parameters,approval_status,approved_by,approved_at,approval_hash
                 FROM backtest.market_rule_effective
                WHERE effective_from <= %s AND COALESCE(effective_to,DATE '9999-12-31') >= %s
                  AND market_rule_version=%s AND fee_rule_version=%s AND approval_status='APPROVED'
                  AND approved_by IS NOT NULL AND approved_at IS NOT NULL AND approval_hash IS NOT NULL
                ORDER BY effective_from,market,rule_version""",
            (manifest["evaluation_end"], manifest["warmup_start"],
             manifest["market_rule_version"], manifest["fee_rule_version"]),
        )
        source_rules = cur.fetchall()
        if not source_rules:
            raise RuntimeError("未找到已审批且覆盖 Dataset 区间的市场/费用规则")
        previous: dict[str, date | None] = {}
        rule_rows = []
        for rule in source_rules:
            market = rule[5]
            if market in previous and (previous[market] is None or rule[3] <= previous[market]):
                raise RuntimeError(f"市场规则有效区间重叠: market={market}, effective_from={rule[3]}")
            previous[market] = rule[4]
            rule_rows.append((dataset_id, *rule[:17], _json_param(rule[17] or {}), *rule[18:], hash_rows([rule])))
        execute_values(cur,
            """INSERT INTO backtest.dataset_rule_snapshot
              (dataset_id,rule_version,market_rule_version,fee_rule_version,effective_from,effective_to,
               market,min_buy_lot,allow_sell_odd_lot,t_plus_one,price_tick,cash_settlement_days,limit_pct,
               commission_rate,min_commission,stamp_duty_sell_rate,transfer_fee_rate,slippage_bps,
               parameters,approval_status,approved_by,approved_at,approval_hash,rule_hash) VALUES %s""",
            rule_rows, page_size=1000)

        benchmark_id = boundaries["index"]
        cur.execute(
            """WITH chosen AS (
                   SELECT DISTINCT ON (r.trade_date) r.trade_date,r.ts_code,
                          NULLIF(r.payload->>'close','')::double precision AS close,
                          r.record_id,r.payload_hash
                     FROM tushare_index_daily_raw r
                     JOIN backtest.dataset_member m ON m.dataset_id=%s AND m.research_date=r.trade_date
                    WHERE r.ts_code=%s AND r.record_id <= %s
                    ORDER BY r.trade_date,r.fetched_at DESC,r.record_id DESC
               ) SELECT trade_date,ts_code,close,record_id,payload_hash FROM chosen ORDER BY trade_date""",
            (dataset_id, manifest["benchmark_code"], benchmark_id),
        )
        benchmark_rows = cur.fetchall()
        if len(benchmark_rows) != len(_dataset_dates(cur, dataset_id)):
            raise RuntimeError("基准指数没有覆盖全部 Dataset 日期")
        if any(row[2] is None or float(row[2]) <= 0 for row in benchmark_rows):
            raise RuntimeError("基准指数存在缺失或非正收盘价")
        execute_values(cur,
            """INSERT INTO backtest.dataset_benchmark_bar
              (dataset_id,trade_date,ts_code,close,source_record_key,source_record_version,
               source_payload_hash) VALUES %s""",
            [(dataset_id, day, code, close, f"{code}:{day}", version, payload_hash)
             for day, code, close, version, payload_hash in benchmark_rows], page_size=1000)
        conn.commit()


def _dataset_dates(cur, dataset_id: str) -> list[date]:
    cur.execute("SELECT research_date FROM backtest.dataset_member WHERE dataset_id=%s ORDER BY research_date", (dataset_id,))
    return [row[0] for row in cur.fetchall()]


def _finalize_build(dataset_id: str, manifest: dict[str, Any], metadata: dict[str, Any]) -> dict[str, Any]:
    with get_connection() as conn, conn.cursor() as cur:
        materialized = _materialized_hashes(cur, dataset_id)
        cur.execute(
            """SELECT trade_date, quality_status, COUNT(*)
                 FROM backtest.dataset_execution_bar
                WHERE dataset_id=%s
                GROUP BY trade_date, quality_status""",
            (dataset_id,),
        )
        quality_by_day: dict[date, dict[str, int]] = {}
        for day, status, count in cur.fetchall():
            quality_by_day.setdefault(day, {})[status] = int(count)
        cur.execute(
            """SELECT research_date,snapshot_input_hash FROM backtest.dataset_member
                WHERE dataset_id=%s ORDER BY research_date""", (dataset_id,)
        )
        members = cur.fetchall()
        member_stats = []
        for day, snapshot_hash in members:
            execution = materialized["execution_by_day"].get(day, {"hasher": _IncrementalRowHasher(), "count": 0})
            corporate = hash_rows(materialized["dividend_by_day"].get(day, []))
            cur.execute(
                """UPDATE backtest.dataset_member
                      SET execution_row_count=%s, execution_hash=%s, corporate_action_hash=%s,
                          quality_status=%s
                    WHERE dataset_id=%s AND research_date=%s""",
                (execution["count"], execution["hasher"].hexdigest(), corporate,
                 ("INVALID" if quality_by_day.get(day, {}).get("INVALID", 0) else
                  "MISSING_SOURCE" if quality_by_day.get(day, {}).get("MISSING_SOURCE", 0) else
                  "WARNING" if quality_by_day.get(day, {}).get("WARNING", 0) else "VALID"),
                 dataset_id, day),
            )
            member_stats.append((dataset_id, day, snapshot_hash, execution["hasher"].hexdigest(), corporate, execution["count"]))
        input_hash = hash_rows(member_stats + [
            ("calendar", materialized["calendar_hash"]),
            ("rules", materialized["rules_hash"]),
            ("execution", materialized["execution_hash"]),
            ("corporate_action", materialized["corporate_action_hash"]),
            ("benchmark", materialized["benchmark_hash"]),
            ("baseline_quality_report", manifest["baseline_quality_report_hash"]),
            ("baseline_source_manifest", manifest["baseline_source_manifest_hash"]),
            ("out_of_sample_start", manifest["out_of_sample_start"]),
        ])
        quality = {
            "execution_rows": materialized["execution_by_day"] and sum(v["count"] for v in materialized["execution_by_day"].values()) or 0,
            "calendar_rows": len(materialized["calendar_rows"]),
            "corporate_action_rows": len(materialized["dividend_rows"]),
            "rule_rows": len(materialized["rule_rows"]),
            "benchmark_rows": len(materialized["benchmark_rows"]),
            "member_count": len(members),
            "missing_execution_rows": sum(
                stats.get("MISSING_SOURCE", 0) for stats in quality_by_day.values()
            ),
            "baseline_quality_report_hash": manifest["baseline_quality_report_hash"],
            "baseline_source_manifest_hash": manifest["baseline_source_manifest_hash"],
            "benchmark_code": manifest["benchmark_code"],
            "benchmark_hash": materialized["benchmark_hash"],
            "out_of_sample_start": manifest["out_of_sample_start"],
            "input_hash": input_hash,
            "build_mode": "date_batched_resumable",
        }
        cur.execute(
            """UPDATE backtest.dataset_manifest
                  SET calendar_hash=%s,rules_hash=%s,execution_hash=%s,
                      corporate_action_hash=%s,benchmark_hash=%s,input_manifest_hash=%s,
                      quality_report=%s,quality_report_hash=%s,
                      code_version=%s,git_commit=%s,git_dirty=%s,diff_hash=%s
                WHERE dataset_id=%s""",
            (materialized["calendar_hash"], materialized["rules_hash"], materialized["execution_hash"],
             materialized["corporate_action_hash"], materialized["benchmark_hash"], input_hash,
             _json_param(quality), _quality_report_hash(quality), metadata["code_version"],
             metadata["commit"], metadata["dirty"], metadata["diff_hash"], dataset_id),
        )
        conn.commit()
        quality["input_hash"] = input_hash
        quality["execution_rows"] = materialized["execution_by_day"] and sum(
            item["count"] for item in materialized["execution_by_day"].values()
        ) or 0
        return quality


def build_dataset_batched(
    dataset_id: str,
    *,
    batch_days: int = 5,
    force_rebuild: bool = False,
) -> dict[str, Any]:
    """Build a Dataset with bounded transactions and resumable date batches."""
    if batch_days < 1 or batch_days > 60:
        raise ValueError("batch_days 必须在 1..60 之间")
    manifest, boundaries, metadata = _prepare_manifest(dataset_id)
    with get_connection() as conn:
        batches = _trading_batches(conn, dataset_id, batch_days)
    if force_rebuild:
        with get_connection() as conn, conn.cursor() as cur:
            cur.execute("DELETE FROM backtest.dataset_build_batch_checkpoint WHERE dataset_id=%s", (dataset_id,))
            conn.commit()
    results = []
    for batch_index, (batch_start, batch_end, _days) in enumerate(batches, start=1):
        with get_connection() as conn, conn.cursor() as cur:
            checkpoint = _batch_checkpoint(cur, dataset_id, batch_start)
        if (
            not force_rebuild
            and checkpoint
            and checkpoint[0] == "COMPLETED"
            and checkpoint[1] == batch_end
            and checkpoint[4] == metadata["code_version"]
            and checkpoint[3]
        ):
            results.append({"batch_start": batch_start, "batch_end": batch_end, "rows": checkpoint[2], "skipped": True})
            print(
                f"BUILD batch={batch_index}/{len(batches)} {batch_start}..{batch_end} "
                f"rows={checkpoint[2]} skipped=true",
                flush=True,
            )
            _set_checkpoint(
                dataset_id, "BUILD", "RUNNING", progress_done=batch_index,
                progress_total=len(batches), last_trade_date=batch_end,
                details={"mode": "date_batched_resumable", "batch_days": batch_days},
            )
            continue
        result = _build_execution_batch(
            dataset_id, batch_start, batch_end, boundaries, metadata["code_version"]
        )
        results.append(result)
        print(
            f"BUILD batch={batch_index}/{len(batches)} {batch_start}..{batch_end} "
            f"rows={result['rows']} skipped=false",
            flush=True,
        )
        _set_checkpoint(
            dataset_id, "BUILD", "RUNNING", progress_done=batch_index,
            progress_total=len(batches), last_trade_date=batch_end,
            details={"mode": "date_batched_resumable", "batch_days": batch_days},
        )
    _rebuild_auxiliary(dataset_id, manifest, boundaries)
    result = _finalize_build(dataset_id, manifest, metadata)
    result.update({"batch_days": batch_days, "batches": len(batches), "skipped_batches": sum(1 for item in results if item.get("skipped"))})
    return result
