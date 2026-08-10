"""Research 快照 Manifest、构建、校验和冻结。

第一版快照粒度固定为：一个 ``snapshot_id`` 对应一个 ``research_cutoff_date``。
生成器只读取通过质量门禁的 PIT/Feature，使用 PostgreSQL REPEATABLE READ
事务读取一致输入；Research/Backtest 后续只能使用 FROZEN 快照。
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import subprocess
import uuid
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

from data_collect.backtest.validation import verify_pit_cutoff_consistency
from data_collect.ops.market_coverage import (
    assert_market_coverage_publishable,
    assert_market_coverage_range_publishable,
)
from data_collect.utils.db import get_connection, require_psycopg2


SCHEMA_VERSION = "research-v1.3"
LEGACY_V12_SCHEMA_VERSION = "research-v1.2"
REQUIRED_LINEAGE_GROUPS = (
    "universe", "tradeability", "price", "valuation", "financial",
    "shareholder_return", "lifecycle", "instrument", "market_facts",
    "fundamental_history",
)

_SNAPSHOT_BATCH_LOCK_NAME = "research_snapshot_batch_v1"


def _acquire_snapshot_batch_lock(cur) -> None:
    """Prevent concurrent builders/validators from touching the same series."""
    cur.execute(
        "SELECT pg_try_advisory_xact_lock(hashtextextended(%s, 0))",
        (_SNAPSHOT_BATCH_LOCK_NAME,),
    )
    if not cur.fetchone()[0]:
        raise RuntimeError(
            "已有另一个 Research Snapshot 批处理任务正在运行，当前任务安全退出"
        )

# 这些表名和字段均为代码内固定定义，不接受命令行直接拼接，避免把哈希范围变成隐式变化。
INPUTS = (
    {
        "schema": "pit", "table": "universe_daily",
        "columns": ("trade_date", "ts_code", "is_listed_asof", "is_st_asof", "is_suspended_asof", "has_price_history", "eligible_by_market", "quality_status", "source_asof_date"),
        "predicate": "trade_date = :research_cutoff_date",
        "where": "trade_date = %s", "params": lambda cutoff: (cutoff,),
        "order": "trade_date, ts_code", "date_column": "trade_date",
    },
    {
        "schema": "pit", "table": "security_tradeability_daily",
        "columns": ("trade_date", "ts_code", "is_tradeable", "can_buy", "can_sell", "is_suspended", "limit_status", "quality_status", "source_asof_date"),
        "predicate": "trade_date = :research_cutoff_date",
        "where": "trade_date = %s", "params": lambda cutoff: (cutoff,),
        "order": "trade_date, ts_code", "date_column": "trade_date",
    },
    {
        "schema": "feature", "table": "price_daily_v2",
        "columns": ("ts_code", "trade_date", "raw_close", "asof_close", "asof_adjustment_factor", "return_1d", "return_20d", "return_60d", "ma20", "ma60", "ma120", "volume_ratio_20d", "available_observation_count", "ma20_observation_count", "ma60_observation_count", "ma120_observation_count", "feature_set_version", "adjustment_mode", "quality_status", "source_asof_date"),
        "predicate": "trade_date = :research_cutoff_date",
        "where": "trade_date = %s", "params": lambda cutoff: (cutoff,),
        "order": "trade_date, ts_code", "date_column": "trade_date",
    },
    {
        "schema": "feature", "table": "valuation_daily_v1",
        "columns": ("ts_code", "trade_date", "pe_ttm", "pb", "ps_ttm", "dv_ratio", "total_mv", "circ_mv", "pe_pct_3y", "pe_pct_5y", "pb_pct_3y", "pb_pct_5y", "is_pe_meaningful", "valuation_observation_count", "pe_window_start_date_5y", "pe_observation_count_5y", "has_full_pe_5y_history", "quality_status", "source_asof_date"),
        "predicate": "trade_date = :research_cutoff_date",
        "where": "trade_date = %s", "params": lambda cutoff: (cutoff,),
        "order": "trade_date, ts_code", "date_column": "trade_date",
    },
    {
        "schema": "pit", "table": "financial_asof",
        "columns": ("decision_date", "ts_code", "report_date", "effective_announce_date", "version_available_date", "vintage_quality", "source_observed_at", "record_version", "source_record_key", "roe", "roa", "debt_to_assets", "revenue_yoy", "tr_yoy", "q_sales_yoy", "netprofit_yoy", "quality_status", "payload_hash"),
        "predicate": "decision_date = :research_cutoff_date",
        "where": "decision_date = %s", "params": lambda cutoff: (cutoff,),
        "order": "decision_date, ts_code", "date_column": "decision_date",
    },
    {
        "schema": "public", "table": "daily_kline",
        "columns": ("stock_code", "trade_date", "open", "high", "low", "close", "volume", "amount"),
        "predicate": "trade_date = :research_cutoff_date",
        "where": "trade_date = %s", "params": lambda cutoff: (cutoff,),
        "order": "trade_date, stock_code", "date_column": "trade_date",
    },
    {
        "schema": "std", "table": "security_status_daily",
        "columns": ("trade_date", "ts_code", "up_limit", "down_limit", "close_price", "limit_status", "quality_status"),
        "predicate": "trade_date = :research_cutoff_date",
        "where": "trade_date = %s", "params": lambda cutoff: (cutoff,),
        "order": "trade_date, ts_code", "date_column": "trade_date",
    },
    {
        "schema": "feature", "table": "shareholder_return_v1",
        "columns": ("ts_code", "asof_date", "cash_div_12m", "dividend_event_count_3y", "dividend_year_count_3y", "quality_status"),
        "predicate": "asof_date = :research_cutoff_date",
        "where": "asof_date = %s", "params": lambda cutoff: (cutoff,),
        "order": "asof_date, ts_code", "date_column": "asof_date",
    },
    {
        "schema": "public", "table": "tushare_stock_basic_raw_version",
        "columns": ("record_id", "ts_code", "business_key", "payload", "payload_hash", "fetched_at"),
        "predicate": "record_id <= :baseline_stock_basic_max_record_id",
        "where": "record_id <= %s",
        "params_from_manifest": lambda cutoff, manifest: (
            manifest["append_only_sources"]["tushare_stock_basic_raw_version"]["max_record_id"],
        ),
        "order": "record_id", "date_column": None,
    },
    {
        "schema": "std", "table": "security_lifecycle",
        "columns": ("ts_code", "name", "market", "list_status", "effective_from", "effective_to", "source", "source_record_key", "record_version", "quality_status"),
        "predicate": "effective_from <= :research_cutoff_date AND (effective_to IS NULL OR effective_to >= :research_cutoff_date)",
        "where": "effective_from <= %s AND (effective_to IS NULL OR effective_to >= %s)", "params": lambda cutoff: (cutoff, cutoff),
        "order": "ts_code, effective_from, effective_to", "date_column": "effective_from",
    },
)


def _qualified(schema: str, table: str) -> str:
    return f'"{schema}"."{table}"'


def _canonical(value):
    if value is None:
        return None
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, (list, tuple)):
        return [_canonical(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _canonical(item) for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))}
    return value


def _row_bytes(row) -> bytes:
    return (json.dumps([_canonical(value) for value in row], ensure_ascii=False,
                       sort_keys=False, separators=(",", ":"), allow_nan=False).encode("utf-8") + b"\n")


def _input_stats(conn, definition: dict, cutoff: date, source_manifest: dict | None = None) -> dict:
    columns = definition["columns"]
    qualified = _qualified(definition["schema"], definition["table"])
    where = definition["where"]
    if "params_from_manifest" in definition:
        if not source_manifest:
            raise RuntimeError(
                f"{definition['schema']}.{definition['table']} 缺少基线来源 Manifest"
            )
        params = definition["params_from_manifest"](cutoff, source_manifest)
    else:
        params = definition["params"](cutoff)
    date_column = definition["date_column"]
    with conn.cursor() as cur:
        if date_column:
            cur.execute(
                f'SELECT COUNT(*), MIN("{date_column}"), MAX("{date_column}") FROM {qualified} WHERE {where}',
                params,
            )
            row_count, min_date, max_date = cur.fetchone()
        else:
            cur.execute(f'SELECT COUNT(*) FROM {qualified} WHERE {where}', params)
            row_count, min_date, max_date = cur.fetchone()[0], None, None

        digest = hashlib.sha256()
        selected = ", ".join(f'"{column}"' for column in columns)
        cur.execute(
            f'SELECT {selected} FROM {qualified} WHERE {where} ORDER BY {definition["order"]}',
            params,
        )
        for row in cur:
            digest.update(_row_bytes(row))
    return {
        "schema_name": definition["schema"], "table_name": definition["table"],
        "selection_predicate": definition["predicate"], "selected_columns": list(columns),
        "hash_algorithm": "sha256", "row_count": int(row_count),
        "min_date": min_date, "max_date": max_date, "content_hash": digest.hexdigest(),
    }


def _manifest_hash(stats: list[dict]) -> str:
    payload = []
    for item in stats:
        payload.append({key: _canonical(value) for key, value in item.items() if key not in {"created_at"}})
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _git_metadata() -> dict:
    root = Path(__file__).resolve().parents[2]
    try:
        commit = subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=root, text=True
        ).strip()
        status = subprocess.check_output(
            ["git", "status", "--porcelain", "--untracked-files=all"],
            cwd=root, text=True,
        )
        dirty = bool(status.strip())
        diff_hash = None
        if dirty:
            digest = hashlib.sha256()
            digest.update(subprocess.check_output(
                ["git", "diff", "--binary", "HEAD"], cwd=root
            ))
            untracked = subprocess.check_output(
                ["git", "ls-files", "--others", "--exclude-standard", "-z"],
                cwd=root,
            )
            for raw_path in untracked.split(b"\0"):
                if not raw_path:
                    continue
                path = root / raw_path.decode("utf-8", "surrogateescape")
                digest.update(raw_path)
                digest.update(b"\0")
                digest.update(path.read_bytes())
            diff_hash = digest.hexdigest()
        return {
            "commit": commit,
            "dirty": dirty,
            "diff_hash": diff_hash,
            "code_version": commit + ("+dirty" if dirty else ""),
        }
    except (OSError, subprocess.CalledProcessError):
        return {
            "commit": None, "dirty": True,
            "diff_hash": "unavailable", "code_version": "unknown+dirty",
        }


def _sha256_file(path: str | Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            size += len(chunk)
            digest.update(chunk)
    return digest.hexdigest(), size


def _quality_report(cutoff: date | None = None) -> tuple[dict, str]:
    from data_collect.jobs.tushare_release import _quality_gate

    report = _quality_gate(cutoff)
    encoded = json.dumps(
        _canonical(report), ensure_ascii=False, sort_keys=True,
        separators=(",", ":"), allow_nan=False,
    )
    return report, hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _assert_adj_factor_vintage(conn, cutoff: date) -> None:
    """Reject adjusted factors first observed after the research cutoff.

    Price features use ``adj_factor_daily``.  Because the current model does
    not store an external announcement timestamp for each factor, the safe
    rule is to require warehouse observation no later than the cutoff for the
    technical warm-up window.  Missing or late factors fail closed instead of
    silently using a later final value.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*)
              FROM daily_kline d
              LEFT JOIN adj_factor_daily a
                ON a.stock_code = d.stock_code
               AND a.trade_date = d.trade_date
               AND a.source = 'tushare'
             WHERE d.trade_date BETWEEN (%s - INTERVAL '365 days')::date AND %s
               AND (a.adj_factor IS NULL OR a.fetched_at::date > %s)
            """,
            (cutoff, cutoff, cutoff),
        )
        invalid_rows = int(cur.fetchone()[0] or 0)
    if invalid_rows:
        raise RuntimeError(
            "adj_factor vintage unavailable/future-observed for technical window: "
            f"cutoff={cutoff}, invalid_rows={invalid_rows}; "
            "pre-cutoff adjusted-factor vintages are required before building a formal snapshot"
        )


def _assert_price_vintage_for_schema(conn, cutoff: date, schema_version: str) -> None:
    """Apply the correct price-input gate for each Research schema."""
    if schema_version == LEGACY_V12_SCHEMA_VERSION:
        _assert_adj_factor_vintage(conn, cutoff)
        return
    if schema_version != SCHEMA_VERSION:
        return
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                (SELECT COUNT(*) FROM daily_kline WHERE trade_date <= %s),
                (SELECT COUNT(*) FROM feature.price_daily_v2 WHERE trade_date <= %s),
                (SELECT COUNT(*) FROM feature.price_adjustment_factor_asof
                   WHERE trade_date <= %s),
                (SELECT COUNT(*) FROM feature.price_daily_v2
                   WHERE trade_date <= %s AND quality_status = 'MISSING_SOURCE')
            """,
            (cutoff, cutoff, cutoff, cutoff),
        )
        daily_rows, price_rows, factor_rows, invalid_rows = cur.fetchone()
    if daily_rows != price_rows or daily_rows != factor_rows or invalid_rows:
        raise RuntimeError(
            "point-in-time price Feature is incomplete or invalid: "
            f"cutoff={cutoff}, daily={daily_rows}, price_v2={price_rows}, "
            f"factor_asof={factor_rows}, missing_source={invalid_rows}"
        )


def _assert_price_vintage_for_dates(conn, cutoffs: list[date], schema_version: str) -> None:
    """Check exact-date price coverage for a batch without repeated history scans."""
    if not cutoffs:
        return
    if schema_version == LEGACY_V12_SCHEMA_VERSION:
        for cutoff in cutoffs:
            _assert_adj_factor_vintage(conn, cutoff)
        return
    if schema_version != SCHEMA_VERSION:
        return
    with conn.cursor() as cur:
        cur.execute(
            """
            WITH dates(trade_date) AS (SELECT unnest(%s::date[])),
            daily AS (
                SELECT trade_date, COUNT(*) AS n FROM daily_kline
                 WHERE trade_date = ANY(%s::date[]) GROUP BY trade_date
            ), prices AS (
                SELECT trade_date, COUNT(*) AS n FROM feature.price_daily_v2
                 WHERE trade_date = ANY(%s::date[]) GROUP BY trade_date
            ), factors AS (
                SELECT trade_date, COUNT(*) AS n FROM feature.price_adjustment_factor_asof
                 WHERE trade_date = ANY(%s::date[]) GROUP BY trade_date
            ), invalid AS (
                SELECT trade_date, COUNT(*) AS n FROM feature.price_daily_v2
                 WHERE trade_date = ANY(%s::date[]) AND quality_status='MISSING_SOURCE'
                 GROUP BY trade_date
            )
            SELECT d.trade_date, COALESCE(x.n,0), COALESCE(p.n,0),
                   COALESCE(f.n,0), COALESCE(i.n,0)
              FROM dates d
              LEFT JOIN daily x USING (trade_date)
              LEFT JOIN prices p USING (trade_date)
              LEFT JOIN factors f USING (trade_date)
              LEFT JOIN invalid i USING (trade_date)
             WHERE COALESCE(x.n,0) <> COALESCE(p.n,0)
                OR COALESCE(x.n,0) <> COALESCE(f.n,0)
                OR COALESCE(i.n,0) <> 0
            """,
            (list(cutoffs), list(cutoffs), list(cutoffs), list(cutoffs), list(cutoffs)),
        )
        invalid_rows = cur.fetchall()
    if invalid_rows:
        raise RuntimeError(f"point-in-time price batch coverage invalid: {invalid_rows[:10]}")


def _baseline(cutoff: date, tag: str, connection=None):
    """确认基线仍可用；传入连接时与当前快照事务使用同一数据库会话。"""
    if connection is None:
        with get_connection() as conn:
            return _baseline(cutoff, tag, conn)
    with connection.cursor() as cur:
        cur.execute("""SELECT release_tag, baseline_cutoff_date, status,
                               quality_report_hash, source_manifest_hash,
                               source_manifest
                       FROM tushare_data_release WHERE release_tag=%s""", (tag,))
        row = cur.fetchone()
    if not row or row[2] != "validated":
        raise RuntimeError(f"数据基线必须是 validated: {tag}")
    if cutoff > row[1]:
        raise ValueError(f"research_cutoff_date {cutoff} 晚于数据基线截止日 {row[1]}")
    if not row[3]:
        raise RuntimeError(f"数据基线缺少质量报告哈希: {tag}")
    if not row[4] or not row[5]:
        raise RuntimeError(f"数据基线缺少 Raw 来源 Manifest: {tag}")
    # Release 之后如果同一截止区间又追加了修订 Raw，旧 release 只能作为
    # 历史审计记录，不能继续生成新的 Snapshot。必须先重建派生层并发布新基线。
    from data_collect.jobs.tushare_release import _source_manifest

    _, current_source_hash = _source_manifest(connection, row[1])
    if current_source_hash != row[4]:
        raise RuntimeError(
            f"数据基线 Raw 来源边界已变化，必须重建派生层并发布新基线: {tag}"
        )
    return row


def create(cutoff: str, baseline_tag: str, schema_version: str = SCHEMA_VERSION, notes: str = "") -> str:
    cutoff_date = date.fromisoformat(cutoff)
    baseline = _baseline(cutoff_date, baseline_tag)
    with get_connection() as vintage_conn:
        _assert_price_vintage_for_schema(vintage_conn, cutoff_date, schema_version)
    quality_report, quality_hash = _quality_report(cutoff_date)
    if quality_report["failures"]:
        raise RuntimeError(f"质量门禁未通过，不能创建 Research 快照: {quality_report['failures']}")
    metadata = _git_metadata()
    snapshot_id = str(uuid.uuid4())
    psycopg2, _ = require_psycopg2()
    Json = psycopg2.extras.Json
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute("""INSERT INTO research.snapshot_manifest
            (snapshot_id, research_cutoff_date, data_baseline_tag, schema_version,
             code_version, git_commit, git_dirty, diff_hash, quality_report_hash,
             baseline_quality_report_hash, baseline_source_manifest_hash,
             quality_report, status, notes)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'DRAFT',%s)""",
                    (snapshot_id, cutoff_date, baseline_tag, schema_version,
                     metadata["code_version"], metadata["commit"], metadata["dirty"],
                     metadata["diff_hash"], quality_hash, baseline[3], baseline[4],
                     Json(quality_report), notes))
        conn.commit()
    return snapshot_id


def existing_snapshot(cutoff: str, baseline_tag: str, schema_version: str = SCHEMA_VERSION):
    """Return the existing snapshot for a logical key, if any.

    A snapshot is logically identified by cutoff date, data baseline and schema
    version.  The unique database index is the final guard; this read helper
    lets a multi-date orchestration task resume instead of creating duplicates.
    """
    cutoff_date = date.fromisoformat(cutoff)
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """SELECT snapshot_id, status
                 FROM research.snapshot_manifest
                WHERE research_cutoff_date=%s
                  AND data_baseline_tag=%s
                  AND schema_version=%s""",
            (cutoff_date, baseline_tag, schema_version),
        )
        row = cur.fetchone()
    return (str(row[0]), row[1]) if row else None


def _manifest(conn, snapshot_id: str, *, for_update: bool = False):
    with conn.cursor() as cur:
        query = """SELECT snapshot_id, research_cutoff_date, data_baseline_tag,
               schema_version, status, code_version, git_commit, git_dirty,
               diff_hash, code_archive_path, code_archive_sha256,
               code_archive_size_bytes, quality_report_hash,
               baseline_quality_report_hash, baseline_source_manifest_hash,
               input_manifest_hash
               FROM research.snapshot_manifest WHERE snapshot_id=%s"""
        if for_update:
            query += " FOR UPDATE"
        cur.execute(query, (snapshot_id,))
        row = cur.fetchone()
    if not row:
        raise ValueError(f"不存在的 snapshot_id: {snapshot_id}")
    return dict(zip(("snapshot_id", "cutoff", "baseline", "schema_version", "status",
                     "code_version", "git_commit", "git_dirty", "diff_hash",
                     "code_archive_path", "code_archive_sha256", "code_archive_size",
                     "quality_hash", "baseline_quality_hash",
                     "baseline_source_manifest_hash", "input_hash"), row))


def _stock_snapshot_sql():
    return """
    INSERT INTO research.stock_snapshot_daily
    (snapshot_id, research_date, ts_code, symbol, name, market, list_status, list_date, delist_date,
     is_listed_asof, is_st_asof, is_suspended_asof, eligible_by_market, is_tradeable, can_buy, can_sell, limit_status,
     raw_open, raw_high, raw_low, raw_close, raw_volume, raw_amount, up_limit, down_limit,
     adj_close_base, price_feature_version, price_adjustment_mode,
     return_1d, return_20d, return_60d, ma20, ma60, ma120, volume_ratio_20d,
     available_observation_count, ma20_observation_count, ma60_observation_count, ma120_observation_count,
     pe_ttm, pb, ps_ttm, dv_ratio, total_mv, circ_mv, pe_pct_3y, pe_pct_5y, pb_pct_3y, pb_pct_5y,
     is_pe_meaningful, valuation_observation_count, pe_window_start_date_5y,
     has_full_pe_5y_history, report_date, effective_announce_date,
     financial_version_available_date, financial_record_version,
     financial_source_record_key, financial_payload_hash, financial_vintage_quality,
     financial_source_observed_at,
     roe, roa, debt_to_assets, revenue_yoy,
     tr_yoy, q_sales_yoy, netprofit_yoy, operating_cashflow, net_profit, cash_div_12m,
     dividend_event_count_3y, dividend_year_count_3y,
     annual_report_count_5y, roe_above_15_years_5y, roe_5y_min,
     roe_5y_all_above_15, ocf_report_count_3y, ocf_positive_years_3y,
     ocf_3y_all_positive, quarterly_report_count_4q,
     q_sales_yoy_above_30_quarters_4q, q_sales_yoy_4q_all_above_30,
     instrument_source_record_key, instrument_source_record_version,
     instrument_source_payload_hash,
     quality_status)
     SELECT %s, u.trade_date, u.ts_code, b.payload->>'symbol',
            COALESCE(l.name, b.payload->>'name'),
            COALESCE(l.market, b.payload->>'market'),
            CASE
              WHEN to_date(NULLIF(b.payload->>'list_date',''), 'YYYYMMDD') > u.trade_date
              THEN 'N'
              WHEN u.is_listed_asof THEN 'L'
              ELSE 'D'
            END,
            CASE
              WHEN to_date(NULLIF(b.payload->>'list_date',''), 'YYYYMMDD') <= u.trade_date
              THEN to_date(NULLIF(b.payload->>'list_date',''), 'YYYYMMDD')
              ELSE NULL
            END,
            CASE
              WHEN to_date(NULLIF(b.payload->>'delist_date',''), 'YYYYMMDD') <= u.trade_date
              THEN to_date(NULLIF(b.payload->>'delist_date',''), 'YYYYMMDD')
              ELSE NULL
            END,
           u.is_listed_asof, u.is_st_asof, u.is_suspended_asof, u.eligible_by_market,
           t.is_tradeable, t.can_buy, t.can_sell, t.limit_status,
           d.open, d.high, d.low, p.raw_close, d.volume, d.amount,
           st.up_limit, st.down_limit,
            p.asof_close, p.feature_set_version, p.adjustment_mode,
            p.return_1d, p.return_20d, p.return_60d,
           p.ma20, p.ma60, p.ma120, p.volume_ratio_20d,
           p.available_observation_count, p.ma20_observation_count,
           p.ma60_observation_count, p.ma120_observation_count,
           v.pe_ttm, v.pb, v.ps_ttm, v.dv_ratio, v.total_mv, v.circ_mv,
           v.pe_pct_3y, v.pe_pct_5y, v.pb_pct_3y, v.pb_pct_5y,
           v.is_pe_meaningful, v.valuation_observation_count,
           v.pe_window_start_date_5y, COALESCE(v.has_full_pe_5y_history, FALSE),
            cf.report_date, cf.effective_announce_date, cf.version_available_date,
            cf.record_version,
            cf.source_record_key, cf.payload_hash, cf.vintage_quality,
            cf.source_observed_at,
            cf.roe, cf.roa, cf.debt_to_assets,
            cf.revenue_yoy, cf.tr_yoy, cf.q_sales_yoy, cf.netprofit_yoy,
           cf.operating_cashflow, cf.net_profit,
           sr.cash_div_12m, sr.dividend_event_count_3y, sr.dividend_year_count_3y,
           COALESCE(fh.annual_report_count_5y, 0),
           COALESCE(fh.roe_above_15_years_5y, 0), fh.roe_5y_min,
           COALESCE(fh.roe_5y_all_above_15, FALSE),
           COALESCE(fh.ocf_report_count_3y, 0),
           COALESCE(fh.ocf_positive_years_3y, 0),
           COALESCE(fh.ocf_3y_all_positive, FALSE),
           COALESCE(fh.quarterly_report_count_4q, 0),
           COALESCE(fh.q_sales_yoy_above_30_quarters_4q, 0),
           COALESCE(fh.q_sales_yoy_4q_all_above_30, FALSE),
           b.business_key, b.record_id, b.payload_hash,
           CASE WHEN 'MISSING_SOURCE' = ANY(ARRAY[
                    COALESCE(u.quality_status,'MISSING_SOURCE'), COALESCE(t.quality_status,'MISSING_SOURCE'),
                    COALESCE(p.quality_status,'MISSING_SOURCE'), COALESCE(v.quality_status,'MISSING_SOURCE'),
                    COALESCE(cf.quality_status,'MISSING_SOURCE'), COALESCE(sr.quality_status,'MISSING_SOURCE')])
                THEN 'MISSING_SOURCE'
                WHEN 'WARNING' = ANY(ARRAY[
                    COALESCE(u.quality_status,'WARNING'), COALESCE(t.quality_status,'WARNING'),
                    COALESCE(p.quality_status,'WARNING'), COALESCE(v.quality_status,'WARNING'),
                    COALESCE(cf.quality_status,'WARNING'), COALESCE(sr.quality_status,'WARNING')])
                THEN 'WARNING' ELSE 'VALID' END
    FROM pit.universe_daily u
    JOIN LATERAL (
      SELECT record_id, business_key, payload, payload_hash
      FROM tushare_stock_basic_raw_version b
      WHERE b.ts_code=u.ts_code AND b.record_id <= %s
      ORDER BY b.fetched_at DESC, b.record_id DESC LIMIT 1
    ) b ON TRUE
    LEFT JOIN pit.security_tradeability_daily t
      ON t.trade_date=u.trade_date AND t.ts_code=u.ts_code
     LEFT JOIN feature.price_daily_v2 p
      ON p.trade_date=u.trade_date AND p.ts_code=u.ts_code
    LEFT JOIN LATERAL (
      SELECT d.open, d.high, d.low, d.close, d.volume, d.amount
      FROM daily_kline d
      WHERE d.trade_date=u.trade_date AND d.stock_code=u.ts_code
      LIMIT 1
    ) d ON TRUE
    LEFT JOIN std.security_status_daily st
      ON st.trade_date=u.trade_date AND st.ts_code=u.ts_code
    LEFT JOIN feature.valuation_daily_v1 v
      ON v.trade_date=u.trade_date AND v.ts_code=u.ts_code
    LEFT JOIN feature.shareholder_return_v1 sr
      ON sr.asof_date=u.trade_date AND sr.ts_code=u.ts_code
    LEFT JOIN LATERAL (
       SELECT l.name, l.market FROM std.security_lifecycle l
      WHERE l.ts_code=u.ts_code AND l.effective_from <= u.trade_date
        AND (l.effective_to IS NULL OR l.effective_to >= u.trade_date)
      ORDER BY l.effective_from DESC, l.lifecycle_id DESC LIMIT 1
    ) l ON TRUE
    LEFT JOIN LATERAL (
      SELECT c.report_date, c.effective_announce_date, c.version_available_date,
             c.record_version, c.source_record_key, c.payload_hash,
             c.roe, c.roa, c.debt_to_assets, c.revenue_yoy, c.tr_yoy,
             c.q_sales_yoy, c.netprofit_yoy, c.operating_cashflow, c.net_profit,
             c.vintage_quality, c.source_observed_at, c.quality_status
      FROM feature.company_financial_v1 c
      WHERE c.ts_code=u.ts_code AND c.version_available_date IS NOT NULL
        AND c.version_available_date <= u.trade_date
      ORDER BY c.report_date DESC, c.version_available_date DESC,
               c.record_version DESC, c.feature_id DESC LIMIT 1
    ) cf ON TRUE
    LEFT JOIN LATERAL (
      WITH visible AS (
        SELECT DISTINCT ON (c.report_date)
               c.report_date, c.roe, c.operating_cashflow, c.q_sales_yoy
        FROM feature.company_financial_v1 c
        WHERE c.ts_code=u.ts_code
          AND c.version_available_date IS NOT NULL
          AND c.version_available_date <= u.trade_date
        ORDER BY c.report_date, c.version_available_date DESC,
                 c.record_version DESC, c.feature_id DESC
      ), annual5 AS (
        SELECT * FROM visible
        WHERE EXTRACT(MONTH FROM report_date)=12
          AND EXTRACT(DAY FROM report_date)=31
        ORDER BY report_date DESC LIMIT 5
      ), annual3 AS (
        SELECT * FROM annual5 ORDER BY report_date DESC LIMIT 3
      ), quarter4 AS (
        SELECT * FROM visible ORDER BY report_date DESC LIMIT 4
      )
      SELECT
        (SELECT COUNT(*)::integer FROM annual5) AS annual_report_count_5y,
        (SELECT COUNT(*) FILTER (WHERE roe > 15)::integer FROM annual5) AS roe_above_15_years_5y,
        (SELECT MIN(roe) FROM annual5) AS roe_5y_min,
        COALESCE((SELECT COUNT(*)=5 AND COUNT(roe)=5
                         AND MAX(EXTRACT(YEAR FROM report_date))
                             - MIN(EXTRACT(YEAR FROM report_date))=4
                         AND BOOL_AND(roe > 15)
                  FROM annual5), FALSE) AS roe_5y_all_above_15,
        (SELECT COUNT(*)::integer FROM annual3) AS ocf_report_count_3y,
        (SELECT COUNT(*) FILTER (WHERE operating_cashflow > 0)::integer FROM annual3) AS ocf_positive_years_3y,
        COALESCE((SELECT COUNT(*)=3 AND COUNT(operating_cashflow)=3
                         AND MAX(EXTRACT(YEAR FROM report_date))
                             - MIN(EXTRACT(YEAR FROM report_date))=2
                         AND BOOL_AND(operating_cashflow > 0)
                  FROM annual3), FALSE) AS ocf_3y_all_positive,
        (SELECT COUNT(*)::integer FROM quarter4) AS quarterly_report_count_4q,
        (SELECT COUNT(*) FILTER (WHERE q_sales_yoy > 30)::integer FROM quarter4) AS q_sales_yoy_above_30_quarters_4q,
        COALESCE((SELECT COUNT(*)=4 AND COUNT(q_sales_yoy)=4
                         AND MAX(EXTRACT(YEAR FROM report_date)::integer * 4
                                 + EXTRACT(QUARTER FROM report_date)::integer)
                             - MIN(EXTRACT(YEAR FROM report_date)::integer * 4
                                   + EXTRACT(QUARTER FROM report_date)::integer)=3
                         AND BOOL_AND(q_sales_yoy > 30)
                  FROM quarter4), FALSE) AS q_sales_yoy_4q_all_above_30
    ) fh ON TRUE
    WHERE u.trade_date=%s
    ON CONFLICT (snapshot_id, research_date, ts_code) DO NOTHING
    """


def _lineage_sql():
    return """
    INSERT INTO research.snapshot_lineage
      (snapshot_id, research_date, ts_code, field_group, source_table,
       source_record_key, source_record_version, source_payload_hash)
    WITH params AS (SELECT %s::uuid AS snapshot_id), groups AS (
        SELECT s.snapshot_id, s.research_date, s.ts_code, g.field_group
        FROM research.stock_snapshot_daily s
        CROSS JOIN LATERAL unnest(ARRAY[
            'universe','tradeability','price','valuation','financial',
            'shareholder_return','lifecycle','instrument','market_facts',
            'fundamental_history'
        ]::varchar[]) AS g(field_group)
        WHERE s.snapshot_id=(SELECT snapshot_id FROM params)
    ), refs AS (
        -- PIT/Feature v1 记录没有独立的版本列；1 表示对应的 v1 派生记录，
        -- 哈希只覆盖本快照实际读取的字段，避免把计算时间等非事实列纳入血缘。
        SELECT s.snapshot_id, s.research_date, s.ts_code, 'universe'::varchar AS field_group,
               'pit.universe_daily'::varchar AS source_table,
               s.research_date::text || ':' || s.ts_code AS source_record_key,
               1::bigint AS source_record_version,
               encode(digest(convert_to(jsonb_build_object(
                   'trade_date', u.trade_date, 'ts_code', u.ts_code,
                   'is_listed_asof', u.is_listed_asof, 'is_st_asof', u.is_st_asof,
                   'is_suspended_asof', u.is_suspended_asof,
                   'eligible_by_market', u.eligible_by_market,
                   'has_price_history', u.has_price_history,
                   'quality_status', u.quality_status,
                   'source_asof_date', u.source_asof_date
               )::text, 'UTF8'), 'sha256'), 'hex')::char(64) AS source_payload_hash
        FROM research.stock_snapshot_daily s
        JOIN pit.universe_daily u ON u.trade_date=s.research_date AND u.ts_code=s.ts_code
        WHERE s.snapshot_id=(SELECT snapshot_id FROM params)
        UNION ALL
        SELECT s.snapshot_id, s.research_date, s.ts_code, 'tradeability',
               'pit.security_tradeability_daily', s.research_date::text || ':' || s.ts_code,
               1::bigint,
               encode(digest(convert_to(jsonb_build_object(
                   'trade_date', t.trade_date, 'ts_code', t.ts_code,
                   'is_tradeable', t.is_tradeable, 'can_buy', t.can_buy,
                   'can_sell', t.can_sell, 'is_suspended', t.is_suspended,
                   'limit_status', t.limit_status, 'quality_status', t.quality_status,
                   'source_asof_date', t.source_asof_date
               )::text, 'UTF8'), 'sha256'), 'hex')::char(64)
        FROM research.stock_snapshot_daily s
        JOIN pit.security_tradeability_daily t ON t.trade_date=s.research_date AND t.ts_code=s.ts_code
        WHERE s.snapshot_id=(SELECT snapshot_id FROM params)
        UNION ALL
        SELECT s.snapshot_id, s.research_date, s.ts_code, 'price',
                'feature.price_daily_v2', p.ts_code || ':' || p.trade_date::text,
               1::bigint,
               encode(digest(convert_to(jsonb_build_object(
                   'ts_code', p.ts_code, 'trade_date', p.trade_date,
                    'raw_close', p.raw_close, 'asof_close', p.asof_close,
                    'asof_adjustment_factor', p.asof_adjustment_factor,
                   'return_1d', p.return_1d, 'return_20d', p.return_20d,
                   'return_60d', p.return_60d, 'ma20', p.ma20, 'ma60', p.ma60,
                   'ma120', p.ma120, 'volume_ratio_20d', p.volume_ratio_20d,
                   'available_observation_count', p.available_observation_count,
                   'ma20_observation_count', p.ma20_observation_count,
                   'ma60_observation_count', p.ma60_observation_count,
                   'ma120_observation_count', p.ma120_observation_count,
                    'feature_set_version', p.feature_set_version,
                    'adjustment_mode', p.adjustment_mode,
                    'quality_status', p.quality_status, 'source_asof_date', p.source_asof_date
               )::text, 'UTF8'), 'sha256'), 'hex')::char(64)
        FROM research.stock_snapshot_daily s
         JOIN feature.price_daily_v2 p ON p.trade_date=s.research_date AND p.ts_code=s.ts_code
        WHERE s.snapshot_id=(SELECT snapshot_id FROM params)
        UNION ALL
        SELECT s.snapshot_id, s.research_date, s.ts_code, 'valuation',
               'feature.valuation_daily_v1', v.ts_code || ':' || v.trade_date::text,
               1::bigint,
               encode(digest(convert_to(jsonb_build_object(
                   'ts_code', v.ts_code, 'trade_date', v.trade_date,
                   'pe_ttm', v.pe_ttm, 'pb', v.pb, 'ps_ttm', v.ps_ttm,
                   'dv_ratio', v.dv_ratio, 'total_mv', v.total_mv, 'circ_mv', v.circ_mv,
                   'pe_pct_3y', v.pe_pct_3y, 'pe_pct_5y', v.pe_pct_5y,
                   'pb_pct_3y', v.pb_pct_3y, 'pb_pct_5y', v.pb_pct_5y,
                   'is_pe_meaningful', v.is_pe_meaningful,
                   'valuation_observation_count', v.valuation_observation_count,
                   'pe_window_start_date_5y', v.pe_window_start_date_5y,
                   'pe_observation_count_5y', v.pe_observation_count_5y,
                   'has_full_pe_5y_history', v.has_full_pe_5y_history,
                   'quality_status', v.quality_status, 'source_asof_date', v.source_asof_date
               )::text, 'UTF8'), 'sha256'), 'hex')::char(64)
        FROM research.stock_snapshot_daily s
        JOIN feature.valuation_daily_v1 v ON v.trade_date=s.research_date AND v.ts_code=s.ts_code
        WHERE s.snapshot_id=(SELECT snapshot_id FROM params)
        UNION ALL
        SELECT s.snapshot_id, s.research_date, s.ts_code, 'financial',
               'feature.company_financial_v1', COALESCE(s.financial_source_record_key,
                   s.ts_code || ':' || s.research_date::text || ':missing'),
               s.financial_record_version, s.financial_payload_hash
        FROM research.stock_snapshot_daily s WHERE s.snapshot_id=(SELECT snapshot_id FROM params)
        UNION ALL
        SELECT s.snapshot_id, s.research_date, s.ts_code, 'shareholder_return',
               'feature.shareholder_return_v1', sr.ts_code || ':' || sr.asof_date::text,
               1::bigint,
               encode(digest(convert_to(jsonb_build_object(
                   'ts_code', sr.ts_code, 'asof_date', sr.asof_date,
                   'cash_div_12m', sr.cash_div_12m,
                   'dividend_event_count_3y', sr.dividend_event_count_3y,
                   'dividend_year_count_3y', sr.dividend_year_count_3y,
                   'quality_status', sr.quality_status
               )::text, 'UTF8'), 'sha256'), 'hex')::char(64)
        FROM research.stock_snapshot_daily s
        JOIN feature.shareholder_return_v1 sr ON sr.asof_date=s.research_date AND sr.ts_code=s.ts_code
        WHERE s.snapshot_id=(SELECT snapshot_id FROM params)
        UNION ALL
        SELECT s.snapshot_id, s.research_date, s.ts_code, 'lifecycle',
               'std.security_lifecycle', l.source_record_key, l.record_version,
               l.source_payload_hash
        FROM research.stock_snapshot_daily s
        JOIN LATERAL (
            SELECT l.* FROM std.security_lifecycle l
            WHERE l.ts_code=s.ts_code AND l.effective_from <= s.research_date
              AND (l.effective_to IS NULL OR l.effective_to >= s.research_date)
            ORDER BY l.effective_from DESC, l.lifecycle_id DESC LIMIT 1
        ) l ON TRUE
        WHERE s.snapshot_id=(SELECT snapshot_id FROM params)
        UNION ALL
        SELECT s.snapshot_id, s.research_date, s.ts_code, 'instrument',
               'tushare_stock_basic_raw_version', s.instrument_source_record_key,
               s.instrument_source_record_version,
               s.instrument_source_payload_hash
        FROM research.stock_snapshot_daily s
        WHERE s.snapshot_id=(SELECT snapshot_id FROM params)
        UNION ALL
        SELECT s.snapshot_id, s.research_date, s.ts_code, 'market_facts',
               'research.stock_snapshot_daily',
               s.ts_code || ':' || s.research_date::text || ':market-facts',
               1::bigint,
               encode(digest(convert_to(jsonb_build_object(
                   'raw_open', s.raw_open, 'raw_high', s.raw_high,
                   'raw_low', s.raw_low, 'raw_close', s.raw_close,
                   'raw_volume', s.raw_volume, 'raw_amount', s.raw_amount,
                   'up_limit', s.up_limit, 'down_limit', s.down_limit
               )::text, 'UTF8'), 'sha256'), 'hex')::char(64)
        FROM research.stock_snapshot_daily s
        WHERE s.snapshot_id=(SELECT snapshot_id FROM params)
        UNION ALL
        SELECT s.snapshot_id, s.research_date, s.ts_code, 'fundamental_history',
               'feature.company_financial_v1:pit-visible-summary',
               s.ts_code || ':' || s.research_date::text || ':fundamental-history',
               1::bigint,
               encode(digest(convert_to(jsonb_build_object(
                   'annual_report_count_5y', s.annual_report_count_5y,
                   'roe_above_15_years_5y', s.roe_above_15_years_5y,
                   'roe_5y_min', s.roe_5y_min,
                   'roe_5y_all_above_15', s.roe_5y_all_above_15,
                   'ocf_report_count_3y', s.ocf_report_count_3y,
                   'ocf_positive_years_3y', s.ocf_positive_years_3y,
                   'ocf_3y_all_positive', s.ocf_3y_all_positive,
                   'quarterly_report_count_4q', s.quarterly_report_count_4q,
                   'q_sales_yoy_above_30_quarters_4q', s.q_sales_yoy_above_30_quarters_4q,
                   'q_sales_yoy_4q_all_above_30', s.q_sales_yoy_4q_all_above_30
               )::text, 'UTF8'), 'sha256'), 'hex')::char(64)
        FROM research.stock_snapshot_daily s
        WHERE s.snapshot_id=(SELECT snapshot_id FROM params)
    )
    SELECT g.snapshot_id, g.research_date, g.ts_code, g.field_group,
           COALESCE(r.source_table, g.field_group || ':missing'),
           COALESCE(r.source_record_key, g.ts_code || ':' || g.research_date::text || ':missing'),
           r.source_record_version, r.source_payload_hash
    FROM groups g
    LEFT JOIN refs r ON r.snapshot_id=g.snapshot_id AND r.research_date=g.research_date
                     AND r.ts_code=g.ts_code AND r.field_group=g.field_group
    ON CONFLICT DO NOTHING
    """


def _stock_snapshot_batch_sql():
    """Return the set-based materialization SQL for several cutoff dates."""
    sql = _stock_snapshot_sql()
    marker = "    INSERT INTO research.stock_snapshot_daily"
    if marker not in sql:
        raise RuntimeError("单日 Snapshot SQL 结构已变化，无法生成批量 SQL")
    sql = sql.replace(
        marker,
        "    WITH params AS (\n"
        "        SELECT snapshot_id, research_cutoff_date, %s::bigint AS stock_basic_max_record_id\n"
        "          FROM research.snapshot_manifest\n"
        "         WHERE snapshot_id = ANY(%s::uuid[])\n"
        "    )\n" + marker,
        1,
    )
    sql = sql.replace("SELECT %s, u.trade_date", "SELECT m.snapshot_id, u.trade_date", 1)
    sql = sql.replace("FROM pit.universe_daily u", "FROM params m JOIN pit.universe_daily u ON u.trade_date=m.research_cutoff_date", 1)
    sql = sql.replace("b.record_id <= %s", "b.record_id <= m.stock_basic_max_record_id", 1)
    sql = sql.replace("WHERE u.trade_date=%s", "WHERE TRUE", 1)
    return sql


def _lineage_batch_sql():
    """Return the lineage SQL with a UUID array instead of one UUID."""
    sql = _lineage_sql()
    marker = "WITH params AS (SELECT %s::uuid AS snapshot_id), groups AS ("
    if marker not in sql:
        raise RuntimeError("单日 lineage SQL 结构已变化，无法生成批量 SQL")
    sql = sql.replace(
        marker,
        "WITH params AS (SELECT unnest(%s::uuid[]) AS snapshot_id), groups AS (",
        1,
    )
    sql = sql.replace(
        "WHERE s.snapshot_id=(SELECT snapshot_id FROM params)",
        "WHERE s.snapshot_id IN (SELECT snapshot_id FROM params)",
    )
    return sql


def create_snapshots_batch(
    cutoff_dates: list[date],
    baseline_tag: str,
    schema_version: str = SCHEMA_VERSION,
    notes_prefix: str = "",
) -> tuple[dict[str, str], ...]:
    """Create several DRAFT manifests with one baseline/quality read.

    The manifests remain one-per-date, but the inserts use one execute_values
    call.  This is the resume-safe creation path used by the series runner.
    """
    dates = tuple(sorted(set(cutoff_dates)))
    if not dates:
        return ()
    baseline, quality_report, quality_hash = None, None, None
    with get_connection() as conn:
        baseline = _baseline(max(dates), baseline_tag, conn)
    quality_report, _ = _quality_report(max(dates))
    if quality_report["failures"]:
        raise RuntimeError(f"质量门禁未通过，不能创建 Research 快照: {quality_report['failures']}")
    metadata = _git_metadata()
    psycopg2, execute_values = require_psycopg2()
    Json = psycopg2.extras.Json
    candidates: list[tuple] = []
    result: dict[str, str] = {}
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """SELECT snapshot_id, research_cutoff_date, status
                 FROM research.snapshot_manifest
                WHERE data_baseline_tag=%s AND schema_version=%s
                  AND research_cutoff_date = ANY(%s::date[])""",
            (baseline_tag, schema_version, list(dates)),
        )
        existing = {row[1]: (str(row[0]), row[2]) for row in cur.fetchall()}
        for cutoff in dates:
            if cutoff in existing:
                snapshot_id, status = existing[cutoff]
                if status == "BUILDING":
                    raise RuntimeError(f"检测到同一截止日已有 BUILDING 快照: {snapshot_id}")
                result[cutoff.isoformat()] = snapshot_id
                continue
            snapshot_id = str(uuid.uuid4())
            report = dict(quality_report)
            report["cutoff_date"] = cutoff.isoformat()
            report_encoded = json.dumps(
                _canonical(report), ensure_ascii=False, sort_keys=True,
                separators=(",", ":"), allow_nan=False,
            )
            report_hash = hashlib.sha256(report_encoded.encode("utf-8")).hexdigest()
            candidates.append(
                (
                    snapshot_id, cutoff, baseline_tag, schema_version,
                    metadata["code_version"], metadata["commit"], metadata["dirty"],
                    metadata["diff_hash"], report_hash, baseline[3], baseline[4],
                    Json(report), (f"{notes_prefix} cutoff={cutoff.isoformat()}").strip(),
                )
            )
            result[cutoff.isoformat()] = snapshot_id
        if candidates:
            execute_values(
                cur,
                """INSERT INTO research.snapshot_manifest
                    (snapshot_id, research_cutoff_date, data_baseline_tag, schema_version,
                     code_version, git_commit, git_dirty, diff_hash, quality_report_hash,
                     baseline_quality_report_hash, baseline_source_manifest_hash,
                     quality_report, status, notes)
                    VALUES %s""",
                candidates,
                template="(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'DRAFT',%s)",
                page_size=100,
            )
        conn.commit()
    return tuple(
        {"snapshot_id": result[cutoff.isoformat()], "research_cutoff_date": cutoff.isoformat()}
        for cutoff in dates
    )


def build_snapshots_batch(
    snapshot_ids: list[str],
    *,
    baseline=None,
    quality_reports: dict[str, tuple[dict, str]] | None = None,
) -> None:
    """Build several DRAFT/FAILED snapshots using set-based inserts."""
    ids = tuple(str(value) for value in snapshot_ids)
    if not ids:
        return
    metadata = _git_metadata()
    from data_collect.jobs.tushare_release import _lock_release_inputs

    conn = get_connection()
    lock_acquired = False
    try:
        conn.set_session(isolation_level="REPEATABLE READ", readonly=False, autocommit=False)
        with conn.cursor() as cur:
            _acquire_snapshot_batch_lock(cur)
            lock_acquired = True
        _lock_release_inputs(conn)
        with conn.cursor() as cur:
            cur.execute(
                """SELECT snapshot_id, research_cutoff_date, data_baseline_tag,
                              schema_version, status, baseline_quality_report_hash,
                              baseline_source_manifest_hash
                         FROM research.snapshot_manifest
                        WHERE snapshot_id = ANY(%s::uuid[])
                        ORDER BY research_cutoff_date
                        FOR UPDATE""",
                (list(ids),),
            )
            rows = cur.fetchall()
        if len(rows) != len(ids):
            raise ValueError("批量构建包含不存在的 Snapshot")
        if any(row[4] not in {"DRAFT", "FAILED"} for row in rows):
            raise RuntimeError("批量构建只允许 DRAFT/FAILED Snapshot")
        baseline_tags = {row[2] for row in rows}
        schema_versions = {row[3] for row in rows}
        if len(baseline_tags) != 1 or len(schema_versions) != 1:
            raise ValueError("同一批次必须使用同一 baseline_tag/schema_version")
        baseline_tag = next(iter(baseline_tags))
        schema_version = next(iter(schema_versions))
        if baseline is None:
            baseline = _baseline(max(row[1] for row in rows), baseline_tag, conn)
        cutoffs = [row[1] for row in rows]
        _assert_price_vintage_for_dates(conn, cutoffs, schema_version)
        for row in rows:
            if baseline[3] != row[5] or baseline[4] != row[6]:
                raise RuntimeError("数据基线 Manifest 已变化，拒绝批量构建")
        reports = {}
        for row in rows:
            if quality_reports is not None:
                report, quality_hash = quality_reports[str(row[0])]
            else:
                report, quality_hash = _quality_report(row[1])
            if report["failures"]:
                raise RuntimeError(f"质量门禁未通过: {row[1]} {report['failures']}")
            reports[row[0]] = (report, quality_hash)
        psycopg2, execute_values = require_psycopg2()
        Json = psycopg2.extras.Json
        with conn.cursor() as cur:
            for row in rows:
                report, quality_hash = reports[row[0]]
                cur.execute(
                    """UPDATE research.snapshot_manifest
                          SET status='BUILDING', quality_report_hash=%s,
                              quality_report=%s, code_version=%s, git_commit=%s,
                              git_dirty=%s, diff_hash=%s, failure_reason=NULL
                        WHERE snapshot_id=%s""",
                    (quality_hash, Json(report), metadata["code_version"], metadata["commit"],
                     metadata["dirty"], metadata["diff_hash"], row[0]),
                )
            for table in ("research.snapshot_lineage", "research.stock_snapshot_daily", "research.snapshot_input"):
                cur.execute(
                    f"DELETE FROM {table} WHERE snapshot_id = ANY(%s::uuid[])",
                    (list(ids),),
                )
            stock_basic_max_record_id = baseline[5]["append_only_sources"][
                "tushare_stock_basic_raw_version"
            ]["max_record_id"]
            if stock_basic_max_record_id is None:
                raise RuntimeError("基线缺少 stock_basic Raw 版本高水位")
            cur.execute(_stock_snapshot_batch_sql(), (stock_basic_max_record_id, list(ids)))
            if cur.rowcount <= 0:
                raise RuntimeError("批量截止日没有可生成的 universe 数据")
            cur.execute(_lineage_batch_sql(), (list(ids),))
            input_rows = []
            for row in rows:
                stats = [_input_stats(conn, definition, row[1], baseline[5]) for definition in INPUTS]
                input_hash = _manifest_hash(stats)
                for item in stats:
                    input_rows.append(
                        (row[0], item["schema_name"], item["table_name"], item["selection_predicate"],
                         item["selected_columns"], item["hash_algorithm"], item["row_count"],
                         item["min_date"], item["max_date"], item["content_hash"])
                    )
                cur.execute(
                    "UPDATE research.snapshot_manifest SET input_manifest_hash=%s WHERE snapshot_id=%s",
                    (input_hash, row[0]),
                )
            execute_values(
                cur,
                """INSERT INTO research.snapshot_input
                    (snapshot_id,schema_name,table_name,selection_predicate,selected_columns,
                     hash_algorithm,row_count,min_date,max_date,content_hash)
                    VALUES %s""",
                input_rows,
                page_size=500,
            )
        conn.commit()
    except Exception as exc:
        conn.rollback()
        if not lock_acquired:
            raise
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """UPDATE research.snapshot_manifest
                          SET status='FAILED', failed_at=CURRENT_TIMESTAMP,
                              failure_reason=%s
                        WHERE snapshot_id = ANY(%s::uuid[])
                          AND status IN ('DRAFT','FAILED')""",
                    (str(exc)[:2000], list(ids)),
                )
            conn.commit()
        except Exception:
            conn.rollback()
        raise
    finally:
        conn.close()


def build(snapshot_id: str) -> None:
    metadata = _git_metadata()
    conn = get_connection()
    try:
        conn.set_session(isolation_level="REPEATABLE READ", readonly=False, autocommit=False)
        # The quality report is intentionally read through the existing
        # quality-gate connections, so acquire the same shared input locks on
        # this transaction before opening that read boundary.  This prevents a
        # Raw/derived rebuild from landing between the quality report and the
        # snapshot materialization.
        from data_collect.jobs.tushare_release import _lock_release_inputs
        _lock_release_inputs(conn)
        manifest = _manifest(conn, snapshot_id, for_update=True)
        _assert_price_vintage_for_schema(conn, manifest["cutoff"], manifest["schema_version"])
        if manifest["status"] not in {"DRAFT", "FAILED"}:
            raise RuntimeError(f"只有 DRAFT/FAILED 快照可构建: {snapshot_id} ({manifest['status']})")
        quality_report, quality_hash = _quality_report(manifest["cutoff"])
        if quality_report["failures"]:
            raise RuntimeError(f"质量门禁未通过，不能构建快照: {quality_report['failures']}")
        baseline = _baseline(manifest["cutoff"], manifest["baseline"], conn)
        if baseline[3] != manifest["baseline_quality_hash"]:
            raise RuntimeError("数据基线质量哈希已变化，拒绝使用旧 Manifest 构建")
        if baseline[4] != manifest["baseline_source_manifest_hash"]:
            raise RuntimeError("数据基线 Raw 来源 Manifest 已变化，拒绝构建")
        with conn.cursor() as cur:
            cur.execute("""UPDATE research.snapshot_manifest
                           SET status='BUILDING', quality_report_hash=%s, quality_report=%s,
                               code_version=%s, git_commit=%s, git_dirty=%s, diff_hash=%s,
                               failure_reason=NULL
                           WHERE snapshot_id=%s""", (quality_hash, json.dumps(quality_report, ensure_ascii=False),
                           metadata["code_version"], metadata["commit"], metadata["dirty"], metadata["diff_hash"], snapshot_id))
            psycopg2, _ = require_psycopg2()
            Json = psycopg2.extras.Json
            cur.execute("UPDATE research.snapshot_manifest SET quality_report=%s WHERE snapshot_id=%s",
                        (Json(quality_report), snapshot_id))
            cur.execute("DELETE FROM research.snapshot_lineage WHERE snapshot_id=%s", (snapshot_id,))
            cur.execute("DELETE FROM research.stock_snapshot_daily WHERE snapshot_id=%s", (snapshot_id,))
            cur.execute("DELETE FROM research.snapshot_input WHERE snapshot_id=%s", (snapshot_id,))
            stock_basic_max_record_id = (
                baseline[5].get("append_only_sources", {})
                .get("tushare_stock_basic_raw_version", {})
                .get("max_record_id")
            )
            if stock_basic_max_record_id is None:
                raise RuntimeError("基线缺少 stock_basic Raw 版本高水位")
            cur.execute(
                _stock_snapshot_sql(),
                (snapshot_id, stock_basic_max_record_id, manifest["cutoff"]),
            )
            if cur.rowcount <= 0:
                raise RuntimeError(f"截止日没有可生成的 universe 数据: {manifest['cutoff']}")
            cur.execute(_lineage_sql(), (snapshot_id,))
            stats = [
                _input_stats(conn, definition, manifest["cutoff"], baseline[5])
                for definition in INPUTS
            ]
            for item in stats:
                cur.execute("""INSERT INTO research.snapshot_input
                    (snapshot_id,schema_name,table_name,selection_predicate,selected_columns,
                     hash_algorithm,row_count,min_date,max_date,content_hash)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                            (snapshot_id, item["schema_name"], item["table_name"], item["selection_predicate"],
                             item["selected_columns"], item["hash_algorithm"], item["row_count"],
                             item["min_date"], item["max_date"], item["content_hash"]))
            input_hash = _manifest_hash(stats)
            cur.execute("UPDATE research.snapshot_manifest SET input_manifest_hash=%s WHERE snapshot_id=%s",
                        (input_hash, snapshot_id))
        conn.commit()
    except Exception as exc:
        conn.rollback()
        try:
            with conn.cursor() as cur:
                cur.execute("UPDATE research.snapshot_manifest SET status='FAILED', failed_at=CURRENT_TIMESTAMP, failure_reason=%s WHERE snapshot_id=%s AND status IN ('DRAFT','BUILDING','FAILED')", (str(exc)[:2000], snapshot_id))
            conn.commit()
        except Exception:
            conn.rollback()
        raise
    finally:
        conn.close()


def validate_snapshots_batch(
    snapshot_ids: list[str],
    *,
    baseline=None,
    quality_reports: dict[str, tuple[dict, str]] | None = None,
) -> None:
    """Validate several BUILDING snapshots under one read boundary."""
    ids = tuple(str(value) for value in snapshot_ids)
    if not ids:
        return
    from data_collect.jobs.tushare_release import _lock_release_inputs

    conn = get_connection()
    lock_acquired = False
    try:
        conn.set_session(isolation_level="REPEATABLE READ", readonly=False, autocommit=False)
        with conn.cursor() as cur:
            _acquire_snapshot_batch_lock(cur)
            lock_acquired = True
        _lock_release_inputs(conn)
        manifests = []
        for snapshot_id in ids:
            manifest = _manifest(conn, snapshot_id, for_update=True)
            if manifest["status"] != "BUILDING":
                raise RuntimeError(f"只有 BUILDING 快照可批量校验: {snapshot_id} ({manifest['status']})")
            manifests.append(manifest)
        baseline_tags = {item["baseline"] for item in manifests}
        schema_versions = {item["schema_version"] for item in manifests}
        if len(baseline_tags) != 1 or len(schema_versions) != 1:
            raise ValueError("同一批次必须使用同一 baseline_tag/schema_version")
        if baseline is None:
            baseline = _baseline(max(item["cutoff"] for item in manifests), next(iter(baseline_tags)), conn)
        _assert_price_vintage_for_dates(
            conn, [item["cutoff"] for item in manifests], next(iter(schema_versions))
        )
        issues_by_date = {}
        for manifest in manifests:
            if baseline[3] != manifest["baseline_quality_hash"]:
                raise RuntimeError("数据基线质量哈希已变化，拒绝批量校验")
            if baseline[4] != manifest["baseline_source_manifest_hash"]:
                raise RuntimeError("数据基线 Raw 来源 Manifest 已变化，拒绝批量校验")
            if quality_reports is not None:
                quality_report, quality_hash = quality_reports[str(manifest["snapshot_id"])]
            else:
                quality_report, quality_hash = _quality_report(manifest["cutoff"])
            if quality_report["failures"] or quality_hash != manifest["quality_hash"]:
                raise RuntimeError(f"质量报告已变化或门禁失败: {manifest['cutoff']}")
            issues = _check_snapshot(conn, manifest, baseline[5])
            pit_report = verify_pit_cutoff_consistency(manifest["cutoff"])
            if not pit_report["passed"]:
                issues.append({"pit_cutoff": pit_report})
            if issues:
                issues_by_date[str(manifest["cutoff"])] = issues
        if issues_by_date:
            raise RuntimeError(f"Research 快照批量校验失败: {issues_by_date}")
        with conn.cursor() as cur:
            cur.execute(
                """UPDATE research.snapshot_manifest
                      SET status='VALIDATED'
                    WHERE snapshot_id = ANY(%s::uuid[])
                      AND status='BUILDING'""",
                (list(ids),),
            )
            if cur.rowcount != len(ids):
                raise RuntimeError("批量校验期间 Snapshot 状态发生变化")
        conn.commit()
    except Exception as exc:
        conn.rollback()
        if not lock_acquired:
            raise
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """UPDATE research.snapshot_manifest
                          SET status='FAILED', failed_at=CURRENT_TIMESTAMP,
                              failure_reason=%s
                        WHERE snapshot_id = ANY(%s::uuid[])
                          AND status='BUILDING'""",
                    (str(exc)[:2000], list(ids)),
                )
            conn.commit()
        except Exception:
            conn.rollback()
        raise
    finally:
        conn.close()


def _check_snapshot(conn, manifest: dict, source_manifest: dict) -> list[dict]:
    stats = [
        _input_stats(conn, definition, manifest["cutoff"], source_manifest)
        for definition in INPUTS
    ]
    issues = []
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*), COUNT(DISTINCT research_date), MIN(research_date), MAX(research_date) FROM research.stock_snapshot_daily WHERE snapshot_id=%s", (manifest["snapshot_id"],))
        rows, dates, min_date, max_date = cur.fetchone()
        if rows == 0 or dates != 1 or min_date != manifest["cutoff"] or max_date != manifest["cutoff"]:
            issues.append({"snapshot_rows": rows, "distinct_dates": dates, "min_date": min_date, "max_date": max_date})
        cur.execute(
            "SELECT COUNT(*) FROM pit.universe_daily WHERE trade_date=%s",
            (manifest["cutoff"],),
        )
        expected_universe_rows = cur.fetchone()[0]
        if rows != expected_universe_rows:
            issues.append({
                "snapshot_universe_row_mismatch": {
                    "snapshot_rows": rows,
                    "universe_rows": expected_universe_rows,
                }
            })
        cur.execute("""SELECT COUNT(*) FROM (
            SELECT snapshot_id,research_date,ts_code
            FROM research.snapshot_lineage WHERE snapshot_id=%s
            GROUP BY snapshot_id,research_date,ts_code
            HAVING COUNT(DISTINCT field_group) <> %s
        ) x""", (manifest["snapshot_id"], len(REQUIRED_LINEAGE_GROUPS)))
        missing_lineage = cur.fetchone()[0]
        if missing_lineage:
            issues.append({"rows_with_incomplete_lineage": missing_lineage})
        cur.execute("""SELECT field_group, COUNT(*)
                       FROM research.snapshot_lineage
                       WHERE snapshot_id=%s
                         AND source_record_key NOT LIKE '%%:missing'
                         AND (source_record_version IS NULL OR source_payload_hash IS NULL)
                       GROUP BY field_group ORDER BY field_group""", (manifest["snapshot_id"],))
        incomplete_refs = cur.fetchall()
        if incomplete_refs:
            issues.append({"lineage_refs_without_version_or_hash": incomplete_refs})
        if manifest["schema_version"] == SCHEMA_VERSION:
            cur.execute("""SELECT
                COUNT(*) FILTER (WHERE ma20 IS NOT NULL AND COALESCE(ma20_observation_count,0) < 20),
                COUNT(*) FILTER (WHERE ma60 IS NOT NULL AND COALESCE(ma60_observation_count,0) < 60),
                COUNT(*) FILTER (WHERE ma120 IS NOT NULL AND COALESCE(ma120_observation_count,0) < 120),
                COUNT(*) FILTER (WHERE pe_pct_5y IS NOT NULL AND (
                    NOT COALESCE(has_full_pe_5y_history,FALSE)
                    OR COALESCE(valuation_observation_count,0) < 1000
                    OR pe_window_start_date_5y IS NULL
                    OR research_date - pe_window_start_date_5y < 1794)),
                COUNT(*) FILTER (WHERE financial_version_available_date > research_date),
                COUNT(*) FILTER (WHERE financial_vintage_quality='WAREHOUSE_OBSERVED_ONLY'
                                  AND financial_source_observed_at::date > research_date),
                COUNT(*) FILTER (WHERE instrument_source_record_key IS NULL
                                  OR instrument_source_record_version IS NULL
                                  OR instrument_source_payload_hash IS NULL),
                COUNT(*) FILTER (WHERE roe_5y_all_above_15 AND annual_report_count_5y <> 5),
                COUNT(*) FILTER (WHERE ocf_3y_all_positive AND ocf_report_count_3y <> 3),
                COUNT(*) FILTER (WHERE q_sales_yoy_4q_all_above_30 AND quarterly_report_count_4q <> 4)
              FROM research.stock_snapshot_daily WHERE snapshot_id=%s""",
                        (manifest["snapshot_id"],))
            maturity_failures = cur.fetchone()
            if any(maturity_failures):
                issues.append({"research_v1_2_maturity_or_pit_failures": maturity_failures})
    current_hash = _manifest_hash(stats)
    if current_hash != manifest["input_hash"]:
        issues.append({"input_manifest_hash_changed": {"stored": manifest["input_hash"], "current": current_hash}})
    return issues


def validate(snapshot_id: str) -> None:
    conn = get_connection()
    try:
        conn.set_session(isolation_level="REPEATABLE READ", readonly=False, autocommit=False)
        manifest = _manifest(conn, snapshot_id, for_update=True)
        _assert_price_vintage_for_schema(conn, manifest["cutoff"], manifest["schema_version"])
        if manifest["status"] != "BUILDING":
            raise RuntimeError(f"只有 BUILDING 快照可校验: {snapshot_id} ({manifest['status']})")
        quality_report, quality_hash = _quality_report(manifest["cutoff"])
        baseline = _baseline(manifest["cutoff"], manifest["baseline"], conn)
        if baseline[3] != manifest["baseline_quality_hash"]:
            raise RuntimeError("数据基线质量哈希已变化，必须重新创建快照")
        if baseline[4] != manifest["baseline_source_manifest_hash"]:
            raise RuntimeError("数据基线 Raw 来源 Manifest 已变化，必须重新创建快照")
        if quality_report["failures"] or quality_hash != manifest["quality_hash"]:
            raise RuntimeError("质量报告已变化或门禁失败，必须重新 build 快照")
        issues = _check_snapshot(conn, manifest, baseline[5])
        pit_report = verify_pit_cutoff_consistency(manifest["cutoff"])
        if not pit_report["passed"]:
            issues.append({"pit_cutoff": pit_report})
        if issues:
            raise RuntimeError(f"Research 快照校验失败: {issues}")
        with conn.cursor() as cur:
            cur.execute("UPDATE research.snapshot_manifest SET status='VALIDATED' WHERE snapshot_id=%s", (snapshot_id,))
        conn.commit()
    except Exception as exc:
        conn.rollback()
        try:
            with conn.cursor() as cur:
                cur.execute("UPDATE research.snapshot_manifest SET status='FAILED', failed_at=CURRENT_TIMESTAMP, failure_reason=%s WHERE snapshot_id=%s AND status='BUILDING'", (str(exc)[:2000], snapshot_id))
            conn.commit()
        except Exception:
            conn.rollback()
        raise
    finally:
        conn.close()


def freeze(snapshot_id: str, code_archive_path: str | None = None) -> None:
    metadata = _git_metadata()
    if metadata["dirty"] and not code_archive_path:
        raise RuntimeError(
            "正式冻结要求 Git 工作区干净；脏工作区必须显式提供存在的 code_archive_path"
        )
    if code_archive_path and not Path(code_archive_path).exists():
        raise FileNotFoundError(f"代码归档不存在: {code_archive_path}")
    archive_hash = archive_size = None
    if code_archive_path:
        archive_hash, archive_size = _sha256_file(code_archive_path)
    with get_connection() as conn, conn.cursor() as cur:
        manifest = _manifest(conn, snapshot_id, for_update=True)
        _assert_price_vintage_for_schema(conn, manifest["cutoff"], manifest["schema_version"])
        if manifest["status"] != "VALIDATED":
            raise RuntimeError(f"只有 VALIDATED 快照可冻结: {snapshot_id} ({manifest['status']})")
        baseline = _baseline(manifest["cutoff"], manifest["baseline"], conn)
        if baseline[3] != manifest["baseline_quality_hash"]:
            raise RuntimeError("数据基线质量哈希已变化，拒绝冻结")
        if baseline[4] != manifest["baseline_source_manifest_hash"]:
            raise RuntimeError("数据基线 Raw 来源 Manifest 已变化，拒绝冻结")
        quality_report, quality_hash = _quality_report(manifest["cutoff"])
        if quality_report["failures"] or quality_hash != manifest["quality_hash"]:
            raise RuntimeError("质量报告已变化或门禁失败，必须重新 build/validate 快照")
        pit_report = verify_pit_cutoff_consistency(
            manifest["cutoff"], snapshot_id=snapshot_id
        )
        if not pit_report["passed"]:
            raise RuntimeError(f"正式冻结前 PIT 截面校验失败: {pit_report['issues']}")
        assert_market_coverage_publishable(manifest["cutoff"])
        cur.execute(
            """UPDATE research.snapshot_manifest
                  SET code_archive_path=COALESCE(%s, code_archive_path),
                      code_archive_sha256=COALESCE(%s, code_archive_sha256),
                      code_archive_size_bytes=COALESCE(%s, code_archive_size_bytes),
                      code_version=%s, git_commit=%s, git_dirty=%s, diff_hash=%s,
                      status='FROZEN'
                WHERE snapshot_id=%s AND status='VALIDATED'""",
            (code_archive_path, archive_hash, archive_size,
             metadata["code_version"], metadata["commit"],
             metadata["dirty"], metadata["diff_hash"], snapshot_id),
        )
        if cur.rowcount != 1:
            raise RuntimeError("Research 快照状态在冻结期间发生变化，拒绝并发冻结")
        conn.commit()


def _assert_pit_cutoff_dates_batch(
    conn, cutoffs: list[date], snapshot_ids: list[str]
) -> None:
    """Run the PIT cutoff checks for a set of snapshots in bounded SQL scans."""
    if not cutoffs:
        return
    with conn.cursor() as cur:
        checks = (
            (
                "financial_asof_announcement",
                """SELECT COUNT(*) FROM pit.financial_asof
                    WHERE decision_date = ANY(%s::date[])
                      AND effective_announce_date > decision_date""",
                (cutoffs,),
            ),
            (
                "price_feature_source_asof",
                """SELECT COUNT(*) FROM feature.price_daily_v2
                    WHERE trade_date = ANY(%s::date[])
                      AND source_asof_date > trade_date""",
                (cutoffs,),
            ),
            (
                "valuation_feature_source_asof",
                """SELECT COUNT(*) FROM feature.valuation_daily_v1
                    WHERE trade_date = ANY(%s::date[])
                      AND source_asof_date > trade_date""",
                (cutoffs,),
            ),
            (
                "tradeability_source_asof",
                """SELECT COUNT(*) FROM pit.security_tradeability_daily
                    WHERE trade_date = ANY(%s::date[])
                      AND source_asof_date > trade_date""",
                (cutoffs,),
            ),
            (
                "research_snapshot_financial_visibility",
                """SELECT COUNT(*) FROM research.stock_snapshot_daily
                    WHERE snapshot_id = ANY(%s::uuid[])
                      AND effective_announce_date > research_date""",
                (snapshot_ids,),
            ),
        )
        failures = []
        for name, query, params in checks:
            cur.execute(query, params)
            count = int(cur.fetchone()[0] or 0)
            if count:
                failures.append({"name": name, "violations": count})
    if failures:
        raise RuntimeError(f"批量冻结 PIT 截止检查失败: {failures}")


def freeze_snapshots_batch(
    snapshot_ids: list[str], code_archive_path: str | None = None
) -> None:
    """Freeze validated snapshots sharing one baseline with bounded IO.

    This is the production path for a historical series.  It keeps the
    per-snapshot immutable rows and audit metadata, while avoiding one full
    Raw-manifest/quality scan for every cutoff date.
    """
    ids = tuple(str(value) for value in snapshot_ids)
    if not ids:
        return
    metadata = _git_metadata()
    if metadata["dirty"] and not code_archive_path:
        raise RuntimeError("批量正式冻结要求 Git 工作区干净")
    if code_archive_path and not Path(code_archive_path).exists():
        raise FileNotFoundError(f"代码归档不存在: {code_archive_path}")
    archive_hash = archive_size = None
    if code_archive_path:
        archive_hash, archive_size = _sha256_file(code_archive_path)

    from data_collect.jobs.tushare_release import _lock_release_inputs

    conn = get_connection()
    try:
        conn.set_session(
            isolation_level="REPEATABLE READ", readonly=False, autocommit=False
        )
        with conn.cursor() as cur:
            _acquire_snapshot_batch_lock(cur)
        _lock_release_inputs(conn)
        with conn.cursor() as cur:
            cur.execute(
                """SELECT snapshot_id, research_cutoff_date, data_baseline_tag,
                              schema_version, status, baseline_quality_report_hash,
                              baseline_source_manifest_hash, quality_report_hash,
                              input_manifest_hash
                         FROM research.snapshot_manifest
                        WHERE snapshot_id = ANY(%s::uuid[])
                        ORDER BY research_cutoff_date
                        FOR UPDATE""",
                (list(ids),),
            )
            rows = cur.fetchall()
        if len(rows) != len(ids):
            raise ValueError("批量冻结包含不存在的 Snapshot")
        if any(row[4] != "VALIDATED" for row in rows):
            raise RuntimeError("批量冻结只允许 VALIDATED Snapshot")
        baseline_tags = {row[2] for row in rows}
        schema_versions = {row[3] for row in rows}
        if len(baseline_tags) != 1 or len(schema_versions) != 1:
            raise ValueError("批量冻结必须使用同一 baseline_tag/schema_version")
        baseline_tag = next(iter(baseline_tags))
        schema_version = next(iter(schema_versions))
        cutoffs = [row[1] for row in rows]
        baseline = _baseline(max(cutoffs), baseline_tag, conn)
        for row in rows:
            if baseline[3] != row[5] or baseline[4] != row[6]:
                raise RuntimeError("Snapshot 绑定的基线哈希已变化，拒绝冻结")
            if not row[7] or not row[8]:
                raise RuntimeError("Snapshot 缺少质量哈希或输入哈希，拒绝冻结")
        _assert_price_vintage_for_dates(conn, cutoffs, schema_version)
        report, _ = _quality_report(max(cutoffs))
        if report["failures"]:
            raise RuntimeError(f"质量门禁未通过，拒绝批量冻结: {report['failures']}")
        for row in rows:
            current = dict(report)
            current["cutoff_date"] = row[1].isoformat()
            encoded = json.dumps(
                _canonical(current), ensure_ascii=False, sort_keys=True,
                separators=(",", ":"), allow_nan=False,
            )
            quality_hash = hashlib.sha256(encoded.encode("utf-8")).hexdigest()
            if quality_hash != row[7]:
                raise RuntimeError(f"质量报告哈希已变化，拒绝冻结: {row[1]}")
        assert_market_coverage_range_publishable(
            min(cutoffs), max(cutoffs), connection=conn
        )
        _assert_pit_cutoff_dates_batch(conn, cutoffs, list(ids))
        with conn.cursor() as cur:
            cur.execute(
                """UPDATE research.snapshot_manifest
                      SET code_archive_path=COALESCE(%s, code_archive_path),
                          code_archive_sha256=COALESCE(%s, code_archive_sha256),
                          code_archive_size_bytes=COALESCE(%s, code_archive_size_bytes),
                          code_version=%s, git_commit=%s, git_dirty=%s, diff_hash=%s,
                          status='FROZEN'
                    WHERE snapshot_id = ANY(%s::uuid[])
                      AND status='VALIDATED'""",
                (
                    code_archive_path, archive_hash, archive_size,
                    metadata["code_version"], metadata["commit"], metadata["dirty"],
                    metadata["diff_hash"], list(ids),
                ),
            )
            if cur.rowcount != len(ids):
                raise RuntimeError("批量冻结期间 Snapshot 状态发生变化")
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def status() -> list[tuple]:
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute("""SELECT snapshot_id,research_cutoff_date,data_baseline_tag,status,
                      schema_version,code_version,git_dirty,created_at,validated_at,frozen_at
                      FROM research.snapshot_manifest ORDER BY created_at""")
        rows = cur.fetchall()
    for row in rows:
        print(" | ".join("" if value is None else str(value) for value in row))
    return rows


def main():
    parser = argparse.ArgumentParser(description="Research 快照 Manifest 管理")
    sub = parser.add_subparsers(dest="command", required=True)
    create_parser = sub.add_parser("create")
    create_parser.add_argument("--cutoff-date", required=True, help="YYYY-MM-DD")
    create_parser.add_argument("--baseline-tag", required=True)
    create_parser.add_argument("--schema-version", default=SCHEMA_VERSION)
    create_parser.add_argument("--notes", default="")
    build_parser = sub.add_parser("build")
    build_parser.add_argument("--snapshot-id", required=True)
    validate_parser = sub.add_parser("validate")
    validate_parser.add_argument("--snapshot-id", required=True)
    freeze_parser = sub.add_parser("freeze")
    freeze_parser.add_argument("--snapshot-id", required=True)
    freeze_parser.add_argument("--code-archive-path")
    sub.add_parser("status")
    args = parser.parse_args()
    if args.command == "create":
        print(create(args.cutoff_date, args.baseline_tag, args.schema_version, args.notes))
    elif args.command == "build":
        build(args.snapshot_id)
    elif args.command == "validate":
        validate(args.snapshot_id)
    elif args.command == "freeze":
        freeze(args.snapshot_id, args.code_archive_path)
    else:
        status()


if __name__ == "__main__":
    main()
