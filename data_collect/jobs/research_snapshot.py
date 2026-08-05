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

from data_collect.utils.db import get_connection, require_psycopg2


SCHEMA_VERSION = "research-v1"
REQUIRED_LINEAGE_GROUPS = (
    "universe", "tradeability", "price", "valuation", "financial",
    "shareholder_return", "lifecycle", "instrument",
)

# 这些表名和字段均为代码内固定定义，不接受命令行直接拼接，避免把哈希范围变成隐式变化。
INPUTS = (
    {
        "schema": "pit", "table": "universe_daily",
        "columns": ("trade_date", "ts_code", "is_listed_asof", "is_st_asof", "is_suspended_asof", "has_price_history", "quality_status", "source_asof_date"),
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
        "schema": "feature", "table": "price_daily_v1",
        "columns": ("ts_code", "trade_date", "raw_close", "adj_close_base", "return_1d", "return_20d", "return_60d", "ma20", "ma60", "ma120", "volume_ratio_20d", "quality_status", "source_asof_date"),
        "predicate": "trade_date = :research_cutoff_date",
        "where": "trade_date = %s", "params": lambda cutoff: (cutoff,),
        "order": "trade_date, ts_code", "date_column": "trade_date",
    },
    {
        "schema": "feature", "table": "valuation_daily_v1",
        "columns": ("ts_code", "trade_date", "pe_ttm", "pb", "ps_ttm", "dv_ratio", "total_mv", "circ_mv", "pe_pct_3y", "pe_pct_5y", "pb_pct_3y", "pb_pct_5y", "is_pe_meaningful", "quality_status", "source_asof_date"),
        "predicate": "trade_date = :research_cutoff_date",
        "where": "trade_date = %s", "params": lambda cutoff: (cutoff,),
        "order": "trade_date, ts_code", "date_column": "trade_date",
    },
    {
        "schema": "pit", "table": "financial_asof",
        "columns": ("decision_date", "ts_code", "report_date", "effective_announce_date", "record_version", "source_record_key", "roe", "roa", "debt_to_assets", "revenue_yoy", "tr_yoy", "q_sales_yoy", "netprofit_yoy", "quality_status", "payload_hash"),
        "predicate": "decision_date = :research_cutoff_date",
        "where": "decision_date = %s", "params": lambda cutoff: (cutoff,),
        "order": "decision_date, ts_code", "date_column": "decision_date",
    },
    {
        "schema": "feature", "table": "shareholder_return_v1",
        "columns": ("ts_code", "asof_date", "cash_div_12m", "dividend_event_count_3y", "dividend_year_count_3y", "quality_status"),
        "predicate": "asof_date = :research_cutoff_date",
        "where": "asof_date = %s", "params": lambda cutoff: (cutoff,),
        "order": "asof_date, ts_code", "date_column": "asof_date",
    },
    {
        "schema": "public", "table": "stock_basic_info",
        "columns": ("stock_code", "symbol", "name", "market", "list_status", "list_date", "delist_date", "source"),
        "predicate": "list_date <= :research_cutoff_date AND (delist_date IS NULL OR delist_date >= :research_cutoff_date)",
        "where": "list_date <= %s AND (delist_date IS NULL OR delist_date >= %s)", "params": lambda cutoff: (cutoff, cutoff),
        "order": "stock_code", "date_column": "list_date",
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


def _input_stats(conn, definition: dict, cutoff: date) -> dict:
    columns = definition["columns"]
    qualified = _qualified(definition["schema"], definition["table"])
    where = definition["where"]
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

    def run(*args) -> str:
        try:
            return subprocess.check_output(["git", *args], cwd=root, stderr=subprocess.DEVNULL).decode("utf-8", "replace").strip()
        except (OSError, subprocess.CalledProcessError):
            return ""

    commit = run("rev-parse", "HEAD")
    status = run("status", "--porcelain", "--untracked-files=all")
    tracked_diff = run("diff", "--binary", "HEAD")
    diff_hash = hashlib.sha256((status + "\n" + tracked_diff).encode("utf-8")).hexdigest() if status or tracked_diff else None
    dirty = bool(status)
    code_version = f"{commit}+dirty" if dirty and commit else (commit or "unknown")
    return {"commit": commit or None, "dirty": dirty, "diff_hash": diff_hash, "code_version": code_version}


def _quality_report() -> tuple[dict, str]:
    from data_collect.jobs.tushare_release import _quality_gate

    report = _quality_gate()
    encoded = json.dumps(report, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
    return report, hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _baseline(cutoff: date, tag: str):
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute("""SELECT release_tag, baseline_cutoff_date, status
                       FROM tushare_data_release WHERE release_tag=%s""", (tag,))
        row = cur.fetchone()
    if not row or row[2] != "validated":
        raise RuntimeError(f"数据基线必须是 validated: {tag}")
    if cutoff > row[1]:
        raise ValueError(f"research_cutoff_date {cutoff} 晚于数据基线截止日 {row[1]}")
    return row


def create(cutoff: str, baseline_tag: str, schema_version: str = SCHEMA_VERSION, notes: str = "") -> str:
    cutoff_date = date.fromisoformat(cutoff)
    _baseline(cutoff_date, baseline_tag)
    quality_report, quality_hash = _quality_report()
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
             quality_report, status, notes)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'DRAFT',%s)""",
                    (snapshot_id, cutoff_date, baseline_tag, schema_version,
                     metadata["code_version"], metadata["commit"], metadata["dirty"],
                     metadata["diff_hash"], quality_hash, Json(quality_report), notes))
        conn.commit()
    return snapshot_id


def _manifest(conn, snapshot_id: str):
    with conn.cursor() as cur:
        cur.execute("""SELECT snapshot_id, research_cutoff_date, data_baseline_tag,
               schema_version, status, code_version, git_commit, git_dirty,
               diff_hash, code_archive_path, quality_report_hash, input_manifest_hash
               FROM research.snapshot_manifest WHERE snapshot_id=%s""", (snapshot_id,))
        row = cur.fetchone()
    if not row:
        raise ValueError(f"不存在的 snapshot_id: {snapshot_id}")
    return dict(zip(("snapshot_id", "cutoff", "baseline", "schema_version", "status",
                     "code_version", "git_commit", "git_dirty", "diff_hash",
                     "code_archive_path", "quality_hash", "input_hash"), row))


def _stock_snapshot_sql():
    return """
    INSERT INTO research.stock_snapshot_daily
    (snapshot_id, research_date, ts_code, symbol, name, market, list_status, list_date, delist_date,
     is_listed_asof, is_st_asof, is_suspended_asof, is_tradeable, can_buy, can_sell, limit_status,
     raw_close, adj_close_base, return_1d, return_20d, return_60d, ma20, ma60, ma120, volume_ratio_20d,
     pe_ttm, pb, ps_ttm, dv_ratio, total_mv, circ_mv, pe_pct_3y, pe_pct_5y, pb_pct_3y, pb_pct_5y,
     is_pe_meaningful, report_date, effective_announce_date, financial_record_version,
     financial_source_record_key, financial_payload_hash, roe, roa, debt_to_assets, revenue_yoy,
     tr_yoy, q_sales_yoy, netprofit_yoy, operating_cashflow, net_profit, cash_div_12m,
     dividend_event_count_3y, dividend_year_count_3y, quality_status)
    SELECT %s, u.trade_date, u.ts_code, b.symbol, COALESCE(l.name, b.name), b.market,
           b.list_status, b.list_date, b.delist_date,
           u.is_listed_asof, u.is_st_asof, u.is_suspended_asof,
           t.is_tradeable, t.can_buy, t.can_sell, t.limit_status,
           p.raw_close, p.adj_close_base, p.return_1d, p.return_20d, p.return_60d,
           p.ma20, p.ma60, p.ma120, p.volume_ratio_20d,
           v.pe_ttm, v.pb, v.ps_ttm, v.dv_ratio, v.total_mv, v.circ_mv,
           v.pe_pct_3y, v.pe_pct_5y, v.pb_pct_3y, v.pb_pct_5y, v.is_pe_meaningful,
           f.report_date, f.effective_announce_date, f.record_version,
           f.source_record_key, f.payload_hash, f.roe, f.roa, f.debt_to_assets,
           f.revenue_yoy, f.tr_yoy, f.q_sales_yoy, f.netprofit_yoy,
           cf.operating_cashflow, cf.net_profit,
           sr.cash_div_12m, sr.dividend_event_count_3y, sr.dividend_year_count_3y,
           CASE WHEN 'MISSING_SOURCE' = ANY(ARRAY[
                    COALESCE(u.quality_status,'MISSING_SOURCE'), COALESCE(t.quality_status,'MISSING_SOURCE'),
                    COALESCE(p.quality_status,'MISSING_SOURCE'), COALESCE(v.quality_status,'MISSING_SOURCE'),
                    COALESCE(f.quality_status,'MISSING_SOURCE'), COALESCE(sr.quality_status,'MISSING_SOURCE')])
                THEN 'MISSING_SOURCE'
                WHEN 'WARNING' = ANY(ARRAY[
                    COALESCE(u.quality_status,'WARNING'), COALESCE(t.quality_status,'WARNING'),
                    COALESCE(p.quality_status,'WARNING'), COALESCE(v.quality_status,'WARNING'),
                    COALESCE(f.quality_status,'WARNING'), COALESCE(sr.quality_status,'WARNING')])
                THEN 'WARNING' ELSE 'VALID' END
    FROM pit.universe_daily u
    LEFT JOIN pit.security_tradeability_daily t
      ON t.trade_date=u.trade_date AND t.ts_code=u.ts_code
    LEFT JOIN feature.price_daily_v1 p
      ON p.trade_date=u.trade_date AND p.ts_code=u.ts_code
    LEFT JOIN feature.valuation_daily_v1 v
      ON v.trade_date=u.trade_date AND v.ts_code=u.ts_code
    LEFT JOIN pit.financial_asof f
      ON f.decision_date=u.trade_date AND f.ts_code=u.ts_code
    LEFT JOIN feature.shareholder_return_v1 sr
      ON sr.asof_date=u.trade_date AND sr.ts_code=u.ts_code
    LEFT JOIN stock_basic_info b ON b.stock_code=u.ts_code
    LEFT JOIN LATERAL (
      SELECT l.name FROM std.security_lifecycle l
      WHERE l.ts_code=u.ts_code AND l.effective_from <= u.trade_date
        AND (l.effective_to IS NULL OR l.effective_to >= u.trade_date)
      ORDER BY l.effective_from DESC, l.lifecycle_id DESC LIMIT 1
    ) l ON TRUE
    LEFT JOIN LATERAL (
      SELECT c.operating_cashflow, c.net_profit
      FROM feature.company_financial_v1 c
      WHERE c.ts_code=u.ts_code AND c.effective_announce_date IS NOT NULL
        AND c.effective_announce_date <= u.trade_date
      ORDER BY c.effective_announce_date DESC, c.report_date DESC,
               c.record_version DESC, c.feature_id DESC LIMIT 1
    ) cf ON TRUE
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
            'shareholder_return','lifecycle','instrument'
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
               'feature.price_daily_v1', p.ts_code || ':' || p.trade_date::text,
               1::bigint,
               encode(digest(convert_to(jsonb_build_object(
                   'ts_code', p.ts_code, 'trade_date', p.trade_date,
                   'raw_close', p.raw_close, 'adj_close_base', p.adj_close_base,
                   'return_1d', p.return_1d, 'return_20d', p.return_20d,
                   'return_60d', p.return_60d, 'ma20', p.ma20, 'ma60', p.ma60,
                   'ma120', p.ma120, 'volume_ratio_20d', p.volume_ratio_20d,
                   'quality_status', p.quality_status, 'source_asof_date', p.source_asof_date
               )::text, 'UTF8'), 'sha256'), 'hex')::char(64)
        FROM research.stock_snapshot_daily s
        JOIN feature.price_daily_v1 p ON p.trade_date=s.research_date AND p.ts_code=s.ts_code
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
                   'quality_status', v.quality_status, 'source_asof_date', v.source_asof_date
               )::text, 'UTF8'), 'sha256'), 'hex')::char(64)
        FROM research.stock_snapshot_daily s
        JOIN feature.valuation_daily_v1 v ON v.trade_date=s.research_date AND v.ts_code=s.ts_code
        WHERE s.snapshot_id=(SELECT snapshot_id FROM params)
        UNION ALL
        SELECT s.snapshot_id, s.research_date, s.ts_code, 'financial',
               'pit.financial_asof', COALESCE(s.financial_source_record_key,
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
               'tushare_stock_basic_raw', b.ts_code, 1::bigint,
               encode(digest(convert_to(b.payload::text, 'UTF8'), 'sha256'), 'hex')::char(64)
        FROM research.stock_snapshot_daily s
        JOIN tushare_stock_basic_raw b ON b.ts_code=s.ts_code
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


def build(snapshot_id: str) -> None:
    quality_report, quality_hash = _quality_report()
    if quality_report["failures"]:
        raise RuntimeError(f"质量门禁未通过，不能构建快照: {quality_report['failures']}")
    metadata = _git_metadata()
    conn = get_connection()
    try:
        conn.set_session(isolation_level="REPEATABLE READ", readonly=False, autocommit=False)
        manifest = _manifest(conn, snapshot_id)
        if manifest["status"] not in {"DRAFT", "FAILED"}:
            raise RuntimeError(f"只有 DRAFT/FAILED 快照可构建: {snapshot_id} ({manifest['status']})")
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
            cur.execute(_stock_snapshot_sql(), (snapshot_id, manifest["cutoff"]))
            if cur.rowcount <= 0:
                raise RuntimeError(f"截止日没有可生成的 universe 数据: {manifest['cutoff']}")
            cur.execute(_lineage_sql(), (snapshot_id,))
            stats = [_input_stats(conn, definition, manifest["cutoff"]) for definition in INPUTS]
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


def _check_snapshot(conn, manifest: dict) -> list[dict]:
    stats = [_input_stats(conn, definition, manifest["cutoff"]) for definition in INPUTS]
    issues = []
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*), COUNT(DISTINCT research_date), MIN(research_date), MAX(research_date) FROM research.stock_snapshot_daily WHERE snapshot_id=%s", (manifest["snapshot_id"],))
        rows, dates, min_date, max_date = cur.fetchone()
        if rows == 0 or dates != 1 or min_date != manifest["cutoff"] or max_date != manifest["cutoff"]:
            issues.append({"snapshot_rows": rows, "distinct_dates": dates, "min_date": min_date, "max_date": max_date})
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
    current_hash = _manifest_hash(stats)
    if current_hash != manifest["input_hash"]:
        issues.append({"input_manifest_hash_changed": {"stored": manifest["input_hash"], "current": current_hash}})
    return issues


def validate(snapshot_id: str) -> None:
    quality_report, quality_hash = _quality_report()
    conn = get_connection()
    try:
        conn.set_session(isolation_level="REPEATABLE READ", readonly=False, autocommit=False)
        manifest = _manifest(conn, snapshot_id)
        if manifest["status"] != "BUILDING":
            raise RuntimeError(f"只有 BUILDING 快照可校验: {snapshot_id} ({manifest['status']})")
        if quality_report["failures"] or quality_hash != manifest["quality_hash"]:
            raise RuntimeError("质量报告已变化或门禁失败，必须重新 build 快照")
        issues = _check_snapshot(conn, manifest)
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
    with get_connection() as conn, conn.cursor() as cur:
        manifest = _manifest(conn, snapshot_id)
        if manifest["status"] != "VALIDATED":
            raise RuntimeError(f"只有 VALIDATED 快照可冻结: {snapshot_id} ({manifest['status']})")
        if code_archive_path:
            cur.execute("UPDATE research.snapshot_manifest SET code_archive_path=%s WHERE snapshot_id=%s", (code_archive_path, snapshot_id))
        cur.execute("UPDATE research.snapshot_manifest SET status='FROZEN' WHERE snapshot_id=%s", (snapshot_id,))
        conn.commit()


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
