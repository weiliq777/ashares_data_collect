"""Build the immutable fina_indicator version layer and its Standard table.

The existing ``tushare_fina_indicator_raw`` table is never updated or deleted.
This module snapshots each payload into ``fina_indicator_raw_version`` and
rebuilds ``fina_indicator_standard`` from that version layer.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import uuid
from datetime import datetime

import pandas as pd

from data_collect.utils.db import get_connection, require_psycopg2


def _date(value):
    if value is None or value == "":
        return None
    parsed = pd.to_datetime(value, format="%Y%m%d", errors="coerce")
    if pd.isna(parsed):
        parsed = pd.to_datetime(value, errors="coerce")
    return None if pd.isna(parsed) else parsed.date()


def _clean(value):
    if value is None:
        return None
    try:
        return None if pd.isna(value) else value
    except (TypeError, ValueError):
        return value


def _payload_and_hash(record: dict) -> tuple[str, str]:
    clean = {key: _clean(value) for key, value in record.items()}
    encoded = json.dumps(
        clean, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
        default=str, allow_nan=False,
    )
    return encoded, hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _version_values(record: dict, batch_id: str, fetched_at=None):
    payload, payload_hash = _payload_and_hash(record)
    ts_code = record.get("ts_code")
    ann_date = _date(record.get("ann_date"))
    end_date = _date(record.get("end_date"))
    f_ann_date = _date(record.get("f_ann_date"))
    business_key = ":".join(
        str(record.get(key) or "")
        for key in ("ts_code", "end_date", "report_type", "comp_type", "end_type")
    )
    return (
        business_key, ts_code, ann_date, f_ann_date, end_date,
        record.get("report_type"), record.get("comp_type"), record.get("end_type"),
        record.get("update_flag"), payload, "tushare", batch_id, payload_hash,
        fetched_at or datetime.now(),
    )


def save_indicator_versions(records: list[dict], batch_id: str | None = None) -> int:
    """Append indicator payload versions idempotently; returns inserted rows."""
    if not records:
        return 0
    _, execute_values = require_psycopg2()
    batch_id = batch_id or f"fina-indicator-{uuid.uuid4().hex}"
    rows = [_version_values(record, batch_id) for record in records]
    sql = """
        INSERT INTO fina_indicator_raw_version
        (business_key, ts_code, ann_date, f_ann_date, end_date, report_type,
         comp_type, end_type, update_flag, payload, source, batch_id,
         payload_hash, fetched_at)
        VALUES %s
        ON CONFLICT (business_key, payload_hash) DO NOTHING
    """
    with get_connection() as conn, conn.cursor() as cur:
        inserted = 0
        for offset in range(0, len(rows), 2000):
            page = rows[offset : offset + 2000]
            execute_values(cur, sql, page, page_size=len(page))
            inserted += max(cur.rowcount, 0)
        conn.commit()
    return inserted


def backfill_from_raw(batch_size: int = 2000) -> int:
    """Backfill the version layer from the immutable existing Raw snapshot."""
    batch_id = f"fina-indicator-backfill-{uuid.uuid4().hex}"
    inserted = 0
    with get_connection() as read_conn, read_conn.cursor(name="fina_indicator_raw_backfill") as read_cur:
        read_cur.itersize = batch_size
        read_cur.execute("SELECT payload, fetched_at FROM tushare_fina_indicator_raw ORDER BY ts_code, ann_date, end_date")
        while True:
            rows = read_cur.fetchmany(batch_size)
            if not rows:
                break
            records = []
            for payload, fetched_at in rows:
                record = payload if isinstance(payload, dict) else json.loads(payload)
                record["_fetched_at"] = fetched_at
                records.append(record)
            # Keep the original Raw fetch time in the version layer.
            values = []
            for record in records:
                fetched_at = record.pop("_fetched_at", None)
                values.append(_version_values(record, batch_id, fetched_at))
            _, execute_values = require_psycopg2()
            with get_connection() as write_conn, write_conn.cursor() as write_cur:
                write_cur.execute("SAVEPOINT fina_indicator_backfill_page")
                execute_values(write_cur, """
                    INSERT INTO fina_indicator_raw_version
                    (business_key, ts_code, ann_date, f_ann_date, end_date, report_type,
                     comp_type, end_type, update_flag, payload, source, batch_id,
                     payload_hash, fetched_at)
                    VALUES %s
                    ON CONFLICT (business_key, payload_hash) DO NOTHING
                """, values, page_size=len(values))
                inserted += max(write_cur.rowcount, 0)
                write_conn.commit()
    return inserted


def _number_sql(field: str) -> str:
    return f"NULLIF(NULLIF(payload->>'{field}', ''), 'NaN')::double precision"


def rebuild_standard() -> int:
    """Rebuild Standard only from the version layer; Raw is untouched."""
    numeric = {
        field: _number_sql(field)
        for field in (
            "roe", "roa", "debt_to_assets", "grossprofit_margin", "netprofit_margin",
            "ocf_to_or", "eps", "bps", "or_yoy", "tr_yoy", "q_sales_yoy", "netprofit_yoy",
        )
    }
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE fina_indicator_standard RESTART IDENTITY")
        cur.execute(f"""
            INSERT INTO fina_indicator_standard
            (ts_code, report_date, ann_date, f_ann_date, effective_announce_date,
              version_available_date,
              vintage_quality, source_observed_at,
              report_type, comp_type, end_type, update_flag, roe, roa, debt_to_assets,
             grossprofit_margin, netprofit_margin, ocf_to_or, eps, bps, or_yoy,
             tr_yoy, q_sales_yoy, netprofit_yoy, source, source_record_key,
             record_version, payload_hash, quality_status, fetched_at)
            WITH base AS (
                SELECT r.*,
                       concat_ws(':', ts_code, end_date::text,
                                 COALESCE(report_type,''), COALESCE(comp_type,''),
                                 COALESCE(end_type,'')) AS logical_business_key,
                       COALESCE(f_ann_date, ann_date) AS effective_date
                  FROM fina_indicator_raw_version r
            ), deduplicated AS (
                SELECT DISTINCT ON (logical_business_key, payload_hash) *
                  FROM base
                 ORDER BY logical_business_key, payload_hash, fetched_at, record_id
            ), versioned AS (
                SELECT d.*,
                       COUNT(*) OVER (
                           PARTITION BY logical_business_key, effective_date
                       ) > 1 AS ambiguous_same_day_revision
                  FROM deduplicated d
            )
            SELECT ts_code, end_date, ann_date, f_ann_date,
                   COALESCE(f_ann_date, ann_date),
                   CASE WHEN ambiguous_same_day_revision
                        THEN GREATEST(COALESCE(f_ann_date, ann_date), fetched_at::date) + 1
                        ELSE COALESCE(f_ann_date, ann_date, fetched_at::date) + 1
                   END,
                   CASE WHEN ambiguous_same_day_revision
                        THEN 'WAREHOUSE_OBSERVED_ONLY'
                        ELSE 'SOURCE_ANNOUNCEMENT_ASSUMED' END,
                   fetched_at,
                   report_type, comp_type, end_type,
                   update_flag, {numeric['roe']}, {numeric['roa']}, {numeric['debt_to_assets']},
                   {numeric['grossprofit_margin']}, {numeric['netprofit_margin']},
                   {numeric['ocf_to_or']}, {numeric['eps']}, {numeric['bps']},
                   {numeric['or_yoy']}, {numeric['tr_yoy']}, {numeric['q_sales_yoy']},
                   {numeric['netprofit_yoy']}, source, business_key, record_id,
                   payload_hash,
                   CASE WHEN end_date IS NULL OR COALESCE(f_ann_date, ann_date) IS NULL
                         THEN 'MISSING_SOURCE' ELSE 'VALID' END,
                   fetched_at
            FROM versioned
        """)
        inserted = cur.rowcount
        conn.commit()
    return inserted


def run(backfill: bool = True, convert: bool = True, batch_size: int = 2000) -> dict:
    result = {}
    if backfill:
        result["version_backfill"] = backfill_from_raw(batch_size)
    if convert:
        result["standard_conversion"] = rebuild_standard()
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="fina_indicator Raw version and Standard rebuild")
    parser.add_argument("--no-backfill", action="store_true")
    parser.add_argument("--no-convert", action="store_true")
    parser.add_argument("--batch-size", type=int, default=2000)
    args = parser.parse_args()
    print(run(not args.no_backfill, not args.no_convert, args.batch_size))
