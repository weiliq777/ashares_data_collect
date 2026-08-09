"""管理 Tushare 历史数据基线发布点。

发布点只控制增量起始日期，不修改任何 Raw 或 Standard 数据。
"""
from __future__ import annotations

import argparse
import hashlib
import json
from datetime import date, datetime
from decimal import Decimal

from data_collect.utils.db import get_connection, require_psycopg2


# Raw high-water marks and quality results must describe the same database
# generation. SHARE locks block Raw inserts and derived-table replacements
# while a release is validated, but still permit the read-only quality queries.
_RELEASE_INPUT_TABLES = (
    "tushare_market_raw_version", "tushare_daily_raw",
    "tushare_daily_basic_raw", "tushare_adj_factor_raw",
    "tushare_trade_cal_raw", "tushare_financial_raw_version",
    "fina_indicator_raw_version", "tushare_stock_st_raw",
    "tushare_suspend_d_raw", "tushare_stk_limit_raw",
    "tushare_namechange_raw", "tushare_dividend_raw",
    "tushare_index_daily_raw", "tushare_stock_basic_raw_version",
    "tushare_history_checkpoint", "stock_basic_info", "daily_kline",
    "valuation_daily", "adj_factor_daily", "trading_calendar",
    "fina_indicator_standard", "financial_income_standard",
    "financial_balance_sheet_standard", "financial_cash_flow_standard",
    "std.security_lifecycle", "std.security_status_daily",
    "std.dividend_event", "pit.universe_daily",
    "pit.security_tradeability_daily", "pit.financial_asof",
    "feature.price_daily_v1", "feature.price_adjustment_factor_asof",
    "feature.price_daily_v2", "feature.valuation_daily_v1",
    "feature.company_financial_v1", "feature.shareholder_return_v1",
)


def _lock_release_inputs(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "LOCK TABLE " + ", ".join(_RELEASE_INPUT_TABLES) + " IN SHARE MODE"
        )


def _json_safe(value):
    """Convert PostgreSQL numeric aggregates into JSON-native values."""
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    if isinstance(value, dict):
        return {key: _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    return value


def _quality_gate(cutoff: date | None = None) -> dict:
    """发布前执行阶段二硬门禁；失败时不改变任何发布状态。"""
    from data_collect.jobs.tushare_data_quality import inspect_phase2_checkpoint_completeness, inspect_phase2_invariants

    invariants = inspect_phase2_invariants()
    checkpoint = inspect_phase2_checkpoint_completeness()
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*) FROM std.security_lifecycle a
            JOIN std.security_lifecycle b
              ON a.ts_code=b.ts_code AND a.lifecycle_id < b.lifecycle_id
             AND a.effective_from <= COALESCE(b.effective_to, DATE '9999-12-31')
             AND b.effective_from <= COALESCE(a.effective_to, DATE '9999-12-31')
        """)
        lifecycle_overlap = cur.fetchone()[0]
        cur.execute("""
            SELECT COUNT(*) FROM (
              SELECT business_key FROM std.dividend_event
              GROUP BY business_key HAVING COUNT(*) > 1
            ) d
        """)
        dividend_duplicates = cur.fetchone()[0]
        cur.execute("""
            SELECT COUNT(*) FROM std.dividend_event
            WHERE implementation_status='实施' AND ex_date IS NULL
        """)
        implementation_without_ex_date = cur.fetchone()[0]

    failures = {}
    for name, value in invariants.items():
        if isinstance(value, dict) or value != 0:
            failures[name] = value
    for name, value in checkpoint.items():
        if value.get("missing", 0) != 0 or value.get("failed", 0) != 0:
            failures[f"checkpoint:{name}"] = value
    # 实施事件缺少 ex_date 属于来源事实警告：Standard 保留，Feature 已排除；
    # 不应因为上游缺字段而替换已验证基线。真正影响可审计性的重复/穿越/重叠仍阻断发布。
    for name, value in {
        "lifecycle_overlap": lifecycle_overlap,
        "dividend_business_key_duplicates": dividend_duplicates,
    }.items():
        if value != 0:
            failures[name] = value
    return _json_safe({"cutoff_date": cutoff.isoformat() if cutoff else None,
                       "invariants": invariants, "checkpoints": checkpoint,
                       "lifecycle_overlap": lifecycle_overlap,
                       "dividend_business_key_duplicates": dividend_duplicates,
                       "implementation_without_ex_date": implementation_without_ex_date,
                       "failures": failures})


def _source_manifest(conn, cutoff: date) -> tuple[dict, str]:
    """Capture immutable source boundaries for one release.

    A release must identify which Raw versions existed when it was validated;
    a cutoff date alone cannot reproduce corrections fetched later.  The
    manifest stores per-source high-water marks and legacy fallback boundaries.
    """
    manifest: dict = {
        "schema_version": "tushare-release-source-v1",
        "cutoff_date": cutoff.isoformat(),
        "market_versions": {},
        "legacy_market_raw": {},
        "append_only_sources": {},
    }
    with conn.cursor() as cur:
        for dataset, date_expression in {
            "daily": "trade_date",
            "daily_basic": "trade_date",
            "adj_factor": "trade_date",
            "trade_cal": "cal_date",
        }.items():
            cur.execute(
                f"""SELECT COUNT(*), MAX(record_id), MAX(fetched_at)
                       FROM tushare_market_raw_version
                      WHERE dataset=%s AND {date_expression} <= %s""",
                (dataset, cutoff),
            )
            count, max_record_id, max_fetched_at = cur.fetchone()
            manifest["market_versions"][dataset] = {
                "row_count": int(count or 0),
                "max_record_id": int(max_record_id) if max_record_id is not None else None,
                "max_fetched_at": max_fetched_at.isoformat() if max_fetched_at else None,
            }

        legacy_specs = {
            "tushare_daily_raw": "trade_date",
            "tushare_daily_basic_raw": "trade_date",
            "tushare_adj_factor_raw": "trade_date",
            "tushare_trade_cal_raw": "cal_date",
        }
        for table, date_column in legacy_specs.items():
            cur.execute(
                f'SELECT COUNT(*), MAX(fetched_at) FROM "{table}" WHERE {date_column} <= %s',
                (cutoff,),
            )
            count, max_fetched_at = cur.fetchone()
            manifest["legacy_market_raw"][table] = {
                "row_count": int(count or 0),
                "max_fetched_at": max_fetched_at.isoformat() if max_fetched_at else None,
            }

        append_only_specs = {
            "tushare_financial_raw_version": (
                "record_id", "COALESCE(f_ann_date, ann_date, fetched_at::date) <= %s"
            ),
            "fina_indicator_raw_version": (
                "record_id", "COALESCE(f_ann_date, ann_date, fetched_at::date) <= %s"
            ),
            "tushare_stock_st_raw": ("record_id", "trade_date <= %s"),
            "tushare_suspend_d_raw": ("record_id", "trade_date <= %s"),
            "tushare_stk_limit_raw": ("record_id", "trade_date <= %s"),
            "tushare_namechange_raw": (
                "record_id", "COALESCE(ann_date, start_date, fetched_at::date) <= %s"
            ),
            "tushare_dividend_raw": (
                "record_id", "COALESCE(ann_date, ex_date, fetched_at::date) <= %s"
            ),
            "tushare_index_daily_raw": ("record_id", "trade_date <= %s"),
            "tushare_stock_basic_raw_version": ("record_id", "TRUE"),
        }
        for table, (id_column, predicate) in append_only_specs.items():
            params = () if predicate == "TRUE" else (cutoff,)
            cur.execute(
                f'SELECT COUNT(*), MAX({id_column}), MAX(fetched_at) FROM "{table}" WHERE {predicate}',
                params,
            )
            count, max_record_id, max_fetched_at = cur.fetchone()
            manifest["append_only_sources"][table] = {
                "row_count": int(count or 0),
                "max_record_id": int(max_record_id) if max_record_id is not None else None,
                "max_fetched_at": max_fetched_at.isoformat() if max_fetched_at else None,
            }
    encoded = json.dumps(manifest, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return manifest, hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _locked_quality_and_source_manifest(cutoff: date) -> tuple[dict, dict, str]:
    """Read quality and Raw boundaries while all release inputs are stable."""
    conn = get_connection()
    try:
        conn.set_session(
            isolation_level="REPEATABLE READ", readonly=False, autocommit=False
        )
        _lock_release_inputs(conn)
        gate = _quality_gate(cutoff)
        source_manifest, source_manifest_hash = _source_manifest(conn, cutoff)
        conn.rollback()
        return gate, source_manifest, source_manifest_hash
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def create(tag: str, cutoff: str, notes: str = "", schema_version: str = "") -> None:
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """INSERT INTO tushare_data_release
               (release_tag, baseline_cutoff_date, status, notes, schema_version)
               VALUES (%s, %s, 'draft', %s, %s)""",
            (tag, datetime.strptime(cutoff, "%Y%m%d").date(), notes, schema_version or None),
        )
        conn.commit()


def validate(tag: str, quality_report_hash: str = "") -> None:
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT baseline_cutoff_date FROM tushare_data_release WHERE release_tag=%s AND status='draft'",
            (tag,),
        )
        row = cur.fetchone()
    if not row:
        raise RuntimeError(f"找不到 draft 发布点或发布点状态不正确: {tag}")
    cutoff = row[0]
    gate, source_manifest, source_manifest_hash = _locked_quality_and_source_manifest(
        cutoff
    )
    if gate["failures"]:
        raise RuntimeError(f"发布质量门禁未通过，保留现有 validated 基线: {gate['failures']}")
    canonical = json.dumps(gate, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
    computed_hash = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    if quality_report_hash and quality_report_hash != computed_hash:
        raise ValueError("外部 quality_report_hash 与实际质量门禁报告不一致")
    psycopg2, _ = require_psycopg2()
    with get_connection() as conn, conn.cursor() as cur:
        conn.set_session(isolation_level="REPEATABLE READ", readonly=False, autocommit=False)
        # Quality and source boundaries were captured under one release-input
        # lock above; do not recalculate either side independently here.
        # The unique partial index allows only one validated release. Supersede the
        # previous release first inside the same transaction; any later failure
        # rolls both updates back, so the project never observes a no-baseline gap.
        cur.execute(
            """UPDATE tushare_data_release
                  SET status='superseded'
                WHERE status='validated' AND release_tag<>%s""",
            (tag,),
        )
        cur.execute(
            """UPDATE tushare_data_release
               SET status='validated', quality_report_hash=%s, quality_report=%s,
                    source_manifest=%s, source_manifest_hash=%s,
                    validated_at=CURRENT_TIMESTAMP
               WHERE release_tag=%s AND status='draft'""",
            (computed_hash, psycopg2.extras.Json(gate),
             psycopg2.extras.Json(source_manifest), source_manifest_hash, tag),
        )
        if cur.rowcount != 1:
            conn.rollback()
            raise RuntimeError(f"找不到 draft 发布点或发布点状态不正确: {tag}")
        conn.commit()


def status() -> None:
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """SELECT release_tag, baseline_cutoff_date, status,
                      quality_report_hash, created_at, validated_at
               FROM tushare_data_release ORDER BY release_id"""
        )
        for row in cur.fetchall():
            print(" | ".join("" if value is None else str(value) for value in row))


def main() -> None:
    parser = argparse.ArgumentParser(description="Tushare 历史基线发布点管理")
    sub = parser.add_subparsers(dest="command", required=True)
    create_parser = sub.add_parser("create")
    create_parser.add_argument("--tag", required=True)
    create_parser.add_argument("--cutoff", required=True, help="YYYYMMDD")
    create_parser.add_argument("--notes", default="")
    create_parser.add_argument("--schema-version", default="")
    validate_parser = sub.add_parser("validate")
    validate_parser.add_argument("--tag", required=True)
    validate_parser.add_argument("--quality-report-hash", default="")
    sub.add_parser("status")
    args = parser.parse_args()
    if args.command == "create":
        create(args.tag, args.cutoff, args.notes, args.schema_version)
    elif args.command == "validate":
        validate(args.tag, args.quality_report_hash)
    else:
        status()


if __name__ == "__main__":
    main()
