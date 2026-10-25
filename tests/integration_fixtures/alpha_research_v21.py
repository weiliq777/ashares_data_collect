"""Synthetic, test-only research-v2.1 data for the isolated integration DB.

The fixture exercises the real Snapshot and Dataset repositories with a small
synthetic universe. It is explicitly not market data and cannot establish
production release quality or investment performance.
"""
from __future__ import annotations

import hashlib
import json
import os
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from data_collect.alpha_research.contracts import SOURCE_QUALITY_EVIDENCE_VERSION
from data_collect.data_access import DataAccessKernel, RunContext
from data_collect.data_access.corporate_action_evidence import EX_DATE_EVIDENCE_VERSION
from data_collect.data_access.policy_catalog import build_default_registry
from data_collect.data_access.repository.dataset_repository import DatasetRepository
from data_collect.data_access.repository.release_repository import ReleaseRepository
from data_collect.data_access.repository.research_snapshot_repository import (
    ResearchSnapshotRepository,
)
from data_collect.jobs import backtest_dataset, research_snapshot

FIXTURE_VERSION = "alpha-research-v21-synthetic-v3"
RELEASE_TAG = f"it-{FIXTURE_VERSION}"
DATASET_ID = "a26bc832-f4c2-43f2-962c-bc7c1770c52e"
DATASET_NAME = "Integration fixture: Alpha Research v2.1"
DATASET_VERSION = FIXTURE_VERSION
SCHEMA_VERSION = "research-v2.1"
BASELINE_SCHEMA = "feature-contract-v2.0"
BASELINE_CUTOFF = date(2025, 1, 31)
EVALUATION_START = date(2024, 9, 12)
SIGNAL_SESSIONS = 23
FUTURE_SESSIONS = 60
TOTAL_SESSIONS = SIGNAL_SESSIONS + FUTURE_SESSIONS
SECURITY_COUNT = 20
BENCHMARK_CODE = "000300.SH"
TICKER_PREFIX = "RGL"
TEST_FIXTURE_MARKER = "TEST_FIXTURE"
CODE_ARCHIVE_PATH = Path(__file__).resolve()
SHANGHAI = ZoneInfo("Asia/Shanghai")


def _canonical_hash(value: Any) -> str:
    encoded = json.dumps(
        value, ensure_ascii=False, sort_keys=True,
        separators=(",", ":"), default=str,
    )
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _payload_hash(payload: dict[str, Any]) -> str:
    return _canonical_hash(payload)


def _sessions(start: date, count: int) -> list[date]:
    """Return deterministic synthetic weekdays; this is not an SSE calendar."""
    result: list[date] = []
    current = start
    while len(result) < count:
        if current.weekday() < 5:
            result.append(current)
        current += timedelta(days=1)
    return result


def _price_dates(sessions: list[date]) -> list[date]:
    result: list[date] = []
    current = sessions[0] - timedelta(days=1)
    while len(result) < 60:
        if current.weekday() < 5:
            result.append(current)
        current -= timedelta(days=1)
    return list(reversed(result)) + sessions


def _symbols() -> list[str]:
    return [f"{TICKER_PREFIX}{index:03d}.SZ" for index in range(SECURITY_COUNT)]


def _context(name: str) -> RunContext:
    return RunContext(
        run_id=f"{TEST_FIXTURE_MARKER.lower()}-{name}-{FIXTURE_VERSION}",
        job_name=f"{TEST_FIXTURE_MARKER.lower()}_{name}",
        code_version=FIXTURE_VERSION,
        config_hash=_canonical_hash({"fixture": FIXTURE_VERSION}),
        pipeline_version="integration-test-v1",
    )


def _require_test_database(kernel: DataAccessKernel) -> None:
    expected = os.environ.get("ASHARES_TEST_ENVIRONMENT_ID")
    if os.environ.get("ASHARES_ENV") != "integration_test" or not expected:
        raise RuntimeError("fixture refuses to run outside integration_test")
    connection = kernel._db_connection()
    with connection.cursor() as cursor:
        cursor.execute(
            """SELECT current_database(), current_user,
                      environment_id::text, environment_kind
                 FROM integration_test.environment_identity
                WHERE singleton=TRUE"""
        )
        row = cursor.fetchone()
    if row != ("ashares_test", "ashares_app", expected, TEST_FIXTURE_MARKER):
        raise RuntimeError(f"fixture connected to an unexpected database identity: {row!r}")


def _insert_many(cursor, sql: str, rows: list[tuple[Any, ...]]) -> None:
    if rows:
        cursor.executemany(sql, rows)


def _seed_source_facts(kernel: DataAccessKernel) -> tuple[list[date], list[date], list[str]]:
    sessions = _sessions(EVALUATION_START, TOTAL_SESSIONS)
    price_dates = _price_dates(sessions)
    symbols = _symbols()
    conn = kernel._db_connection()
    conn.rollback()
    try:
        with conn.cursor() as cursor:
            stock_rows = []
            for index, symbol in enumerate(symbols):
                payload = {
                    "ts_code": symbol, "symbol": symbol[:6],
                    "name": f"Synthetic {index:03d}", "market": "主板",
                    "list_status": "L", "list_date": "20100101", "delist_date": "",
                }
                key = f"{symbol}:stock_basic:{FIXTURE_VERSION}"
                stock_rows.append((
                    symbol, key, json.dumps(payload, ensure_ascii=False),
                    _payload_hash(payload), TEST_FIXTURE_MARKER, FIXTURE_VERSION,
                    datetime.combine(price_dates[0], time(8, 0)),
                ))
            _insert_many(cursor, """INSERT INTO public.tushare_stock_basic_raw_version
                (ts_code,business_key,payload,payload_hash,source,batch_id,fetched_at)
                VALUES (%s,%s,%s::jsonb,%s,%s,%s,%s) ON CONFLICT DO NOTHING""", stock_rows)

            calendar_rows = []
            previous_open = None
            open_set = set(sessions)
            calendar_day = sessions[0]
            while calendar_day <= sessions[-1]:
                is_open = calendar_day in open_set
                payload = {
                    "exchange": "SSE", "cal_date": calendar_day.strftime("%Y%m%d"),
                    "is_open": "1" if is_open else "0",
                    "pretrade_date": previous_open.strftime("%Y%m%d") if previous_open else "",
                }
                calendar_rows.append((
                    "trade_cal", f"SSE:{calendar_day.isoformat()}", None, None,
                    "SSE", calendar_day, json.dumps(payload), TEST_FIXTURE_MARKER,
                    FIXTURE_VERSION, _payload_hash(payload),
                    datetime.combine(calendar_day, time(17, 0)),
                ))
                if is_open:
                    previous_open = calendar_day
                calendar_day += timedelta(days=1)
            _insert_many(cursor, """INSERT INTO public.tushare_market_raw_version
                (dataset,business_key,ts_code,trade_date,exchange,cal_date,payload,source,batch_id,payload_hash,fetched_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s::jsonb,%s,%s,%s,%s) ON CONFLICT DO NOTHING""", calendar_rows)

            legacy_cal_payload = {
                "exchange": "SSE", "cal_date": sessions[0].strftime("%Y%m%d"),
                "is_open": "1", "pretrade_date": "",
            }
            cursor.execute("""INSERT INTO public.tushare_trade_cal_raw
                (exchange,cal_date,payload,source,fetched_at)
                VALUES ('SSE',%s,%s::jsonb,'tushare',%s) ON CONFLICT DO NOTHING""",
                (sessions[0], json.dumps(legacy_cal_payload), datetime.combine(sessions[0], time(17, 0))))

            all_days = []
            current = sessions[0]
            while current <= sessions[-1]:
                is_open = current in open_set
                pretrade = max((day for day in sessions if day < current), default=None)
                all_days.append(("SSE", current, is_open, pretrade, TEST_FIXTURE_MARKER,
                                 datetime.combine(current, time(17, 0))))
                current += timedelta(days=1)
            _insert_many(cursor, """INSERT INTO std.trading_calendar
                (exchange,trade_date,is_open,pretrade_date,source,fetched_at)
                VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING""", all_days)

            daily_rows = []
            std_rows = []
            limit_rows = []
            index_rows = []
            status_rows = []
            universe_rows = []
            tradeability_rows = []
            price_rows = []
            adjustment_rows = []
            valuation_rows = []
            shareholder_rows = []
            lifecycle_rows = []

            for ordinal, symbol in enumerate(symbols):
                base = 8.0 + ordinal * 0.8
                drift = (ordinal - (SECURITY_COUNT / 2)) * 0.00012
                closes: list[float] = []
                for position, day in enumerate(price_dates):
                    session_index = position - 60
                    close = round(base * ((1.0 + drift) ** session_index), 4)
                    prior_close = closes[-1] if closes else round(base * ((1.0 + drift) ** (session_index - 1)), 4)
                    open_price = round(prior_close * 1.001, 4)
                    high = round(max(open_price, close) * 1.01, 4)
                    low = round(min(open_price, close) * 0.99, 4)
                    volume = 1000.0 + ordinal
                    amount = volume * close / 10.0
                    closes.append(close)
                    std_rows.append((symbol, day, open_price, high, low, close,
                                     volume * 100.0, amount * 1000.0))

                    ma20 = sum(closes[-20:]) / 20 if len(closes) >= 20 else None
                    ma60 = sum(closes[-60:]) / 60 if len(closes) >= 60 else None
                    return20 = close / closes[-21] - 1.0 if len(closes) >= 21 else None
                    return60 = close / closes[-61] - 1.0 if len(closes) >= 61 else None
                    price_rows.append((
                        symbol, day, close, close, 1.0,
                        close / prior_close - 1.0 if prior_close else 0.0,
                        return20, return60, ma20, ma60,
                        ma60, 1.2 + ordinal / 100.0, len(closes),
                        min(20, len(closes)), min(60, len(closes)), min(120, len(closes)),
                        "VALID", "price_daily_v2", "point_in_time_total_return", day,
                        datetime.combine(day, time(17, 0)), close, close,
                        "point-in-time-price-v2",
                    ))
                    adjustment_rows.append((symbol, day, 1.0, close, 0,
                                            None, "point_in_time_total_return", "VALID", day,
                                            datetime.combine(day, time(17, 0)), 1.0, close))

                    if day in open_set:
                        pe = 8.0 + ordinal * 1.1
                        valuation_rows.append((
                            symbol, day, pe, 1.0 + ordinal / 10.0, 1.0,
                            1.0, 100000.0 + ordinal * 1000, 80000.0 + ordinal * 500,
                            0.2 + ordinal / 100.0, 0.2 + ordinal / 100.0,
                            0.2, 0.2, True, 1200, day - timedelta(days=1825),
                            1200, True, "VALID", day, pe, 1.0, 1.0, 0.0,
                            "TEST_FIXTURE", 0.0, day - timedelta(days=1825), day,
                            1200, True, datetime.combine(day, time(14, 0), SHANGHAI),
                            "feature-contract-v2.0",
                        ))
                        shareholder_rows.append((symbol, day, 0.0, 0, 0, "VALID"))

                        open_price = round(prior_close * 1.001, 4)
                        high = round(max(open_price, close) * 1.01, 4)
                        low = round(min(open_price, close) * 0.99, 4)
                        daily_payload = {
                            "open": open_price, "high": high, "low": low,
                            "close": close, "vol": volume, "amount": amount,
                        }
                        daily_rows.append((
                            "daily", f"{symbol}:{day.isoformat()}", symbol, day,
                            "SZ", None, json.dumps(daily_payload), TEST_FIXTURE_MARKER,
                            FIXTURE_VERSION, _payload_hash(daily_payload),
                            datetime.combine(day, time(16, 0)),
                        ))
                        limit_payload = {
                            "up_limit": round(prior_close * 1.10, 4),
                            "down_limit": round(prior_close * 0.90, 4),
                        }
                        limit_rows.append((
                            symbol, day, json.dumps(limit_payload), TEST_FIXTURE_MARKER,
                            FIXTURE_VERSION, _payload_hash(limit_payload),
                            datetime.combine(day, time(16, 0)),
                        ))
                        status_rows.append((
                            day, symbol, True, False, False, True, "NORMAL",
                            limit_payload["up_limit"], limit_payload["down_limit"],
                            close, None, TEST_FIXTURE_MARKER, "VALID",
                            datetime.combine(day, time(17, 0)),
                        ))
                        universe_rows.append((day, symbol, True, False, False, True,
                                              max(1, (day - date(2010, 1, 1)).days),
                                              True, None, "VALID", day))
                        tradeability_rows.append((day, symbol, True, True, True, False,
                                                  "NORMAL", None, "VALID", day))

                        benchmark_close = 3500.0 * (1.0 + (position * 0.0001))
                        benchmark_payload = {"close": benchmark_close}
                        index_rows.append((
                            BENCHMARK_CODE, day, json.dumps(benchmark_payload),
                            TEST_FIXTURE_MARKER, FIXTURE_VERSION,
                            _payload_hash(benchmark_payload),
                            datetime.combine(day, time(16, 0)),
                        ))

                lifecycle_rows.append((
                    symbol, symbol[:6], f"Synthetic {ordinal:03d}", "SZ", "主板", "L",
                    date(2010, 1, 1), None, date(2010, 1, 1), None,
                    TEST_FIXTURE_MARKER, f"{symbol}:lifecycle:{FIXTURE_VERSION}", 1,
                    _payload_hash({"symbol": symbol, "fixture": FIXTURE_VERSION}),
                    "VALID", datetime.combine(price_dates[0], time(8, 0)),
                ))

            _insert_many(cursor, """INSERT INTO std.daily_kline
                (stock_code,trade_date,open,high,low,close,volume,amount)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING""", std_rows)
            _insert_many(cursor, """INSERT INTO feature.price_daily_v2
                (ts_code,trade_date,raw_close,asof_close,asof_adjustment_factor,return_1d,
                 return_20d,return_60d,ma20,ma60,ma120,volume_ratio_20d,
                 available_observation_count,ma20_observation_count,ma60_observation_count,
                 ma120_observation_count,quality_status,feature_set_version,adjustment_mode,
                 source_asof_date,calculated_at,adj_close_base,total_return_price,
                 price_coordinate_contract_version)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT DO NOTHING""", price_rows)
            _insert_many(cursor, """INSERT INTO feature.price_adjustment_factor_asof
                (ts_code,trade_date,asof_adjustment_factor,asof_close,event_count,
                 source_event_max_ex_date,adjustment_mode,quality_status,source_asof_date,
                 calculated_at,adj_adjustment_factor,adj_close_base)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING""", adjustment_rows)
            _insert_many(cursor, """INSERT INTO feature.valuation_daily_v1
                (ts_code,trade_date,pe_ttm,pb,ps_ttm,dv_ratio,total_mv,circ_mv,pe_pct_3y,
                 pe_pct_5y,pb_pct_3y,pb_pct_5y,is_pe_meaningful,valuation_observation_count,
                 pe_window_start_date_5y,pe_observation_count_5y,has_full_pe_5y_history,
                 quality_status,source_asof_date,pe_ttm_vendor,ps_ttm_vendor,pb_vendor,
                 dv_ttm,dv_ttm_source,dv_ttm_percentile_5y,dv_window_start,dv_window_end,
                 dv_observation_days,dv_window_complete,data_available_timestamp,
                 feature_contract_version)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT DO NOTHING""", valuation_rows)
            _insert_many(cursor, """INSERT INTO feature.shareholder_return_v1
                (ts_code,asof_date,cash_div_12m,dividend_event_count_3y,
                 dividend_year_count_3y,quality_status)
                VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING""", shareholder_rows)
            _insert_many(cursor, """INSERT INTO std.security_lifecycle
                (ts_code,symbol,name,exchange,market,list_status,list_date,delist_date,
                 effective_from,effective_to,source,source_record_key,record_version,
                 source_payload_hash,quality_status,fetched_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT DO NOTHING""", lifecycle_rows)
            _insert_many(cursor, """INSERT INTO std.security_status_daily
                (trade_date,ts_code,is_listed,is_st,is_suspended,is_tradeable,limit_status,
                 up_limit,down_limit,close_price,status_reason,source,quality_status,fetched_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING""", status_rows)
            _insert_many(cursor, """INSERT INTO pit.universe_daily
                (trade_date,ts_code,is_listed_asof,is_st_asof,is_suspended_asof,
                 has_price_history,days_since_listing,eligible_by_market,exclusion_reason,
                 quality_status,source_asof_date)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING""", universe_rows)
            _insert_many(cursor, """INSERT INTO pit.security_tradeability_daily
                (trade_date,ts_code,is_tradeable,can_buy,can_sell,is_suspended,limit_status,
                 blocked_reason,quality_status,source_asof_date)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING""", tradeability_rows)
            _insert_many(cursor, """INSERT INTO public.tushare_market_raw_version
                (dataset,business_key,ts_code,trade_date,exchange,cal_date,payload,source,
                 batch_id,payload_hash,fetched_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s::jsonb,%s,%s,%s,%s) ON CONFLICT DO NOTHING""", daily_rows)
            _insert_many(cursor, """INSERT INTO public.tushare_stk_limit_raw
                (ts_code,trade_date,payload,source,batch_id,payload_hash,fetched_at)
                VALUES (%s,%s,%s::jsonb,%s,%s,%s,%s) ON CONFLICT DO NOTHING""", limit_rows)
            _insert_many(cursor, """INSERT INTO public.tushare_index_daily_raw
                (ts_code,trade_date,payload,source,batch_id,payload_hash,fetched_at)
                VALUES (%s,%s,%s::jsonb,%s,%s,%s,%s) ON CONFLICT DO NOTHING""", index_rows)

            # The lower watermark boundaries must be present even when there
            # are no matching suspension or implemented-dividend events.
            suspend_payload = {"suspend_type": "TEST_BOUNDARY_ONLY"}
            cursor.execute("""INSERT INTO public.tushare_suspend_d_raw
                (ts_code,trade_date,suspend_type,payload,source,batch_id,payload_hash,fetched_at)
                VALUES ('TST999.SZ',%s,'TEST',%s::jsonb,%s,%s,%s,%s) ON CONFLICT DO NOTHING""",
                (sessions[0], json.dumps(suspend_payload), TEST_FIXTURE_MARKER,
                 FIXTURE_VERSION, _payload_hash(suspend_payload), datetime.combine(sessions[0], time(16, 0))))
            dividend_payload = {"div_proc": "预案", "cash_div": 0, "stk_div": 0}
            cursor.execute("""INSERT INTO public.tushare_dividend_raw
                (ts_code,end_date,ann_date,div_proc,record_date,ex_date,payload,source,batch_id,payload_hash,fetched_at)
                VALUES ('TST999.SZ',%s,%s,'预案',NULL,NULL,%s::jsonb,%s,%s,%s,%s)
                ON CONFLICT DO NOTHING""",
                (sessions[0], sessions[0], json.dumps(dividend_payload), TEST_FIXTURE_MARKER,
                 FIXTURE_VERSION, _payload_hash(dividend_payload), datetime.combine(sessions[0], time(16, 0))))
            legacy_daily_payload = {"open": 1, "high": 1, "low": 1, "close": 1, "vol": 1, "amount": 1}
            cursor.execute("""INSERT INTO public.tushare_daily_raw
                (ts_code,trade_date,payload,source,fetched_at)
                VALUES ('TST999.SZ',%s,%s::jsonb,'tushare',%s) ON CONFLICT DO NOTHING""",
                (sessions[0], json.dumps(legacy_daily_payload), datetime.combine(sessions[0], time(17, 0))))

            financial_rows = []
            financial_v1_rows = []
            for ordinal, symbol in enumerate(symbols):
                fingerprint = _canonical_hash({"fixture": FIXTURE_VERSION, "symbol": symbol, "financial": 1})
                component_lineage = {
                    "__quality_evidence__": {
                        "version": SOURCE_QUALITY_EVIDENCE_VERSION,
                    },
                    "income": {"record_key": f"{symbol}:income:20230630", "record_version": 1,
                               "payload_hash": _canonical_hash({"income": symbol}),
                               "source_quality_status": "VALID",
                               "used_by": ["roe_ttm", "revenue_growth_yoy", "profit_growth_yoy"]},
                    "balancesheet": {"record_key": f"{symbol}:balancesheet:20230630", "record_version": 1,
                                      "payload_hash": _canonical_hash({"balancesheet": symbol}),
                                      "source_quality_status": "VALID",
                                      "used_by": ["roe_ttm"]},
                }
                available = datetime(2023, 8, 20, 16, 0, tzinfo=SHANGHAI)
                financial_rows.append((
                    symbol, date(2023, 6, 30), date(2023, 8, 20), available,
                    available, "1", "1", "1", 100.0, 80.0, 70.0,
                    0.7, 12.0 + ordinal / 10.0, 0.5, 0.1,
                    -10.0 + ordinal * 1.5, -5.0 + ordinal,
                    True, "VALID", "VALID", "VALID", fingerprint,
                    "SOURCE_ANNOUNCEMENT_ASSUMED", "VALID", "feature-contract-v2.0",
                    json.dumps(component_lineage),
                ))
                v1_hash = _canonical_hash({"fixture": FIXTURE_VERSION, "symbol": symbol, "financial_v1": 1})
                financial_v1_rows.append((
                    symbol, date(2023, 6, 30), date(2023, 8, 20), date(2023, 8, 20),
                    "SOURCE_ANNOUNCEMENT_ASSUMED", datetime(2023, 8, 20, 16, 0), 1,
                    f"{symbol}:financial-v1:20230630", 12.0 + ordinal / 10.0,
                    8.0 + ordinal / 20.0, 30.0 + ordinal / 10.0,
                    -10.0 + ordinal * 1.5, -10.0 + ordinal * 1.5,
                    -10.0 + ordinal * 1.5, -5.0 + ordinal,
                    100.0, 80.0, "VALID", v1_hash,
                ))
            _insert_many(cursor, """INSERT INTO feature.company_financial_v2
                (ts_code,report_date,effective_announce_date,data_available_timestamp,
                 source_observed_at,report_type,comp_type,end_type,
                 net_profit_consolidated_ttm,net_profit_parent_ttm,operating_cashflow_ttm,
                 ocf_to_net_profit_consolidated_ttm,roe_ttm,gross_margin_ttm,
                 net_margin_consolidated_ttm,revenue_growth_yoy,profit_growth_yoy,
                 ttm_complete,cashflow_quality_status,roe_quality_status,growth_quality_status,
                 source_fingerprint,pit_evidence_level,quality_status,feature_contract_version,
                 component_lineage)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT DO NOTHING""", financial_rows)
            _insert_many(cursor, """INSERT INTO feature.company_financial_v1
                (ts_code,report_date,effective_announce_date,version_available_date,
                 vintage_quality,source_observed_at,record_version,source_record_key,roe,roa,
                 debt_to_assets,revenue_yoy,tr_yoy,q_sales_yoy,netprofit_yoy,
                 operating_cashflow,net_profit,quality_status,payload_hash)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT DO NOTHING""", financial_v1_rows)
        conn.commit()
    except Exception:
        conn.rollback()
        raise

    return sessions, price_dates, symbols


def _quality_report() -> dict[str, Any]:
    return {
        "fixture_kind": TEST_FIXTURE_MARKER,
        "fixture_version": FIXTURE_VERSION,
        "full_market_quality_claim": False,
        "failures": {},
        "corporate_action_evidence_version": EX_DATE_EVIDENCE_VERSION,
        "implementation_without_ex_date": 0,
        "implementation_without_ex_date_scope": {
            "audit_version": EX_DATE_EVIDENCE_VERSION,
            "schema_contract_status": "PASS",
            "schema_contract_issues": {},
            "global_missing_count": 0,
            "unresolved_count": 0,
            "in_scope_unresolved_count": 0,
            "schema_extension_unresolved_count": 0,
            "records": [],
            "classification_counts": {},
        },
    }


def _source_manifest(kernel: DataAccessKernel, sessions: list[date]) -> dict[str, Any]:
    conn = kernel._db_connection()
    with conn.cursor() as cursor:
        cursor.execute("""SELECT
            (SELECT MAX(record_id) FROM public.tushare_market_raw_version WHERE dataset='daily') AS daily_id,
            (SELECT MAX(record_id) FROM public.tushare_market_raw_version WHERE dataset='trade_cal') AS cal_id,
            (SELECT MAX(record_id) FROM public.tushare_stock_basic_raw_version) AS stock_id,
            (SELECT MAX(record_id) FROM public.tushare_stk_limit_raw) AS limit_id,
            (SELECT MAX(record_id) FROM public.tushare_suspend_d_raw) AS suspend_id,
            (SELECT MAX(record_id) FROM public.tushare_dividend_raw) AS dividend_id,
            (SELECT MAX(record_id) FROM public.tushare_index_daily_raw) AS index_id,
            (SELECT MAX(fetched_at) FROM public.tushare_daily_raw WHERE source='tushare') AS legacy_daily_at,
            (SELECT MAX(fetched_at) FROM public.tushare_trade_cal_raw WHERE source='tushare') AS legacy_cal_at""")
        row = cursor.fetchone()
    daily_id, cal_id, stock_id, limit_id, suspend_id, dividend_id, index_id, legacy_daily_at, legacy_cal_at = row
    required = (daily_id, cal_id, stock_id, limit_id, suspend_id, dividend_id, index_id, legacy_daily_at, legacy_cal_at)
    if any(value is None for value in required):
        raise RuntimeError(f"test source manifest is missing a Dataset raw boundary: {required!r}")
    return {
        "schema_version": "tushare-release-source-v1",
        "cutoff_date": BASELINE_CUTOFF.isoformat(),
        "market_versions": {
            "daily": {"max_record_id": daily_id, "min_date": sessions[0].isoformat(), "max_date": sessions[-1].isoformat()},
            "trade_cal": {"max_record_id": cal_id, "min_date": sessions[0].isoformat(), "max_date": sessions[-1].isoformat()},
        },
        "legacy_market_raw": {
            "tushare_daily_raw": {"max_fetched_at": legacy_daily_at.isoformat(sep=" ")},
            "tushare_trade_cal_raw": {"max_fetched_at": legacy_cal_at.isoformat(sep=" ")},
        },
        "append_only_sources": {
            "tushare_stock_basic_raw_version": {"max_record_id": stock_id},
            "tushare_stk_limit_raw": {"max_record_id": limit_id},
            "tushare_suspend_d_raw": {"max_record_id": suspend_id},
            "tushare_dividend_raw": {"max_record_id": dividend_id},
            "tushare_index_daily_raw": {"max_record_id": index_id},
        },
    }


def _ensure_test_release(kernel: DataAccessKernel, sessions: list[date]) -> tuple[dict[str, Any], str, str]:
    report = _quality_report()
    quality_hash = _canonical_hash(report)
    source_manifest = _source_manifest(kernel, sessions)
    source_manifest_hash = _canonical_hash(source_manifest)
    context = _context("release")
    repository = ReleaseRepository(kernel)
    current = repository.get_release(RELEASE_TAG, context)
    if current:
        full = kernel._db_connection()
        with full.cursor() as cursor:
            cursor.execute("""SELECT status,quality_report_hash,source_manifest_hash,
                                      schema_version,baseline_domain
                                 FROM ops.tushare_data_release WHERE release_tag=%s""", (RELEASE_TAG,))
            actual = cursor.fetchone()
        expected = ("validated", quality_hash, source_manifest_hash, BASELINE_SCHEMA, "RESEARCH")
        if actual != expected:
            raise RuntimeError(f"existing test release identity differs from fixture: {actual!r}")
        return report, quality_hash, source_manifest_hash

    repository.create(
        RELEASE_TAG, BASELINE_CUTOFF,
        f"{TEST_FIXTURE_MARKER}; synthetic contract fixture; not production evidence",
        BASELINE_SCHEMA, context,
        data_cutoff_timestamp=datetime.combine(BASELINE_CUTOFF, time(17, 0), SHANGHAI),
        temporal_evidence_level="LEGACY",
    )
    repository.commit_validation({
        "tag": RELEASE_TAG,
        "baseline_domain": "RESEARCH",
        "cutoff": BASELINE_CUTOFF,
        "schema_version": BASELINE_SCHEMA,
        "data_cutoff_timestamp": datetime.combine(BASELINE_CUTOFF, time(17, 0), SHANGHAI),
        "temporal_evidence_level": "LEGACY",
        "visibility_policy_version": "integration-test-v1",
        "release_change_watermark": None,
        "gate": report,
        "quality_hash": quality_hash,
        "source_manifest": source_manifest,
        "source_manifest_hash": source_manifest_hash,
    }, context)
    return report, quality_hash, source_manifest_hash


def _require_test_rule(kernel: DataAccessKernel, effective_from: date) -> None:
    connection = kernel._db_connection()
    with connection.cursor() as cursor:
        cursor.execute("""SELECT COUNT(*)
                            FROM foundation.market_rule_effective m
                            JOIN foundation.tax_rule_effective t
                              ON t.market=m.market
                             AND t.tax_rule_version=%s
                             AND t.effective_from=m.effective_from
                            JOIN backtest.app_execution_params e
                              ON e.market=m.market
                             AND e.fee_rule_version=%s
                             AND e.effective_from=m.effective_from
                           WHERE m.rule_version=%s AND m.effective_from=%s
                             AND m.approval_status='APPROVED'
                             AND t.approval_status='APPROVED'
                             AND e.approval_status='APPROVED'
                             AND m.approved_by=%s AND t.approved_by=%s
                             AND e.approved_by=%s""",
                       ("research-fee-assumption-v3", "research-fee-assumption-v3",
                        FIXTURE_VERSION, effective_from, TEST_FIXTURE_MARKER,
                        TEST_FIXTURE_MARKER, TEST_FIXTURE_MARKER))
        if int(cursor.fetchone()[0]) != 1:
            raise RuntimeError(
                "missing TEST_FIXTURE execution rules; initialize them with "
                "tools/integration_test_db.ps1 -Action test"
            )


def _snapshot_quality_reports(snapshot_ids: list[str], sessions_by_id: dict[str, date], baseline) -> dict[str, tuple[dict[str, Any], str]]:
    base = dict(baseline.quality_report or {})
    reports = {}
    for snapshot_id in snapshot_ids:
        report = {**base, "cutoff_date": sessions_by_id[snapshot_id].isoformat()}
        reports[snapshot_id] = (report, _canonical_hash(report))
    return reports


def _ensure_snapshots(kernel: DataAccessKernel, sessions: list[date], report: dict[str, Any], baseline) -> list[str]:
    ids: list[str] = []
    for offset in range(0, len(sessions), 20):
        batch_dates = sessions[offset:offset + 20]
        records = research_snapshot.create_snapshots_batch(
            batch_dates, RELEASE_TAG, SCHEMA_VERSION,
            notes_prefix=f"{TEST_FIXTURE_MARKER}:{FIXTURE_VERSION}",
            verify_source_manifest=False,
        )
        ids.extend(str(item["snapshot_id"]) for item in records)

    sessions_by_id = {
        str(item["snapshot_id"]): date.fromisoformat(item["research_cutoff_date"])
        for offset in range(0, len(sessions), 20)
        for item in research_snapshot.create_snapshots_batch(
            sessions[offset:offset + 20], RELEASE_TAG, SCHEMA_VERSION,
            notes_prefix=f"{TEST_FIXTURE_MARKER}:{FIXTURE_VERSION}",
            verify_source_manifest=False,
        )
    }
    reports = _snapshot_quality_reports(ids, sessions_by_id, baseline)
    kernel_repo = ResearchSnapshotRepository(kernel)
    context = _context("snapshot-lifecycle")
    manifests = []
    for snapshot_id in ids:
        manifests.append(kernel_repo.manifest(snapshot_id, context))
    build_ids = [str(item["snapshot_id"]) for item in manifests if item["status"] in {"DRAFT", "FAILED"}]
    for offset in range(0, len(build_ids), 20):
        batch = build_ids[offset:offset + 20]
        research_snapshot.build_snapshots_batch(
            batch, baseline=baseline,
            quality_reports={snapshot_id: reports[snapshot_id] for snapshot_id in batch},
        )

    manifests = [kernel_repo.manifest(snapshot_id, context) for snapshot_id in ids]
    validate_ids = [str(item["snapshot_id"]) for item in manifests if item["status"] == "BUILDING"]
    for offset in range(0, len(validate_ids), 20):
        batch = validate_ids[offset:offset + 20]
        issues = research_snapshot.inspect_snapshots_batch(
            batch, baseline=baseline,
            quality_reports={snapshot_id: reports[snapshot_id] for snapshot_id in batch},
            allowed_statuses=("BUILDING",), verify_source_manifest=False,
            verify_global_checks=True,
        )
        if issues:
            raise RuntimeError(f"TEST_FIXTURE Snapshot validation findings: {issues}")
        research_snapshot.validate_snapshots_batch(
            batch, baseline=baseline,
            quality_reports={snapshot_id: reports[snapshot_id] for snapshot_id in batch},
        )

    manifests = [kernel_repo.manifest(snapshot_id, context) for snapshot_id in ids]
    freeze_ids = [str(item["snapshot_id"]) for item in manifests if item["status"] == "VALIDATED"]
    archive_hash, archive_size = research_snapshot._sha256_file(CODE_ARCHIVE_PATH)
    metadata = research_snapshot._git_metadata()
    # The job-level freeze also runs a full-market publishability gate. A
    # synthetic universe must not claim that gate passed, so the fixture uses
    # the same Snapshot Repository state transition after the full scoped
    # input, PIT and lineage validations above. The SQL FROZEN immutability
    # trigger remains active and is separately tested.
    for offset in range(0, len(freeze_ids), 20):
        batch = freeze_ids[offset:offset + 20]
        with kernel_repo.lifecycle_locks(batch):
            for snapshot_id in batch:
                manifest = kernel_repo.manifest(snapshot_id, context)
                kernel_repo.freeze_snapshot(
                    manifest, metadata,
                    {"path": str(CODE_ARCHIVE_PATH), "sha256": archive_hash,
                     "size": archive_size}, context,
                )

    final = [kernel_repo.manifest(snapshot_id, context) for snapshot_id in ids]
    if any(item["status"] != "FROZEN" for item in final):
        raise RuntimeError("TEST_FIXTURE Snapshot batch did not reach FROZEN")
    return ids


def ensure_alpha_research_dataset() -> dict[str, Any]:
    """Build or reuse the deterministic synthetic v2.1 Dataset in ashares_test."""
    kernel = DataAccessKernel(policy_registry=build_default_registry())
    try:
        _require_test_database(kernel)
        sessions, _price_dates_list, symbols = _seed_source_facts(kernel)
        report, _quality_hash, _source_hash = _ensure_test_release(kernel, sessions)
        baseline = ResearchSnapshotRepository(kernel).baseline(
            BASELINE_CUTOFF, RELEASE_TAG, _context("baseline"),
            verify_source_manifest=False,
        )
        if baseline.quality_report != report:
            raise RuntimeError("stored test release report changed from the deterministic fixture")
        _require_test_rule(kernel, sessions[0])
        snapshot_ids = _ensure_snapshots(kernel, sessions, report, baseline)

        dataset_repo = DatasetRepository(kernel)
        context = _context("dataset-manifest")
        existing = None
        try:
            existing = dataset_repo.manifest(DATASET_ID, context)
        except ValueError:
            pass
        if existing is None:
            backtest_dataset.create_dataset(
                snapshot_ids, DATASET_NAME, DATASET_VERSION, RELEASE_TAG,
                sessions[0], sessions[0], sessions[-1],
                out_of_sample_start=sessions[SIGNAL_SESSIONS],
                benchmark_code=BENCHMARK_CODE, dataset_id=DATASET_ID,
            )
            existing = dataset_repo.manifest(DATASET_ID, context)
        elif (
            str(existing["baseline"]) != RELEASE_TAG
            or existing["schema_version"] != backtest_dataset.SCHEMA_VERSION
            or existing["dataset_version"] != DATASET_VERSION
        ):
            raise RuntimeError("existing test Dataset identity differs from the fixture")

        build_result = None
        if existing["status"] in {"DRAFT", "FAILED", "BUILDING"}:
            build_result = backtest_dataset.build_dataset(DATASET_ID)
        validation = None
        current = dataset_repo.manifest(DATASET_ID, context)
        if current["status"] in {"BUILDING", "FAILED"}:
            validation = backtest_dataset.validate_dataset(
                DATASET_ID, resume_failed=current["status"] == "FAILED",
            )
        current = dataset_repo.manifest(DATASET_ID, context)
        if current["status"] == "VALIDATED":
            backtest_dataset.freeze_dataset(DATASET_ID, str(CODE_ARCHIVE_PATH))
        current = dataset_repo.manifest(DATASET_ID, context)
        if current["status"] != "FROZEN":
            raise RuntimeError(f"test Dataset lifecycle ended at {current['status']}")
        return {
            "dataset_id": DATASET_ID,
            "dataset_status": current["status"],
            "snapshot_count": len(snapshot_ids),
            "security_count": len(symbols),
            "signal_session_count": SIGNAL_SESSIONS,
            "forward_tail_session_count": FUTURE_SESSIONS,
            "build_result": build_result,
            "validation": validation,
            "fixture_version": FIXTURE_VERSION,
        }
    finally:
        kernel.close()
