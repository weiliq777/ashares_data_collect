"""Isolated import and downstream fixture for the bounded real golden sample.

This fixture starts from real frozen Snapshot rows, execution bars and
Standard financial events exported through the production app-role Repositories.
It intentionally does not claim Raw-to-Feature conversion or full-market
Release quality.
"""
from __future__ import annotations

import gzip
import hashlib
import json
import os
import uuid
from collections import defaultdict
from collections.abc import Callable
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from psycopg2.extras import Json

from data_collect.alpha_research.factors import factor_values
from data_collect.alpha_research.golden_sample import content_hash
from data_collect.alpha_research.labels import build_forward_labels
from data_collect.data_access import DataAccessKernel, RunContext, WorkScope
from data_collect.data_access.corporate_action_evidence import (
    DIVIDEND_EVENT_SUPPORTED_COLUMNS,
    EX_DATE_EVIDENCE_VERSION,
    summarize_missing_ex_date_scope,
)
from data_collect.data_access.policy_catalog import build_default_registry
from data_collect.data_access.repository.alpha_research_repository import (
    AlphaResearchRepository,
)
from data_collect.data_access.repository.dataset_repository import DatasetRepository
from data_collect.data_access.repository.financial_repository import FinancialRepository
from data_collect.data_access.repository.release_repository import ReleaseRepository
from data_collect.data_access.repository.research_snapshot_repository import (
    ResearchSnapshotRepository,
)
from data_collect.jobs import backtest_dataset, research_snapshot
from data_collect.jobs.tushare_foundation_features import (
    calculate_debt_to_assets_features,
    calculate_features,
)

SAMPLE_VERSION = os.environ.get("ASHARES_REAL_GOLDEN_SAMPLE_VERSION", "real-golden-v1")
if SAMPLE_VERSION not in {
    "real-golden-v1", "real-golden-v2", "real-golden-v3",
    "real-financial-window-v1",
}:
    raise RuntimeError(f"unsupported real golden sample version: {SAMPLE_VERSION}")
FIXTURE_MARKER = "REAL_SAMPLE_TEST_ONLY"
TEST_DATABASE = "ashares_test"
TEST_ROLE = "ashares_app"
_WINDOW_E2E_VERSION = os.environ.get(
    "ASHARES_REAL_GOLDEN_E2E_CONTRACT_VERSION", "real-financial-window-e2e-v6",
)
if SAMPLE_VERSION == "real-financial-window-v1" and _WINDOW_E2E_VERSION not in {
    "real-financial-window-e2e-v6", "real-financial-window-e2e-v7",
}:
    raise RuntimeError(f"unsupported real financial window E2E version: {_WINDOW_E2E_VERSION}")
E2E_CONTRACT_VERSION = (
    _WINDOW_E2E_VERSION
    if SAMPLE_VERSION == "real-financial-window-v1"
    else "real-golden-e2e-v6" if SAMPLE_VERSION == "real-golden-v3"
    else "real-golden-e2e-v5" if SAMPLE_VERSION == "real-golden-v2"
    else "real-golden-e2e-v4"
)
RELEASE_TAG = f"it-{E2E_CONTRACT_VERSION}"
DATASET_ID = (
    "21722345-aef0-55b6-a297-2517aa70412d"
    if SAMPLE_VERSION == "real-golden-v1"
    else str(uuid.uuid5(
        uuid.NAMESPACE_URL,
        "ashares_data_collect:real-financial-window-v1:research-v2.2"
        if E2E_CONTRACT_VERSION == "real-financial-window-e2e-v6"
        else f"ashares_data_collect:{SAMPLE_VERSION}:{E2E_CONTRACT_VERSION}:research-v2.2",
    ))
)
DATASET_NAME = f"REAL_SAMPLE_TEST_ONLY: {E2E_CONTRACT_VERSION}"
DATASET_VERSION = E2E_CONTRACT_VERSION
RESEARCH_SCHEMA = (
    "research-v2.1" if SAMPLE_VERSION == "real-golden-v1" else "research-v2.2"
)
BASELINE_SCHEMA = "feature-contract-v2.0"
BENCHMARK_CODE = "000300.SH"
MARKET_RULE_VERSION = (
    "real-financial-window-market-v1"
    if SAMPLE_VERSION == "real-financial-window-v1" else "cn-a-share-v2"
)
FEE_RULE_VERSION = (
    "real-financial-window-fees-v1"
    if SAMPLE_VERSION == "real-financial-window-v1" else "real-golden-fees-v1"
)
SHANGHAI = ZoneInfo("Asia/Shanghai")
ROOT = Path(__file__).resolve().parents[2]
BUNDLE_PATH = ROOT / ".codex" / "real-sample" / f"{SAMPLE_VERSION}.json.gz"
MANIFEST_PATH = ROOT / "tests" / "fixtures" / (
    "real_financial_window_v1.manifest.json"
    if SAMPLE_VERSION == "real-financial-window-v1"
    else f"real_golden_sample_{SAMPLE_VERSION.rsplit('-', 1)[1]}.manifest.json"
)


def _canonical(value: Any) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
        default=str, allow_nan=False,
    ).encode("utf-8")


def _context(name: str) -> RunContext:
    return RunContext(
        run_id=f"real-sample-{E2E_CONTRACT_VERSION}-{name}",
        job_name=f"real_sample_{name}",
        code_version=E2E_CONTRACT_VERSION,
        config_hash=hashlib.sha256(E2E_CONTRACT_VERSION.encode()).hexdigest(),
        pipeline_version=E2E_CONTRACT_VERSION,
    )


def _reference_comparison_passes(comparison: dict[str, Any]) -> bool:
    return (
        comparison.get("factor_match_status") == "PASS"
        and comparison.get("label_match_status") == "PASS"
    )


def _apply_reference_freeze_gate(
    dataset_status: str,
    comparison: dict[str, Any],
    freeze: Callable[[], None],
) -> str:
    if dataset_status != "VALIDATED":
        return f"NOT_APPLICABLE_{dataset_status}"
    if not _reference_comparison_passes(comparison):
        return "BLOCKED_REFERENCE_MISMATCH"
    freeze()
    return "FROZEN_AFTER_REFERENCE_PASS"


def _load_bundle() -> tuple[dict[str, Any], dict[str, Any], bytes, bytes]:
    compressed = BUNDLE_PATH.read_bytes()
    manifest_bytes = MANIFEST_PATH.read_bytes()
    manifest = json.loads(manifest_bytes.decode("utf-8"))
    bundle = json.loads(gzip.decompress(compressed).decode("utf-8"))
    if hashlib.sha256(compressed).hexdigest() != manifest["bundle_sha256"]:
        raise RuntimeError("real sample bundle does not match its immutable manifest")
    if bundle.get("sample_version") != SAMPLE_VERSION:
        raise RuntimeError("real sample bundle version mismatch")
    if bundle.get("source_dataset_id") != manifest["source_dataset"]["dataset_id"]:
        raise RuntimeError("real sample source Dataset identity mismatch")
    for key in (
        "observations", "execution_bars", "company_actions", "benchmark",
        "financial_standard_events", "financial_features_recomputed",
        "debt_to_assets_features", "factor_reference", "label_reference",
        "system_labels", "signal_observations",
    ):
        expected = manifest["content_hashes"].get(key)
        if expected and content_hash(bundle.get(key)) != expected:
            raise RuntimeError(f"real sample {key} content hash mismatch")
    if (
        SAMPLE_VERSION == "real-golden-v2"
        and bundle.get("schema_version") != "real-golden-sample-bundle-v2"
    ):
        raise RuntimeError("real-golden-v2 bundle schema mismatch")
    elif (
        SAMPLE_VERSION == "real-golden-v3"
        and bundle.get("schema_version") != "real-golden-sample-bundle-v3"
    ):
        raise RuntimeError("real-golden-v3 bundle schema mismatch")
    elif (
        SAMPLE_VERSION == "real-financial-window-v1"
        and bundle.get("schema_version") != "real-financial-window-bundle-v1"
    ):
        raise RuntimeError("real-financial-window-v1 bundle schema mismatch")
    return bundle, manifest, compressed, manifest_bytes


def _require_test_database(kernel: DataAccessKernel) -> None:
    expected = os.environ.get("ASHARES_TEST_ENVIRONMENT_ID")
    if os.environ.get("ASHARES_ENV") != "integration_test" or not expected:
        raise RuntimeError("real sample fixture refuses to run outside integration_test")
    connection = kernel._db_connection()
    with connection.cursor() as cursor:
        cursor.execute(
            """SELECT current_database(), current_user,
                      environment_id::text, environment_kind,
                      (SELECT rolsuper FROM pg_roles WHERE rolname=current_user)
                 FROM integration_test.environment_identity WHERE singleton=TRUE"""
        )
        row = cursor.fetchone()
    if row != (TEST_DATABASE, TEST_ROLE, expected, "TEST_FIXTURE", False):
        raise RuntimeError(f"refusing real-sample import into unexpected DB identity: {row!r}")
    with connection.cursor() as cursor:
        cursor.execute(
            """SELECT
                   (SELECT COUNT(*) FROM std.trading_calendar
                     WHERE source='TEST_FIXTURE'
                       AND trade_date BETWEEN DATE '2023-09-22' AND DATE '2023-12-25'),
                   (SELECT COUNT(*) FROM ops.tushare_data_release
                     WHERE release_tag='it-alpha-research-v21-synthetic-v3')"""
        )
        synthetic_rows = cursor.fetchone()
    if synthetic_rows != (0, 0):
        raise RuntimeError(
            "REAL_SAMPLE_TEST_ONLY must run in a clean fixture scope; "
            "synthetic TEST_FIXTURE calendar/release rows are present. "
            "Reset only the verified ashares_test volume before this run."
        )


def _parse_date(value: Any) -> date | None:
    if value is None or value == "":
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    return date.fromisoformat(str(value)[:10])


def _assert_debt_to_assets_source_identity(
    features: list[dict[str, Any]], standard_events: list[dict[str, Any]],
) -> None:
    """Reject fixture D/A rows detached from their exact Standard report period."""
    report_dates_by_identity: dict[tuple[str, str, int, str], set[date]] = defaultdict(set)
    for event in standard_events:
        if event.get("dataset") != "balance":
            continue
        identity = (
            str(event.get("ts_code") or ""),
            str(event.get("source_record_key") or ""),
            int(event["record_version"]) if event.get("record_version") is not None else 0,
            str(event.get("payload_hash") or "").lower(),
        )
        report_day = _parse_date(event.get("report_date"))
        if all(identity) and report_day is not None:
            report_dates_by_identity[identity].add(report_day)

    for feature in features:
        identity = (
            str(feature.get("ts_code") or ""),
            str(feature.get("source_record_key") or ""),
            int(feature["record_version"]) if feature.get("record_version") is not None else 0,
            str(feature.get("payload_hash") or "").lower(),
        )
        if not all(identity):
            continue
        source_dates = report_dates_by_identity.get(identity, set())
        if len(source_dates) != 1:
            raise RuntimeError(
                "D/A feature source identity does not resolve to exactly one Standard report_date"
            )
        if _parse_date(feature.get("report_date")) not in source_dates:
            raise RuntimeError(
                "D/A feature report_date does not match Standard source identity"
            )


def _parse_datetime(value: Any) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value
    raw = str(value)
    if raw.endswith("Z"):
        raw = f"{raw[:-1]}+00:00"
    return datetime.fromisoformat(raw)


def _exchange(ts_code: str) -> str:
    return str(ts_code).rsplit(".", 1)[-1]


def _insert_dict_rows(cursor, table: str, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    schema, name = table.split(".", 1)
    cursor.execute(
        """SELECT column_name, data_type, is_identity, column_default
             FROM information_schema.columns
            WHERE table_schema=%s AND table_name=%s ORDER BY ordinal_position""",
        (schema, name),
    )
    metadata = cursor.fetchall()
    available = {item[0]: item[1:] for item in metadata}
    columns = [
        column for column in rows[0]
        if column in available
        and available[column][1] != "YES"
        and not (available[column][2] and "nextval(" in str(available[column][2]))
    ]
    if not columns:
        return 0
    statement = (
        f"INSERT INTO {table} ({', '.join(columns)}) VALUES "
        f"({', '.join(['%s'] * len(columns))}) ON CONFLICT DO NOTHING"
    )
    prepared = []
    for row in rows:
        values = []
        for column in columns:
            value = row.get(column)
            data_type = available[column][0]
            if value is not None and data_type == "date":
                value = _parse_date(value)
            elif value is not None and data_type in {"timestamp without time zone", "timestamp with time zone"}:
                value = _parse_datetime(value)
            elif value is not None and data_type in {"json", "jsonb"} and not isinstance(value, Json):
                value = Json(value)
            values.append(value)
        prepared.append(values)
    cursor.executemany(statement, prepared)
    return max(0, int(cursor.rowcount or 0))


def _parse_financial_events(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result = []
    for raw in events:
        row = dict(raw)
        for column in ("report_date", "effective_announce_date", "version_available_date"):
            if column in row:
                row[column] = _parse_date(row.get(column))
        for column in ("data_available_timestamp", "source_observed_at"):
            if column in row:
                row[column] = _parse_datetime(row.get(column))
        result.append(row)
    return result


def _recomputed_features(bundle: dict[str, Any]) -> list[dict[str, Any]]:
    events = _parse_financial_events(bundle["financial_standard_events"])
    output_end = (
        _parse_date(bundle["target_date"])
        if SAMPLE_VERSION == "real-financial-window-v1"
        else date(2023, 6, 30)
    )
    rows = calculate_features(events, date(2015, 1, 1), output_end)
    rows.sort(key=lambda row: (
        str(row.get("ts_code") or ""), row.get("report_date"),
        row.get("data_available_timestamp"), int(row.get("record_version") or 0),
    ))
    if content_hash(rows) != content_hash(bundle["financial_features_recomputed"]):
        raise RuntimeError("recomputed real financial features differ from exported references")
    return rows


def _financial_features_for_fixture(bundle: dict[str, Any]) -> list[dict[str, Any]]:
    """Use archived v1 outputs; v2 must match the current feature calculator."""
    if SAMPLE_VERSION == "real-golden-v1":
        # v1 records the previous unitless D/A output. Preserve it as a
        # historical oracle; do not pretend today's percentage contract
        # reproduces that version. Current behavior is verified by v2.
        return list(bundle["financial_features_recomputed"])
    if SAMPLE_VERSION == "real-financial-window-v1":
        # The financial-window importer recomputes these rows from Standard
        # events after they have been loaded into the isolated database.
        return []
    return _recomputed_features(bundle)


def _recomputed_debt_to_assets_features(bundle: dict[str, Any]) -> list[dict[str, Any]]:
    output_end = (
        _parse_date(bundle["target_date"])
        if SAMPLE_VERSION == "real-financial-window-v1"
        else date(2023, 6, 30)
    )
    events = _parse_financial_events(bundle["financial_standard_events"])
    _assert_debt_to_assets_source_identity(
        bundle["debt_to_assets_features"], events,
    )
    rows = calculate_debt_to_assets_features(
        events,
        date(2015, 1, 1), output_end,
    )
    _assert_debt_to_assets_source_identity(rows, events)
    rows.sort(key=lambda row: (
        str(row.get("ts_code") or ""), row.get("report_date"),
        row.get("data_available_timestamp"), int(row.get("record_version") or 0),
    ))
    if content_hash(rows) != content_hash(bundle["debt_to_assets_features"]):
        raise RuntimeError("recomputed D/A features differ from the immutable sample bundle")
    return rows


def _financial_standard_rows(events: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    """Map exported Standard facts to the minimal columns read by FinancialRepository."""
    table_by_dataset = {
        "income": "std.financial_income_standard",
        "cashflow": "std.financial_cash_flow_standard",
        "balance": "std.financial_balance_sheet_standard",
        "indicator": "std.fina_indicator_standard",
    }
    rows_by_dataset = {dataset: [] for dataset in table_by_dataset}
    for event in _parse_financial_events(events):
        dataset = str(event["dataset"])
        if dataset not in rows_by_dataset:
            raise RuntimeError(f"unsupported financial Standard event: {dataset}")
        values = event.get("values") or {}
        row = {
            "ts_code": event["ts_code"],
            "report_date": event["report_date"],
            "data_available_timestamp": event["data_available_timestamp"],
            "source_observed_at": event.get("source_observed_at"),
            "vintage_quality": event.get("vintage_quality"),
            "report_type": event.get("report_type"),
            "comp_type": event.get("comp_type"),
            "end_type": event.get("end_type"),
            "source_record_key": event.get("source_record_key"),
            "record_version": event.get("record_version"),
            "payload_hash": event.get("payload_hash"),
            "quality_status": event.get("quality_status") or "UNVERIFIED",
            "source": FIXTURE_MARKER,
            "fetched_at": event.get("source_observed_at"),
        }
        if dataset in {"income", "cashflow", "balance"}:
            row["stock_code"] = row.pop("ts_code")
        if dataset == "income":
            row.update({
                "revenue": values.get("revenue"),
                "oper_cost": values.get("oper_cost"),
                "total_cogs": values.get("total_cogs"),
                "net_profit_consolidated": values.get("net_profit_consolidated"),
                "net_profit_parent": values.get("net_profit_parent"),
            })
        elif dataset == "cashflow":
            row["operating_cashflow_ytd"] = values.get("operating_cashflow_ytd")
        elif dataset == "balance":
            row.update({
                "parent_equity_end": values.get("parent_equity_end"),
                "total_equity_end": values.get("total_equity_end"),
                "total_assets": values.get("total_assets"),
                "total_liab": values.get("total_liab"),
            })
        else:
            row.update({
                "roe_reported": values.get("roe_reported"),
                "roe_annualized": values.get("roe_annualized"),
                "roe_quarterly": values.get("roe_quarterly"),
                "debt_to_assets": values.get("debt_to_assets"),
                "linked_comp_type": event.get("comp_type"),
            })
        rows_by_dataset[dataset].append(row)
    return {
        table_by_dataset[dataset]: rows
        for dataset, rows in rows_by_dataset.items()
        if rows
    }


def _balance_sheet_standard_rows(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Import only the original balance-sheet Standard versions in the sample."""
    rows = []
    for event in _parse_financial_events(events):
        if event.get("dataset") != "balance":
            continue
        values = event.get("values") or {}
        rows.append({
            "stock_code": event.get("ts_code"),
            "report_date": event.get("report_date"),
            # Original announcement-date columns are not in this export.
            # Preserve that absence instead of manufacturing provenance.
            "announce_date": None,
            "version_available_date": None,
            "data_available_timestamp": event.get("data_available_timestamp"),
            "source_observed_at": event.get("source_observed_at"),
            "vintage_quality": event.get("vintage_quality"),
            "report_type": event.get("report_type"),
            "comp_type": event.get("comp_type"),
            "end_type": event.get("end_type"),
            "parent_equity_end": values.get("parent_equity_end"),
            "total_equity_end": values.get("total_equity_end"),
            "total_assets": values.get("total_assets"),
            "total_liab": values.get("total_liab"),
            "source": FIXTURE_MARKER,
            "source_record_key": event.get("source_record_key"),
            "record_version": event.get("record_version"),
            "quality_status": event.get("quality_status") or "UNVERIFIED",
            "payload_hash": event.get("payload_hash"),
            "fetched_at": event.get("source_observed_at"),
        })
    return rows


def _v2_factor_reference(
    bundle: dict[str, Any], da_features: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    signal_date = _parse_date(bundle["signal_date"])
    visible_by_code: dict[str, list[dict[str, Any]]] = defaultdict(list)
    cutoff = datetime.combine(signal_date, time(15, 0), SHANGHAI)
    for feature in da_features:
        if (
            feature.get("report_date") <= signal_date
            and feature.get("data_available_timestamp") is not None
            and feature["data_available_timestamp"] < cutoff
        ):
            visible_by_code[str(feature["ts_code"])].append(feature)

    reference = []
    for original in bundle["factor_reference"]:
        row = dict(original)
        code = str(row["ts_code"])
        visible = sorted(
            visible_by_code.get(code, ()),
            key=lambda item: (
                item["report_date"], item["data_available_timestamp"],
                int(item.get("record_version") or 0),
            ),
        )
        da = visible[-1] if visible else None
        factor_row = {
            "debt_to_assets": da.get("debt_to_assets") if da else None,
            "debt_to_assets_quality_status": da.get("quality_status") if da else None,
            "debt_to_assets_quality_reason": da.get("quality_reason") if da else None,
            "debt_to_assets_data_available_timestamp": da.get("data_available_timestamp") if da else None,
            "debt_to_assets_source_observed_at": da.get("source_observed_at") if da else None,
            "debt_to_assets_vintage_quality": da.get("vintage_quality") if da else None,
            "debt_to_assets_source_quality_status": da.get("source_quality_status") if da else None,
            "debt_to_assets_source_record_key": da.get("source_record_key") if da else None,
            "debt_to_assets_source_record_version": da.get("record_version") if da else None,
            "debt_to_assets_source_payload_hash": da.get("payload_hash") if da else None,
            "debt_to_assets_feature_contract_version": da.get("feature_contract_version") if da else None,
            "debt_to_assets_report_date": da.get("report_date") if da else None,
        }
        value = factor_values(factor_row)["debt_to_assets"]
        row["factors"] = {
            **row["factors"],
            "debt_to_assets": {
                "status": value.status, "reason": value.reason, "value": value.value,
            },
        }
        reference.append(row)
    return reference


def _seed_real_source_rows(cursor, bundle: dict[str, Any]) -> dict[str, int]:
    observations = bundle["observations"]
    bars = bundle["execution_bars"]
    codes = sorted({str(row["ts_code"]) for row in bundle["securities"]})
    dates = [_parse_date(day) for day in bundle["calendar"]]
    signal_date = _parse_date(bundle["signal_date"])

    # Calendar and versioned market rows are imported with a distinct source
    # marker. Closed calendar days are included so Dataset auxiliary rebuild
    # can prove complete date coverage without inventing open sessions.
    open_dates = set(dates)
    calendar_rows: list[dict[str, Any]] = []
    versioned_calendar_rows: list[dict[str, Any]] = []
    current = dates[0]
    previous_open = None
    while current <= dates[-1]:
        is_open = current in open_dates
        payload = {
            "exchange": "SSE", "cal_date": current.strftime("%Y%m%d"),
            "is_open": "1" if is_open else "0",
            "pretrade_date": previous_open.strftime("%Y%m%d") if previous_open else "",
        }
        calendar_rows.append({
            "exchange": "SSE", "trade_date": current, "is_open": is_open,
            "pretrade_date": previous_open, "source": FIXTURE_MARKER,
            "fetched_at": datetime.combine(current, time(17, 0)),
        })
        versioned_calendar_rows.append({
            "dataset": "trade_cal", "business_key": f"SSE:{current.isoformat()}",
            "exchange": "SSE", "cal_date": current,
            "payload": payload, "source": FIXTURE_MARKER,
            "batch_id": SAMPLE_VERSION, "payload_hash": content_hash(payload),
            "fetched_at": datetime.combine(current, time(17, 0)),
        })
        if is_open:
            previous_open = current
        current += timedelta(days=1)

    stocks = []
    lifecycle = []
    by_code: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in observations:
        by_code[str(row["ts_code"])].append(row)
    for code in codes:
        rows = by_code[code]
        first = rows[0]
        list_date = min((_parse_date(item.get("list_date")) for item in rows if item.get("list_date")), default=None)
        delist_date = min((_parse_date(item.get("delist_date")) for item in rows if item.get("delist_date")), default=None)
        payload = {
            "ts_code": code, "symbol": first.get("symbol") or code.split(".")[0],
            "name": first.get("name") or code, "market": first.get("market") or "主板",
            "list_status": first.get("list_status") or "L",
            "list_date": list_date.strftime("%Y%m%d") if list_date else "",
            "delist_date": delist_date.strftime("%Y%m%d") if delist_date else "",
        }
        source_key = first.get("instrument_source_record_key") or f"{code}:stock-basic:{SAMPLE_VERSION}"
        stocks.append({
            "ts_code": code, "business_key": source_key, "payload": payload,
            "payload_hash": first.get("instrument_source_payload_hash") or content_hash(payload),
            "source": FIXTURE_MARKER, "batch_id": SAMPLE_VERSION,
            "fetched_at": datetime.combine(signal_date, time(14, 0)),
        })
        lifecycle.append({
            "ts_code": code, "symbol": payload["symbol"], "name": payload["name"],
            "exchange": _exchange(code), "market": payload["market"],
            "list_status": "L", "list_date": list_date, "delist_date": delist_date,
            "effective_from": list_date or date(1990, 1, 1),
            "effective_to": delist_date, "source": FIXTURE_MARKER,
            "source_record_key": f"{code}:lifecycle:{SAMPLE_VERSION}",
            "record_version": 1,
            "source_payload_hash": first.get("instrument_source_payload_hash") or content_hash(payload),
            "quality_status": "VALID", "fetched_at": datetime.combine(signal_date, time(14, 0)),
        })

    universe, tradeability, prices, adjustments, valuations, shareholders, statuses = [], [], [], [], [], [], []
    standard_bars, raw_daily, limits, suspensions = [], [], [], []
    for obs in observations:
        day, code = _parse_date(obs["research_date"]), str(obs["ts_code"])
        tradeable = bool(obs.get("is_tradeable"))
        universe.append({
            "trade_date": day, "ts_code": code,
            "is_listed_asof": obs.get("is_listed_asof"),
            "is_st_asof": obs.get("is_st_asof"),
            "is_suspended_asof": obs.get("is_suspended_asof"),
            "has_price_history": bool(obs.get("available_observation_count")),
            "days_since_listing": max(0, (day - (_parse_date(obs.get("list_date")) or day)).days),
            "eligible_by_market": obs.get("eligible_by_market"),
            "exclusion_reason": None,
            "quality_status": "VALID" if obs.get("is_listed_asof") is not None else "WARNING",
            "source_asof_date": day,
        })
        tradeability.append({
            "trade_date": day, "ts_code": code, "is_tradeable": obs.get("is_tradeable"),
            "can_buy": obs.get("can_buy"), "can_sell": obs.get("can_sell"),
            "is_suspended": obs.get("is_suspended_asof"),
            "limit_status": obs.get("limit_status"), "blocked_reason": None,
            "quality_status": "VALID" if obs.get("is_tradeable") is not None else "WARNING",
            "source_asof_date": day,
        })
        close = obs.get("raw_close")
        if obs.get("price_feature_version"):
            price_quality = "WARNING" if close is None else "VALID"
            price_row = {
                "ts_code": code, "trade_date": day, "raw_close": close,
                "asof_close": obs.get("adj_close_base"),
                "asof_adjustment_factor": (
                    float(obs["adj_close_base"]) / float(close)
                    if close and obs.get("adj_close_base") is not None else None
                ),
                "return_1d": obs.get("return_1d"), "return_20d": obs.get("return_20d"),
                "return_60d": obs.get("return_60d"), "ma20": obs.get("ma20"),
                "ma60": obs.get("ma60"), "ma120": obs.get("ma120"),
                "volume_ratio_20d": obs.get("volume_ratio_20d"),
                "available_observation_count": obs.get("available_observation_count"),
                "ma20_observation_count": obs.get("ma20_observation_count"),
                "ma60_observation_count": obs.get("ma60_observation_count"),
                "ma120_observation_count": obs.get("ma120_observation_count"),
                "quality_status": price_quality,
                "feature_set_version": obs["price_feature_version"],
                "adjustment_mode": obs.get("price_adjustment_mode"),
                "source_asof_date": day,
                "calculated_at": datetime.combine(day, time(17, 0), SHANGHAI),
                "adj_close_base": obs.get("adj_close_base"), "total_return_price": obs.get("adj_close_base"),
                "price_coordinate_contract_version": "point-in-time-price-v2",
            }
            prices.append(price_row)
            adjustments.append({
                "ts_code": code, "trade_date": day,
                "asof_adjustment_factor": price_row["asof_adjustment_factor"],
                "asof_close": obs.get("adj_close_base"), "event_count": 0,
                "source_event_max_ex_date": None,
                "adjustment_mode": obs.get("price_adjustment_mode"),
                "quality_status": price_quality, "source_asof_date": day,
                "calculated_at": datetime.combine(day, time(17, 0), SHANGHAI),
                "adj_adjustment_factor": price_row["asof_adjustment_factor"],
                "adj_close_base": obs.get("adj_close_base"),
            })
        valuations.append({
            "ts_code": code, "trade_date": day,
            **{field: obs.get(field) for field in (
                "pe_ttm", "pb", "ps_ttm", "dv_ratio", "total_mv", "circ_mv",
                "pe_pct_3y", "pe_pct_5y", "pb_pct_3y", "pb_pct_5y",
                "is_pe_meaningful", "valuation_observation_count",
                "pe_window_start_date_5y", "has_full_pe_5y_history",
                "dv_ttm", "dv_ttm_source", "dv_ttm_percentile_5y",
                "dv_window_start", "dv_window_end", "dv_observation_days",
                "data_available_timestamp",
            )},
            # This feature contract requires an explicit boolean. Missing source
            # evidence is represented as an incomplete window, never as complete.
            "dv_window_complete": bool(obs.get("dv_window_complete", False)),
            "pe_observation_count_5y": obs.get("valuation_observation_count"),
            "quality_status": "VALID" if obs.get("pe_ttm") is not None else "WARNING",
            "source_asof_date": day, "pe_ttm_vendor": obs.get("pe_ttm"),
            "ps_ttm_vendor": obs.get("ps_ttm"), "pb_vendor": obs.get("pb"),
            "dv_ttm_vendor": obs.get("dv_ttm"),
            "feature_contract_version": "feature-contract-v2.0",
            "calculated_at": datetime.combine(day, time(17, 0), SHANGHAI),
        })
        shareholders.append({
            "ts_code": code, "asof_date": day, "cash_div_12m": obs.get("cash_div_12m"),
            "dividend_event_count_3y": obs.get("dividend_event_count_3y"),
            "dividend_year_count_3y": obs.get("dividend_year_count_3y"),
            "quality_status": "VALID" if obs.get("cash_div_12m") is not None else "WARNING",
            "feature_set_version": SAMPLE_VERSION,
        })
        bar = next(
            (item for item in bars if item["ts_code"] == code and _parse_date(item["trade_date"]) == day),
            None,
        )
        if bar is None:
            continue
        raw_payload = {
            "open": bar.get("raw_open"), "high": bar.get("raw_high"),
            "low": bar.get("raw_low"), "close": bar.get("raw_close"),
            "vol": float(bar["volume"]) / 100 if bar.get("volume") is not None else None,
            "amount": float(bar["amount"]) / 1000 if bar.get("amount") is not None else None,
        }
        bar_hash = bar.get("source_payload_hash") or content_hash(raw_payload)
        daily_key = bar.get("source_record_key") or f"{code}:{day.isoformat()}"
        # A Dataset execution row with no close is a missing-source marker,
        # not a Tushare daily fact. Do not copy that derived placeholder into
        # Raw/Standard: it would manufacture a source record and break the
        # price-vintage row-count contract.
        if bar.get("raw_close") is not None:
            standard_bars.append({
                "stock_code": code, "trade_date": day,
                "open": bar.get("raw_open"), "high": bar.get("raw_high"),
                "low": bar.get("raw_low"), "close": bar.get("raw_close"),
                "volume": bar.get("volume"), "amount": bar.get("amount"),
            })
            raw_daily.append({
                "dataset": "daily", "business_key": daily_key,
                "ts_code": code, "trade_date": day, "exchange": _exchange(code),
                "payload": raw_payload, "source": FIXTURE_MARKER,
                "batch_id": SAMPLE_VERSION, "payload_hash": bar_hash,
                "fetched_at": datetime.combine(day, time(16, 0)),
            })
        limit_payload = {"up_limit": bar.get("up_limit"), "down_limit": bar.get("down_limit")}
        limits.append({
            "ts_code": code, "trade_date": day, "payload": limit_payload,
            "source": FIXTURE_MARKER, "batch_id": SAMPLE_VERSION,
            "payload_hash": content_hash(limit_payload),
            "fetched_at": datetime.combine(day, time(16, 0)),
        })
        statuses.append({
            "trade_date": day, "ts_code": code,
            "is_listed": obs.get("is_listed_asof"), "is_st": obs.get("is_st_asof"),
            "is_suspended": bar.get("is_suspended"),
            "is_tradeable": tradeable, "limit_status": bar.get("limit_status"),
            "up_limit": bar.get("up_limit"), "down_limit": bar.get("down_limit"),
            "close_price": bar.get("raw_close"), "status_reason": None,
            "source": FIXTURE_MARKER,
            "quality_status": "VALID" if bar.get("raw_close") is not None else "WARNING",
            "fetched_at": datetime.combine(day, time(17, 0)),
        })
        if bar.get("is_suspended"):
            suspension_payload = {"suspend_type": "SUSPENDED", "source_record_key": daily_key}
            suspensions.append({
                "ts_code": code, "trade_date": day, "suspend_type": "S",
                "payload": suspension_payload, "source": FIXTURE_MARKER,
                "batch_id": SAMPLE_VERSION, "payload_hash": content_hash(suspension_payload),
                "fetched_at": datetime.combine(day, time(16, 0)),
            })

    actions = []
    raw_actions = []
    for action in bundle["company_actions"]:
        row = dict(action)
        row["report_date"] = _parse_date(row.get("report_date"))
        for field in (
            "announce_date", "implementation_announce_date", "record_date",
            "ex_date", "pay_date", "div_list_date",
        ):
            row[field] = _parse_date(row.get(field))
        action_payload = {
            "div_proc": row["implementation_status"], "stk_div": row.get("stock_div"),
            "stk_bo_rate": row.get("stock_bonus_rate"),
            "stk_co_rate": row.get("stock_conversion_rate"),
            "cash_div": row.get("cash_div"), "cash_div_tax": row.get("cash_div_tax"),
            "pay_date": row["pay_date"].strftime("%Y%m%d") if row.get("pay_date") else "",
            "div_listdate": row["div_list_date"].strftime("%Y%m%d") if row.get("div_list_date") else "",
            "imp_ann_date": row["implementation_announce_date"].strftime("%Y%m%d") if row.get("implementation_announce_date") else "",
        }
        row.update({
            "source": FIXTURE_MARKER,
            "source_record_key": row.get("source_record_key") or f"{row['ts_code']}:{row['ex_date']}:{SAMPLE_VERSION}",
            "record_version": 1,
            "business_key": row.get("source_record_key") or f"{row['ts_code']}:{row['ex_date']}:{SAMPLE_VERSION}",
            "payload_hash": row.get("source_payload_hash") or content_hash(action_payload),
            "quality_status": "VALID",
            "fetched_at": datetime.combine(row["announce_date"] or row["ex_date"], time(16, 0)),
        })
        actions.append(row)
        raw_actions.append({
            "ts_code": row["ts_code"], "end_date": row["report_date"],
            "ann_date": row["announce_date"], "div_proc": row["implementation_status"],
            "record_date": row["record_date"], "ex_date": row["ex_date"],
            "payload": action_payload, "source": FIXTURE_MARKER,
            "batch_id": SAMPLE_VERSION, "payload_hash": row["payload_hash"],
            "fetched_at": row["fetched_at"],
        })

    benchmark_rows = []
    raw_index = []
    for row in bundle["benchmark"]:
        day = _parse_date(row["trade_date"])
        payload = {"close": row["close"]}
        benchmark_rows.append({
            "ts_code": BENCHMARK_CODE, "trade_date": day,
            "payload": payload, "source": FIXTURE_MARKER,
            "batch_id": SAMPLE_VERSION, "payload_hash": content_hash(payload),
            "fetched_at": datetime.combine(day, time(16, 0)),
        })
        raw_index.append(benchmark_rows[-1])

    v1_features: dict[tuple[str, date, str, int], dict[str, Any]] = {}
    for obs in observations:
        report_date = _parse_date(obs.get("report_date"))
        if report_date is None:
            continue
        code = str(obs["ts_code"])
        record_version = int(obs.get("financial_record_version") or 0)
        source_key = str(obs.get("financial_source_record_key") or f"{code}:{report_date}:{record_version}")
        identity = (code, report_date, source_key, record_version)
        v1_features[identity] = {
            "ts_code": code, "report_date": report_date,
            "effective_announce_date": obs.get("effective_announce_date"),
            "version_available_date": obs.get("financial_version_available_date"),
            "vintage_quality": obs.get("financial_vintage_quality"),
            "source_observed_at": obs.get("financial_source_observed_at"),
            "record_version": record_version, "source_record_key": source_key,
            "roe": obs.get("roe"), "roa": obs.get("roa"),
            "debt_to_assets": obs.get("debt_to_assets"),
            "revenue_yoy": obs.get("revenue_yoy"), "tr_yoy": obs.get("tr_yoy"),
            "q_sales_yoy": obs.get("q_sales_yoy"),
            "netprofit_yoy": obs.get("netprofit_yoy"),
            "operating_cashflow": obs.get("operating_cashflow"),
            "net_profit": obs.get("net_profit"),
            "quality_status": "VALID" if obs.get("financial_payload_hash") else "WARNING",
            "payload_hash": obs.get("financial_payload_hash"),
            "feature_set_version": SAMPLE_VERSION,
        }

    features = _financial_features_for_fixture(bundle)
    balance_standard = []
    debt_to_assets_features = []
    financial_standard = {}
    if SAMPLE_VERSION in {"real-golden-v2", "real-golden-v3"}:
        balance_standard = _balance_sheet_standard_rows(
            bundle["financial_standard_events"],
        )
        debt_to_assets_features = _recomputed_debt_to_assets_features(bundle)
    elif SAMPLE_VERSION == "real-financial-window-v1":
        financial_standard = _financial_standard_rows(
            bundle["financial_standard_events"],
        )
    counts = {}
    counts["stock_basic"] = _insert_dict_rows(cursor, "public.tushare_stock_basic_raw_version", stocks)
    counts["versioned_calendar"] = _insert_dict_rows(cursor, "public.tushare_market_raw_version", versioned_calendar_rows)
    counts["std_calendar"] = _insert_dict_rows(cursor, "std.trading_calendar", calendar_rows)
    counts["lifecycle"] = _insert_dict_rows(cursor, "std.security_lifecycle", lifecycle)
    counts["universe"] = _insert_dict_rows(cursor, "pit.universe_daily", universe)
    counts["tradeability"] = _insert_dict_rows(cursor, "pit.security_tradeability_daily", tradeability)
    counts["price"] = _insert_dict_rows(cursor, "feature.price_daily_v2", prices)
    counts["price_adjustments"] = _insert_dict_rows(cursor, "feature.price_adjustment_factor_asof", adjustments)
    counts["valuation"] = _insert_dict_rows(cursor, "feature.valuation_daily_v1", valuations)
    counts["shareholder_return"] = _insert_dict_rows(cursor, "feature.shareholder_return_v1", shareholders)
    counts["std_daily"] = _insert_dict_rows(cursor, "std.daily_kline", standard_bars)
    counts["market_daily"] = _insert_dict_rows(cursor, "public.tushare_market_raw_version", raw_daily)
    counts["limits"] = _insert_dict_rows(cursor, "public.tushare_stk_limit_raw", limits)
    counts["suspensions"] = _insert_dict_rows(cursor, "public.tushare_suspend_d_raw", suspensions)
    counts["security_status"] = _insert_dict_rows(cursor, "std.security_status_daily", statuses)
    counts["dividends_raw"] = _insert_dict_rows(cursor, "public.tushare_dividend_raw", raw_actions)
    counts["dividends_standard"] = _insert_dict_rows(cursor, "std.dividend_event", actions)
    counts["benchmark"] = _insert_dict_rows(cursor, "public.tushare_index_daily_raw", raw_index)
    counts["financial_v1"] = _insert_dict_rows(cursor, "feature.company_financial_v1", list(v1_features.values()))
    counts["financial_v2"] = _insert_dict_rows(cursor, "feature.company_financial_v2", features)
    if SAMPLE_VERSION in {"real-golden-v2", "real-golden-v3"}:
        counts["balance_sheet_standard"] = _insert_dict_rows(
            cursor, "std.financial_balance_sheet_standard", balance_standard,
        )
        counts["debt_to_assets_v1"] = _insert_dict_rows(
            cursor, "feature.debt_to_assets_v1", debt_to_assets_features,
        )
    elif SAMPLE_VERSION == "real-financial-window-v1":
        for target, rows in financial_standard.items():
            counts[f"{target.replace('.', '_')}"] = _insert_dict_rows(cursor, target, rows)

    # Dataset manifests require a finite legacy source watermark even though
    # every selected market row is versioned. These non-member boundary rows
    # preserve the normal source contract and cannot enter the sample.
    legacy_day = dates[0]
    legacy_daily_payload = {"open": None, "high": None, "low": None, "close": None, "vol": None, "amount": None}
    counts["legacy_boundary"] = 0
    counts["legacy_boundary"] += _insert_dict_rows(cursor, "public.tushare_daily_raw", [{
        "ts_code": "ZZZ999.SZ", "trade_date": legacy_day,
        "payload": legacy_daily_payload, "source": "tushare",
        "fetched_at": datetime.combine(legacy_day, time(12, 0)),
    }])
    counts["legacy_boundary"] += _insert_dict_rows(cursor, "public.tushare_trade_cal_raw", [{
        "exchange": "SSE", "cal_date": legacy_day,
        "payload": {"exchange": "SSE", "cal_date": legacy_day.strftime("%Y%m%d"), "is_open": "1", "pretrade_date": ""},
        "source": "tushare", "fetched_at": datetime.combine(legacy_day, time(12, 0)),
    }])
    return counts


def _rebuild_window_features_from_test_standard(
    kernel: DataAccessKernel, bundle: dict[str, Any],
) -> dict[str, int]:
    """Recompute and append the bounded features from imported test Standard facts."""
    if SAMPLE_VERSION != "real-financial-window-v1":
        return {}

    codes = tuple(sorted({str(row["ts_code"]) for row in bundle["securities"]}))
    output_start = date(2015, 1, 1)
    output_end = _parse_date(bundle["target_date"])
    context = _context("financial-feature-rebuild")
    repository = FinancialRepository(kernel)
    events = repository.load_events(codes, output_start, output_end, context)
    expected_events = _parse_financial_events(bundle["financial_standard_events"])
    event_order = lambda row: (
        str(row.get("dataset") or ""), str(row.get("ts_code") or ""),
        str(row.get("report_date") or ""),
        str(row.get("data_available_timestamp") or ""),
        str(row.get("source_record_key") or ""),
        int(row.get("record_version") or 0), str(row.get("payload_hash") or ""),
    )
    if content_hash(sorted(events, key=event_order)) != content_hash(
        sorted(expected_events, key=event_order)
    ):
        raise RuntimeError("test Standard source events differ from the immutable real window bundle")

    financial_features = calculate_features(events, output_start, output_end)
    da_features = calculate_debt_to_assets_features(events, output_start, output_end)
    for rows, key in (
        (financial_features, "financial_features_recomputed"),
        (da_features, "debt_to_assets_features"),
    ):
        feature_order = lambda row: (
            str(row.get("ts_code") or ""), row.get("report_date"),
            str(row.get("data_available_timestamp") or ""),
            int(row.get("record_version") or 0),
        )
        rows.sort(key=feature_order)
        expected_rows = sorted(bundle[key], key=feature_order)
        if content_hash(rows) != content_hash(expected_rows):
            raise RuntimeError(f"test Standard recomputation differs from bundle: {key}")

    scope = WorkScope(
        date_from=output_start, date_to=output_end, codes=codes,
        dimensions={"date_column": "report_date", "code_column": "ts_code"},
    )
    financial_rows = repository._normalize_feature_rows(financial_features)
    financial_inserted = kernel.write.upsert(
        context, scope, "feature.company_financial_v2", financial_rows,
        key=("ts_code", "report_date", "data_available_timestamp", "source_fingerprint"),
        update_columns=(),
    )
    da_inserted = kernel.write.upsert(
        context, scope, "feature.debt_to_assets_v1", da_features,
        key=("source_fingerprint",), update_columns=(),
    )
    return {
        "standard_events_verified": len(events),
        "company_financial_v2_calculated": len(financial_features),
        "company_financial_v2_inserted": financial_inserted,
        "debt_to_assets_v1_calculated": len(da_features),
        "debt_to_assets_v1_inserted": da_inserted,
    }


def import_real_golden_sample() -> dict[str, Any]:
    """Idempotently import the immutable bundle and its bounded source facts."""
    bundle, manifest, compressed, manifest_bytes = _load_bundle()
    kernel = DataAccessKernel(policy_registry=build_default_registry())
    try:
        _require_test_database(kernel)
        connection = kernel._db_connection()
        connection.rollback()
        bundle_hash = hashlib.sha256(compressed).hexdigest()
        manifest_hash = hashlib.sha256(manifest_bytes).hexdigest()
        counts: dict[str, int] = {}
        with connection.cursor() as cursor:
            cursor.execute(
                """SELECT fixture_marker, loader_contract_version,
                          bundle_sha256, manifest_sha256,
                          bundle_bytes, bundle_payload
                     FROM integration_test.real_sample_import
                    WHERE sample_version=%s""",
                (SAMPLE_VERSION,),
            )
            existing = cursor.fetchone()
            if existing:
                if (
                    existing[:5] != (
                        FIXTURE_MARKER, E2E_CONTRACT_VERSION, bundle_hash,
                        manifest_hash, len(compressed),
                    )
                    or bytes(existing[5]) != compressed
                ):
                    raise RuntimeError("real sample version already exists with different immutable content")
                connection.rollback()
                import_status = "REUSED"
            else:
                cursor.execute(
                    """INSERT INTO integration_test.real_sample_import
                           (sample_version,environment_id,fixture_marker,loader_contract_version,bundle_sha256,
                            manifest_sha256,bundle_bytes,bundle_payload,manifest)
                         VALUES (%s,%s::uuid,%s,%s,%s,%s,%s,%s,%s::jsonb)""",
                    (
                        SAMPLE_VERSION, os.environ["ASHARES_TEST_ENVIRONMENT_ID"],
                        FIXTURE_MARKER, E2E_CONTRACT_VERSION, bundle_hash,
                        manifest_hash, len(compressed),
                        compressed, json.dumps(manifest, ensure_ascii=False),
                    ),
                )
                counts = _seed_real_source_rows(cursor, bundle)
                import_status = "IMPORTED"
            connection.commit()
        rebuilt = _rebuild_window_features_from_test_standard(kernel, bundle)
        return {
            "sample_version": SAMPLE_VERSION, "fixture_marker": FIXTURE_MARKER,
            "loader_contract_version": E2E_CONTRACT_VERSION,
            "status": import_status, "bundle_sha256": bundle_hash,
            "manifest_sha256": manifest_hash, "bundle_bytes": len(compressed),
            "source_rows": counts if import_status == "IMPORTED" else None,
            "recomputed_features": rebuilt,
        }
    except Exception:
        kernel._db_connection().rollback()
        raise
    finally:
        kernel.close()


def _quality_report() -> dict[str, Any]:
    bundle, _, _, _ = _load_bundle()
    missing_ex_date_rows = [
        row for row in bundle["company_actions"]
        if row.get("implementation_status") == "实施" and not row.get("ex_date")
    ]
    action_scope = summarize_missing_ex_date_scope(
        missing_ex_date_rows,
        scope_start=_parse_date(bundle["signal_date"]),
        scope_end=_parse_date(bundle["target_date"]),
        schema_columns=set(DIVIDEND_EVENT_SUPPORTED_COLUMNS),
    )
    return {
        "fixture_kind": FIXTURE_MARKER,
        "sample_version": SAMPLE_VERSION,
        "full_market_quality_claim": False,
        "bounded_fixture_action_count": len(bundle["company_actions"]),
        "failures": {},
        "corporate_action_evidence_version": EX_DATE_EVIDENCE_VERSION,
        "implementation_without_ex_date": action_scope["global_missing_count"],
        "implementation_without_ex_date_scope": action_scope,
        "quality_scope": "30 deterministic sampled securities and one bounded label window only",
    }


def _ensure_test_release(kernel: DataAccessKernel, cutoff: date) -> tuple[dict, str, str]:
    report = _quality_report()
    quality_hash = hashlib.sha256(_canonical(report)).hexdigest()
    repository = ReleaseRepository(kernel)
    context = _context("release")
    source_manifest, source_hash = repository.source_manifest(cutoff, context)
    current = repository.get_release(RELEASE_TAG, context)
    if current:
        connection = kernel._db_connection()
        with connection.cursor() as cursor:
            cursor.execute(
                """SELECT status,quality_report_hash,source_manifest_hash,
                          schema_version,baseline_domain
                     FROM ops.tushare_data_release WHERE release_tag=%s""",
                (RELEASE_TAG,),
            )
            stored = cursor.fetchone()
        if stored != ("validated", quality_hash, source_hash, BASELINE_SCHEMA, "RESEARCH"):
            raise RuntimeError(f"existing REAL_SAMPLE_TEST_ONLY Release identity differs: {stored!r}")
        return report, quality_hash, source_hash

    cutoff_timestamp = datetime.combine(cutoff, time(17, 0), SHANGHAI)
    repository.create(
        RELEASE_TAG, cutoff,
        f"{FIXTURE_MARKER}: bounded sample only; not production Release evidence",
        BASELINE_SCHEMA, context,
        data_cutoff_timestamp=cutoff_timestamp,
        temporal_evidence_level="WAREHOUSE_OBSERVED",
        visibility_policy_version=SAMPLE_VERSION,
    )
    repository.commit_validation({
        "tag": RELEASE_TAG, "baseline_domain": "RESEARCH", "cutoff": cutoff,
        "schema_version": BASELINE_SCHEMA, "data_cutoff_timestamp": cutoff_timestamp,
        "temporal_evidence_level": "WAREHOUSE_OBSERVED",
        "visibility_policy_version": SAMPLE_VERSION, "release_change_watermark": None,
        "gate": report, "quality_hash": quality_hash,
        "source_manifest": source_manifest, "source_manifest_hash": source_hash,
    }, context)
    return report, quality_hash, source_hash


def _snapshot_series(kernel: DataAccessKernel, dates: list[date]) -> list[str]:
    report, _, _ = _ensure_test_release(kernel, dates[-1])
    baseline_repo = ResearchSnapshotRepository(kernel)
    baseline = baseline_repo.baseline(dates[-1], RELEASE_TAG, _context("baseline"), verify_source_manifest=True)
    if baseline.quality_report != report:
        raise RuntimeError("stored REAL_SAMPLE_TEST_ONLY quality contract changed")
    context = _context("snapshot-create")
    existing = baseline_repo.existing_snapshots(dates, RELEASE_TAG, RESEARCH_SCHEMA, context)
    metadata = research_snapshot._git_metadata()
    reports = {}
    candidates = []
    for day in dates:
        snapshot_id = existing.get(day, (None, None))[0]
        if snapshot_id:
            continue
        snapshot_id = hashlib.sha256(
            f"{E2E_CONTRACT_VERSION}:{RELEASE_TAG}:{day.isoformat()}".encode()
        ).hexdigest()[:32]
        snapshot_id = f"{snapshot_id[:8]}-{snapshot_id[8:12]}-5{snapshot_id[13:16]}-a{snapshot_id[17:20]}-{snapshot_id[20:32]}"
        snapshot_report = {**report, "cutoff_date": day.isoformat()}
        report_hash = hashlib.sha256(_canonical(snapshot_report)).hexdigest()
        candidates.append({
            "snapshot_id": snapshot_id, "research_cutoff_date": day,
            "data_baseline_tag": RELEASE_TAG, "schema_version": RESEARCH_SCHEMA,
            "metadata": metadata, "quality_report_hash": report_hash,
            "baseline_quality_report_hash": baseline.quality_report_hash,
            "baseline_source_manifest_hash": baseline.source_manifest_hash,
            "quality_report": snapshot_report,
            "notes": f"{FIXTURE_MARKER} {SAMPLE_VERSION} cutoff={day.isoformat()}",
        })
        reports[snapshot_id] = (snapshot_report, report_hash)
    if candidates:
        baseline_repo.create_manifests(candidates, context)
    ids_by_day = {
        **{day: value[0] for day, value in existing.items()},
        **{item["research_cutoff_date"]: item["snapshot_id"] for item in candidates},
    }
    ids = [ids_by_day[day] for day in dates]
    for offset in range(0, len(ids), 20):
        batch = ids[offset:offset + 20]
        manifests = [baseline_repo.manifest(snapshot_id, context) for snapshot_id in batch]
        to_build = [item for item in manifests if item["status"] in {"DRAFT", "FAILED"}]
        if to_build:
            selected = [str(item["snapshot_id"]) for item in to_build]
            research_snapshot.build_snapshots_batch(
                selected, baseline=baseline,
                quality_reports={
                    snapshot_id: reports[snapshot_id]
                    if snapshot_id in reports
                    else (dict(report, cutoff_date=baseline_repo.manifest(snapshot_id, context)["cutoff"].isoformat()),
                          hashlib.sha256(_canonical(dict(report, cutoff_date=baseline_repo.manifest(snapshot_id, context)["cutoff"].isoformat()))).hexdigest())
                    for snapshot_id in selected
                },
            )
        built = [baseline_repo.manifest(snapshot_id, context) for snapshot_id in batch]
        building = [str(item["snapshot_id"]) for item in built if item["status"] == "BUILDING"]
        if building:
            issues = research_snapshot.inspect_snapshots_batch(
                building, baseline=baseline,
                quality_reports={
                    snapshot_id: reports.get(snapshot_id, (
                        dict(report, cutoff_date=baseline_repo.manifest(snapshot_id, context)["cutoff"].isoformat()),
                        hashlib.sha256(_canonical(dict(report, cutoff_date=baseline_repo.manifest(snapshot_id, context)["cutoff"].isoformat()))).hexdigest(),
                    )) for snapshot_id in building
                },
                allowed_statuses=("BUILDING",), verify_source_manifest=False,
                verify_global_checks=True,
            )
            if issues:
                raise RuntimeError(f"real sample Snapshot checks failed: {issues}")
            research_snapshot.validate_snapshots_batch(
                building, baseline=baseline,
                quality_reports={snapshot_id: reports.get(snapshot_id, (
                    dict(report, cutoff_date=baseline_repo.manifest(snapshot_id, context)["cutoff"].isoformat()),
                    hashlib.sha256(_canonical(dict(report, cutoff_date=baseline_repo.manifest(snapshot_id, context)["cutoff"].isoformat()))).hexdigest(),
                )) for snapshot_id in building},
            )
        validated = [baseline_repo.manifest(snapshot_id, context) for snapshot_id in batch]
        freeze_ids = [str(item["snapshot_id"]) for item in validated if item["status"] == "VALIDATED"]
        archive_hash, archive_size = research_snapshot._sha256_file(Path(__file__).resolve())
        if freeze_ids:
            with baseline_repo.lifecycle_locks(freeze_ids):
                for snapshot_id in freeze_ids:
                    current = baseline_repo.manifest(snapshot_id, context)
                    baseline_repo.freeze_snapshot(
                        current, metadata,
                        {"path": str(Path(__file__).resolve()), "sha256": archive_hash, "size": archive_size},
                        context,
                    )
    final = [baseline_repo.manifest(snapshot_id, context) for snapshot_id in ids]
    if any(row["status"] != "FROZEN" for row in final):
        raise RuntimeError(f"real sample Snapshot series did not freeze: {[row['status'] for row in final]}")
    return ids


def _verify_reference_mismatch_blocks_freeze(
    snapshot_ids: list[str], dates: list[date], comparison: dict[str, Any],
) -> dict[str, Any]:
    if E2E_CONTRACT_VERSION not in {
        "real-financial-window-e2e-v7", "real-golden-e2e-v6",
    }:
        return {"status": "NOT_APPLICABLE"}

    mismatch_dataset_id = str(uuid.uuid5(
        uuid.NAMESPACE_URL,
        f"ashares_data_collect:{E2E_CONTRACT_VERSION}:reference-mismatch-dataset",
    ))
    kernel = DataAccessKernel(policy_registry=build_default_registry())
    try:
        _require_test_database(kernel)
        repository = DatasetRepository(kernel)
        context = _context("reference-mismatch-gate")
        try:
            current = repository.manifest(mismatch_dataset_id, context)
        except ValueError:
            current = None
        if current is None:
            backtest_dataset.create_dataset(
                snapshot_ids,
                f"REAL_SAMPLE_TEST_ONLY: {E2E_CONTRACT_VERSION}: mismatch gate",
                f"golden-{SAMPLE_VERSION.rsplit('-', 1)[-1]}-mismatch-v1",
                RELEASE_TAG,
                dates[0], dates[0], dates[-1], out_of_sample_start=dates[-1],
                benchmark_code=BENCHMARK_CODE,
                calendar_rule_version="tushare-sse-v1",
                market_rule_version=MARKET_RULE_VERSION,
                fee_rule_version=FEE_RULE_VERSION,
                dataset_id=mismatch_dataset_id,
            )
            current = repository.manifest(mismatch_dataset_id, context)
        if current["status"] == "DRAFT" or (
            current["status"] in {"FAILED", "BUILDING"}
            and not current.get("execution_hash")
        ):
            backtest_dataset.build_dataset(mismatch_dataset_id)
            current = repository.manifest(mismatch_dataset_id, context)
        if current["status"] in {"BUILDING", "FAILED"}:
            backtest_dataset.validate_dataset(
                mismatch_dataset_id, resume_failed=current["status"] == "FAILED",
            )
            current = repository.manifest(mismatch_dataset_id, context)
        if current["status"] != "VALIDATED":
            raise RuntimeError(
                "reference-mismatch gate fixture must reach VALIDATED before the gate test: "
                f"{current['status']}"
            )

        injected_mismatch = {
            **comparison,
            "factor_match_status": "FAIL",
            "factor_mismatch_count": max(
                int(comparison.get("factor_mismatch_count") or 0), 1,
            ),
        }
        action = _apply_reference_freeze_gate(
            current["status"],
            injected_mismatch,
            lambda: backtest_dataset.freeze_dataset(
                mismatch_dataset_id,
                str(Path(__file__).resolve()),
                allow_validation_only=True,
            ),
        )
        after = repository.manifest(mismatch_dataset_id, context)
        return {
            "dataset_id": mismatch_dataset_id,
            "status_before": current["status"],
            "status_after": after["status"],
            "injected_factor_mismatch_count": injected_mismatch["factor_mismatch_count"],
            "freeze_action": action,
        }
    finally:
        kernel.close()


def ensure_real_golden_dataset() -> dict[str, Any]:
    """Import the fixed sample and attempt its real v2.1 Snapshot/Dataset path."""
    imported = import_real_golden_sample()
    bundle, _, _, _ = _load_bundle()
    kernel = DataAccessKernel(policy_registry=build_default_registry())
    try:
        _require_test_database(kernel)
        dates = [_parse_date(item) for item in bundle["calendar"]]
        snapshot_ids = _snapshot_series(kernel, dates)
        dataset_repo = DatasetRepository(kernel)
        context = _context("dataset")
        try:
            current = dataset_repo.manifest(DATASET_ID, context)
        except ValueError:
            current = None
        if current is None:
            backtest_dataset.create_dataset(
                snapshot_ids, DATASET_NAME, DATASET_VERSION, RELEASE_TAG,
                dates[0], dates[0], dates[-1], out_of_sample_start=dates[-1],
                benchmark_code=BENCHMARK_CODE,
                calendar_rule_version="tushare-sse-v1",
                market_rule_version=MARKET_RULE_VERSION,
                fee_rule_version=FEE_RULE_VERSION, dataset_id=DATASET_ID,
            )
            current = dataset_repo.manifest(DATASET_ID, context)
        if current["status"] == "DRAFT" or (
            current["status"] in {"FAILED", "BUILDING"}
            and not current.get("execution_hash")
        ):
            build = backtest_dataset.build_dataset(DATASET_ID)
        else:
            build = None
        current = dataset_repo.manifest(DATASET_ID, context)
        validation = None
        if current["status"] in {"BUILDING", "FAILED"}:
            try:
                validation = backtest_dataset.validate_dataset(
                    DATASET_ID, resume_failed=current["status"] == "FAILED",
                )
            except RuntimeError as exc:
                failed = dataset_repo.manifest(DATASET_ID, context)
                if failed["status"] != "FAILED":
                    raise
                validation = {
                    "status": "FAILED",
                    "error": str(exc),
                    "quality_report": failed.get("quality_report"),
                }
        current = dataset_repo.manifest(DATASET_ID, context)
        comparisons = _compare_materialized_sample(dataset_repo, context, dates, bundle)
        comparison_passed = (
            _reference_comparison_passes(comparisons)
        )
        freeze_gate = "PASS" if comparison_passed else "BLOCKED_REFERENCE_MISMATCH"
        dataset_status_before_freeze = current["status"]
        freeze_action = _apply_reference_freeze_gate(
            current["status"],
            comparisons,
            lambda: backtest_dataset.freeze_dataset(
                DATASET_ID, str(Path(__file__).resolve()), allow_validation_only=True,
            ),
        )
        if freeze_action == "FROZEN_AFTER_REFERENCE_PASS":
            current = dataset_repo.manifest(DATASET_ID, context)
        elif current["status"] != "VALIDATED":
            freeze_action = f"NOT_APPLICABLE_{dataset_status_before_freeze}"
        alpha_reader_comparison = None
        if (
            SAMPLE_VERSION in {"real-financial-window-v1", "real-golden-v3"}
            and current["status"] == "FROZEN"
        ):
            alpha_reader_comparison = _verify_alpha_reader(dataset_repo, bundle, dates)
        result = {
            "sample_version": SAMPLE_VERSION,
            "fixture_marker": FIXTURE_MARKER,
            "import": imported,
            "signal_dates": [
                _parse_date(day).isoformat()
                for day in bundle.get("signal_dates", [bundle["signal_date"]])
            ],
            "calendar_sessions": len(dates),
            "snapshot_count": len(snapshot_ids),
            "snapshot_statuses": sorted({row["status"] for row in (
                ResearchSnapshotRepository(kernel).manifest(snapshot_id, _context("snapshot-status"))
                for snapshot_id in snapshot_ids
            )}),
            "dataset_id": DATASET_ID,
            "dataset_status": current["status"],
            "dataset_build": build,
            "dataset_validation": validation,
            "dataset_quality_report": current.get("quality_report"),
            "dataset_freeze_gate": freeze_gate,
            "dataset_freeze_action": freeze_action,
            "independent_reference_comparison": comparisons,
            "alpha_reader_comparison": alpha_reader_comparison,
            "bundle_sha256": imported["bundle_sha256"],
        }
        alpha_failed = alpha_reader_comparison is not None and (
            alpha_reader_comparison["factor_match_status"] != "PASS"
            or alpha_reader_comparison["label_match_status"] != "PASS"
        )
        if (
            comparisons["factor_match_status"] != "PASS"
            or comparisons["label_match_status"] != "PASS"
            or alpha_failed
        ):
            result["end_to_end_status"] = "FAIL"
        elif current["status"] != "FROZEN":
            result["end_to_end_status"] = "BLOCKED"
        else:
            result["end_to_end_status"] = "PASS"
        result["reference_mismatch_freeze_gate"] = (
            _verify_reference_mismatch_blocks_freeze(
                snapshot_ids, dates, comparisons,
            )
        )
        result_path = ROOT / ".codex" / "real-sample" / "results" / f"{E2E_CONTRACT_VERSION}.json"
        result["result_artifact"] = str(result_path.relative_to(ROOT))
        result_path.parent.mkdir(parents=True, exist_ok=True)
        temporary = result_path.with_suffix(result_path.suffix + ".tmp")
        temporary.write_text(
            json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2, default=str) + "\n",
            encoding="utf-8",
        )
        temporary.replace(result_path)
        return result
    finally:
        kernel.close()


def _compare_materialized_sample(
    repository: DatasetRepository,
    context: RunContext,
    dates: list[date],
    bundle: dict[str, Any],
) -> dict[str, Any]:
    """Compare test Dataset reads to immutable factor/label references.

    This is a pre-freeze QA read. A FAILED Dataset remains ineligible for
    AlphaResearchRepository or formal research; validation evidence is not
    bypassed to make this diagnostic possible.
    """
    factor_columns = sorted({
        "research_date", "ts_code", "delist_date",
        "financial_feature_contract_version", "financial_vintage_quality",
        "financial_component_lineage", "financial_data_available_timestamp",
        "roe_quality_status", "growth_quality_status", "price_feature_version",
        "price_adjustment_mode",
        "debt_to_assets_quality_status", "debt_to_assets_quality_reason",
        "debt_to_assets_data_available_timestamp", "debt_to_assets_source_observed_at",
        "debt_to_assets_vintage_quality", "debt_to_assets_source_quality_status",
        "debt_to_assets_source_record_key", "debt_to_assets_source_record_version",
        "debt_to_assets_source_payload_hash", "debt_to_assets_feature_contract_version",
        "debt_to_assets_report_date",
        "pe_pct_5y", "pe_ttm", "is_pe_meaningful", "has_full_pe_5y_history",
        "roe_ttm", "debt_to_assets", "revenue_growth_yoy", "profit_growth_yoy",
        "return_20d", "return_60d", "adj_close_base", "ma60", "volume_ratio_20d",
    })
    signal_dates = {
        _parse_date(day) for day in bundle.get("signal_dates", [bundle["signal_date"]])
    }
    observations = repository.observation_page(
        DATASET_ID, factor_columns, context, date_from=dates[0], date_to=dates[-1],
    )
    signal_rows = [row for row in observations if row["research_date"] in signal_dates]
    factor_expected = {
        (_parse_date(row.get("research_date") or bundle["signal_date"]), str(row["ts_code"])): row
        for row in bundle["factor_reference"]
    }
    if len(signal_rows) != len(bundle["factor_reference"]):
        raise RuntimeError(
            "materialized Dataset signal Snapshot row count differs from the golden reference: "
            f"{len(signal_rows)} != {len(bundle['factor_reference'])}"
        )
    factor_count = 0
    factor_mismatches: list[dict[str, Any]] = []
    financial_feature_presence_mismatches: list[dict[str, Any]] = []
    missing_pit_factor_rows = 0
    for row in signal_rows:
        code = str(row["ts_code"])
        research_date = _parse_date(row["research_date"])
        expected_row = factor_expected[(research_date, code)]
        expected = expected_row["factors"]
        actual_feature_present = row.get("financial_data_available_timestamp") is not None
        if actual_feature_present != expected_row["financial_feature_present"]:
            financial_feature_presence_mismatches.append({
                "ts_code": code,
                "research_date": research_date.isoformat(),
                "expected": expected_row["financial_feature_present"],
                "actual": actual_feature_present,
            })
        actual = factor_values(row)
        if code == "688426.SH" and any(
            value.reason == "FINANCIAL_PIT_EVIDENCE_NOT_SOURCE_ANNOUNCEMENT"
            for name, value in actual.items()
            if name in {"roe_ttm", "revenue_growth_yoy", "profit_growth_yoy"}
        ):
            missing_pit_factor_rows += 1
        for name in expected:
            value = actual[name]
            reference = expected[name]
            difference = None
            if value.status != reference["status"] or value.reason != reference["reason"]:
                difference = "STATUS_OR_REASON"
            elif value.value is None or reference["value"] is None:
                if value.value is not reference["value"]:
                    difference = "NULLABILITY"
            elif abs(float(value.value) - float(reference["value"])) > 1e-12:
                difference = "VALUE"
            if difference:
                factor_mismatches.append({
                    "ts_code": code,
                    "research_date": research_date.isoformat(),
                    "factor": name,
                    "difference": difference,
                    "expected": reference,
                    "actual": {
                        "status": value.status,
                        "reason": value.reason,
                        "value": value.value,
                    },
                })
            factor_count += 1

    bars = repository.bar_page(
        DATASET_ID, context, limit=50_000,
        date_from=dates[0], date_to=dates[-1],
    )
    bars_by_day: dict[date, dict[str, dict[str, Any]]] = defaultdict(dict)
    unresolved_execution_cases = []
    for bar in bars:
        bars_by_day[_parse_date(bar["trade_date"])][str(bar["ts_code"])] = bar
        if (
            bar.get("raw_close") is None
            and str(bar.get("quality_status") or "").upper() == "MISSING_SOURCE"
            and not bool(bar.get("is_suspended"))
        ):
            unresolved_execution_cases.append({
                "trade_date": _parse_date(bar["trade_date"]).isoformat(),
                "ts_code": str(bar["ts_code"]),
                "quality_status": bar.get("quality_status"),
            })
    actions_by_day: dict[date, dict[str, list[dict[str, Any]]]] = defaultdict(
        lambda: defaultdict(list),
    )
    actions = repository.dividend_page(
        DATASET_ID, context, limit=50_000,
        date_from=dates[0], date_to=dates[-1],
    )
    for action in actions:
        actions_by_day[_parse_date(action["ex_date"])][str(action["ts_code"])].append(action)
    delist_dates = {}
    for row in observations:
        day = _parse_date(row.get("delist_date"))
        code = str(row["ts_code"])
        if day is not None and (code not in delist_dates or day < delist_dates[code]):
            delist_dates[code] = day
    actual_labels = [
        item.as_dict()
        for item in build_forward_labels(
            signal_rows, calendar=dates, bars=bars_by_day,
            actions=actions_by_day, delist_dates=delist_dates,
            horizons=(20, 40, 60),
        )
    ]
    expected_labels = sorted(
        bundle["label_reference"],
        key=lambda row: (row["signal_date"], row["ts_code"], int(row["horizon"])),
    )
    actual_labels.sort(key=lambda row: (row["signal_date"], row["ts_code"], int(row["horizon"])))
    label_mismatches: list[dict[str, Any]] = []
    if len(actual_labels) != len(expected_labels):
        label_mismatches.append({
            "difference": "ROW_COUNT",
            "expected": len(expected_labels),
            "actual": len(actual_labels),
        })
    identity_fields = ("signal_date", "ts_code", "horizon", "target_date", "status", "reason")
    numeric_fields = (
        "prediction_total_return", "entry_reference_total_return", "relative_total_return",
        "relative_pool_mean_return",
    )
    for index, (expected, actual) in enumerate(zip(expected_labels, actual_labels)):
        for field in identity_fields:
            if expected.get(field) != actual.get(field):
                label_mismatches.append({
                    "index": index,
                    "ts_code": expected.get("ts_code"),
                    "horizon": expected.get("horizon"),
                    "field": field,
                    "expected": expected.get(field),
                    "actual": actual.get(field),
                })
        for field in numeric_fields:
            left, right = expected.get(field), actual.get(field)
            if left is None or right is None:
                if left is not right:
                    label_mismatches.append({
                        "index": index,
                        "ts_code": expected.get("ts_code"),
                        "horizon": expected.get("horizon"),
                        "field": field,
                        "expected": left,
                        "actual": right,
                    })
            elif abs(float(left) - float(right)) > 1e-12:
                label_mismatches.append({
                    "index": index,
                    "ts_code": expected.get("ts_code"),
                    "horizon": expected.get("horizon"),
                    "field": field,
                    "expected": left,
                    "actual": right,
                })
    suspended_label_rows = sum(
        item["reason"] == "SUSPENDED_PATH" for item in actual_labels
    )
    unknown_delist_terminal_rows = sum(
        item["reason"] == "DELISTED_WITHIN_HORIZON_NO_TERMINAL_VALUE"
        for item in actual_labels
    )
    return {
        "factor_rows": factor_count,
        "factor_match_status": "PASS" if not factor_mismatches else "FAIL",
        "factor_mismatch_count": len(factor_mismatches),
        "factor_mismatches": factor_mismatches[:100],
        "financial_feature_presence_mismatch_count": len(financial_feature_presence_mismatches),
        "financial_feature_presence_mismatches": financial_feature_presence_mismatches,
        "signal_observations": len(signal_rows),
        "label_rows": len(actual_labels),
        "label_match_status": "PASS" if not label_mismatches else "FAIL",
        "label_mismatch_count": len(label_mismatches),
        "label_mismatches": label_mismatches[:100],
        "bar_rows": len(bars),
        "action_rows": len(actions),
        "missing_pit_factor_rows": missing_pit_factor_rows,
        "suspended_label_rows": suspended_label_rows,
        "unknown_delist_terminal_rows": unknown_delist_terminal_rows,
        "unresolved_execution_cases": unresolved_execution_cases,
    }


def _verify_alpha_reader(
    repository: DatasetRepository, bundle: dict[str, Any], dates: list[date],
) -> dict[str, Any]:
    """Read the frozen Dataset through the public Alpha research adapter."""
    columns = (
        "research_date", "ts_code", "delist_date",
        "financial_feature_contract_version", "financial_vintage_quality",
        "financial_component_lineage", "financial_data_available_timestamp",
        "roe_quality_status", "growth_quality_status", "price_feature_version",
        "price_adjustment_mode", "debt_to_assets_quality_status",
        "debt_to_assets_quality_reason", "debt_to_assets_data_available_timestamp",
        "debt_to_assets_source_observed_at", "debt_to_assets_vintage_quality",
        "debt_to_assets_source_quality_status", "debt_to_assets_source_record_key",
        "debt_to_assets_source_record_version", "debt_to_assets_source_payload_hash",
        "debt_to_assets_feature_contract_version", "debt_to_assets_report_date",
        "pe_pct_5y", "pe_ttm", "is_pe_meaningful", "has_full_pe_5y_history",
        "roe_ttm", "debt_to_assets", "revenue_growth_yoy", "profit_growth_yoy",
        "return_20d", "return_60d", "adj_close_base", "ma60", "volume_ratio_20d",
    )
    context = _context("alpha-reader")
    reader = AlphaResearchRepository(repository, DATASET_ID, context)
    signal_dates = {
        _parse_date(day) for day in bundle.get("signal_dates", [bundle["signal_date"]])
    }
    signal_start, signal_end = min(signal_dates), max(signal_dates)
    observations = [
        row
        for page in reader.observation_pages(signal_start, signal_end, columns)
        for row in page
    ]
    expected = {
        (_parse_date(row.get("research_date") or bundle["signal_date"]), str(row["ts_code"])): row
        for row in bundle["factor_reference"]
    }
    mismatches = []
    status_counts: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for row in observations:
        day = _parse_date(row["research_date"])
        code = str(row["ts_code"])
        actual = factor_values(row)
        reference = expected[(day, code)]["factors"]
        for name, value in actual.items():
            status_counts[name][value.status + (":" + value.reason if value.reason else "")] += 1
            if name not in reference:
                continue
            oracle = reference[name]
            same_value = (
                value.value is None and oracle["value"] is None
            ) or (
                value.value is not None and oracle["value"] is not None
                and abs(float(value.value) - float(oracle["value"])) <= 1e-12
            )
            if value.status != oracle["status"] or value.reason != oracle["reason"] or not same_value:
                mismatches.append({
                    "research_date": day.isoformat(), "ts_code": code, "factor": name,
                    "expected": oracle,
                    "actual": {"status": value.status, "reason": value.reason, "value": value.value},
                })

    calendar = tuple(dates)
    all_observations = [
        row
        for page in reader.observation_pages(dates[0], dates[-1], columns)
        for row in page
    ]
    labels = [
        item.as_dict()
        for item in build_forward_labels(
            observations, calendar=calendar, bars=reader.bars(dates[0], dates[-1]),
            actions=reader.actions(dates[0], dates[-1]),
            delist_dates={
                str(row["ts_code"]): _parse_date(row["delist_date"])
                for row in all_observations if row.get("delist_date") is not None
            },
            horizons=(20, 40, 60),
        )
    ]
    expected_labels = sorted(
        bundle["label_reference"],
        key=lambda row: (row["signal_date"], row["ts_code"], int(row["horizon"])),
    )
    labels.sort(key=lambda row: (row["signal_date"], row["ts_code"], int(row["horizon"])))
    label_mismatches = []
    fields = (
        "signal_date", "ts_code", "horizon", "target_date", "status", "reason",
        "prediction_total_return", "entry_reference_total_return", "relative_total_return",
        "relative_pool_mean_return",
    )
    if len(labels) != len(expected_labels):
        label_mismatches.append({"difference": "ROW_COUNT", "actual": len(labels), "expected": len(expected_labels)})
    for index, (actual, oracle) in enumerate(zip(labels, expected_labels)):
        for field in fields:
            left, right = actual.get(field), oracle.get(field)
            if field in {
                "prediction_total_return", "entry_reference_total_return",
                "relative_total_return", "relative_pool_mean_return",
            } and left is not None and right is not None:
                equal = abs(float(left) - float(right)) <= 1e-12
            else:
                equal = left == right
            if not equal:
                label_mismatches.append({
                    "index": index, "field": field, "expected": right, "actual": left,
                })
    return {
        "dataset_status": "FROZEN",
        "schema_versions": list(reader.identity.schema_versions),
        "snapshot_count": reader.identity.snapshot_count,
        "signal_observations": len(observations),
        "factor_status_counts": {
            name: dict(counts) for name, counts in sorted(status_counts.items())
        },
        "factor_match_status": "PASS" if not mismatches else "FAIL",
        "factor_mismatch_count": len(mismatches),
        "factor_mismatches": mismatches[:100],
        "label_rows": len(labels),
        "label_match_status": "PASS" if not label_mismatches else "FAIL",
        "label_mismatch_count": len(label_mismatches),
        "label_mismatches": label_mismatches[:100],
        "debt_to_assets_source_identity_check": _alpha_reader_source_identity_check(
            observations,
        ),
    }


def _alpha_reader_source_identity_check(
    observations: list[dict[str, Any]],
) -> dict[str, Any] | None:
    if SAMPLE_VERSION != "real-golden-v3":
        return None
    row = next(
        item for item in observations
        if str(item.get("ts_code")) == "000666.SZ"
    )
    value = factor_values(row)["debt_to_assets"]
    report_date = _parse_date(row.get("debt_to_assets_report_date"))
    return {
        "ts_code": "000666.SZ",
        "report_date": report_date.isoformat() if report_date else None,
        "source_record_key": row.get("debt_to_assets_source_record_key"),
        "source_record_version": row.get("debt_to_assets_source_record_version"),
        "source_payload_hash": row.get("debt_to_assets_source_payload_hash"),
        "status": value.status,
        "reason": value.reason,
        "value": value.value,
    }
