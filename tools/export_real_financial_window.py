"""Export an immutable, bounded 23-signal-day real financial integration window.

The source is the production ``ashares_app`` role in a verified read-only
transaction.  The resulting bundle is imported only into the isolated
integration-test database; it is not a production Release artifact.
"""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
from collections import Counter, defaultdict
from datetime import date, datetime, time
from pathlib import Path
from zoneinfo import ZoneInfo

from data_collect.alpha_research.factors import factor_values
from data_collect.alpha_research.golden_sample import canonical_json, content_hash
from data_collect.data_access import DataAccessKernel, RunContext
from data_collect.data_access.policy_catalog import build_default_registry
from data_collect.data_access.repository.dataset_repository import DatasetRepository
from data_collect.data_access.repository.financial_repository import FinancialRepository
from data_collect.data_access.repository.snapshot_repository import SnapshotRepository
from data_collect.jobs.tushare_foundation_features import (
    calculate_debt_to_assets_features,
    calculate_features,
)
from tests.integration_fixtures.real_golden_reference import independent_forward_labels

ROOT = Path(__file__).resolve().parents[1]
SOURCE_BUNDLE = ROOT / ".codex" / "real-sample" / "real-golden-v2.json.gz"
SOURCE_MANIFEST = ROOT / "tests" / "fixtures" / "real_golden_sample_v2.manifest.json"
SAMPLE_VERSION = "real-financial-window-v1"
SOURCE_DATASET_ID = "f348bc11-b4f0-41fe-a8b5-d376bb5cf401"
SOURCE_DATABASE = "ashares"
SOURCE_ROLE = "ashares_app"
SIGNAL_START = date(2023, 9, 12)
SIGNAL_END = date(2023, 10, 20)
FINANCIAL_WARMUP_START = date(2015, 1, 1)
BENCHMARK_CODE = "000300.SH"
SHANGHAI = ZoneInfo("Asia/Shanghai")
BUNDLE_PATH = ROOT / ".codex" / "real-sample" / f"{SAMPLE_VERSION}.json.gz"
MANIFEST_PATH = ROOT / "tests" / "fixtures" / f"{SAMPLE_VERSION.replace('-', '_')}.manifest.json"


def _context() -> RunContext:
    return RunContext(
        run_id=f"{SAMPLE_VERSION}-export-readonly",
        job_name="export_real_financial_window",
        code_version="real-financial-window-v1",
        config_hash=content_hash({"sample_version": SAMPLE_VERSION}),
        pipeline_version=SAMPLE_VERSION,
    )


def _date(value):
    if value is None or isinstance(value, date) and not isinstance(value, datetime):
        return value
    return date.fromisoformat(str(value)[:10])


def _require_readonly_production(kernel: DataAccessKernel) -> None:
    connection = kernel._db_connection()
    with connection.cursor() as cursor:
        cursor.execute(
            """SELECT current_database(), current_user,
                      current_setting('transaction_read_only'),
                      (SELECT rolsuper FROM pg_roles WHERE rolname=current_user),
                      has_table_privilege(current_user,
                          'backtest.dataset_component', 'SELECT'),
                      has_table_privilege(current_user,
                          'backtest.dataset_component', 'INSERT'),
                      has_table_privilege(current_user,
                          'backtest.dataset_component', 'UPDATE'),
                      has_table_privilege(current_user,
                          'backtest.dataset_component', 'DELETE')"""
        )
        identity = cursor.fetchone()
    if identity != (SOURCE_DATABASE, SOURCE_ROLE, "on", False, True, False, False, False):
        raise RuntimeError(
            "financial-window export requires the non-superuser production app role "
            "inside a server-enforced read-only transaction"
        )


def _visible(rows: dict[str, list[dict]], code: str, signal_day: date):
    cutoff = datetime.combine(signal_day, time(15, 0), SHANGHAI)
    candidates = []
    for row in rows.get(code, ()):
        report_date = _date(row.get("report_date"))
        available = row.get("data_available_timestamp")
        if isinstance(available, str):
            available = datetime.fromisoformat(available)
        if (
            report_date is not None and report_date <= signal_day
            and isinstance(available, datetime)
            and available.tzinfo is not None
            and available.astimezone(SHANGHAI) < cutoff
        ):
            candidates.append(row)
    if not candidates:
        return None
    return max(
        candidates,
        key=lambda row: (
            _date(row["report_date"]), row["data_available_timestamp"],
            int(row.get("record_version") or 0),
        ),
    )


def _financial_factor_row(observation, company_feature, da_feature):
    row = dict(observation)
    if company_feature is not None:
        row.update({
            "financial_component_lineage": company_feature.get("component_lineage"),
            "financial_feature_contract_version": company_feature.get("feature_contract_version"),
            "financial_data_available_timestamp": company_feature.get("data_available_timestamp"),
            "financial_vintage_quality": company_feature.get("pit_evidence_level"),
            "roe_ttm": company_feature.get("roe_ttm"),
            "revenue_growth_yoy": company_feature.get("revenue_growth_yoy"),
            "profit_growth_yoy": company_feature.get("profit_growth_yoy"),
            "roe_quality_status": company_feature.get("roe_quality_status"),
            "growth_quality_status": company_feature.get("growth_quality_status"),
        })
    if da_feature is not None:
        row.update({
            "debt_to_assets": da_feature.get("debt_to_assets"),
            "debt_to_assets_quality_status": da_feature.get("quality_status"),
            "debt_to_assets_quality_reason": da_feature.get("quality_reason"),
            "debt_to_assets_data_available_timestamp": da_feature.get("data_available_timestamp"),
            "debt_to_assets_source_observed_at": da_feature.get("source_observed_at"),
            "debt_to_assets_vintage_quality": da_feature.get("vintage_quality"),
            "debt_to_assets_source_quality_status": da_feature.get("source_quality_status"),
            "debt_to_assets_source_record_key": da_feature.get("source_record_key"),
            "debt_to_assets_source_record_version": da_feature.get("record_version"),
            "debt_to_assets_source_payload_hash": da_feature.get("payload_hash"),
            "debt_to_assets_feature_contract_version": da_feature.get("feature_contract_version"),
            "debt_to_assets_report_date": da_feature.get("report_date"),
        })
    return row


def _factor_references(observations, company_features, da_features):
    company_by_code: dict[str, list[dict]] = defaultdict(list)
    da_by_code: dict[str, list[dict]] = defaultdict(list)
    for item in company_features:
        company_by_code[str(item["ts_code"])].append(item)
    for item in da_features:
        da_by_code[str(item["ts_code"])].append(item)
    result = []
    counts = {name: Counter() for name in (
        "debt_to_assets", "roe_ttm", "revenue_growth_yoy", "profit_growth_yoy",
    )}
    for observation in observations:
        signal_day = _date(observation["research_date"])
        code = str(observation["ts_code"])
        company = _visible(company_by_code, code, signal_day)
        da = _visible(da_by_code, code, signal_day)
        values = factor_values(_financial_factor_row(observation, company, da))
        factors = {}
        for name, status_counts in counts.items():
            value = values[name]
            factors[name] = {
                "status": value.status, "reason": value.reason, "value": value.value,
            }
            status_counts[value.status + (":" + value.reason if value.reason else "")] += 1
        result.append({
            "research_date": signal_day,
            "ts_code": code,
            "financial_feature_present": company is not None,
            "financial_feature_hash": content_hash(company) if company else None,
            "debt_to_assets_feature_hash": content_hash(da) if da else None,
            "factors": factors,
        })
    return result, {name: dict(value) for name, value in counts.items()}


def _label_references(signal_observations, calendar, bars, actions, observations):
    delist_dates = {}
    for row in observations:
        code = str(row["ts_code"])
        day = _date(row.get("delist_date"))
        if day is not None and (code not in delist_dates or day < delist_dates[code]):
            delist_dates[code] = day
    reference = independent_forward_labels(
        signal_observations,
        calendar=calendar,
        bars=bars,
        actions=actions,
        delist_dates=delist_dates,
        horizons=(20, 40, 60),
    )
    return reference, delist_dates


def _write_immutable(path: Path, value: bytes, label: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if path.read_bytes() != value:
            raise RuntimeError(f"{label} exists with different bytes; use a new sample version")
        return
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_bytes(value)
    temporary.replace(path)


def export(bundle_path: Path = BUNDLE_PATH, manifest_path: Path = MANIFEST_PATH):
    source_bundle = json.loads(gzip.decompress(SOURCE_BUNDLE.read_bytes()))
    source_manifest_bytes = SOURCE_MANIFEST.read_bytes()
    source_manifest = json.loads(source_manifest_bytes)
    securities = source_bundle["securities"]
    codes = [str(row["ts_code"]) for row in securities]
    if len(codes) != 30 or len(set(codes)) != 30:
        raise RuntimeError("the parent golden v2 manifest must contain 30 unique securities")

    kernel = DataAccessKernel(
        policy_registry=build_default_registry(), read_only_audit=True,
    )
    try:
        _require_readonly_production(kernel)
        context = _context()
        datasets = DatasetRepository(kernel)
        snapshots_repo = SnapshotRepository(kernel)
        all_dates = list(datasets.open_calendar(SOURCE_DATASET_ID, context))
        try:
            start_index = all_dates.index(SIGNAL_START)
            signal_end_index = all_dates.index(SIGNAL_END)
        except ValueError as exc:
            raise RuntimeError("requested signal window is absent from frozen source calendar") from exc
        tail_end_index = signal_end_index + 60
        if tail_end_index >= len(all_dates):
            raise RuntimeError("source Dataset does not contain 60 open sessions after signal end")
        calendar = all_dates[start_index:tail_end_index + 1]
        signal_dates = all_dates[start_index:signal_end_index + 1]
        if len(signal_dates) != 23 or len(calendar) != 83:
            raise RuntimeError(
                f"unexpected window size: signals={len(signal_dates)}, total={len(calendar)}"
            )

        contracts = datasets.research_snapshot_contracts(SOURCE_DATASET_ID, context)
        contracts_by_date = {
            row["research_date"]: row for row in contracts
            if calendar[0] <= row["research_date"] <= calendar[-1]
        }
        if set(contracts_by_date) != set(calendar):
            raise RuntimeError("source Dataset lacks a frozen Snapshot for a window trade date")
        snapshots, observations = [], []
        for day in calendar:
            contract = contracts_by_date[day]
            if (
                contract["snapshot_status"] != "FROZEN"
                or contract["research_schema_version"] != "research-v2.1"
            ):
                raise RuntimeError(f"source Snapshot contract is not frozen research-v2.1: {day}")
            snapshot_id = str(contract["research_snapshot_id"])
            manifest = snapshots_repo.load_manifest(snapshot_id, context)
            rows = snapshots_repo.read_frozen_rows(snapshot_id, context, limit=100, codes=codes)
            if any(row["research_date"] != day for row in rows):
                raise RuntimeError(f"source Snapshot date mismatch: {day}")
            observations.extend(rows)
            snapshots.append({
                "snapshot_id": snapshot_id,
                "research_date": day,
                "status": manifest["status"],
                "schema_version": manifest["schema_version"],
                "baseline_tag": manifest.get("baseline", manifest.get("data_baseline_tag")),
                "input_manifest_hash": manifest.get("input_manifest_hash"),
                "baseline_quality_report_hash": manifest.get("baseline_quality_report_hash"),
                "baseline_source_manifest_hash": manifest.get("baseline_source_manifest_hash"),
                "observation_count": len(rows),
                "observation_hash": content_hash(rows),
            })
        signal_set = set(signal_dates)
        signal_observations = [
            row for row in observations if _date(row["research_date"]) in signal_set
        ]
        for day in signal_dates:
            present = {str(row["ts_code"]) for row in signal_observations if _date(row["research_date"]) == day}
            if present != set(codes):
                raise RuntimeError(
                    f"signal Snapshot security mismatch for {day}: "
                    f"missing={sorted(set(codes) - present)}"
                )

        bars = datasets.bar_page(
            SOURCE_DATASET_ID, context, limit=50_000,
            date_from=calendar[0], date_to=calendar[-1], codes=codes,
        )
        actions = datasets.dividend_page(
            SOURCE_DATASET_ID, context, limit=50_000,
            date_from=calendar[0], date_to=calendar[-1],
        )
        actions = [row for row in actions if str(row["ts_code"]) in set(codes)]
        benchmark_closes = datasets.load_benchmark_closes(
            SOURCE_DATASET_ID, BENCHMARK_CODE, context,
        )
        benchmark = [
            {"trade_date": day, "ts_code": BENCHMARK_CODE, "close": benchmark_closes[day]}
            for day in calendar if day in benchmark_closes
        ]
        if len(benchmark) != len(calendar):
            raise RuntimeError("frozen benchmark does not cover the requested window")

        events = FinancialRepository(kernel).load_events(
            codes, FINANCIAL_WARMUP_START, calendar[-1], context,
        )
        financial_features = calculate_features(
            events, FINANCIAL_WARMUP_START, calendar[-1],
        )
        da_features = calculate_debt_to_assets_features(
            events, FINANCIAL_WARMUP_START, calendar[-1],
        )
        factor_reference, factor_counts = _factor_references(
            signal_observations, financial_features, da_features,
        )
        label_reference, delist_dates = _label_references(
            signal_observations, calendar, bars, actions, observations,
        )

        bars_by_day = defaultdict(dict)
        for row in bars:
            bars_by_day[_date(row["trade_date"])][str(row["ts_code"])] = row
        actions_by_day = defaultdict(lambda: defaultdict(list))
        for row in actions:
            actions_by_day[_date(row["ex_date"])][str(row["ts_code"])].append(row)
        from data_collect.alpha_research.labels import build_forward_labels
        system_labels = [
            row.as_dict() for row in build_forward_labels(
                signal_observations,
                calendar=calendar,
                bars=bars_by_day,
                actions=actions_by_day,
                delist_dates=delist_dates,
                horizons=(20, 40, 60),
            )
        ]
        if len(system_labels) != len(label_reference):
            raise RuntimeError("independent and system forward-label row counts differ")
        for left, right in zip(label_reference, system_labels, strict=True):
            for field in ("signal_date", "ts_code", "horizon", "target_date", "status", "reason"):
                if left.get(field) != right.get(field):
                    raise RuntimeError(f"label system/reference mismatch: {field}")
            for field in ("prediction_total_return", "entry_reference_total_return"):
                a, b = left.get(field), right.get(field)
                if a is None or b is None:
                    if a is not b:
                        raise RuntimeError(f"label system/reference nullability mismatch: {field}")
                elif abs(float(a) - float(b)) > 1e-12:
                    raise RuntimeError(f"label system/reference value mismatch: {field}")

        bundle = {
            "schema_version": "real-financial-window-bundle-v1",
            "sample_version": SAMPLE_VERSION,
            "signal_date": SIGNAL_START,
            "signal_dates": signal_dates,
            "target_date": calendar[-1],
            "calendar": calendar,
            "securities": securities,
            "source_dataset_id": SOURCE_DATASET_ID,
            "source_dataset_manifest_hash": source_manifest["source_dataset"]["input_manifest_hash"],
            "source_sample_manifest_sha256": hashlib.sha256(source_manifest_bytes).hexdigest(),
            "source_snapshots": snapshots,
            "observations": observations,
            "signal_observations": signal_observations,
            "execution_bars": bars,
            "company_actions": actions,
            "benchmark": benchmark,
            "financial_standard_events": events,
            "financial_features_recomputed": financial_features,
            "debt_to_assets_features": da_features,
            "factor_reference": factor_reference,
            "label_reference": label_reference,
            "system_labels": system_labels,
            "factor_status_counts": factor_counts,
            "versions": {
                "financial_feature_contract": "feature-contract-v2.0",
                "debt_to_assets_feature_contract": "debt-to-assets-v1",
                "financial_source_quality": "financial-source-quality-v1",
                "factor_set": "frozen-factor-set-v3",
                "label": "forward-total-return-v1",
            },
        }
        compressed = gzip.compress(canonical_json(bundle), mtime=0)
        bundle_hash = hashlib.sha256(compressed).hexdigest()
        plain_hash = hashlib.sha256(canonical_json(bundle)).hexdigest()
        manifest = {
            "sample_version": SAMPLE_VERSION,
            "schema_version": bundle["schema_version"],
            "selection_method": "reuse the immutable 30-security real-golden-v2 manifest; no security was reselected",
            "parent_sample_manifest_sha256": hashlib.sha256(source_manifest_bytes).hexdigest(),
            "signal_start": SIGNAL_START.isoformat(),
            "signal_end": SIGNAL_END.isoformat(),
            "signal_dates": [day.isoformat() for day in signal_dates],
            "target_date": calendar[-1].isoformat(),
            "calendar_sessions": len(calendar),
            "horizons": [20, 40, 60],
            "financial_warmup_start": FINANCIAL_WARMUP_START.isoformat(),
            "securities": securities,
            "source_dataset": {
                "dataset_id": SOURCE_DATASET_ID,
                "status": "FROZEN",
                "schema_version": "research-v2.1",
                "input_manifest_hash": bundle["source_dataset_manifest_hash"],
                "scope_note": "legacy frozen source used only to export bounded real facts; not v2.2 Release evidence",
            },
            "source_snapshots": snapshots,
            "row_counts": {
                "observations": len(observations),
                "signal_observations": len(signal_observations),
                "execution_bars": len(bars),
                "company_actions": len(actions),
                "benchmark_bars": len(benchmark),
                "financial_standard_events": len(events),
                "financial_features_recomputed": len(financial_features),
                "debt_to_assets_features": len(da_features),
                "factor_reference": len(factor_reference),
                "label_reference": len(label_reference),
            },
            "factor_status_counts": factor_counts,
            "content_hashes": {
                "observations": content_hash(observations),
                "signal_observations": content_hash(signal_observations),
                "execution_bars": content_hash(bars),
                "company_actions": content_hash(actions),
                "benchmark": content_hash(benchmark),
                "financial_standard_events": content_hash(events),
                "financial_features_recomputed": content_hash(financial_features),
                "debt_to_assets_features": content_hash(da_features),
                "factor_reference": content_hash(factor_reference),
                "label_reference": content_hash(label_reference),
                "system_labels": content_hash(system_labels),
            },
            "bundle_sha256": bundle_hash,
            "bundle_uncompressed_sha256": plain_hash,
        }
        _write_immutable(bundle_path, compressed, "financial-window bundle")
        _write_immutable(manifest_path, canonical_json(manifest) + b"\n", "financial-window manifest")
        return manifest
    finally:
        kernel.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bundle", type=Path, default=BUNDLE_PATH)
    parser.add_argument("--manifest", type=Path, default=MANIFEST_PATH)
    parser.add_argument("--verify", action="store_true")
    args = parser.parse_args()
    if args.verify:
        manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
        compressed = args.bundle.read_bytes()
        if hashlib.sha256(compressed).hexdigest() != manifest["bundle_sha256"]:
            raise RuntimeError("financial-window bundle hash mismatch")
        bundle = json.loads(gzip.decompress(compressed))
        if bundle["sample_version"] != SAMPLE_VERSION:
            raise RuntimeError("financial-window sample version mismatch")
        result = {
            "sample_version": SAMPLE_VERSION,
            "securities": len(bundle["securities"]),
            "signal_dates": len(bundle["signal_dates"]),
            "observations": len(bundle["observations"]),
            "factor_reference": len(bundle["factor_reference"]),
            "labels": len(bundle["label_reference"]),
            "bundle_sha256": manifest["bundle_sha256"],
        }
    else:
        manifest = export(args.bundle, args.manifest)
        result = {key: manifest[key] for key in (
            "sample_version", "signal_start", "signal_end", "target_date",
            "calendar_sessions", "row_counts", "factor_status_counts", "bundle_sha256",
        )}
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
