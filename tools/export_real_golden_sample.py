"""Export a deterministic, bounded real-data integration sample.

The source is the production app role and the read path is restricted to
Repositories. The exported bundle is local/ignored; only its immutable
manifest is suitable for version control. This proves frozen-data downstream
behavior, not Raw-to-Feature transformation correctness.
"""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import uuid
from collections import defaultdict
from datetime import date
from pathlib import Path

from data_collect.alpha_research.contracts import (
    FACTOR_SET_VERSION,
    LABEL_VERSION,
    SOURCE_QUALITY_EVIDENCE_VERSION,
)
from data_collect.alpha_research.factors import factor_values
from data_collect.alpha_research.golden_sample import (
    GOLDEN_SAMPLE_VERSION,
    SAMPLE_SEED,
    SAMPLE_SIZE,
    canonical_json,
    content_hash,
    select_codes,
    validate_manifest,
)
from data_collect.alpha_research.labels import build_forward_labels
from data_collect.data_access import DataAccessKernel, RunContext
from data_collect.data_access.policy_catalog import build_default_registry
from data_collect.data_access.repository.dataset_repository import DatasetRepository
from data_collect.data_access.repository.financial_repository import FinancialRepository
from data_collect.data_access.repository.snapshot_repository import SnapshotRepository
from data_collect.data_access.repository.source_audit_repository import (
    SourceAuditRepository,
)
from data_collect.jobs.tushare_foundation_features import calculate_features
from tests.integration_fixtures.real_golden_reference import independent_forward_labels

SOURCE_DATASET_ID = "f348bc11-b4f0-41fe-a8b5-d376bb5cf401"
SIGNAL_DATE = date(2023, 9, 22)
FINANCIAL_WARMUP_START = date(2015, 1, 1)
PRODUCTION_DATABASE = "ashares"
APPLICATION_ROLE = "ashares_app"
BUNDLE_SCHEMA = "real-golden-sample-bundle-v1"
OBSERVATION_SOURCE_HASH_FIELDS = (
    "financial_source_record_key", "financial_payload_hash",
    "financial_data_available_timestamp", "instrument_source_record_key",
    "instrument_source_record_version", "instrument_source_payload_hash",
    "price_feature_version", "price_adjustment_mode",
)
SHANGHAI = __import__("zoneinfo").ZoneInfo("Asia/Shanghai")


def _context() -> RunContext:
    return RunContext(
        run_id=f"real-golden-export-{uuid.uuid4().hex}",
        job_name="export_real_golden_sample",
        code_version="real-golden-v1",
        config_hash=content_hash({"version": GOLDEN_SAMPLE_VERSION}),
        pipeline_version=GOLDEN_SAMPLE_VERSION,
    )


def _as_json(value):
    return json.loads(json.dumps(value, ensure_ascii=False, default=str, allow_nan=False))


def _require_read_only_application_role(kernel: DataAccessKernel) -> None:
    if os.getenv("ASHARES_ENV") == "integration_test":
        raise RuntimeError("export must connect to production through the application read role")
    connection = kernel._db_connection()
    with connection.cursor() as cursor:
        cursor.execute(
            """SELECT current_database(), current_user,
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
        database, role, is_superuser, can_select, can_insert, can_update, can_delete = cursor.fetchone()
    if database != PRODUCTION_DATABASE:
        raise RuntimeError("refusing to export: configured database is not the production research source")
    if role != APPLICATION_ROLE or is_superuser:
        raise RuntimeError("refusing to export: connection is not the non-superuser application role")
    if not can_select or any((can_insert, can_update, can_delete)):
        raise RuntimeError("refusing to export: dataset-component permission is not read-only")


def _financial_reference(events: list[dict]) -> list[dict]:
    features = calculate_features(
        events, FINANCIAL_WARMUP_START, date(2023, 6, 30),
    )
    return sorted(
        features,
        key=lambda row: (
            str(row.get("ts_code") or ""), row.get("report_date"),
            row.get("data_available_timestamp"), int(row.get("record_version") or 0),
        ),
    )


def _visible_feature(
    features_by_code: dict[str, list[dict]], code: str, asof: date,
) -> dict | None:
    visible = [
        feature for feature in features_by_code.get(code, ())
        if feature.get("report_date") <= asof
        and feature.get("data_available_timestamp") is not None
        and feature["data_available_timestamp"].date() <= asof
    ]
    if not visible:
        return None
    return max(
        visible,
        key=lambda row: (
            row["report_date"], row["data_available_timestamp"],
            int(row.get("record_version") or 0),
        ),
    )


def _augmented_factor_row(snapshot_row: dict, feature: dict | None) -> dict:
    if feature is None:
        return snapshot_row
    row = dict(snapshot_row)
    row.update({
        "financial_component_lineage": feature.get("component_lineage"),
        "financial_feature_contract_version": feature.get("feature_contract_version"),
        "financial_data_available_timestamp": feature.get("data_available_timestamp"),
        "financial_vintage_quality": feature.get("pit_evidence_level"),
        "roe_ttm": feature.get("roe_ttm"),
        "revenue_growth_yoy": feature.get("revenue_growth_yoy"),
        "profit_growth_yoy": feature.get("profit_growth_yoy"),
        "roe_quality_status": feature.get("roe_quality_status"),
        "growth_quality_status": feature.get("growth_quality_status"),
    })
    return row


def _factor_reference(
    observations: list[dict], features: list[dict],
) -> list[dict]:
    by_code: dict[str, list[dict]] = defaultdict(list)
    for feature in features:
        by_code[str(feature["ts_code"])].append(feature)
    result = []
    for observation in observations:
        feature = _visible_feature(
            by_code, str(observation["ts_code"]), observation["research_date"],
        )
        row = _augmented_factor_row(observation, feature)
        values = factor_values(row)
        result.append({
            "research_date": observation["research_date"],
            "ts_code": observation["ts_code"],
            "financial_feature_present": feature is not None,
            "financial_feature_hash": content_hash(feature) if feature else None,
            "factors": {
                name: {
                    "status": value.status,
                    "reason": value.reason,
                    "value": value.value,
                }
                for name, value in values.items()
            },
        })
    return result


def _financial_warning_case(features: list[dict], open_calendar: list[date]) -> dict:
    candidates = []
    for feature in features:
        lineage = feature.get("component_lineage") or {}
        warning_components = [
            (key, value)
            for key, value in lineage.items()
            if isinstance(value, dict)
            and str(value.get("source_quality_status") or "").upper() == "WARNING"
            and value.get("used_by")
        ]
        if not warning_components:
            continue
        available = feature.get("data_available_timestamp")
        if available is None:
            continue
        visible_date = available.astimezone(SHANGHAI).date() if available.tzinfo else available.date()
        signal_day = next((day for day in open_calendar if day > visible_date), None)
        if signal_day is None:
            continue
        row = {
            "research_date": signal_day,
            "financial_component_lineage": lineage,
            "financial_feature_contract_version": feature.get("feature_contract_version"),
            "financial_data_available_timestamp": available,
            "financial_vintage_quality": feature.get("pit_evidence_level"),
            "roe_ttm": feature.get("roe_ttm"),
            "revenue_growth_yoy": feature.get("revenue_growth_yoy"),
            "profit_growth_yoy": feature.get("profit_growth_yoy"),
            "roe_quality_status": feature.get("roe_quality_status"),
            "growth_quality_status": feature.get("growth_quality_status"),
        }
        blocked_factors = [
            name for name, value in factor_values(row).items()
            if value.reason == "FINANCIAL_SOURCE_COMPONENT_QUALITY_NOT_VALID"
        ]
        if blocked_factors:
            candidates.append((
                str(feature.get("ts_code")), feature.get("report_date"),
                available, feature, warning_components, signal_day, blocked_factors,
            ))
    if not candidates:
        raise RuntimeError(
            "the selected real sample contains no factor-relevant WARNING source case"
        )
    code, report_date, available, feature, components, signal_day, blocked = min(
        candidates, key=lambda item: (item[0], item[1], item[2]),
    )
    return {
        "ts_code": code,
        "report_date": report_date,
        "data_available_timestamp": available,
        "research_date": signal_day,
        "feature_sha256": content_hash(feature),
        "warning_components": [key for key, _ in components],
        "blocked_factors": blocked,
        "factor_reasons": {
            name: factor_values({
                "research_date": signal_day,
                "financial_component_lineage": feature.get("component_lineage"),
                "financial_feature_contract_version": feature.get("feature_contract_version"),
                "financial_data_available_timestamp": available,
                "financial_vintage_quality": feature.get("pit_evidence_level"),
                "roe_ttm": feature.get("roe_ttm"),
                "revenue_growth_yoy": feature.get("revenue_growth_yoy"),
                "profit_growth_yoy": feature.get("profit_growth_yoy"),
                "roe_quality_status": feature.get("roe_quality_status"),
                "growth_quality_status": feature.get("growth_quality_status"),
            })[name].reason
            for name in blocked
        },
    }


def _missing_pit_case(signal_observations: list[dict]) -> dict:
    candidates = [
        row for row in signal_observations
        if row.get("financial_data_available_timestamp") is None
    ]
    if not candidates:
        raise RuntimeError("the real signal Snapshot contains no missing financial PIT timestamp case")
    row = min(candidates, key=lambda item: str(item["ts_code"]))
    return {
        "ts_code": row["ts_code"],
        "research_date": row["research_date"],
        "observation_sha256": content_hash(row),
        "financial_data_available_timestamp": None,
        "lineage_sha256": content_hash(row.get("financial_component_lineage") or {}),
        "source_contract_result": factor_values(row)["roe_ttm"].reason,
        "isolated_timestamp_gate_result": "FINANCIAL_AVAILABILITY_TIMESTAMP_MISSING",
    }


def _label_reference(
    signal_observations: list[dict], calendar: list[date], bars: list[dict],
    actions: list[dict], observations: list[dict],
) -> list[dict]:
    delist_dates = {}
    for observation in observations:
        day = observation.get("delist_date")
        code = str(observation["ts_code"])
        if day is not None and (code not in delist_dates or day < delist_dates[code]):
            delist_dates[code] = day
    return independent_forward_labels(
        signal_observations,
        calendar=calendar,
        bars=bars,
        actions=actions,
        delist_dates=delist_dates,
        horizons=(20, 40, 60),
    )


def _system_labels(
    signal_observations: list[dict], calendar: list[date], bars: list[dict],
    actions: list[dict], observations: list[dict],
) -> list[dict]:
    bars_by_day: dict[date, dict[str, dict]] = defaultdict(dict)
    for bar in bars:
        bars_by_day[bar["trade_date"]][str(bar["ts_code"])] = bar
    actions_by_day: dict[date, dict[str, list[dict]]] = defaultdict(lambda: defaultdict(list))
    for action in actions:
        actions_by_day[action["ex_date"]][str(action["ts_code"])].append(action)
    delist_dates = {}
    for observation in observations:
        day = observation.get("delist_date")
        code = str(observation["ts_code"])
        if day is not None and (code not in delist_dates or day < delist_dates[code]):
            delist_dates[code] = day
    return [
        label.as_dict()
        for label in build_forward_labels(
            signal_observations,
            calendar=calendar,
            bars=bars_by_day,
            actions=actions_by_day,
            delist_dates=delist_dates,
            horizons=(20, 40, 60),
        )
    ]


def _assert_label_outputs_match(reference: list[dict], system: list[dict]) -> None:
    if len(reference) != len(system):
        raise RuntimeError("independent and system label row counts differ")
    identity_fields = (
        "signal_date", "ts_code", "horizon", "target_date", "status",
        "reason", "entry_reference_status", "execution_status",
        "relative_pool_complete_count", "relative_pool_total_count",
    )
    numeric_fields = (
        "prediction_total_return", "entry_reference_total_return",
        "relative_total_return", "relative_pool_mean_return",
    )
    for index, (expected, actual) in enumerate(zip(reference, system, strict=True)):
        if any(expected.get(field) != actual.get(field) for field in identity_fields):
            raise RuntimeError(f"label status/reference mismatch at row {index}")
        for field in numeric_fields:
            left, right = expected.get(field), actual.get(field)
            if left is None or right is None:
                if left is not right:
                    raise RuntimeError(f"label value mismatch at row {index}: {field}")
            elif abs(float(left) - float(right)) > 1e-12:
                raise RuntimeError(f"label value mismatch at row {index}: {field}")


def _source_observation_identity(row: dict) -> dict:
    return {
        "research_date": row["research_date"],
        "ts_code": row["ts_code"],
        "input_row_sha256": content_hash(row),
        "financial_component_lineage_sha256": content_hash(
            row.get("financial_component_lineage") or {}
        ),
        **{field: row.get(field) for field in OBSERVATION_SOURCE_HASH_FIELDS},
    }


def _write_immutable(path: Path, payload: bytes, *, label: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        current = path.read_bytes()
        if current != payload:
            raise RuntimeError(
                f"{label} already exists with different content; create a new sample version"
            )
        return
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_bytes(payload)
    temporary.replace(path)


def export_sample(bundle_path: Path, manifest_path: Path) -> dict:
    kernel = DataAccessKernel(policy_registry=build_default_registry())
    try:
        _require_read_only_application_role(kernel)
        context = _context()
        source = DatasetRepository(kernel)
        snapshot_repo = SnapshotRepository(kernel)
        sample_repo = SourceAuditRepository(kernel)

        open_calendar = list(source.open_calendar(SOURCE_DATASET_ID, context))
        try:
            signal_index = open_calendar.index(SIGNAL_DATE)
        except ValueError as exc:
            raise RuntimeError(f"signal date is absent from frozen source calendar: {SIGNAL_DATE}") from exc
        if signal_index + 60 >= len(open_calendar):
            raise RuntimeError("source Dataset has fewer than 60 future open sessions")
        calendar = open_calendar[signal_index : signal_index + 61]
        target_date = calendar[-1]

        # A 100-row deterministic candidate page lets us reserve space for
        # required edge cases without introducing an unbounded universe read.
        candidates = sample_repo.sample_codes(
            SIGNAL_DATE, 100, SAMPLE_SEED, context,
        )
        securities = select_codes(candidates, seed=SAMPLE_SEED)
        codes = [row["ts_code"] for row in securities]

        contracts = source.research_snapshot_contracts(SOURCE_DATASET_ID, context)
        contracts_by_date = {
            row["research_date"]: row
            for row in contracts
            if calendar[0] <= row["research_date"] <= calendar[-1]
        }
        missing_dates = [day for day in calendar if day not in contracts_by_date]
        if missing_dates:
            raise RuntimeError(f"source Dataset has no frozen Snapshot for dates: {missing_dates[:3]}")

        snapshots = []
        observations = []
        for day in calendar:
            contract = contracts_by_date[day]
            if (
                contract["snapshot_status"] != "FROZEN"
                or contract["research_schema_version"] != "research-v2.1"
            ):
                raise RuntimeError(f"source Snapshot is not frozen research-v2.1: {day}")
            snapshot_id = str(contract["research_snapshot_id"])
            manifest = snapshot_repo.load_manifest(snapshot_id, context)
            rows = snapshot_repo.read_frozen_rows(
                snapshot_id, context, limit=SAMPLE_SIZE, codes=codes,
            )
            if len({row["ts_code"] for row in rows}) != len(rows):
                raise RuntimeError(f"duplicate source observations for {day}")
            for row in rows:
                if row["research_date"] != day:
                    raise RuntimeError(f"snapshot date mismatch for {snapshot_id}")
            observations.extend(rows)
            snapshots.append({
                "snapshot_id": snapshot_id,
                "research_date": day,
                "status": manifest["status"],
                "schema_version": manifest["schema_version"],
                "baseline_tag": manifest.get(
                    "baseline", manifest.get("data_baseline_tag"),
                ),
                "input_manifest_hash": manifest.get("input_manifest_hash"),
                "baseline_quality_report_hash": manifest.get("baseline_quality_report_hash"),
                "baseline_source_manifest_hash": manifest.get("baseline_source_manifest_hash"),
                "observation_count": len(rows),
                "observation_hash": content_hash(rows),
            })

        signal_observations = [row for row in observations if row["research_date"] == SIGNAL_DATE]
        signal_codes = {row["ts_code"] for row in signal_observations}
        if signal_codes != set(codes):
            raise RuntimeError(
                "the selected code set is not fully present in the signal Snapshot: "
                f"missing={sorted(set(codes)-signal_codes)}"
            )

        bars = source.bar_page(
            SOURCE_DATASET_ID, context, limit=50_000,
            date_from=calendar[0], date_to=calendar[-1], codes=codes,
        )
        actions = source.dividend_page(
            SOURCE_DATASET_ID, context, limit=50_000,
            date_from=calendar[0], date_to=calendar[-1],
        )
        actions = [row for row in actions if str(row["ts_code"]) in set(codes)]
        all_benchmark = source.load_benchmark_closes(
            SOURCE_DATASET_ID, "000300.SH", context,
        )
        benchmark = [
            {"trade_date": day, "ts_code": "000300.SH", "close": all_benchmark[day]}
            for day in calendar if day in all_benchmark
        ]
        if len(benchmark) != len(calendar):
            raise RuntimeError("source Dataset benchmark does not cover the sample window")

        events = FinancialRepository(kernel).load_events(
            codes, FINANCIAL_WARMUP_START, SIGNAL_DATE, context,
        )
        financial_features = _financial_reference(events)
        factor_reference = _factor_reference(signal_observations, financial_features)
        quality_gate_cases = {
            "financial_source_warning": _financial_warning_case(
                financial_features, open_calendar,
            ),
            "missing_financial_pit_timestamp": _missing_pit_case(signal_observations),
        }
        label_reference = _label_reference(
            signal_observations, calendar, bars, actions, observations,
        )
        system_labels = _system_labels(
            signal_observations, calendar, bars, actions, observations,
        )
        _assert_label_outputs_match(label_reference, system_labels)
        if len(label_reference) != SAMPLE_SIZE * 3:
            raise RuntimeError("forward-label reference row count is incomplete")

        source_dataset = source.manifest(SOURCE_DATASET_ID, context)
        source_snapshots = [
            {
                **snapshot,
                "observation_identities": [
                    _source_observation_identity(row)
                    for row in observations if row["research_date"] == snapshot["research_date"]
                ],
            }
            for snapshot in snapshots
        ]
        bundle = {
            "schema_version": BUNDLE_SCHEMA,
            "sample_version": GOLDEN_SAMPLE_VERSION,
            "signal_date": SIGNAL_DATE,
            "target_date": target_date,
            "calendar": calendar,
            "securities": securities,
            "source_dataset_id": SOURCE_DATASET_ID,
            "source_dataset_manifest_hash": source_dataset.get("input_manifest_hash"),
            "source_snapshots": snapshots,
            "observations": observations,
            "execution_bars": bars,
            "company_actions": actions,
            "benchmark": benchmark,
            "financial_standard_events": events,
            "financial_features_recomputed": financial_features,
            "factor_reference": factor_reference,
            "quality_gate_cases": quality_gate_cases,
            "label_reference": label_reference,
            "system_labels": system_labels,
            "versions": {
                "factor_set": FACTOR_SET_VERSION,
                "label": LABEL_VERSION,
                "financial_source_quality": SOURCE_QUALITY_EVIDENCE_VERSION,
            },
        }
        plain = canonical_json(bundle)
        compressed = gzip.compress(plain, mtime=0)
        bundle_hash = hashlib.sha256(compressed).hexdigest()
        manifest = {
            "sample_version": GOLDEN_SAMPLE_VERSION,
            "selection_method": "6 fixed boundary cases + lowest md5(ts_code || seed) from at most 100 signal-date standard-kline candidates",
            "sample_seed": SAMPLE_SEED,
            "signal_date": SIGNAL_DATE.isoformat(),
            "target_date": target_date.isoformat(),
            "horizons": [20, 40, 60],
            "financial_warmup_start": FINANCIAL_WARMUP_START.isoformat(),
            "securities": securities,
            "source_dataset": {
                "dataset_id": SOURCE_DATASET_ID,
                "status": source_dataset.get("status"),
                "baseline_tag": source_dataset.get("baseline"),
                "schema_version": source_dataset.get("schema_version"),
                "input_manifest_hash": source_dataset.get("input_manifest_hash"),
                "scope_note": "legacy frozen source used only to extract a bounded real sample; not a new v3-quality release",
            },
            "source_snapshots": source_snapshots,
            "row_counts": {
                "observations": len(observations),
                "execution_bars": len(bars),
                "company_actions": len(actions),
                "benchmark_bars": len(benchmark),
                "financial_standard_events": len(events),
                "financial_features_recomputed": len(financial_features),
                "factor_reference": len(factor_reference),
                "label_reference": len(label_reference),
            },
            "content_hashes": {
                "observation_rows": content_hash(observations),
                "execution_bars": content_hash(bars),
                "company_actions": content_hash(actions),
                "benchmark": content_hash(benchmark),
                "financial_standard_events": content_hash(events),
                "financial_features_recomputed": content_hash(financial_features),
                "factor_reference": content_hash(factor_reference),
                "label_reference": content_hash(label_reference),
                "system_labels": content_hash(system_labels),
                "bundle_sha256": bundle_hash,
                "bundle_uncompressed_sha256": hashlib.sha256(plain).hexdigest(),
            },
            "quality_cases": {
                "financial_source_warning_count": len(
                    quality_gate_cases["financial_source_warning"]["warning_components"]
                ),
                "financial_source_warning_case": quality_gate_cases["financial_source_warning"],
                "missing_financial_pit_count": 1,
                "missing_financial_pit_case": quality_gate_cases["missing_financial_pit_timestamp"],
                "suspension_label_count": sum(
                    label["reason"] == "SUSPENDED_PATH" for label in label_reference
                ),
                "unknown_delisting_terminal_count": sum(
                    label["reason"] == "DELISTED_WITHIN_HORIZON_NO_TERMINAL_VALUE"
                    for label in label_reference
                ),
            },
            "bundle_sha256": bundle_hash,
        }
        validate_manifest(manifest)
        manifest_bytes = canonical_json(manifest) + b"\n"
        _write_immutable(bundle_path, compressed, label="real golden sample bundle")
        _write_immutable(manifest_path, manifest_bytes, label="real golden sample manifest")
        return manifest
    finally:
        kernel.close()


def verify_bundle(bundle_path: Path, manifest_path: Path) -> dict:
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    validate_manifest(manifest)
    compressed = bundle_path.read_bytes()
    if hashlib.sha256(compressed).hexdigest() != manifest["bundle_sha256"]:
        raise RuntimeError("golden sample bundle hash does not match its manifest")
    bundle = json.loads(gzip.decompress(compressed).decode("utf-8"))
    if bundle.get("sample_version") != manifest["sample_version"]:
        raise RuntimeError("golden sample bundle version mismatch")
    if content_hash(bundle.get("securities")) != content_hash(manifest.get("securities")):
        raise RuntimeError("golden sample bundle security set differs from manifest")
    if len(bundle.get("label_reference", ())) != SAMPLE_SIZE * 3:
        raise RuntimeError("golden sample bundle is missing 20/40/60-day reference labels")
    return {
        "sample_version": manifest["sample_version"],
        "securities": len(manifest["securities"]),
        "observations": manifest["row_counts"]["observations"],
        "execution_bars": manifest["row_counts"]["execution_bars"],
        "labels": manifest["row_counts"]["label_reference"],
        "bundle_sha256": manifest["bundle_sha256"],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--verify", action="store_true", help="verify an existing immutable local bundle")
    parser.add_argument(
        "--bundle", type=Path,
        default=Path(".codex/real-sample/real-golden-v1.json.gz"),
    )
    parser.add_argument(
        "--manifest", type=Path,
        default=Path("tests/fixtures/real_golden_sample_v1.manifest.json"),
    )
    args = parser.parse_args()
    if args.verify:
        result = verify_bundle(args.bundle, args.manifest)
    else:
        full = export_sample(args.bundle, args.manifest)
        result = {
            "sample_version": full["sample_version"],
            "signal_date": full["signal_date"],
            "target_date": full["target_date"],
            "securities": len(full["securities"]),
            "row_counts": full["row_counts"],
            "quality_cases": {
                key: value for key, value in full["quality_cases"].items()
                if not key.endswith("_case")
            },
            "bundle_sha256": full["bundle_sha256"],
        }
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
