"""Replay the full bounded financial PIT projection into an isolated test DB."""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import math
import os
import sys
from datetime import date, datetime, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from data_collect.data_access import DataAccessKernel, RunContext, WorkScope
from data_collect.data_access.policy_catalog import build_default_registry
from data_collect.data_access.repository.financial_projection_repository import (
    FinancialProjectionRepository,
    FinancialProjectionSelection,
    canonical_hash,
    projection_row_hash,
)
from data_collect.data_access.repository.financial_repository import FinancialRepository
from data_collect.data_access.repository.financial_standard_repository import (
    FinancialStandardRepository,
)
from data_collect.jobs.tushare_financial_raw_to_standard import transform

BATCH_DIR = ROOT / ".codex" / "p2-batches"
INPUT_PATH = BATCH_DIR / "financial-pit-projection-input-v1.json.gz"
INPUT_MANIFEST_PATH = BATCH_DIR / "financial-pit-projection-input-v1.manifest.json"
OUTPUT_PATH = BATCH_DIR / "financial-pit-projection-replay-v6.result.json"
PROJECTION_VERSION = "financial-source-announcement-projection-v6"
PROJECTION_RULE_VERSION = "announcement-next-day-shanghai-midnight-v1"
BASE_PROJECTION_VERSION = "standard-inline-v1"
REPORT_START = date(2015, 1, 1)
REPORT_END = date(2021, 8, 1)
SHANGHAI = ZoneInfo("Asia/Shanghai")
SOURCE_MAP = {
    "balance_sheet": ("balancesheet", "balance", "std.financial_balance_sheet_standard"),
    "cash_flow": ("cashflow", "cashflow", "std.financial_cash_flow_standard"),
    "income": ("income", "income", "std.financial_income_standard"),
}


def sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def parse_date(value) -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    return date.fromisoformat(str(value)[:10])


def parse_datetime(value) -> datetime:
    if isinstance(value, datetime):
        return value
    text = str(value)
    parsed = datetime.fromisoformat(
        text[:-1] + "+00:00" if text.endswith("Z") else text
    )
    if parsed.tzinfo is None:
        raise ValueError("projected timestamps must be timezone-aware")
    return parsed


def normalize(value):
    if value is None:
        return None
    if isinstance(value, str) and value.strip().casefold() == "nan":
        return None
    if isinstance(value, pd.Timestamp):
        value = value.to_pydatetime()
    if pd.isna(value):
        return None
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if hasattr(value, "item"):
        return normalize(value.item())
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def standard_content_hash(row: dict) -> str:
    return canonical_hash({
        key: normalize(value)
        for key, value in row.items()
        if key not in {"financial_id", "_standard_id"}
    })


def same_value(left, right) -> bool:
    left = normalize(left)
    right = normalize(right)
    if isinstance(left, (float, int)) and isinstance(right, (float, int)):
        return math.isclose(float(left), float(right), rel_tol=1e-12, abs_tol=1e-9)
    return left == right


def prepare_projection_rows(content: dict) -> tuple[list[dict], dict]:
    bundle_rows = content["rows"]
    by_source: dict[str, list[dict]] = {source: [] for source in SOURCE_MAP}
    for bundle_row in bundle_rows:
        by_source[bundle_row["candidate"]["source"]].append(bundle_row)

    projections = []
    transform_diagnostics = {}
    for candidate_source, bundle in by_source.items():
        if not bundle:
            transform_diagnostics[candidate_source] = {"input_rows": 0, "output_rows": 0}
            continue
        raw_dataset, _, _ = SOURCE_MAP[candidate_source]
        frame = pd.DataFrame([item["raw"] for item in bundle])
        transformed = transform(frame, raw_dataset)
        outputs = transformed.to_dict("records")
        output_by_raw_id = {int(row["record_version"]): row for row in outputs}
        if len(output_by_raw_id) != len(bundle):
            raise RuntimeError(f"official conversion row count mismatch for {candidate_source}")

        _, source_dataset, _ = SOURCE_MAP[candidate_source]
        source_diagnostics = {
            "input_rows": len(bundle), "output_rows": len(outputs),
            "source_identity_differences": 0,
            "nan_null_end_type_normalized": 0,
            "announcement_assumption_rows": 0,
        }
        for item in bundle:
            candidate = item["candidate"]
            standard = item["standard"]
            raw = item["raw"]
            output = output_by_raw_id[int(candidate["raw_record_id"])]
            if not candidate["raw_announce_date"]:
                raise RuntimeError("missing source announcement dates must reject projection")
            announced = parse_date(candidate["raw_announce_date"])
            expected_available = datetime.combine(
                announced + timedelta(days=1), time.min, SHANGHAI,
            )
            actual_available = output["data_available_timestamp"]
            if isinstance(actual_available, pd.Timestamp):
                actual_available = actual_available.to_pydatetime()
            if actual_available != expected_available:
                raise RuntimeError("official conversion did not use announcement date + one day")
            if output["vintage_quality"] != "SOURCE_ANNOUNCEMENT_ASSUMED":
                raise RuntimeError("projected announcement date was assigned unsupported PIT quality")
            if int(output["record_version"]) != int(candidate["raw_record_id"]):
                raise RuntimeError("Raw record_version identity was changed")
            if output["payload_hash"] != candidate["raw_payload_hash"]:
                raise RuntimeError("Raw payload hash changed in conversion")
            if output["quality_status"] != standard["quality_status"]:
                raise RuntimeError("projected source quality conflicts with the existing Standard row")
            if normalize(output["source_observed_at"]) != normalize(raw["_fetched_at"]):
                raise RuntimeError("Raw fetched_at was not retained as source-observed trace")

            differences = []
            for output_field in (
                "stock_code", "report_date", "ann_date", "f_ann_date",
                "announce_date", "report_type", "comp_type", "end_type", "update_flag",
            ):
                expected = standard.get(output_field)
                actual = output.get(output_field)
                if output_field == "end_type" and candidate["null_end_type_normalization"]:
                    if normalize(actual) is not None or normalize(expected) is not None:
                        differences.append(output_field)
                    else:
                        source_diagnostics["nan_null_end_type_normalized"] += 1
                    continue
                if not same_value(actual, expected):
                    differences.append(output_field)
            expected_key = str(standard.get("source_record_key") or "").replace("-", "")
            actual_key = str(output.get("source_record_key") or "").replace("-", "")
            if expected_key != actual_key:
                differences.append("source_record_key")
            if differences:
                source_diagnostics["source_identity_differences"] += len(differences)
                raise RuntimeError(
                    f"projection changes existing Standard source identity source={candidate_source} "
                    f"standard_id={candidate['standard_id']} fields={differences}"
                )

            # This run adds a PIT interpretation only. Existing Standard facts
            # stay byte-for-byte unchanged; newer converter aliases are not
            # materialized as replacement financial values by this projection.
            if output["payload_hash"] != standard["payload_hash"]:
                raise RuntimeError("projection Raw payload hash differs from Standard source hash")

            source_diagnostics["announcement_assumption_rows"] += 1
            row = {
                "projection_version": PROJECTION_VERSION,
                "source_dataset": source_dataset,
                "batch_number": 1,  # replaced after the deterministic batch split
                "standard_id": int(candidate["standard_id"]),  # remapped after isolated import
                "source_standard_id": int(candidate["standard_id"]),
                "raw_dataset": raw_dataset,
                "raw_record_id": int(candidate["raw_record_id"]),
                "raw_business_key": str(candidate["raw_business_key"]),
                "raw_payload_sha256": str(candidate["raw_payload_hash"]),
                "standard_source": str(standard["source"]),
                "source_record_key": str(standard["source_record_key"]),
                "source_record_version": int(standard["record_version"]),
                "standard_payload_sha256": str(standard["payload_hash"]),
                "standard_content_sha256": standard_content_hash(standard),
                "ts_code": str(standard["stock_code"]),
                "report_date": parse_date(standard["report_date"]),
                "report_type": standard.get("report_type"),
                "comp_type": standard.get("comp_type"),
                "projected_end_type": normalize(output.get("end_type")),
                "projected_announce_date": announced,
                "data_available_timestamp": expected_available,
                "vintage_quality": output["vintage_quality"],
                "quality_status": str(output["quality_status"]),
                "source_observed_at": normalize(output.get("source_observed_at")),
                "projection_rule_version": PROJECTION_RULE_VERSION,
                "created_run_id": "p2-financial-pit-projection-v1",
            }
            projections.append(row)
        transform_diagnostics[candidate_source] = source_diagnostics
    projections.sort(key=lambda row: (row["source_dataset"], row["standard_id"]))
    return projections, transform_diagnostics


def verify_environment(kernel, expected_environment_id: str) -> tuple:
    with kernel._db_connection().cursor() as cursor:
        cursor.execute(
            "SELECT current_database(), current_user, environment_id::text, environment_kind, "
            "current_setting('transaction_read_only'), "
            "(SELECT rolsuper FROM pg_roles WHERE rolname=current_user) "
            "FROM integration_test.environment_identity WHERE singleton=TRUE"
        )
        identity = cursor.fetchone()
    if identity != (
        "ashares_test", "ashares_app", expected_environment_id,
        "TEST_FIXTURE", "off", False,
    ):
        raise RuntimeError("projection replay refuses a database without the expected isolated identity")
    return identity


def import_standard_base_rows(
    rows: list[dict], standard_repo: FinancialStandardRepository, kernel,
    context: RunContext,
) -> dict[tuple[str, str, int], int]:
    by_source: dict[str, list[dict]] = {source: [] for source in SOURCE_MAP}
    for item in rows:
        candidate_source = item["candidate"]["source"]
        standard = dict(item["standard"])
        standard.pop("financial_id", None)
        standard.pop("_standard_id", None)
        by_source[candidate_source].append(standard)

    ids: dict[tuple[str, str, int], int] = {}
    for candidate_source, table_rows in by_source.items():
        if not table_rows:
            continue
        _, source_dataset, target = SOURCE_MAP[candidate_source]
        standard_lookup_dataset = {
            "balance": "balance_sheet",
            "cashflow": "cash_flow",
            "income": "income",
        }[source_dataset]
        for offset in range(0, len(table_rows), 500):
            page = table_rows[offset:offset + 500]
            codes = tuple(sorted({str(row["stock_code"]) for row in page}))
            dates = [parse_date(row["report_date"]) for row in page]
            scope = WorkScope(
                date_from=min(dates), date_to=max(dates), codes=codes,
                partition=f"REAL_PIT_PROJECTION_TEST_ONLY:standard-import:{source_dataset}:{offset // 500}",
            )
            kernel.write.bulk_insert_ignore(context, scope, target, page)

        for offset in range(0, len(table_rows), 500):
            page = table_rows[offset:offset + 500]
            keys = [str(row["source_record_key"]) for row in page]
            versions = [int(row["record_version"]) for row in page]
            dates = [parse_date(row["report_date"]) for row in page]
            persisted = standard_repo.standard_rows_by_source_identities(
                standard_lookup_dataset, keys, versions, context,
                min(dates), max(dates), page_limit=500,
            )
            by_identity = {
                (str(row["source_record_key"]), int(row["record_version"])): row
                for row in persisted
            }
            if len(by_identity) != len(page):
                raise RuntimeError(f"isolated Standard import count mismatch for {candidate_source}")
            for original in page:
                identity = (str(original["source_record_key"]), int(original["record_version"]))
                persisted_row = by_identity.get(identity)
                if persisted_row is None or standard_content_hash(persisted_row) != standard_content_hash(original):
                    raise RuntimeError(f"isolated Standard fixture differs at {candidate_source}:{identity}")
                ids[(source_dataset, identity[0], identity[1])] = int(persisted_row["_standard_id"])
    if len(ids) != len(rows):
        raise RuntimeError("isolated Standard identity mapping is incomplete")
    return ids


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--stop-after-batches", type=int)
    parser.add_argument("--resume", action="store_true")
    args = parser.parse_args()
    if args.stop_after_batches is not None and args.stop_after_batches <= 0:
        raise ValueError("stop-after-batches must be positive")
    if args.stop_after_batches is not None and args.resume:
        raise ValueError("stop-after-batches and resume cannot be used together")

    environment_id = os.environ.get("ASHARES_TEST_ENVIRONMENT_ID", "").strip()
    if os.environ.get("ASHARES_ENV") != "integration_test" or not environment_id:
        raise RuntimeError("set the matching isolated-test environment identity before replay")
    input_bytes = INPUT_PATH.read_bytes()
    input_manifest_bytes = INPUT_MANIFEST_PATH.read_bytes()
    input_manifest = json.loads(input_manifest_bytes)
    if sha256(input_bytes) != input_manifest["bundle_sha256"]:
        raise RuntimeError("input bundle hash does not match its manifest")
    content = json.loads(gzip.decompress(input_bytes))
    if len(content["rows"]) != int(input_manifest["row_count"]):
        raise RuntimeError("input bundle row count does not match its manifest")
    projection_rows, transform_diagnostics = prepare_projection_rows(content)
    if len(projection_rows) != 20_469:
        raise RuntimeError(f"expected 20,469 projection rows, got {len(projection_rows)}")

    batch_size = 500
    expected_rows_by_source = {
        source_dataset: sum(row["source_dataset"] == source_dataset for row in projection_rows)
        for source_dataset in ("balance", "cashflow", "income")
    }
    for source_dataset in ("balance", "cashflow", "income"):
        source_rows = sorted(
            (row for row in projection_rows if row["source_dataset"] == source_dataset),
            key=lambda row: int(row["source_standard_id"]),
        )
        for offset in range(0, len(source_rows), batch_size):
            batch_number = offset // batch_size + 1
            for row in source_rows[offset:offset + batch_size]:
                row["batch_number"] = batch_number
                row["projection_content_sha256"] = projection_row_hash(row)
    expected_output_sha256 = canonical_hash([
        [row["source_dataset"], int(row["source_standard_id"]),
         row["projection_content_sha256"]]
        for row in sorted(
            projection_rows,
            key=lambda item: (item["source_dataset"], int(item["source_standard_id"])),
        )
    ])

    identity = {
        "schema": "financial-standard-pit-projection-run-manifest-v1",
        "projection_version": PROJECTION_VERSION,
        "projection_rule_version": PROJECTION_RULE_VERSION,
        "base_projection_version": BASE_PROJECTION_VERSION,
        "input_bundle_sha256": input_manifest["bundle_sha256"],
        "input_manifest_sha256": sha256(input_manifest_bytes),
        "candidate_sha256": input_manifest["candidate_hash"],
        "expected_rows": len(projection_rows),
        "expected_rows_by_source": expected_rows_by_source,
        "expected_output_sha256": expected_output_sha256,
        "batch_size": batch_size,
        "report_start": REPORT_START.isoformat(),
        "report_end": REPORT_END.isoformat(),
        "projection_code_sha256": "pending",
        "execution_environment": "TEST",
    }
    code_files = [
        ROOT / "data_collect/jobs/tushare_financial_raw_to_standard.py",
        ROOT / "data_collect/data_access/repository/financial_standard_repository.py",
        ROOT / "data_collect/data_access/repository/financial_projection_repository.py",
        ROOT / "data_collect/data_access/repository/financial_repository.py",
        ROOT / "data_collect/jobs/tushare_foundation_features.py",
        ROOT / "tools/export_financial_pit_projection.py",
        ROOT / "tools/replay_financial_pit_projection.py",
    ]
    identity["projection_code_sha256"] = canonical_hash({
        path.relative_to(ROOT).as_posix(): sha256(path.read_bytes()) for path in code_files
    })
    manifest_sha256 = canonical_hash(identity)
    selection = FinancialProjectionSelection(PROJECTION_VERSION, manifest_sha256)
    run_id = "p2-financial-pit-projection-v6"
    context = RunContext(
        run_id=run_id,
        job_name="financial_pit_projection_isolated_replay",
        code_version=identity["projection_code_sha256"],
        config_hash=manifest_sha256,
        pipeline_version=PROJECTION_VERSION,
    )

    kernel = DataAccessKernel(policy_registry=build_default_registry())
    try:
        db_identity = verify_environment(kernel, environment_id)
        standard_repo = FinancialStandardRepository(kernel)
        projection_repo = FinancialProjectionRepository(kernel)
        base_id_map = import_standard_base_rows(
            content["rows"], standard_repo, kernel, context,
        )

        projection_manifest_row = {
            "projection_version": PROJECTION_VERSION,
            "manifest_sha256": manifest_sha256,
            "candidate_sha256": input_manifest["candidate_hash"],
            "raw_bundle_sha256": input_manifest["bundle_sha256"],
            "projection_code_sha256": identity["projection_code_sha256"],
            "projection_rule_version": PROJECTION_RULE_VERSION,
            "base_projection_version": BASE_PROJECTION_VERSION,
            "expected_rows": len(projection_rows),
            "expected_rows_by_source": expected_rows_by_source,
            "expected_output_sha256": expected_output_sha256,
            "report_start": REPORT_START,
            "report_end": REPORT_END,
            "created_run_id": run_id,
            "execution_environment": "TEST",
        }
        projection_repo.register_run(projection_manifest_row, context)

        remapped_rows = []
        for row in projection_rows:
            key = (row["source_dataset"], row["source_record_key"], row["source_record_version"])
            test_standard_id = base_id_map.get(key)
            if test_standard_id is None:
                raise RuntimeError(f"isolated Standard mapping missing: {key}")
            remapped = dict(row)
            remapped["standard_id"] = test_standard_id
            remapped_rows.append(remapped)

        batches = []
        for source_dataset in ("balance", "cashflow", "income"):
            selected = [row for row in remapped_rows if row["source_dataset"] == source_dataset]
            selected.sort(key=lambda row: int(row["source_standard_id"]))
            for offset in range(0, len(selected), batch_size):
                number = offset // batch_size + 1
                page = [dict(row) for row in selected[offset:offset + batch_size]]
                for row in page:
                    if int(row["batch_number"]) != number:
                        raise RuntimeError("projection batch numbering is not stable by source Standard identity")
                input_hash = canonical_hash([
                    {
                        "source_standard_id": row["source_standard_id"],
                        "raw_record_id": row["raw_record_id"],
                        "raw_payload_sha256": row["raw_payload_sha256"],
                        "standard_content_sha256": row["standard_content_sha256"],
                    }
                    for row in page
                ])
                output_hash = canonical_hash([
                    row["projection_content_sha256"] for row in page
                ])
                batch_identity = {
                    "projection_version": PROJECTION_VERSION,
                    "source_dataset": source_dataset,
                    "batch_number": number,
                    "first_standard_id": min(row["source_standard_id"] for row in page),
                    "last_standard_id": max(row["source_standard_id"] for row in page),
                    "row_count": len(page),
                    "input_sha256": input_hash,
                    "output_sha256": output_hash,
                    "created_run_id": run_id,
                    "report_start": min(row["report_date"] for row in page),
                    "report_end": max(row["report_date"] for row in page),
                }
                batches.append((batch_identity, page))

        old_standard_hash_before = canonical_hash([
            [row["source_dataset"], row["source_standard_id"], row["standard_content_sha256"]]
            for _, page in batches for row in page
        ])

        incomplete_rejected = False
        interrupted_after = 0
        if args.resume:
            existing_batches = kernel.read._repository_query(
                context, projection_repo.BATCH_TABLE,
                "SELECT COUNT(*) AS batch_count FROM pit.financial_standard_projection_batch "
                "WHERE projection_version=%s",
                (PROJECTION_VERSION,),
                WorkScope(
                    date_from=REPORT_START, date_to=REPORT_END,
                    partition="projection-resume-checkpoint-count",
                ),
                2,
            )
            interrupted_after = int(existing_batches[0]["batch_count"])
            try:
                projection_repo.assert_complete(
                    selection, context, REPORT_START, REPORT_END,
                )
            except ValueError as exc:
                if "incomplete" not in str(exc):
                    raise
                incomplete_rejected = True
            if not 0 < interrupted_after < len(batches) or not incomplete_rejected:
                raise RuntimeError("resume requires an incomplete persisted checkpoint")
            batches_to_write = batches
        else:
            batches_to_write = batches
            if args.stop_after_batches is not None:
                if args.stop_after_batches >= len(batches):
                    raise ValueError("stop-after-batches must be less than total batch count")
                batches_to_write = batches[:args.stop_after_batches]

        for batch_identity, page in batches_to_write:
            projection_repo.append_batch(batch_identity, context)
            projection_repo.append_rows(page, context)

        if args.stop_after_batches is not None:
            try:
                projection_repo.assert_complete(
                    selection, context, REPORT_START, REPORT_END,
                )
            except ValueError as exc:
                if "incomplete" not in str(exc):
                    raise
                incomplete_rejected = True
            if not incomplete_rejected:
                raise RuntimeError("interrupted projection was not rejected by the reader gate")
            print(json.dumps({
                "status": "INTERRUPTED_CHECKPOINT",
                "projection_version": PROJECTION_VERSION,
                "manifest_sha256": manifest_sha256,
                "committed_batches": len(batches_to_write),
                "expected_batches": len(batches),
                "incomplete_version_rejected": incomplete_rejected,
            }, sort_keys=True))
            return

        completeness = projection_repo.assert_complete(
            selection, context, REPORT_START, REPORT_END,
        )

        conflict_rejected = False
        conflict_row = dict(batches[0][1][0])
        conflict_row["source_observed_at"] = (
            conflict_row["source_observed_at"] + timedelta(seconds=1)
            if isinstance(conflict_row["source_observed_at"], datetime)
            else datetime.fromisoformat(str(conflict_row["source_observed_at"])) + timedelta(seconds=1)
        )
        conflict_row["projection_content_sha256"] = projection_row_hash(conflict_row)
        try:
            projection_repo.append_rows([conflict_row], context)
        except ValueError as exc:
            if "different content" not in str(exc):
                raise
            conflict_rejected = True
        if not conflict_rejected:
            raise RuntimeError("same projection identity with changed content was not rejected")

        # The projection table contains interpretations only. It must not alter
        # imported legacy Standard facts, including their old NULL PIT values.
        source_standard_id_by_local_id = {
            (row["source_dataset"], int(row["standard_id"])): int(row["source_standard_id"])
            for row in remapped_rows
        }
        after_hash_parts = []
        for _, source, _ in SOURCE_MAP.values():
            selected = [row for row in remapped_rows if row["source_dataset"] == source]
            for offset in range(0, len(selected), 500):
                page = selected[offset:offset + 500]
                found = standard_repo.standard_rows_by_ids(
                    {"income": "income", "balance": "balance_sheet", "cashflow": "cash_flow"}[source],
                    [int(row["standard_id"]) for row in page], context,
                    min(row["report_date"] for row in page),
                    max(row["report_date"] for row in page), page_limit=500,
                )
                for found_row in found:
                    source_standard_id = source_standard_id_by_local_id[
                        (source, int(found_row["_standard_id"]))
                    ]
                    after_hash_parts.append([
                        source,
                        source_standard_id,
                        standard_content_hash(found_row),
                        found_row.get("data_available_timestamp"),
                    ])
        old_standard_hash_after = canonical_hash([
            [source, sid, digest]
            for source, sid, digest, _ in sorted(after_hash_parts, key=lambda item: (item[0], item[1]))
        ])
        if old_standard_hash_after != old_standard_hash_before:
            raise RuntimeError("old Standard content changed during PIT projection replay")
        if any(item[3] is not None for item in after_hash_parts):
            raise RuntimeError("legacy Standard PIT values were modified by overlay projection")

        sample_codes = sorted({row["ts_code"] for row in remapped_rows})[:30]
        financial_repo = FinancialRepository(kernel)
        base_events = financial_repo.load_events(
            sample_codes, REPORT_START, REPORT_END, context,
        )
        selected_events = financial_repo.load_events(
            sample_codes, REPORT_START, REPORT_END, context, selection,
        )
        if base_events:
            raise RuntimeError("unprojected Standard lookup unexpectedly exposed missing PIT rows")
        projected_events = [
            event for event in selected_events
            if event["dataset"] in {"income", "balance", "cashflow"}
        ]
        if not projected_events or any(
            event["projection_version"] != PROJECTION_VERSION
            or event["vintage_quality"] != "SOURCE_ANNOUNCEMENT_ASSUMED"
            for event in projected_events
        ):
            raise RuntimeError("Feature input reader did not select the requested PIT projection")
        expected_raw_identities = {
            (row["source_dataset"], row["source_record_key"], int(row["raw_record_id"]))
            for row in remapped_rows
        }
        if any(
            (event["dataset"], event["source_record_key"], int(event["record_version"]))
            not in expected_raw_identities
            for event in projected_events
        ):
            raise RuntimeError("projected input changed the source record_version")

        count_rows = kernel.read._repository_query(
            context,
            "pit.financial_standard_projection",
            """SELECT source_dataset, COUNT(*) AS row_count,
                      COUNT(DISTINCT raw_record_id) AS unique_raw_versions,
                      COUNT(*) FILTER (WHERE vintage_quality='SOURCE_ANNOUNCEMENT_ASSUMED') AS assumed_rows,
                      COUNT(*) FILTER (WHERE projected_end_type IS NULL) AS null_end_type_rows
                 FROM pit.financial_standard_projection
                WHERE projection_version=%s
                GROUP BY source_dataset ORDER BY source_dataset""",
            (PROJECTION_VERSION,),
            WorkScope(date_from=REPORT_START, date_to=REPORT_END, partition="projection-result-counts"),
            10,
        )
        counts_by_source = {
            row["source_dataset"]: {
                "rows": int(row["row_count"]),
                "unique_raw_versions": int(row["unique_raw_versions"]),
                "assumed_pit_rows": int(row["assumed_rows"]),
                "null_end_type_rows": int(row["null_end_type_rows"]),
            }
            for row in count_rows
        }
        with kernel._db_connection().cursor() as cursor:
            cursor.execute("SELECT pg_database_size(current_database())")
            database_bytes = int(cursor.fetchone()[0])
    finally:
        kernel.close()

    result = {
        "schema": "financial-pit-projection-replay-result-v1",
        "status": "ISOLATED_TEST_ONLY",
        "database_identity": {
            "database": db_identity[0], "role": db_identity[1],
            "environment_id": db_identity[2], "environment_kind": db_identity[3],
            "transaction_read_only": db_identity[4], "superuser": db_identity[5],
        },
        "projection_version": PROJECTION_VERSION,
        "manifest_sha256": manifest_sha256,
        "candidate_sha256": input_manifest["candidate_hash"],
        "input_bundle_sha256": input_manifest["bundle_sha256"],
        "projection_code_sha256": identity["projection_code_sha256"],
        "expected_rows": len(projection_rows),
        "isolated_standard_base_rows": len(content["rows"]),
        "projection_rows_by_source": counts_by_source,
        "total_projection_rows": completeness["actual_rows"],
        "completed_batches": completeness["batch_count"],
        "interrupt_after_batches": interrupted_after,
        "incomplete_version_rejected": incomplete_rejected,
        "same_identity_changed_content_rejected": conflict_rejected,
        "old_standard_before_sha256": old_standard_hash_before,
        "old_standard_after_sha256": old_standard_hash_after,
        "old_standard_content_unchanged": old_standard_hash_before == old_standard_hash_after,
        "old_standard_pit_null_rows_unchanged": len(after_hash_parts),
        "base_reader_rows_for_missing_timestamps": len(base_events),
        "explicit_projection_reader_rows": len(projected_events),
        "feature_input_projection_version": PROJECTION_VERSION,
        "transform_diagnostics": transform_diagnostics,
        "database_bytes": database_bytes,
        "recovery": "process_restart_same_run_and_manifest; committed rows were idempotent no-ops",
        "production_write": "NOT RUN",
    }
    out_bytes = json.dumps(
        result, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
    ).encode("utf-8") + b"\n"
    if OUTPUT_PATH.exists() and OUTPUT_PATH.read_bytes() != out_bytes:
        raise RuntimeError("immutable projection result collision; create a new run version")
    if not OUTPUT_PATH.exists():
        OUTPUT_PATH.write_bytes(out_bytes)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
