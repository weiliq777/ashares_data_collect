"""Export the bounded P2-B Raw + Standard identities through read-only repositories."""
from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import json
import sys
from datetime import date, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from data_collect.data_access import DataAccessKernel, RunContext
from data_collect.data_access.policy_catalog import build_default_registry
from data_collect.data_access.repository.financial_standard_repository import (
    FinancialStandardRepository,
)

BATCH_DIR = ROOT / ".codex" / "p2-batches"
CANDIDATE_PATH = BATCH_DIR / "financial-pit-projection-candidates-v2.json.gz"
CANDIDATE_MANIFEST_PATH = BATCH_DIR / "financial-pit-projection-candidates-v2.manifest.json"
OUTPUT_PATH = BATCH_DIR / "financial-pit-projection-input-v1.json.gz"
OUTPUT_MANIFEST_PATH = BATCH_DIR / "financial-pit-projection-input-v1.manifest.json"
SOURCE_MAP = {
    "balance_sheet": ("balancesheet", "balance_sheet"),
    "cash_flow": ("cashflow", "cash_flow"),
    "income": ("income", "income"),
}
REPORT_START = date(2015, 1, 1)
REPORT_END = date(2021, 8, 1)


def json_value(value):
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    raise TypeError(f"cannot serialize {type(value).__name__}")


def canonical_bytes(value) -> bytes:
    return json.dumps(
        value, sort_keys=True, ensure_ascii=False, separators=(",", ":"),
        default=json_value, allow_nan=False,
    ).encode("utf-8")


def sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def write_immutable(path: Path, payload: bytes) -> None:
    if path.exists() and path.read_bytes() != payload:
        raise RuntimeError(f"immutable artifact collision at {path.name}; use a new version")
    if not path.exists():
        path.write_bytes(payload)


def read_bundle(path: Path) -> dict:
    return json.loads(gzip.decompress(path.read_bytes()))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=OUTPUT_PATH)
    parser.add_argument("--manifest", type=Path, default=OUTPUT_MANIFEST_PATH)
    args = parser.parse_args()

    candidate_bytes = CANDIDATE_PATH.read_bytes()
    candidate_manifest_bytes = CANDIDATE_MANIFEST_PATH.read_bytes()
    candidate_manifest = json.loads(candidate_manifest_bytes)
    if sha256(candidate_bytes) != candidate_manifest["bundle_sha256"]:
        raise RuntimeError("candidate bundle hash does not match its immutable manifest")
    candidates = read_bundle(CANDIDATE_PATH)["candidate_rows"]
    if len(candidates) != int(candidate_manifest["counts"]["total"]):
        raise RuntimeError("candidate row count differs from its manifest")

    context = RunContext(
        run_id="p2-pit-projection-export-v1",
        job_name="p2_pit_projection_readonly_export",
        code_version="financial-source-announcement-projection-v2",
        config_hash=candidate_manifest["batch_hash"],
        pipeline_version="financial-pit-projection-input-v1",
    )
    kernel = DataAccessKernel(policy_registry=build_default_registry(), read_only_audit=True)
    exported = []
    try:
        with kernel._db_connection().cursor() as cursor:
            cursor.execute(
                "SELECT current_database(), current_user, current_setting('transaction_read_only'), "
                "(SELECT rolsuper FROM pg_roles WHERE rolname=current_user)"
            )
            identity = cursor.fetchone()
        if identity != ("ashares", "ashares_app", "on", False):
            raise RuntimeError("production source identity is not the verified read-only app role")

        repository = FinancialStandardRepository(kernel)
        for source, (raw_dataset, _) in SOURCE_MAP.items():
            source_candidates = [row for row in candidates if row["source"] == source]
            for offset in range(0, len(source_candidates), 200):
                page = source_candidates[offset:offset + 200]
                raw_ids = [int(row["raw_record_id"]) for row in page]
                standard_ids = [int(row["standard_id"]) for row in page]
                raw_rows = repository.raw_versions_by_ids(
                    raw_dataset, raw_ids, context, REPORT_START, REPORT_END,
                    page_limit=200,
                )
                standard_rows = repository.standard_rows_by_ids(
                    source, standard_ids, context, REPORT_START, REPORT_END,
                    page_limit=200,
                )
                raw_by_id = {int(row["_record_id"]): row for row in raw_rows}
                standard_by_id = {
                    int(row["_standard_id"]): row for row in standard_rows
                }
                if set(raw_by_id) != set(raw_ids) or set(standard_by_id) != set(standard_ids):
                    raise RuntimeError(f"exact source lookup failed for {source} page {offset // 200}")
                for candidate in page:
                    raw = raw_by_id[int(candidate["raw_record_id"])]
                    standard = standard_by_id[int(candidate["standard_id"])]
                    checks = {
                        "raw_payload_hash": raw["_payload_hash"] == candidate["raw_payload_hash"],
                        "raw_version": int(raw["_record_id"]) == int(candidate["raw_record_id"]),
                        "raw_code": raw.get("ts_code") == candidate["raw_ts_code"],
                        "raw_report_date": str(raw.get("end_date")).replace("-", "")[:8]
                        == str(candidate["raw_report_date"]).replace("-", "")[:8],
                        "standard_id": int(standard["financial_id"]) == int(candidate["standard_id"]),
                        "standard_version": int(standard["record_version"]) == int(candidate["record_version"]),
                        "standard_payload_hash": standard["payload_hash"] == candidate["standard_payload_hash"],
                        "standard_missing_timestamp": standard.get("data_available_timestamp") is None,
                        "standard_code": standard["stock_code"] == candidate["ts_code"],
                        "standard_report_date": str(standard["report_date"])[:10] == str(candidate["report_date"])[:10],
                    }
                    if not all(checks.values()):
                        raise RuntimeError(
                            f"source identity mismatch source={source} standard_id="
                            f"{candidate['standard_id']} checks={checks}"
                        )
                    exported.append({
                        "candidate": candidate,
                        "raw": raw,
                        "standard": standard,
                    })
    finally:
        kernel.close()

    exported.sort(key=lambda item: (
        item["candidate"]["source"], int(item["candidate"]["standard_id"]),
    ))
    if len(exported) != len(candidates):
        raise RuntimeError("exported source row count does not match candidate manifest")
    content = {
        "schema": "financial-pit-projection-input-bundle-v1",
        "projection_version": candidate_manifest["projection_version"],
        "candidate_bundle_sha256": candidate_manifest["bundle_sha256"],
        "candidate_manifest_sha256": sha256(candidate_manifest_bytes),
        "candidate_hash": candidate_manifest["candidate_hash"],
        "rows": exported,
    }
    uncompressed = canonical_bytes(content)
    compressed_buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=compressed_buffer, mode="wb", filename="", mtime=0, compresslevel=9) as stream:
        stream.write(uncompressed)
    bundle_bytes = compressed_buffer.getvalue()
    manifest = {
        "schema": "financial-pit-projection-input-manifest-v1",
        "status": "READ_ONLY_SOURCE_EXPORT",
        "projection_version": candidate_manifest["projection_version"],
        "candidate_bundle_sha256": candidate_manifest["bundle_sha256"],
        "candidate_manifest_sha256": sha256(candidate_manifest_bytes),
        "candidate_hash": candidate_manifest["candidate_hash"],
        "row_count": len(exported),
        "by_source": {
            source: sum(row["candidate"]["source"] == source for row in exported)
            for source in SOURCE_MAP
        },
        "raw_strict_identity": candidate_manifest["counts"]["raw_exact_identity"],
        "nan_null_end_type_rows": candidate_manifest["counts"]["nan_null_normalizations"],
        "uncompressed_sha256": sha256(uncompressed),
        "bundle_sha256": sha256(bundle_bytes),
        "bundle_bytes": len(bundle_bytes),
        "input_content_sha256": sha256(canonical_bytes(exported)),
        "production_identity": {
            "database": "ashares", "role": "ashares_app",
            "transaction_read_only": "on", "superuser": False,
        },
    }
    manifest_bytes = canonical_bytes(manifest) + b"\n"
    write_immutable(args.output, bundle_bytes)
    write_immutable(args.manifest, manifest_bytes)
    print(json.dumps({
        "bundle": str(args.output), "bundle_sha256": manifest["bundle_sha256"],
        "bundle_bytes": manifest["bundle_bytes"], "row_count": manifest["row_count"],
        "input_content_sha256": manifest["input_content_sha256"],
        "manifest": str(args.manifest),
    }, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
