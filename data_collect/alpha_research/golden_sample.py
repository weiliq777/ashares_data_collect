"""Deterministic identity helpers for bounded real-market integration samples."""
from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Mapping
from typing import Any

GOLDEN_SAMPLE_VERSION = "real-golden-v1"
SAMPLE_SEED = GOLDEN_SAMPLE_VERSION
SAMPLE_SIZE = 30
RANDOM_SAMPLE_SIZE = 24

# These cases are retained deliberately so the sample exercises distinct
# failure paths. They are not part of the hash-selected population.
EDGE_CASES = {
    "000666.SZ": "known delisting path; terminal value must remain unknown",
    "002800.SZ": "suspension path in the forward-label window",
    "002721.SZ": "later suspension path in a longer forward window",
    "000046.SZ": "near-delisting path with unresolved terminal value",
    "688426.SH": "historical snapshot lacks a usable financial PIT feature",
    "300965.SZ": "signal-date price gap; included to exercise missing-price handling",
}


def canonical_json(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
        allow_nan=False,
    ).encode("utf-8")


def content_hash(value: Any) -> str:
    return hashlib.sha256(canonical_json(value)).hexdigest()


def select_codes(
    candidates: Iterable[str],
    *,
    seed: str = SAMPLE_SEED,
    edge_cases: Mapping[str, str] = EDGE_CASES,
    sample_size: int = SAMPLE_SIZE,
) -> list[dict[str, str]]:
    """Choose fixed boundary cases plus a deterministic hash-ordered sample."""
    required = list(edge_cases)
    if len(required) >= sample_size:
        raise ValueError("edge cases must leave room for hash-selected securities")
    available = {str(code).strip() for code in candidates if str(code).strip()}

    random_quota = sample_size - len(required)
    ordinary = sorted(
        available - set(required),
        key=lambda code: (hashlib.md5(f"{code}{seed}".encode()).hexdigest(), code),
    )[:random_quota]
    if len(ordinary) != random_quota:
        raise ValueError(
            f"candidate universe has only {len(ordinary)} ordinary securities; "
            f"{random_quota} required"
        )
    rows = [
        {"ts_code": code, "selection_role": "EDGE_CASE", "selection_reason": reason}
        for code, reason in edge_cases.items()
    ]
    rows.extend(
        {
            "ts_code": code,
            "selection_role": "HASH_SAMPLE",
            "selection_reason": f"lowest md5(ts_code || {seed}) among signal-date eligible candidates",
        }
        for code in ordinary
    )
    return rows


def validate_manifest(manifest: Mapping[str, Any]) -> None:
    required = {
        "sample_version", "signal_date", "target_date", "sample_seed",
        "securities", "source_dataset", "source_snapshots", "bundle_sha256",
    }
    missing = sorted(required - set(manifest))
    if missing:
        raise ValueError(f"golden sample manifest is missing fields: {missing}")
    if manifest["sample_version"] != GOLDEN_SAMPLE_VERSION:
        raise ValueError("golden sample version mismatch")
    rows = manifest["securities"]
    if not isinstance(rows, list) or len(rows) != SAMPLE_SIZE:
        raise ValueError(f"golden sample must contain exactly {SAMPLE_SIZE} securities")
    codes = [str(row.get("ts_code") or "") for row in rows]
    if any(not code for code in codes) or len(set(codes)) != len(codes):
        raise ValueError("golden sample securities must be non-empty and unique")
    if not str(manifest["bundle_sha256"]).strip():
        raise ValueError("golden sample bundle hash is required")
