"""Build an immutable v3 sample from v2's retained Standard evidence."""
from __future__ import annotations

import copy
import gzip
import hashlib
import json
from datetime import date
from pathlib import Path
from typing import Any

from data_collect.alpha_research.contracts import FACTOR_SET_VERSION
from data_collect.alpha_research.golden_sample import canonical_json, content_hash
from data_collect.jobs.tushare_foundation_features import (
    calculate_debt_to_assets_features,
    calculate_features,
)
from tests.integration_fixtures.real_golden_sample import (
    _assert_debt_to_assets_source_identity,
)
from tools.build_real_golden_sample_v2 import (
    _factor_reference,
    _parse_events,
    _write_immutable,
)

ROOT = Path(__file__).resolve().parents[1]
V2_BUNDLE = ROOT / ".codex" / "real-sample" / "real-golden-v2.json.gz"
V2_MANIFEST = ROOT / "tests" / "fixtures" / "real_golden_sample_v2.manifest.json"
V3_BUNDLE = ROOT / ".codex" / "real-sample" / "real-golden-v3.json.gz"
V3_MANIFEST = ROOT / "tests" / "fixtures" / "real_golden_sample_v3.manifest.json"
FEATURE_START = date(2015, 1, 1)
FEATURE_END = date(2023, 6, 30)


def _validate_parent(bundle: dict[str, Any], manifest: dict[str, Any], compressed: bytes) -> None:
    if hashlib.sha256(compressed).hexdigest() != manifest.get("bundle_sha256"):
        raise RuntimeError("v2 bundle hash mismatch; refusing to derive v3")
    if bundle.get("sample_version") != "real-golden-v2":
        raise RuntimeError("unexpected parent golden-sample version")
    if bundle.get("schema_version") != "real-golden-sample-bundle-v2":
        raise RuntimeError("unexpected parent golden-sample schema")
    for key in (
        "observations", "execution_bars", "company_actions", "benchmark",
        "financial_standard_events", "financial_features_recomputed",
        "debt_to_assets_features", "factor_reference", "label_reference",
        "system_labels", "signal_observations",
    ):
        expected = manifest.get("content_hashes", {}).get(key)
        if expected and content_hash(bundle.get(key)) != expected:
            raise RuntimeError(f"v2 {key} content hash mismatch; refusing to derive v3")


def build() -> dict[str, Any]:
    compressed_v2 = V2_BUNDLE.read_bytes()
    manifest_v2_bytes = V2_MANIFEST.read_bytes()
    manifest_v2 = json.loads(manifest_v2_bytes.decode("utf-8"))
    source = json.loads(gzip.decompress(compressed_v2).decode("utf-8"))
    _validate_parent(source, manifest_v2, compressed_v2)

    bundle = copy.deepcopy(source)
    events = _parse_events(bundle["financial_standard_events"])
    financial_features = calculate_features(events, FEATURE_START, FEATURE_END)
    financial_features.sort(key=lambda row: (
        str(row.get("ts_code") or ""), row["report_date"],
        row["data_available_timestamp"], int(row.get("record_version") or 0),
    ))
    debt_to_assets_features = calculate_debt_to_assets_features(
        events, FEATURE_START, FEATURE_END,
    )
    debt_to_assets_features.sort(key=lambda row: (
        str(row.get("ts_code") or ""), row["report_date"],
        row["data_available_timestamp"], int(row.get("record_version") or 0),
    ))
    _assert_debt_to_assets_source_identity(debt_to_assets_features, events)

    signal_date = date.fromisoformat(str(bundle["signal_date"])[:10])
    factor_reference = _factor_reference(
        bundle["factor_reference"], debt_to_assets_features, signal_date,
    )
    bundle.update({
        "sample_version": "real-golden-v3",
        "schema_version": "real-golden-sample-bundle-v3",
        "financial_features_recomputed": financial_features,
        "debt_to_assets_features": debt_to_assets_features,
        "factor_reference": factor_reference,
        "source_provenance": {
            "parent_sample_version": "real-golden-v2",
            "parent_bundle_sha256": hashlib.sha256(compressed_v2).hexdigest(),
            "parent_manifest_sha256": hashlib.sha256(manifest_v2_bytes).hexdigest(),
            "financial_standard_events_sha256": content_hash(
                bundle["financial_standard_events"],
            ),
            "feature_contract_version": "debt-to-assets-v1",
            "source_selection": "exact Standard balance-sheet source identity",
        },
    })

    plain = canonical_json(bundle)
    compressed = gzip.compress(plain, mtime=0)
    bundle_sha256 = hashlib.sha256(compressed).hexdigest()
    manifest = copy.deepcopy(manifest_v2)
    manifest.update({
        "sample_version": "real-golden-v3",
        "sample_seed": "real-golden-v3-preserves-real-golden-v2-selection",
        "selection_method": (
            "Preserves the v2 securities and dates; recalculates financial and D/A "
            "Features from the immutable Standard events, validating exact report-period identity."
        ),
        "bundle_schema_version": bundle["schema_version"],
        "bundle_sha256": bundle_sha256,
        "research_schema_version": "research-v2.2",
        "factor_set_version": FACTOR_SET_VERSION,
        "factor_contracts": {"debt_to_assets": "debt-to-assets-v1"},
        "parent_sample": {
            "sample_version": "real-golden-v2",
            "bundle_sha256": hashlib.sha256(compressed_v2).hexdigest(),
            "manifest_sha256": hashlib.sha256(manifest_v2_bytes).hexdigest(),
        },
        "source_provenance": bundle["source_provenance"],
        "row_counts": {
            **manifest_v2["row_counts"],
            "financial_features_recomputed": len(financial_features),
            "debt_to_assets_features": len(debt_to_assets_features),
            "factor_reference": len(factor_reference),
        },
        "content_hashes": {
            **manifest_v2["content_hashes"],
            "financial_features_recomputed": content_hash(financial_features),
            "debt_to_assets_features": content_hash(debt_to_assets_features),
            "factor_reference": content_hash(factor_reference),
            "bundle_sha256": bundle_sha256,
            "bundle_uncompressed_sha256": hashlib.sha256(plain).hexdigest(),
        },
        "source_dataset": {
            **manifest_v2["source_dataset"],
            "scope_note": (
                "Bounded legacy source sample derived from retained Standard events. "
                "D/A rows are matched to the exact source report period; this is not "
                "a production Release or full-market quality claim."
            ),
        },
    })
    manifest_bytes = canonical_json(manifest) + b"\n"
    _write_immutable(V3_BUNDLE, compressed)
    _write_immutable(V3_MANIFEST, manifest_bytes)
    return {
        "sample_version": bundle["sample_version"],
        "parent_bundle_sha256": manifest["parent_sample"]["bundle_sha256"],
        "parent_manifest_sha256": manifest["parent_sample"]["manifest_sha256"],
        "standard_events": len(bundle["financial_standard_events"]),
        "financial_features": len(financial_features),
        "debt_to_assets_features": len(debt_to_assets_features),
        "factor_reference_rows": len(factor_reference),
        "bundle_sha256": bundle_sha256,
        "manifest_sha256": hashlib.sha256(manifest_bytes).hexdigest(),
        "bundle_bytes": len(compressed),
        "manifest_path": str(V3_MANIFEST.relative_to(ROOT)),
        "bundle_path": str(V3_BUNDLE.relative_to(ROOT)),
    }


if __name__ == "__main__":
    print(json.dumps(build(), ensure_ascii=False, sort_keys=True, indent=2))
