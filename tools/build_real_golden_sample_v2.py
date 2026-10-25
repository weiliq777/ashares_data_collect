"""Build the immutable v2 golden bundle from v1's retained Standard evidence."""
from __future__ import annotations

import copy
import gzip
import hashlib
import json
from collections import defaultdict
from datetime import date, datetime, time
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from data_collect.alpha_research.contracts import FACTOR_SET_VERSION
from data_collect.alpha_research.factors import factor_values
from data_collect.alpha_research.golden_sample import canonical_json, content_hash
from data_collect.jobs.tushare_foundation_features import (
    calculate_debt_to_assets_features,
    calculate_features,
)

ROOT = Path(__file__).resolve().parents[1]
V1_BUNDLE = ROOT / ".codex" / "real-sample" / "real-golden-v1.json.gz"
V1_MANIFEST = ROOT / "tests" / "fixtures" / "real_golden_sample_v1.manifest.json"
V2_BUNDLE = ROOT / ".codex" / "real-sample" / "real-golden-v2.json.gz"
V2_MANIFEST = ROOT / "tests" / "fixtures" / "real_golden_sample_v2.manifest.json"
SHANGHAI = ZoneInfo("Asia/Shanghai")


def _parse_events(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    parsed = []
    for raw in events:
        row = dict(raw)
        for name in ("report_date", "effective_announce_date", "version_available_date"):
            value = row.get(name)
            if value:
                row[name] = date.fromisoformat(str(value)[:10])
        for name in ("data_available_timestamp", "source_observed_at"):
            value = row.get(name)
            if value:
                text = str(value)
                if text.endswith("Z"):
                    text = f"{text[:-1]}+00:00"
                row[name] = datetime.fromisoformat(text)
        parsed.append(row)
    return parsed


def _factor_reference(
    source: list[dict[str, Any]], features: list[dict[str, Any]], signal_date: date,
) -> list[dict[str, Any]]:
    cutoff = datetime.combine(signal_date, time(15, 0), SHANGHAI)
    by_code: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in features:
        available = row.get("data_available_timestamp")
        if row["report_date"] <= signal_date and available is not None and available < cutoff:
            by_code[str(row["ts_code"])].append(row)

    output = []
    for original in source:
        row = dict(original)
        visible = sorted(
            by_code.get(str(row["ts_code"]), ()),
            key=lambda item: (
                item["report_date"], item["data_available_timestamp"],
                int(item.get("record_version") or 0),
            ),
        )
        feature = visible[-1] if visible else None
        factor_row = {
            "research_date": signal_date,
            "debt_to_assets": feature.get("debt_to_assets") if feature else None,
            "debt_to_assets_quality_status": feature.get("quality_status") if feature else None,
            "debt_to_assets_quality_reason": feature.get("quality_reason") if feature else None,
            "debt_to_assets_data_available_timestamp": feature.get("data_available_timestamp") if feature else None,
            "debt_to_assets_source_observed_at": feature.get("source_observed_at") if feature else None,
            "debt_to_assets_vintage_quality": feature.get("vintage_quality") if feature else None,
            "debt_to_assets_source_quality_status": feature.get("source_quality_status") if feature else None,
            "debt_to_assets_source_record_key": feature.get("source_record_key") if feature else None,
            "debt_to_assets_source_record_version": feature.get("record_version") if feature else None,
            "debt_to_assets_source_payload_hash": feature.get("payload_hash") if feature else None,
            "debt_to_assets_feature_contract_version": feature.get("feature_contract_version") if feature else None,
            "debt_to_assets_report_date": feature.get("report_date") if feature else None,
        }
        value = factor_values(factor_row)["debt_to_assets"]
        row["factors"] = {
            **row["factors"],
            "debt_to_assets": {
                "status": value.status,
                "reason": value.reason,
                "value": value.value,
            },
        }
        output.append(row)
    return output


def _write_immutable(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if path.read_bytes() != payload:
            raise RuntimeError(f"refusing to overwrite immutable artifact: {path}")
        return
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_bytes(payload)
    temporary.replace(path)


def build() -> dict[str, Any]:
    compressed_v1 = V1_BUNDLE.read_bytes()
    manifest_v1 = json.loads(V1_MANIFEST.read_text(encoding="utf-8"))
    if hashlib.sha256(compressed_v1).hexdigest() != manifest_v1["bundle_sha256"]:
        raise RuntimeError("v1 bundle hash mismatch; v1 must remain the source of this derivation")
    source = json.loads(gzip.decompress(compressed_v1).decode("utf-8"))
    if source.get("sample_version") != "real-golden-v1":
        raise RuntimeError("unexpected source golden-sample version")

    bundle = copy.deepcopy(source)
    bundle["sample_version"] = "real-golden-v2"
    bundle["schema_version"] = "real-golden-sample-bundle-v2"
    financial_features = calculate_features(
        _parse_events(bundle["financial_standard_events"]),
        date(2015, 1, 1), date(2023, 6, 30),
    )
    financial_features.sort(key=lambda row: (
        str(row.get("ts_code") or ""), row["report_date"],
        row["data_available_timestamp"], int(row.get("record_version") or 0),
    ))
    bundle["financial_features_recomputed"] = financial_features
    da_features = calculate_debt_to_assets_features(
        _parse_events(bundle["financial_standard_events"]),
        date(2015, 1, 1), date(2023, 6, 30),
    )
    da_features.sort(key=lambda row: (
        str(row.get("ts_code") or ""), row["report_date"],
        row["data_available_timestamp"], int(row.get("record_version") or 0),
    ))
    bundle["debt_to_assets_features"] = da_features
    bundle["factor_reference"] = _factor_reference(
        bundle["factor_reference"], da_features,
        date.fromisoformat(str(bundle["signal_date"])[:10]),
    )
    warning_case = bundle["quality_gate_cases"]["financial_source_warning"]
    warning_feature = next(
        row for row in financial_features
        if row["ts_code"] == warning_case["ts_code"]
        and str(row["report_date"])[:10] == str(warning_case["report_date"])[:10]
    )
    manifest_quality_cases = copy.deepcopy(manifest_v1["quality_cases"])
    manifest_quality_cases["financial_source_warning_case"]["feature_sha256"] = content_hash(
        warning_feature,
    )
    bundle["versions"] = {
        **bundle.get("versions", {}),
        "factor_set": FACTOR_SET_VERSION,
        "research_schema": "research-v2.2",
        "debt_to_assets_feature_contract": "debt-to-assets-v1",
    }

    plain = canonical_json(bundle)
    compressed = gzip.compress(plain, mtime=0)
    bundle_hash = hashlib.sha256(compressed).hexdigest()
    manifest = copy.deepcopy(manifest_v1)
    manifest.update({
        "sample_version": "real-golden-v2",
        "sample_seed": "real-golden-v2-preserves-real-golden-v1-selection",
        "selection_method": (
            "Preserves the exact v1 securities and dates; derives independent D/A "
            "features and references from bundled versioned Standard balance-sheet evidence."
        ),
        "bundle_sha256": bundle_hash,
        "bundle_schema_version": bundle["schema_version"],
        "research_schema_version": "research-v2.2",
        "factor_set_version": FACTOR_SET_VERSION,
        "factor_contracts": {"debt_to_assets": "debt-to-assets-v1"},
        "quality_cases": manifest_quality_cases,
        "row_counts": {
            **manifest_v1["row_counts"],
            "debt_to_assets_features": len(da_features),
        },
        "content_hashes": {
            **manifest_v1["content_hashes"],
            "financial_features_recomputed": content_hash(bundle["financial_features_recomputed"]),
            "debt_to_assets_features": content_hash(da_features),
            "factor_reference": content_hash(bundle["factor_reference"]),
            "bundle_sha256": bundle_hash,
            "bundle_uncompressed_sha256": hashlib.sha256(plain).hexdigest(),
        },
        "source_dataset": {
            **manifest_v1["source_dataset"],
            "scope_note": (
                "Same bounded legacy v2.1 source sample as v1. The v2 D/A oracle is "
                "independently derived from the bundled versioned Standard balance-sheet "
                "events; it is not a production Release or full-market quality claim."
            ),
        },
    })
    manifest_bytes = canonical_json(manifest) + b"\n"
    _write_immutable(V2_BUNDLE, compressed)
    _write_immutable(V2_MANIFEST, manifest_bytes)
    return {
        "sample_version": bundle["sample_version"],
        "securities": len(bundle["securities"]),
        "balance_sheet_features": len(da_features),
        "factor_reference_rows": len(bundle["factor_reference"]),
        "bundle_sha256": bundle_hash,
        "manifest_sha256": hashlib.sha256(manifest_bytes).hexdigest(),
        "bundle_bytes": len(compressed),
        "manifest_path": str(V2_MANIFEST.relative_to(ROOT)),
        "bundle_path": str(V2_BUNDLE.relative_to(ROOT)),
    }


if __name__ == "__main__":
    print(json.dumps(build(), ensure_ascii=False, sort_keys=True, indent=2))
