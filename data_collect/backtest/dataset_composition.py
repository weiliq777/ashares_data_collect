"""Pure contracts for composing immutable, date-partitioned backtest datasets.

Composition is metadata-only: it records which frozen materialized dataset owns
each open date.  It never copies bars, snapshots, or other fact rows.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import hashlib
import json
from typing import Any, Iterable, Mapping, Sequence


MAX_COMPONENTS = 128
COMPOSITION_SCHEMA_VERSION = "backtest-dataset-composition-v1"


@dataclass(frozen=True)
class DatasetComponent:
    dataset_id: str
    effective_from: date
    effective_to: date
    input_manifest_hash: str
    manifest_hash: str
    baseline_tag: str
    baseline_quality_report_hash: str

    def canonical(self) -> dict[str, Any]:
        return {
            "dataset_id": self.dataset_id,
            "effective_from": self.effective_from.isoformat(),
            "effective_to": self.effective_to.isoformat(),
            "input_manifest_hash": self.input_manifest_hash,
            "manifest_hash": self.manifest_hash,
            "baseline_tag": self.baseline_tag,
            "baseline_quality_report_hash": self.baseline_quality_report_hash,
        }


def stable_hash(value: Any) -> str:
    payload = json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
        default=lambda item: item.isoformat() if isinstance(item, date) else str(item),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def frozen_manifest_hash(manifest: Mapping[str, Any]) -> str:
    """Hash the immutable inputs a physical component contributes."""
    fields = (
        "dataset_id", "dataset_version", "dataset_name", "baseline",
        "baseline_quality_report_hash", "baseline_source_manifest_hash",
        "schema_version", "adapter_version", "feature_version",
        "price_adjustment_mode", "warmup_start", "evaluation_start",
        "evaluation_end", "calendar_rule_version", "market_rule_version",
        "fee_rule_version", "benchmark_code", "input_manifest_hash",
        "calendar_hash", "rules_hash", "execution_hash",
        "corporate_action_hash", "benchmark_hash", "quality_report_hash",
        "selected_columns",
        "availability_rules",
    )
    payload = {key: manifest.get(key) for key in fields}
    for key, value in tuple(payload.items()):
        if key.endswith("_hash") and isinstance(value, str):
            payload[key] = value.strip()
    return stable_hash(payload)


def assert_compatible_manifests(manifests: Sequence[Mapping[str, Any]]) -> None:
    if len(manifests) < 2:
        raise ValueError("组合数据集至少需要两个已冻结的数据分段")
    contract_fields = (
        "schema_version", "adapter_version", "feature_version",
        "price_adjustment_mode", "calendar_rule_version", "market_rule_version",
        "fee_rule_version", "benchmark_code", "selected_columns",
        "availability_rules",
    )
    first = manifests[0]
    mismatches = {
        field: [manifest.get(field) for manifest in manifests]
        for field in contract_fields
        if any(manifest.get(field) != first.get(field) for manifest in manifests[1:])
    }
    if mismatches:
        raise ValueError(f"分段数据集契约不兼容: {mismatches}")
    if any(manifest.get("status") != "FROZEN" for manifest in manifests):
        raise ValueError("组合数据集只能引用 FROZEN 子数据集")
    if any(manifest.get("dataset_kind", "MATERIALIZED") != "MATERIALIZED" for manifest in manifests):
        raise ValueError("组合的叶子分段必须是物化 FROZEN Dataset")
    for manifest in manifests:
        if not manifest.get("input_manifest_hash"):
            raise ValueError("FROZEN 子数据集缺少 input_manifest_hash")
        if not manifest.get("baseline_quality_report_hash"):
            raise ValueError("FROZEN 子数据集缺少基线质量报告哈希")


def validate_component_coverage(
    components: Sequence[DatasetComponent],
    expected_open_dates: Iterable[date],
) -> None:
    """Require ordered spans to partition the frozen open-date calendar exactly."""
    if not components:
        raise ValueError("组合数据集没有分段")
    if len(components) > MAX_COMPONENTS:
        raise ValueError(
            f"组合分段数超过安全上限 {MAX_COMPONENTS}；请按批次合并元数据清单"
        )
    dates = tuple(sorted(set(expected_open_dates)))
    if not dates:
        raise ValueError("组合数据集的冻结交易日历为空")
    if components[0].effective_from != dates[0] or components[-1].effective_to != dates[-1]:
        raise ValueError("组合分段边界必须覆盖完整冻结交易日范围")
    if any(item.effective_from > item.effective_to for item in components):
        raise ValueError("组合分段日期范围无效")
    if any(
        left.effective_to >= right.effective_from
        for left, right in zip(components, components[1:])
    ):
        raise ValueError("组合分段必须按日期严格递增且不得重叠")

    assigned = []
    for component in components:
        segment_dates = [
            day for day in dates
            if component.effective_from <= day <= component.effective_to
        ]
        if not segment_dates:
            raise ValueError(f"组合分段没有覆盖任何开市日: {component.dataset_id}")
        if segment_dates[0] != component.effective_from or segment_dates[-1] != component.effective_to:
            raise ValueError(
                f"分段边界必须是开市日: {component.dataset_id} "
                f"[{component.effective_from}, {component.effective_to}]"
            )
        assigned.extend(segment_dates)
    if tuple(assigned) != dates:
        raise ValueError("组合分段之间存在交易日缺口或重复归属")


def assert_resumable_component_prefix(
    existing: Sequence[Mapping[str, Any]],
    intended: Sequence[Mapping[str, Any]],
) -> None:
    """Allow retry only when persisted DRAFT rows are an exact intended prefix.

    Component insertion may be chunked by the write gateway.  A retry may fill
    missing suffix rows, but must never silently rewrite a prior component or
    leave stale extra rows from a different attempted composition.
    """
    fields = (
        "ordinal", "component_dataset_id", "effective_from", "effective_to",
        "component_input_manifest_hash", "component_manifest_hash",
        "component_baseline_tag", "component_baseline_quality_report_hash",
    )
    if len(existing) > len(intended):
        raise ValueError("现有 DRAFT 组件多于本次意图，拒绝覆盖旧组合身份")
    for index, actual in enumerate(existing):
        expected = intended[index]
        for field in fields:
            actual_value = actual.get(field)
            expected_value = expected.get(field)
            if field == "component_dataset_id":
                actual_value = str(actual_value)
                expected_value = str(expected_value)
            elif field.endswith("_hash"):
                actual_value = str(actual_value or "").strip()
                expected_value = str(expected_value or "").strip()
            if actual_value != expected_value:
                raise ValueError(
                    "现有 DRAFT 组件不是本次组合的精确前缀，拒绝改变重试身份: "
                    f"ordinal={index}, field={field}"
                )


def composition_hash(components: Sequence[DatasetComponent]) -> str:
    return stable_hash({
        "schema_version": COMPOSITION_SCHEMA_VERSION,
        "components": [component.canonical() for component in components],
    })
