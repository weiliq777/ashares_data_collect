"""Frozen Dataset adapter for Alpha research reads (Repository-only SQL boundary)."""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Iterator, Mapping, Sequence

from data_collect.alpha_research.contracts import (
    FACTOR_INPUT_FIELDS,
    LABEL_ACTION_FIELDS,
    LABEL_BAR_FIELDS,
    SUPPORTED_RESEARCH_SCHEMAS,
)
from data_collect.backtest.dataset_composition import (
    DatasetComponent,
    assert_compatible_manifests,
    composition_hash,
    frozen_manifest_hash,
    validate_component_coverage,
)
from data_collect.data_access.context import RunContext
from data_collect.data_access.repository.dataset_repository import DatasetRepository


def _as_date(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def _parse_json(value: Any) -> Any:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except (TypeError, ValueError, json.JSONDecodeError):
            return value
    return value


def _flatten_fields(value: Any) -> set[str]:
    parsed = _parse_json(value)
    if isinstance(parsed, Mapping):
        fields: set[str] = set()
        for child in parsed.values():
            fields.update(_flatten_fields(child))
        return fields
    if isinstance(parsed, (list, tuple, set)):
        return {str(item) for item in parsed if isinstance(item, (str, int))}
    return set()


@dataclass(frozen=True, slots=True)
class FrozenResearchIdentity:
    dataset_id: str
    dataset_kind: str
    baseline: str | None
    input_manifest_hash: str
    quality_report_hash: str | None
    frozen_start: date
    evaluation_start: date
    frozen_end: date
    selected_fields: frozenset[str]
    schema_versions: tuple[str, ...]
    snapshot_count: int
    research_dates: tuple[date, ...]
    calendar: tuple[date, ...]
    benchmark_code: str | None = None
    benchmark_hash: str | None = None

    def factor_availability(self, required_fields: Sequence[str]) -> tuple[bool, str | None]:
        missing = sorted(set(required_fields) - set(self.selected_fields))
        if missing:
            return False, "NOT_INCLUDED_IN_DATASET_CONTRACT:" + ",".join(missing)
        return True, None


class AlphaResearchRepository:
    """Validate a frozen research boundary and expose bounded reads only."""

    def __init__(
        self,
        repository: DatasetRepository,
        dataset_id: str,
        context: RunContext,
        *,
        page_size: int = 10_000,
    ) -> None:
        if not 1 <= page_size <= 50_000:
            raise ValueError("page_size must be between 1 and 50000")
        self._repository = repository
        self._dataset_id = dataset_id
        self._context = context
        self._page_size = page_size
        self.identity = self._validate_identity()

    def _validate_identity(self) -> FrozenResearchIdentity:
        manifest = self._repository.manifest(self._dataset_id, self._context)
        if str(manifest.get("status") or "").upper() != "FROZEN":
            raise ValueError("Alpha research requires a FROZEN Backtest Dataset")
        input_hash = str(manifest.get("input_manifest_hash") or "").strip()
        if not input_hash:
            raise ValueError("FROZEN Dataset has no input_manifest_hash")
        if not str(manifest.get("quality_report_hash") or "").strip():
            raise ValueError("FROZEN Dataset has no quality_report_hash")
        quality_report = _parse_json(manifest.get("quality_report"))
        if not isinstance(quality_report, Mapping) or str(quality_report.get("status") or "").upper() not in {"VALID", "PASS"}:
            raise ValueError("FROZEN Dataset quality report is not VALID")

        contracts = self._repository.research_snapshot_contracts(self._dataset_id, self._context)
        if not contracts:
            raise ValueError("FROZEN Dataset has no Research Snapshot members")
        schemas: set[str] = set()
        baselines: set[str] = set()
        for row in contracts:
            if str(row.get("snapshot_status") or "").upper() != "FROZEN":
                raise ValueError("Dataset references a Research Snapshot that is not FROZEN")
            schema = str(row.get("research_schema_version") or "")
            if schema not in SUPPORTED_RESEARCH_SCHEMAS:
                raise ValueError(f"unsupported frozen Research Snapshot schema: {schema!r}")
            schemas.add(schema)
            if row.get("research_baseline") is not None:
                baselines.add(str(row["research_baseline"]))
        if len(baselines) > 1:
            raise ValueError("Dataset mixes multiple Research baseline tags")
        if len(schemas) != 1:
            raise ValueError("Alpha research requires one Research Snapshot schema per Run")
        manifest_baseline = str(manifest.get("baseline") or "").strip() or None
        if baselines and manifest_baseline and baselines != {manifest_baseline}:
            raise ValueError("Dataset and Research Snapshot baseline tags do not match")

        selected_fields = _flatten_fields(manifest.get("selected_columns"))
        if not selected_fields:
            raise ValueError("FROZEN Dataset has no selected-column contract")
        missing_label_fields = sorted((LABEL_BAR_FIELDS | LABEL_ACTION_FIELDS) - selected_fields)
        if missing_label_fields:
            raise ValueError(
                "FROZEN Dataset does not declare all required label inputs: "
                + ", ".join(missing_label_fields)
            )
        calendar = tuple(_as_date(day) for day in self._repository.open_calendar(self._dataset_id, self._context))
        if not calendar or tuple(sorted(set(calendar))) != calendar:
            raise ValueError("FROZEN Dataset has no valid ascending open calendar")
        start = _as_date(manifest.get("warmup_start"))
        evaluation_start = _as_date(manifest.get("evaluation_start"))
        end = _as_date(manifest.get("evaluation_end"))
        if not start <= evaluation_start <= end or start < calendar[0] or end > calendar[-1]:
            raise ValueError("FROZEN Dataset bounds are inconsistent with its trading calendar")
        if str(manifest.get("dataset_kind") or "MATERIALIZED").upper() == "COMPOSITE":
            self._verify_composition(manifest, calendar)
        return FrozenResearchIdentity(
            dataset_id=self._dataset_id,
            dataset_kind=str(manifest.get("dataset_kind") or "MATERIALIZED"),
            baseline=manifest_baseline,
            input_manifest_hash=input_hash,
            quality_report_hash=str(manifest.get("quality_report_hash") or ""),
            benchmark_code=(str(manifest.get("benchmark_code") or "").strip() or None),
            benchmark_hash=(str(manifest.get("benchmark_hash") or "").strip() or None),
            frozen_start=start,
            evaluation_start=evaluation_start,
            frozen_end=end,
            selected_fields=frozenset(selected_fields),
            schema_versions=tuple(sorted(schemas)),
            snapshot_count=len(contracts),
            research_dates=tuple(sorted({_as_date(row["research_date"]) for row in contracts})),
            calendar=calendar,
        )

    def _verify_composition(
        self, manifest: Mapping[str, Any], root_calendar: Sequence[date],
    ) -> None:
        rows = self._repository.composition_components(self._dataset_id, self._context)
        if not rows or len(rows) > 128:
            raise ValueError("FROZEN COMPOSITE Dataset components are empty or exceed the limit")
        components: list[DatasetComponent] = []
        child_manifests: list[dict[str, Any]] = []
        for expected_ordinal, row in enumerate(rows):
            if int(row.get("ordinal", -1)) != expected_ordinal:
                raise ValueError("FROZEN COMPOSITE component ordinals are not contiguous")
            child = self._repository.manifest(str(row["component_dataset_id"]), self._context)
            if (
                str(child.get("status") or "").upper() != "FROZEN"
                or str(child.get("dataset_kind") or "MATERIALIZED").upper() != "MATERIALIZED"
            ):
                raise ValueError("FROZEN COMPOSITE components must be frozen materialized Datasets")
            proof = _parse_json(child.get("quality_report"))
            if (
                not isinstance(proof, Mapping)
                or str(proof.get("status") or "").upper() != "VALID"
                or proof.get("issues") != []
                or not str(child.get("quality_report_hash") or "").strip()
            ):
                raise ValueError("FROZEN COMPOSITE component lacks valid quality evidence")
            counts = proof.get("validation_counts")
            required_counts = (
                "missing_execution_rows", "missing_execution_lineage",
                "missing_limit_rows", "missing_calendar_lineage",
                "future_dividend_rows", "invalid_dividend_dates",
                "missing_dividend_lineage", "inconsistent_stock_rates",
            )
            if isinstance(counts, Mapping):
                if any(counts.get(name) != 0 for name in required_counts):
                    raise ValueError("FROZEN COMPOSITE component has invalid quality counts")
            elif counts is not None:
                raise ValueError("FROZEN COMPOSITE component quality counts are malformed")
            elif any(int(proof.get(name) or 0) != 0 for name in (
                "missing_execution_rows", "missing_limit_rows",
            )):
                raise ValueError("FROZEN COMPOSITE component lacks acceptable legacy quality counts")
            child_input_hash = str(child.get("input_manifest_hash") or "").strip()
            child_baseline = str(child.get("baseline") or "").strip()
            child_quality_hash = str(child.get("baseline_quality_report_hash") or "").strip()
            if not child_input_hash or not child_baseline or not child_quality_hash:
                raise ValueError("FROZEN COMPOSITE component lacks baseline/input identity")
            effective_from = _as_date(row["effective_from"])
            effective_to = _as_date(row["effective_to"])
            if (
                str(row.get("component_input_manifest_hash") or "").strip() != child_input_hash
                or str(row.get("component_manifest_hash") or "").strip() != frozen_manifest_hash(child)
                or str(row.get("component_baseline_tag") or "") != child_baseline
                or str(row.get("component_baseline_quality_report_hash") or "").strip() != child_quality_hash
            ):
                raise ValueError("FROZEN COMPOSITE component identity/hash mismatch")
            if not child["warmup_start"] <= effective_from <= effective_to <= child["evaluation_end"]:
                raise ValueError("FROZEN COMPOSITE component span escapes child Dataset bounds")
            components.append(DatasetComponent(
                dataset_id=str(child["dataset_id"]),
                effective_from=effective_from,
                effective_to=effective_to,
                input_manifest_hash=child_input_hash,
                manifest_hash=frozen_manifest_hash(child),
                baseline_tag=child_baseline,
                baseline_quality_report_hash=child_quality_hash,
            ))
            child_manifests.append(child)
        assert_compatible_manifests(child_manifests)
        if composition_hash(components) != str(manifest.get("input_manifest_hash") or "").strip():
            raise ValueError("FROZEN COMPOSITE Dataset composition hash mismatch")
        if (
            components[0].effective_from != manifest.get("warmup_start")
            or components[-1].effective_to != manifest.get("evaluation_end")
            or str(manifest.get("baseline") or "") != components[-1].baseline_tag
        ):
            raise ValueError("FROZEN COMPOSITE Dataset root boundaries/provenance mismatch")
        validate_component_coverage(components, root_calendar)
        for component in components:
            child_calendar = tuple(
                _as_date(day) for day in self._repository.open_calendar(
                    component.dataset_id, self._context,
                )
            )
            expected_segment = tuple(
                day for day in root_calendar
                if component.effective_from <= day <= component.effective_to
            )
            observed_segment = tuple(
                day for day in child_calendar
                if component.effective_from <= day <= component.effective_to
            )
            if observed_segment != expected_segment:
                raise ValueError("FROZEN COMPOSITE component calendar is inconsistent")

    def available_factor_fields(self) -> frozenset[str]:
        return frozenset(FACTOR_INPUT_FIELDS & self.identity.selected_fields)

    def observation_pages(
        self,
        date_from: date,
        date_to: date,
        columns: Sequence[str],
    ) -> Iterator[list[dict[str, Any]]]:
        if date_from > date_to:
            raise ValueError("observation range start must not exceed end")
        if date_from < self.identity.frozen_start or date_to > self.identity.frozen_end:
            raise ValueError("observation range is outside FROZEN Dataset bounds")
        unavailable = sorted(set(columns) - self.identity.selected_fields)
        if unavailable:
            raise ValueError(
                "requested observation fields are outside Dataset selected_columns: "
                + ", ".join(unavailable)
            )
        last_key = None
        while True:
            page = self._repository.observation_page(
                self._dataset_id,
                columns,
                self._context,
                last_key=last_key,
                limit=self._page_size,
                date_from=date_from,
                date_to=date_to,
            )
            if not page:
                return
            key = (page[-1]["research_date"], page[-1]["ts_code"])
            if last_key is not None and key <= last_key:
                raise RuntimeError("observation pagination did not advance")
            yield page
            if len(page) < self._page_size:
                return
            last_key = key

    def bars(
        self,
        date_from: date,
        date_to: date,
    ) -> dict[date, dict[str, dict[str, Any]]]:
        if date_from > date_to:
            raise ValueError("bar range start must not exceed end")
        if date_from < self.identity.frozen_start or date_to > self.identity.frozen_end:
            raise ValueError("bar range is outside FROZEN Dataset bounds")
        result: dict[date, dict[str, dict[str, Any]]] = {}
        last_key = None
        while True:
            page = self._repository.bar_page(
                self._dataset_id,
                self._context,
                last_key=last_key,
                limit=self._page_size,
                date_from=date_from,
                date_to=date_to,
            )
            if not page:
                break
            for row in page:
                day = _as_date(row["trade_date"])
                code = str(row["ts_code"])
                if code in result.setdefault(day, {}):
                    raise RuntimeError(f"duplicate frozen execution bar: {day}/{code}")
                result[day][code] = row
            key = (page[-1]["trade_date"], page[-1]["ts_code"])
            if last_key is not None and key <= last_key:
                raise RuntimeError("execution pagination did not advance")
            if len(page) < self._page_size:
                break
            last_key = key
        return result

    def actions(
        self,
        date_from: date,
        date_to: date,
    ) -> dict[date, dict[str, list[dict[str, Any]]]]:
        if date_from > date_to:
            raise ValueError("corporate-action range start must not exceed end")
        if date_from < self.identity.frozen_start or date_to > self.identity.frozen_end:
            raise ValueError("corporate-action range is outside FROZEN Dataset bounds")
        result: dict[date, dict[str, list[dict[str, Any]]]] = {}
        last_key = None
        while True:
            page = self._repository.dividend_page(
                self._dataset_id,
                self._context,
                last_key=last_key,
                limit=self._page_size,
                date_from=date_from,
                date_to=date_to,
            )
            if not page:
                break
            for row in page:
                ex_date = _as_date(row["ex_date"])
                result.setdefault(ex_date, {}).setdefault(str(row["ts_code"]), []).append(row)
            row = page[-1]
            key = (
                row["ex_date"], row["ts_code"], row.get("source_record_key") or "",
                int(row.get("source_record_version") or 0),
            )
            if last_key is not None and key <= last_key:
                raise RuntimeError("corporate-action pagination did not advance")
            if len(page) < self._page_size:
                break
            last_key = key
        return result

    def benchmark_closes(self) -> dict[date, float]:
        """Read the frozen benchmark series used only for causal market regimes."""
        code = self.identity.benchmark_code
        if not code or not self.identity.benchmark_hash:
            return {}
        rows = self._repository.load_benchmark_closes(
            self._dataset_id, code, self._context,
        )
        return {_as_date(day): float(close) for day, close in rows.items()}


__all__ = ["AlphaResearchRepository", "FrozenResearchIdentity"]
