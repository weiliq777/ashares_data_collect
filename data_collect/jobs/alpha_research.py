"""Read-only offline Alpha factor research over a FROZEN Backtest Dataset."""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from dataclasses import asdict
from datetime import date, datetime
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping, Sequence

from data_collect.alpha_research.contracts import (
    FACTORS,
    MARKET_STAGE_VERSION,
    AlphaResearchSpec,
    ResearchRunIdentity,
)
from data_collect.alpha_research.evaluation import (
    classify_market_stages,
    evaluate_cross_section,
    summarize_evaluations,
)
from data_collect.alpha_research.factors import factor_values
from data_collect.alpha_research.labels import build_forward_labels
from data_collect.alpha_research.report import ResearchArtifactWriter
from data_collect.data_access import DataAccessKernel
from data_collect.data_access.policy_catalog import build_default_registry
from data_collect.data_access.repository.alpha_research_repository import AlphaResearchRepository
from data_collect.data_access.repository.dataset_repository import DatasetRepository
from data_collect.data_access.run_context import build_run_context


def _month_groups(days: Sequence[date]) -> list[tuple[date, ...]]:
    groups: list[list[date]] = []
    current_key: tuple[int, int] | None = None
    for day in days:
        key = (day.year, day.month)
        if key != current_key:
            groups.append([])
            current_key = key
        groups[-1].append(day)
    return [tuple(group) for group in groups]


def _as_date(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def _record_delist_evidence(
    delist_dates: dict[str, date], observation: Mapping[str, Any],
) -> bool:
    raw_delist_date = observation.get("delist_date")
    if raw_delist_date is None:
        return False
    observed_date = _as_date(observation["research_date"])
    delist_date = _as_date(raw_delist_date)
    if delist_date > observed_date:
        raise ValueError("FROZEN PIT delist_date is later than its observation date")
    ts_code = str(observation["ts_code"])
    previous = delist_dates.get(ts_code)
    if previous is not None and previous != delist_date:
        raise RuntimeError(f"inconsistent FROZEN delist_date for {ts_code}")
    delist_dates[ts_code] = delist_date
    return True


def _research_code_hash() -> str:
    project_root = Path(__file__).resolve().parents[2]
    paths = [
        *sorted((project_root / "data_collect" / "alpha_research").glob("*.py")),
        project_root / "data_collect" / "data_access" / "repository" / "alpha_research_repository.py",
        project_root / "data_collect" / "data_access" / "repository" / "dataset_repository.py",
        project_root / "data_collect" / "data_access" / "research_contract.py",
        project_root / "data_collect" / "backtest" / "dataset_composition.py",
        project_root / "data_collect" / "data_access" / "repository" / "feature_repository.py",
        project_root / "data_collect" / "observation" / "hashing.py",
        Path(__file__).resolve(),
    ]
    digest = hashlib.sha256()
    for path in sorted(set(paths), key=lambda item: item.as_posix()):
        digest.update(path.relative_to(project_root).as_posix().encode("utf-8"))
        digest.update(b"\0")
        digest.update(path.read_text(encoding="utf-8").replace("\r\n", "\n").encode("utf-8"))
        digest.update(b"\n")
    return digest.hexdigest()


def _sample_row(record: Mapping[str, Any], horizon: int) -> dict[str, Any]:
    label = record["labels"][horizon]
    factors = record["factor_values"]
    return {
        "signal_date": record["research_date"],
        "ts_code": record["ts_code"],
        "horizon": horizon,
        "market_stage": record["market_stage"],
        "benchmark_return_60d": record["benchmark_return_60d"],
        "factors": {
            name: {
                "value": value.value,
                "status": value.status,
                "reason": value.reason,
            }
            for name, value in sorted(factors.items())
        },
        "label": label.as_dict(),
    }


def _previous_session_total_mv(
    signal_date: date,
    ts_code: str,
    calendar_indices: Mapping[date, int],
    calendar: Sequence[date],
    total_mv_by_date_code: Mapping[tuple[date, str], Any],
) -> Any:
    signal_index = calendar_indices.get(signal_date)
    if signal_index is None or signal_index == 0:
        return None
    previous_date = calendar[signal_index - 1]
    return total_mv_by_date_code.get((previous_date, ts_code))


def _factor_availability(adapter: AlphaResearchRepository) -> tuple[list[dict[str, Any]], list[str]]:
    selected = adapter.identity.selected_fields
    missing_keys = sorted({"research_date", "ts_code"} - set(selected))
    if missing_keys:
        raise ValueError(
            "FROZEN Dataset omits required observation keys: " + ", ".join(missing_keys)
        )
    available_specs = []
    availability = []
    for factor in FACTORS:
        available, reason = adapter.identity.factor_availability(factor.input_fields)
        availability.append({
            "factor": factor.name,
            "category": factor.category,
            "input_fields": list(factor.input_fields),
            "available": available,
            "reason": reason,
        })
        if available:
            available_specs.append(factor)
    read_fields = sorted({
        field
        for factor in available_specs
        for field in factor.input_fields
    } | {"research_date", "ts_code"}
      | ({"total_mv"} if "total_mv" in selected else set())
      | ({"delist_date"} if "delist_date" in selected else set()))
    return availability, read_fields


def run_research(
    dataset_id: str,
    signal_start: date,
    signal_end: date,
    output_dir: str | Path,
    *,
    horizons: Sequence[int] = (20, 40, 60),
    page_size: int = 10_000,
    bootstrap_repetitions: int = 1_000,
    min_bootstrap_dates: int = 60,
    seed: int = 20261016,
) -> dict[str, Any]:
    """Run the frozen-data factor study and return local artifact paths."""
    spec = AlphaResearchSpec(
        dataset_id=dataset_id,
        signal_start=signal_start,
        signal_end=signal_end,
        horizons=tuple(horizons),
        page_size=page_size,
        bootstrap_repetitions=bootstrap_repetitions,
        min_bootstrap_dates=min_bootstrap_dates,
        seed=seed,
    )
    kernel = DataAccessKernel(policy_registry=build_default_registry())
    context = build_run_context("alpha_research", pipeline_version="alpha-research-v2")
    writer: ResearchArtifactWriter | None = None
    try:
        adapter = AlphaResearchRepository(
            DatasetRepository(kernel), dataset_id, context, page_size=page_size,
        )
        identity = adapter.identity
        if signal_start < identity.evaluation_start or signal_end > identity.frozen_end:
            raise ValueError(
                "signal interval must be within Dataset evaluation_start/evaluation_end"
            )
        signal_days = tuple(
            day for day in identity.calendar
            if signal_start <= day <= signal_end
            and identity.evaluation_start <= day <= identity.frozen_end
        )
        if not signal_days:
            raise ValueError("signal interval contains no frozen open trading dates")
        benchmark_closes = adapter.benchmark_closes()
        market_stages = classify_market_stages(
            signal_days,
            calendar=identity.calendar,
            benchmark_closes=benchmark_closes,
        )
        factor_availability, observation_fields = _factor_availability(adapter)
        run_identity = ResearchRunIdentity.build(
            dataset_id=dataset_id,
            input_hash=identity.input_manifest_hash,
            spec=spec,
            selected_fields=tuple(identity.selected_fields),
            schema_versions=identity.schema_versions,
            code_hash=_research_code_hash(),
        )
        writer = ResearchArtifactWriter(output_dir, run_identity.run_id)

        daily_evaluations: list[dict[str, Any]] = []
        unique_stocks_by_factor: dict[tuple[str, int], set[str]] = defaultdict(set)
        label_status_counts: Counter[str] = Counter()
        label_status_by_horizon: dict[str, Counter[str]] = defaultdict(Counter)
        label_reason_counts: Counter[str] = Counter()
        label_reason_by_horizon: dict[str, Counter[str]] = defaultdict(Counter)
        factor_status_counts: dict[str, Counter[str]] = defaultdict(Counter)
        factor_reason_counts: dict[str, Counter[str]] = defaultdict(Counter)
        known_delisted_codes: set[str] = set()
        signal_observation_count = 0
        signal_dates_seen: set[date] = set()
        loaded_bar_count = 0
        loaded_action_count = 0
        data_quality_by_date: dict[str, Any] = {}
        calendar_indices = {day: index for index, day in enumerate(identity.calendar)}
        for block_days in _month_groups(signal_days):
            block_start, block_end = block_days[0], block_days[-1]
            last_signal_index = calendar_indices[block_end]
            target_index = min(last_signal_index + max(spec.horizons), len(identity.calendar) - 1)
            target_end = identity.calendar[target_index]

            observations: list[dict[str, Any]] = []
            seen: set[tuple[date, str]] = set()
            for page in adapter.observation_pages(block_start, block_end, observation_fields):
                for row in page:
                    research_date = row["research_date"]
                    research_date = research_date.date() if hasattr(research_date, "date") else research_date
                    code = str(row["ts_code"])
                    key = (research_date, code)
                    if key in seen:
                        raise RuntimeError(f"duplicate FROZEN observation: {research_date}/{code}")
                    seen.add(key)
                    normalized = dict(row)
                    normalized["research_date"] = research_date
                    normalized["ts_code"] = code
                    observations.append(normalized)
                    signal_dates_seen.add(research_date)
            observations.sort(key=lambda row: (row["research_date"], row["ts_code"]))
            signal_observation_count += len(observations)

            total_mv_by_date_code = {
                (row["research_date"], row["ts_code"]): row.get("total_mv")
                for row in observations
            }
            if "total_mv" in identity.selected_fields:
                first_signal_index = calendar_indices[block_start]
                if first_signal_index > 0:
                    previous_date = identity.calendar[first_signal_index - 1]
                    for page in adapter.observation_pages(
                        previous_date, previous_date,
                        ("research_date", "ts_code", "total_mv"),
                    ):
                        for row in page:
                            prior_day = row["research_date"]
                            prior_day = prior_day.date() if hasattr(prior_day, "date") else prior_day
                            total_mv_by_date_code[(prior_day, str(row["ts_code"]))] = row.get("total_mv")

            delist_dates: dict[str, date] = {}
            delist_evidence_row_count = 0
            delist_evidence_available = "delist_date" in identity.selected_fields
            if delist_evidence_available:
                for observation in observations:
                    if _record_delist_evidence(delist_dates, observation):
                        delist_evidence_row_count += 1
                future_start_index = last_signal_index + 1
                if future_start_index <= target_index:
                    future_start = identity.calendar[future_start_index]
                    for page in adapter.observation_pages(
                        future_start, target_end,
                        ("research_date", "ts_code", "delist_date"),
                    ):
                        for outcome_observation in page:
                            if _record_delist_evidence(delist_dates, outcome_observation):
                                delist_evidence_row_count += 1
                known_delisted_codes.update(delist_dates)

            bars = adapter.bars(block_start, target_end)
            loaded_bar_count += sum(len(rows) for rows in bars.values())
            action_start_index = calendar_indices[block_start] + 1
            if action_start_index <= target_index:
                actions = adapter.actions(identity.calendar[action_start_index], target_end)
            else:
                actions = {}
            loaded_action_count += sum(
                len(events)
                for by_code in actions.values()
                for events in by_code.values()
            )
            labels = build_forward_labels(
                observations,
                calendar=identity.calendar,
                bars=bars,
                actions=actions,
                delist_dates=delist_dates,
                horizons=spec.horizons,
            )
            for label in labels:
                if label.reason:
                    label_reason_counts[label.reason] += 1
                    label_reason_by_horizon[str(label.horizon)][label.reason] += 1
            label_by_key = {
                (label.signal_date, label.ts_code, label.horizon): label
                for label in labels
            }
            records_by_date_horizon: dict[tuple[date, int], list[dict[str, Any]]] = defaultdict(list)
            for observation in observations:
                research_date = observation["research_date"]
                code = observation["ts_code"]
                factors = factor_values(observation)
                for factor_name, value in factors.items():
                    factor_status_counts[factor_name][value.status] += 1
                    if value.reason:
                        factor_reason_counts[factor_name][value.reason] += 1
                record: dict[str, Any] = {
                    "research_date": research_date,
                    "ts_code": code,
                    "factor_values": factors,
                    "market_stage": market_stages.get(research_date, {}).get("stage", "UNKNOWN"),
                    "benchmark_return_60d": market_stages.get(research_date, {}).get("benchmark_return"),
                    "labels": {},
                }
                for horizon in spec.horizons:
                    label = label_by_key[(research_date, code, horizon)]
                    record["labels"][horizon] = label
                    label_status_counts[label.status] += 1
                    label_status_by_horizon[str(horizon)][label.status] += 1
                    record_for_eval = {
                        "signal_date": research_date,
                        "ts_code": code,
                        "factor_values": factors,
                        "label_status": label.status,
                        "label_reason": label.reason,
                        "prediction_total_return": label.prediction_total_return,
                        "relative_total_return": label.relative_total_return,
                        "total_mv": _previous_session_total_mv(
                            research_date, code, calendar_indices, identity.calendar,
                            total_mv_by_date_code,
                        ),
                        "market_stage": record["market_stage"],
                    }
                    records_by_date_horizon[(research_date, horizon)].append(record_for_eval)
                    writer.write_sample(_sample_row(record, horizon))
            for (research_date, horizon), records in sorted(records_by_date_horizon.items()):
                for record in records:
                    if record["label_status"] != "COMPLETE":
                        continue
                    for factor in FACTORS:
                        value = record["factor_values"].get(factor.name)
                        if getattr(value, "status", None) == "VALID":
                            unique_stocks_by_factor[(factor.name, horizon)].add(
                                str(record["ts_code"])
                            )
                daily_rows = evaluate_cross_section(
                    research_date,
                    horizon,
                    records,
                    factor_specs=FACTORS,
                )
                if "total_mv" not in identity.selected_fields:
                    for row in daily_rows:
                        row["size_control_status"] = "NOT AVAILABLE_NOT_IN_DATASET_CONTRACT"
                daily_evaluations.extend(daily_rows)
            data_quality_by_date[f"{block_start.isoformat()}..{block_end.isoformat()}"] = {
                "signal_observation_count": len(observations),
                "execution_bar_count_including_forward_buffer": sum(len(rows) for rows in bars.values()),
                "corporate_action_count_including_forward_buffer": sum(
                    len(events) for by_code in actions.values() for events in by_code.values()
                ),
                "delist_evidence_available": delist_evidence_available,
                "delist_evidence_row_count": delist_evidence_row_count,
                "delisted_security_count": len(delist_dates),
                "forward_data_end": target_end.isoformat(),
            }

        evaluation = summarize_evaluations(
            daily_evaluations,
            horizons=spec.horizons,
            bootstrap_repetitions=spec.bootstrap_repetitions,
            min_bootstrap_dates=spec.min_bootstrap_dates,
            seed=spec.seed,
            expected_signal_dates=signal_days,
            unique_stocks_by_factor=unique_stocks_by_factor,
            market_stages={day: item["stage"] for day, item in market_stages.items()},
        )
        expected_signal_days = {
            day for day in identity.calendar
            if signal_start <= day <= signal_end
            and identity.evaluation_start <= day <= identity.frozen_end
        }
        frozen_snapshot_days = set(identity.research_dates)
        missing_snapshot_days = sorted(expected_signal_days - frozen_snapshot_days)
        missing_observation_days = sorted(expected_signal_days - signal_dates_seen)
        unresolved_delist_count = (
            label_reason_counts["ALREADY_DELISTED_AT_SIGNAL"]
            + label_reason_counts["DELISTED_WITHIN_HORIZON_NO_TERMINAL_VALUE"]
        )
        incomplete_label_count = label_status_counts["INCOMPLETE"]
        immature_label_count = label_status_counts["IMMATURE"]
        factor_quality = {}
        for factor in FACTORS:
            statuses = factor_status_counts[factor.name]
            factor_quality[factor.name] = {
                "observations": signal_observation_count,
                "status_counts": dict(sorted(statuses.items())),
                "reason_counts": dict(sorted(factor_reason_counts[factor.name].items())),
                "valid_count": int(statuses.get("VALID", 0)),
                "missing_ratio": (
                    1.0 - statuses.get("VALID", 0) / signal_observation_count
                    if signal_observation_count else None
                ),
            }
        if unresolved_delist_count:
            outcome_completeness_status = "BLOCKED_UNRESOLVED_DELISTING_TERMINAL_VALUE"
        elif incomplete_label_count or immature_label_count:
            outcome_completeness_status = "INCOMPLETE_LABELS_EXCLUDED_FROM_STATISTICS"
        else:
            outcome_completeness_status = "COMPLETE_LABELS"
        payload = {
            "status": "EXPLORATORY_ONLY_STAGE1_GATES_UNRESOLVED",
            "research_validity_status": "NOT_FORMAL_FACTOR_EFFICACY_ASSESSMENT",
            "identity": asdict(run_identity),
            "dataset": {
                "dataset_id": identity.dataset_id,
                "dataset_kind": identity.dataset_kind,
                "baseline": identity.baseline,
                "input_manifest_hash": identity.input_manifest_hash,
                "quality_report_hash": identity.quality_report_hash,
                "benchmark_code": identity.benchmark_code,
                "benchmark_hash": identity.benchmark_hash,
                "evaluation_start": identity.evaluation_start.isoformat(),
                "evaluation_end": identity.frozen_end.isoformat(),
                "schema_versions": list(identity.schema_versions),
                "snapshot_count": identity.snapshot_count,
                "selected_fields": sorted(identity.selected_fields),
            },
            "parameters": {
                **spec.canonical(),
                "factor_set_version": run_identity.factor_version,
                "label_version": run_identity.label_version,
                "evaluation_version": run_identity.evaluation_version,
                "market_stage_version": MARKET_STAGE_VERSION,
            },
            "factor_availability": factor_availability,
            "quality": {
                "available_factor_count": sum(bool(item["available"]) for item in factor_availability),
                "total_factor_count": len(factor_availability),
                "signal_observation_count": signal_observation_count,
                "signal_snapshot_days_seen": len(signal_dates_seen),
                "expected_open_signal_days": len(expected_signal_days),
                "missing_snapshot_days": [day.isoformat() for day in missing_snapshot_days],
                "missing_snapshot_day_count": len(missing_snapshot_days),
                "missing_observation_days": [day.isoformat() for day in missing_observation_days],
                "missing_observation_day_count": len(missing_observation_days),
                "loaded_execution_bar_count": loaded_bar_count,
                "loaded_corporate_action_count": loaded_action_count,
                "benchmark_close_count": len(benchmark_closes),
                "market_stage_counts": dict(sorted(Counter(
                    item["stage"] for item in market_stages.values()
                ).items())),
                "market_stage_status": (
                    "AVAILABLE" if any(item["stage"] != "UNKNOWN" for item in market_stages.values())
                    else "NOT AVAILABLE_BENCHMARK_COVERAGE"
                ),
                "label_status_counts": dict(sorted(label_status_counts.items())),
                "label_status_by_horizon": {
                    horizon: dict(sorted(counts.items()))
                    for horizon, counts in sorted(label_status_by_horizon.items())
                },
                "label_reason_counts": dict(sorted(label_reason_counts.items())),
                "label_reason_by_horizon": {
                    horizon: dict(sorted(counts.items()))
                    for horizon, counts in sorted(label_reason_by_horizon.items())
                },
                "factor_statistics_population": "COMPLETE_LABELS_ONLY",
                "factor_quality": factor_quality,
                "size_control_input": (
                    "PREVIOUS_SESSION_TOTAL_MV"
                    if "total_mv" in identity.selected_fields
                    else "NOT_AVAILABLE_NOT_IN_DATASET_CONTRACT"
                ),
                "outcome_completeness_status": outcome_completeness_status,
                "unresolved_delist_label_count": unresolved_delist_count,
                "unclassified_missing_path_label_count": label_reason_counts["MISSING_PATH_BAR"],
                "delist_evidence_status": (
                    "FROZEN_PIT_DELIST_DATE_AVAILABLE" if "delist_date" in identity.selected_fields
                    else "NOT AVAILABLE_NOT_IN_DATASET_CONTRACT"
                ),
                "known_delisted_security_count": len(known_delisted_codes),
                "monthly_read_evidence": data_quality_by_date,
            },
            "evaluation": evaluation,
            "strategy_incrementality": {
                "status": "NOT RUN_STAGE1_EVIDENCE_UNRESOLVED",
                "required_gates": [
                    "逐证券未来行情覆盖审计通过",
                    "回撤口径一致性问题解决并独立复核",
                    "四组策略×资金场景均有可复现执行证据",
                ],
                "execution_engine": "现有 BacktestEngine；当前 Alpha Run 不创建或写入策略回测",
            },
        }
        if "total_mv" not in identity.selected_fields:
            evaluation["method_notes"]["size_control"] = (
                "NOT AVAILABLE because total_mv is outside Dataset selected_columns."
            )
        else:
            evaluation["method_notes"]["size_control"] = (
                "Uses previous-session total_mv because same-day daily_basic values are available after the close."
            )
        paths = writer.finish(payload)
        return {"run_id": run_identity.run_id, **paths}
    except Exception:
        if writer is not None:
            writer.abort()
        raise
    finally:
        kernel.close()


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="FROZEN Dataset Alpha 因子研究（只读）")
    parser.add_argument("--dataset-id", required=True)
    parser.add_argument("--signal-start", required=True, type=date.fromisoformat)
    parser.add_argument("--signal-end", required=True, type=date.fromisoformat)
    parser.add_argument("--output-dir", default="data/alpha_research")
    parser.add_argument("--horizon", type=int, action="append", choices=(20, 40, 60))
    parser.add_argument("--page-size", type=int, default=10_000)
    parser.add_argument("--bootstrap-repetitions", type=int, default=1_000)
    parser.add_argument("--min-bootstrap-dates", type=int, default=60)
    parser.add_argument("--seed", type=int, default=20261016)
    args = parser.parse_args(argv)
    result = run_research(
        args.dataset_id,
        args.signal_start,
        args.signal_end,
        Path(args.output_dir),
        horizons=args.horizon or (20, 40, 60),
        page_size=args.page_size,
        bootstrap_repetitions=args.bootstrap_repetitions,
        min_bootstrap_dates=args.min_bootstrap_dates,
        seed=args.seed,
    )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
