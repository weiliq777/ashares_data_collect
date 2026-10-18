"""Recoverable evaluation of a verified factor-label sample artifact."""
from __future__ import annotations

from collections import Counter, defaultdict
from datetime import date
import hashlib
import json
import math
import os
from pathlib import Path
import tempfile
from time import perf_counter
from typing import Any, Mapping, Sequence

from data_collect.alpha_research.contracts import EVALUATION_VERSION, FACTORS
from data_collect.alpha_research.evaluation import (
    evaluate_cross_section,
    evaluate_delist_sensitivity,
    summarize_evaluations,
)
from data_collect.alpha_research.factors import FactorValue
from data_collect.alpha_research.report import write_evaluation_report


ARTIFACT_VERSION = "daily-factor-evaluation-v1"
DEFAULT_DELIST_RETURN_ASSUMPTIONS = (-1.0, -0.5, 0.0)


def recover_sample_manifest(
    sample_path: str | Path,
    *,
    expected_sha256: str,
    expected_rows: int,
    expected_rows_by_horizon: Mapping[str, int],
    identity: Mapping[str, Any],
    dataset: Mapping[str, Any],
    parameters: Mapping[str, Any],
    factor_availability: Sequence[Mapping[str, Any]],
    report_context: Mapping[str, Any],
    expected_label_coverage: Mapping[str, Any] | None = None,
    recovered_from: str | None = None,
) -> dict[str, Any]:
    """Validate a complete interrupted JSONL and publish its sample manifest."""
    path = Path(sample_path).resolve()
    if not path.is_file():
        raise FileNotFoundError(path)
    if path.name != "samples.jsonl":
        raise ValueError("recovered sample must first be promoted to samples.jsonl")
    digest = hashlib.sha256()
    rows_by_horizon: Counter[str] = Counter()
    statuses_by_horizon: dict[str, Counter[str]] = defaultdict(Counter)
    reasons_by_horizon: dict[str, Counter[str]] = defaultdict(Counter)
    dates: list[str] = []
    previous_sort_key: tuple[str, str, int] | None = None
    current_day: str | None = None
    daily_keys: set[tuple[str, int]] = set()
    final_newline = False
    total_rows = 0
    with path.open("rb") as stream:
        for line_number, raw_line in enumerate(stream, 1):
            digest.update(raw_line)
            final_newline = raw_line.endswith(b"\n")
            try:
                row = json.loads(raw_line)
            except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                raise ValueError(f"invalid JSON at sample line {line_number}") from exc
            day = str(row.get("signal_date") or "")
            code = str(row.get("ts_code") or "")
            horizon = int(row.get("horizon", -1))
            label = row.get("label")
            if not day or not code or not isinstance(label, Mapping):
                raise ValueError(f"sample identity is incomplete at line {line_number}")
            date.fromisoformat(day)
            if (
                label.get("signal_date") != day
                or label.get("ts_code") != code
                or int(label.get("horizon", -2)) != horizon
            ):
                raise ValueError(f"sample label identity mismatch at line {line_number}")
            sort_key = (day, code, horizon)
            if previous_sort_key is not None and sort_key < previous_sort_key:
                raise ValueError("sample rows are not ordered by date, security, and horizon")
            previous_sort_key = sort_key
            if current_day != day:
                if current_day is not None and day <= current_day:
                    raise ValueError("sample signal dates are not strictly ascending")
                current_day = day
                dates.append(day)
                daily_keys.clear()
            daily_key = (code, horizon)
            if daily_key in daily_keys:
                raise ValueError(f"duplicate sample row at {day}/{code}/{horizon}")
            daily_keys.add(daily_key)
            horizon_key = str(horizon)
            rows_by_horizon[horizon_key] += 1
            status = str(label.get("status") or "UNKNOWN")
            statuses_by_horizon[horizon_key][status] += 1
            if label.get("reason"):
                reasons_by_horizon[horizon_key][str(label["reason"])] += 1
            if status == "COMPLETE":
                value = label.get("prediction_total_return")
                if value is None or not math.isfinite(float(value)):
                    raise ValueError(f"complete label has no finite return at line {line_number}")
            total_rows += 1
    actual_sha256 = digest.hexdigest()
    expected_rows_by_horizon = {str(k): int(v) for k, v in expected_rows_by_horizon.items()}
    if actual_sha256 != expected_sha256:
        raise ValueError("recovered sample SHA-256 does not match the recorded coverage audit")
    if total_rows != expected_rows or dict(rows_by_horizon) != expected_rows_by_horizon:
        raise ValueError("recovered sample row counts do not match the coverage audit")
    if not dates or dates != sorted(set(dates)):
        raise ValueError("recovered sample signal dates are empty or duplicated")
    status_output = {
        horizon: dict(sorted(counter.items()))
        for horizon, counter in sorted(statuses_by_horizon.items())
    }
    reason_output = {
        horizon: dict(sorted(counter.items()))
        for horizon, counter in sorted(reasons_by_horizon.items())
    }
    if expected_label_coverage is not None:
        for horizon, expected in expected_label_coverage.get("by_horizon", {}).items():
            actual_status = status_output.get(str(horizon), {})
            actual_reasons = reason_output.get(str(horizon), {})
            if actual_status.get("COMPLETE", 0) != int(expected.get("complete", -1)):
                raise ValueError(f"complete label count mismatch for horizon {horizon}")
            if actual_status.get("INCOMPLETE", 0) != int(expected.get("incomplete", -1)):
                raise ValueError(f"incomplete label count mismatch for horizon {horizon}")
            if actual_reasons != expected.get("incomplete_reason_counts", {}):
                raise ValueError(f"incomplete label reason counts mismatch for horizon {horizon}")
    manifest = {
        "artifact_version": "factor-label-sample-v1",
        "identity": dict(identity),
        "dataset": dict(dataset),
        "parameters": dict(parameters),
        "factor_availability": list(factor_availability),
        "report_context": dict(report_context),
        "signal_dates": dates,
        "sample": {
            "path": path.name,
            "sha256": actual_sha256,
            "size_bytes": path.stat().st_size,
            "row_count": total_rows,
            "rows_by_horizon": dict(sorted(rows_by_horizon.items())),
            "label_status_counts_by_horizon": status_output,
            "label_reason_counts_by_horizon": reason_output,
            "final_newline": final_newline,
            "recovered_from": recovered_from,
        },
    }
    manifest_path = path.parent / "sample_manifest.json"
    _atomic_json(manifest_path, manifest)
    return {"manifest_path": str(manifest_path.resolve()), "manifest": manifest}


def _json_bytes(value: Any) -> bytes:
    return (json.dumps(
        value, ensure_ascii=False, sort_keys=True, indent=2,
        separators=(",", ": "), allow_nan=False,
    ) + "\n").encode("utf-8")


def _atomic_json(path: Path, value: Any) -> bytes:
    data = _json_bytes(value)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_name: str | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb", dir=path.parent, prefix=f".{path.name}.",
            suffix=".tmp", delete=False,
        ) as stream:
            temporary_name = stream.name
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary_name, path)
    finally:
        if temporary_name and os.path.exists(temporary_name):
            os.unlink(temporary_name)
    return data


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _relative_pool_references(
    run_dir: Path,
    sample_path: Path,
    sample_manifest: Mapping[str, Any],
) -> tuple[dict[tuple[date, int], float | None], dict[str, Any], float]:
    """Load or validate date/horizon equal-weight return baselines from labels."""
    started = perf_counter()
    sample = sample_manifest["sample"]
    sample_sha256 = str(sample["sha256"])
    reference_path = run_dir / "relative_pool_reference.json"
    if reference_path.is_file():
        try:
            cached = json.loads(reference_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            cached = {}
        references = cached.get("references", [])
        references_hash = hashlib.sha256(_json_bytes(references)).hexdigest()
        if (
            cached.get("artifact_version") == "relative-pool-reference-v2"
            and cached.get("sample_sha256") == sample_sha256
            and cached.get("references_sha256") == references_hash
        ):
            values = {
                (date.fromisoformat(item["signal_date"]), int(item["horizon"])): item.get("mean_return")
                for item in references
            }
            expected_keys = {
                (date.fromisoformat(day), int(horizon))
                for day in sample_manifest["signal_dates"]
                for horizon in sample_manifest["parameters"]["horizons"]
            }
            if set(values) == expected_keys:
                return values, {
                    "artifact_version": cached["artifact_version"],
                    "sample_sha256": sample_sha256,
                    "references_sha256": references_hash,
                    "reference_count": len(references),
                    "sample_pool_count_mismatch_pairs": sum(
                        int(item.get("sample_row_count", -1)) != int(item.get("pool_total_count", -1))
                        for item in references
                    ),
                    "sample_pool_count_difference": sum(
                        int(item.get("pool_total_count", 0)) - int(item.get("sample_row_count", 0))
                        for item in references
                    ),
                    "validated_from_existing_artifact": True,
                }, perf_counter() - started

    digest = hashlib.sha256()
    rows_by_horizon: Counter[str] = Counter()
    statuses: dict[tuple[str, int], Counter[str]] = defaultdict(Counter)
    rows_per_key: Counter[tuple[str, int]] = Counter()
    references_by_key: dict[tuple[str, int], tuple[float | None, int, int]] = {}
    with sample_path.open("rb") as stream:
        for line_number, raw_line in enumerate(stream, 1):
            digest.update(raw_line)
            try:
                row = json.loads(raw_line)
                label = row["label"]
                day = str(row["signal_date"])
                horizon = int(row["horizon"])
                if (
                    label["signal_date"] != day
                    or label["ts_code"] != row["ts_code"]
                    or int(label["horizon"]) != horizon
                ):
                    raise ValueError("label identity differs from its sample row")
                raw_mean = label.get("relative_pool_mean_return")
                pool_mean = None if raw_mean is None else float(raw_mean)
                if pool_mean is not None and not math.isfinite(pool_mean):
                    raise ValueError("relative pool mean is non-finite")
                complete_count = int(label.get("relative_pool_complete_count", -1))
                total_count = int(label.get("relative_pool_total_count", -1))
                if complete_count < 0 or total_count < 1:
                    raise ValueError("relative pool counts are missing")
                key = (day, horizon)
                facts = (pool_mean, complete_count, total_count)
                previous = references_by_key.setdefault(key, facts)
                if previous[1:] != facts[1:] or (
                    previous[0] is None
                ) != (pool_mean is None) or (
                    previous[0] is not None and pool_mean is not None
                    and not math.isclose(previous[0], pool_mean, rel_tol=1e-12, abs_tol=1e-12)
                ):
                    raise ValueError(f"relative pool evidence varies within {day}/{horizon}")
                status = str(label.get("status") or "UNKNOWN")
                statuses[key][status] += 1
                rows_per_key[key] += 1
                rows_by_horizon[str(horizon)] += 1
                if status == "COMPLETE":
                    absolute = label.get("prediction_total_return")
                    relative = label.get("relative_total_return")
                    if pool_mean is None or absolute is None or relative is None:
                        raise ValueError(f"complete sample has an incomplete relative return at line {line_number}")
                    absolute, relative = float(absolute), float(relative)
                    if not math.isclose(relative, absolute - pool_mean, rel_tol=1e-10, abs_tol=1e-12):
                        raise ValueError(f"relative return does not reconcile with its pool mean at line {line_number}")
            except (KeyError, TypeError, ValueError, json.JSONDecodeError, UnicodeDecodeError) as exc:
                raise ValueError(f"invalid relative-return evidence at sample line {line_number}: {exc}") from exc

    if digest.hexdigest() != sample_sha256:
        raise ValueError("relative-pool scan found a sample SHA-256 mismatch")
    if sum(rows_by_horizon.values()) != int(sample["row_count"]):
        raise ValueError("relative-pool scan row count does not match the sample manifest")
    expected_rows_by_horizon = {str(key): int(value) for key, value in sample["rows_by_horizon"].items()}
    if dict(rows_by_horizon) != expected_rows_by_horizon:
        raise ValueError("relative-pool scan horizon counts do not match the sample manifest")
    expected_keys = {
        (str(day), int(horizon))
        for day in sample_manifest["signal_dates"]
        for horizon in sample_manifest["parameters"]["horizons"]
    }
    if set(references_by_key) != expected_keys:
        raise ValueError("relative-pool references do not cover every signal-date/horizon pair")
    for key, (mean_return, complete_count, total_count) in references_by_key.items():
        if total_count < rows_per_key[key] or statuses[key]["COMPLETE"] != complete_count:
            raise ValueError(f"relative-pool counts do not reconcile for {key[0]}/{key[1]}")
        if complete_count > 0 and mean_return is None:
            raise ValueError(f"complete relative-pool sample has no mean for {key[0]}/{key[1]}")

    references = [
        {
            "signal_date": day,
            "horizon": horizon,
            "mean_return": facts[0],
            "complete_count": facts[1],
            "pool_total_count": facts[2],
            "sample_row_count": rows_per_key[(day, horizon)],
        }
        for (day, horizon), facts in sorted(references_by_key.items())
    ]
    references_hash = hashlib.sha256(_json_bytes(references)).hexdigest()
    artifact = {
        "artifact_version": "relative-pool-reference-v2",
        "sample_sha256": sample_sha256,
        "references_sha256": references_hash,
        "references": references,
    }
    _atomic_json(reference_path, artifact)
    values = {
        (date.fromisoformat(item["signal_date"]), int(item["horizon"])): item["mean_return"]
        for item in references
    }
    return values, {
        "artifact_version": artifact["artifact_version"],
        "sample_sha256": sample_sha256,
        "references_sha256": references_hash,
        "reference_count": len(references),
        "sample_pool_count_mismatch_pairs": sum(
            item["sample_row_count"] != item["pool_total_count"] for item in references
        ),
        "sample_pool_count_difference": sum(
            item["pool_total_count"] - item["sample_row_count"] for item in references
        ),
        "validated_from_existing_artifact": False,
    }, perf_counter() - started


def _code_hash() -> str:
    root = Path(__file__).resolve().parents[2]
    paths = (
        Path(__file__).resolve(),
        root / "data_collect" / "alpha_research" / "contracts.py",
        root / "data_collect" / "alpha_research" / "evaluation.py",
        root / "data_collect" / "alpha_research" / "factors.py",
        root / "data_collect" / "alpha_research" / "report.py",
    )
    digest = hashlib.sha256()
    for path in sorted(paths, key=lambda item: item.as_posix()):
        digest.update(path.relative_to(root).as_posix().encode("utf-8"))
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\n")
    return digest.hexdigest()


def _factor_specs(manifest: Mapping[str, Any]):
    availability = {
        str(item["factor"]): bool(item.get("available"))
        for item in manifest.get("factor_availability", [])
    }
    selected = tuple(factor for factor in FACTORS if availability.get(factor.name, False))
    if not selected:
        raise ValueError("sample manifest has no available factor inputs")
    return selected


def _sample_path(manifest_path: Path, manifest: Mapping[str, Any]) -> Path:
    sample_name = str(manifest.get("sample", {}).get("path") or "").strip()
    if not sample_name:
        raise ValueError("sample manifest is missing sample.path")
    path = (manifest_path.parent / sample_name).resolve()
    try:
        path.relative_to(manifest_path.parent.resolve())
    except ValueError as exc:
        raise ValueError("sample.path must stay inside the manifest directory") from exc
    return path


def _restore_record(row: Mapping[str, Any]) -> dict[str, Any]:
    label = row.get("label")
    if not isinstance(label, Mapping):
        raise ValueError("sample row is missing its label object")
    if str(label.get("signal_date")) != str(row.get("signal_date")):
        raise ValueError("sample signal date does not match its label")
    if str(label.get("ts_code")) != str(row.get("ts_code")):
        raise ValueError("sample security does not match its label")
    if int(label.get("horizon", -1)) != int(row.get("horizon", -2)):
        raise ValueError("sample horizon does not match its label")
    factor_values: dict[str, FactorValue] = {}
    raw_factors = row.get("factors")
    if not isinstance(raw_factors, Mapping):
        raise ValueError("sample row is missing factor values")
    for name, value in raw_factors.items():
        if not isinstance(value, Mapping):
            raise ValueError(f"invalid factor record for {name}")
        number = value.get("value")
        if number is not None:
            number = float(number)
            if not math.isfinite(number):
                raise ValueError(f"non-finite factor value for {name}")
        factor_values[str(name)] = FactorValue(
            str(name), number, str(value.get("status") or ""),
            str(value["reason"]) if value.get("reason") is not None else None,
        )
    return {
        "signal_date": str(row["signal_date"]),
        "ts_code": str(row["ts_code"]),
        "factor_values": factor_values,
        "label_status": str(label.get("status") or ""),
        "label_reason": label.get("reason"),
        "prediction_total_return": label.get("prediction_total_return"),
        "relative_total_return": label.get("relative_total_return"),
        "total_mv": None,
        "market_stage": str(row.get("market_stage") or "UNKNOWN"),
    }


def _evaluate_date(
    signal_date: str,
    rows: Sequence[Mapping[str, Any]],
    *,
    horizons: Sequence[int],
    factor_specs: Sequence[Any],
    assumptions: Sequence[float],
    sample_sha256: str,
    evaluator_hash: str,
    run_id: str,
    evaluation_id: str,
    size_control_status: str,
) -> dict[str, Any]:
    by_horizon: dict[int, list[dict[str, Any]]] = defaultdict(list)
    securities_by_horizon: dict[int, set[str]] = defaultdict(set)
    for source in rows:
        restored = _restore_record(source)
        horizon = int(source["horizon"])
        if horizon not in horizons:
            raise ValueError(f"unexpected sample horizon: {horizon}")
        if restored["ts_code"] in securities_by_horizon[horizon]:
            raise ValueError(f"duplicate sample security/date/horizon: {signal_date}/{restored['ts_code']}/{horizon}")
        securities_by_horizon[horizon].add(restored["ts_code"])
        by_horizon[horizon].append(restored)
    if set(by_horizon) != set(horizons):
        raise ValueError(f"sample date {signal_date} does not contain every requested horizon")
    if len({len(securities_by_horizon[horizon]) for horizon in horizons}) != 1:
        raise ValueError(f"sample date {signal_date} has inconsistent security counts by horizon")

    daily_rows: list[dict[str, Any]] = []
    sensitivity_rows: list[dict[str, Any]] = []
    for horizon in horizons:
        records = by_horizon[horizon]
        day_rows = evaluate_cross_section(
            date.fromisoformat(signal_date), horizon, records,
            factor_specs=factor_specs,
        )
        if size_control_status.startswith("NOT AVAILABLE"):
            for item in day_rows:
                item["size_control_status"] = size_control_status
        daily_rows.extend(day_rows)
        sensitivity_rows.extend(evaluate_delist_sensitivity(
            date.fromisoformat(signal_date), horizon, records,
            assumed_total_returns=assumptions,
            factor_specs=factor_specs,
        ))
    return {
        "artifact_version": ARTIFACT_VERSION,
        "sample_run_id": run_id,
        "evaluation_id": evaluation_id,
        "sample_sha256": sample_sha256,
        "evaluator_code_hash": evaluator_hash,
        "signal_date": signal_date,
        "row_count": len(rows),
        "security_count": len(securities_by_horizon[horizons[0]]),
        "daily_evaluations": daily_rows,
        "delist_sensitivity": sensitivity_rows,
    }


def _read_checkpoint(
    directory: Path,
    signal_date: str,
    checkpoint_hashes: Mapping[str, str],
    *,
    sample_sha256: str,
    evaluator_hash: str,
    run_id: str,
    evaluation_id: str,
) -> dict[str, Any] | None:
    expected_hash = checkpoint_hashes.get(signal_date)
    if not expected_hash:
        return None
    path = directory / f"{signal_date}.json"
    if not path.is_file() or _sha256(path) != expected_hash:
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if (
        payload.get("artifact_version") != ARTIFACT_VERSION
        or payload.get("sample_run_id") != run_id
        or payload.get("evaluation_id") != evaluation_id
        or payload.get("sample_sha256") != sample_sha256
        or payload.get("evaluator_code_hash") != evaluator_hash
        or payload.get("signal_date") != signal_date
    ):
        return None
    return payload


def _read_completed_evaluation(
    run_dir: Path,
    sample_sha256: str,
    expected_dates: Sequence[str],
    factor_names: Sequence[str],
    horizons: Sequence[int],
    assumptions: Sequence[float],
    evaluation_dir: Path,
) -> tuple[dict[str, Any], list[dict[str, Any]], Path] | None:
    # This version identifies date-level calculation semantics. Summary/report
    # changes may reuse these files; change ARTIFACT_VERSION if daily rows change.
    manifest_paths = (
        evaluation_dir / "daily_evaluation_manifest.json",
        run_dir / "daily_evaluation_manifest.json",
    )
    for manifest_path in manifest_paths:
        if not manifest_path.is_file():
            continue
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if (
            manifest.get("status") != "COMPLETE"
            or manifest.get("artifact_version") != ARTIFACT_VERSION
            or manifest.get("sample_sha256") != sample_sha256
            or manifest.get("factor_names") != list(factor_names)
            or manifest.get("horizons") != list(horizons)
        ):
            continue
        cached_assumptions = manifest.get("summary_parameters", {}).get("assumed_delist_returns")
        if cached_assumptions != list(assumptions):
            continue
        if set(manifest.get("signal_dates", ())) != set(expected_dates):
            continue
        if set(manifest.get("daily_artifacts", {})) != set(expected_dates):
            continue
        cached_evaluation_id = str(manifest.get("evaluation_id") or "")
        if not cached_evaluation_id:
            continue
        directory = run_dir / "daily_evaluations" / cached_evaluation_id[:24]
        daily_rows: list[dict[str, Any]] = []
        try:
            for signal_date, expected_hash in sorted(manifest["daily_artifacts"].items()):
                path = directory / f"{signal_date}.json"
                if not path.is_file() or _sha256(path) != expected_hash:
                    break
                payload = json.loads(path.read_text(encoding="utf-8"))
                if payload.get("signal_date") != signal_date:
                    break
                daily_rows.extend(payload["daily_evaluations"])
            else:
                return manifest, daily_rows, manifest_path
        except (KeyError, OSError, json.JSONDecodeError, TypeError):
            continue
    return None


def _summarize_sensitivity(
    daily_sensitivity: Sequence[Mapping[str, Any]],
    factor_summary: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    baseline_by_key: dict[tuple[str, int], dict[str, Mapping[str, Any]]] = {}
    for item in factor_summary:
        key = (str(item["factor"]), int(item["horizon"]))
        baseline_by_key[key] = {
            str(daily["signal_date"]): daily
            for daily in item.get("daily_series", ())
        }
    grouped: dict[tuple[str, int, float], list[Mapping[str, Any]]] = defaultdict(list)
    for row in daily_sensitivity:
        grouped[(str(row["factor"]), int(row["horizon"]), float(row["assumed_delist_total_return"]))].append(row)
    result = []
    for (factor, horizon, assumption), rows in sorted(grouped.items()):
        baselines = baseline_by_key.get((factor, horizon), {})
        matched_ic: list[tuple[float, float]] = []
        matched_spreads: list[tuple[float, float]] = []
        matched_rows: list[tuple[Mapping[str, Any], Mapping[str, Any]]] = []
        for row in rows:
            baseline_row = baselines.get(str(row.get("signal_date") or ""))
            if baseline_row is None:
                continue
            scenario_ic = row.get("rank_ic")
            baseline_ic = baseline_row.get("rank_ic")
            if scenario_ic is not None and baseline_ic is not None:
                matched_ic.append((float(baseline_ic), float(scenario_ic)))
            scenario_spread = row.get("group_spread_q5_q1")
            baseline_spread = baseline_row.get("group_spread_q5_q1")
            if scenario_spread is not None and baseline_spread is not None:
                matched_spreads.append((float(baseline_spread), float(scenario_spread)))
            if (
                (scenario_ic is not None and baseline_ic is not None)
                or (scenario_spread is not None and baseline_spread is not None)
            ):
                matched_rows.append((row, baseline_row))

        def pair_mean(pairs: Sequence[tuple[float, float]], index: int) -> float | None:
            return sum(pair[index] for pair in pairs) / len(pairs) if pairs else None

        baseline_ic = pair_mean(matched_ic, 0)
        mean_ic = pair_mean(matched_ic, 1)
        baseline_spread = pair_mean(matched_spreads, 0)
        mean_spread = pair_mean(matched_spreads, 1)
        baseline_pairs = sum(
            int(baseline_row.get("n_stocks") or 0)
            for row, baseline_row in matched_rows
        )
        scenario_pairs = sum(
            int(row.get("n_stocks") or 0) for row, _ in matched_rows
        )
        status = (
            "CALCULATED"
            if matched_ic or matched_spreads
            else "NO_MATCHED_VALID_DATES"
        )
        result.append({
            "factor": factor,
            "horizon": horizon,
            "assumed_delist_total_return": assumption,
            "status": status,
            "comparison_basis": "MATCHED_SIGNAL_DATES_COMPLETE_CASE_VS_HYPOTHETICAL_DELIST_IMPUTATION",
            "n_sensitivity_dates": len(rows),
            "n_valid_dates": len(matched_ic),
            "n_matched_dates": len(matched_ic),
            "n_matched_spread_dates": len(matched_spreads),
            "n_baseline_stock_date_pairs": baseline_pairs,
            "n_scenario_stock_date_pairs": scenario_pairs,
            "n_delist_imputed": sum(
                int(row.get("n_delist_imputed") or 0) for row in rows
            ),
            "n_delist_imputed_on_matched_dates": sum(
                int(row.get("n_delist_imputed") or 0)
                for row, _ in matched_rows
            ),
            "mean_baseline_rank_ic": baseline_ic,
            "mean_rank_ic": mean_ic,
            "mean_rank_ic_delta": (
                mean_ic - baseline_ic
                if mean_ic is not None and baseline_ic is not None else None
            ),
            "rank_ic_positive_rate": (
                sum(scenario > 0 for _, scenario in matched_ic) / len(matched_ic)
                if matched_ic else None
            ),
            "mean_baseline_group_spread_q5_q1": baseline_spread,
            "mean_group_spread_q5_q1": mean_spread,
            "mean_group_spread_q5_q1_delta": (
                mean_spread - baseline_spread
                if mean_spread is not None and baseline_spread is not None else None
            ),
            "rank_ic_direction_changed": (
                (baseline_ic > 0) != (mean_ic > 0)
                if baseline_ic is not None and mean_ic is not None else None
            ),
            "group_spread_direction_changed": (
                (baseline_spread > 0) != (mean_spread > 0)
                if baseline_spread is not None and mean_spread is not None else None
            ),
            "baseline_population": "COMPLETE_LABELS_AND_VALID_FACTOR",
            "scenario_population": "COMPLETE_LABELS_PLUS_HYPOTHETICALLY_IMPUTED_DELISTS",
            "interpretation": "HYPOTHETICAL_ONLY_NOT_REALIZED_TERMINAL_VALUE",
        })
    return result


def _build_report(
    manifest: Mapping[str, Any],
    evaluation: Mapping[str, Any],
    sensitivity: Sequence[Mapping[str, Any]],
    row_counts: Mapping[str, int],
    date_counts: Mapping[str, int],
) -> dict[str, Any]:
    context = manifest.get("report_context", {})
    identity = manifest["identity"]
    dataset = dict(context.get("dataset", manifest.get("dataset", {})))
    quality = dict(context.get("quality", manifest.get("quality", {})))
    sample_parameters = dict(manifest["parameters"])
    report_parameters = dict(sample_parameters)
    report_parameters["evaluation_version"] = EVALUATION_VERSION
    quality["label_status_by_horizon"] = {
        horizon: dict(sorted(counts.items()))
        for horizon, counts in row_counts.items()
    }
    total_status_counts: Counter[str] = Counter()
    for counts in row_counts.values():
        total_status_counts.update(counts)
    quality["label_status_counts"] = dict(sorted(total_status_counts.items()))
    quality["observed_signal_dates_by_horizon"] = dict(sorted(date_counts.items()))
    quality["factor_statistics_population"] = "COMPLETE_LABELS_ONLY"
    first_factor = evaluation.get("factors", [{}])[0]
    stage_by_date = {
        str(row["signal_date"]): str(row.get("market_stage") or "UNKNOWN")
        for row in first_factor.get("daily_series", [])
    }
    quality["market_stage_counts"] = dict(sorted(Counter(stage_by_date.values()).items()))
    return {
        "status": "EXPLORATORY_ONLY_STAGE1_GATES_UNRESOLVED",
        "research_validity_status": "NOT_FORMAL_FACTOR_EFFICACY_ASSESSMENT",
        "identity": identity,
        "dataset": dataset,
        "parameters": report_parameters,
        "sample_artifact_parameters": sample_parameters,
        "factor_availability": manifest.get("factor_availability", []),
        "quality": quality,
        "evaluation": dict(evaluation),
        "delist_sensitivity": list(sensitivity),
        "strategy_incrementality": context.get("strategy_incrementality", {
            "status": "NOT RUN_STAGE1_EVIDENCE_UNRESOLVED",
            "required_gates": [
                "逐证券未来行情覆盖审计通过",
                "回撤口径一致性问题解决并独立复核",
                "四组策略×资金场景均有可复现执行证据",
            ],
            "execution_engine": "现有 BacktestEngine；当前 Alpha Run 不创建或写入策略回测",
        }),
        "sample_artifact": {
            "artifact_version": manifest["artifact_version"],
            "sha256": manifest["sample"]["sha256"],
            "row_count": manifest["sample"]["row_count"],
        },
    }


def evaluate_sample_artifact(
    sample_manifest_path: str | Path,
    *,
    bootstrap_repetitions: int | None = None,
    min_bootstrap_dates: int | None = None,
    seed: int | None = None,
    assumed_delist_returns: Sequence[float] = DEFAULT_DELIST_RETURN_ASSUMPTIONS,
) -> dict[str, Any]:
    """Evaluate a verified sample JSONL, checkpoint by signal date, and publish a report.

    If a complete daily evaluation artifact is present and its content hashes
    validate, only the statistical summary and report are rebuilt.
    """
    started = perf_counter()
    manifest_path = Path(sample_manifest_path).resolve()
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("artifact_version") != "factor-label-sample-v1":
        raise ValueError("unsupported factor-label sample artifact version")
    identity = manifest.get("identity")
    if not isinstance(identity, Mapping) or not identity.get("run_id"):
        raise ValueError("sample manifest is missing identity.run_id")
    sample_info = manifest.get("sample")
    if not isinstance(sample_info, Mapping) or not sample_info.get("sha256"):
        raise ValueError("sample manifest is missing sample hash")
    sample_sha256 = str(sample_info["sha256"])
    sample_path = _sample_path(manifest_path, manifest)
    parameters = manifest.get("parameters", {})
    horizons = tuple(sorted(int(value) for value in parameters.get("horizons", (20, 40, 60))))
    if not horizons or len(set(horizons)) != len(horizons) or any(value not in {20, 40, 60} for value in horizons):
        raise ValueError("sample manifest contains unsupported or duplicate horizons")
    expected_dates = tuple(str(value) for value in manifest.get("signal_dates", ()))
    if not expected_dates or tuple(sorted(set(expected_dates))) != expected_dates:
        raise ValueError("sample manifest must list its complete signal_dates")
    factor_specs = _factor_specs(manifest)
    assumptions = tuple(float(value) for value in assumed_delist_returns)
    if len(set(assumptions)) != len(assumptions) or any(
        not math.isfinite(value) or value < -1.0 for value in assumptions
    ):
        raise ValueError("delist sensitivity assumptions must be unique finite returns >= -1")
    summary_parameters = {
        "bootstrap_repetitions": int(
            bootstrap_repetitions if bootstrap_repetitions is not None else parameters.get("bootstrap_repetitions", 1_000)
        ),
        "min_bootstrap_dates": int(
            min_bootstrap_dates if min_bootstrap_dates is not None else parameters.get("min_bootstrap_dates", 60)
        ),
        "seed": int(seed if seed is not None else parameters.get("seed", 20261016)),
        "assumed_delist_returns": list(assumptions),
    }
    if summary_parameters["bootstrap_repetitions"] < 100:
        raise ValueError("bootstrap_repetitions must be at least 100")
    if summary_parameters["min_bootstrap_dates"] < 2:
        raise ValueError("min_bootstrap_dates must be at least 2")
    evaluator_hash = _code_hash()
    evaluation_id = hashlib.sha256(_json_bytes({
        "sample_run_id": identity["run_id"],
        "sample_sha256": sample_sha256,
        "artifact_version": ARTIFACT_VERSION,
        "evaluation_version": EVALUATION_VERSION,
        "evaluator_code_hash": evaluator_hash,
        "factor_names": [factor.name for factor in factor_specs],
        "horizons": horizons,
        "summary_parameters": summary_parameters,
    })).hexdigest()
    run_dir = manifest_path.parent
    evaluation_dir = run_dir / "evaluations" / evaluation_id[:16]
    checkpoint_dir = run_dir / "daily_evaluations" / evaluation_id[:24]
    checkpoint_index_path = checkpoint_dir / "checkpoint_manifest.json"

    phase_start = perf_counter()
    completed = _read_completed_evaluation(
        run_dir, sample_sha256, expected_dates,
        [factor.name for factor in factor_specs], horizons, assumptions,
        evaluation_dir,
    )
    cache_hit = completed is not None
    cache_verification_seconds = perf_counter() - phase_start
    if completed is not None:
        evaluation_manifest, daily_rows, evaluation_manifest_path = completed
        unique_stock_counts = {
            (item["factor"], int(item["horizon"])): int(item["n_stocks"])
            for item in evaluation_manifest["unique_stock_counts"]
        }
        daily_sensitivity = evaluation_manifest.get("delist_sensitivity", [])
        row_counts = evaluation_manifest.get("label_status_counts_by_horizon", {})
        date_counts = {str(horizon): len(expected_dates) for horizon in horizons}
        sample_scan_seconds = 0.0
        evaluation_seconds = 0.0
    else:
        if not sample_path.is_file():
            raise FileNotFoundError(f"verified sample artifact is unavailable: {sample_path}")
        expected_rows = int(sample_info.get("row_count", 0))
        expected_rows_by_horizon = {
            str(key): int(value)
            for key, value in sample_info.get("rows_by_horizon", {}).items()
        }
        if not expected_rows or set(expected_rows_by_horizon) != {str(h) for h in horizons}:
            raise ValueError("sample manifest needs row_count and rows_by_horizon")

        try:
            checkpoint_index = json.loads(checkpoint_index_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            checkpoint_index = {}
        if (
            checkpoint_index.get("sample_sha256") != sample_sha256
            or checkpoint_index.get("evaluator_code_hash") != evaluator_hash
            or checkpoint_index.get("evaluation_id") != evaluation_id
        ):
            checkpoint_hashes: dict[str, str] = {}
        else:
            checkpoint_hashes = dict(checkpoint_index.get("daily_artifacts", {}))
        source_digest = hashlib.sha256()
        total_rows = 0
        row_counts_counter: dict[str, Counter[str]] = defaultdict(Counter)
        unique_codes: dict[tuple[str, int], set[str]] = defaultdict(set)
        market_stage_by_date: dict[str, str] = {}
        daily_sensitivity: list[dict[str, Any]] = []
        daily_rows_by_date: dict[str, list[dict[str, Any]]] = {}
        evaluation_seconds = 0.0
        evaluation_time = [0.0]
        sample_scan_start = perf_counter()
        current_date: str | None = None
        current_rows: list[dict[str, Any]] = []
        seen_keys: set[tuple[str, int]] = set()
        observed_dates: list[str] = []
        observed_by_horizon: Counter[str] = Counter()
        with sample_path.open("rb") as stream:
            for line_number, raw_line in enumerate(stream, 1):
                source_digest.update(raw_line)
                if not raw_line.strip():
                    raise ValueError(f"blank sample row at line {line_number}")
                try:
                    source_row = json.loads(raw_line)
                except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                    raise ValueError(f"invalid sample JSON at line {line_number}") from exc
                signal_date = str(source_row.get("signal_date") or "")
                if not signal_date:
                    raise ValueError(f"sample row has no signal_date at line {line_number}")
                try:
                    date.fromisoformat(signal_date)
                except ValueError as exc:
                    raise ValueError(f"invalid signal_date at line {line_number}") from exc
                if current_date is None:
                    current_date = signal_date
                    observed_dates.append(signal_date)
                elif signal_date != current_date:
                    if signal_date < current_date:
                        raise ValueError("sample rows must be ordered by signal_date")
                    _finalize_date(
                        current_date, current_rows, expected_dates, horizons,
                        factor_specs, assumptions, sample_sha256, evaluator_hash,
                        str(identity["run_id"]), evaluation_id, checkpoint_dir, checkpoint_index_path,
                        checkpoint_hashes, unique_codes,
                        daily_rows_by_date, daily_sensitivity,
                        size_control_status=str(manifest.get("size_control_status", "NOT AVAILABLE_NOT_IN_DATASET_CONTRACT")),
                        evaluator_seconds=lambda elapsed: evaluation_time.__setitem__(0, evaluation_time[0] + elapsed),
                    )
                    current_date = signal_date
                    current_rows = []
                    seen_keys.clear()
                    observed_dates.append(signal_date)
                horizon = int(source_row.get("horizon", -1))
                code = str(source_row.get("ts_code") or "")
                if horizon not in horizons or not code:
                    raise ValueError(f"invalid security/horizon at sample line {line_number}")
                key = (code, horizon)
                if key in seen_keys:
                    raise ValueError(f"duplicate sample row at {signal_date}/{code}/{horizon}")
                seen_keys.add(key)
                label = source_row.get("label") or {}
                row_counts_counter[str(horizon)][str(label.get("status") or "UNKNOWN")] += 1
                observed_by_horizon[str(horizon)] += 1
                total_rows += 1
                stage = str(source_row.get("market_stage") or "UNKNOWN")
                previous_stage = market_stage_by_date.setdefault(signal_date, stage)
                if previous_stage != stage:
                    raise ValueError(f"mixed market stages within signal date {signal_date}")
                current_rows.append(source_row)
            if current_date is not None:
                _finalize_date(
                    current_date, current_rows, expected_dates, horizons,
                    factor_specs, assumptions, sample_sha256, evaluator_hash,
                    str(identity["run_id"]), evaluation_id, checkpoint_dir, checkpoint_index_path,
                    checkpoint_hashes, unique_codes,
                    daily_rows_by_date, daily_sensitivity,
                    size_control_status=str(manifest.get("size_control_status", "NOT AVAILABLE_NOT_IN_DATASET_CONTRACT")),
                    evaluator_seconds=lambda elapsed: evaluation_time.__setitem__(0, evaluation_time[0] + elapsed),
                )
        sample_scan_seconds = perf_counter() - sample_scan_start
        evaluation_seconds = evaluation_time[0]
        if source_digest.hexdigest() != sample_sha256:
            raise ValueError("sample artifact SHA-256 does not match sample_manifest.json")
        if total_rows != expected_rows:
            raise ValueError(f"sample row count mismatch: expected {expected_rows}, observed {total_rows}")
        if dict(observed_by_horizon) != expected_rows_by_horizon:
            raise ValueError(
                f"sample horizon row counts mismatch: expected {expected_rows_by_horizon}, "
                f"observed {dict(observed_by_horizon)}"
            )
        if tuple(observed_dates) != expected_dates:
            raise ValueError("observed signal dates do not match sample manifest")
        unique_stock_counts = {
            key: len(codes) for key, codes in unique_codes.items()
        }
        daily_rows = [row for day in expected_dates for row in daily_rows_by_date[day]]
        if len(daily_rows) != len(factor_specs) * len(horizons) * len(expected_dates):
            raise ValueError("daily evaluation artifacts do not cover every date/factor/horizon")
        daily_sensitivity = [
            row for day in expected_dates
            for row in json.loads((checkpoint_dir / f"{day}.json").read_text(encoding="utf-8")).get("delist_sensitivity", [])
        ]
        finalized_evaluation_manifest = {
            "artifact_version": ARTIFACT_VERSION,
            "status": "COMPLETE",
            "evaluation_id": evaluation_id,
            "sample_run_id": identity["run_id"],
            "sample_sha256": sample_sha256,
            "evaluation_version": EVALUATION_VERSION,
            "evaluator_code_hash": evaluator_hash,
            "factor_names": [factor.name for factor in factor_specs],
            "horizons": list(horizons),
            "signal_dates": list(expected_dates),
            "summary_parameters": summary_parameters,
            "unique_stock_counts": [
                {"factor": key[0], "horizon": key[1], "n_stocks": count}
                for key, count in sorted(unique_stock_counts.items())
            ],
            "label_status_counts_by_horizon": {
                horizon: dict(sorted(counter.items()))
                for horizon, counter in sorted(row_counts_counter.items())
            },
            "daily_artifacts": {
                day: _sha256(checkpoint_dir / f"{day}.json")
                for day in expected_dates
            },
            "delist_sensitivity": daily_sensitivity,
        }
        evaluation_manifest_path = evaluation_dir / "daily_evaluation_manifest.json"
        _atomic_json(evaluation_manifest_path, finalized_evaluation_manifest)
        evaluation_manifest = finalized_evaluation_manifest
        row_counts = evaluation_manifest["label_status_counts_by_horizon"]
        date_counts = {str(horizon): len(expected_dates) for horizon in horizons}

    reference_values, reference_metadata, reference_seconds = _relative_pool_references(
        run_dir, sample_path, manifest,
    )
    summarize_start = perf_counter()
    factor_availability = manifest.get("factor_availability", [])
    available_names = {factor.name for factor in factor_specs}
    unavailable = [item for item in factor_availability if not item.get("available")]
    evaluation = summarize_evaluations(
        daily_rows,
        horizons=horizons,
        bootstrap_repetitions=summary_parameters["bootstrap_repetitions"],
        min_bootstrap_dates=summary_parameters["min_bootstrap_dates"],
        seed=summary_parameters["seed"],
        expected_signal_dates=[date.fromisoformat(day) for day in expected_dates],
        factor_specs=factor_specs,
        unique_stock_counts=unique_stock_counts,
        relative_pool_means={
            (day, horizon): value
            for (day, horizon), value in reference_values.items()
        },
        market_stages={date.fromisoformat(day): stage for day, stage in market_stage_by_date.items()}
        if not cache_hit else {
            date.fromisoformat(row["signal_date"]): str(row.get("market_stage") or "UNKNOWN")
            for row in daily_rows if row.get("factor") == factor_specs[0].name
        },
    )
    evaluation["method_notes"]["factor_availability"] = {
        "available": sorted(available_names),
        "unavailable": unavailable,
    }
    evaluation["method_notes"]["relative_pool_reference"] = (
        "Group returns are shown as absolute returns and as excess over the same signal-date/horizon "
        "complete-label equal-weight pool mean. Rank IC and Q5-Q1 are invariant to subtracting that "
        "common date/horizon baseline."
    )
    size_control_status = str(
        manifest.get("size_control_status", "NOT AVAILABLE_NOT_IN_DATASET_CONTRACT")
    )
    evaluation["method_notes"]["size"] = (
        f"Unavailable for this frozen sample: {size_control_status}. No size-adjusted factor results are reported."
        if size_control_status.startswith("NOT AVAILABLE")
        else "Partial Spearman uses cross-sectional ranks residualized on log(total_mv)."
    )
    if evaluation_manifest:
        daily_sensitivity = evaluation_manifest.get("delist_sensitivity", daily_sensitivity)
    sensitivity = _summarize_sensitivity(daily_sensitivity, evaluation["factors"])
    row_counts = evaluation_manifest.get("label_status_counts_by_horizon", row_counts)
    date_counts = {str(horizon): len(expected_dates) for horizon in horizons}
    report = _build_report(manifest, evaluation, sensitivity, row_counts, date_counts)
    report["evaluation_artifact"] = {
        "artifact_version": ARTIFACT_VERSION,
        "evaluation_id": evaluation_id,
        "daily_evaluation_id": evaluation_manifest["evaluation_id"],
        "evaluation_version": EVALUATION_VERSION,
        "evaluator_code_hash": evaluator_hash,
        "summary_parameters": summary_parameters,
    }
    report["relative_pool_reference"] = {
        key: value for key, value in reference_metadata.items()
        if key != "validated_from_existing_artifact"
    }
    report_paths = write_evaluation_report(
        manifest_path.parent, report, report_dir=evaluation_dir,
    )
    summarize_seconds = perf_counter() - summarize_start

    performance = {
        "evaluation_id": evaluation_id,
        "cache_hit": cache_hit,
        "source_scan_seconds": sample_scan_seconds,
        "daily_evaluation_seconds": evaluation_seconds,
        "checkpoint_verification_seconds": cache_verification_seconds,
        "relative_pool_reference_seconds": reference_seconds,
        "relative_pool_reference_count": reference_metadata["reference_count"],
        "relative_pool_reference_cache_hit": reference_metadata["validated_from_existing_artifact"],
        "summary_and_report_seconds": summarize_seconds,
        "total_seconds": perf_counter() - started,
        "source_bytes": int(sample_info.get("size_bytes", 0)),
        "source_rows": int(sample_info.get("row_count", 0)),
        "signal_dates": len(expected_dates),
        "factors": len(factor_specs),
        "horizons": len(horizons),
    }
    performance_path = evaluation_dir / "performance_evaluation.json"
    _atomic_json(performance_path, performance)
    return {
        "run_id": identity["run_id"],
        "evaluation_id": evaluation_id,
        "report": report_paths,
        "daily_evaluation_manifest": str(evaluation_manifest_path.resolve()),
        "performance": str(performance_path.resolve()),
        "cache_hit": cache_hit,
        "statistics": evaluation,
    }


def _finalize_date(
    signal_date: str,
    rows: Sequence[Mapping[str, Any]],
    expected_dates: Sequence[str],
    horizons: Sequence[int],
    factor_specs: Sequence[Any],
    assumptions: Sequence[float],
    sample_sha256: str,
    evaluator_hash: str,
    run_id: str,
    evaluation_id: str,
    checkpoint_dir: Path,
    checkpoint_index_path: Path,
    checkpoint_hashes: dict[str, str],
    unique_codes: dict[tuple[str, int], set[str]],
    daily_rows_by_date: dict[str, list[dict[str, Any]]],
    daily_sensitivity: list[dict[str, Any]],
    *,
    size_control_status: str,
    evaluator_seconds,
) -> None:
    if signal_date not in expected_dates:
        raise ValueError(f"sample includes unexpected signal date: {signal_date}")
    by_code: dict[str, set[int]] = defaultdict(set)
    for row in rows:
        by_code[str(row["ts_code"])].add(int(row["horizon"]))
        label = row.get("label") or {}
        horizon = int(row["horizon"])
        if str(label.get("status")) == "COMPLETE":
            for factor in factor_specs:
                factor_record = (row.get("factors") or {}).get(factor.name, {})
                if factor_record.get("status") == "VALID":
                    unique_codes[(factor.name, horizon)].add(str(row["ts_code"]))
    if any(codes != set(horizons) for codes in by_code.values()):
        raise ValueError(f"signal date {signal_date} has missing horizon rows for a security")

    checkpoint = _read_checkpoint(
        checkpoint_dir, signal_date, checkpoint_hashes,
        sample_sha256=sample_sha256,
        evaluator_hash=evaluator_hash,
        run_id=run_id,
        evaluation_id=evaluation_id,
    )
    if checkpoint is None:
        evaluation_started = perf_counter()
        checkpoint = _evaluate_date(
            signal_date,
            rows,
            horizons=horizons,
            factor_specs=factor_specs,
            assumptions=assumptions,
            sample_sha256=sample_sha256,
            evaluator_hash=evaluator_hash,
            run_id=run_id,
            evaluation_id=evaluation_id,
            size_control_status=size_control_status,
        )
        evaluator_seconds(perf_counter() - evaluation_started)
        checkpoint_path = checkpoint_dir / f"{signal_date}.json"
        encoded = _atomic_json(checkpoint_path, checkpoint)
        checkpoint_hashes[signal_date] = hashlib.sha256(encoded).hexdigest()
        _atomic_json(checkpoint_index_path, {
            "artifact_version": ARTIFACT_VERSION,
            "evaluation_id": evaluation_id,
            "sample_sha256": sample_sha256,
            "evaluator_code_hash": evaluator_hash,
            "daily_artifacts": dict(sorted(checkpoint_hashes.items())),
        })
    daily_rows_by_date[signal_date] = list(checkpoint["daily_evaluations"])
    daily_sensitivity.extend(checkpoint.get("delist_sensitivity", []))


__all__ = [
    "ARTIFACT_VERSION",
    "DEFAULT_DELIST_RETURN_ASSUMPTIONS",
    "evaluate_sample_artifact",
    "recover_sample_manifest",
]
