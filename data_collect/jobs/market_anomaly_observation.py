"""Build the stock-first Market Anomaly Observation series in bounded tiles.

The job owns orchestration only.  SQL lives in Observation repositories, and
all calculations are pure functions from :mod:`data_collect.observation`.
Each trading date is its own lock/checkpoint boundary; the job never wraps the
historical range in one transaction.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta, timezone
import json
import uuid
from collections import defaultdict
from statistics import median
from typing import Any, Iterable, Mapping, Sequence

from data_collect.data_access import DataAccessKernel, RunContext
from data_collect.data_access.context import WorkScope
from data_collect.data_access.policy_catalog import build_default_registry
from data_collect.data_access.repository.observation_repository import (
    SectorAnomalyObservationRepository,
    StockAnomalyObservationRepository,
)
from data_collect.observation import (
    CONTRACT_VERSION,
    ObservationPolicy,
    aggregate_sector_rows,
    calculate_scores,
    confirm_stock_sector,
    hash_object,
    hash_rows,
    select_primary_state,
    build_tags,
    compute_stock_series,
)
from data_collect.observation.contracts import ObservationIdentity


JOB_NAME = "market_anomaly_observation"
DEFAULT_BASELINE = "foundation_v2_20260821"
BENCHMARK_LOOKBACK_CALENDAR_DAYS = 3
INPUT_HASH_COLUMNS = (
    "trade_date", "ts_code", "eligible_by_market", "is_tradeable", "is_suspended",
    "raw_close", "volume", "amount", "turnover_rate_f", "free_share",
    "adj_close_base", "total_return_price", "industry_system", "industry_level",
    "industry_code", "industry_name", "industry_source_asof_date",
    "industry_data_available_timestamp", "industry_quality_status",
)


def _as_date(value: date | str) -> date:
    return value if isinstance(value, date) else date.fromisoformat(str(value))


def _run_context(run_id: str | None = None) -> RunContext:
    return RunContext(
        run_id=run_id or f"observation-{uuid.uuid4().hex}",
        job_name=JOB_NAME,
        code_version="unknown",
        config_hash="observation-default-policy",
        pipeline_version="data-access-v1",
    )


def _market_return_map(rows: Sequence[Mapping[str, Any]], horizon: int = 20) -> dict[date, float | None]:
    values = []
    for row in rows:
        try:
            day = _as_date(row["trade_date"])
            close = float(row["close"])
        except (KeyError, TypeError, ValueError):
            continue
        if close > 0:
            values.append((day, close))
    result: dict[date, float | None] = {}
    for index, (day, close) in enumerate(values):
        if index < horizon or values[index - horizon][1] <= 0:
            result[day] = None
        else:
            result[day] = close / values[index - horizon][1] - 1.0
    return result


def _sector_status(row: Mapping[str, Any]) -> str:
    # A daily membership capture is not automatically a validated five-year
    # PIT history.  It remains DAILY_CAPTURED until a separate continuity and
    # coverage gate promotes the input to CONFIRMED.
    if row.get("industry_code") and str(row.get("industry_quality_status") or "").upper() == "VALID":
        return "DAILY_CAPTURED"
    return "MISSING"


def _quality_status(metrics: Mapping[str, Any]) -> str:
    if metrics.get("calculation_status") == "INVALID":
        return "INVALID"
    if metrics.get("missing_reason_codes"):
        return "WARNING"
    return "VALID"


OBSERVATION_HASH_FIELDS = (
    "observation_date", "ts_code", "amount_ratio_5_60", "amount_ratio_20_60",
    "turnover_ratio_5_60", "turnover_ratio_20_60", "amount_zscore_60",
    "amount_single_ratio_60", "abnormal_days_20", "return_1d", "return_5d",
    "return_20d", "price_position_52w", "distance_ma20", "distance_ma60",
    "max_drawdown_20d", "up_volume_ratio_20", "down_volume_ratio_20",
    "up_down_volume_ratio", "price_response_efficiency_20", "anomaly_flag",
    "anomaly_streak", "anomaly_frequency_20", "days_since_first_anomaly",
    "relative_return_20d", "relative_volume_zscore", "trading_anomaly_score",
    "price_structure_score", "persistence_score", "sector_confirmation_score",
    "risk_score", "observation_priority", "primary_state", "state_reason_codes",
    "sector_data_status", "sector_confirmation_status", "calculation_status",
    "quality_status", "missing_reason_codes",
)


def _observation_hash(row: Mapping[str, Any]) -> str:
    return hash_object({key: row.get(key) for key in OBSERVATION_HASH_FIELDS})


def _prepare_metric(
    metric: Mapping[str, Any],
    *,
    snapshot_id: str,
    observation_date: date,
    baseline_id: str,
    policy: ObservationPolicy,
    previous: Mapping[str, Any] | None,
    market_returns: Mapping[date, float | None],
    relative_median: float | None,
    input_manifest_hash: str,
    input_row_count: int,
    calculation_run_id: str,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    current = dict(metric)
    current["sector_data_status"] = _sector_status(current)
    current["sector_confirmation_status"] = "UNKNOWN"
    current["sector_confirmation_score"] = None
    current["previous_anomaly_flag"] = previous.get("anomaly_flag") if previous else None
    current["previous_state"] = previous.get("primary_state") if previous else None
    market_return = market_returns.get(observation_date)
    if current.get("return_20d") is not None and market_return is not None:
        current["relative_return_20d"] = float(current["return_20d"]) - market_return
    else:
        current["relative_return_20d"] = None
    if relative_median is not None and current.get("amount_zscore_60") is not None:
        current["relative_volume_zscore"] = float(current["amount_zscore_60"]) - relative_median
    else:
        current["relative_volume_zscore"] = None
        missing = set(current.get("missing_reason_codes") or ())
        missing.add("INSUFFICIENT_UNIVERSE")
        current["missing_reason_codes"] = tuple(sorted(missing))
        current["calculation_status"] = "PARTIAL"
        current["quality_status"] = "WARNING"

    identity = ObservationIdentity(
        observation_date=observation_date,
        ts_code=str(current["ts_code"]),
        methodology_hash=policy.methodology_hash,
        data_baseline_id=baseline_id,
    )
    observation_id = str(uuid.uuid5(uuid.NAMESPACE_URL, identity.as_string()))
    # The caller resolves the aggregate row later.  The placeholder contains
    # the exact stock identity needed for a deterministic confirmation row.
    confirmation = confirm_stock_sector(current, None, policy)
    scores, score_missing, priority = calculate_scores(
        current, policy, sector_confirmation_score=confirmation["sector_confirmation_score"]
    )
    current.update(scores)
    current["observation_priority"] = priority
    state, state_reasons = select_primary_state(
        current, policy, previous_state=current.get("previous_state")
    )
    tags = list(build_tags(current, policy))
    if confirmation["sector_confirmation_status"] == "CONFIRMED":
        tags.append("SECTOR_CONFIRMATION")
    current["primary_state"] = state
    current["state_reason_codes"] = tuple(state_reasons)
    missing = set(current.get("missing_reason_codes") or ())
    missing.update(score_missing)
    if state is None:
        missing.add("STATE_UNDETERMINED")
    current["missing_reason_codes"] = tuple(sorted(missing))
    current["quality_status"] = _quality_status(current)
    current["calculation_status"] = "VALID" if not missing else "PARTIAL"

    observation_hash = _observation_hash(current)
    main = {
        "observation_id": observation_id,
        "snapshot_id": snapshot_id,
        "observation_date": observation_date,
        "ts_code": current["ts_code"],
        "methodology_hash": policy.methodology_hash,
        "data_baseline_id": baseline_id,
        **{key: current.get(key) for key in StockAnomalyObservationRepository.STOCK_COLUMNS if key not in {
            "observation_id", "snapshot_id", "observation_date", "ts_code",
            "methodology_hash", "data_baseline_id", "state_reason_codes",
            "missing_reason_codes", "input_row_count", "input_manifest_hash",
            "observation_hash", "calculation_run_id",
        }},
        "state_reason_codes": tuple(state_reasons),
        "missing_reason_codes": tuple(sorted(missing)),
        "input_row_count": input_row_count,
        "input_manifest_hash": input_manifest_hash,
        "observation_hash": observation_hash,
        "calculation_run_id": calculation_run_id,
    }
    tags_rows = [
        {
            "observation_id": observation_id,
            "tag_code": tag,
            "tag_payload": {"methodology_hash": policy.methodology_hash},
        }
        for tag in sorted(set(tags))
    ]
    confirmation_row = {
        "observation_id": observation_id,
        "industry_system": current.get("industry_system"),
        "industry_level": current.get("industry_level"),
        "industry_code": current.get("industry_code"),
        "industry_name": current.get("industry_name"),
        "input_manifest_hash": input_manifest_hash,
        **confirmation,
    }
    return main, tags_rows, confirmation_row


def _observation_scope(start: date, end: date, snapshot_id: str, codes: Sequence[str] | None = None) -> WorkScope:
    return WorkScope(
        date_from=start,
        date_to=end,
        codes=tuple(codes) if codes else None,
        dimensions={"domain": "market_anomaly_observation", "snapshot_id": snapshot_id},
    )


def run_observation(
    start_date: date | str,
    end_date: date | str,
    data_baseline_id: str = DEFAULT_BASELINE,
    *,
    policy: ObservationPolicy | None = None,
    limit_codes: int | None = None,
    calculation_run_id: str | None = None,
    kernel: DataAccessKernel | None = None,
    context: RunContext | None = None,
) -> dict[str, Any]:
    """Build a bounded Observation snapshot and leave it VALIDATED, not frozen."""
    start, end = _as_date(start_date), _as_date(end_date)
    if start > end:
        raise ValueError("observation start must not be after end")
    policy = policy or ObservationPolicy()
    owns_kernel = kernel is None
    kernel = kernel or DataAccessKernel(policy_registry=build_default_registry())
    context = context or _run_context(calculation_run_id)
    calculation_run_id = calculation_run_id or context.run_id
    stock_repo = StockAnomalyObservationRepository(kernel)
    sector_repo = SectorAnomalyObservationRepository(kernel)
    try:
        baseline = stock_repo.validated_baseline(data_baseline_id, context)
        cutoff = baseline.get("baseline_cutoff_date")
        if cutoff is not None and end > _as_date(cutoff):
            raise ValueError(
                f"observation end {end} exceeds validated data baseline cutoff {cutoff}: {data_baseline_id}"
            )
        dates = stock_repo.observation_dates(start, end, context)
        if not dates:
            raise ValueError(f"no open trading dates in observation range: {start}..{end}")
        snapshot = stock_repo.ensure_snapshot(
            dates[0], dates[-1], data_baseline_id, policy,
            hash_object({"contract_version": CONTRACT_VERSION}), context,
        )
        snapshot_id = str(snapshot["snapshot_id"])
        status = str(snapshot.get("status") or "DRAFT").upper()
        if status in {"FROZEN", "SUPERSEDED", "CORRUPTED", "VALIDATED"}:
            raise RuntimeError(
                f"observation snapshot is immutable/already validated ({status}); create a new methodology or baseline"
            )
        if status in {"DRAFT", "FAILED"}:
            stock_repo.transition_snapshot(
                snapshot_id, context, dates[0], dates[-1], (status,), "BUILDING",
                {"built_at": datetime.now(timezone.utc), "failure_reason": None, "failed_at": None},
            )

        market_start = dates[0] - timedelta(days=policy.max_required_history * BENCHMARK_LOOKBACK_CALENDAR_DAYS)
        market_rows = stock_repo.market_rows(market_start, dates[-1], policy.benchmark_code, context)
        market_returns = _market_return_map(market_rows)
        previous_states: dict[str, dict[str, Any]] | None = None
        date_reports: dict[str, dict[str, Any]] = {}
        for observation_date in dates:
            checkpoint_key = f"observation-date:{snapshot_id}:{observation_date.isoformat()}"
            checkpoint = kernel.checkpoint.load(context.run_id, context.job_name, checkpoint_key)
            if checkpoint is not None and checkpoint.done:
                previous_states = None
                continue

            code_limit = limit_codes or 10_000
            codes = stock_repo.eligible_codes(observation_date, context, limit=code_limit)
            if previous_states is None:
                previous_states = stock_repo.previous_states(snapshot_id, observation_date, codes, context)
            warmup_start = observation_date - timedelta(days=policy.max_required_history * BENCHMARK_LOOKBACK_CALENDAR_DAYS)
            target_metrics: list[tuple[dict[str, Any], str, int]] = []
            with kernel.lock_manager.session_exclusive(
                "observation.snapshot", f"{snapshot_id}:{observation_date.isoformat()}"
            ):
                for offset in range(0, len(codes), policy.code_batch_size):
                    code_batch = codes[offset:offset + policy.code_batch_size]
                    input_rows = stock_repo.input_rows(
                        warmup_start, observation_date, code_batch, context,
                        max_rows=100_000, snapshot_id=snapshot_id,
                        industry_system=policy.industry_system,
                        industry_level=policy.industry_level,
                    )
                    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
                    for row in input_rows:
                        grouped[str(row["ts_code"])].append(row)
                    for code in code_batch:
                        code_rows = grouped.get(code, [])
                        series = compute_stock_series(code_rows, policy)
                        current = [item for item in series if _as_date(item["observation_date"]) == observation_date]
                        if not current:
                            continue
                        target_metrics.append((current[-1], hash_rows(code_rows, columns=INPUT_HASH_COLUMNS)[0], len(code_rows)))

                zscores = [
                    float(metric.get("amount_zscore_60"))
                    for metric, _, _ in target_metrics
                    if metric.get("eligible_by_market") is True and metric.get("amount_zscore_60") is not None
                ]
                relative_median = median(zscores) if len(zscores) >= policy.min_universe_size else None
                main_rows: list[dict[str, Any]] = []
                tag_rows: list[dict[str, Any]] = []
                confirmation_rows: list[dict[str, Any]] = []
                prepared: list[tuple[dict[str, Any], list[dict[str, Any]], dict[str, Any]]] = []
                for metric, input_hash, input_count in target_metrics:
                    prepared.append(_prepare_metric(
                        metric,
                        snapshot_id=snapshot_id,
                        observation_date=observation_date,
                        baseline_id=data_baseline_id,
                        policy=policy,
                        previous=previous_states.get(str(metric["ts_code"])) if previous_states else None,
                        market_returns=market_returns,
                        relative_median=relative_median,
                        input_manifest_hash=input_hash,
                        input_row_count=input_count,
                        calculation_run_id=calculation_run_id,
                    ))
                    main_rows.append(prepared[-1][0])
                    tag_rows.extend(prepared[-1][1])
                    confirmation_rows.append(prepared[-1][2])

                sector_input = []
                for main in main_rows:
                    sector_input.append({
                        **main,
                        "observation_date": main["observation_date"],
                        "previous_anomaly_flag": (previous_states or {}).get(main["ts_code"], {}).get("anomaly_flag"),
                    })
                sector_rows = aggregate_sector_rows(sector_input, policy)
                sector_index = {
                    (row["industry_system"], row["industry_level"], row["industry_code"]): row
                    for row in sector_rows
                }
                confirmation_by_id = {
                    row["observation_id"]: row for row in confirmation_rows
                }
                for main in main_rows:
                    key = (main.get("industry_system"), main.get("industry_level"), main.get("industry_code"))
                    sector_row = sector_index.get(key)
                    confirmation = confirm_stock_sector(main, sector_row, policy)
                    main["sector_data_status"] = confirmation["sector_data_status"]
                    main["sector_confirmation_status"] = confirmation["sector_confirmation_status"]
                    main["sector_confirmation_score"] = confirmation["sector_confirmation_score"]
                    if confirmation["sector_confirmation_status"] == "CONFIRMED":
                        main["sector_confirmation_score"] = confirmation["sector_confirmation_score"]
                    # The map is keyed by the deterministic observation id, so
                    # sector confirmation stays linear in the tile size.
                    confirmation_by_id[main["observation_id"]].update(confirmation)

                    scores, score_missing, priority = calculate_scores(
                        main, policy,
                        sector_confirmation_score=main["sector_confirmation_score"],
                    )
                    main.update(scores)
                    main["observation_priority"] = priority
                    score_fields = {
                        "TRADING_ANOMALY_MISSING", "PRICE_STRUCTURE_MISSING",
                        "PERSISTENCE_MISSING", "SECTOR_CONFIRMATION_MISSING", "RISK_MISSING",
                    }
                    missing = {
                        code for code in (main.get("missing_reason_codes") or ())
                        if code not in score_fields
                    }
                    missing.update(score_missing)
                    if main.get("primary_state") is None:
                        missing.add("STATE_UNDETERMINED")
                    main["missing_reason_codes"] = tuple(sorted(missing))
                    main["quality_status"] = _quality_status(main)
                    main["calculation_status"] = "VALID" if not missing else "PARTIAL"
                    main["observation_hash"] = _observation_hash(main)

                for main in main_rows:
                    if main["sector_confirmation_status"] == "CONFIRMED" and "SECTOR_CONFIRMATION" not in [
                        row["tag_code"] for row in tag_rows if row["observation_id"] == main["observation_id"]
                    ]:
                        tag_rows.append({"observation_id": main["observation_id"], "tag_code": "SECTOR_CONFIRMATION", "tag_payload": {}})

                if main_rows:
                    tile = policy.code_batch_size
                    for offset in range(0, len(main_rows), tile):
                        main_tile = main_rows[offset:offset + tile]
                        ids = [row["observation_id"] for row in main_tile]
                        tile_tags = [row for row in tag_rows if row["observation_id"] in ids]
                        tile_confirmations = [row for row in confirmation_rows if row["observation_id"] in ids]
                        tile_codes = [row["ts_code"] for row in main_tile]
                        tile_scope = _observation_scope(observation_date, observation_date, snapshot_id, tile_codes)
                        stock_repo.write_stock_rows(main_tile, context, tile_scope)
                        stock_repo.write_tags(tile_tags, context, tile_scope)
                        stock_repo.write_confirmations(tile_confirmations, context, tile_scope)
                    sector_repo.write_rows(
                        [
                            {
                                **row,
                                "snapshot_id": snapshot_id,
                                "sector_observation_id": str(uuid.uuid5(
                                    uuid.NAMESPACE_URL,
                                    "sector|" + snapshot_id + "|" + "|".join(str(row[key]) for key in (
                                        "observation_date", "industry_system", "industry_level", "industry_code"
                                    )),
                                )),
                                "input_manifest_hash": hash_object(row),
                            }
                            for row in sector_rows
                        ],
                        context,
                        _observation_scope(observation_date, observation_date, snapshot_id),
                    )

                date_report = stock_repo.validate_date(snapshot_id, observation_date, context)
                date_reports[observation_date.isoformat()] = {
                    key: int(value or 0) if key.endswith("count") else value
                    for key, value in date_report.items()
                }
                kernel.checkpoint.mark_done(context.run_id, context.job_name, checkpoint_key)
                previous_states = {
                    row["ts_code"]: {"primary_state": row.get("primary_state"), "anomaly_flag": row.get("anomaly_flag")}
                    for row in main_rows
                }

        summary = stock_repo.summarize_snapshot(snapshot_id, dates[0], dates[-1], context)
        quality_report = {
            "contract_version": CONTRACT_VERSION,
            "methodology_hash": policy.methodology_hash,
            "data_baseline_id": data_baseline_id,
            "snapshot_id": snapshot_id,
            "dates": date_reports,
            "summary": summary,
            "sector_mode": "OPTIONAL_DAILY_CAPTURED",
            "failures": {},
        }
        if int(summary.get("row_count") or 0) == 0:
            quality_report["failures"]["empty_observation"] = "no stock observations were written"
        if any(int(item.get("invalid_count") or 0) > 0 for item in date_reports.values()):
            quality_report["failures"]["invalid_calculation"] = "one or more observations have INVALID calculation status"
        input_stream = stock_repo.snapshot_hash_rows(snapshot_id, dates[0], dates[-1], context)
        input_manifest_hash, _ = hash_rows(input_stream, columns=("observation_date", "ts_code", "input_manifest_hash", "observation_hash"))
        quality_hash = hash_object(quality_report)
        if quality_report["failures"]:
            stock_repo.transition_snapshot(
                snapshot_id, context, dates[0], dates[-1], ("BUILDING",), "FAILED",
                {"failure_reason": json.dumps(quality_report["failures"], ensure_ascii=False), "failed_at": datetime.now(timezone.utc)},
            )
            raise RuntimeError(f"observation quality gate failed: {quality_report['failures']}")
        stock_repo.transition_snapshot(
            snapshot_id, context, dates[0], dates[-1], ("BUILDING",), "VALIDATED",
            {
                "input_manifest_hash": input_manifest_hash,
                "quality_report_hash": quality_hash,
                "quality_report": json.dumps(quality_report, ensure_ascii=False, default=str),
                "row_count": int(summary.get("row_count") or 0),
                "stock_count": int(summary.get("stock_count") or 0),
                "sector_count": int(summary.get("sector_count") or 0),
                "date_count": int(summary.get("date_count") or 0),
                "validated_at": datetime.now(timezone.utc),
            },
        )
        return {"snapshot_id": snapshot_id, "status": "VALIDATED", "summary": summary, "quality_report": quality_report}
    except Exception as exc:
        # Keep the failure state explicit, but do not mask the original error
        # if the snapshot transition itself cannot be committed.
        try:
            if "snapshot_id" in locals():
                stock_repo.transition_snapshot(
                    snapshot_id, context, start, end, ("BUILDING",), "FAILED",
                    {"failure_reason": str(exc)[:4000], "failed_at": datetime.now(timezone.utc)},
                )
        except Exception:
            pass
        raise
    finally:
        if owns_kernel:
            kernel.close()


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build bounded Market Anomaly Observation snapshots")
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--baseline", default=DEFAULT_BASELINE)
    parser.add_argument("--limit-codes", type=int)
    parser.add_argument("--run-id")
    args = parser.parse_args(argv)
    result = run_observation(
        args.start_date, args.end_date, args.baseline,
        limit_codes=args.limit_codes, calculation_run_id=args.run_id,
    )
    print(json.dumps(result, ensure_ascii=False, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
