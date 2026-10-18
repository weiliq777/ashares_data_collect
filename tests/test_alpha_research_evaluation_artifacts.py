from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from data_collect.alpha_research.evaluation_artifacts import (
    _summarize_sensitivity,
    evaluate_sample_artifact,
    recover_sample_manifest,
)


def _sample_row(day: str, code: str, horizon: int, value: float, status: str, result: float | None, reason: str | None = None):
    return {
        "signal_date": day,
        "ts_code": code,
        "horizon": horizon,
        "market_stage": "UPTREND",
        "benchmark_return_60d": 0.1,
        "factors": {
            "pe_ttm": {"value": value, "status": "VALID", "reason": None},
        },
        "label": {
            "signal_date": day,
            "ts_code": code,
            "horizon": horizon,
            "status": status,
            "reason": reason,
            "prediction_total_return": result,
            "entry_reference_total_return": result,
            "entry_reference_status": status,
            "relative_total_return": None if result is None else result - 0.02,
            "relative_pool_mean_return": 0.02,
            "relative_pool_complete_count": 5,
            "relative_pool_total_count": 7,
        },
    }


def test_sample_artifact_evaluation_checkpoints_daily_results_and_reuses_them(tmp_path):
    run_id = "run-a"
    run_dir = tmp_path / run_id
    run_dir.mkdir()
    rows = [
        _sample_row("2025-01-02", f"00000{index}.SZ", 20, float(index), "COMPLETE", index / 100)
        for index in range(1, 6)
    ]
    rows.append(_sample_row(
        "2025-01-02", "000006.SZ", 20, 6.0, "INCOMPLETE", None,
        "DELISTED_WITHIN_HORIZON_NO_TERMINAL_VALUE",
    ))
    sample_bytes = b"".join(
        (json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n").encode()
        for row in rows
    )
    sample_path = run_dir / "samples.jsonl"
    sample_path.write_bytes(sample_bytes)
    manifest = {
        "artifact_version": "factor-label-sample-v1",
        "identity": {"run_id": run_id, "dataset_id": "dataset-a", "input_hash": "input-a"},
        "dataset": {"schema_versions": ["research-v2.1"], "snapshot_count": 6},
        "parameters": {
            "horizons": [20], "signal_start": "2025-01-02", "signal_end": "2025-01-02",
            "bootstrap_repetitions": 100, "min_bootstrap_dates": 2, "seed": 42,
        },
        "factor_availability": [{"factor": "pe_ttm", "available": True}],
        "signal_dates": ["2025-01-02"],
        "sample": {
            "path": sample_path.name,
            "sha256": hashlib.sha256(sample_bytes).hexdigest(),
            "size_bytes": len(sample_bytes),
            "row_count": len(rows),
            "rows_by_horizon": {"20": len(rows)},
        },
        "report_context": {
            "dataset": {"schema_versions": ["research-v2.1"], "snapshot_count": 6},
            "quality": {"available_factor_count": 1, "total_factor_count": 1},
            "strategy_incrementality": {"status": "NOT RUN_STAGE1_EVIDENCE_UNRESOLVED"},
        },
    }
    manifest_path = run_dir / "sample_manifest.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    legacy_report = run_dir / "report.json"
    legacy_report.write_text('{"legacy":"report"}\n', encoding="utf-8")
    legacy_daily_manifest = run_dir / "daily_evaluation_manifest.json"
    legacy_daily_manifest.write_text('{"legacy":"daily manifest"}\n', encoding="utf-8")
    legacy_performance = run_dir / "performance_evaluation.json"
    legacy_performance.write_text('{"legacy":"performance"}\n', encoding="utf-8")

    first = evaluate_sample_artifact(manifest_path)
    report_path = Path(first["report"]["json"])
    report_hash = hashlib.sha256(report_path.read_bytes()).hexdigest()
    report = json.loads(report_path.read_text(encoding="utf-8"))
    performance = json.loads(Path(first["performance"]).read_text(encoding="utf-8"))
    daily_manifest = json.loads(Path(first["daily_evaluation_manifest"]).read_text(encoding="utf-8"))

    assert first["cache_hit"] is False
    assert performance["cache_hit"] is False
    assert report_path.parent != run_dir
    assert report["evaluation_artifact"]["evaluation_version"] == "cross-sectional-evaluation-v5"
    assert legacy_report.read_text(encoding="utf-8") == '{"legacy":"report"}\n'
    assert legacy_daily_manifest.read_text(encoding="utf-8") == '{"legacy":"daily manifest"}\n'
    assert legacy_performance.read_text(encoding="utf-8") == '{"legacy":"performance"}\n'
    assert daily_manifest["status"] == "COMPLETE"
    assert len(daily_manifest["daily_artifacts"]) == 1
    assert report["evaluation"]["factors"][0]["n_stock_date_pairs"] == 5
    assert report["evaluation"]["factors"][0]["mean_missing_ratio"] == 1 / 6
    assert report["quality"]["label_status_counts"] == {"COMPLETE": 5, "INCOMPLETE": 1}
    assert report["delist_sensitivity"]
    assert report["relative_pool_reference"]["reference_count"] == 1
    assert report["relative_pool_reference"]["sample_pool_count_mismatch_pairs"] == 1
    assert report["relative_pool_reference"]["sample_pool_count_difference"] == 1
    assert report["evaluation"]["factors"][0]["group_return_summary"][0][
        "mean_daily_relative_cross_sectional_return"
    ] == pytest.approx(-0.01)
    assert "假设值不是实际结算收益" in Path(first["report"]["markdown"]).read_text(encoding="utf-8")

    second = evaluate_sample_artifact(manifest_path)
    performance = json.loads(Path(second["performance"]).read_text(encoding="utf-8"))
    assert second["cache_hit"] is True
    assert performance["cache_hit"] is True
    assert performance["relative_pool_reference_count"] == 1
    assert hashlib.sha256(report_path.read_bytes()).hexdigest() == report_hash


def test_recover_manifest_accepts_complete_final_json_without_newline(tmp_path):
    row = _sample_row(
        "2025-01-02", "000001.SZ", 20, 10.0, "COMPLETE", 0.05,
    )
    raw = json.dumps(row, sort_keys=True, separators=(",", ":")).encode()
    path = tmp_path / "samples.jsonl"
    path.write_bytes(raw)

    result = recover_sample_manifest(
        path,
        expected_sha256=hashlib.sha256(raw).hexdigest(),
        expected_rows=1,
        expected_rows_by_horizon={"20": 1},
        identity={"run_id": "run-a"},
        dataset={"dataset_id": "dataset-a"},
        parameters={"horizons": [20]},
        factor_availability=[{"factor": "pe_ttm", "available": True}],
        report_context={},
        expected_label_coverage={
            "by_horizon": {
                "20": {
                    "complete": 1,
                    "incomplete": 0,
                    "incomplete_reason_counts": {},
                },
            },
        },
    )

    assert result["manifest"]["sample"]["final_newline"] is False
    assert result["manifest"]["signal_dates"] == ["2025-01-02"]
    assert (tmp_path / "sample_manifest.json").is_file()


def test_recover_manifest_rejects_wrong_recorded_hash(tmp_path):
    row = _sample_row("2025-01-02", "000001.SZ", 20, 10.0, "COMPLETE", 0.05)
    path = tmp_path / "samples.jsonl"
    path.write_text(json.dumps(row) + "\n", encoding="utf-8")

    with pytest.raises(ValueError, match="SHA-256"):
        recover_sample_manifest(
            path,
            expected_sha256="wrong-hash",
            expected_rows=1,
            expected_rows_by_horizon={"20": 1},
            identity={"run_id": "run-a"},
            dataset={},
            parameters={"horizons": [20]},
            factor_availability=[{"factor": "pe_ttm", "available": True}],
            report_context={},
        )


def test_sensitivity_direction_uses_the_same_affected_dates_as_baseline():
    sensitivity = [{
        "signal_date": "2025-01-02",
        "factor": "factor",
        "horizon": 20,
        "assumed_delist_total_return": -1.0,
        "n_delist_imputed": 1,
        "n_stocks": 6,
        "rank_ic": -0.1,
        "group_spread_q5_q1": -0.02,
    }]
    factor_summary = [{
        "factor": "factor",
        "horizon": 20,
        "mean_rank_ic": 0.3,
        "mean_group_spread_q5_q1": 0.04,
        "daily_series": [
            {
                "signal_date": "2025-01-02",
                "rank_ic": -0.2,
                "group_spread_q5_q1": -0.03,
                "n_stocks": 5,
            },
            {
                "signal_date": "2025-01-03",
                "rank_ic": 0.8,
                "group_spread_q5_q1": 0.11,
                "n_stocks": 5,
            },
        ],
    }]

    result = _summarize_sensitivity(sensitivity, factor_summary)[0]

    assert result["mean_baseline_rank_ic"] == pytest.approx(-0.2)
    assert result["mean_rank_ic"] == pytest.approx(-0.1)
    assert result["mean_rank_ic_delta"] == pytest.approx(0.1)
    assert result["rank_ic_direction_changed"] is False
    assert result["mean_baseline_group_spread_q5_q1"] == pytest.approx(-0.03)
    assert result["mean_group_spread_q5_q1"] == pytest.approx(-0.02)
    assert result["group_spread_direction_changed"] is False
    assert result["n_matched_dates"] == 1
    assert result["n_baseline_stock_date_pairs"] == 5
    assert result["n_scenario_stock_date_pairs"] == 6


def test_sensitivity_without_matched_valid_dates_does_not_report_direction():
    sensitivity = [{
        "signal_date": "2025-01-02",
        "factor": "factor",
        "horizon": 20,
        "assumed_delist_total_return": -0.5,
        "n_delist_imputed": 1,
        "n_stocks": 1,
        "rank_ic": None,
        "group_spread_q5_q1": None,
    }]
    factor_summary = [{
        "factor": "factor",
        "horizon": 20,
        "daily_series": [{
            "signal_date": "2025-01-02",
            "rank_ic": None,
            "group_spread_q5_q1": None,
            "n_stocks": 0,
        }],
    }]

    result = _summarize_sensitivity(sensitivity, factor_summary)[0]

    assert result["status"] == "NO_MATCHED_VALID_DATES"
    assert result["n_matched_dates"] == 0
    assert result["rank_ic_direction_changed"] is None
    assert result["group_spread_direction_changed"] is None
