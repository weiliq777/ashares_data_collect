import hashlib
import json

from data_collect.alpha_research.report import write_research_artifacts


def test_report_artifacts_are_repeatable_and_keep_stage1_gate_visible(tmp_path):
    payload = {
        "status": "EXPLORATORY_ONLY_STAGE1_GATES_UNRESOLVED",
        "research_validity_status": "NOT_FORMAL_FACTOR_EFFICACY_ASSESSMENT",
        "identity": {"run_id": "run-a", "dataset_id": "dataset-a", "input_hash": "abc"},
        "dataset": {"schema_versions": ["research-v2.1"], "snapshot_count": 4},
        "parameters": {"signal_start": "2024-01-02", "signal_end": "2024-02-01"},
        "quality": {
            "available_factor_count": 2,
            "total_factor_count": 10,
            "label_status_counts": {"COMPLETE": 8, "INCOMPLETE": 1},
        },
        "evaluation": {
            "method_notes": {"industry": "NOT AVAILABLE"},
            "factors": [{
                "factor": "pe_ttm", "horizon": 20, "n_dates": 4,
                "n_stocks": 6, "n_stock_date_pairs": 30, "mean_rank_ic": 0.1,
                "rank_ic_positive_rate": 0.75,
                "mean_group_spread_q5_q1": 0.02,
                "rank_ic_confidence_interval_95": None,
                "confidence_interval_status": "NOT AVAILABLE",
                "group_spread_confidence_interval_95": None,
                "group_spread_confidence_interval_status": "NOT AVAILABLE",
                "group_return_summary": [{
                    "group": 1, "n_valid_dates": 4, "mean_stocks_per_date": 10.0,
                    "mean_daily_cross_sectional_return": -0.01,
                    "mean_daily_cross_sectional_median_return": -0.02,
                }],
            }],
        },
        "strategy_incrementality": {"status": "NOT RUN_STAGE1_EVIDENCE_UNRESOLVED"},
    }
    samples = [{"signal_date": "2024-01-02", "ts_code": "000001.SZ", "label": {"status": "COMPLETE"}}]
    write_research_artifacts(tmp_path, "run-a", payload, samples)
    first_hashes = {
        name: hashlib.sha256((tmp_path / "run-a" / filename).read_bytes()).hexdigest()
        for name, filename in (("json", "report.json"), ("markdown", "report.md"), ("samples", "samples.jsonl"))
    }
    parsed = json.loads((tmp_path / "run-a" / "report.json").read_text(encoding="utf-8"))
    assert parsed["strategy_incrementality"]["status"] == "NOT RUN_STAGE1_EVIDENCE_UNRESOLVED"
    assert "不完整标签均保留状态" in (tmp_path / "run-a" / "report.md").read_text(encoding="utf-8")
    assert "## 五分组收益" in (tmp_path / "run-a" / "report.md").read_text(encoding="utf-8")
    sample_manifest = json.loads((tmp_path / "run-a" / "sample_manifest.json").read_text(encoding="utf-8"))
    assert sample_manifest["sample"]["sha256"] == first_hashes["samples"]
    assert sample_manifest["sample"]["row_count"] == 1
    assert sample_manifest["signal_dates"] == ["2024-01-02"]

    write_research_artifacts(tmp_path, "run-a", payload, samples)
    second_hashes = {
        name: hashlib.sha256((tmp_path / "run-a" / filename).read_bytes()).hexdigest()
        for name, filename in (("json", "report.json"), ("markdown", "report.md"), ("samples", "samples.jsonl"))
    }
    assert first_hashes == second_hashes
