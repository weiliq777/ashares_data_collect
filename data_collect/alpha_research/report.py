"""Stable JSON, Markdown, and JSONL artifacts for an Alpha research Run."""
from __future__ import annotations

from dataclasses import asdict, is_dataclass
from datetime import date, datetime
from decimal import Decimal
import json
import hashlib
import os
from pathlib import Path
import tempfile
from typing import Any, Iterable, Mapping


def _json_default(value: Any) -> Any:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if is_dataclass(value):
        return asdict(value)
    if isinstance(value, (set, frozenset)):
        return sorted(value)
    raise TypeError(f"unsupported JSON value: {type(value).__name__}")


def _atomic_write(path: Path, data: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_name: str | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", newline="\n", dir=path.parent,
            prefix=f".{path.name}.", suffix=".tmp", delete=False,
        ) as stream:
            temporary_name = stream.name
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary_name, path)
    finally:
        if temporary_name and os.path.exists(temporary_name):
            os.unlink(temporary_name)


class ResearchArtifactWriter:
    """Stream sample rows to a temporary JSONL before publishing a complete Run."""

    def __init__(self, output_dir: str | Path, run_id: str) -> None:
        self.run_dir = Path(output_dir) / run_id
        self.run_id = run_id
        self.run_dir.mkdir(parents=True, exist_ok=True)
        self.sample_path = self.run_dir / "samples.jsonl"
        self._sample_temp = tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", newline="\n", dir=self.run_dir,
            prefix=".samples.", suffix=".tmp", delete=False,
        )
        self._sample_digest = hashlib.sha256()
        self._sample_row_count = 0
        self._sample_rows_by_horizon: dict[str, int] = {}
        self._sample_dates: set[str] = set()
        self._finished = False

    def write_sample(self, row: Mapping[str, Any]) -> None:
        if self._finished:
            raise RuntimeError("research artifact writer is already finished")
        line = json.dumps(
            row, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
            default=_json_default, allow_nan=False,
        ) + "\n"
        self._sample_temp.write(line)
        self._sample_digest.update(line.encode("utf-8"))
        self._sample_row_count += 1
        horizon = row.get("horizon")
        if horizon is not None:
            key = str(horizon)
            self._sample_rows_by_horizon[key] = self._sample_rows_by_horizon.get(key, 0) + 1
        signal_date = row.get("signal_date")
        if signal_date is not None:
            self._sample_dates.add(str(signal_date)[:10])

    def finish(self, payload: Mapping[str, Any]) -> dict[str, str]:
        if self._finished:
            raise RuntimeError("research artifact writer is already finished")
        try:
            report_identity = payload.get("identity")
            if not isinstance(report_identity, Mapping) or report_identity.get("run_id") != self.run_id:
                raise ValueError("report identity run_id does not match the artifact directory")
            self._sample_temp.flush()
            os.fsync(self._sample_temp.fileno())
            self._sample_temp.close()
            os.replace(self._sample_temp.name, self.sample_path)
            json_text = json.dumps(
                payload, ensure_ascii=False, sort_keys=True, indent=2,
                default=_json_default, allow_nan=False,
            ) + "\n"
            _atomic_write(self.run_dir / "report.json", json_text)
            _atomic_write(self.run_dir / "report.md", _markdown(payload))
            sample_manifest = {
                "artifact_version": "factor-label-sample-v1",
                "identity": payload["identity"],
                "dataset": payload.get("dataset", {}),
                "parameters": payload.get("parameters", {}),
                "factor_availability": payload.get("factor_availability", []),
                "quality": payload.get("quality", {}),
                "strategy_incrementality": payload.get("strategy_incrementality", {}),
                "signal_dates": sorted(self._sample_dates),
                "sample": {
                    "path": self.sample_path.name,
                    "sha256": self._sample_digest.hexdigest(),
                    "size_bytes": self.sample_path.stat().st_size,
                    "row_count": self._sample_row_count,
                    "rows_by_horizon": dict(sorted(self._sample_rows_by_horizon.items())),
                    "final_newline": True,
                },
            }
            _atomic_write(
                self.run_dir / "sample_manifest.json",
                json.dumps(
                    sample_manifest, ensure_ascii=False, sort_keys=True, indent=2,
                    default=_json_default, allow_nan=False,
                ) + "\n",
            )
            self._finished = True
            return {
                "run_dir": str(self.run_dir.resolve()),
                "json": str((self.run_dir / "report.json").resolve()),
                "markdown": str((self.run_dir / "report.md").resolve()),
                "samples": str(self.sample_path.resolve()),
                "sample_manifest": str((self.run_dir / "sample_manifest.json").resolve()),
            }
        except Exception:
            self.abort()
            raise

    def abort(self) -> None:
        if not self._sample_temp.closed:
            self._sample_temp.close()
        if os.path.exists(self._sample_temp.name):
            os.unlink(self._sample_temp.name)
        self._finished = True


def _markdown(payload: Mapping[str, Any]) -> str:
    identity = payload["identity"]
    dataset = payload["dataset"]
    evaluations = payload["evaluation"]["factors"]
    lines = [
        "# Alpha 因子研究报告",
        "",
        f"- Run ID: `{identity['run_id']}`",
        f"- Dataset: `{identity['dataset_id']}`",
        f"- Dataset 输入哈希: `{identity['input_hash']}`",
        f"- 信号区间: {payload['parameters']['signal_start']} 至 {payload['parameters']['signal_end']}",
        f"- 评价实现版本: `{payload['evaluation'].get('evaluation_version', 'N/A')}`；样本产物原始版本保存在 JSON 的 `sample_artifact_parameters` 中",
        f"- Research schema: {', '.join(dataset['schema_versions']) or 'N/A'}",
        f"- 工程状态: **{payload['status']}**",
        f"- 因子有效性结论: **{payload.get('research_validity_status', 'NOT FORMAL')}**",
        "",
        "## 输入与标签质量",
        "",
        f"- Snapshot 成员数: {dataset['snapshot_count']}",
        f"- 因子可用性: {payload['quality']['available_factor_count']}/{payload['quality']['total_factor_count']}",
        f"- 标签状态计数: `{json.dumps(payload['quality']['label_status_counts'], ensure_ascii=False, sort_keys=True)}`",
        f"- 标签原因计数: `{json.dumps(payload['quality'].get('label_reason_counts', {}), ensure_ascii=False, sort_keys=True)}`",
        f"- 标签完整性: **{payload['quality'].get('outcome_completeness_status', 'UNKNOWN')}**",
        f"- 相对收益基准: {payload.get('relative_pool_reference', {}).get('reference_count', 0)} 个日期/窗口；样本行数与基准池总数不一致的组合 {payload.get('relative_pool_reference', {}).get('sample_pool_count_mismatch_pairs', 0)} 个，差额合计 {payload.get('relative_pool_reference', {}).get('sample_pool_count_difference', 0)} 条。完整标签数均已与基准记录核对。",
        f"- 退市证据: {payload['quality'].get('delist_evidence_status', 'UNKNOWN')}；未解决退市标签 {payload['quality'].get('unresolved_delist_label_count', 0)} 条",
        f"- 市场阶段覆盖: `{json.dumps(payload['quality'].get('market_stage_counts', {}), ensure_ascii=False, sort_keys=True)}`",
        f"- 行业控制: {payload['evaluation']['method_notes']['industry']}",
        f"- 市值控制输入: {payload['quality'].get('size_control_input', 'UNKNOWN')}",
        f"- 策略增量评估: **{payload['strategy_incrementality']['status']}**",
        "",
        "## 因子字段质量",
        "",
        "有效率按信号日横截面观测计算；共享增长质量状态为 `INSUFFICIENT_HISTORY` 但本因子值有限时，保留该因子并将另一增长指标不足记入原因计数。",
        "",
        "| 因子 | 有效值 | 观测数 | 缺失率 | 状态计数 | 原因计数 |",
        "|---|---:|---:|---:|---|---|",
    ]
    for factor, item in sorted(payload["quality"].get("factor_quality", {}).items()):
        missing_ratio = item.get("missing_ratio")
        lines.append(
            "| {factor} | {valid} | {total} | {missing} | `{statuses}` | `{reasons}` |".format(
                factor=factor,
                valid=item.get("valid_count", 0),
                total=item.get("observations", 0),
                missing="N/A" if missing_ratio is None else f"{missing_ratio:.1%}",
                statuses=json.dumps(item.get("status_counts", {}), ensure_ascii=False, sort_keys=True),
                reasons=json.dumps(item.get("reason_counts", {}), ensure_ascii=False, sort_keys=True),
            )
        )
    lines.extend([
        "",
        "## 因子结果",
        "",
        "| 因子 | 窗口 | 有效 IC 日期 | 唯一股票 | 因子有效股票日 | 有效收益配对 | 因子缺失率 | 配对缺失率 | 平均 Rank IC | IC 正值占比 | Q5−Q1 均值 | Rank IC 95% 区间 | Q5−Q1 95% 区间 |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---|",
    ])
    for item in evaluations:
        interval = item.get("rank_ic_confidence_interval_95")
        interval_text = (
            f"[{interval[0]:.4f}, {interval[1]:.4f}]" if interval else item["confidence_interval_status"]
        )
        spread_interval = item.get("group_spread_confidence_interval_95")
        spread_interval_text = (
            f"[{spread_interval[0]:.4f}, {spread_interval[1]:.4f}]"
            if spread_interval else item["group_spread_confidence_interval_status"]
        )
        lines.append(
            "| {factor} | {horizon} | {dates} | {stocks} | {factor_pairs} | {pairs} | {factor_missing} | {pair_missing} | {ic} | {positive} | {spread} | {interval} | {spread_interval} |".format(
                factor=item["factor"], horizon=item["horizon"], dates=item.get("n_valid_dates", item["n_dates"]),
                stocks="N/A" if item.get("n_stocks") is None else item["n_stocks"],
                factor_pairs=item.get("n_factor_valid_stock_date_pairs", "N/A"),
                pairs=item.get("n_stock_date_pairs", "N/A"),
                factor_missing="N/A" if item.get("factor_missing_ratio") is None else f"{item['factor_missing_ratio']:.1%}",
                pair_missing="N/A" if item.get("mean_missing_ratio") is None else f"{item['mean_missing_ratio']:.1%}",
                ic="N/A" if item.get("mean_rank_ic") is None else f"{item['mean_rank_ic']:.4f}",
                positive="N/A" if item.get("rank_ic_positive_rate") is None else f"{item['rank_ic_positive_rate']:.1%}",
                spread="N/A" if item.get("mean_group_spread_q5_q1") is None else f"{item['mean_group_spread_q5_q1']:.4f}",
                interval=interval_text,
                spread_interval=spread_interval_text,
            )
        )
    if any(item.get("group_return_summary") for item in evaluations):
        lines.extend([
            "",
            "## 五分组收益",
            "",
            "每个信号日的五组横截面均值等权汇总；相对池超额为该组收益减去同日、同窗口完整标签股票池等权均值。Rank IC 与 Q5−Q1 对该公共基准平移不变；这些统计不等同于持仓组合的复利收益。",
            "",
            "| 因子 | 窗口 | 组别 | 有效日期 | 平均股票数 | 日均绝对收益 | 日均相对池超额 | 日均绝对中位数 | 日均相对中位数 |",
            "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
        ])
        for item in evaluations:
            for group in item.get("group_return_summary", []):
                lines.append(
                    "| {factor} | {horizon} | Q{group} | {dates} | {stocks} | {mean_return} | {relative_return} | {median_return} | {relative_median} |".format(
                        factor=item["factor"], horizon=item["horizon"], group=group["group"],
                        dates=group["n_valid_dates"],
                        stocks="N/A" if group["mean_stocks_per_date"] is None else f"{group['mean_stocks_per_date']:.1f}",
                        mean_return="N/A" if group["mean_daily_cross_sectional_return"] is None else f"{group['mean_daily_cross_sectional_return']:.4f}",
                        relative_return="N/A" if group.get("mean_daily_relative_cross_sectional_return") is None else f"{group['mean_daily_relative_cross_sectional_return']:.4f}",
                        median_return="N/A" if group["mean_daily_cross_sectional_median_return"] is None else f"{group['mean_daily_cross_sectional_median_return']:.4f}",
                        relative_median="N/A" if group.get("mean_daily_relative_cross_sectional_median_return") is None else f"{group['mean_daily_relative_cross_sectional_median_return']:.4f}",
                    )
                )
    if any(item.get("factor_exclusion_reasons") for item in evaluations):
        lines.extend([
            "",
            "## 因子缺失与无效原因",
            "",
            "| 因子 | 窗口 | 因子有效股票日 | 因子缺失/无效原因计数 |",
            "|---|---:|---:|---|",
        ])
        for item in evaluations:
            if item.get("factor_exclusion_reasons"):
                lines.append(
                    "| {factor} | {horizon} | {valid_pairs} | `{reasons}` |".format(
                        factor=item["factor"], horizon=item["horizon"],
                        valid_pairs=item.get("n_factor_valid_stock_date_pairs", 0),
                        reasons=json.dumps(item["factor_exclusion_reasons"], ensure_ascii=False, sort_keys=True),
                    )
                )
    if any(item.get("annual_stability") for item in evaluations):
        lines.extend([
            "",
            "## 年度稳定性",
            "",
            "| 因子 | 窗口 | 年份 | 有效日期 | 平均 Rank IC | IC 正值占比 | 平均 Q5−Q1 |",
            "|---|---:|---:|---:|---:|---:|---:|",
        ])
        for item in evaluations:
            for annual in item.get("annual_stability", []):
                lines.append(
                    "| {factor} | {horizon} | {year} | {dates} | {ic} | {positive} | {spread} |".format(
                        factor=item["factor"], horizon=item["horizon"], year=annual["year"],
                        dates=annual["n_valid_dates"],
                        ic="N/A" if annual["rank_ic_mean"] is None else f"{annual['rank_ic_mean']:.4f}",
                        positive="N/A" if annual["rank_ic_positive_rate"] is None else f"{annual['rank_ic_positive_rate']:.1%}",
                        spread="N/A" if annual["group_spread_mean"] is None else f"{annual['group_spread_mean']:.4f}",
                    )
                )
    if any(item.get("market_regime_stability") for item in evaluations):
        lines.extend([
            "",
            "## 市场阶段稳定性",
            "",
            "| 因子 | 窗口 | 市场阶段 | 有效日期 | 平均 Rank IC | IC 正值占比 | 平均 Q5−Q1 |",
            "|---|---:|---|---:|---:|---:|---:|",
        ])
        for item in evaluations:
            for regime in item.get("market_regime_stability", []):
                lines.append(
                    "| {factor} | {horizon} | {stage} | {dates} | {ic} | {positive} | {spread} |".format(
                        factor=item["factor"], horizon=item["horizon"], stage=regime["market_stage"],
                        dates=regime["n_valid_dates"],
                        ic="N/A" if regime["mean_rank_ic"] is None else f"{regime['mean_rank_ic']:.4f}",
                        positive="N/A" if regime["rank_ic_positive_rate"] is None else f"{regime['rank_ic_positive_rate']:.1%}",
                        spread="N/A" if regime["mean_group_spread_q5_q1"] is None else f"{regime['mean_group_spread_q5_q1']:.4f}",
                    )
                )
    sensitivity = payload.get("delist_sensitivity")
    if sensitivity:
        def direction_label(value: bool | None) -> str:
            if value is None:
                return "N/A"
            return "是" if value else "否"

        lines.extend([
            "",
            "## 未解决退市标签敏感性",
            "",
            "以下为假设收益情景，仅用于检验结论对未解决退市样本的敏感程度；假设值不是实际结算收益。基准与情景按相同信号日期、因子和窗口配对。基准只含完整标签，情景在这些完整标签上增加假设退市样本，因此同时列出两侧股票—日期样本数。",
            "",
            "| 因子 | 窗口 | 假设退市收益 | 加入样本数 | 匹配日期 | 基准/情景股票日数 | 基准/情景 Rank IC | ΔIC | IC 方向变化 | 基准/情景 Q5−Q1 | 分组方向变化 | 状态 |",
            "|---|---:|---:|---:|---:|---:|---:|---:|---|---:|---|---|",
        ])
        for item in sensitivity:
            baseline_ic = item.get("mean_baseline_rank_ic")
            scenario_ic = item.get("mean_rank_ic")
            ic_pair = (
                f"{baseline_ic:.4f} / {scenario_ic:.4f}"
                if baseline_ic is not None and scenario_ic is not None else "N/A"
            )
            baseline_spread = item.get("mean_baseline_group_spread_q5_q1")
            scenario_spread = item.get("mean_group_spread_q5_q1")
            spread_pair = (
                f"{baseline_spread:.4f} / {scenario_spread:.4f}"
                if baseline_spread is not None and scenario_spread is not None else "N/A"
            )
            lines.append(
                "| {factor} | {horizon} | {assumption:.0%} | {imputed} | {dates} | {pairs} | {ic_pair} | {ic_delta} | {ic_flip} | {spread_pair} | {spread_flip} | {status} |".format(
                    factor=item["factor"], horizon=item["horizon"],
                    assumption=item["assumed_delist_total_return"],
                    imputed=item["n_delist_imputed"], dates=item["n_matched_dates"],
                    pairs=f"{item['n_baseline_stock_date_pairs']}/{item['n_scenario_stock_date_pairs']}",
                    ic_pair=ic_pair,
                    ic_delta="N/A" if item["mean_rank_ic_delta"] is None else f"{item['mean_rank_ic_delta']:.4f}",
                    ic_flip=direction_label(item["rank_ic_direction_changed"]),
                    spread_pair=spread_pair,
                    spread_flip=direction_label(item["group_spread_direction_changed"]),
                    status=item["status"],
                )
            )
    lines.extend([
        "",
        "## 解释边界",
        "",
        "信号预测标签以信号日收盘为基准；相对收益是减去同信号日、同窗口的完整股票池等权平均收益。Rank IC 与 Q5−Q1 的横截面排序/差值对这个共同基准不变；五分组同时列绝对收益与相对股票池超额收益。T+1 开盘参考收益不含费用，也不代表真实成交。统计只使用完整标签；所有不完整标签均保留状态并按原因计数。退市结算/退出价值不在当前冻结 Dataset 中，因此退市窗口标为未解决且不能用于正式因子有效性结论。行业 PIT 暴露不可用；正式策略收益增量尚未由回测引擎验收。",
        "",
    ])
    return "\n".join(lines)


def write_research_artifacts(
    output_dir: str | Path,
    run_id: str,
    payload: Mapping[str, Any],
    sample_rows: Iterable[Mapping[str, Any]],
) -> dict[str, str]:
    """Write content-addressed artifacts atomically and return their paths."""
    writer = ResearchArtifactWriter(output_dir, run_id)
    try:
        for row in sample_rows:
            writer.write_sample(row)
        return writer.finish(payload)
    except Exception:
        writer.abort()
        raise


def write_evaluation_report(
    output_dir: str | Path,
    payload: Mapping[str, Any],
    *,
    report_dir: str | Path | None = None,
) -> dict[str, str]:
    """Atomically publish a report derived from a verified sample artifact."""
    run_id = str((payload.get("identity") or {}).get("run_id") or "").strip()
    if not run_id:
        raise ValueError("evaluation report requires identity.run_id")
    run_dir = Path(report_dir) if report_dir is not None else Path(output_dir) / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    json_path = run_dir / "report.json"
    markdown_path = run_dir / "report.md"
    _atomic_write(
        json_path,
        json.dumps(
            payload, ensure_ascii=False, sort_keys=True, indent=2,
            default=_json_default, allow_nan=False,
        ) + "\n",
    )
    _atomic_write(markdown_path, _markdown(payload))
    return {"json": str(json_path.resolve()), "markdown": str(markdown_path.resolve())}


__all__ = ["ResearchArtifactWriter", "write_evaluation_report", "write_research_artifacts"]
