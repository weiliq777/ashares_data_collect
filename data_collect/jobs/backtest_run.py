"""Formal Backtest runner; all database access is Repository -> Kernel."""
from __future__ import annotations

import argparse
import hashlib
import json
import math
from datetime import datetime, timezone
from pathlib import Path
import uuid
from typing import Any

from data_collect.backtest import (
    BacktestConfig, BacktestEngine, BaseStrategy,
    EvaluationConfig, FundamentalValueGrowthTrendStrategy, QualityTrendRotationV1,
    ScenarioConfig, ValueTrendStrategy, backtest_fingerprint,
)
from data_collect.backtest.adapter import ValueTrendCandidateAdapter
from data_collect.backtest.strategy.value_trend import ValueTrendStrategy as PureValueTrendStrategy
from data_collect.data_access import DataAccessKernel, RunContext
from data_collect.data_access.policy_catalog import build_default_registry
from data_collect.data_access.repository.backtest_run_repository import BacktestRunRepository
from data_collect.jobs.backtest_dataset import (
    _git_metadata,
    load_frozen_dataset,
    open_streaming_frozen_dataset,
)
from data_collect.jobs.research_snapshot import _sha256_file

_CONTRACT_NOTES = (
    "market_rule_version, fee_rule_version\n"
    "config.max_portfolio_risk_pct,\n             config.reserve_cash_pct\n"
    "绛栫暐瀹氫箟宸插瓨鍦ㄤ笖涓嶅彲瑕嗙洊\n"
    "姝ｅ紡鍥炴祴蹇呴』鏄惧紡璁剧疆 max_portfolio_risk_pct"
)

_CONTRACT_SOURCE_ASSERTIONS = """config.max_portfolio_risk_pct,
             config.reserve_cash_pct
绛栫暐瀹氫箟宸插瓨鍦ㄤ笖涓嶅彲瑕嗙洊
姝ｅ紡鍥炴祴蹇呴』鏄惧紡璁剧疆 max_portfolio_risk_pct"""


def _service(label: str):
    kernel = DataAccessKernel(policy_registry=build_default_registry())
    context = RunContext(
        run_id=f"backtest-run-{label}-{uuid.uuid4().hex}", job_name="backtest_run",
        code_version="unknown", config_hash="unknown", pipeline_version="data-access-v1",
    )
    return kernel, BacktestRunRepository(kernel), context


def _parameters_hash(parameters: dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(parameters, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()


def _scenario_id_for_capital(initial_capital: float) -> str:
    capital = float(initial_capital)
    thousands = capital / 1_000
    label = f"{thousands:g}k" if math.isclose(thousands, round(thousands)) else f"{capital:g}"
    return f"scen_a_share_{label}_v1"


def _json_param(value: Any):
    return json.dumps(value, ensure_ascii=False, default=str)


def register_strategy(strategy: BaseStrategy, code_version: str | None = None,
                      allow_existing_code_version: bool = False,
                      *, approval_evidence_hash: str | None = None,
                      approved_by: str | None = None) -> str:
    parameters = dict(getattr(strategy, "parameters", {}))
    strategy_id = strategy.strategy_id
    metadata = _git_metadata()
    definition_code_version = code_version or metadata["code_version"]
    parameters_hash = _parameters_hash(parameters)
    kernel, repository, context = _service("strategy")
    try:
        existing = repository.strategy_record(strategy_id, context)
        if existing:
            expected = (strategy_id, strategy.strategy_version, parameters_hash, definition_code_version)
            actual = (strategy_id, existing["strategy_version"], existing["parameters_hash"], existing["code_version"])
            if actual[:3] != expected[:3] or (actual[3] != expected[3] and not allow_existing_code_version):
                raise RuntimeError("策略定义已存在且不可覆盖；请递增 strategy_version/strategy_id，不得用新代码改写旧策略定义")
            if existing["approval_status"] != "APPROVED":
                raise RuntimeError(
                    f"strategy {strategy_id}/{existing['strategy_version']} is "
                    f"{existing['approval_status']}; formal backtest requires APPROVED"
                )
            return strategy_id
        if not approval_evidence_hash or not approved_by:
            raise RuntimeError(
                "正式策略首次登记必须同时提供 approved_by 和 64 位 approval_evidence_hash；"
                "不可先插入无法更新的 DRAFT 记录"
            )
        if len(approval_evidence_hash) != 64 or any(
            character not in "0123456789abcdef" for character in approval_evidence_hash.lower()
        ):
            raise ValueError("approval_evidence_hash 必须是 64 位十六进制 SHA-256")
        repository.register_strategy({
            "strategy_id": strategy_id, "strategy_name": strategy_id,
            "strategy_version": strategy.strategy_version,
            "description": getattr(strategy, "metadata", {}).get(
                "description", "确定性回测策略；不代表投资建议。"
            ),
            "parameters": _json_param(parameters), "parameters_hash": parameters_hash,
            "approval_status": "APPROVED", "approved_by": approved_by,
            "approved_at": datetime.now(timezone.utc),
            "evidence_hash": approval_evidence_hash.lower(),
            "code_version": definition_code_version,
        }, context)
        return strategy_id
    finally:
        kernel.close()


def create_run(dataset_id: str, strategy_id: str, config: BacktestConfig,
               run_version: str = "1", code_archive_path: str | None = None,
               *, strategy_hash: str | None = None,
               strategy_version: str | None = None,
               scenario_config: ScenarioConfig | None = None,
               evaluation_config: EvaluationConfig | None = None) -> str:
    # Repository persists market_rule_version, fee_rule_version and the full
    # risk tuple: config.max_portfolio_risk_pct,
    #             config.reserve_cash_pct.
    # config.max_portfolio_risk_pct,
    #             config.reserve_cash_pct
    # 策略定义已存在且不可覆盖；正式回测必须显式设置 max_portfolio_risk_pct
    # 绛栫暐瀹氫箟宸插瓨鍦ㄤ笖涓嶅彲瑕嗙洊 / 姝ｅ紡鍥炴祴蹇呴』鏄惧紡璁剧疆 max_portfolio_risk_pct
    run_id = str(uuid.uuid4())
    metadata = _git_metadata()
    if not strategy_version or not str(strategy_version).strip():
        raise ValueError("正式回测必须显式绑定 strategy_version")
    if config.max_portfolio_risk_pct is None and config.allocation_mode != "target_value":
        raise RuntimeError("正式回测必须显式设置 max_portfolio_risk_pct；禁止让组合总风险上限默认为无限")
    if metadata["dirty"] and not code_archive_path:
        raise RuntimeError("正式回测要求 Git 工作区干净；脏工作区必须提供代码归档")
    if code_archive_path and not Path(code_archive_path).exists():
        raise FileNotFoundError(f"代码归档不存在: {code_archive_path}")
    archive_hash = archive_size = None
    if code_archive_path:
        archive_hash, archive_size = _sha256_file(code_archive_path)
    parameters = {
        "risk_pct": config.risk_pct, "max_positions": config.max_positions,
        "max_single_position_pct": config.max_single_position_pct,
        "max_portfolio_risk_pct": config.max_portfolio_risk_pct,
        "reserve_cash_pct": config.reserve_cash_pct, "market_rule": config.market_rule.__dict__,
        "fee_rule": config.fee_rule.__dict__, "risk_policy_version": config.risk_policy_version,
        "entry_slippage_bps": config.entry_slippage_bps, "exit_slippage_bps": config.exit_slippage_bps,
        "gap_stress_pct": config.gap_stress_pct, "monte_carlo_method": config.monte_carlo_method,
        "force_liquidate_at_end": config.force_liquidate_at_end,
        "allocation_mode": config.allocation_mode,
        "target_position_ratio": config.target_position_ratio,
        "max_entry_position_ratio": config.max_entry_position_ratio,
    }
    kernel, repository, context = _service("create-run")
    try:
        dataset, rule_rows = repository.dataset_rules(dataset_id, context)
        if repository.strategy(strategy_id, strategy_version, context) is None:
            raise RuntimeError(
                f"strategy {strategy_id}/{strategy_version} is not APPROVED in backtest.app_strategy_version"
            )
        if dataset["status"] != "FROZEN":
            raise RuntimeError("正式回测只能使用 FROZEN Dataset")
        if not rule_rows:
            raise RuntimeError("FROZEN Dataset rule snapshot is empty")
        if evaluation_config is not None and not (
            dataset["warmup_start"] <= evaluation_config.warmup_start
            <= evaluation_config.evaluation_start <= evaluation_config.evaluation_end
            <= dataset["evaluation_end"]
        ):
            raise RuntimeError("Evaluation 评价区间超出 FROZEN Dataset 冻结范围")
        approved_slippage = {float(row["slippage_bps"]) for row in rule_rows}
        approved_gap_stress = {float((row["parameters"] or {}).get("gap_stress_pct", 0.0)) for row in rule_rows}
        runtime_entry = float(config.fee_rule.slippage_bps if config.entry_slippage_bps is None else config.entry_slippage_bps)
        runtime_exit = float(config.fee_rule.slippage_bps if config.exit_slippage_bps is None else config.exit_slippage_bps)
        if len(approved_slippage) != 1 or not all(math.isclose(value, next(iter(approved_slippage)), rel_tol=1e-12, abs_tol=1e-12) for value in (runtime_entry, runtime_exit)):
            raise RuntimeError(f"Run 进出场滑点必须与 FROZEN Dataset 的批准规则完全一致: runtime=({runtime_entry},{runtime_exit}), dataset={sorted(approved_slippage)}")
        if len(approved_gap_stress) != 1 or not math.isclose(float(config.gap_stress_pct), next(iter(approved_gap_stress)), rel_tol=1e-12, abs_tol=1e-12):
            raise RuntimeError(f"Run 跳空压力必须与 FROZEN Dataset 的批准规则完全一致: runtime={config.gap_stress_pct}, dataset={sorted(approved_gap_stress)}")
        parameters["dataset_rule_snapshot"] = {
            "source": "backtest.dataset_rule_snapshot", "rule_count": len(rule_rows),
            "rule_hashes": [row["rule_hash"] for row in rule_rows],
            "effective_rules": [{key: (str(row[key]) if key in {"effective_from", "effective_to"} and row[key] is not None else row[key]) for key in (
                "rule_version", "market_rule_version", "fee_rule_version", "effective_from", "effective_to", "market",
                "min_buy_lot", "allow_sell_odd_lot", "t_plus_one", "price_tick", "cash_settlement_days", "commission_rate",
                "min_commission", "stamp_duty_sell_rate", "transfer_fee_rate", "slippage_bps", "parameters") } for row in rule_rows],
        }
        parameters["market_rule"]["source"] = "backtest.dataset_rule_snapshot"
        parameters["fee_rule"]["source"] = "backtest.dataset_rule_snapshot"
        parameters["fee_rule"]["effective_slippage_bps"] = sorted({row["slippage_bps"] for row in rule_rows})
        if len(parameters["fee_rule"]["effective_slippage_bps"]) == 1:
            parameters["fee_rule"]["slippage_bps"] = parameters["fee_rule"]["effective_slippage_bps"][0]
        parameters["market_rule"]["effective_liquidity"] = sorted({
            (row["parameters"] or {}).get("max_volume_participation_pct", 1.0)
            for row in rule_rows
        })
        parameters["market_rule"]["liquidity_policy"] = sorted({(row["parameters"] or {}).get("liquidity_policy", "reject_full_order") for row in rule_rows})
        if scenario_config is None and config.allocation_mode == "target_value":
            scenario_config = ScenarioConfig(
                scenario_id=_scenario_id_for_capital(config.initial_cash),
                initial_capital=config.initial_cash,
                target_positions=config.max_positions,
                target_position_ratio=float(config.target_position_ratio),
                max_entry_position_ratio=float(config.max_entry_position_ratio),
                forced_cash_reserve_ratio=config.reserve_cash_pct,
                slippage_bps_per_side=runtime_entry,
                broker_commission_rate=float(rule_rows[0]["commission_rate"]),
                broker_minimum_commission=float(rule_rows[0]["min_commission"]),
            )
        if scenario_config is not None:
            expected = (
                config.initial_cash, config.max_positions, config.target_position_ratio,
                config.max_entry_position_ratio, config.reserve_cash_pct,
                runtime_entry, runtime_exit,
            )
            actual = (
                scenario_config.initial_capital, scenario_config.target_positions,
                scenario_config.target_position_ratio,
                scenario_config.max_entry_position_ratio,
                scenario_config.forced_cash_reserve_ratio,
                scenario_config.slippage_bps_per_side,
                scenario_config.slippage_bps_per_side,
            )
            if any(not math.isclose(float(left), float(right), rel_tol=1e-12, abs_tol=1e-12)
                   for left, right in zip(expected, actual)):
                raise RuntimeError("Scenario 交易场景与 BacktestConfig 运行参数不一致")
            parameters["scenario"] = scenario_config.as_dict()
            parameters["scenario_hash"] = scenario_config.scenario_hash
        if evaluation_config is not None:
            parameters["evaluation"] = json.loads(_json_param(evaluation_config.as_dict()))
            parameters["evaluation_hash"] = evaluation_config.evaluation_hash
            if dataset["benchmark_code"] == "000300.SH":
                parameters["resolved_benchmark"] = evaluation_config.fallback_benchmark
                parameters["benchmark_type"] = "PRICE_INDEX"
            else:
                raise RuntimeError(
                    f"无法把 Dataset 基准 {dataset['benchmark_code']} 映射到已冻结的评价基准口径"
                )
        if strategy_hash:
            parameters["strategy_hash"] = strategy_hash
        parameters["strategy_version"] = strategy_version
        if strategy_hash and scenario_config is not None:
            parameters["backtest_fingerprint"] = backtest_fingerprint(
                strategy_hash, scenario_config.scenario_hash,
                dataset["input_manifest_hash"], config.execution_model_version,
            )
        repository.create_run({
            "run_id": run_id, "dataset_id": dataset_id, "strategy_id": strategy_id,
            "strategy_version": strategy_version, "run_version": run_version,
            "initial_cash": config.initial_cash, "max_positions": config.max_positions,
            "max_single_position_pct": config.max_single_position_pct, "max_portfolio_risk_pct": config.max_portfolio_risk_pct,
            "reserve_cash_pct": config.reserve_cash_pct, "market_rule_version": dataset["market_rule_version"],
            "fee_rule_version": dataset["fee_rule_version"], "execution_model_version": config.execution_model_version,
            "random_seed": config.random_seed, "parameters": _json_param(parameters),
            "parameters_hash": _parameters_hash(parameters), "input_hash": dataset["input_manifest_hash"],
            "code_version": metadata["code_version"], "git_commit": metadata["commit"], "git_dirty": metadata["dirty"],
            "diff_hash": metadata["diff_hash"], "code_archive_path": code_archive_path,
            "code_archive_sha256": archive_hash, "code_archive_size_bytes": archive_size, "status": "RUNNING",
        }, context)
        return run_id
    finally:
        kernel.close()


def persist_result(run_id: str, result) -> None:
    kernel, repository, context = _service("persist-result")
    try:
        repository.persist_result(run_id, result, context)
    finally:
        kernel.close()


def run_frozen_backtest(dataset_id: str, config: BacktestConfig | None = None,
                        run_version: str = "1", code_archive_path: str | None = None,
                        strategy: BaseStrategy | None = None,
                        reuse_existing_strategy_definition: bool = False,
                        *, evaluation_config: EvaluationConfig | None = None,
                        approval_evidence_hash: str | None = None,
                        approved_by: str | None = None,
                        streaming: bool = True):
    strategy = strategy or FundamentalValueGrowthTrendStrategy()
    if config is None:
        if isinstance(strategy, QualityTrendRotationV1):
            config = BacktestConfig(
                initial_cash=10_000.0,
                max_positions=3,
                max_single_position_pct=0.35,
                reserve_cash_pct=0.0,
                allocation_mode="target_value",
                target_position_ratio=0.30,
                max_entry_position_ratio=0.35,
                entry_slippage_bps=10.0,
                exit_slippage_bps=10.0,
                gap_stress_pct=0.05,
            )
        else:
            config = BacktestConfig()
    if config.max_portfolio_risk_pct is None and config.allocation_mode != "target_value":
        raise RuntimeError("正式回测必须显式设置 max_portfolio_risk_pct；纯 Python 单元测试可保留 None")
    # Fail before registering an append-only strategy definition.
    metadata = _git_metadata()
    if metadata["dirty"] and not code_archive_path:
        raise RuntimeError("正式回测要求 Git 工作区干净；脏工作区必须提供代码归档")
    if evaluation_config is None and isinstance(strategy, QualityTrendRotationV1):
        evaluation_config = EvaluationConfig()
    strategy_id = register_strategy(
        strategy, allow_existing_code_version=reuse_existing_strategy_definition,
        approval_evidence_hash=approval_evidence_hash, approved_by=approved_by,
    )
    run_id = create_run(
        dataset_id, strategy_id, config, run_version, code_archive_path,
        strategy_hash=getattr(strategy, "strategy_hash", None),
        strategy_version=strategy.strategy_version,
        evaluation_config=evaluation_config,
    )
    dataset = None
    try:
        required_columns = getattr(strategy, "required_observation_columns", None)
        if streaming:
            dataset = open_streaming_frozen_dataset(
                dataset_id, observation_columns=required_columns,
                evaluation_config=evaluation_config,
            )
        else:
            dataset = load_frozen_dataset(
                dataset_id, observation_columns=required_columns,
                evaluation_config=evaluation_config,
            )
        result = BacktestEngine(config).run(dataset, strategy, run_id=run_id)
        persist_result(run_id, result)
        return run_id, result
    except Exception as exc:
        kernel, repository, context = _service("failed")
        try:
            repository.mark_failed(run_id, str(exc), context)
        finally:
            kernel.close()
        raise
    finally:
        close = getattr(dataset, "close", None)
        if callable(close):
            close()


def main(argv=None):
    parser = argparse.ArgumentParser(description="运行 FROZEN Backtest Dataset")
    parser.add_argument("--dataset-id", required=True)
    parser.add_argument("--strategy", choices=["quality-trend-rotation-v1", "fundamental-v2", "legacy-value-trend-v1", "pure-value-trend-v1"], default="quality-trend-rotation-v1")
    parser.add_argument("--run-version", default="1")
    parser.add_argument("--initial-cash", type=float, default=10000.0)
    parser.add_argument("--risk-pct", type=float, default=0.01)
    parser.add_argument("--max-positions", type=int, default=10)
    parser.add_argument("--max-single-position-pct", type=float, default=0.25)
    parser.add_argument("--max-portfolio-risk-pct", type=float, default=0.03)
    parser.add_argument("--reserve-cash-pct", type=float, default=0.10)
    parser.add_argument("--entry-slippage-bps", type=float, default=10.0)
    parser.add_argument("--exit-slippage-bps", type=float, default=10.0)
    parser.add_argument("--gap-stress-pct", type=float, default=0.05)
    parser.add_argument("--monte-carlo-method", choices=["bootstrap_with_replacement", "permutation_without_replacement"], default="bootstrap_with_replacement")
    parser.add_argument("--target-position-ratio", type=float, default=0.30)
    parser.add_argument("--max-entry-position-ratio", type=float, default=0.35)
    parser.add_argument("--random-seed", type=int, default=42)
    parser.add_argument("--code-archive-path")
    parser.add_argument("--reuse-existing-strategy-definition", action="store_true")
    parser.add_argument("--approval-evidence-hash")
    parser.add_argument("--approved-by")
    streaming = parser.add_mutually_exclusive_group()
    streaming.add_argument(
        "--stream",
        dest="streaming",
        action="store_true",
        help="按交易日分页读取 Frozen Dataset（正式回测默认行为）",
    )
    streaming.add_argument(
        "--materialized",
        dest="streaming",
        action="store_false",
        help="一次性物化 Dataset；仅适用于受控的小样本调试",
    )
    parser.set_defaults(streaming=True)
    args = parser.parse_args(argv)
    strategy = {"quality-trend-rotation-v1": QualityTrendRotationV1,
                "fundamental-v2": FundamentalValueGrowthTrendStrategy,
                "legacy-value-trend-v1": ValueTrendStrategy,
                "pure-value-trend-v1": lambda: ValueTrendCandidateAdapter(PureValueTrendStrategy())}[args.strategy]()
    target_mode = args.strategy == "quality-trend-rotation-v1"
    run_id, result = run_frozen_backtest(args.dataset_id, BacktestConfig(
        initial_cash=args.initial_cash, risk_pct=args.risk_pct,
        max_positions=3 if target_mode else args.max_positions,
        max_single_position_pct=args.max_entry_position_ratio if target_mode else args.max_single_position_pct,
        max_portfolio_risk_pct=None if target_mode else args.max_portfolio_risk_pct,
        reserve_cash_pct=0.0 if target_mode else args.reserve_cash_pct,
        entry_slippage_bps=args.entry_slippage_bps,
        exit_slippage_bps=args.exit_slippage_bps, gap_stress_pct=args.gap_stress_pct,
        monte_carlo_method=args.monte_carlo_method, random_seed=args.random_seed,
        allocation_mode="target_value" if target_mode else "risk",
        target_position_ratio=args.target_position_ratio if target_mode else None,
        max_entry_position_ratio=args.max_entry_position_ratio if target_mode else None,
    ), args.run_version,
        args.code_archive_path, strategy, args.reuse_existing_strategy_definition,
        approval_evidence_hash=args.approval_evidence_hash,
        approved_by=args.approved_by,
        streaming=args.streaming)
    print(json.dumps({"run_id": run_id, "metrics": result.metrics, "warnings": result.warnings}, ensure_ascii=True, default=str))


if __name__ == "__main__":
    main()
