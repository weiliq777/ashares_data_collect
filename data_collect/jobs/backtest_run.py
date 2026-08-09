"""正式 Backtest 运行器。

运行器只接受 ``backtest.dataset_manifest.status='FROZEN'``，把纯 Python 引擎产生的
信号、订单、成交、持仓、现金和绩效写入同一个 RUNNING Run；成功后才转换为
SUCCEEDED。失败会标记 FAILED，避免把半成品当作正式结果。
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
from pathlib import Path
import uuid
from typing import Any

from data_collect.backtest import (
    BacktestConfig, BacktestEngine, BaseStrategy,
    FundamentalValueGrowthTrendStrategy, ValueTrendStrategy,
)
from data_collect.backtest.strategy.value_trend import ValueTrendStrategy as PureValueTrendStrategy
from data_collect.backtest.adapter import ValueTrendCandidateAdapter
from data_collect.jobs.backtest_dataset import _git_metadata, _json_param, load_frozen_dataset
from data_collect.jobs.research_snapshot import _sha256_file
from data_collect.utils.db import get_connection, require_psycopg2


def _parameters_hash(parameters: dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(parameters, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()


def register_strategy(
    strategy: BaseStrategy,
    code_version: str | None = None,
    allow_existing_code_version: bool = False,
) -> str:
    parameters = dict(getattr(strategy, "parameters", {}))
    strategy_id = strategy.strategy_id
    metadata = _git_metadata()
    definition_code_version = code_version or metadata["code_version"]
    parameters_hash = _parameters_hash(parameters)
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """SELECT strategy_name, strategy_version, parameters_hash, code_version
                 FROM backtest.strategy_definition
                WHERE strategy_id=%s""",
            (strategy_id,),
        )
        existing = cur.fetchone()
        if existing:
            expected = (strategy_id, strategy.strategy_version, parameters_hash, definition_code_version)
            if existing[:3] != expected[:3] or (
                existing[3] != expected[3] and not allow_existing_code_version
            ):
                raise RuntimeError(
                    "策略定义已存在且不可覆盖；请递增 strategy_version/strategy_id，"
                    "不得用新代码改写旧策略定义"
                )
            return strategy_id
        cur.execute(
            """INSERT INTO backtest.strategy_definition
                (strategy_id,strategy_name,strategy_version,description,parameters,
                 parameters_hash,code_version)
                VALUES (%s,%s,%s,%s,%s,%s,%s)""",
            (strategy_id, strategy_id, strategy.strategy_version,
             "确定性价值估值 + 趋势过滤示例策略；不代表投资建议。",
             _json_param(parameters), _parameters_hash(parameters), code_version or metadata["code_version"]),
        )
        conn.commit()
    return strategy_id


def create_run(
    dataset_id: str,
    strategy_id: str,
    config: BacktestConfig,
    run_version: str = "1",
    code_archive_path: str | None = None,
) -> str:
    run_id = str(uuid.uuid4())
    metadata = _git_metadata()
    if config.max_portfolio_risk_pct is None:
        raise RuntimeError(
            "正式回测必须显式设置 max_portfolio_risk_pct；"
            "禁止让组合总风险上限默认为无限"
        )
    if metadata["dirty"] and not code_archive_path:
        raise RuntimeError("正式回测要求 Git 工作区干净；脏工作区必须提供代码归档")
    if code_archive_path and not Path(code_archive_path).exists():
        raise FileNotFoundError(f"代码归档不存在: {code_archive_path}")
    archive_hash = archive_size = None
    if code_archive_path:
        archive_hash, archive_size = _sha256_file(code_archive_path)
    parameters = {
        "risk_pct": config.risk_pct,
        "max_positions": config.max_positions,
        "max_single_position_pct": config.max_single_position_pct,
        "max_portfolio_risk_pct": config.max_portfolio_risk_pct,
        "reserve_cash_pct": config.reserve_cash_pct,
        "market_rule": config.market_rule.__dict__,
        "fee_rule": config.fee_rule.__dict__,
        "risk_policy_version": config.risk_policy_version,
        "entry_slippage_bps": config.entry_slippage_bps,
        "exit_slippage_bps": config.exit_slippage_bps,
        "gap_stress_pct": config.gap_stress_pct,
        "monte_carlo_method": config.monte_carlo_method,
        "force_liquidate_at_end": config.force_liquidate_at_end,
    }
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """SELECT status, input_manifest_hash, market_rule_version, fee_rule_version
                 FROM backtest.dataset_manifest WHERE dataset_id=%s""",
            (dataset_id,),
        )
        dataset = cur.fetchone()
        if not dataset or dataset[0] != "FROZEN":
            raise RuntimeError("正式回测只能使用 FROZEN Dataset")
        cur.execute(
            """SELECT rule_version, market_rule_version, fee_rule_version,
                      effective_from, effective_to, market, min_buy_lot,
                      allow_sell_odd_lot, t_plus_one, price_tick,
                      cash_settlement_days, commission_rate, min_commission,
                      stamp_duty_sell_rate, transfer_fee_rate, slippage_bps,
                      parameters, rule_hash
                 FROM backtest.dataset_rule_snapshot
                WHERE dataset_id=%s
                ORDER BY effective_from, market, rule_version""",
            (dataset_id,),
        )
        rule_rows = cur.fetchall()
        if not rule_rows:
            raise RuntimeError("FROZEN Dataset rule snapshot is empty")
        approved_slippage = {float(row[15]) for row in rule_rows}
        approved_gap_stress = {
            float((row[16] or {}).get("gap_stress_pct", 0.0))
            for row in rule_rows
        }
        runtime_entry_slippage = float(
            config.fee_rule.slippage_bps
            if config.entry_slippage_bps is None else config.entry_slippage_bps
        )
        runtime_exit_slippage = float(
            config.fee_rule.slippage_bps
            if config.exit_slippage_bps is None else config.exit_slippage_bps
        )
        if len(approved_slippage) != 1 or not all(
            math.isclose(value, next(iter(approved_slippage)), rel_tol=1e-12, abs_tol=1e-12)
            for value in (runtime_entry_slippage, runtime_exit_slippage)
        ):
            raise RuntimeError(
                "Run 进出场滑点必须与 FROZEN Dataset 的批准规则完全一致: "
                f"runtime=({runtime_entry_slippage},{runtime_exit_slippage}), "
                f"dataset={sorted(approved_slippage)}"
            )
        if len(approved_gap_stress) != 1 or not math.isclose(
            float(config.gap_stress_pct), next(iter(approved_gap_stress)),
            rel_tol=1e-12, abs_tol=1e-12,
        ):
            raise RuntimeError(
                "Run 跳空压力必须与 FROZEN Dataset 的批准规则完全一致: "
                f"runtime={config.gap_stress_pct}, dataset={sorted(approved_gap_stress)}"
            )
        parameters["dataset_rule_snapshot"] = {
            "source": "backtest.dataset_rule_snapshot",
            "rule_count": len(rule_rows),
            "rule_hashes": [row[17] for row in rule_rows],
            "effective_rules": [
                {
                    "rule_version": row[0],
                    "market_rule_version": row[1],
                    "fee_rule_version": row[2],
                    "effective_from": str(row[3]),
                    "effective_to": str(row[4]) if row[4] is not None else None,
                    "market": row[5],
                    "min_buy_lot": row[6],
                    "allow_sell_odd_lot": row[7],
                    "t_plus_one": row[8],
                    "price_tick": row[9],
                    "cash_settlement_days": row[10],
                    "commission_rate": row[11],
                    "min_commission": row[12],
                    "stamp_duty_sell_rate": row[13],
                    "transfer_fee_rate": row[14],
                    "slippage_bps": row[15],
                    "parameters": row[16] or {},
                }
                for row in rule_rows
            ],
        }
        parameters["market_rule"]["source"] = "backtest.dataset_rule_snapshot"
        parameters["fee_rule"]["source"] = "backtest.dataset_rule_snapshot"
        parameters["fee_rule"]["effective_slippage_bps"] = sorted({row[15] for row in rule_rows})
        if len(parameters["fee_rule"]["effective_slippage_bps"]) == 1:
            parameters["fee_rule"]["slippage_bps"] = parameters["fee_rule"]["effective_slippage_bps"][0]
        parameters["market_rule"]["effective_liquidity"] = sorted({
            (row[16] or {}).get("max_volume_participation_pct")
            for row in rule_rows
        })
        if len(parameters["market_rule"]["effective_liquidity"]) == 1:
            parameters["market_rule"]["max_volume_participation_pct"] = parameters["market_rule"]["effective_liquidity"][0]
        parameters["market_rule"]["liquidity_policy"] = sorted({
            (row[16] or {}).get("liquidity_policy", "reject_full_order")
            for row in rule_rows
        })
        cur.execute(
            """INSERT INTO backtest.run_manifest
              (run_id,dataset_id,strategy_id,run_version,initial_cash,max_positions,
               max_single_position_pct,max_portfolio_risk_pct,reserve_cash_pct,
                 market_rule_version,fee_rule_version,execution_model_version,random_seed,
                 parameters,parameters_hash,input_hash,code_version,git_commit,git_dirty,diff_hash,
                 code_archive_path,code_archive_sha256,code_archive_size_bytes)
              VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (run_id, dataset_id, strategy_id, run_version, config.initial_cash,
             config.max_positions, config.max_single_position_pct, config.max_portfolio_risk_pct,
             config.reserve_cash_pct, dataset[2], dataset[3],
              config.execution_model_version, config.random_seed, _json_param(parameters), _parameters_hash(parameters),
             dataset[1], metadata["code_version"], metadata["commit"], metadata["dirty"],
             metadata["diff_hash"], code_archive_path, archive_hash, archive_size),
        )
        cur.execute("UPDATE backtest.run_manifest SET status='RUNNING' WHERE run_id=%s", (run_id,))
        conn.commit()
    return run_id


def _execute_values(cur, sql: str, rows: list[tuple]):
    if rows:
        execute_values = require_psycopg2()[1]
        execute_values(cur, sql, rows, page_size=1000)


def persist_result(run_id: str, result) -> None:
    with get_connection() as conn, conn.cursor() as cur:
        signal_rows = [(
            row["signal_id"], run_id, row.get("research_snapshot_id"), row["ts_code"],
            row["signal_date"], row.get("decision_time"), row["intended_execution_date"],
            row["direction"], row.get("indicator_price"), row.get("raw_reference_price"),
            row.get("stop_loss_price_raw"), row.get("signal_score", 0.0),
            _json_param(row.get("reason", {})), row.get("status", "CREATED"),
        ) for row in result.signals]
        _execute_values(
            cur,
            """INSERT INTO backtest.signal
              (signal_id,run_id,research_snapshot_id,ts_code,signal_date,decision_time,
               intended_execution_date,direction,indicator_price,raw_reference_price,
               stop_loss_price_raw,signal_score,reason,status) VALUES %s""",
            signal_rows,
        )

        order_rows = [(
            row["order_id"], run_id, row.get("signal_id"), row["ts_code"], row["decision_date"],
            row["order_date"], row["expire_date"], row["direction"],
            row.get("planned_shares", row["order_shares"]), row["order_shares"],
            row.get("requested_shares", row.get("planned_shares", row["order_shares"])),
            # Do not infer realized shares from a plan when the field is absent.
            row.get("actual_shares", 0),
            row.get("volume_capacity_shares"), row.get("liquidity_status", "NOT_CHECKED"),
            row.get("liquidity_reject_reason"), row.get("liquidity_reference_date"),
            row.get("risk_policy_version"),
            row.get("theoretical_shares"), row.get("required_capital"),
            row.get("max_loss_at_stop"), row.get("gap_stress_loss"),
            row.get("buy_fees"), row.get("estimated_sell_fees"),
            row.get("estimated_slippage"), row.get("actual_risk_pct"),
            row.get("reference_price"),
            row.get("stop_loss_price_raw"), row["status"], row.get("reject_reason"),
        ) for row in result.orders]
        _execute_values(
            cur,
            """INSERT INTO backtest.order
              (order_id,run_id,signal_id,ts_code,decision_date,order_date,expire_date,
               direction,planned_shares,order_shares,requested_shares,actual_shares,
               volume_capacity_shares,liquidity_status,liquidity_reject_reason,
               liquidity_reference_date,risk_policy_version,
               theoretical_shares,required_capital,max_loss_at_stop,gap_stress_loss,buy_fees,
               estimated_sell_fees,estimated_slippage,actual_risk_pct,
               reference_price,stop_loss_price_raw,status,reject_reason)
              VALUES %s""",
            order_rows,
        )

        fill_rows = [(
            row["fill_id"], run_id, row["order_id"], row["ts_code"], row["fill_date"],
            row.get("actual_fill_time"), row["direction"], row["fill_price"], row["fill_shares"],
            row["commission"], row["stamp_duty"], row["transfer_fee"], row.get("slippage_cost", 0),
        ) for row in result.fills]
        _execute_values(
            cur,
            """INSERT INTO backtest.fill
              (fill_id,run_id,order_id,ts_code,fill_date,actual_fill_time,direction,
               fill_price,fill_shares,commission,stamp_duty,transfer_fee,slippage_cost)
              VALUES %s""",
            fill_rows,
        )

        lot_rows = [(
            row["lot_id"], run_id, row["ts_code"], row["open_fill_id"], row["open_date"],
            row["available_sell_date"], row["open_shares"], row["remaining_shares"],
            row["cost_price"], row.get("stop_loss_price_raw"), row.get("close_date"),
            row.get("close_fill_id"), row.get("opening_event_type", "FILL"),
            row.get("opening_event_key"),
            row.get("accrued_cash_dividend_per_share", 0.0),
        ) for row in result.position_lots]
        _execute_values(
            cur,
            """INSERT INTO backtest.position_lot
              (lot_id,run_id,ts_code,open_fill_id,open_date,available_sell_date,open_shares,
               remaining_shares,cost_price,stop_loss_price_raw,close_date,close_fill_id,
               opening_event_type,opening_event_key,accrued_cash_dividend_per_share)
              VALUES %s""",
            lot_rows,
        )

        position_rows = [(
            run_id, row["trade_date"], row["ts_code"], row["hold_shares"], row.get("average_cost"),
            row.get("raw_close"), row.get("market_value"), row.get("unrealized_pnl"), row.get("valuation_status", "VALID"),
        ) for row in result.position_daily]
        _execute_values(
            cur,
            """INSERT INTO backtest.position_daily
              (run_id,trade_date,ts_code,hold_shares,average_cost,raw_close,market_value,
               unrealized_pnl,valuation_status) VALUES %s""",
            position_rows,
        )

        cash_rows = [(
            run_id, row["trade_date"], row["event_type"], row["amount"], row["balance"],
            row["available_date"], row.get("reference_id"), row.get("description"),
        ) for row in result.cash_ledger]
        _execute_values(
            cur,
            """INSERT INTO backtest.cash_ledger
              (run_id,trade_date,event_type,amount,balance,available_date,reference_id,description)
              VALUES %s""",
            cash_rows,
        )

        performance_rows = [(
            run_id, row["trade_date"], row["cash_balance"], row["position_value"],
            row.get("cash_receivable", 0), row.get("stock_receivable_value", 0),
            row.get("receivable_total", 0),
            row["total_equity"], row.get("daily_return"), row.get("cumulative_return"),
            row.get("drawdown"), row.get("turnover", 0), row.get("valuation_status", "VALID"),
        ) for row in result.performance_daily]
        _execute_values(
            cur,
            """INSERT INTO backtest.performance_daily
              (run_id,trade_date,cash_balance,position_value,
               cash_receivable,stock_receivable_value,receivable_total,
               total_equity,daily_return,cumulative_return,drawdown,turnover,
               valuation_status) VALUES %s""",
            performance_rows,
        )
        metric_rows = [(run_id, name, float(value), None, _json_param({})) for name, value in result.metrics.items()]
        for audit_name, payload in getattr(result, "audit_metadata", {}).items():
            if audit_name in {"monte_carlo", "end_of_data_liquidation"}:
                metric_rows.append((
                    run_id,
                    f"{audit_name}_metadata",
                    None,
                    str(payload.get("method", "")),
                    _json_param(payload),
                ))
        _execute_values(
            cur,
            """INSERT INTO backtest.metric
              (run_id,metric_name,metric_value,metric_text,parameters) VALUES %s""",
            metric_rows,
        )
        cur.execute("UPDATE backtest.run_manifest SET status='SUCCEEDED' WHERE run_id=%s", (run_id,))
        conn.commit()


def run_frozen_backtest(
    dataset_id: str,
    config: BacktestConfig | None = None,
    run_version: str = "1",
    code_archive_path: str | None = None,
    strategy: BaseStrategy | None = None,
    reuse_existing_strategy_definition: bool = False,
):
    config = config or BacktestConfig()
    strategy = strategy or FundamentalValueGrowthTrendStrategy()
    if config.max_portfolio_risk_pct is None:
        raise RuntimeError(
            "正式回测必须显式设置 max_portfolio_risk_pct；"
            "纯 Python 单元测试可保留 None"
        )
    # Fail before registering an append-only strategy definition.  Otherwise
    # a dirty workspace with no archive could leave a dangling strategy row
    # even though the formal Run is correctly rejected later by create_run().
    metadata = _git_metadata()
    if metadata["dirty"] and not code_archive_path:
        raise RuntimeError("正式回测要求 Git 工作区干净；脏工作区必须提供代码归档")
    strategy_id = register_strategy(
        strategy,
        allow_existing_code_version=reuse_existing_strategy_definition,
    )
    run_id = create_run(dataset_id, strategy_id, config, run_version, code_archive_path)
    try:
        required_columns = getattr(strategy, "required_observation_columns", None)
        print(
            f"backtest loading frozen dataset dataset_id={dataset_id} "
            f"strategy={strategy_id} compact_columns={bool(required_columns)}",
            flush=True,
        )
        dataset = load_frozen_dataset(dataset_id, observation_columns=required_columns)
        print(
            f"backtest dataset loaded calendar={len(dataset.calendar)} "
            f"evaluation={dataset.evaluation_start}..{dataset.evaluation_end}",
            flush=True,
        )
        result = BacktestEngine(config).run(dataset, strategy, run_id=run_id)
        persist_result(run_id, result)
        return run_id, result
    except Exception as exc:
        with get_connection() as conn, conn.cursor() as cur:
            cur.execute("UPDATE backtest.run_manifest SET status='FAILED', failure_reason=%s WHERE run_id=%s AND status='RUNNING'", (str(exc)[:4000], run_id))
            conn.commit()
        raise


def main(argv=None):
    parser = argparse.ArgumentParser(description="运行 FROZEN Backtest Dataset")
    parser.add_argument("--dataset-id", required=True)
    parser.add_argument(
        "--strategy", choices=["fundamental-v2", "legacy-value-trend-v1", "pure-value-trend-v1"],
        default="fundamental-v2",
        help="正式默认 fundamental-v2；legacy 仅用于复现旧 Run；pure 用于验证解耦后的策略适配器架构",
    )
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
    parser.add_argument(
        "--monte-carlo-method",
        choices=["bootstrap_with_replacement", "permutation_without_replacement"],
        default="bootstrap_with_replacement",
    )
    parser.add_argument("--random-seed", type=int, default=42)
    parser.add_argument("--code-archive-path")
    parser.add_argument(
        "--reuse-existing-strategy-definition",
        action="store_true",
        help="仅在策略规则未改变、仅运行器/加载器变更时复用既有策略定义；Run 仍记录当前代码版本",
    )
    args = parser.parse_args(argv)
    if args.strategy == "fundamental-v2":
        strategy = FundamentalValueGrowthTrendStrategy()
    elif args.strategy == "legacy-value-trend-v1":
        strategy = ValueTrendStrategy()
    elif args.strategy == "pure-value-trend-v1":
        strategy = ValueTrendCandidateAdapter(PureValueTrendStrategy())
    else:
        raise ValueError(f"Unknown strategy: {args.strategy}")

    run_id, result = run_frozen_backtest(
        args.dataset_id,
        BacktestConfig(
            initial_cash=args.initial_cash,
            risk_pct=args.risk_pct,
            max_positions=args.max_positions,
            max_single_position_pct=args.max_single_position_pct,
            max_portfolio_risk_pct=args.max_portfolio_risk_pct,
            reserve_cash_pct=args.reserve_cash_pct,
            entry_slippage_bps=args.entry_slippage_bps,
            exit_slippage_bps=args.exit_slippage_bps,
            gap_stress_pct=args.gap_stress_pct,
            monte_carlo_method=args.monte_carlo_method,
            random_seed=args.random_seed,
        ),
        args.run_version,
        args.code_archive_path,
        strategy,
        args.reuse_existing_strategy_definition,
    )
    print(json.dumps({"run_id": run_id, "metrics": result.metrics, "warnings": result.warnings}, ensure_ascii=True, default=str))


if __name__ == "__main__":
    main()
