"""Persistence and Air-Gap operations for daily AI review."""
from __future__ import annotations

from datetime import date
import json
import uuid
from typing import Any, Mapping

from data_collect.decision.audit import normalize_ai_audit
from data_collect.decision.candidate_service import (
    _domain_rules, _hash, _rule_for_market, get_candidate_packet,
)
from data_collect.backtest.risk import check_liquidity, size_position
from data_collect.jobs.backtest_dataset import _json_param
from data_collect.data_access import DataAccessKernel, RunContext
from data_collect.data_access.policy_catalog import build_default_registry
from data_collect.data_access.repository.audit_repository import AuditRepository


def _service(label: str) -> tuple[DataAccessKernel, AuditRepository, RunContext]:
    kernel = DataAccessKernel(policy_registry=build_default_registry())
    context = RunContext(
        run_id=f"audit-{label}-{uuid.uuid4().hex}", job_name="daily_candidate_audit",
        code_version="unknown", config_hash="unknown", pipeline_version="data-access-v1",
    )
    return kernel, AuditRepository(kernel), context


def _number(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def store_ai_audit(candidate_id: str, payload: Mapping[str, Any], *, portfolio_id: str | None = None) -> dict[str, Any]:
    """Validate and append one AI audit; never changes Portfolio state."""
    packet = get_candidate_packet(candidate_id, portfolio_id=portfolio_id)
    normalized = normalize_ai_audit(payload, packet)
    kernel, repository, context = _service("store")
    audit_id = str(uuid.uuid4())
    try:
        if repository.candidate_hash(candidate_id, context) is None:
            raise ValueError(f"不存在的候选: {candidate_id}")
        existing = repository.existing_ai_audit(candidate_id, normalized, context)
        if existing:
            if str(existing["output_hash"]).strip() != normalized["output_hash"]:
                raise ValueError("同一候选/输入/模型已有不同 AI 审计，拒绝覆盖")
            return {"audit_id": str(existing["audit_id"]), "output_hash": normalized["output_hash"], "idempotent": True}
        repository.append_ai_audit({
            "audit_id": audit_id, "candidate_id": candidate_id, "portfolio_id": portfolio_id,
            "input_hash": normalized["input_hash"], "skill_name": normalized["skill_name"],
            "skill_version": normalized["skill_version"], "model_provider": normalized.get("model_provider"),
            "model_version": normalized["model_version"], "prompt_version": normalized["prompt_version"],
            "action": normalized["action"], "risk_level": normalized["risk_level"],
            "evidence_ids": normalized["evidence_ids"], "result": normalized, "output_hash": normalized["output_hash"],
        }, context)
        return {"audit_id": audit_id, "output_hash": normalized["output_hash"], "idempotent": False}
    finally:
        kernel.close()




def record_human_decision(candidate_id: str, *, audit_id: str, decision: str, actor: str, reason: str | None = None, portfolio_id: str | None = None) -> dict[str, Any]:
    """Append the human gate. APPROVE does not approve an order intent."""
    if decision not in {"APPROVE", "REJECT", "DEFER"}:
        raise ValueError("decision 必须为 APPROVE/REJECT/DEFER")
    if not actor.strip():
        raise ValueError("actor 不能为空")
    kernel, repository, context = _service("decision")
    try:
        audit = repository.decision_context(audit_id, candidate_id, context)
        if not audit:
            raise ValueError("AI 审计不存在或与候选不匹配")
        if audit["portfolio_id"] is not None and str(audit["portfolio_id"]) != str(portfolio_id):
            raise ValueError("AI 审计绑定的 Portfolio 不匹配")
        if decision == "APPROVE":
            if not bool(audit["eligible"]):
                raise ValueError("不可批准未通过系统风险/可交易门禁的候选")
            if audit["action"] == "REJECT" or audit["risk_level"] == "HIGH":
                raise ValueError("不可批准 AI 已明确 REJECT 的候选")
            if audit["direction"] == "BUY":
                evidence_ids = [str(item) for item in (audit["evidence_ids"] or [])]
                if not evidence_ids:
                    raise ValueError("BUY 批准必须引用正方、反方和可审计来源证据")
                evidence_rows = repository.evidence_for_ids(evidence_ids, str(audit["snapshot_id"]), str(audit["ts_code"]), context)
                if len(evidence_rows) != len(set(evidence_ids)):
                    raise ValueError("AI 引用的证据与候选股票/快照不完全一致")
                roles = {item["claim_role"] for item in evidence_rows}
                systems = {item["source_system"] for item in evidence_rows}
                authorities = {item["authority_level"] for item in evidence_rows}
                if not {"supporting", "contrary"}.issubset(roles):
                    raise ValueError("BUY 批准必须同时引用 supporting 与 contrary 证据")
                if len(systems) < 2:
                    raise ValueError("BUY 批准至少需要两个独立 source_system 交叉验证")
                if not authorities.intersection({"OFFICIAL", "INTERNAL_FACT"}):
                    raise ValueError("BUY 批准至少需要一条官方或内部事实证据")
                for item in evidence_rows:
                    cutoff = item["research_cutoff_date"]
                    if not item["content_hash"] or len(str(item["content_hash"]).strip()) != 64:
                        raise ValueError("BUY 批准证据必须具有内容哈希")
                    if cutoff is not None and item["published_at"] is not None and item["published_at"].date() > cutoff:
                        raise ValueError("BUY 批准证据发布时间晚于 Research 截止日")
                    if cutoff is not None and item["effective_date"] is not None and item["effective_date"] > cutoff:
                        raise ValueError("BUY 批准证据生效日晚于 Research 截止日")
        prior = repository.existing_decision(candidate_id, portfolio_id, context)
        if prior:
            if prior["decision"] != decision:
                raise ValueError("该候选已有不同的人工作决定，禁止覆盖")
            return {"decision_id": str(prior["decision_id"]), "decision": prior["decision"], "idempotent": True}
        decision_id = str(uuid.uuid4())
        repository.append_decision({"decision_id": decision_id, "candidate_id": candidate_id, "audit_id": audit_id, "portfolio_id": portfolio_id, "decision": decision, "actor": actor.strip(), "reason": reason}, context)
        return {"decision_id": decision_id, "decision": decision, "idempotent": False}
    finally:
        kernel.close()




def bind_snapshot_to_portfolio(portfolio_id: str, snapshot_id: str, *, candidate_run_id: str | None = None) -> dict[str, Any]:
    """Append one daily FROZEN Snapshot binding, idempotently."""
    kernel, repository, context = _service("binding")
    try:
        snapshot = repository.snapshot(snapshot_id, context)
        if not snapshot or snapshot["status"] != "FROZEN":
            raise ValueError("Portfolio 每日绑定必须使用 FROZEN Snapshot")
        valuation_date = snapshot["research_cutoff_date"]
        repository.append_binding({"portfolio_id": portfolio_id, "valuation_date": valuation_date, "snapshot_id": snapshot_id, "candidate_run_id": candidate_run_id}, context)
        row = repository.binding(portfolio_id, valuation_date, context)
        if not row or str(row["snapshot_id"]) != str(snapshot_id):
            raise ValueError("Portfolio 当日已有不同 Snapshot，禁止替换")
        return {"portfolio_id": portfolio_id, "valuation_date": valuation_date, "snapshot_id": snapshot_id, "idempotent": True}
    finally:
        kernel.close()




def create_daily_signal_intent(candidate_id: str, *, audit_id: str, portfolio_id: str, actor: str) -> dict[str, Any]:
    """Turn an approved human decision into a PENDING paper intent.

    Never approves or fills; the later Portfolio workflow remains a separate gate.
    """
    kernel, repository, context = _service("intent")
    try:
        row = repository.daily_signal_row(candidate_id, audit_id, portfolio_id, context)
        if not row or row["decision"] != "APPROVE":
            raise ValueError("候选没有对应的 APPROVE 人工决定")
        if not row["eligible"] or row["status"] != "SUCCEEDED":
            raise ValueError("候选未通过系统门禁或运行未成功")
        if row["action"] == "REJECT":
            raise ValueError("AI 已拒绝该候选")
        if row["direction"] == "HOLD":
            raise ValueError("HOLD 审计只记录继续持有决定，不创建订单意图")
        allowed_actions = {"BUY": {"PASS_REVIEW", "NEEDS_REVIEW"}, "SELL": {"PASS_REVIEW", "NEEDS_REVIEW", "SELL_REVIEW"}}
        if row["action"] not in allowed_actions.get(row["direction"], set()):
            raise ValueError("AI 审计动作与订单方向不匹配")
        shares = int(row["planned_shares"])
        if shares <= 0:
            raise ValueError("候选计划股数为 0，不能创建订单意图")
        decision_date = row["research_date"]
        next_day = repository.next_trade_date(decision_date, context)
        if not next_day:
            raise ValueError("找不到候选日之后的 SSE 交易日")
        repository.append_binding({"portfolio_id": portfolio_id, "valuation_date": decision_date, "snapshot_id": row["snapshot_id"], "candidate_run_id": row["candidate_run_id"]}, context)
        binding = repository.binding(portfolio_id, decision_date, context)
        if not binding or str(binding["snapshot_id"]) != str(row["snapshot_id"]):
            raise ValueError("Portfolio 当日已绑定不同 Snapshot，拒绝创建纸面意图")
        existing = repository.existing_intent(portfolio_id, candidate_id, context)
        if existing:
            return {"intent_id": str(existing["intent_id"]), "status": "PENDING_APPROVAL", "idempotent": True}
        intent_id = str(uuid.uuid4())
        reason = {"source": "DAILY_SIGNAL", "candidate_id": candidate_id, "candidate_hash": str(row["candidate_hash"]).strip(), "ai_audit_id": audit_id, "ai_action": row["action"], "ai_output_hash": str(row["output_hash"]).strip(), "created_by": actor, "execution_note": "T+1 开盘前需重新核验价格、股数、涨跌停和流动性；本意图不会自动成交。", "strategy_reason": row["reason"] or {}}
        repository.persist_daily_intent(
            {"intent_id": intent_id, "portfolio_id": portfolio_id, "snapshot_id": row["snapshot_id"], "watchlist_id": None, "ts_code": row["ts_code"], "decision_date": decision_date, "intended_execution_date": next_day, "direction": row["direction"], "requested_shares": shares, "reference_price": row["raw_reference_price"], "stop_loss_price_raw": row["stop_loss_price_raw"], "source_type": "DAILY_SIGNAL", "reason": reason, "source_candidate_id": candidate_id, "source_ai_audit_id": audit_id, "source_candidate_hash": str(row["candidate_hash"]).strip()},
            {"portfolio_id": portfolio_id, "event_date": decision_date, "event_type": "DAILY_SIGNAL_INTENT_CREATED", "intent_id": intent_id, "payload": reason}, context,
        )
        return {"intent_id": intent_id, "status": "PENDING_APPROVAL", "idempotent": False}
    finally:
        kernel.close()




def prepare_daily_signal_execution(portfolio_id: str, intent_id: str) -> dict[str, Any]:
    """用已落 Raw 的完整执行日 bar 对纸面意图做收盘后复核。

    该函数会使用全天成交量做流动性门禁，因此不是盘中实盘下单入口。
    It may resize a still-pending intent, but it never approves or fills it.
    Missing/invalid execution data returns a fail-closed rejection result and
    leaves the intent pending for an explicit human expiry/rejection.
    # Contract guard: only status='PENDING_APPROVAL' is resized; resized_at_execution
    # is audit metadata, and this path never approves or fills.
    """
    kernel, repository, context = _service("execution")
    try:
        row = repository.execution_intent(portfolio_id, intent_id, context)
        if not row:
            raise ValueError("不存在的每日纸面意图")
        if row[0] != "PENDING_APPROVAL" or row[1] != "DAILY_SIGNAL":
            raise ValueError("只有 DAILY_SIGNAL/PENDING_APPROVAL 意图可以重新定价")
        candidate_id = str(row[9])
        packet = get_candidate_packet(candidate_id, portfolio_id=portfolio_id)
        source_quote = repository.market_raw_quote(row[3], row[6], context)
        if not source_quote:
            return {"intent_id": intent_id, "eligible": False, "actual_shares": 0, "reject_reason": "缺少 T+1 Raw 执行行情，保持 PENDING_APPROVAL"}
        market_row = repository.snapshot_market(row[2], row[3], context)
        if not market_row:
            return {"intent_id": intent_id, "eligible": False, "actual_shares": 0, "reject_reason": "缺少 FROZEN Snapshot 市场分类，保持 PENDING_APPROVAL"}
        execution_market = market_row[0]
        checkpoint_key = row[6].strftime("%Y%m%d")
        checkpoints = repository.execution_checkpoints(checkpoint_key, context)
        if any(
            checkpoints.get(dataset, (None,))[0] not in {"done", "empty"}
            for dataset in ("suspend_d_incremental", "stk_limit_incremental")
        ):
            return {"intent_id": intent_id, "eligible": False, "actual_shares": 0, "reject_reason": "T+1 停牌/涨跌停 Raw 同步未完成，保持 PENDING_APPROVAL"}
        limit_source = repository.limit_raw(row[3], row[6], context)
        if not limit_source:
            return {"intent_id": intent_id, "eligible": False, "actual_shares": 0, "reject_reason": "缺少 T+1 涨跌停 Raw，保持 PENDING_APPROVAL"}
        suspend_source = repository.suspend_raw(row[3], row[6], context)
        if suspend_source:
            return {"intent_id": intent_id, "eligible": False, "actual_shares": 0, "reject_reason": "T+1 股票停牌，保持 PENDING_APPROVAL"}
        if suspend_source:
            return {"intent_id": intent_id, "eligible": False, "actual_shares": 0, "reject_reason": "T+1 股票停牌，保持 PENDING_APPROVAL"}
        payload = source_quote[1] or {}
        quote = (
            payload.get("open"), payload.get("high"), payload.get("low"),
            payload.get("close"),
            None if payload.get("vol") is None else float(payload["vol"]) * 100.0,
            None if payload.get("amount") is None else float(payload["amount"]) * 1000.0,
        )
        if quote[0] is None or float(quote[0]) <= 0:
            return {"intent_id": intent_id, "eligible": False, "actual_shares": 0, "reject_reason": "缺少有效 T+1 开盘价，保持 PENDING_APPROVAL"}
        open_price = float(quote[0])
        limit_payload = limit_source[1] or {}
        up_limit = _number(limit_payload.get("up_limit"))
        down_limit = _number(limit_payload.get("down_limit"))
        if up_limit is None or down_limit is None or up_limit <= 0 or down_limit <= 0:
            return {"intent_id": intent_id, "eligible": False, "actual_shares": 0, "reject_reason": "T+1 涨跌停价格无效，保持 PENDING_APPROVAL"}
        if row[4] == "BUY" and open_price >= up_limit - 1e-9:
            return {"intent_id": intent_id, "eligible": False, "actual_shares": 0, "reject_reason": "T+1 开盘涨停，保持 PENDING_APPROVAL"}
        if row[4] == "SELL" and open_price <= down_limit + 1e-9:
            return {"intent_id": intent_id, "eligible": False, "actual_shares": 0, "reject_reason": "T+1 开盘跌停，保持 PENDING_APPROVAL"}
        rule_snapshot = packet.get("rule_snapshot") or {}
        rule_row = _rule_for_market(rule_snapshot, execution_market)
        market_rule, fee_rule, risk_policy = _domain_rules(rule_row)
        if not bool(row[12]):
            return {"intent_id": intent_id, "eligible": False, "actual_shares": 0, "reject_reason": "候选已不满足系统门禁，保持 PENDING_APPROVAL"}
        if row[4] == "SELL":
            actual_shares = int(row[7])
            liquidity = check_liquidity(
                quote[4], actual_shares,
                market_rule,
                amount=quote[5], close_price=open_price,
                low_price=quote[2], high_price=quote[1],
            )
            if not liquidity.allowed:
                return {"intent_id": intent_id, "eligible": False, "actual_shares": 0, "reject_reason": liquidity.reject_reason or liquidity.liquidity_status}
            sizing = None
        elif row[4] == "BUY":
            params = row[11] or {}
            state = (packet.get("portfolio_state") or {}).get("valuation") or {}
            equity = float(state.get("equity_base") or 0)
            cash = float(state.get("cash_balance") or 0)
            available_cash_balance = float(state.get("available_cash", cash) or 0)
            risk_cap = equity * float(params.get("max_portfolio_risk_pct", 0.03))
            existing_risk = state.get("existing_stop_risk")
            remaining_risk = max(0.0, risk_cap - (float(existing_risk) if existing_risk is not None else risk_cap))
            reserve_cash_pct = float(params.get("reserve_cash_pct", 0.10))
            available_cash = max(0.0, available_cash_balance - equity * reserve_cash_pct)
            sizing = size_position(
                available_cash=available_cash,
                risk_pct=float(params.get("risk_pct", 0.01)),
                entry_price=open_price,
                stop_loss_price=float(row[8] or 0),
                fee_rule=fee_rule,
                market_rule=market_rule,
                equity_base=equity,
                max_position_value=equity * float(params.get("max_single_position_pct", 0.25)),
                max_risk_amount=remaining_risk,
                risk_policy=risk_policy,
            )
            if not sizing.eligible:
                return {"intent_id": intent_id, "eligible": False, "actual_shares": 0, "reject_reason": sizing.reject_reason}
            liquidity = check_liquidity(
                quote[4], sizing.planned_shares,
                market_rule,
                amount=quote[5], close_price=open_price,
                low_price=quote[2], high_price=quote[1],
            )
            if not liquidity.allowed:
                return {"intent_id": intent_id, "eligible": False, "actual_shares": 0, "reject_reason": liquidity.reject_reason or liquidity.liquidity_status}
            actual_shares = int(sizing.planned_shares)
        else:
            raise ValueError("每日纸面意图方向必须为 BUY 或 SELL")
        if actual_shares <= 0:
            return {"intent_id": intent_id, "eligible": False, "actual_shares": 0, "reject_reason": "重新定价后股数为 0"}
        reason = dict(row[10] or {})
        execution_observation = {
            "ts_code": row[3], "trade_date": row[6].isoformat(),
            "open": quote[0], "high": quote[1], "low": quote[2],
            "close": quote[3], "volume": quote[4], "amount": quote[5],
            "source_record_id": int(source_quote[0]),
            "source_payload_hash": str(source_quote[2]).strip(),
            "source_fetched_at": source_quote[3].isoformat(),
            "source_batch_id": source_quote[4],
            "market": execution_market,
            "up_limit": up_limit,
            "down_limit": down_limit,
            "limit_source_record_id": int(limit_source[0]),
            "limit_source_payload_hash": str(limit_source[2]).strip(),
            "limit_source_fetched_at": limit_source[3].isoformat(),
            "limit_source_batch_id": limit_source[4],
            "suspend_source_record_id": None,
            "status_checkpoint": {
                key: {"status": value[0], "updated_at": value[1].isoformat()}
                for key, value in sorted(checkpoints.items())
            },
        }
        reason["execution_preparation"] = {
            "execution_price": open_price,
            "planned_shares": int(row[7]),
            "actual_shares": actual_shares,
            "resized_at_execution": actual_shares != int(row[7]),
            "execution_date": row[6].isoformat(),
            "source_candidate_hash": str(row[13]).strip(),
            "rule_snapshot_hash": packet.get("rule_snapshot_hash"),
            "execution_observation": execution_observation,
            "execution_observation_hash": _hash(execution_observation),
            "timing": "post-session paper audit; full-day liquidity is known",
        }
        repository.update_pending_intent(portfolio_id, intent_id, actual_shares, open_price, reason, context)
        return {
            "intent_id": intent_id,
            "eligible": True,
            "planned_shares": int(row[7]),
            "actual_shares": actual_shares,
            "resized_at_execution": actual_shares != int(row[7]),
            "execution_price": open_price,
            "status": "PENDING_APPROVAL",
        }
    finally:
        kernel.close()
