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
from data_collect.utils.db import get_connection, require_psycopg2


def _number(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def store_ai_audit(
    candidate_id: str,
    payload: Mapping[str, Any],
    *,
    portfolio_id: str | None = None,
) -> dict[str, Any]:
    """Validate and append one AI audit; never changes Portfolio state."""
    packet = get_candidate_packet(candidate_id, portfolio_id=portfolio_id)
    normalized = normalize_ai_audit(payload, packet)
    audit_id = str(uuid.uuid4())
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """SELECT candidate_hash FROM research.candidate WHERE candidate_id=%s""",
            (candidate_id,),
        )
        candidate = cur.fetchone()
        if not candidate:
            raise ValueError(f"不存在的候选: {candidate_id}")
        psycopg2, _ = require_psycopg2()
        cur.execute(
            """SELECT audit_id, output_hash FROM research.ai_audit
                WHERE candidate_id=%s AND input_hash=%s AND skill_name=%s
                  AND skill_version=%s AND model_version=%s AND prompt_version=%s""",
            (candidate_id, normalized["input_hash"], normalized["skill_name"],
             normalized["skill_version"], normalized["model_version"],
             normalized["prompt_version"]),
        )
        existing = cur.fetchone()
        if existing:
            if str(existing[1]).strip() != normalized["output_hash"]:
                raise ValueError("同一候选/输入/模型已有不同 AI 审计，拒绝覆盖")
            return {"audit_id": str(existing[0]), "output_hash": normalized["output_hash"], "idempotent": True}
        cur.execute(
            """INSERT INTO research.ai_audit
               (audit_id,candidate_id,portfolio_id,input_hash,skill_name,skill_version,
                model_provider,model_version,prompt_version,action,risk_level,
                evidence_ids,result,output_hash)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (audit_id, candidate_id, portfolio_id, normalized["input_hash"],
             normalized["skill_name"], normalized["skill_version"],
             normalized.get("model_provider"), normalized["model_version"],
             normalized["prompt_version"], normalized["action"], normalized["risk_level"],
             _json_param(normalized["evidence_ids"]), _json_param(normalized),
             normalized["output_hash"]),
        )
        conn.commit()
    return {"audit_id": audit_id, "output_hash": normalized["output_hash"], "idempotent": False}


def record_human_decision(
    candidate_id: str,
    *,
    audit_id: str,
    decision: str,
    actor: str,
    reason: str | None = None,
    portfolio_id: str | None = None,
) -> dict[str, Any]:
    """Append the human gate. APPROVE does not approve an order intent."""
    if decision not in {"APPROVE", "REJECT", "DEFER"}:
        raise ValueError("decision 必须为 APPROVE/REJECT/DEFER")
    if not actor.strip():
        raise ValueError("actor 不能为空")
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """SELECT a.action, a.risk_level, a.candidate_id, a.portfolio_id, a.output_hash,
                      c.eligible, c.direction, c.snapshot_id, c.ts_code, a.evidence_ids
                 FROM research.ai_audit a
                 JOIN research.candidate c ON c.candidate_id=a.candidate_id
                WHERE a.audit_id=%s""",
            (audit_id,),
        )
        audit = cur.fetchone()
        if not audit or str(audit[2]) != str(candidate_id):
            raise ValueError("AI 审计不存在或与候选不匹配")
        if audit[3] is not None and str(audit[3]) != str(portfolio_id):
            raise ValueError("AI 审计绑定的 Portfolio 不匹配")
        if decision == "APPROVE":
            if not bool(audit[5]):
                raise ValueError("不可批准未通过系统风险/可交易门禁的候选")
            if audit[0] == "REJECT" or audit[1] == "HIGH":
                raise ValueError("不可批准 AI 已明确 REJECT 的候选")
            if audit[6] == "BUY":
                evidence_ids = [str(item) for item in (audit[9] or [])]
                if not evidence_ids:
                    raise ValueError("BUY 批准必须引用正方、反方和可审计来源证据")
                cur.execute(
                    """SELECT claim_role,source_system,authority_level,content_hash,
                              published_at,effective_date,m.research_cutoff_date
                         FROM research.evidence_reference e
                         JOIN research.snapshot_manifest m ON m.snapshot_id=e.snapshot_id
                        WHERE e.evidence_id = ANY(%s::uuid[])
                          AND e.snapshot_id=%s AND e.ts_code=%s""",
                    (evidence_ids, audit[7], audit[8]),
                )
                evidence_rows = cur.fetchall()
                if len(evidence_rows) != len(set(evidence_ids)):
                    raise ValueError("AI 引用的证据与候选股票/快照不完全一致")
                roles = {item[0] for item in evidence_rows}
                systems = {item[1] for item in evidence_rows}
                authorities = {item[2] for item in evidence_rows}
                if not {"supporting", "contrary"}.issubset(roles):
                    raise ValueError("BUY 批准必须同时引用 supporting 与 contrary 证据")
                if len(systems) < 2:
                    raise ValueError("BUY 批准至少需要两个独立 source_system 交叉验证")
                if not authorities.intersection({"OFFICIAL", "INTERNAL_FACT"}):
                    raise ValueError("BUY 批准至少需要一条官方或内部事实证据")
                for _, _, _, content_hash, published_at, effective_date, cutoff in evidence_rows:
                    if not content_hash or len(str(content_hash).strip()) != 64:
                        raise ValueError("BUY 批准证据必须具有内容哈希")
                    if published_at is not None and published_at.date() > cutoff:
                        raise ValueError("BUY 批准证据发布时间晚于 Research 截止日")
                    if effective_date is not None and effective_date > cutoff:
                        raise ValueError("BUY 批准证据生效日晚于 Research 截止日")
        cur.execute(
            """SELECT decision_id, decision FROM research.human_decision
                WHERE candidate_id=%s AND portfolio_id IS NOT DISTINCT FROM %s""",
            (candidate_id, portfolio_id),
        )
        prior = cur.fetchone()
        if prior:
            if prior[1] != decision:
                raise ValueError("该候选已有不同的人工作决定，禁止覆盖")
            conn.commit()
            return {"decision_id": str(prior[0]), "decision": prior[1], "idempotent": True}
        decision_id = str(uuid.uuid4())
        cur.execute(
            """INSERT INTO research.human_decision
               (decision_id,candidate_id,audit_id,portfolio_id,decision,actor,reason)
               VALUES (%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (candidate_id, portfolio_id) DO NOTHING
            RETURNING decision_id""",
            (decision_id, candidate_id, audit_id, portfolio_id, decision, actor.strip(), reason),
        )
        row = cur.fetchone()
        if row is None:
            cur.execute(
                """SELECT decision_id, decision FROM research.human_decision
                    WHERE candidate_id=%s AND portfolio_id IS NOT DISTINCT FROM %s""",
                (candidate_id, portfolio_id),
            )
            existing = cur.fetchone()
            if not existing or existing[1] != decision:
                raise ValueError("该候选已有不同的人工作决定，禁止覆盖")
            conn.commit()
            return {"decision_id": str(existing[0]), "decision": existing[1], "idempotent": True}
        conn.commit()
    return {"decision_id": decision_id, "decision": decision, "idempotent": False}


def bind_snapshot_to_portfolio(
    portfolio_id: str,
    snapshot_id: str,
    *,
    candidate_run_id: str | None = None,
) -> dict[str, Any]:
    """Append one daily FROZEN Snapshot binding, idempotently."""
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """SELECT research_cutoff_date, status FROM research.snapshot_manifest
                WHERE snapshot_id=%s FOR SHARE""",
            (snapshot_id,),
        )
        snapshot = cur.fetchone()
        if not snapshot or snapshot[1] != "FROZEN":
            raise ValueError("Portfolio 每日绑定必须使用 FROZEN Snapshot")
        valuation_date = snapshot[0]
        cur.execute(
            """INSERT INTO portfolio.snapshot_binding
                      (portfolio_id,valuation_date,snapshot_id,candidate_run_id)
               VALUES (%s,%s,%s,%s)
            ON CONFLICT (portfolio_id,valuation_date) DO NOTHING""",
            (portfolio_id, valuation_date, snapshot_id, candidate_run_id),
        )
        cur.execute(
            """SELECT snapshot_id,candidate_run_id FROM portfolio.snapshot_binding
                WHERE portfolio_id=%s AND valuation_date=%s""",
            (portfolio_id, valuation_date),
        )
        row = cur.fetchone()
        if not row or str(row[0]) != str(snapshot_id):
            raise ValueError("Portfolio 当日已有不同 Snapshot，禁止替换")
        conn.commit()
    return {"portfolio_id": portfolio_id, "valuation_date": valuation_date, "snapshot_id": snapshot_id, "idempotent": True}


def create_daily_signal_intent(
    candidate_id: str,
    *,
    audit_id: str,
    portfolio_id: str,
    actor: str,
) -> dict[str, Any]:
    """Turn an approved human decision into a PENDING paper intent.

    The function intentionally stops before APPROVED and before a fill.  A
    later explicit call to the existing Portfolio approval/fill functions is
    required, preserving the two-person-style Air-Gap in a single-user setup.
    """
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """SELECT d.decision,d.actor,a.action,a.output_hash,c.snapshot_id,
                      c.candidate_run_id,c.research_date,c.ts_code,c.direction,
                      c.planned_shares,c.raw_reference_price,c.stop_loss_price_raw,
                      c.candidate_hash,c.reason,c.eligible,r.status
                 FROM research.human_decision d
                 JOIN research.ai_audit a ON a.audit_id=d.audit_id
                 JOIN research.candidate c ON c.candidate_id=d.candidate_id
                 JOIN research.candidate_run r ON r.candidate_run_id=c.candidate_run_id
                WHERE d.candidate_id=%s AND d.audit_id=%s
                  AND d.portfolio_id=%s""",
            (candidate_id, audit_id, portfolio_id),
        )
        row = cur.fetchone()
        if not row or row[0] != "APPROVE":
            raise ValueError("候选没有对应的 APPROVE 人工决定")
        if not row[14] or row[15] != "SUCCEEDED":
            raise ValueError("候选未通过系统门禁或运行未成功")
        if row[2] == "REJECT":
            raise ValueError("AI 已拒绝该候选")
        if row[8] == "HOLD":
            raise ValueError("HOLD 审计只记录继续持有决定，不创建订单意图")
        allowed_actions = {
            "BUY": {"PASS_REVIEW", "NEEDS_REVIEW"},
            "SELL": {"PASS_REVIEW", "NEEDS_REVIEW", "SELL_REVIEW"},
        }
        if row[2] not in allowed_actions.get(row[8], set()):
            raise ValueError("AI 审计动作与订单方向不匹配")
        shares = int(row[9])
        if shares <= 0:
            raise ValueError("候选计划股数为 0，不能创建订单意图")
        decision_date = row[6]
        cur.execute(
            """SELECT trade_date FROM trading_calendar
                WHERE exchange='SSE' AND is_open IS TRUE AND trade_date>%s
                ORDER BY trade_date LIMIT 1""",
            (decision_date,),
        )
        next_day = cur.fetchone()
        if not next_day:
            raise ValueError("找不到候选日之后的 SSE 交易日")
        cur.execute(
            """INSERT INTO portfolio.snapshot_binding
                      (portfolio_id,valuation_date,snapshot_id,candidate_run_id)
               VALUES (%s,%s,%s,%s)
            ON CONFLICT (portfolio_id,valuation_date) DO NOTHING""",
            (portfolio_id, decision_date, row[4], row[5]),
        )
        cur.execute(
            """SELECT snapshot_id FROM portfolio.snapshot_binding
                WHERE portfolio_id=%s AND valuation_date=%s""",
            (portfolio_id, decision_date),
        )
        binding = cur.fetchone()
        if not binding or str(binding[0]) != str(row[4]):
            raise ValueError("Portfolio 当日已绑定不同 Snapshot，拒绝创建纸面意图")
        intent_id = str(uuid.uuid4())
        reason = {
            "source": "DAILY_SIGNAL",
            "candidate_id": candidate_id,
            "candidate_hash": str(row[12]).strip(),
            "ai_audit_id": audit_id,
            "ai_action": row[2],
            "ai_output_hash": str(row[3]).strip(),
            "created_by": actor,
            "execution_note": "T+1 开盘前需重新核验价格、股数、涨跌停和流动性；本意图不会自动成交。",
            "strategy_reason": row[13] or {},
        }
        cur.execute(
            """SELECT source_candidate_id,intent_id FROM portfolio.order_intent
                WHERE portfolio_id=%s AND source_candidate_id=%s""",
            (portfolio_id, candidate_id),
        )
        existing = cur.fetchone()
        if existing:
            conn.commit()
            return {"intent_id": str(existing[1]), "status": "PENDING_APPROVAL", "idempotent": True}
        cur.execute(
            """INSERT INTO portfolio.order_intent
               (intent_id,portfolio_id,snapshot_id,watchlist_id,ts_code,decision_date,
                intended_execution_date,direction,requested_shares,reference_price,
                stop_loss_price_raw,source_type,reason,source_candidate_id,
                source_ai_audit_id,source_candidate_hash)
               VALUES (%s,%s,%s,NULL,%s,%s,%s,%s,%s,%s,%s,'DAILY_SIGNAL',%s,%s,%s,%s)""",
            (intent_id, portfolio_id, row[4], row[7], decision_date, next_day[0], row[8],
             shares, row[10], row[11], _json_param(reason), candidate_id, audit_id,
             str(row[12]).strip()),
        )
        cur.execute(
            """INSERT INTO portfolio.audit_event
               (portfolio_id,event_date,event_type,intent_id,payload)
               VALUES (%s,%s,'DAILY_SIGNAL_INTENT_CREATED',%s,%s)""",
            (portfolio_id, decision_date, intent_id, _json_param(reason)),
        )
        conn.commit()
    return {"intent_id": intent_id, "status": "PENDING_APPROVAL", "idempotent": False}


def prepare_daily_signal_execution(portfolio_id: str, intent_id: str) -> dict[str, Any]:
    """用已落 Raw 的完整执行日 bar 对纸面意图做收盘后复核。

    该函数会使用全天成交量做流动性门禁，因此不是盘中实盘下单入口。
    It may resize a still-pending intent, but it never approves or fills it.
    Missing/invalid execution data returns a fail-closed rejection result and
    leaves the intent pending for an explicit human expiry/rejection.
    """
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """SELECT i.status,i.source_type,i.snapshot_id,i.ts_code,i.direction,
                      i.decision_date,i.intended_execution_date,i.requested_shares,
                      i.stop_loss_price_raw,i.source_candidate_id,i.reason,
                      r.strategy_parameters,c.eligible,c.candidate_hash
                 FROM portfolio.order_intent i
                 JOIN research.candidate c ON c.candidate_id=i.source_candidate_id
                 JOIN research.candidate_run r ON r.candidate_run_id=c.candidate_run_id
                WHERE i.intent_id=%s AND i.portfolio_id=%s
                FOR UPDATE""",
            (intent_id, portfolio_id),
        )
        row = cur.fetchone()
        if not row:
            raise ValueError("不存在的每日纸面意图")
        if row[0] != "PENDING_APPROVAL" or row[1] != "DAILY_SIGNAL":
            raise ValueError("只有 DAILY_SIGNAL/PENDING_APPROVAL 意图可以重新定价")
        candidate_id = str(row[9])
        packet = get_candidate_packet(candidate_id, portfolio_id=portfolio_id)
        cur.execute(
            """SELECT record_id,payload,payload_hash,fetched_at,batch_id
                 FROM tushare_market_raw_version
                WHERE dataset='daily' AND ts_code=%s AND trade_date=%s
                ORDER BY fetched_at DESC,record_id DESC LIMIT 1""",
            (row[3], row[6]),
        )
        source_quote = cur.fetchone()
        if not source_quote:
            return {"intent_id": intent_id, "eligible": False, "actual_shares": 0, "reject_reason": "缺少 T+1 Raw 执行行情，保持 PENDING_APPROVAL"}
        cur.execute(
            """SELECT market FROM research.stock_snapshot_daily
                WHERE snapshot_id=%s AND ts_code=%s""",
            (row[2], row[3]),
        )
        market_row = cur.fetchone()
        if not market_row:
            return {"intent_id": intent_id, "eligible": False, "actual_shares": 0, "reject_reason": "缺少 FROZEN Snapshot 市场分类，保持 PENDING_APPROVAL"}
        execution_market = market_row[0]
        checkpoint_key = row[6].strftime("%Y%m%d")
        cur.execute(
            """SELECT dataset,status,updated_at
                 FROM tushare_history_checkpoint
                WHERE dataset IN ('suspend_d_incremental','stk_limit_incremental')
                  AND checkpoint_key=%s""",
            (checkpoint_key,),
        )
        checkpoints = {item[0]: (item[1], item[2]) for item in cur.fetchall()}
        if any(
            checkpoints.get(dataset, (None,))[0] not in {"done", "empty"}
            for dataset in ("suspend_d_incremental", "stk_limit_incremental")
        ):
            return {"intent_id": intent_id, "eligible": False, "actual_shares": 0, "reject_reason": "T+1 停牌/涨跌停 Raw 同步未完成，保持 PENDING_APPROVAL"}
        cur.execute(
            """SELECT record_id,payload,payload_hash,fetched_at,batch_id
                 FROM tushare_stk_limit_raw
                WHERE ts_code=%s AND trade_date=%s
                ORDER BY fetched_at DESC,record_id DESC LIMIT 1""",
            (row[3], row[6]),
        )
        limit_source = cur.fetchone()
        if not limit_source:
            return {"intent_id": intent_id, "eligible": False, "actual_shares": 0, "reject_reason": "缺少 T+1 涨跌停 Raw，保持 PENDING_APPROVAL"}
        cur.execute(
            """SELECT record_id,payload_hash,fetched_at,batch_id
                 FROM tushare_suspend_d_raw
                WHERE ts_code=%s AND trade_date=%s AND suspend_type='S'
                ORDER BY fetched_at DESC,record_id DESC LIMIT 1""",
            (row[3], row[6]),
        )
        suspend_source = cur.fetchone()
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
        cur.execute(
            """UPDATE portfolio.order_intent
                  SET requested_shares=%s, reference_price=%s, reason=%s
                WHERE intent_id=%s AND portfolio_id=%s AND status='PENDING_APPROVAL'""",
            (actual_shares, open_price, _json_param(reason), intent_id, portfolio_id),
        )
        conn.commit()
    return {
        "intent_id": intent_id,
        "eligible": True,
        "planned_shares": int(row[7]),
        "actual_shares": actual_shares,
        "resized_at_execution": actual_shares != int(row[7]),
        "execution_price": open_price,
        "status": "PENDING_APPROVAL",
    }
