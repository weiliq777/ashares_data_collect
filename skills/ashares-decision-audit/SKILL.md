---
name: ashares-decision-audit
description: Review one FROZEN A-share daily candidate using deterministic snapshot data, historical prices, and as-of evidence, then return a strict AI audit JSON. Use for manual candidate review only; it never places orders or approves a Portfolio intent.
---

# A-share decision audit

Use this skill only with a `candidate_id` produced by the daily candidate run.
Never substitute a live stock-code lookup for the candidate packet.

1. Run `scripts/run_daily_candidates.py` only when a FROZEN Snapshot has been
   selected and a new daily candidate run is wanted.
2. Run `scripts/list_candidates.py --candidate-run-id ...` to obtain the review
   queue and candidate IDs. Run `scripts/get_candidate_packet.py --candidate-id ...`.
   The packet is the
   source of truth for the decision date, strategy facts, risk sizing, and
   evidence identifiers.
3. Run `scripts/get_stock_history.py` or `scripts/get_chart_data.py` for the candidate code when trend or
   price-path context is needed. Use only rows on or before the packet date.
4. Review positive factors, contrary evidence, missing data, liquidity, stop,
   fees, and the fact that planned shares are not a fill.
5. Return JSON only, matching the `ai-audit-v1` schema in
   `references/audit_schema.md`. Do not invent
   evidence IDs, future facts, a fill price, or a different share count.
6. Store the JSON with `scripts/store_ai_audit.py` only after checking that its
   `candidate_id` and `input_hash` exactly match the packet. This creates an
   audit record, not a trade.
7. A human may then record APPROVE/REJECT/DEFER. APPROVE creates only a
   `DAILY_SIGNAL/PENDING_APPROVAL` paper intent; a separate human approval and
   explicit paper fill are still required.
8. After the next-open bar exists, run the execution-preparation command. It
   may resize a still-pending intent using the actual open and liquidity, but
   it never approves or fills the intent.

The skill is deliberately read-first and fail-closed. It does not call a
broker, scheduler, news crawler, or automatic trading path.

See `references/api_contract.md` for the command contract and
`references/audit_schema.md` for the exact output schema.
