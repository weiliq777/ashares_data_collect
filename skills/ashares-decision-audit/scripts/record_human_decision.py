from __future__ import annotations

import argparse
from _common import emit
from data_collect.decision import record_human_decision


parser = argparse.ArgumentParser()
parser.add_argument("--candidate-id", required=True)
parser.add_argument("--audit-id", required=True)
parser.add_argument("--decision", required=True, choices=("APPROVE", "REJECT", "DEFER"))
parser.add_argument("--actor", required=True)
parser.add_argument("--reason")
parser.add_argument("--portfolio-id")
args = parser.parse_args()
emit(record_human_decision(
    args.candidate_id, audit_id=args.audit_id, decision=args.decision,
    actor=args.actor, reason=args.reason, portfolio_id=args.portfolio_id,
))
