from __future__ import annotations

import argparse
from _common import emit
from data_collect.decision import create_daily_signal_intent


parser = argparse.ArgumentParser()
parser.add_argument("--candidate-id", required=True)
parser.add_argument("--audit-id", required=True)
parser.add_argument("--portfolio-id", required=True)
parser.add_argument("--actor", required=True)
args = parser.parse_args()
emit(create_daily_signal_intent(
    args.candidate_id, audit_id=args.audit_id,
    portfolio_id=args.portfolio_id, actor=args.actor,
))
