from __future__ import annotations

import argparse
from _common import emit
from data_collect.decision import get_candidate_packet


parser = argparse.ArgumentParser()
parser.add_argument("--candidate-id", required=True)
parser.add_argument("--portfolio-id")
args = parser.parse_args()
emit(get_candidate_packet(args.candidate_id, portfolio_id=args.portfolio_id))
