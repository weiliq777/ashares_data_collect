from __future__ import annotations

import argparse
from _common import emit
from data_collect.decision import create_candidate_run


parser = argparse.ArgumentParser()
parser.add_argument("--snapshot-id", required=True)
parser.add_argument("--portfolio-id")
args = parser.parse_args()
emit(create_candidate_run(args.snapshot_id, portfolio_id=args.portfolio_id))
