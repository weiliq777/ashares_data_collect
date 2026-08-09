from __future__ import annotations

import argparse
from _common import emit
from data_collect.decision import list_candidates


parser = argparse.ArgumentParser()
parser.add_argument("--candidate-run-id", required=True)
parser.add_argument("--eligible-only", action="store_true")
args = parser.parse_args()
emit(list_candidates(args.candidate_run_id, eligible_only=args.eligible_only))
