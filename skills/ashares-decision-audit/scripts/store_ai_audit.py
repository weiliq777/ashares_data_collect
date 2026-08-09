from __future__ import annotations

import argparse
import json
from _common import emit
from data_collect.decision import store_ai_audit


parser = argparse.ArgumentParser()
parser.add_argument("--candidate-id", required=True)
parser.add_argument("--json-file", required=True)
parser.add_argument("--portfolio-id")
args = parser.parse_args()
with open(args.json_file, "r", encoding="utf-8") as handle:
    payload = json.load(handle)
emit(store_ai_audit(args.candidate_id, payload, portfolio_id=args.portfolio_id))
