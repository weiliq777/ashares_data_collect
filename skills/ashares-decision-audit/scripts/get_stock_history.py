from __future__ import annotations

import argparse
from datetime import date
from _common import emit
from data_collect.decision import get_stock_history


parser = argparse.ArgumentParser()
parser.add_argument("--ts-code", required=True)
parser.add_argument("--end-date", required=True)
parser.add_argument("--start-date")
parser.add_argument("--limit", type=int, default=120)
args = parser.parse_args()
emit(get_stock_history(
    args.ts_code,
    date.fromisoformat(args.end_date),
    start_date=date.fromisoformat(args.start_date) if args.start_date else None,
    limit=args.limit,
))
