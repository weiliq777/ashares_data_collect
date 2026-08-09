from __future__ import annotations

import argparse
from datetime import date
from _common import emit
from data_collect.decision import get_stock_chart_data


parser = argparse.ArgumentParser()
parser.add_argument("--ts-code", required=True)
parser.add_argument("--end-date", required=True)
parser.add_argument("--limit", type=int, default=120)
args = parser.parse_args()
emit(get_stock_chart_data(args.ts_code, date.fromisoformat(args.end_date), limit=args.limit))
