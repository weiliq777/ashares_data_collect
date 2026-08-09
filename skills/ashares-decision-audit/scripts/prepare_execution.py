from __future__ import annotations

import argparse
from _common import emit
from data_collect.decision import prepare_daily_signal_execution


parser = argparse.ArgumentParser()
parser.add_argument("--portfolio-id", required=True)
parser.add_argument("--intent-id", required=True)
args = parser.parse_args()
emit(prepare_daily_signal_execution(args.portfolio_id, args.intent_id))
