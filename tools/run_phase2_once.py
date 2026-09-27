"""Run one explicitly bounded phase-2 Raw -> Standard/PIT/Feature increment."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from data_collect.jobs.tushare_phase2_incremental import run


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("run_date", help="cutoff date, YYYYMMDD or YYYY-MM-DD")
    parser.add_argument("--lookback-days", type=int, default=1)
    parser.add_argument("--event-lookback-days", type=int, default=30)
    parser.add_argument("--batch-pause", type=float, default=0.2)
    args = parser.parse_args()
    print(
        run(
            run_date=args.run_date,
            lookback_days=args.lookback_days,
            event_lookback_days=args.event_lookback_days,
            batch_pause=args.batch_pause,
            rebuild_standard=True,
        ),
        flush=True,
    )


if __name__ == "__main__":
    main()
