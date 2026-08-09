"""Batch-freeze a validated Research Snapshot series."""
from __future__ import annotations

import argparse

from data_collect.jobs.research_snapshot import freeze_snapshots_batch
from data_collect.utils.db import get_connection


def main() -> None:
    parser = argparse.ArgumentParser(description="批量冻结已验证的 Research Snapshot")
    parser.add_argument("--baseline-tag", required=True)
    parser.add_argument("--schema-version", required=True)
    parser.add_argument("--code-archive-path")
    args = parser.parse_args()

    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """SELECT snapshot_id
                 FROM research.snapshot_manifest
                WHERE data_baseline_tag=%s
                  AND schema_version=%s
                  AND status='VALIDATED'
                ORDER BY research_cutoff_date""",
            (args.baseline_tag, args.schema_version),
        )
        snapshot_ids = [str(row[0]) for row in cur.fetchall()]
    if not snapshot_ids:
        raise RuntimeError("没有可冻结的 VALIDATED Snapshot")
    print(f"freezing {len(snapshot_ids)} snapshots", flush=True)
    freeze_snapshots_batch(snapshot_ids, args.code_archive_path)
    print(f"freeze complete {len(snapshot_ids)} snapshots", flush=True)


if __name__ == "__main__":
    main()
