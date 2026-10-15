"""Bind an explicit late-arriving benchmark repair to a BUILDING Dataset."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from uuid import uuid4

from data_collect.data_access import DataAccessKernel, RunContext
from data_collect.data_access.policy_catalog import build_default_registry
from data_collect.data_access.repository.dataset_repository import DatasetRepository
from data_collect.jobs.tushare_job_lock import warehouse_write_job


def _service():
    kernel = DataAccessKernel(policy_registry=build_default_registry())
    context = RunContext(
        run_id=f"benchmark-repair-{uuid4().hex}",
        job_name="repair_backtest_benchmark",
        code_version="unknown",
        config_hash="unknown",
        pipeline_version="data-access-v1",
    )
    return kernel, DatasetRepository(kernel), context


@warehouse_write_job
def repair(dataset_id: str) -> dict:
    kernel, repository, context = _service()
    try:
        with repository.lifecycle_lock(dataset_id):
            result = repository.bind_benchmark_source_repair(dataset_id, context)
        if result is None:
            return {"dataset_id": dataset_id, "status": "NO_REPAIR_REQUIRED"}
        return {
            "dataset_id": dataset_id,
            "status": "BOUND",
            "schema_version": result["schema_version"],
            "benchmark_code": result["benchmark_code"],
            "repair_dates": [entry["trade_date"] for entry in result["repairs"]],
            "repair_count": len(result["repairs"]),
            "bound_at": datetime.now(timezone.utc).isoformat(),
        }
    finally:
        kernel.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Bind late-arriving benchmark rows to a Dataset")
    parser.add_argument("--dataset-id", required=True)
    args = parser.parse_args()
    print(repair(args.dataset_id))


if __name__ == "__main__":
    main()
