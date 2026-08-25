"""Foundation v2 quality-gate orchestration without direct DB access."""
from __future__ import annotations

import argparse
import json
from datetime import date
from uuid import uuid4

from data_collect.data_access import DataAccessKernel, RunContext
from data_collect.data_access.policy_catalog import build_default_registry
from data_collect.data_access.repository.foundation_quality_repository import FoundationQualityRepository


def inspect_foundation_v2(
    start_date: date,
    end_date: date,
    codes: list[str] | None = None,
    company_table: str = "feature.company_financial_v2",
    valuation_table: str = "feature.valuation_daily_v1",
) -> dict:
    kernel = DataAccessKernel(policy_registry=build_default_registry())
    context = RunContext(
        run_id=f"foundation-quality-{uuid4().hex}", job_name="tushare_foundation_quality",
        code_version="unknown", config_hash="unknown", pipeline_version="data-access-v1",
    )
    try:
        return FoundationQualityRepository(kernel).inspect_foundation_v2(
            start_date, end_date, context, codes, company_table, valuation_table,
        )
    finally:
        kernel.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--codes", nargs="*")
    parser.add_argument("--company-table", default="feature.company_financial_v2")
    parser.add_argument("--valuation-table", default="feature.valuation_daily_v1")
    args = parser.parse_args()
    print(json.dumps(inspect_foundation_v2(
        date.fromisoformat(args.start_date), date.fromisoformat(args.end_date),
        args.codes, args.company_table, args.valuation_table,
    ), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
