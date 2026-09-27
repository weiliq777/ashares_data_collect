"""Build the reusable daily technical feature set used by wave3."""
from __future__ import annotations

from datetime import date, timedelta

import pandas as pd

from data_collect.data_access import DataAccessKernel, RunContext
from data_collect.data_access.policy_catalog import build_default_registry
from data_collect.data_access.repository.wave3_repository import Wave3Repository
from data_collect.features.technical_daily import calculate_technical_features, to_feature_rows

LOOKBACK_DAYS = 60
OUTPUT_CHUNK_DAYS = 7
STOCK_BATCH_SIZE = 250


def _service(run_id: str):
    kernel = DataAccessKernel(policy_registry=build_default_registry())
    context = RunContext(run_id=run_id, job_name="technical_daily", code_version="unknown",
                         config_hash="unknown", pipeline_version="data-access-v1")
    return kernel, Wave3Repository(kernel), context


def _windows(start: date, end: date):
    cursor = start
    while cursor <= end:
        chunk_end = min(end, cursor + timedelta(days=OUTPUT_CHUNK_DAYS - 1))
        yield cursor, chunk_end
        cursor = chunk_end + timedelta(days=1)


def build(output_start: str | date, output_end: str | date, *, run_id: str = "technical-daily") -> int:
    start = date.fromisoformat(output_start) if isinstance(output_start, str) else output_start
    end = date.fromisoformat(output_end) if isinstance(output_end, str) else output_end
    if start > end:
        raise ValueError("output_start cannot be after output_end")
    kernel, repository, context = _service(run_id)
    try:
        written = 0
        for chunk_start, chunk_end in _windows(start, end):
            input_start = chunk_start - timedelta(days=LOOKBACK_DAYS * 2)
            codes = repository.daily_kline_stock_codes(chunk_start, chunk_end, context)
            for offset in range(0, len(codes), STOCK_BATCH_SIZE):
                code_batch = codes[offset : offset + STOCK_BATCH_SIZE]
                prices = pd.DataFrame(
                    repository.daily_kline_for_codes(
                        input_start, chunk_end, code_batch, context
                    )
                )
                if prices.empty:
                    continue
                adjustment_factors = pd.DataFrame(
                    repository.price_adjustment_factors(
                        code_batch, context, start_date=input_start, end_date=chunk_end
                    )
                )
                expected = prices[["stock_code", "trade_date"]].drop_duplicates()
                actual = adjustment_factors[["stock_code", "trade_date"]].drop_duplicates()
                missing = expected.merge(actual, on=["stock_code", "trade_date"], how="left", indicator=True)
                missing = missing.loc[missing["_merge"].eq("left_only")]
                if not missing.empty:
                    sample = missing[["stock_code", "trade_date"]].head(5).to_dict("records")
                    raise RuntimeError(
                        "feature.price_adjustment_factor_asof coverage is incomplete; "
                        f"missing={len(missing)} sample={sample}"
                    )
                features = calculate_technical_features(prices, adjustment_factors)
                features = features[
                    (features["trade_date"] >= chunk_start)
                    & (features["trade_date"] <= chunk_end)
                ]
                written += repository.upsert_technical_features(
                    to_feature_rows(features), chunk_start, chunk_end, context
                )
                del prices, adjustment_factors, features
        return written
    finally:
        kernel.close()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("output_start")
    parser.add_argument("output_end")
    args = parser.parse_args()
    print(build(args.output_start, args.output_end))
