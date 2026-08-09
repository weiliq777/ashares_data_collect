"""阶段二增量后的派生依赖重建编排。

该模块只读 Raw/Standard，按依赖顺序重建，不访问 Tushare，也不修改 Raw。
当前优先保证一致性，采用可审计的全表重建；后续可在不改变依赖关系的前提下优化为分区重建。
"""
from __future__ import annotations


def rebuild_after_market() -> dict:
    from data_collect.jobs.tushare_phase2_standard import run as standard_run
    from data_collect.jobs.tushare_phase2_features import run as feature_run

    standard = standard_run("status")
    standard.update(standard_run("universe"))
    standard.update(standard_run("tradeability"))
    features = {}
    features.update(feature_run("price"))
    features.update(feature_run("price_v2"))
    features.update(feature_run("valuation"))
    return {"standard": standard, "features": features}


def rebuild_after_phase2(datasets: set[str]) -> dict:
    from data_collect.jobs.tushare_phase2_incremental import _rebuild_standard

    # _rebuild_standard 负责 Standard/PIT/Feature 的依赖顺序；保留一个统一入口
    # 方便日后调度器和质量门禁调用。
    return _rebuild_standard(datasets)


def rebuild_after_financial() -> dict:
    from data_collect.jobs.tushare_financial_incremental import _rebuild_financial_derived

    _rebuild_financial_derived()
    return {"status": "rebuilt"}
