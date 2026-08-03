"""后台顺序运行四张财务 Raw 表。"""
from data_collect.jobs.tushare_financial_raw_init import run


for dataset in ("fina_indicator", "income", "balancesheet", "cashflow"):
    print(run(dataset, batch_size=25, batch_pause=5.0), flush=True)
