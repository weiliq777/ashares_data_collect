"""顺序执行财务 Raw 版本回填和三张 Standard 全量转换。"""
from data_collect.jobs.tushare_financial_raw_version_backfill import run as backfill
from data_collect.jobs.tushare_financial_raw_to_standard import run as convert


print({"version_backfill": backfill("all")}, flush=True)
print({"standard_conversion": convert("all")}, flush=True)
