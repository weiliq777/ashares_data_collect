"""顺序执行财务 Raw 版本回填和三张 Standard 全量转换。"""
from data_collect.jobs.tushare_financial_raw_version_backfill import run as backfill
from data_collect.jobs.tushare_financial_raw_to_standard import run as convert
from data_collect.jobs.tushare_fina_indicator_version import run as indicator_run


print({"version_backfill": backfill("all")}, flush=True)
print({"standard_conversion": convert("all")}, flush=True)
print({"fina_indicator_version_and_standard": indicator_run()}, flush=True)
