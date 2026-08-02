from __future__ import annotations

from data_collect.providers.tushare_client import call_api, get_pro
from data_collect.normalize.tushare_daily import normalize_daily
from data_collect.utils.db import save_to_postgres
from data_collect.utils.date_utils import is_market_day
from data_collect.utils.df_utils import normalize_trade_date


def run(run_date: str, **kwargs) -> str:
    trade_date = normalize_trade_date(run_date)
    if not is_market_day(trade_date):
        return f"{trade_date} 非交易日，Tushare日线跳过"
    df = call_api(get_pro().daily, trade_date=trade_date)
    out = normalize_daily(df)
    tried, inserted = save_to_postgres(out, pre_aligned_df=out, table_name="daily_kline")
    return f"Tushare日线完成：{inserted}/{tried}，日期={trade_date}"


def run_backfill(start_date: str, end_date: str, limit_stocks=None) -> str:
    pro = get_pro()
    days = call_api(pro.trade_cal, exchange="SSE", start_date=start_date, end_date=end_date, is_open="1")
    total = tried = inserted = 0
    for day in days["cal_date"].tolist() if days is not None and not days.empty else []:
        out = normalize_daily(call_api(pro.daily, trade_date=str(day)))
        if limit_stocks and not out.empty:
            out = out.head(int(limit_stocks))
        t, i = save_to_postgres(out, pre_aligned_df=out, table_name="daily_kline")
        total += 1; tried += t; inserted += i
    return f"Tushare日线补历史完成：{total}个交易日，{inserted}/{tried}"
