"""只读验证 Tushare 是否支持全市场计数及近五年历史数据。

用法（PowerShell）：
    $env:TUSHARE_API_KEY = "你的Token"
    python dev_doc/tushare_validation/validate_history.py

脚本只请求接口并输出摘要，不写 PostgreSQL、不保存 Token。
"""
from __future__ import annotations

import os
import sys
import time
from datetime import date


API_URL = "https://teajoin.com"
TEST_CODE = "000001.SZ"


def client():
    import tushare as ts

    token = os.environ.get("TUSHARE_API_KEY", "").strip()
    if not token:
        raise RuntimeError("未设置 TUSHARE_API_KEY")
    ts.set_token(token)
    pro = ts.pro_api()
    pro._DataApi_token = token
    pro._DataApi__http_url = API_URL
    return pro


def request(label, fn, **kwargs):
    started = time.perf_counter()
    try:
        df = fn(**kwargs)
        rows = 0 if df is None else len(df)
        cols = [] if df is None else list(df.columns)
        print(f"[OK] {label}: rows={rows}, columns={cols}, seconds={time.perf_counter()-started:.2f}")
        if rows:
            print(df.head(2).to_string(index=False))
        return df
    except Exception as exc:  # noqa: BLE001
        print(f"[FAIL] {label}: {type(exc).__name__}: {str(exc)[:300]}")
        return None


def main() -> int:
    try:
        pro = client()
    except Exception as exc:
        print(f"[ERROR] {exc}")
        return 2

    end = date.today().strftime("%Y%m%d")
    start = date(date.today().year - 5, date.today().month, date.today().day).strftime("%Y%m%d")
    period = f"{date.today().year - 1}1231"
    results = []

    # stock_basic 返回的是股票清单；按状态分别统计，避免把退市股票漏算。
    for status in ("L", "D", "P"):
        df = request(
            f"stock_basic/list_status={status}",
            pro.stock_basic,
            exchange="",
            list_status=status,
            fields="ts_code,symbol,name,list_status,list_date,delist_date",
        )
        results.append(df is not None)
        if df is not None:
            print(f"  股票数量({status}) = {len(df)}")

    # 5 年历史能力：日线、每日估值、财务指标、现金流。
    checks = [
        ("daily/5y", pro.daily, dict(ts_code=TEST_CODE, start_date=start, end_date=end,
                                      fields="ts_code,trade_date,open,high,low,close,vol,amount")),
        ("daily_basic/5y", pro.daily_basic, dict(ts_code=TEST_CODE, start_date=start, end_date=end,
                                                 fields="ts_code,trade_date,turnover_rate,pe,pb,total_mv,circ_mv")),
        ("fina_indicator/5y", pro.fina_indicator, dict(ts_code=TEST_CODE, start_date=start, end_date=end,
                                                        fields="ts_code,ann_date,end_date,roe,roa,debt_to_assets,ocf_to_or")),
        ("cashflow/5y", pro.cashflow, dict(ts_code=TEST_CODE, start_date=start, end_date=end,
                                           fields="ts_code,ann_date,end_date,n_cashflow_act")),
        ("income/period", pro.income, dict(ts_code=TEST_CODE, period=period,
                                           fields="ts_code,ann_date,end_date,revenue,n_income")),
        ("balancesheet/period", pro.balancesheet, dict(ts_code=TEST_CODE, period=period,
                                                       fields="ts_code,ann_date,end_date,total_assets,total_liab")),
    ]
    for label, fn, kwargs in checks:
        results.append(request(label, fn, **kwargs) is not None)
        time.sleep(0.5)

    passed = sum(results)
    print(f"SUMMARY: {passed}/{len(results)} checks returned successfully")
    print(f"HISTORY_WINDOW: {start} -> {end}; SAMPLE: {TEST_CODE}")
    return 0 if passed == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())
