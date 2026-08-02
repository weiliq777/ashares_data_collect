"""验证 Tushare 兼容服务的基础数据接口。

Token 只从 TUSHARE_API_KEY 环境变量读取，严禁写入源码或日志。
"""
from __future__ import annotations

import os
import sys
import time
from datetime import datetime, timedelta


API_URL = "https://teajoin.com"
TEST_CODE = "000001.SZ"


def build_client():
    try:
        import tushare as ts
    except ImportError as exc:
        raise RuntimeError("缺少 tushare，请先执行: uv pip install tushare") from exc

    token = os.environ.get("TUSHARE_API_KEY", "").strip()
    if not token:
        raise RuntimeError("请先设置环境变量 TUSHARE_API_KEY")

    ts.set_token(token)
    pro = ts.pro_api()
    # Tushare SDK 的兼容服务配置方式，按 Tushare_doc/使用指南.md。
    pro._DataApi_token = token
    pro._DataApi__http_url = API_URL
    return pro


def call(name: str, fn, **kwargs):
    """调用接口并只输出安全摘要。"""
    started = time.perf_counter()
    try:
        df = fn(**kwargs)
        elapsed = time.perf_counter() - started
        columns = list(df.columns) if df is not None else []
        rows = 0 if df is None else len(df)
        print(f"[OK] {name}: rows={rows}, columns={columns}, seconds={elapsed:.2f}")
        return df
    except Exception as exc:  # noqa: BLE001
        elapsed = time.perf_counter() - started
        print(f"[FAIL] {name}: {type(exc).__name__}: {str(exc)[:300]}, seconds={elapsed:.2f}")
        return None


def main() -> int:
    try:
        pro = build_client()
    except Exception as exc:
        print(f"[ERROR] {exc}")
        return 2

    today = datetime.now().strftime("%Y%m%d")
    start = (datetime.now() - timedelta(days=30)).strftime("%Y%m%d")
    passed = 0
    total = 0

    checks = [
        ("stock_basic", pro.stock_basic, {"exchange": "", "list_status": "L", "fields": "ts_code,symbol,name,area,industry,list_date"}),
        ("trade_cal", pro.trade_cal, {"exchange": "SSE", "start_date": start, "end_date": today, "fields": "exchange,cal_date,is_open,pretrade_date"}),
        ("daily", pro.daily, {"ts_code": TEST_CODE, "start_date": start, "end_date": today, "fields": "ts_code,trade_date,open,high,low,close,vol,amount"}),
        ("daily_basic", pro.daily_basic, {"ts_code": TEST_CODE, "start_date": start, "end_date": today, "fields": "ts_code,trade_date,turnover_rate,pe,pb,total_mv,circ_mv"}),
        ("income", pro.income, {"ts_code": TEST_CODE, "period": "20241231", "fields": "ts_code,ann_date,end_date,revenue,n_income"}),
        ("balancesheet", pro.balancesheet, {"ts_code": TEST_CODE, "period": "20241231", "fields": "ts_code,ann_date,end_date,total_assets,total_liab"}),
        ("cashflow", pro.cashflow, {"ts_code": TEST_CODE, "period": "20241231", "fields": "ts_code,ann_date,end_date,n_cashflow_act"}),
        ("fina_indicator", pro.fina_indicator, {"ts_code": TEST_CODE, "period": "20241231", "fields": "ts_code,ann_date,end_date,roe,roa,debt_to_assets,ocf_to_or"}),
    ]

    for name, fn, kwargs in checks:
        total += 1
        if call(name, fn, **kwargs) is not None:
            passed += 1
        time.sleep(0.25)

    print(f"SUMMARY: {passed}/{total} interfaces returned successfully")
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
