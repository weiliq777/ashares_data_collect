"""ETF 基金信息快照(+changelog)。xtquant get_instrument_detail + akshare 规模/类型 合并。仿 a_share_instrument。"""
from __future__ import annotations

import logging
import sys
from typing import Dict, List, Tuple

import pandas as pd
from tqdm import tqdm

from data_collect.utils.akshare_utils import get_etf_spot, get_fund_name_map
from data_collect.utils.date_utils import is_market_day
from data_collect.data_access import DataAccessKernel, RunContext
from data_collect.data_access.policy_catalog import build_default_registry
from data_collect.data_access.repository.etf_info_repository import EtfInfoRepository
from data_collect.utils.df_utils import normalize_trade_date
from data_collect.utils.etf_utils import to_bare_code, with_suffix
from data_collect.utils.notify import send_dingtalk
from data_collect.utils.xtquant_utils import require_xtdata, get_etf_codes

logger = logging.getLogger(__name__)
INFO_TABLE = "etf_info"
CHANGELOG_TABLE = "etf_info_changelog"

# etf_info 列 -> 类型
_COLUMNS = {
    "name": "TEXT", "ext_name": "TEXT", "exchange": "TEXT", "list_date": "TEXT",
    "total_volume": "DOUBLE PRECISION", "price_tick": "DOUBLE PRECISION",
    "fund_type": "TEXT", "short_name": "TEXT",
    "total_mv": "DOUBLE PRECISION", "latest_share": "DOUBLE PRECISION",
}

# 每日随市值波动的字段：入快照（保留当前值）但**不记 changelog**，避免日志膨胀（仿 instrument 排除每日变动字段）
_DYNAMIC_FIELDS = {"total_mv", "latest_share"}

_tables_ready = False


def _ensure_tables() -> None:
    global _tables_ready
    if _tables_ready:
        return
    # Tables are provisioned by migrations; business jobs cannot create them.
    _tables_ready = True


def build_info_record(detail: dict, spot_row: dict | None, name_row: tuple | None) -> dict:
    """合并 xtquant 合约详情 + akshare 规模/类型 -> etf_info 单条（不含 code）。"""
    def g(d, k, default=None):
        return d.get(k, default) if isinstance(d, dict) else default
    rec = {
        "name": g(detail, "InstrumentName"),
        "ext_name": g(detail, "ExtendName"),
        "exchange": g(detail, "ExchangeID"),
        "list_date": g(detail, "OpenDate"),
        "total_volume": g(detail, "TotalVolume") or g(detail, "TotalVolumn"),
        "price_tick": g(detail, "PriceTick"),
        "fund_type": name_row[1] if name_row else None,
        "short_name": name_row[0] if name_row else None,
        "total_mv": None, "latest_share": None,
    }
    if spot_row is not None:
        rec["total_mv"] = spot_row.get("总市值")
        rec["latest_share"] = spot_row.get("最新份额")
    return rec


def _collect(limit_stocks=None) -> Dict[str, dict]:
    xtdata = require_xtdata()
    codes = get_etf_codes()
    if limit_stocks:
        codes = codes[:limit_stocks]
    spot = get_etf_spot()
    spot_map = {str(c).zfill(6): row for c, row in zip(spot["代码"], spot.to_dict("records"))}
    name_map = get_fund_name_map()
    out = {}
    for code in tqdm(codes, desc="ETF信息", unit="只", file=sys.stdout):
        bare = to_bare_code(code)
        try:
            detail = xtdata.get_instrument_detail(with_suffix(bare), iscomplete=True) or {}
            out[bare] = build_info_record(detail, spot_map.get(bare), name_map.get(bare))
        except Exception as exc:
            logger.debug(f"ETF信息 {bare} 失败: {exc}")
    return out


def _load_existing(codes: List[str]) -> Dict[str, dict]:
    if not codes:
        return {}
    kernel = DataAccessKernel(policy_registry=build_default_registry())
    context = RunContext(run_id="etf-info-read", job_name="etf_info", code_version="unknown", config_hash="unknown", pipeline_version="data-access-v1")
    try:
        return EtfInfoRepository(kernel).existing(codes, context)
    finally:
        kernel.close()


def _upsert_and_log(details: Dict[str, dict], run_date: str) -> Tuple[int, int, int]:
    if not details:
        return 0, 0, 0
    existing = _load_existing(list(details.keys()))
    today = pd.to_datetime(run_date).date()
    fields = list(_COLUMNS.keys())
    upserts, changelogs = [], []
    for code, new in details.items():
        old = existing.get(code)
        if old is None:
            upserts.append((code, new)); continue
        changed = {f: (str(old.get(f)), str(new.get(f))) for f in fields if str(new.get(f)) != str(old.get(f))}
        if changed:
            upserts.append((code, new))
            for f, (o, n) in changed.items():
                if f not in _DYNAMIC_FIELDS:   # 市值/份额每日波动，只入快照不记 changelog
                    changelogs.append((code, today, f, o, n))
    upsert_rows = [{"code": code, **new} for code, new in upserts]
    changelog_rows = [{"code": code, "changed_at": changed, "field_name": field, "old_value": old, "new_value": new} for code, changed, field, old, new in changelogs]
    kernel = DataAccessKernel(policy_registry=build_default_registry())
    context = RunContext(run_id=f"etf-info-{run_date}", job_name="etf_info", code_version="unknown", config_hash="unknown", pipeline_version="data-access-v1")
    try:
        EtfInfoRepository(kernel).persist(upsert_rows, changelog_rows, context)
    finally:
        kernel.close()
    return new_cnt, len(upserts) - new_cnt, len(changelogs)


def run(run_date: str, **kwargs) -> str:
    trade_date = normalize_trade_date(run_date)
    if not is_market_day(trade_date):
        return f"{trade_date} 非交易日，ETF信息跳过。"
    _ensure_tables()
    details = _collect(kwargs.get("limit_stocks"))
    new, upd, chg = _upsert_and_log(details, trade_date)
    return f"{trade_date} ETF信息完成，新增 {new}，更新 {upd}，变更 {chg} 字段。"


def run_backfill(start_date: str, end_date: str, limit_stocks=None) -> str:
    """快照无历史，backfill 等同建 baseline。"""
    msg = run(end_date, limit_stocks=limit_stocks)
    send_dingtalk(msg)
    return msg
