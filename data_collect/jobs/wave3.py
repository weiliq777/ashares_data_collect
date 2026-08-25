"""
3浪3选股任务

基于日线K线数据（daily_kline表）计算技术指标，筛选符合条件的股票，
写入 wave3_stocks 表，通过钉钉和邮件发送结果。

选股条件：
- K(KDJ) > 80
- WR(10日) < 20 且 WR(20日) < 20（威廉超卖）
- RSI(9日) > 70（相对强弱超买）
- 当天有交易（volume > 0）
- 非 ST / 退市股票

附加存储（不作为筛选条件，供 web 端自行组合过滤）：
- 总市值（亿元）：每日任务从 instrument_info，backfill 从 fin_capital
- 已上市天数（自然天）：从 instrument_info.OpenDate 计算
"""

from __future__ import annotations

import logging
import os
import smtplib
import tempfile
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email import encoders
from typing import List

import numpy as np
import pandas as pd
from tqdm import tqdm

from data_collect.config import _load_config
from data_collect.utils.date_utils import is_market_day, date_range, add_mark_day
from data_collect.data_access import DataAccessKernel, RunContext
from data_collect.data_access.policy_catalog import build_default_registry
from data_collect.data_access.repository.wave3_repository import Wave3Repository
from data_collect.utils.df_utils import normalize_trade_date
from data_collect.utils.notify import send_dingtalk

logger = logging.getLogger(__name__)

TABLE_NAME = "wave3_stocks"
HISTORY_DAYS = 60  # 指标计算所需的历史天数缓冲


def _service(label: str):
    kernel = DataAccessKernel(policy_registry=build_default_registry())
    context = RunContext(run_id=f"wave3-{label}", job_name="wave3", code_version="unknown", config_hash="unknown", pipeline_version="data-access-v1")
    return kernel, Wave3Repository(kernel), context


# ======== 配置 ========

def _get_wave3_config() -> dict:
    cfg = _load_config()
    return cfg.get("wave3", {})


# ======== 数据读取 ========

def _fetch_technical_features(end_date: str, days: int = HISTORY_DAYS) -> pd.DataFrame:
    """Read precomputed foundation technical features through Wave3Repository."""
    start_date = add_mark_day(end_date, -days)
    kernel, repository, context = _service("kline")
    try:
        return pd.DataFrame(repository.technical_features(pd.to_datetime(start_date).date(), pd.to_datetime(end_date).date(), context))
    finally:
        kernel.close()


def _prepare_indicators(trade_date: str) -> pd.DataFrame:
    """Read the foundation indicator output for the screening window."""
    return _fetch_technical_features(trade_date, HISTORY_DAYS)


def _fetch_stock_summary() -> pd.DataFrame:
    kernel, repository, context = _service("summary")
    try:
        return pd.DataFrame(repository.stock_summary(context))
    except Exception as exc:
        logger.warning(f"读取 股票全部信息汇总 失败: {exc}")
        return pd.DataFrame()
    finally:
        kernel.close()


def _fetch_historical_capital() -> pd.DataFrame:
    kernel, repository, context = _service("capital")
    try:
        return pd.DataFrame(repository.historical_capital(context))
    except Exception as exc:
        logger.warning(f"读取 fin_capital 失败（表可能不存在）: {exc}")
        return pd.DataFrame()
    finally:
        kernel.close()




def _compute_for_date(
    df: pd.DataFrame,
    trade_date,
    summary_df: pd.DataFrame | None = None,
    capital_df: dict | None = None,
) -> pd.DataFrame:
    """筛选指定交易日符合条件的股票。

    Parameters
    ----------
    summary_df : "股票全部信息汇总" 数据（上市日期、总股本万股、股票名称）
    capital_df : 预分组的历史股本 dict[stock_code → DataFrame]，backfill 时传入
    """
    df_target = df[df["trade_date"] == trade_date].copy()
    if df_target.empty:
        return pd.DataFrame()

    # 合并股票汇总信息
    if summary_df is None:
        summary_df = _fetch_stock_summary()
    if not summary_df.empty:
        df_target = df_target.merge(summary_df, on="stock_code", how="left")
    else:
        df_target["list_date"] = None
        df_target["total_capital_wan"] = np.nan
        df_target["stock_name"] = None
        df_target["industry"] = None

    # 排除 ST / 退市
    st_mask = df_target["stock_name"].fillna("").str.contains("ST|退")
    df_target = df_target[~st_mask]

    # 计算已上市天数（上市日期来自"股票全部信息汇总"）
    trade_dt = pd.to_datetime(str(trade_date))
    def _calc_list_days(x):
        if pd.isna(x):
            return None
        s = str(x).strip()
        if len(s) != 8 or not s.isdigit():
            return None
        try:
            return (trade_dt - pd.to_datetime(s, format="%Y%m%d")).days
        except Exception:
            return None
    df_target["list_days"] = df_target["list_date"].apply(_calc_list_days)

    # 计算总市值（亿元）— 用不复权收盘价
    # 总股本优先级：fin_capital 历史值 > "股票全部信息汇总" 当前值
    close_num = pd.to_numeric(df_target["close_raw"], errors="coerce")

    if capital_df:
        # backfill：从预分组的 capital_df 取 report_date <= trade_date 的最新总股本
        trade_d = trade_dt.date() if hasattr(trade_dt, 'date') else trade_dt
        latest_cap = {}
        for code in df_target["stock_code"].unique():
            grp = capital_df.get(code)
            if grp is None:
                continue
            mask = grp["report_date"] <= trade_d
            if mask.any():
                latest_cap[code] = grp.loc[mask, "total_capital"].iloc[-1]
        if latest_cap:
            hist_capital = df_target["stock_code"].map(latest_cap)
            df_target["market_cap"] = pd.to_numeric(hist_capital, errors="coerce") * close_num / 1e8
        else:
            df_target["market_cap"] = np.nan

        # fallback：fin_capital 无记录的用汇总表（万股 × 10000 = 股）
        missing = df_target["market_cap"].isna()
        if missing.any():
            fallback = pd.to_numeric(df_target.loc[missing, "total_capital_wan"], errors="coerce") * 10000
            df_target.loc[missing, "market_cap"] = fallback * close_num[missing] / 1e8
    else:
        # 每日任务：从"股票全部信息汇总"的总股本（万股）计算
        total_shares = pd.to_numeric(df_target["total_capital_wan"], errors="coerce") * 10000
        df_target["market_cap"] = total_shares * close_num / 1e8

    # 选股条件（市值和上市天数仅存储，不作为筛选条件）
    cond = (
        (df_target["k"] > 80)
        & (df_target["wr1"] < 20)
        & (df_target["wr2"] < 20)
        & (df_target["rsi9"] > 70)
        & (df_target["volume"] > 0)
    )

    result = df_target[cond].copy()

    # 写入 DB 的价格用不复权数据（close_raw），指标基于前复权计算
    if "close_raw" in result.columns:
        result["close"] = result["close_raw"]

    result = result.drop(
        columns=["list_date", "total_capital_wan", "close_raw"],
        errors="ignore",
    )
    return result


# ======== 写入 ========

def _ensure_table() -> None:
    """Schema is provisioned by migrations; jobs cannot create tables."""
    return None


def _insert_results(df_res: pd.DataFrame) -> int:
    if df_res.empty:
        return 0
    def clean(value):
        if pd.isna(value):
            return None
        return int(value) if isinstance(value, (np.integer, int)) else value
    cols = ["trade_date", "stock_code", "open", "high", "low", "close", "volume", "amount", "wr1", "wr2", "k", "d", "j", "rsi9", "market_cap", "list_days"]
    rows = [{col: clean(row.get(col)) for col in cols} for _, row in df_res.iterrows()]
    kernel, repository, context = _service("write")
    try:
        return repository.insert_results(rows, context)
    finally:
        kernel.close()




# ======== 通知 ========

def _generate_excel(trade_date: str, df_res: pd.DataFrame) -> str | None:
    """生成选股结果 Excel 文件，返回临时文件路径。"""
    if df_res.empty:
        return None

    from openpyxl import Workbook
    from openpyxl.utils.dataframe import dataframe_to_rows

    # 准备输出列
    out = df_res.copy()
    out["amount_wan"] = out["amount"] / 10000
    col_map = {
        "stock_code": "stock_code",
        "stock_name": "股票名称",
        "close": "close",
        "amount_wan": "amount",
        "k": "k",
        "d": "d",
        "j": "j",
        "rsi9": "rsi9",
        "industry": "所属行业",
        "market_cap": "市值/亿",
        "list_days": "上市天数",
    }
    out = out[[c for c in col_map if c in out.columns]].rename(columns=col_map)
    out = out.sort_values("amount", ascending=False)

    wb = Workbook()
    ws = wb.active
    ws.title = "3浪3选股"
    for row in dataframe_to_rows(out, index=False, header=True):
        ws.append(row)

    filename = f"{trade_date}_3浪3统计.xlsx"
    tmp_path = os.path.join(tempfile.gettempdir(), filename)
    wb.save(tmp_path)
    return tmp_path


def _format_summary(trade_date: str, df_res: pd.DataFrame, inserted: int) -> str:
    """生成统计摘要文本（钉钉 + 邮件正文共用）。"""
    if df_res.empty:
        return f"wave3选股 {trade_date}：无符合条件的股票"

    lines = [f"wave3选股 {trade_date}，共 {len(df_res)} 只，写入 {inserted} 条\n"]

    # 行业成交额 Top5
    if "industry" in df_res.columns:
        amt_by_ind = df_res.groupby("industry")["amount"].sum().sort_values(ascending=False)
        lines.append("行业成交额 Top5：")
        for ind, amt in amt_by_ind.head(5).items():
            lines.append(f"  {ind} {amt / 1e8:.2f}亿")

    # 行业市值 Top5
    if "market_cap" in df_res.columns and "industry" in df_res.columns:
        cap_by_ind = df_res.groupby("industry")["market_cap"].sum().sort_values(ascending=False)
        lines.append("\n行业市值 Top5：")
        for ind, cap in cap_by_ind.head(5).items():
            lines.append(f"  {ind} {cap:.0f}亿")

    return "\n".join(lines)


def _send_email(subject: str, body: str, attachment_path: str | None = None) -> None:
    """通过 QQ 邮箱发送文本，可附带 Excel 附件。"""
    cfg = _get_wave3_config().get("email", {})
    sender = cfg.get("sender", "")
    app_key = cfg.get("app_key", "")
    recipients = cfg.get("recipients", [])
    smtp_server = cfg.get("smtp_server", "smtp.qq.com")
    smtp_port = cfg.get("smtp_port", 465)

    if not sender or not app_key or not recipients:
        logger.warning("邮件配置不完整，跳过发送")
        return

    try:
        msg = MIMEMultipart()
        msg["From"] = sender
        msg["To"] = ",".join(recipients)
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "plain", "utf-8"))

        if attachment_path and os.path.isfile(attachment_path):
            with open(attachment_path, "rb") as f:
                part = MIMEBase("application", "octet-stream")
                part.set_payload(f.read())
            encoders.encode_base64(part)
            fname = os.path.basename(attachment_path)
            part.add_header(
                "Content-Disposition", "attachment",
                filename=("utf-8", "", fname),
            )
            msg.attach(part)

        with smtplib.SMTP_SSL(smtp_server, smtp_port) as smtp:
            smtp.login(sender, app_key)
            smtp.sendmail(sender, recipients, msg.as_string())
        logger.info("邮件发送成功")
    except Exception as exc:
        logger.warning(f"邮件发送失败: {exc}")


def _send_notifications(trade_date: str, df_res: pd.DataFrame, inserted: int) -> None:
    """发送钉钉统计摘要 + 邮件（统计摘要 + Excel 附件）。"""
    summary = _format_summary(trade_date, df_res, inserted)
    send_dingtalk(summary)

    excel_path = _generate_excel(trade_date, df_res)
    try:
        subject = f"wave3选股 {trade_date} 共{len(df_res)}只"
        _send_email(subject, summary, attachment_path=excel_path)
    finally:
        if excel_path:
            try:
                os.remove(excel_path)
            except Exception:
                pass


# ======== Pipeline 标准接口 ========

def run(run_date: str, **kwargs) -> str:
    """计算指定日期的 wave3 选股，写入 DB，发送通知。"""
    trade_date = normalize_trade_date(run_date)

    if not is_market_day(trade_date):
        return f"{trade_date} 非交易日，wave3 任务跳过。"

    _ensure_table()

    df = _prepare_indicators(trade_date)
    if df.empty:
        msg = f"{trade_date} wave3: 无日线数据，跳过。"
        send_dingtalk(msg)
        return msg

    target = pd.to_datetime(trade_date).date()
    df_res = _compute_for_date(df, target)
    inserted = _insert_results(df_res)

    _send_notifications(trade_date, df_res, inserted)

    return f"{trade_date} wave3 完成，选出 {len(df_res)} 只，写入 {inserted} 条。"


def run_backfill(start_date: str, end_date: str, limit_stocks: int | None = None) -> str:
    """批量回测历史日期，写入 wave3_stocks 表（不发通知）。"""
    trading_days = date_range(start_date, end_date)
    total_selected, total_inserted = 0, 0

    # 一次性加载股票汇总和历史股本，预分组避免每天重复 groupby
    summary_df = _fetch_stock_summary()
    raw_capital = _fetch_historical_capital()
    capital_df = dict(list(raw_capital.groupby("stock_code"))) if not raw_capital.empty else {}

    from data_collect.utils.progress import DualProgress

    with DualProgress(trading_days) as dp:
        for trade_date in dp.iter_days():
            df = _prepare_indicators(trade_date)
            if df.empty:
                continue

            target = pd.to_datetime(trade_date).date()
            df_res = _compute_for_date(df, target, summary_df=summary_df, capital_df=capital_df)
            inserted = _insert_results(df_res)
            total_selected += len(df_res)
            total_inserted += inserted
            dp.log(f"选出{len(df_res)} 写入{inserted}")

    message = (
        f"wave3 回测完成 ({start_date}~{end_date})，"
        f"共 {len(trading_days)} 个交易日，"
        f"选出 {total_selected}，写入 {total_inserted} 条。"
    )
    send_dingtalk(message)
    return message
