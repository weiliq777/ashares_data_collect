"""Local read-only dashboard for a persisted Backtest Run（回测运行）."""
from __future__ import annotations

import argparse
from datetime import date, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
from threading import Lock
from typing import Any, Mapping, Sequence
from urllib.parse import parse_qs, unquote, urlparse

from data_collect.data_access import DataAccessKernel, RunContext
from data_collect.data_access.policy_catalog import build_default_registry
from data_collect.data_access.repository.backtest_run_repository import BacktestRunRepository
from data_collect.evaluation.markdown import (
    render_capital_sensitivity_bundle_markdown,
    render_capital_sensitivity_markdown,
    render_diagnostic_markdown,
)
from data_collect.evaluation.standard import DEFAULT_CAPITALS, build_capital_sensitivity
from data_collect.jobs.backtest_diagnostics import diagnose_run


_STATIC_FILE = Path(__file__).with_name("static") / "backtest_dashboard.html"
_REPORT_CACHE: dict[str, dict[str, Any]] = {}
_COMPARISON_CACHE: dict[tuple[str, ...], dict[str, Any]] = {}
_CACHE_LOCK = Lock()
_TERMINAL_RUN_STATUSES = frozenset({"SUCCEEDED", "FAILED", "CANCELLED"})


def _service(label: str):
    kernel = DataAccessKernel(policy_registry=build_default_registry())
    context = RunContext(
        run_id=f"backtest-dashboard-{label}",
        job_name="backtest_dashboard",
        code_version="unknown",
        config_hash="unknown",
        pipeline_version="data-access-v1",
    )
    return kernel, BacktestRunRepository(kernel), context


def _cached_report(run_id: str) -> dict[str, Any]:
    with _CACHE_LOCK:
        cached = _REPORT_CACHE.get(run_id)
    if cached is not None:
        return cached
    report = diagnose_run(run_id)
    status = str((report.get("run") or {}).get("status") or "").upper()
    if status in _TERMINAL_RUN_STATUSES:
        with _CACHE_LOCK:
            _REPORT_CACHE[run_id] = report
    return report


def _cached_capital_comparison(run_ids: Sequence[str]) -> dict[str, Any]:
    normalized = tuple(dict.fromkeys(item.strip() for item in run_ids if item.strip()))
    if not 1 <= len(normalized) <= 10:
        raise ValueError("资金敏感性比较必须提供 1 至 10 个 Run ID")
    with _CACHE_LOCK:
        cached = _COMPARISON_CACHE.get(normalized)
    if cached is not None:
        return cached
    reports = [_cached_report(run_id) for run_id in normalized]
    result = build_capital_sensitivity(reports)
    if all(
        str((report.get("run") or {}).get("status") or "").upper()
        in _TERMINAL_RUN_STATUSES
        for report in reports
    ):
        with _CACHE_LOCK:
            _COMPARISON_CACHE[normalized] = result
    return result


def _capital_sensitivity_catalog(base_run_id: str) -> dict[str, Any]:
    """Return preset capital Runs for the same Dataset and Strategy."""
    kernel, repository, context = _service("capital-catalog")
    try:
        base = repository.diagnostic_run(base_run_id, context)
        if not base:
            raise LookupError(f"未找到回测运行: {base_run_id}")
        runs = repository.diagnostic_capital_runs(
            str(base["dataset_id"]),
            str(base["strategy_id"]),
            list(DEFAULT_CAPITALS),
            context,
        )
        return {
            "base_run_id": base_run_id,
            "dataset_id": base["dataset_id"],
            "strategy_id": base["strategy_id"],
            "strategy_version": base.get("strategy_version"),
            "scenarios": [
                {
                    "run_id": row["run_id"],
                    "run_version": row["run_version"],
                    "initial_capital": row["initial_cash"],
                    "status": row["status"],
                    "finished_at": row.get("finished_at"),
                }
                for row in runs
            ],
        }
    finally:
        kernel.close()


def _capital_sensitivity_bundle(base_run_id: str) -> str:
    catalog = _capital_sensitivity_catalog(base_run_id)
    run_ids = [
        str(row["run_id"])
        for row in catalog.get("scenarios") or []
        if str(row.get("status") or "").upper() == "SUCCEEDED"
    ]
    if not run_ids:
        raise LookupError("当前策略没有已完成的资金场景 Run")
    comparison = _cached_capital_comparison(run_ids)
    reports = [_cached_report(run_id) for run_id in run_ids]
    return render_capital_sensitivity_bundle_markdown(reports, comparison)


def _trade_chart(run_id: str, lot_id: str) -> dict[str, Any]:
    report = _cached_report(run_id)
    trade = next((row for row in report.get("trades", []) if str(row.get("lot_id")) == lot_id), None)
    if trade is None:
        raise LookupError(f"未找到交易批次（trade lot）: {lot_id}")
    buy_date = date.fromisoformat(str(trade["buy_date"])[:10])
    sell_date = date.fromisoformat(str(trade["sell_date"])[:10]) if trade.get("sell_date") else buy_date + timedelta(days=120)
    kernel, repository, context = _service("chart")
    try:
        rows = repository.diagnostic_trade_chart(
            str(report["run"]["dataset_id"]),
            str(trade["ts_code"]),
            buy_date - timedelta(days=90),
            sell_date + timedelta(days=5),
            context,
        )
    finally:
        kernel.close()
    return {
        "lot_id": lot_id,
        "ts_code": trade.get("ts_code"),
        "name": trade.get("name"),
        "buy_point": {
            "date": trade.get("buy_date"),
            "price": trade.get("buy_price"),
        },
        "sell_point": {
            "date": trade.get("sell_date"),
            "price": trade.get("sell_price"),
            "reason": trade.get("exit_reason"),
        },
        "series": [
            {
                "date": row.get("research_date").isoformat() if hasattr(row.get("research_date"), "isoformat") else str(row.get("research_date")),
                "raw_close": row.get("raw_close"),
                "adj_close_base": row.get("adj_close_base"),
                "ma20": row.get("ma20"),
                "ma60": row.get("ma60"),
                "return_60d": row.get("return_60d"),
                "is_st_asof": row.get("is_st_asof"),
                "quality_status": row.get("quality_status"),
            }
            for row in rows
        ],
    }


class _DashboardHandler(BaseHTTPRequestHandler):
    server_version = "BacktestDashboard/1.0"

    def _send(
        self,
        status: int,
        body: bytes,
        content_type: str,
        headers: Mapping[str, str] | None = None,
    ) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        for key, value in (headers or {}).items():
            self.send_header(key, value)
        self.end_headers()
        self.wfile.write(body)

    def _json(self, status: int, payload: Any) -> None:
        self._send(
            status,
            json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8"),
            "application/json; charset=utf-8",
        )

    def do_GET(self) -> None:  # noqa: N802 - stdlib HTTP handler API
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/")
        try:
            if path in {"", "/index.html"}:
                self._send(200, _STATIC_FILE.read_bytes(), "text/html; charset=utf-8")
                return
            if path == "/healthz":
                self._json(200, {"status": "ok", "read_only": True})
                return
            if path == "/api/backtests/compare":
                values = parse_qs(parsed.query).get("run_ids", [])
                run_ids = [item for value in values for item in value.split(",")]
                self._json(200, _cached_capital_comparison(run_ids))
                return
            if path == "/api/backtests/compare.md":
                values = parse_qs(parsed.query).get("run_ids", [])
                run_ids = [item for value in values for item in value.split(",")]
                markdown = render_capital_sensitivity_markdown(_cached_capital_comparison(run_ids))
                self._send(
                    200,
                    markdown.encode("utf-8"),
                    "text/markdown; charset=utf-8",
                    {"Content-Disposition": 'attachment; filename="capital-sensitivity.md"'},
                )
                return
            if path == "/api/backtests/capital-sensitivity/diagnostics.md":
                values = parse_qs(parsed.query).get("base_run_id", [])
                if not values or not values[0].strip():
                    raise ValueError("缺少 base_run_id")
                markdown = _capital_sensitivity_bundle(values[0].strip())
                self._send(
                    200,
                    markdown.encode("utf-8"),
                    "text/markdown; charset=utf-8",
                    {"Content-Disposition": 'attachment; filename="all-capital-diagnostics.md"'},
                )
                return
            if path == "/api/backtests/capital-sensitivity":
                values = parse_qs(parsed.query).get("base_run_id", [])
                if not values or not values[0].strip():
                    raise ValueError("缺少 base_run_id")
                self._json(200, _capital_sensitivity_catalog(values[0].strip()))
                return
            parts = [unquote(part) for part in path.split("/") if part]
            if len(parts) == 4 and parts[:2] == ["api", "backtests"] and parts[3] == "diagnostics.md":
                markdown = render_diagnostic_markdown(_cached_report(parts[2]))
                safe_run_id = "".join(char for char in parts[2] if char.isalnum() or char in "-_")[:80] or "run"
                self._send(
                    200,
                    markdown.encode("utf-8"),
                    "text/markdown; charset=utf-8",
                    {"Content-Disposition": f'attachment; filename="backtest-{safe_run_id}.md"'},
                )
                return
            if len(parts) == 4 and parts[:2] == ["api", "backtests"] and parts[3] == "diagnostics":
                self._json(200, _cached_report(parts[2]))
                return
            if len(parts) == 6 and parts[:2] == ["api", "backtests"] and parts[3] == "trades" and parts[5] == "chart":
                self._json(200, _trade_chart(parts[2], parts[4]))
                return
            self._json(404, {"error": "not found"})
        except LookupError as exc:
            self._json(404, {"error": str(exc)})
        except Exception as exc:  # keep the local API JSON-shaped for the UI
            self._json(500, {"error": str(exc)})

    def log_message(self, format: str, *args: Any) -> None:
        print(f"[backtest-dashboard] {self.address_string()} - {format % args}")


def serve(host: str = "127.0.0.1", port: int = 8765) -> None:
    server = ThreadingHTTPServer((host, port), _DashboardHandler)
    print(f"Backtest dashboard（回测看板）: http://{host}:{port}/")
    print("API is read-only; press Ctrl+C to stop.")
    try:
        server.serve_forever()
    finally:
        server.server_close()


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Serve a read-only Backtest Run dashboard")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args(argv)
    serve(args.host, args.port)


if __name__ == "__main__":
    main()
