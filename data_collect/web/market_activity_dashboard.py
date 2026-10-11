"""Local read-only dashboard for frozen Market Activity（成交活跃度）data."""
from __future__ import annotations

import argparse
from datetime import date
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
from typing import Any, Sequence
from urllib.parse import parse_qs, urlparse
from uuid import uuid4

from data_collect.data_access import DataAccessKernel, RunContext
from data_collect.data_access.policy_catalog import build_default_registry
from data_collect.data_access.repository.market_activity_dashboard_repository import (
    MarketActivityDashboardRepository,
)


_STATIC_FILE = Path(__file__).with_name("static") / "market_activity_dashboard.html"


def _service(label: str):
    kernel = DataAccessKernel(policy_registry=build_default_registry())
    context = RunContext(
        run_id=f"market-activity-dashboard-{label}-{uuid4().hex[:10]}",
        job_name="market_activity_dashboard",
        code_version="unknown",
        config_hash="unknown",
        pipeline_version="data-access-v1",
    )
    return kernel, MarketActivityDashboardRepository(kernel), context


def _day_payload(trade_date: date, view: str) -> dict[str, Any]:
    kernel, repository, context = _service("day")
    try:
        return repository.frozen_day(trade_date, context, view=view)
    finally:
        kernel.close()


def _stock_detail_payload(activity_snapshot_id: str, trade_date: date, ts_code: str) -> dict[str, Any]:
    kernel, repository, context = _service("stock-detail")
    try:
        return repository.stock_detail(activity_snapshot_id, trade_date, ts_code, context)
    finally:
        kernel.close()


def _dates_payload() -> dict[str, Any]:
    today = date.today()
    kernel, repository, context = _service("dates")
    try:
        dates = repository.available_dates(today, context)
        return {"dates": [value.isoformat() for value in dates]}
    finally:
        kernel.close()


class _DashboardHandler(BaseHTTPRequestHandler):
    server_version = "MarketActivityDashboard/1.0"

    def _send(self, status: int, body: bytes, content_type: str) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
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
            elif path == "/healthz":
                self._json(200, {"status": "ok", "read_only": True})
            elif path == "/api/market-activity/dates":
                self._json(200, _dates_payload())
            elif path == "/api/market-activity/day":
                query = parse_qs(parsed.query)
                raw_date = (query.get("date") or [""])[0]
                view = (query.get("view") or ["amount"])[0]
                if not raw_date:
                    raise ValueError("必须指定 date=YYYY-MM-DD")
                self._json(200, _day_payload(date.fromisoformat(raw_date), view))
            elif path == "/api/market-activity/stock-detail":
                query = parse_qs(parsed.query)
                snapshot_id = (query.get("activity_snapshot_id") or [""])[0]
                raw_date = (query.get("trade_date") or [""])[0]
                ts_code = (query.get("ts_code") or [""])[0]
                if not snapshot_id or not raw_date or not ts_code:
                    raise ValueError("activity_snapshot_id、trade_date、ts_code 均为必填")
                self._json(200, _stock_detail_payload(
                    snapshot_id, date.fromisoformat(raw_date), ts_code,
                ))
            else:
                self._json(404, {"error": "not found"})
        except ValueError as exc:
            self._json(400, {"error": str(exc)})
        except LookupError as exc:
            self._json(404, {"error": str(exc)})
        except Exception:  # do not expose SQL/connection details to the browser
            self._json(500, {"error": "internal server error"})

    def log_message(self, format: str, *args: Any) -> None:
        print(f"[market-activity-dashboard] {self.address_string()} - {format % args}")


def serve(host: str = "127.0.0.1", port: int = 8766) -> None:
    server = ThreadingHTTPServer((host, port), _DashboardHandler)
    print(f"Market Activity dashboard（成交活跃度看板）: http://{host}:{port}/")
    print("API is read-only; press Ctrl+C to stop.")
    try:
        server.serve_forever()
    finally:
        server.server_close()


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Serve a read-only Market Activity dashboard")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8766)
    args = parser.parse_args(argv)
    serve(args.host, args.port)


if __name__ == "__main__":
    main()
