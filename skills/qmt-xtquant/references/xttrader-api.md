# xttrader API intentionally excluded

This project is limited to QMT `xtdata` market-data collection and local market-data cache maintenance. It does not connect a trading account or place, cancel, or query live orders.

Trading API examples were removed from the active project skill so agents do not treat them as an approved integration path. The repository's executable Python is checked by `tools/check_paper_trading_boundary.py`; any proposed trading integration requires a separately authorized security and architecture review.
