"""
A-Share Day-by-Day Historical Backtest Simulator Engine - v1.0
Implements the strict Phase 3 specifications:
1. Price Dual-Track: Strategy uses QFQ prices for indicators, execution uses RAW market prices.
2. Market Temperature: Scales risk dynamically using Howard Marks' model.
3. A-Share Position Sizing: Follows Van Tharp's 100-share rounding & fee constraints.
4. Livermore Pyramiding & Stop-Loss: Stops averaging down; manages trailing ATR stops.
5. Order/Signal Separation: Signals generated today can only execute tomorrow.
6. Portfolio Ledger: Tracks daily cash, position nodes, fees, and prints performance metrics.
"""

import sys
import os
import math
from typing import List, Dict, Any, Union
import numpy as np
import pandas as pd

# Add paths to ensure imports find quant_risk_models.py
sys.path.append("/workspace/scratch")
sys.path.append("/workspace/artifacts")

try:
    from quant_risk_models import (
        VanTharpAshareSizer,
        TalebMonteCarloTester,
        HowardMarksTempAdjuster,
        LivermorePyramidTracker
    )
except ImportError:
    sys.path.append(os.path.abspath(os.path.dirname(__file__)))
    from quant_risk_models import (
        VanTharpAshareSizer,
        TalebMonteCarloTester,
        HowardMarksTempAdjuster,
        LivermorePyramidTracker
    )

class BaseStrategy:
    """
    Abstract Base Class for Strategies.
    Override 'generate_signal' to return action dicts.
    """
    def __init__(self, name: str, version: str):
        self.name = name
        self.version = version

    def generate_signal(self, trade_date: pd.Timestamp, historical_data: pd.DataFrame) -> Dict[str, Any]:
        """
        Args:
            trade_date: Current decision date.
            historical_data: Cleaned/standardized data up to trade_date (strictly PIT, no future leakage).
        Returns:
            Dict containing signal details or None if no action.
            Format: {
                "ts_code": "000001.SZ",
                "direction": "BUY" or "SELL",
                "stop_loss_price": float
            }
        """
        raise NotImplementedError("Strategies must implement 'generate_signal'!")


class DemoValueTrendStrategy(BaseStrategy):
    """
    Demo Strategy implementing Phase 2 and Phase 3:
    1. Value Filter: PE percentile < 30% (Marks' undervalued margin)
    2. Trend Trigger: Price crosses above MA20 (Livermore momentum)
    """
    def __init__(self):
        super().__init__("DemoValueTrend", "v1.0")

    def generate_signal(self, trade_date: pd.Timestamp, historical_data: pd.DataFrame) -> Dict[str, Any]:
        # Filter for current date
        df_today = historical_data[historical_data['trade_date'] == trade_date]
        if df_today.empty:
            return None

        # Take the target asset to analyze
        row = df_today.iloc[0]
        ts_code = row['ts_code']
        
        # Fetch the subset of historical_data for ts_code up to trade_date
        df_hist = historical_data[(historical_data['ts_code'] == ts_code) & (historical_data['trade_date'] <= trade_date)]
        if len(df_hist) < 20:
            return None  # Not enough history for MA20
            
        # 1. Check Value filter (PE percentile of 5-year history)
        pe_percentile = row.get('pe_percentile', 0.5)  # Default to mid
        is_undervalued = pe_percentile <= 0.30
        
        # 2. Check Trend momentum (Price > MA20)
        close_series = df_hist['qfq_close'].values
        ma20 = np.mean(close_series[-20:])
        price_today = row['qfq_close']
        price_yesterday = close_series[-2] if len(close_series) > 1 else price_today
        
        is_bullish = price_today > ma20 and price_yesterday <= np.mean(close_series[-21:-1])
        
        # 3. Decision
        if is_undervalued and is_bullish:
            # Entry Signal: Place a buy signal with initial stop loss at 2 * ATR below entry
            atr = row.get('atr', price_today * 0.05)  # fallback if ATR not present
            stop_loss = price_today - (2.0 * atr)
            return {
                "ts_code": ts_code,
                "direction": "BUY",
                "stop_loss_price": stop_loss,
                "reason": {"pe_pct": pe_percentile, "ma20": ma20, "price": price_today}
            }
        elif price_today < ma20:
            # Exit Signal: Price falls below MA20
            return {
                "ts_code": ts_code,
                "direction": "SELL",
                "reason": "趋势跌破MA20"
            }
        return None


class AshareBacktestEngine:
    """
    High-Fidelity A-Share Backtesting Simulator Loop
    """
    def __init__(
        self, 
        initial_capital: float = 10000.0,
        commission_rate: float = 0.0003,
        min_commission: float = 5.0,
        stamp_duty_rate: float = 0.0005,
        transfer_fee_rate: float = 0.00001
    ):
        self.initial_capital = initial_capital
        self.cash = initial_capital
        
        # Initialize risk objects
        self.sizer = VanTharpAshareSizer(
            commission_rate=commission_rate, 
            min_commission=min_commission, 
            stamp_duty_rate=stamp_duty_rate,
            transfer_fee_rate=transfer_fee_rate
        )
        
        # State variables
        self.active_positions: Dict[str, LivermorePyramidTracker] = {}
        self.portfolio_value_history: List[Dict[str, Any]] = []
        self.order_book: List[Dict[str, Any]] = []
        self.trades_history: List[Dict[str, Any]] = []  # Completed transactions
        self.r_multiples: List[float] = []              # List of R multiples achieved

    def run(
        self, 
        strategy: BaseStrategy, 
        trade_calendar: List[pd.Timestamp],
        historical_market_data: pd.DataFrame,
        market_temperature_data: pd.DataFrame = None
    ) -> Dict[str, Any]:
        print(f"[ENGINE] Starting backtest simulation over {len(trade_calendar)} periods.")
        print(f"[ENGINE] Initial Equity: ¥{self.initial_capital:.2f}")
        
        # Sort trade calendar
        sorted_calendar = sorted(trade_calendar)
        
        for day_idx, current_day in enumerate(sorted_calendar):
            # Today's market price row
            today_data = historical_market_data[historical_market_data['trade_date'] == current_day]
            
            # Update daily position valuations first (Mark-to-Market)
            self._mark_to_market(current_day, today_data)
            
            # Check stop losses on active positions BEFORE executing new orders
            self._check_stops_and_liquidation(current_day, today_data)
            
            # Calculate and record end-of-day equity snapshot
            current_equity = self.cash + self._get_current_positions_market_value(today_data)
            self.portfolio_value_history.append({
                "trade_date": current_day,
                "cash": self.cash,
                "portfolio_value": current_equity
            })
            
            # 2. Process any pending orders generated on YESTERDAY's signal
            # (Signals generated today can only execute tomorrow)
            self._execute_pending_orders(current_day, today_data)
            
            # 3. Query Strategy for new signals at end of day
            temp_val = 0.5  # Default to neutral
            if market_temperature_data is not None:
                temp_row = market_temperature_data[market_temperature_data['trade_date'] == current_day]
                if not temp_row.empty:
                    temp_val = temp_row.iloc[0].get('pe_percentile_5y', 0.5)
            
            # Generate signals from Strategy (only uses historical slice up to current_day)
            historical_slice = historical_market_data[historical_market_data['trade_date'] <= current_day]
            sig = strategy.generate_signal(current_day, historical_slice)
            if sig:
                self._handle_strategy_signal(current_day, sig, temp_val, today_data)
                
        # 4. Generate final backtest statistics
        performance_report = self._calculate_performance_metrics()
        return performance_report

    def _mark_to_market(self, current_day: pd.Timestamp, today_data: pd.DataFrame):
        if not self.active_positions or today_data.empty:
            return
            
        for ts_code, tracker in list(self.active_positions.items()):
            asset_row = today_data[today_data['ts_code'] == ts_code]
            if asset_row.empty:
                continue
            
            close_raw = asset_row.iloc[0]['raw_close']
            atr = asset_row.iloc[0].get('atr', close_raw * 0.05)
            
            # Update trailing stop in tracker
            tracker.update_trailing_stop(close_raw, atr)

    def _check_stops_and_liquidation(self, current_day: pd.Timestamp, today_data: pd.DataFrame):
        if not self.active_positions or today_data.empty:
            return
            
        for ts_code, tracker in list(self.active_positions.items()):
            asset_row = today_data[today_data['ts_code'] == ts_code]
            if asset_row.empty:
                continue
            
            low_raw = asset_row.iloc[0]['low_raw']
            open_raw = asset_row.iloc[0]['open_raw']
            
            if low_raw <= tracker.stop_loss:
                # STOP LOSS BREACHED!
                exit_price = min(tracker.stop_loss, open_raw)  # Gap down protection
                shares_to_sell = tracker.total_shares
                
                # Execute sale transaction
                raw_value = shares_to_sell * exit_price
                comm = max(raw_value * self.sizer.comm_rate, self.sizer.min_comm)
                stamp = raw_value * self.sizer.stamp_duty
                transfer = raw_value * self.sizer.transfer_fee
                net_cash_returned = raw_value - (comm + stamp + transfer)
                
                self.cash += net_cash_returned
                
                # Calculate finished trade PnL and R multiple
                initial_risk_budget = (tracker.average_cost - tracker.positions[0]['stop']) * tracker.positions[0]['shares']
                actual_pnl = net_cash_returned - (tracker.average_cost * shares_to_sell)
                r_multiple = actual_pnl / initial_risk_budget if initial_risk_budget > 0 else -1.0
                
                self.r_multiples.append(r_multiple)
                self.trades_history.append({
                    "ts_code": ts_code,
                    "direction": "SELL_STOP_LOSS",
                    "exit_date": current_day,
                    "exit_price": exit_price,
                    "shares": shares_to_sell,
                    "pnl": actual_pnl,
                    "r_multiple": r_multiple
                })
                
                print(f"[STOP LOSS] {current_day.strftime('%Y-%m-%d')} - Liquidated {shares_to_sell} shares of {ts_code} at ¥{exit_price:.2f}. PnL: ¥{actual_pnl:.2f} ({r_multiple:.2f}R)")
                del self.active_positions[ts_code]

    def _execute_pending_orders(self, current_day: pd.Timestamp, today_data: pd.DataFrame):
        if today_data.empty:
            return
            
        pending_orders = [o for o in self.order_book if o['status'] == 'PENDING']
        for order in pending_orders:
            ts_code = order['ts_code']
            asset_row = today_data[today_data['ts_code'] == ts_code]
            if asset_row.empty:
                order['status'] = 'EXPIRED'
                order['reject_reason'] = "今日停牌或无行情，订单失效"
                continue
                
            # A-Share tradeability checks
            is_suspended = asset_row.iloc[0].get('is_suspended', False)
            is_limit_up = asset_row.iloc[0].get('is_limit_up', False)
            is_limit_down = asset_row.iloc[0].get('is_limit_down', False)
            
            if is_suspended:
                order['status'] = 'REJECTED'
                order['reject_reason'] = "停牌无法成交"
                continue
                
            open_raw = asset_row.iloc[0]['open_raw']
            
            if order['direction'] == 'BUY':
                if is_limit_up:
                    order['status'] = 'REJECTED'
                    order['reject_reason'] = "一字涨停无法买入"
                    continue
                
                # Simulate buy execution
                shares = order['order_shares']
                raw_cost_value = shares * open_raw
                comm = max(raw_cost_value * self.sizer.comm_rate, self.sizer.min_comm)
                transfer = raw_cost_value * self.sizer.transfer_fee
                total_buy_cost = raw_cost_value + comm + transfer
                
                if total_buy_cost <= self.cash:
                    self.cash -= total_buy_cost
                    
                    if ts_code not in self.active_positions:
                        self.active_positions[ts_code] = LivermorePyramidTracker(ts_code)
                    
                    tracker = self.active_positions[ts_code]
                    try:
                        tracker.add_position_node(open_raw, shares, order['stop_loss_price'])
                        order['status'] = 'FILLED'
                        order['filled_price'] = open_raw
                        print(f"[BUY EXEC] {current_day.strftime('%Y-%m-%d')} - Bought {shares} shares of {ts_code} at ¥{open_raw:.2f}. Total Cost: ¥{total_buy_cost:.2f}")
                    except PermissionError as e:
                        order['status'] = 'REJECTED'
                        order['reject_reason'] = str(e)
                        self.cash += total_buy_cost
                else:
                    order['status'] = 'REJECTED'
                    order['reject_reason'] = "账户现金余额不足以支付成交金额及交易手续费"
            
            elif order['direction'] == 'SELL':
                if is_limit_down:
                    order['status'] = 'REJECTED'
                    order['reject_reason'] = "一字跌停无法卖出"
                    continue
                    
                if ts_code in self.active_positions:
                    tracker = self.active_positions[ts_code]
                    shares_to_sell = tracker.total_shares
                    
                    raw_value = shares_to_sell * open_raw
                    comm = max(raw_value * self.sizer.comm_rate, self.sizer.min_comm)
                    stamp = raw_value * self.sizer.stamp_duty
                    transfer = raw_value * self.sizer.transfer_fee
                    net_cash_returned = raw_value - (comm + stamp + transfer)
                    
                    self.cash += net_cash_returned
                    order['status'] = 'FILLED'
                    order['filled_price'] = open_raw
                    
                    initial_risk_budget = (tracker.average_cost - tracker.positions[0]['stop']) * tracker.positions[0]['shares']
                    actual_pnl = net_cash_returned - (tracker.average_cost * shares_to_sell)
                    r_multiple = actual_pnl / initial_risk_budget if initial_risk_budget > 0 else 0.0
                    
                    self.r_multiples.append(r_multiple)
                    self.trades_history.append({
                        "ts_code": ts_code,
                        "direction": "SELL_STRATEGY",
                        "exit_date": current_day,
                        "exit_price": open_raw,
                        "shares": shares_to_sell,
                        "pnl": actual_pnl,
                        "r_multiple": r_multiple
                    })
                    
                    print(f"[SELL EXEC] {current_day.strftime('%Y-%m-%d')} - Strategically sold {shares_to_sell} shares of {ts_code} at ¥{open_raw:.2f}. PnL: ¥{actual_pnl:.2f} ({r_multiple:.2f}R)")
                    del self.active_positions[ts_code]

    def _handle_strategy_signal(self, current_day: pd.Timestamp, signal: Dict[str, Any], temp_val: float, today_data: pd.DataFrame):
        ts_code = signal['ts_code']
        direction = signal['direction']
        
        if direction == 'BUY':
            asset_row = today_data[today_data['ts_code'] == ts_code]
            if asset_row.empty:
                return
                
            close_raw = asset_row.iloc[0]['raw_close']
            stop_loss = signal['stop_loss_price']
            
            # Adjust risk_pct dynamically using Howard Marks Temp Adjuster
            scaled_risk_pct = HowardMarksTempAdjuster.adjust_risk_percent(0.02, temp_val)
            
            # Run Van Tharp sizer to get recommended shares
            sizer_result = self.sizer.calculate_position(
                self.cash, 
                scaled_risk_pct, 
                close_raw, 
                stop_loss
            )
            
            if sizer_result['eligible']:
                self.order_book.append({
                    "trade_date": current_day,
                    "ts_code": ts_code,
                    "direction": "BUY",
                    "order_shares": sizer_result['shares'],
                    "stop_loss_price": stop_loss,
                    "status": "PENDING"
                })
                print(f"[SIGNAL ORDER] {current_day.strftime('%Y-%m-%d')} - BUY Signal. Placed PENDING order for {sizer_result['shares']} shares. Marks Adjusted Risk: {scaled_risk_pct*100:.2f}%")
            else:
                print(f"[SIGNAL REJECT] {current_day.strftime('%Y-%m-%d')} - BUY Signal rejected by Van Tharp sizer: {sizer_result['reason']}")
                
        elif direction == 'SELL':
            if ts_code in self.active_positions:
                self.order_book.append({
                    "trade_date": current_day,
                    "ts_code": ts_code,
                    "direction": "SELL",
                    "order_shares": self.active_positions[ts_code].total_shares,
                    "stop_loss_price": 0.0,
                    "status": "PENDING"
                })
                print(f"[SIGNAL ORDER] {current_day.strftime('%Y-%m-%d')} - STRATEGY SELL Signal for {ts_code}.")

    def _get_current_positions_market_value(self, today_data: pd.DataFrame) -> float:
        total_val = 0.0
        if not self.active_positions or today_data.empty:
            return total_val
            
        for ts_code, tracker in self.active_positions.items():
            asset_row = today_data[today_data['ts_code'] == ts_code]
            if not asset_row.empty:
                close_raw = asset_row.iloc[0]['raw_close']
                total_val += tracker.total_shares * close_raw
            else:
                total_val += tracker.total_shares * tracker.average_cost
        return total_val

    def _calculate_performance_metrics(self) -> Dict[str, Any]:
        if not self.portfolio_value_history:
            return {"status": "FAILED", "reason": "No simulation history."}
            
        df_portfolio = pd.DataFrame(self.portfolio_value_history)
        initial_val = self.initial_capital
        final_val = df_portfolio.iloc[-1]['portfolio_value']
        total_return_pct = (final_val - initial_val) / initial_val * 100
        
        # Max Drawdown calculation
        values = df_portfolio['portfolio_value'].values
        peaks = np.maximum.accumulate(values)
        drawdowns = (peaks - values) / peaks
        max_dd_pct = np.max(drawdowns) * 100
        
        # Trade analytics
        num_trades = len(self.trades_history)
        wins = [t for t in self.trades_history if t['pnl'] > 0]
        win_rate = len(wins) / num_trades if num_trades > 0 else 0.0
        
        # Taleb Ruin Test integration
        taleb_report = {}
        if self.r_multiples:
            taleb_report = TalebMonteCarloTester.run_ruin_test(
                self.r_multiples,
                initial_capital=self.initial_capital,
                risk_pct=0.02,
                num_simulations=1000
            )
            
        return {
            "status": "SUCCESS",
            "initial_capital": round(initial_val, 2),
            "final_capital": round(final_val, 2),
            "total_return_pct": round(total_return_pct, 2),
            "max_drawdown_pct": round(max_dd_pct, 2),
            "num_trades": num_trades,
            "win_rate": round(win_rate * 100, 2),
            "taleb_ruin_probability_pct": taleb_report.get("ruin_probability", 0.0) * 100 if taleb_report else 0.0,
            "taleb_expected_drawdown_pct": taleb_report.get("expected_max_drawdown", 0.0) * 100 if taleb_report else 0.0,
            "taleb_expectancy_r": taleb_report.get("expectancy_r", 0.0) if taleb_report else 0.0
        }
