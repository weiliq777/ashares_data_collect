"""
Quant Risk Models Module - v1.0
Inspired by Van Tharp, Nassim Taleb, Howard Marks, and Jesse Livermore.
Specifically adapted for A-share constraints and small-capital retail accounts (e.g., ¥10,000).

This module contains:
1. VanTharpAshareSizer: Fixed fractional position sizing with A-share lot constraints & minimum commission rules.
2. TalebMonteCarloTester: Bootstrap resampling of R-multiples to calculate drawdown and ruin probabilities.
3. HowardMarksTempAdjuster: Dynamic risk scaling based on macro market valuation percentiles.
4. LivermorePyramidTracker: A strict position tracker that forbids averaging down on losses and manages trailing stops.
"""

import numpy as np
import math
from typing import List, Dict, Any, Union

class VanTharpAshareSizer:
    def __init__(
        self, 
        commission_rate: float = 0.0003,  # 佣金万三
        min_commission: float = 5.0,      # 单笔最低5元
        stamp_duty_rate: float = 0.0005,  # 印花税千分之0.5（仅卖出收取，A股2023最新）
        transfer_fee_rate: float = 0.00001 # 过户费十万分之一
    ):
        self.comm_rate = commission_rate
        self.min_comm = min_commission
        self.stamp_duty = stamp_duty_rate
        self.transfer_fee = transfer_fee_rate

    def calculate_position(
        self, 
        total_capital: float, 
        risk_pct: float, 
        entry_price: float, 
        stop_loss_price: float
    ) -> Dict[str, Any]:
        """
        计算符合A股100股整数倍、印花税及最低5元佣金硬约束下的安全头寸规模。
        
        Args:
            total_capital: 账户总本金 (如 10000)
            risk_pct: 预设单笔最大风险比例 (如 0.02 代表 2%)
            entry_price: 计划买入价
            stop_loss_price: 预设硬止损价
            
        Returns:
            Dict 包含具体执行参数
        """
        if entry_price <= stop_loss_price:
            raise ValueError("买入价必须高于止损价！该仓位计算器目前仅支持多头策略。")
        if total_capital <= 0 or risk_pct <= 0:
            raise ValueError("账户本金和风险比例必须大于0！")

        # 1. 绝对风险金额 (Max Loss)
        max_risk_amount = total_capital * risk_pct
        
        # 2. 单股风险 (1R)
        risk_per_share = entry_price - stop_loss_price
        
        # 3. 理论可买股数
        theoretical_shares = max_risk_amount / risk_per_share
        
        # 4. A股向下取整到一手 (100股) 的倍数
        actual_shares = math.floor(theoretical_shares / 100) * 100
        
        # 如果算出来连一手都买不起，直接判定资金不足
        if actual_shares < 100:
            return {
                "eligible": False,
                "shares": 0,
                "required_capital": 0.0,
                "actual_risk_pct": 0.0,
                "reason": f"资金不足：理论可买 {theoretical_shares:.1f} 股，不足A股最低交易单位(100股)。"
            }

        # 5. 循环校验资金硬约束 (考虑交易摩擦费用)
        while actual_shares >= 100:
            raw_cost = actual_shares * entry_price
            
            # 购买费用：佣金 + 过户费
            buy_comm = max(raw_cost * self.comm_rate, self.min_comm)
            buy_transfer = raw_cost * self.transfer_fee
            total_buy_cost = raw_cost + buy_comm + buy_transfer
            
            if total_buy_cost <= total_capital:
                # 资金充足，可以买入该股数
                actual_risk_amount = actual_shares * risk_per_share
                actual_risk_pct = actual_risk_amount / total_capital
                return {
                    "eligible": True,
                    "shares": actual_shares,
                    "required_capital": round(total_buy_cost, 2),
                    "actual_risk_amount": round(actual_risk_amount, 2),
                    "actual_risk_pct": round(actual_risk_pct, 4),
                    "buy_fees": round(buy_comm + buy_transfer, 2),
                    "reason": "风控校验通过"
                }
            else:
                # 资金超限，向下缩减1手(100股)后重试
                actual_shares -= 100

        return {
            "eligible": False,
            "shares": 0,
            "required_capital": 0.0,
            "actual_risk_pct": 0.0,
            "reason": "包含手续费后的所需资金超过账户总本金上限。"
        }


class TalebMonteCarloTester:
    @staticmethod
    def run_ruin_test(
        r_multiples: List[float],
        initial_capital: float = 10000.0,
        risk_pct: float = 0.02,
        num_simulations: int = 1000,
        sequence_length: int = 50,
        ruin_threshold_pct: float = 0.50
    ) -> Dict[str, Any]:
        """
        利用蒙特卡洛Bootstrap重抽样算法，测试策略在打乱交易顺序下的破产率和最大回撤。
        防御“幸存者偏差”与“顺序依赖陷阱”。
        
        Args:
            r_multiples: 历史回测产出的R乘数序列 (如 [2.5, -1.0, 3.2, -1.0, -1.0])
            initial_capital: 初始试验本金
            risk_pct: 每次交易暴露的风险比例
            num_simulations: 模拟运行的路径数量
            sequence_length: 每次模拟的连续交易笔数 (如50笔)
            ruin_threshold_pct: 破产/毁灭阈值 (如 0.50 代表本金亏损50%即判定为系统毁灭)
        """
        if not r_multiples:
            raise ValueError("历史R乘数序列不能为空！")
        
        r_arr = np.array(r_multiples)
        ruin_count = 0
        all_max_drawdowns = []
        final_capitals = []

        # 毁灭水位线
        ruin_barrier = initial_capital * (1.0 - ruin_threshold_pct)

        for _ in range(num_simulations):
            # Bootstrap 随机有放回抽样生成交易序列
            sim_sequence = np.random.choice(r_arr, size=sequence_length, replace=True)
            
            capital = initial_capital
            peak_capital = initial_capital
            max_dd = 0.0
            is_ruined = False

            for r in sim_sequence:
                # 动态计算仓位绝对风险额：基于当前可用本金计算
                risk_amount = capital * risk_pct
                trade_pnl = risk_amount * r
                capital += trade_pnl
                
                # 破产硬界限检查
                if capital <= ruin_barrier:
                    is_ruined = True
                    # 即使破产也记录该状态，直接断开后续模拟
                    capital = max(0.0, capital)
                    break
                
                # 追踪最大回撤
                if capital > peak_capital:
                    peak_capital = capital
                drawdown = (peak_capital - capital) / peak_capital
                if drawdown > max_dd:
                    max_dd = drawdown

            if is_ruined:
                ruin_count += 1
                max_dd = max(max_dd, ruin_threshold_pct)  # 至少达到了破产线回撤
                
            all_max_drawdowns.append(max_dd)
            final_capitals.append(capital)

        ruin_probability = ruin_count / num_simulations
        avg_max_dd = np.mean(all_max_drawdowns)
        percentile_95_dd = np.percentile(all_max_drawdowns, 95)
        median_final_capital = np.median(final_capitals)

        return {
            "ruin_probability": round(ruin_probability, 4),
            "expected_max_drawdown": round(avg_max_dd, 4),
            "percentile_95_drawdown": round(percentile_95_dd, 4),
            "median_final_capital": round(median_final_capital, 2),
            "win_rate_in_r": round(np.sum(r_arr > 0) / len(r_arr), 4),
            "avg_win_r": round(np.mean(r_arr[r_arr > 0]) if any(r_arr > 0) else 0.0, 2),
            "avg_loss_r": round(np.mean(r_arr[r_arr < 0]) if any(r_arr < 0) else 0.0, 2),
            "expectancy_r": round(np.mean(r_arr), 4) # 平均R期望值
        }


class HowardMarksTempAdjuster:
    """
    霍华德·马克斯的“市场温标调节器”。
    根据宽基指数(如沪深300/中证全指)的估值历史分位数，动态伸缩单笔风险暴露(Risk Scaling)。
    """
    @staticmethod
    def adjust_risk_percent(
        base_risk_pct: float, 
        market_valuation_percentile: float
    ) -> float:
        """
        根据情绪温标，动态返回调整后的单笔风险暴露比例。
        
        Args:
            base_risk_pct: 基准单笔最大风险比例 (如 0.02)
            market_valuation_percentile: 市场目前所处的历史估值百分位 (0.00 ~ 1.00)
        """
        if not (0.0 <= market_valuation_percentile <= 1.0):
            raise ValueError("估值百分位必须在0.0到1.0之间！")

        # 极热防守，极冷进攻。动态风险系数
        if market_valuation_percentile >= 0.85:
            # 极热状态：风险系数极度压缩，转入战术防守
            multiplier = 0.25
        elif market_valuation_percentile >= 0.70:
            # 偏热状态：减少风险敞口
            multiplier = 0.50
        elif market_valuation_percentile >= 0.35:
            # 中性状态：标准风险敞口
            multiplier = 1.00
        elif market_valuation_percentile >= 0.15:
            # 偏冷状态：具有安全边际，可以略微进取
            multiplier = 1.25
        else:
            # 极冷状态(熊市谷底)：安全边际极大，释放更积极的进攻型仓位
            multiplier = 1.50

        adjusted_risk = base_risk_pct * multiplier
        return round(adjusted_risk, 4)


class LivermorePyramidTracker:
    """
    杰西·利弗莫尔的“金字塔持仓与追踪止损管理器”。
    物理锁定两条反人性纪律：
    1. 严禁向下摊平亏损：若当前持仓处于亏损状态，禁止任何加仓动作。
    2. 金字塔式上涨加仓：只有在持仓已经实现盈利的前提下，才允许继续加仓。
    """
    def __init__(self, ts_code: str):
        self.ts_code = ts_code
        self.positions: List[Dict[str, float]] = [] # 记录每次建仓详情
        self.total_shares: int = 0
        self.average_cost: float = 0.0
        self.stop_loss: float = 0.0

    def add_position_node(
        self, 
        current_price: float, 
        shares: int, 
        initial_stop_loss: float
    ) -> Dict[str, Any]:
        """
        尝试加入一个加仓节点。
        """
        # 1. 物理检查：严禁向下均摊亏损 (Averaging Down is Prohibited)
        if self.total_shares > 0 and current_price < self.average_cost:
            raise PermissionError(
                f"【物理风控拦截】当前持仓均价为 ¥{self.average_cost:.2f}，当前价格为 ¥{current_price:.2f}。"
                f"持仓处于账面亏损状态，物理禁止任何加仓动作！利弗莫尔铁律：决不向下均摊亏损。"
            )

        # 2. 允许建仓/加仓，计算新账本
        new_node = {
            "price": current_price,
            "shares": shares,
            "stop": initial_stop_loss
        }
        self.positions.append(new_node)
        
        # 重新计算整体仓位
        total_value = sum(pos["price"] * pos["shares"] for pos in self.positions)
        self.total_shares = sum(pos["shares"] for pos in self.positions)
        self.average_cost = total_value / self.total_shares
        
        # 止损价采取“最新加仓节点预设止损”与“历史止损”的较优者，保护本金
        self.stop_loss = max(self.stop_loss, initial_stop_loss)

        return {
            "status": "SUCCESS",
            "total_shares": self.total_shares,
            "average_cost": round(self.average_cost, 2),
            "current_stop_loss": round(self.stop_loss, 2)
        }

    def update_trailing_stop(self, current_price: float, atr: float) -> float:
        """
        动态追踪止损：当价格创出新高时，无情上移止损线，让利润奔跑。
        最新跟踪止损 = max(当前止损价, 今日价格 - 3 * ATR)
        """
        if self.total_shares == 0:
            return 0.0
            
        potential_new_stop = current_price - 3.0 * atr
        if potential_new_stop > self.stop_loss:
            self.stop_loss = potential_new_stop
            
        return round(self.stop_loss, 2)
