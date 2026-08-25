-- 收紧程序账号在 public（公共模式）的旧业务对象权限。
-- Raw（原始层）正式入口仍保留 SELECT/INSERT；超级用户权限不受影响。
-- 这里只撤销 ashares_app，避免旧表成为绕过 Repository（仓储）/Kernel（内核）的后门。
REVOKE ALL ON TABLE
    public.divid_factors,
    public.etf_daily_kline,
    public.etf_divid_factors,
    public.fin_balance,
    public.fin_capital,
    public.fin_cashflow,
    public.fin_holdernum,
    public.fin_income,
    public.fin_indicator,
    public.fin_top10_flow_holder,
    public.fin_top10_holder,
    public.index_weight,
    public.index_weight_changelog,
    public.instrument_changelog,
    public.sector_changelog,
    public.sector_stock,
    public.tushare_daily_basic_raw_corrupt_20260811,
    public.tushare_daily_raw_corrupt_20260811
FROM ashares_app;
