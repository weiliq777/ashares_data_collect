"""SET-free, bounded quality inspection for Foundation v2 outputs."""
from __future__ import annotations

import re
from datetime import date

from data_collect.data_access.context import RunContext, WorkScope
from data_collect.data_access.repository.base import BaseRepository
from data_collect.features.financial_v2 import DIVIDEND_PERCENTILE_MIN_OBSERVATIONS


_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*$")


class FoundationQualityRepository(BaseRepository):
    @staticmethod
    def _table(value: str) -> str:
        if not _IDENTIFIER.fullmatch(value):
            raise ValueError(f"invalid qualified table: {value}")
        return value

    def inspect_foundation_v2(
        self,
        start_date: date,
        end_date: date,
        context: RunContext,
        codes: list[str] | None = None,
        company_table: str = "feature.company_financial_v2",
        valuation_table: str = "feature.valuation_daily_v1",
    ) -> dict:
        company = self._table(company_table)
        valuation = self._table(valuation_table)
        code_company = " AND ts_code=ANY(%s)" if codes else ""
        code_standard = " AND stock_code=ANY(%s)" if codes else ""
        code_valuation = " AND ts_code=ANY(%s)" if codes else ""
        suffix = [codes] if codes else []
        scope = WorkScope(
            date_from=start_date, date_to=end_date,
            codes=tuple(codes) if codes else None,
            partition="foundation-quality-v2",
        )
        checks = {
            "income_mapping_mismatch": (
                "std.financial_income_standard",
                """SELECT COUNT(*) AS value FROM std.financial_income_standard
                   WHERE report_date BETWEEN %s AND %s
                     AND (net_profit IS DISTINCT FROM net_profit_consolidated
                          OR net_profit_parent IS DISTINCT FROM n_income_attr_p)""" + code_standard,
                [start_date, end_date, *suffix],
            ),
            "cashflow_mapping_mismatch": (
                "std.financial_cash_flow_standard",
                """SELECT COUNT(*) AS value FROM std.financial_cash_flow_standard
                   WHERE report_date BETWEEN %s AND %s
                     AND operating_cashflow_ytd IS DISTINCT FROM n_cashflow_act""" + code_standard,
                [start_date, end_date, *suffix],
            ),
            "indicator_future_comp_type_link": (
                "std.fina_indicator_standard",
                """SELECT COUNT(*) AS value FROM std.fina_indicator_standard f
                   WHERE f.report_date BETWEEN %s AND %s AND f.linked_comp_type IS NOT NULL
                     AND NOT EXISTS (
                       SELECT 1 FROM std.financial_income_standard i
                        WHERE i.stock_code=f.ts_code AND i.report_date=f.report_date
                          AND i.comp_type=f.linked_comp_type AND COALESCE(i.report_type,'1')='1'
                          AND i.data_available_timestamp <= f.data_available_timestamp)""" + (" AND f.ts_code=ANY(%s)" if codes else ""),
                [start_date, end_date, *suffix],
            ),
            "company_rows": (
                company,
                f"SELECT COUNT(*) AS value FROM {company} WHERE report_date BETWEEN %s AND %s" + code_company,
                [start_date, end_date, *suffix],
            ),
            "company_missing_report_periods": (
                company,
                f"""SELECT COUNT(*) AS value FROM (
                    SELECT DISTINCT i.stock_code,i.report_date FROM std.financial_income_standard i
                     WHERE i.report_date BETWEEN %s AND %s AND COALESCE(i.report_type,'1')='1'
                       AND ((i.end_type='1' AND EXTRACT(MONTH FROM i.report_date)=3 AND EXTRACT(DAY FROM i.report_date)=31)
                         OR (i.end_type='2' AND EXTRACT(MONTH FROM i.report_date)=6 AND EXTRACT(DAY FROM i.report_date)=30)
                         OR (i.end_type='3' AND EXTRACT(MONTH FROM i.report_date)=9 AND EXTRACT(DAY FROM i.report_date)=30)
                         OR (i.end_type='4' AND EXTRACT(MONTH FROM i.report_date)=12 AND EXTRACT(DAY FROM i.report_date)=31))
                       AND i.data_available_timestamp IS NOT NULL
                       AND i.data_available_timestamp < (%s::date + 1)::timestamp AT TIME ZONE 'Asia/Shanghai'
                       {code_standard}
                       AND NOT EXISTS (
                         SELECT 1 FROM {company} c WHERE c.ts_code=i.stock_code AND c.report_date=i.report_date
                           AND c.data_available_timestamp < (%s::date + 1)::timestamp AT TIME ZONE 'Asia/Shanghai')
                ) missing""",
                [start_date, end_date, end_date, *suffix, end_date],
            ),
            "company_contract_mismatch": (
                company,
                f"SELECT COUNT(*) AS value FROM {company} WHERE report_date BETWEEN %s AND %s AND feature_contract_version<>'feature-contract-v2.0'" + code_company,
                [start_date, end_date, *suffix],
            ),
            "invalid_complete_ttm": (
                company,
                f"""SELECT COUNT(*) AS value FROM {company} WHERE report_date BETWEEN %s AND %s
                   AND ttm_complete AND (ttm_component_count<>ttm_required_component_count
                     OR net_profit_consolidated_ttm IS NULL OR net_profit_parent_ttm IS NULL
                     OR operating_cashflow_ttm IS NULL)""" + code_company,
                [start_date, end_date, *suffix],
            ),
            # The Feature table is a mutable current view and can contain
            # announcements observed after the release cutoff.  Keep this
            # count for audit evidence; Snapshot/PIT selection is responsible
            # for excluding these rows from a historical cutoff.
            "company_future_rows": (
                company,
                f"""SELECT COUNT(*) AS value FROM {company} WHERE report_date BETWEEN %s AND %s
                   AND data_available_timestamp >= (%s::date + 1)::timestamp AT TIME ZONE 'Asia/Shanghai'""" + code_company,
                [start_date, end_date, end_date, *suffix],
            ),
            "invalid_ocf_ratio": (
                company,
                f"""SELECT COUNT(*) AS value FROM {company} WHERE report_date BETWEEN %s AND %s AND comp_type='1'
                   AND ((net_profit_consolidated_ttm<=0 AND ocf_to_net_profit_consolidated_ttm IS NOT NULL)
                     OR (net_profit_consolidated_ttm>0 AND operating_cashflow_ttm IS NOT NULL
                         AND ocf_to_net_profit_consolidated_ttm IS DISTINCT FROM operating_cashflow_ttm/net_profit_consolidated_ttm))""" + code_company,
                [start_date, end_date, *suffix],
            ),
            "invalid_comp_type_cashflow": (
                company,
                f"""SELECT COUNT(*) AS value FROM {company} WHERE report_date BETWEEN %s AND %s
                   AND ((comp_type IS NULL AND (cashflow_quality_status<>'UNKNOWN_COMP_TYPE' OR ocf_to_net_profit_consolidated_ttm IS NOT NULL))
                     OR (comp_type IS NOT NULL AND comp_type<>'1' AND (cashflow_quality_status<>'NOT_APPLICABLE' OR ocf_to_net_profit_consolidated_ttm IS NOT NULL)))""" + code_company,
                [start_date, end_date, *suffix],
            ),
            "missing_pit_evidence": (
                company,
                f"SELECT COUNT(*) AS value FROM {company} WHERE report_date BETWEEN %s AND %s AND COALESCE(pit_evidence_level,'UNKNOWN')='UNKNOWN'" + code_company,
                [start_date, end_date, *suffix],
            ),
            "valuation_rows": (
                valuation,
                f"SELECT COUNT(*) AS value FROM {valuation} WHERE trade_date BETWEEN %s AND %s" + code_valuation,
                [start_date, end_date, *suffix],
            ),
            "valuation_missing_rows": (
                valuation,
                f"""SELECT COUNT(*) AS value FROM std.valuation_daily s WHERE s.trade_date BETWEEN %s AND %s
                   AND s.source='tushare' {code_standard}
                   AND NOT EXISTS (SELECT 1 FROM {valuation} v WHERE v.ts_code=s.stock_code AND v.trade_date=s.trade_date)""",
                [start_date, end_date, *suffix],
            ),
            "valuation_extra_rows": (
                valuation,
                f"""SELECT COUNT(*) AS value FROM {valuation} v WHERE v.trade_date BETWEEN %s AND %s {code_valuation}
                   AND NOT EXISTS (SELECT 1 FROM std.valuation_daily s WHERE s.stock_code=v.ts_code AND s.trade_date=v.trade_date AND s.source='tushare')""",
                [start_date, end_date, *suffix],
            ),
            "valuation_contract_mismatch": (
                valuation,
                f"SELECT COUNT(*) AS value FROM {valuation} WHERE trade_date BETWEEN %s AND %s AND feature_contract_version<>'feature-contract-v2.0'" + code_valuation,
                [start_date, end_date, *suffix],
            ),
            "invalid_dividend_percentile": (
                valuation,
                f"""SELECT COUNT(*) AS value FROM {valuation} WHERE trade_date BETWEEN %s AND %s
                   AND dv_ttm_percentile_5y IS NOT NULL AND (COALESCE(dv_observation_days,0)<%s OR dv_ttm_percentile_5y<0 OR dv_ttm_percentile_5y>1)""" + code_valuation,
                [start_date, end_date, DIVIDEND_PERCENTILE_MIN_OBSERVATIONS, *suffix],
            ),
            "valuation_future_rows": (
                valuation,
                f"""SELECT COUNT(*) AS value FROM {valuation} WHERE trade_date BETWEEN %s AND %s
                   AND (data_available_timestamp IS NULL OR data_available_timestamp < trade_date::timestamp AT TIME ZONE 'Asia/Shanghai'
                        OR data_available_timestamp >= (trade_date + 1)::timestamp AT TIME ZONE 'Asia/Shanghai')""" + code_valuation,
                [start_date, end_date, *suffix],
            ),
        }
        report = {"scope": {"start_date": start_date.isoformat(), "end_date": end_date.isoformat(), "codes": codes or "ALL"}, "checks": {}, "failures": {}}
        for name, (source, sql, params) in checks.items():
            report["checks"][name] = int(self.kernel.read._repository_query(context, source, sql, params=params, scope=scope, max_rows=2)[0]["value"] or 0)
        zero_required = {
            "income_mapping_mismatch", "cashflow_mapping_mismatch", "indicator_future_comp_type_link",
            "company_missing_report_periods", "company_contract_mismatch", "invalid_complete_ttm",
            "invalid_ocf_ratio", "invalid_comp_type_cashflow", "missing_pit_evidence",
            "valuation_missing_rows", "valuation_extra_rows", "valuation_contract_mismatch",
            "invalid_dividend_percentile", "valuation_future_rows",
        }
        for name in zero_required:
            if report["checks"][name] != 0:
                report["failures"][name] = report["checks"][name]
        for name in ("company_rows", "valuation_rows"):
            if report["checks"][name] <= 0:
                report["failures"][name] = "NO_ROWS"
        report["informational"] = {
            "company_future_rows": report["checks"]["company_future_rows"],
        }
        report["passed"] = not report["failures"]
        return report
