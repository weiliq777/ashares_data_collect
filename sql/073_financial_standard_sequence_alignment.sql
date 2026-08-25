-- Align canonical Standard surrogate-key sequences after physical migration.
-- This is required before any incremental upsert omits the surrogate key.
SELECT setval(
    'std.financial_income_standard_financial_id_seq'::regclass,
    COALESCE((SELECT MAX(financial_id) FROM std.financial_income_standard), 1),
    (SELECT COUNT(*) > 0 FROM std.financial_income_standard)
);
SELECT setval(
    'std.financial_balance_sheet_standard_financial_id_seq'::regclass,
    COALESCE((SELECT MAX(financial_id) FROM std.financial_balance_sheet_standard), 1),
    (SELECT COUNT(*) > 0 FROM std.financial_balance_sheet_standard)
);
SELECT setval(
    'std.financial_cash_flow_standard_financial_id_seq'::regclass,
    COALESCE((SELECT MAX(financial_id) FROM std.financial_cash_flow_standard), 1),
    (SELECT COUNT(*) > 0 FROM std.financial_cash_flow_standard)
);
SELECT setval(
    'std.fina_indicator_standard_indicator_id_seq'::regclass,
    COALESCE((SELECT MAX(indicator_id) FROM std.fina_indicator_standard), 1),
    (SELECT COUNT(*) > 0 FROM std.fina_indicator_standard)
);
