-- Legacy financial indicator compatibility target.
-- This table remains only for the old Raw-to-Standard adapter; canonical
-- financial v2 consumers use the four versioned std financial tables.
CREATE TABLE IF NOT EXISTS std.stock_financial_indicator
    (LIKE public.stock_financial_indicator INCLUDING ALL);
ALTER TABLE std.stock_financial_indicator OWNER TO ashares;
