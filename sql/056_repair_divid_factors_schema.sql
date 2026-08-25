-- Repair an older production instance that predates the complete dividend-factor contract.
-- Additive only: no rows are rewritten or removed.
ALTER TABLE IF EXISTS public.divid_factors
    ADD COLUMN IF NOT EXISTS interest NUMERIC(12,4),
    ADD COLUMN IF NOT EXISTS allot_price NUMERIC(12,4),
    ADD COLUMN IF NOT EXISTS dr NUMERIC(18,10);

GRANT SELECT, INSERT, UPDATE, DELETE ON public.divid_factors TO ashares_app;
