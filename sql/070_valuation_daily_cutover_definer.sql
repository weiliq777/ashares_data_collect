-- Owner-safe physical cutover for the validated valuation Standard table.
CREATE OR REPLACE FUNCTION ops.cutover_valuation_daily_partitioned(
    p_migration_id UUID,
    p_approved_by TEXT,
    p_backup_name TEXT
) RETURNS TEXT
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, std, ops
AS $$
DECLARE
    v_backup TEXT := NULLIF(BTRIM(p_backup_name), '');
BEGIN
    IF p_migration_id IS NULL OR NULLIF(BTRIM(p_approved_by), '') IS NULL THEN
        RAISE EXCEPTION 'valuation_daily cutover requires migration_id and approved_by';
    END IF;
    IF v_backup IS NULL OR v_backup !~ '^_legacy_valuation_daily_[0-9a-f]{12}$' THEN
        RAISE EXCEPTION 'invalid valuation_daily backup name';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM ops.foundation_table_migration
         WHERE migration_id = p_migration_id AND status = 'VALIDATED'
           AND source_table = 'public.valuation_daily'
           AND target_table = 'std.valuation_daily'
    ) THEN
        RAISE EXCEPTION 'valuation_daily migration is not VALIDATED: %', p_migration_id;
    END IF;
    IF to_regclass('public.valuation_daily') IS NULL
       OR to_regclass('std.valuation_daily') IS NULL
       OR to_regclass(format('public.%I', v_backup)) IS NOT NULL THEN
        RAISE EXCEPTION 'valuation_daily cutover catalog state is invalid';
    END IF;

    LOCK TABLE public.valuation_daily IN ACCESS EXCLUSIVE MODE;
    LOCK TABLE std.valuation_daily IN ACCESS EXCLUSIVE MODE;
    EXECUTE format('ALTER TABLE public.valuation_daily RENAME TO %I', v_backup);
    EXECUTE format('REVOKE ALL ON TABLE public.%I FROM ashares_app', v_backup);

    UPDATE ops.foundation_table_migration
       SET status = 'CUTOVER', cutover_at = CURRENT_TIMESTAMP,
           approved_by = LEFT(p_approved_by, 120)
     WHERE migration_id = p_migration_id AND status = 'VALIDATED';
    IF NOT FOUND THEN
        RAISE EXCEPTION 'valuation_daily cutover state changed concurrently';
    END IF;
    RETURN 'public.' || v_backup;
END;
$$;

ALTER FUNCTION ops.cutover_valuation_daily_partitioned(UUID, TEXT, TEXT) OWNER TO ashares;
REVOKE ALL ON FUNCTION ops.cutover_valuation_daily_partitioned(UUID, TEXT, TEXT) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION ops.cutover_valuation_daily_partitioned(UUID, TEXT, TEXT) TO ashares_app;
