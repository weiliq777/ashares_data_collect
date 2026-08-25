-- Physical cutover helper for the partitioned research snapshot.
-- The application role must not own research tables or receive general DDL.
-- This narrowly scoped function is owned by the schema owner and is callable
-- only by the ordinary runtime role after the Repository has validated data.
CREATE OR REPLACE FUNCTION research.cutover_stock_snapshot_partitioned(
    p_migration_id UUID,
    p_approved_by TEXT,
    p_backup_name TEXT
) RETURNS TEXT
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, research, ops
AS $$
DECLARE
    v_backup TEXT := NULLIF(BTRIM(p_backup_name), '');
BEGIN
    IF p_migration_id IS NULL OR NULLIF(BTRIM(p_approved_by), '') IS NULL THEN
        RAISE EXCEPTION 'snapshot cutover requires migration_id and approved_by';
    END IF;
    IF v_backup IS NULL OR v_backup !~ '^_legacy_stock_snapshot_daily_[0-9a-f]{12}$' THEN
        RAISE EXCEPTION 'invalid snapshot backup name';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM ops.foundation_table_migration
         WHERE migration_id = p_migration_id AND status = 'VALIDATED'
    ) THEN
        RAISE EXCEPTION 'snapshot migration is not VALIDATED: %', p_migration_id;
    END IF;
    IF to_regclass('research.stock_snapshot_daily') IS NULL
       OR to_regclass('research.stock_snapshot_daily_partitioned') IS NULL
       OR to_regclass(format('research.%I', v_backup)) IS NOT NULL THEN
        RAISE EXCEPTION 'snapshot cutover catalog state is invalid';
    END IF;

    LOCK TABLE research.stock_snapshot_daily IN ACCESS EXCLUSIVE MODE;
    EXECUTE format(
        'ALTER TABLE research.stock_snapshot_daily RENAME TO %I', v_backup
    );
    EXECUTE 'ALTER TABLE research.stock_snapshot_daily_partitioned '
            'RENAME TO stock_snapshot_daily';
    EXECUTE 'CREATE TRIGGER trg_research_stock_snapshot_mutable '
            'BEFORE INSERT OR UPDATE OR DELETE ON research.stock_snapshot_daily '
            'FOR EACH ROW EXECUTE FUNCTION research.assert_snapshot_child_mutable()';
    EXECUTE 'CREATE TRIGGER trg_research_stock_snapshot_date '
            'BEFORE INSERT OR UPDATE ON research.stock_snapshot_daily '
            'FOR EACH ROW EXECUTE FUNCTION research.assert_snapshot_date()';

    UPDATE ops.foundation_table_migration
       SET status = 'CUTOVER', cutover_at = CURRENT_TIMESTAMP,
           approved_by = LEFT(p_approved_by, 120)
     WHERE migration_id = p_migration_id AND status = 'VALIDATED';
    IF NOT FOUND THEN
        RAISE EXCEPTION 'snapshot cutover state changed concurrently';
    END IF;
    RETURN 'research.' || v_backup;
END;
$$;

ALTER FUNCTION research.cutover_stock_snapshot_partitioned(UUID, TEXT, TEXT)
    OWNER TO ashares;
REVOKE ALL ON FUNCTION research.cutover_stock_snapshot_partitioned(UUID, TEXT, TEXT)
    FROM PUBLIC;
GRANT EXECUTE ON FUNCTION research.cutover_stock_snapshot_partitioned(UUID, TEXT, TEXT)
    TO ashares_app;
