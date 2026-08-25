-- Financial Standard cutover function.
-- The canonical std table is validated before this function retires public.
CREATE OR REPLACE FUNCTION ops.cutover_financial_standard(
    p_source_table text,
    p_backup_table text,
    p_approved_by text,
    p_migration_id text
)
RETURNS text
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, ops, public, std
AS $$
DECLARE
    backup_qualified text;
BEGIN
    IF p_source_table NOT IN (
        'financial_income_standard',
        'financial_balance_sheet_standard',
        'financial_cash_flow_standard',
        'fina_indicator_standard',
        'stock_financial_indicator'
    ) THEN
        RAISE EXCEPTION 'unsupported financial Standard source table: %', p_source_table;
    END IF;
    IF p_backup_table !~ '^[A-Za-z_][A-Za-z0-9_]*$'
       OR length(p_backup_table) > 60 THEN
        RAISE EXCEPTION 'invalid financial Standard backup table name';
    END IF;
    IF coalesce(trim(p_approved_by), '') = '' OR coalesce(trim(p_migration_id), '') = '' THEN
        RAISE EXCEPTION 'approved_by and migration_id are required';
    END IF;
    IF to_regclass(format('public.%I', p_source_table)) IS NULL THEN
        RAISE EXCEPTION 'legacy financial Standard table does not exist: %', p_source_table;
    END IF;
    IF to_regclass(format('std.%I', p_source_table)) IS NULL THEN
        RAISE EXCEPTION 'canonical financial Standard table does not exist: %', p_source_table;
    END IF;

    EXECUTE format('LOCK TABLE public.%I IN ACCESS EXCLUSIVE MODE', p_source_table);
    EXECUTE format('ALTER TABLE public.%I RENAME TO %I', p_source_table, p_backup_table);
    backup_qualified := format('public.%I', p_backup_table);
    EXECUTE format('REVOKE ALL ON TABLE %s FROM PUBLIC', backup_qualified);
    EXECUTE format('REVOKE ALL ON TABLE %s FROM ashares_app', backup_qualified);
    RETURN backup_qualified;
END;
$$;

ALTER FUNCTION ops.cutover_financial_standard(text, text, text, text) OWNER TO ashares;
REVOKE ALL ON FUNCTION ops.cutover_financial_standard(text, text, text, text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION ops.cutover_financial_standard(text, text, text, text) TO ashares_app;
