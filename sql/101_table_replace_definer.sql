-- Narrow catalog-DDL bridge for the two registered TABLE_REPLACE workflows.
-- The runtime login has no CREATE/ALTER privilege on business schemas. The
-- fill SQL remains in the typed Repository; these functions only perform the
-- catalog steps required by RebuildGateway. Keep the allow-list narrow.

CREATE OR REPLACE FUNCTION ops.create_table_replace_staging(
    p_target text,
    p_staging text
) RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, ops, std, pit
AS $$
DECLARE
    v_schema text;
BEGIN
    IF p_target NOT IN ('std.security_lifecycle', 'pit.financial_asof') THEN
        RAISE EXCEPTION 'unsupported TABLE_REPLACE target: %', p_target;
    END IF;
    v_schema := split_part(p_target, '.', 1);
    IF p_staging !~ ('^' || v_schema || '\._staging_[a-z][a-z0-9_]*_[0-9a-f]{12}$') THEN
        RAISE EXCEPTION 'invalid TABLE_REPLACE staging name: %', p_staging;
    END IF;
    IF to_regclass(p_target) IS NULL THEN
        RAISE EXCEPTION 'TABLE_REPLACE target does not exist: %', p_target;
    END IF;
    IF to_regclass(p_staging) IS NOT NULL THEN
        RAISE EXCEPTION 'TABLE_REPLACE staging already exists: %', p_staging;
    END IF;

    -- Preserve target indexes and unique/primary-key semantics in the shadow
    -- table. PostgreSQL assigns fresh names to LIKE-copied indexes/constraints,
    -- so they do not collide with objects still attached to the live target.
    EXECUTE format(
        'CREATE TABLE %s (LIKE %s INCLUDING DEFAULTS INCLUDING INDEXES)',
        p_staging, p_target
    );
    EXECUTE format(
        'GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE %s TO ashares_app',
        p_staging
    );
END;
$$;

CREATE OR REPLACE FUNCTION ops.swap_table_replace_staging(
    p_target text,
    p_staging text,
    p_old text
) RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, ops, std, pit
AS $$
DECLARE
    v_schema text;
    v_target_name text;
    v_old_name text;
    v_sequence text;
    v_sequence_names text[] := ARRAY[]::text[];
    v_sequence_columns text[] := ARRAY[]::text[];
    v_column record;
    v_index integer;
BEGIN
    IF p_target NOT IN ('std.security_lifecycle', 'pit.financial_asof') THEN
        RAISE EXCEPTION 'unsupported TABLE_REPLACE target: %', p_target;
    END IF;
    v_schema := split_part(p_target, '.', 1);
    v_target_name := split_part(p_target, '.', 2);
    IF p_staging !~ ('^' || v_schema || '\._staging_[a-z][a-z0-9_]*_[0-9a-f]{12}$')
       OR p_old !~ ('^' || v_schema || '\._old_[a-z][a-z0-9_]*_[0-9a-f]{12}$') THEN
        RAISE EXCEPTION 'invalid TABLE_REPLACE relation name';
    END IF;
    v_old_name := split_part(p_old, '.', 2);
    IF to_regclass(p_target) IS NULL
       OR to_regclass(p_staging) IS NULL
       OR to_regclass(p_old) IS NOT NULL THEN
        RAISE EXCEPTION 'invalid TABLE_REPLACE catalog state';
    END IF;

    EXECUTE format('LOCK TABLE %s IN ACCESS EXCLUSIVE MODE', p_target);
    EXECUTE format('LOCK TABLE %s IN ACCESS EXCLUSIVE MODE', p_staging);

    -- LIKE INCLUDING DEFAULTS preserves a serial default, whose sequence is
    -- initially owned by the live table. Detach those sequence ownerships
    -- before dropping the renamed old table, then attach them to the new
    -- table after the catalog swap. Otherwise DROP old would cascade into a
    -- sequence still referenced by the new table's default.
    FOR v_column IN
        SELECT column_name
          FROM information_schema.columns
         WHERE table_schema = v_schema AND table_name = v_target_name
         ORDER BY ordinal_position
    LOOP
        v_sequence := pg_get_serial_sequence(p_target, v_column.column_name);
        IF v_sequence IS NOT NULL THEN
            EXECUTE format('ALTER SEQUENCE %s OWNED BY NONE', v_sequence);
            v_sequence_names := array_append(v_sequence_names, v_sequence);
            v_sequence_columns := array_append(
                v_sequence_columns, v_column.column_name
            );
        END IF;
    END LOOP;

    EXECUTE format('ALTER TABLE %s RENAME TO %I', p_target, v_old_name);
    EXECUTE format('ALTER TABLE %s RENAME TO %I', p_staging, v_target_name);
    EXECUTE format('DROP TABLE %s', p_old);

    FOR v_index IN 1..coalesce(array_length(v_sequence_names, 1), 0)
    LOOP
        EXECUTE format(
            'ALTER SEQUENCE %s OWNED BY %s.%I',
            v_sequence_names[v_index], p_target,
            v_sequence_columns[v_index]
        );
    END LOOP;
END;
$$;

CREATE OR REPLACE FUNCTION ops.drop_table_replace_staging(
    p_staging text
) RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, ops, std, pit
AS $$
BEGIN
    IF p_staging !~ '^(std|pit)\._staging_[a-z][a-z0-9_]*_[0-9a-f]{12}$' THEN
        RAISE EXCEPTION 'invalid TABLE_REPLACE staging name: %', p_staging;
    END IF;
    IF to_regclass(p_staging) IS NOT NULL THEN
        EXECUTE format('DROP TABLE %s', p_staging);
    END IF;
END;
$$;

ALTER FUNCTION ops.create_table_replace_staging(text, text) OWNER TO ashares;
ALTER FUNCTION ops.swap_table_replace_staging(text, text, text) OWNER TO ashares;
ALTER FUNCTION ops.drop_table_replace_staging(text) OWNER TO ashares;
REVOKE ALL ON FUNCTION ops.create_table_replace_staging(text, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION ops.swap_table_replace_staging(text, text, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION ops.drop_table_replace_staging(text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION ops.create_table_replace_staging(text, text) TO ashares_app;
GRANT EXECUTE ON FUNCTION ops.swap_table_replace_staging(text, text, text) TO ashares_app;
GRANT EXECUTE ON FUNCTION ops.drop_table_replace_staging(text) TO ashares_app;
