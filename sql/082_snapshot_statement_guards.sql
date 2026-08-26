-- Snapshot child-table guard optimization.
--
-- The original child guards were FOR EACH ROW and performed one manifest
-- lookup per row.  A rebuild of one historical snapshot can delete tens of
-- thousands of lineage rows, so that implementation turns a bounded delete
-- into tens of thousands of repeated index probes.  These AFTER STATEMENT
-- triggers inspect PostgreSQL transition tables once per statement.  If a
-- statement touches a FROZEN snapshot, the trigger raises and PostgreSQL
-- rolls the whole statement back; the immutability contract is unchanged.

CREATE OR REPLACE FUNCTION research.assert_snapshot_child_new_rows_mutable()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE frozen_id UUID;
BEGIN
    SELECT m.snapshot_id INTO frozen_id
      FROM research.snapshot_manifest m
      JOIN new_rows n ON n.snapshot_id = m.snapshot_id
     WHERE m.status = 'FROZEN'
     LIMIT 1;
    IF frozen_id IS NOT NULL THEN
        RAISE EXCEPTION 'FROZEN research snapshot child is immutable: %', frozen_id;
    END IF;
    RETURN NULL;
END;
$$;

CREATE OR REPLACE FUNCTION research.assert_snapshot_child_old_rows_mutable()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE frozen_id UUID;
BEGIN
    SELECT m.snapshot_id INTO frozen_id
      FROM research.snapshot_manifest m
      JOIN old_rows o ON o.snapshot_id = m.snapshot_id
     WHERE m.status = 'FROZEN'
     LIMIT 1;
    IF frozen_id IS NOT NULL THEN
        RAISE EXCEPTION 'FROZEN research snapshot child is immutable: %', frozen_id;
    END IF;
    RETURN NULL;
END;
$$;

CREATE OR REPLACE FUNCTION research.assert_snapshot_child_old_new_rows_mutable()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE frozen_id UUID;
BEGIN
    WITH touched AS (
        SELECT snapshot_id FROM old_rows
        UNION
        SELECT snapshot_id FROM new_rows
    )
    SELECT m.snapshot_id INTO frozen_id
      FROM research.snapshot_manifest m
      JOIN touched t ON t.snapshot_id = m.snapshot_id
     WHERE m.status = 'FROZEN'
     LIMIT 1;
    IF frozen_id IS NOT NULL THEN
        RAISE EXCEPTION 'FROZEN research snapshot child is immutable: %', frozen_id;
    END IF;
    RETURN NULL;
END;
$$;

-- Install one statement-level guard for each child table.  Separate triggers
-- are intentional: PostgreSQL only permits a transition table corresponding
-- to an event supported by that trigger.
DO $$
DECLARE table_name TEXT;
BEGIN
    FOREACH table_name IN ARRAY ARRAY[
        'snapshot_input', 'stock_snapshot_daily', 'snapshot_lineage'
    ] LOOP
        EXECUTE format(
            'DROP TRIGGER IF EXISTS %I ON research.%I',
            'trg_research_' || table_name || '_mutable', table_name
        );
        EXECUTE format(
            'DROP TRIGGER IF EXISTS %I ON research.%I',
            'trg_research_' || table_name || '_mutable_stmt_ins', table_name
        );
        EXECUTE format(
            'DROP TRIGGER IF EXISTS %I ON research.%I',
            'trg_research_' || table_name || '_mutable_stmt_upd', table_name
        );
        EXECUTE format(
            'DROP TRIGGER IF EXISTS %I ON research.%I',
            'trg_research_' || table_name || '_mutable_stmt_del', table_name
        );
        EXECUTE format(
            'CREATE TRIGGER %I AFTER INSERT ON research.%I '
            'REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT '
            'EXECUTE FUNCTION research.assert_snapshot_child_new_rows_mutable()',
            'trg_research_' || table_name || '_mutable_stmt_ins', table_name
        );
        EXECUTE format(
            'CREATE TRIGGER %I AFTER UPDATE ON research.%I '
            'REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows '
            'FOR EACH STATEMENT '
            'EXECUTE FUNCTION research.assert_snapshot_child_old_new_rows_mutable()',
            'trg_research_' || table_name || '_mutable_stmt_upd', table_name
        );
        EXECUTE format(
            'CREATE TRIGGER %I AFTER DELETE ON research.%I '
            'REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT '
            'EXECUTE FUNCTION research.assert_snapshot_child_old_rows_mutable()',
            'trg_research_' || table_name || '_mutable_stmt_del', table_name
        );
    END LOOP;
END;
$$;

CREATE OR REPLACE FUNCTION research.assert_snapshot_date_new_rows()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE invalid_id UUID;
BEGIN
    SELECT n.snapshot_id INTO invalid_id
      FROM new_rows n
      LEFT JOIN research.snapshot_manifest m ON m.snapshot_id = n.snapshot_id
     WHERE m.research_cutoff_date IS NULL
        OR n.research_date <> m.research_cutoff_date
     LIMIT 1;
    IF invalid_id IS NOT NULL THEN
        RAISE EXCEPTION 'snapshot research_date does not match manifest: %', invalid_id;
    END IF;
    RETURN NULL;
END;
$$;

DROP TRIGGER IF EXISTS trg_research_stock_snapshot_date ON research.stock_snapshot_daily;
DROP TRIGGER IF EXISTS trg_research_stock_snapshot_date_stmt_ins ON research.stock_snapshot_daily;
DROP TRIGGER IF EXISTS trg_research_stock_snapshot_date_stmt_upd ON research.stock_snapshot_daily;

CREATE TRIGGER trg_research_stock_snapshot_date_stmt_ins
AFTER INSERT ON research.stock_snapshot_daily
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT
EXECUTE FUNCTION research.assert_snapshot_date_new_rows();

CREATE TRIGGER trg_research_stock_snapshot_date_stmt_upd
AFTER UPDATE ON research.stock_snapshot_daily
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT
EXECUTE FUNCTION research.assert_snapshot_date_new_rows();
