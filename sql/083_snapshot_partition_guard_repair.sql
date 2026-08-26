-- Repair statement-level snapshot guards on existing partitions.
-- PostgreSQL does not clone transition-table triggers from a partitioned
-- parent to partitions that already exist.  Install the same guard directly
-- on every current partition and remove the legacy row-level guard.

CREATE OR REPLACE FUNCTION research.install_stock_snapshot_partition_guards()
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    partition_name TEXT;
BEGIN
    -- A trigger created on the partitioned parent owns cloned triggers on
    -- every partition.  Drop the parent copies first; PostgreSQL then drops
    -- their clones atomically.
    DROP TRIGGER IF EXISTS trg_research_stock_snapshot_mutable
        ON research.stock_snapshot_daily;
    DROP TRIGGER IF EXISTS trg_research_stock_snapshot_date
        ON research.stock_snapshot_daily;
    FOR partition_name IN
        SELECT child.relname
          FROM pg_inherits i
          JOIN pg_class parent ON parent.oid = i.inhparent
          JOIN pg_class child ON child.oid = i.inhrelid
          JOIN pg_namespace n ON n.oid = child.relnamespace
         WHERE parent.oid = 'research.stock_snapshot_daily'::regclass
           AND n.nspname = 'research'
    LOOP
        EXECUTE format(
            'DROP TRIGGER IF EXISTS trg_research_stock_snapshot_mutable ON research.%I',
            partition_name
        );
        EXECUTE format(
            'DROP TRIGGER IF EXISTS trg_research_stock_snapshot_date ON research.%I',
            partition_name
        );
        EXECUTE format(
            'DROP TRIGGER IF EXISTS trg_research_stock_snapshot_mutable_stmt_ins ON research.%I',
            partition_name
        );
        EXECUTE format(
            'DROP TRIGGER IF EXISTS trg_research_stock_snapshot_mutable_stmt_upd ON research.%I',
            partition_name
        );
        EXECUTE format(
            'DROP TRIGGER IF EXISTS trg_research_stock_snapshot_mutable_stmt_del ON research.%I',
            partition_name
        );
        EXECUTE format(
            'CREATE TRIGGER trg_research_stock_snapshot_mutable_stmt_ins '
            'AFTER INSERT ON research.%I REFERENCING NEW TABLE AS new_rows '
            'FOR EACH STATEMENT EXECUTE FUNCTION research.assert_snapshot_child_new_rows_mutable()',
            partition_name
        );
        EXECUTE format(
            'CREATE TRIGGER trg_research_stock_snapshot_mutable_stmt_upd '
            'AFTER UPDATE ON research.%I REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows '
            'FOR EACH STATEMENT EXECUTE FUNCTION research.assert_snapshot_child_old_new_rows_mutable()',
            partition_name
        );
        EXECUTE format(
            'CREATE TRIGGER trg_research_stock_snapshot_mutable_stmt_del '
            'AFTER DELETE ON research.%I REFERENCING OLD TABLE AS old_rows '
            'FOR EACH STATEMENT EXECUTE FUNCTION research.assert_snapshot_child_old_rows_mutable()',
            partition_name
        );
        EXECUTE format(
            'CREATE TRIGGER trg_research_stock_snapshot_date_stmt_ins '
            'AFTER INSERT ON research.%I REFERENCING NEW TABLE AS new_rows '
            'FOR EACH STATEMENT EXECUTE FUNCTION research.assert_snapshot_date_new_rows()',
            partition_name
        );
        EXECUTE format(
            'CREATE TRIGGER trg_research_stock_snapshot_date_stmt_upd '
            'AFTER UPDATE ON research.%I REFERENCING NEW TABLE AS new_rows '
            'FOR EACH STATEMENT EXECUTE FUNCTION research.assert_snapshot_date_new_rows()',
            partition_name
        );
    END LOOP;
END;
$$;

SELECT research.install_stock_snapshot_partition_guards();
