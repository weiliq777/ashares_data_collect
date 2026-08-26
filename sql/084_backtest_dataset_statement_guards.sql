-- Replace row-by-row Dataset child mutability checks with statement-level
-- transition-table checks.  This is required for bulk Dataset rebuilds,
-- especially backtest.dataset_execution_bar (回测执行行情表).

CREATE OR REPLACE FUNCTION backtest.assert_dataset_child_new_rows_mutable()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE invalid_id UUID;
BEGIN
    SELECT n.dataset_id INTO invalid_id
      FROM new_rows n
      LEFT JOIN backtest.dataset_manifest m ON m.dataset_id = n.dataset_id
     WHERE m.dataset_id IS NULL OR m.status IN ('VALIDATED', 'FROZEN')
     LIMIT 1;
    IF invalid_id IS NOT NULL THEN
        IF NOT EXISTS (
            SELECT 1 FROM backtest.dataset_manifest m
             WHERE m.dataset_id = invalid_id
        ) THEN
            RAISE EXCEPTION 'missing backtest dataset: %', invalid_id;
        END IF;
        RAISE EXCEPTION 'validated/frozen backtest dataset child is immutable: %', invalid_id;
    END IF;
    RETURN NULL;
END;
$$;

CREATE OR REPLACE FUNCTION backtest.assert_dataset_child_old_rows_mutable()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE invalid_id UUID;
BEGIN
    SELECT o.dataset_id INTO invalid_id
      FROM old_rows o
      LEFT JOIN backtest.dataset_manifest m ON m.dataset_id = o.dataset_id
     WHERE m.dataset_id IS NULL OR m.status IN ('VALIDATED', 'FROZEN')
     LIMIT 1;
    IF invalid_id IS NOT NULL THEN
        IF NOT EXISTS (
            SELECT 1 FROM backtest.dataset_manifest m
             WHERE m.dataset_id = invalid_id
        ) THEN
            RAISE EXCEPTION 'missing backtest dataset: %', invalid_id;
        END IF;
        RAISE EXCEPTION 'validated/frozen backtest dataset child is immutable: %', invalid_id;
    END IF;
    RETURN NULL;
END;
$$;

CREATE OR REPLACE FUNCTION backtest.assert_dataset_child_old_new_rows_mutable()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE invalid_id UUID;
BEGIN
    WITH touched AS (
        SELECT dataset_id FROM old_rows
        UNION
        SELECT dataset_id FROM new_rows
    )
    SELECT t.dataset_id INTO invalid_id
      FROM touched t
      LEFT JOIN backtest.dataset_manifest m ON m.dataset_id = t.dataset_id
     WHERE m.dataset_id IS NULL OR m.status IN ('VALIDATED', 'FROZEN')
     LIMIT 1;
    IF invalid_id IS NOT NULL THEN
        IF NOT EXISTS (
            SELECT 1 FROM backtest.dataset_manifest m
             WHERE m.dataset_id = invalid_id
        ) THEN
            RAISE EXCEPTION 'missing backtest dataset: %', invalid_id;
        END IF;
        RAISE EXCEPTION 'validated/frozen backtest dataset child is immutable: %', invalid_id;
    END IF;
    RETURN NULL;
END;
$$;

DO $$
DECLARE table_name TEXT;
BEGIN
    FOREACH table_name IN ARRAY ARRAY[
        'dataset_member', 'dataset_execution_bar', 'dataset_calendar',
        'dataset_dividend_event', 'dataset_rule_snapshot'
    ] LOOP
        EXECUTE format(
            'DROP TRIGGER IF EXISTS %I ON backtest.%I',
            CASE table_name
                WHEN 'dataset_member' THEN 'trg_backtest_dataset_member_mutable'
                WHEN 'dataset_execution_bar' THEN 'trg_backtest_execution_bar_mutable'
                WHEN 'dataset_calendar' THEN 'trg_backtest_calendar_mutable'
                WHEN 'dataset_dividend_event' THEN 'trg_backtest_dividend_mutable'
                ELSE 'trg_backtest_rule_snapshot_mutable'
            END,
            table_name
        );
        EXECUTE format(
            'CREATE TRIGGER %I AFTER INSERT ON backtest.%I '
            'REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT '
            'EXECUTE FUNCTION backtest.assert_dataset_child_new_rows_mutable()',
            'trg_backtest_' || table_name || '_mutable_stmt_ins', table_name
        );
        EXECUTE format(
            'CREATE TRIGGER %I AFTER UPDATE ON backtest.%I '
            'REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows '
            'FOR EACH STATEMENT '
            'EXECUTE FUNCTION backtest.assert_dataset_child_old_new_rows_mutable()',
            'trg_backtest_' || table_name || '_mutable_stmt_upd', table_name
        );
        EXECUTE format(
            'CREATE TRIGGER %I AFTER DELETE ON backtest.%I '
            'REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT '
            'EXECUTE FUNCTION backtest.assert_dataset_child_old_rows_mutable()',
            'trg_backtest_' || table_name || '_mutable_stmt_del', table_name
        );
    END LOOP;
END;
$$;
