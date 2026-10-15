-- Preserve the persisted liquidity-reference invariant for v4 while retaining
-- the audit contract for existing v2/v3 runs.
BEGIN;

CREATE OR REPLACE FUNCTION backtest.assert_order_liquidity_reference()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE execution_version VARCHAR(80);
BEGIN
    SELECT execution_model_version INTO execution_version
      FROM backtest.run_manifest WHERE run_id=NEW.run_id;
    IF execution_version IN (
           'daily-next-open-v2', 'daily-next-open-v3', 'daily-next-open-v4'
       )
       AND NEW.status='FILLED'
       AND (NEW.liquidity_reference_date IS NULL
            OR NEW.liquidity_reference_date >= NEW.order_date) THEN
        RAISE EXCEPTION 'FILLED order requires a prior-day liquidity reference: %', NEW.order_id;
    END IF;
    RETURN NEW;
END;
$$;

COMMENT ON COLUMN backtest.order.liquidity_reference_date IS
    'daily-next-open-v2/v3/v4: last completed daily bar used for liquidity capacity; legacy v1 rows remain NULL';

COMMIT;
