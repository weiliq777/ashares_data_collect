-- T-064: prepare the partitioned physical target for the legacy research
-- snapshot.  This is DDL only; the resumable copy is data_collect.jobs.
-- The original table remains the read/write table until an explicit,
-- validated short-lock cutover is performed.

CREATE TABLE IF NOT EXISTS research.stock_snapshot_daily_partitioned
    (LIKE research.stock_snapshot_daily INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING COMMENTS)
    PARTITION BY RANGE (research_date);

ALTER TABLE research.stock_snapshot_daily_partitioned
    DROP CONSTRAINT IF EXISTS stock_snapshot_daily_partitioned_pkey;
ALTER TABLE research.stock_snapshot_daily_partitioned
    ADD CONSTRAINT stock_snapshot_daily_partitioned_pkey
    PRIMARY KEY (snapshot_id, research_date, ts_code);

ALTER TABLE research.stock_snapshot_daily_partitioned
    DROP CONSTRAINT IF EXISTS stock_snapshot_daily_partitioned_snapshot_id_fkey;
ALTER TABLE research.stock_snapshot_daily_partitioned
    ADD CONSTRAINT stock_snapshot_daily_partitioned_snapshot_id_fkey
    FOREIGN KEY (snapshot_id) REFERENCES research.snapshot_manifest(snapshot_id);

DO $$
DECLARE y INTEGER;
BEGIN
    FOR y IN 2010..2035 LOOP
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS research.stock_snapshot_daily_partitioned_p%s PARTITION OF research.stock_snapshot_daily_partitioned FOR VALUES FROM (%L) TO (%L)',
            y, make_date(y, 1, 1), make_date(y + 1, 1, 1)
        );
    END LOOP;
END;
$$;

CREATE INDEX IF NOT EXISTS idx_snapshot_partitioned_code
    ON research.stock_snapshot_daily_partitioned (ts_code, research_date, snapshot_id);
CREATE INDEX IF NOT EXISTS idx_snapshot_partitioned_feature_version
    ON research.stock_snapshot_daily_partitioned (snapshot_id, price_feature_version);

-- Do not install the FROZEN-child trigger on the copy target yet: the target
-- is populated from already-FROZEN source rows.  The explicit short-lock
-- cutover installs that trigger on the final table in the same transaction
-- as the catalog rename.
DROP TRIGGER IF EXISTS trg_research_stock_snapshot_partitioned_mutable
    ON research.stock_snapshot_daily_partitioned;

-- The date trigger also performs a per-row manifest lookup.  Source rows are
-- copied only after source-table validation and each window is hash-checked;
-- install this trigger only on the final table during the cutover transaction.
DROP TRIGGER IF EXISTS trg_research_stock_snapshot_partitioned_date
    ON research.stock_snapshot_daily_partitioned;

COMMENT ON TABLE research.stock_snapshot_daily_partitioned IS
    '正式分区物理目标；经每日窗口复制、校验和短锁切换后替代 legacy stock_snapshot_daily。';
