-- 阶段三：Research 观察池与证据引用最小模型。
-- 观察池只引用快照；证据表只保存 OpenSearch/URL/人工材料的定位信息，不复制正文。

CREATE TABLE IF NOT EXISTS research.watchlist (
    watchlist_id UUID PRIMARY KEY,
    snapshot_id UUID NOT NULL REFERENCES research.snapshot_manifest(snapshot_id),
    name VARCHAR(120) NOT NULL,
    rule_version VARCHAR(40) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'DRAFT',
    max_members INTEGER NOT NULL DEFAULT 50 CHECK (max_members > 0),
    created_by VARCHAR(80),
    notes TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    activated_at TIMESTAMP,
    CHECK (status IN ('DRAFT','ACTIVE','ARCHIVED')),
    UNIQUE (snapshot_id, name)
);

CREATE INDEX IF NOT EXISTS idx_research_watchlist_snapshot
    ON research.watchlist (snapshot_id, status, created_at DESC);

CREATE TABLE IF NOT EXISTS research.watchlist_member (
    watchlist_id UUID NOT NULL REFERENCES research.watchlist(watchlist_id),
    snapshot_id UUID NOT NULL REFERENCES research.snapshot_manifest(snapshot_id),
    research_date DATE NOT NULL,
    ts_code VARCHAR(20) NOT NULL,
    rank_no INTEGER NOT NULL CHECK (rank_no > 0),
    score NUMERIC(12,6) NOT NULL,
    selection_track VARCHAR(40) NOT NULL,
    rule_version VARCHAR(40) NOT NULL,
    rationale JSONB NOT NULL,
    quality_status VARCHAR(30) NOT NULL DEFAULT 'VALID',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (watchlist_id, ts_code),
    CHECK (selection_track IN ('value_quality','growth_trend'))
);

CREATE INDEX IF NOT EXISTS idx_research_watchlist_member_lookup
    ON research.watchlist_member (snapshot_id, research_date, selection_track, rank_no);

CREATE TABLE IF NOT EXISTS research.evidence_reference (
    evidence_id UUID PRIMARY KEY,
    watchlist_id UUID REFERENCES research.watchlist(watchlist_id),
    snapshot_id UUID NOT NULL REFERENCES research.snapshot_manifest(snapshot_id),
    ts_code VARCHAR(20) NOT NULL,
    source_system VARCHAR(30) NOT NULL,
    source_type VARCHAR(40) NOT NULL,
    source_locator TEXT NOT NULL,
    document_index VARCHAR(120),
    document_id VARCHAR(240),
    source_url TEXT,
    content_hash CHAR(64),
    published_at TIMESTAMP,
    effective_date DATE,
    claim_role VARCHAR(20) NOT NULL DEFAULT 'context',
    locator JSONB NOT NULL DEFAULT '{}'::jsonb,
    note TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (claim_role IN ('supporting','contrary','context')),
    CHECK (source_system IN ('opensearch','url','manual','postgres')),
    UNIQUE (snapshot_id, ts_code, source_system, source_locator, claim_role)
);

CREATE INDEX IF NOT EXISTS idx_research_evidence_lookup
    ON research.evidence_reference (snapshot_id, ts_code, claim_role, published_at DESC);

CREATE OR REPLACE FUNCTION research.assert_watchlist_snapshot()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE snapshot_status VARCHAR(20); cutoff DATE;
BEGIN
    SELECT status, research_cutoff_date INTO snapshot_status, cutoff
    FROM research.snapshot_manifest WHERE snapshot_id = NEW.snapshot_id;
    IF snapshot_status IS NULL THEN
        RAISE EXCEPTION 'watchlist references missing snapshot: %', NEW.snapshot_id;
    END IF;
    IF NEW.status = 'ACTIVE' AND snapshot_status <> 'FROZEN' THEN
        RAISE EXCEPTION 'ACTIVE watchlist requires FROZEN snapshot: % (%)', NEW.snapshot_id, snapshot_status;
    END IF;
    IF NEW.status = 'ACTIVE' THEN
        NEW.activated_at := COALESCE(NEW.activated_at, CURRENT_TIMESTAMP);
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_research_watchlist_snapshot ON research.watchlist;
CREATE TRIGGER trg_research_watchlist_snapshot
BEFORE INSERT OR UPDATE ON research.watchlist
FOR EACH ROW EXECUTE FUNCTION research.assert_watchlist_snapshot();

CREATE OR REPLACE FUNCTION research.assert_watchlist_member()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE parent_snapshot UUID; parent_status VARCHAR(20); expected_cutoff DATE; stock_exists BOOLEAN;
BEGIN
    SELECT w.snapshot_id, w.status INTO parent_snapshot, parent_status
    FROM research.watchlist w WHERE w.watchlist_id = NEW.watchlist_id;
    IF parent_snapshot IS NULL OR parent_snapshot <> NEW.snapshot_id THEN
        RAISE EXCEPTION 'watchlist member snapshot mismatch: % <> %', NEW.snapshot_id, parent_snapshot;
    END IF;
    SELECT m.status, m.research_cutoff_date INTO parent_status, expected_cutoff
    FROM research.snapshot_manifest m WHERE m.snapshot_id = NEW.snapshot_id;
    IF parent_status IS NULL OR (parent_status <> 'FROZEN' AND parent_status <> 'VALIDATED') THEN
        RAISE EXCEPTION 'watchlist member requires VALIDATED/FROZEN snapshot: %', NEW.snapshot_id;
    END IF;
    IF NEW.research_date <> expected_cutoff THEN
        RAISE EXCEPTION 'watchlist member date mismatch: % <> %', NEW.research_date, expected_cutoff;
    END IF;
    SELECT EXISTS (
        SELECT 1 FROM research.stock_snapshot_daily s
        WHERE s.snapshot_id = NEW.snapshot_id
          AND s.research_date = NEW.research_date
          AND s.ts_code = NEW.ts_code
    ) INTO stock_exists;
    IF NOT stock_exists THEN
        RAISE EXCEPTION 'watchlist member is absent from stock snapshot: %', NEW.ts_code;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_research_watchlist_member ON research.watchlist_member;
CREATE TRIGGER trg_research_watchlist_member
BEFORE INSERT OR UPDATE ON research.watchlist_member
FOR EACH ROW EXECUTE FUNCTION research.assert_watchlist_member();

CREATE OR REPLACE FUNCTION research.assert_evidence_reference()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE watchlist_snapshot UUID; snapshot_status VARCHAR(20);
BEGIN
    IF NEW.watchlist_id IS NOT NULL THEN
        SELECT snapshot_id INTO watchlist_snapshot FROM research.watchlist
        WHERE watchlist_id = NEW.watchlist_id;
        IF watchlist_snapshot IS NULL OR watchlist_snapshot <> NEW.snapshot_id THEN
            RAISE EXCEPTION 'evidence watchlist snapshot mismatch: % <> %', NEW.snapshot_id, watchlist_snapshot;
        END IF;
    END IF;
    SELECT status INTO snapshot_status FROM research.snapshot_manifest
    WHERE snapshot_id = NEW.snapshot_id;
    IF snapshot_status IS NULL OR snapshot_status NOT IN ('VALIDATED','FROZEN') THEN
        RAISE EXCEPTION 'evidence requires VALIDATED/FROZEN snapshot: %', NEW.snapshot_id;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_research_evidence_reference ON research.evidence_reference;
CREATE TRIGGER trg_research_evidence_reference
BEFORE INSERT OR UPDATE ON research.evidence_reference
FOR EACH ROW EXECUTE FUNCTION research.assert_evidence_reference();
