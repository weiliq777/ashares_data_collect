-- 阶段三：ACTIVE Research 结果只允许追加，禁止回改历史审计数据。

CREATE OR REPLACE FUNCTION research.assert_watchlist_snapshot()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE snapshot_status VARCHAR(20);
BEGIN
    IF TG_OP = 'DELETE' THEN
        IF OLD.status IN ('ACTIVE', 'ARCHIVED') THEN
            RAISE EXCEPTION 'ACTIVE/ARCHIVED watchlist cannot be deleted: %', OLD.watchlist_id;
        END IF;
        RETURN OLD;
    END IF;

    IF OLD.status = 'ARCHIVED' THEN
        RAISE EXCEPTION 'ARCHIVED watchlist is immutable: %', OLD.watchlist_id;
    END IF;
    IF OLD.status = 'ACTIVE' AND (
        NEW.status NOT IN ('ACTIVE', 'ARCHIVED') OR
        NEW.snapshot_id <> OLD.snapshot_id OR NEW.name <> OLD.name OR
        NEW.rule_version <> OLD.rule_version OR NEW.max_members <> OLD.max_members OR
        COALESCE(NEW.created_by, '') <> COALESCE(OLD.created_by, '') OR
        COALESCE(NEW.notes, '') <> COALESCE(OLD.notes, '')
    ) THEN
        RAISE EXCEPTION 'ACTIVE watchlist can only transition to ARCHIVED: %', OLD.watchlist_id;
    END IF;

    SELECT status INTO snapshot_status
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
BEFORE INSERT OR UPDATE OR DELETE ON research.watchlist
FOR EACH ROW EXECUTE FUNCTION research.assert_watchlist_snapshot();

CREATE OR REPLACE FUNCTION research.assert_watchlist_member()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE target_watchlist UUID; target_snapshot UUID; watchlist_status VARCHAR(20);
        snapshot_status VARCHAR(20); expected_cutoff DATE; stock_exists BOOLEAN;
BEGIN
    target_watchlist := CASE WHEN TG_OP = 'DELETE' THEN OLD.watchlist_id ELSE NEW.watchlist_id END;
    SELECT w.snapshot_id, w.status INTO target_snapshot, watchlist_status
    FROM research.watchlist w WHERE w.watchlist_id = target_watchlist;
    IF target_snapshot IS NULL THEN
        RAISE EXCEPTION 'watchlist member references missing watchlist: %', target_watchlist;
    END IF;
    IF watchlist_status = 'ARCHIVED' THEN
        RAISE EXCEPTION 'ARCHIVED watchlist members are immutable: %', target_watchlist;
    END IF;
    IF TG_OP IN ('UPDATE', 'DELETE') AND watchlist_status = 'ACTIVE' THEN
        RAISE EXCEPTION 'ACTIVE watchlist members are append-only: %', target_watchlist;
    END IF;
    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    IF target_snapshot <> NEW.snapshot_id THEN
        RAISE EXCEPTION 'watchlist member snapshot mismatch: % <> %', NEW.snapshot_id, target_snapshot;
    END IF;
    SELECT status, research_cutoff_date INTO snapshot_status, expected_cutoff
    FROM research.snapshot_manifest WHERE snapshot_id = NEW.snapshot_id;
    IF snapshot_status IS NULL OR snapshot_status NOT IN ('VALIDATED', 'FROZEN') THEN
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
BEFORE INSERT OR UPDATE OR DELETE ON research.watchlist_member
FOR EACH ROW EXECUTE FUNCTION research.assert_watchlist_member();

CREATE OR REPLACE FUNCTION research.assert_evidence_reference()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE target_watchlist UUID; target_snapshot UUID; watchlist_status VARCHAR(20);
        snapshot_status VARCHAR(20); cutoff DATE; stock_exists BOOLEAN; member_exists BOOLEAN;
BEGIN
    target_watchlist := CASE WHEN TG_OP = 'DELETE' THEN OLD.watchlist_id ELSE NEW.watchlist_id END;
    target_snapshot := CASE WHEN TG_OP = 'DELETE' THEN OLD.snapshot_id ELSE NEW.snapshot_id END;
    IF target_watchlist IS NOT NULL THEN
        SELECT w.snapshot_id, w.status INTO target_snapshot, watchlist_status
        FROM research.watchlist w WHERE w.watchlist_id = target_watchlist;
        IF target_snapshot IS NULL THEN
            RAISE EXCEPTION 'evidence references missing watchlist: %', target_watchlist;
        END IF;
        IF TG_OP <> 'DELETE' AND target_snapshot <> NEW.snapshot_id THEN
            RAISE EXCEPTION 'evidence watchlist snapshot mismatch: % <> %', NEW.snapshot_id, target_snapshot;
        END IF;
        IF watchlist_status = 'ARCHIVED' THEN
            RAISE EXCEPTION 'ARCHIVED evidence is immutable: %', target_watchlist;
        END IF;
        IF TG_OP IN ('UPDATE', 'DELETE') AND watchlist_status = 'ACTIVE' THEN
            RAISE EXCEPTION 'ACTIVE evidence is append-only: %', target_watchlist;
        END IF;
    END IF;
    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    SELECT status, research_cutoff_date INTO snapshot_status, cutoff
    FROM research.snapshot_manifest WHERE snapshot_id = NEW.snapshot_id;
    IF snapshot_status IS NULL OR snapshot_status NOT IN ('VALIDATED', 'FROZEN') THEN
        RAISE EXCEPTION 'evidence requires VALIDATED/FROZEN snapshot: %', NEW.snapshot_id;
    END IF;
    SELECT EXISTS (
        SELECT 1 FROM research.stock_snapshot_daily s
        WHERE s.snapshot_id = NEW.snapshot_id
          AND s.research_date = cutoff
          AND s.ts_code = NEW.ts_code
    ) INTO stock_exists;
    IF NOT stock_exists THEN
        RAISE EXCEPTION 'evidence stock is absent from snapshot: %', NEW.ts_code;
    END IF;
    IF NEW.watchlist_id IS NOT NULL THEN
        SELECT EXISTS (
            SELECT 1 FROM research.watchlist_member m
            WHERE m.watchlist_id = NEW.watchlist_id
              AND m.snapshot_id = NEW.snapshot_id
              AND m.ts_code = NEW.ts_code
        ) INTO member_exists;
        IF NOT member_exists THEN
            RAISE EXCEPTION 'evidence stock is not a member of watchlist: %', NEW.ts_code;
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_research_evidence_reference ON research.evidence_reference;
CREATE TRIGGER trg_research_evidence_reference
BEFORE INSERT OR UPDATE OR DELETE ON research.evidence_reference
FOR EACH ROW EXECUTE FUNCTION research.assert_evidence_reference();
