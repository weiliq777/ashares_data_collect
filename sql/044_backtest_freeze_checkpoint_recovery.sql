-- 允许 Freeze 在首次门禁失败、随后成功冻结后收口旧 checkpoint。
-- 仅允许 FREEZE: RUNNING/FAILED -> COMPLETED；不允许修改其他冻结 checkpoint。
CREATE OR REPLACE FUNCTION backtest.assert_dataset_checkpoint_mutable()
RETURNS TRIGGER AS $$
DECLARE
    current_status VARCHAR(20);
BEGIN
    SELECT status INTO current_status
      FROM backtest.dataset_manifest
     WHERE dataset_id = COALESCE(NEW.dataset_id, OLD.dataset_id);
    IF current_status = 'FROZEN'
       AND NOT (
           (TG_OP = 'INSERT'
            AND NEW.stage = 'FREEZE'
            AND NEW.status = 'COMPLETED'
            AND EXISTS (
                SELECT 1 FROM backtest.dataset_build_checkpoint c
                 WHERE c.dataset_id=NEW.dataset_id AND c.stage='FREEZE'
            ))
           OR
           (TG_OP = 'UPDATE'
            AND OLD.stage = 'FREEZE'
            AND OLD.status IN ('RUNNING','FAILED')
            AND NEW.status = 'COMPLETED')
       ) THEN
        RAISE EXCEPTION 'FROZEN Dataset 的构建 checkpoint 不可修改';
    END IF;
    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
