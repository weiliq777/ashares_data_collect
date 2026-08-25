# 财务 PIT warm-up 修复记录

- 日期：2026-08-11
- 状态：代码修复完成，历史 Raw 补齐任务后台执行中

## 根因

五年 Research 起点为 `2021-08-02`，但财务 Raw/Standard 最早公告日也从 `2021-08-02` 开始。由于 `version_available_date` 按公告日次日计算，`pit.financial_asof` 在首个研究日没有任何可见财务记录，导致首批 Snapshot 的财务字段全部为空并标记为 `MISSING_SOURCE`。

## 修复

- `tushare_financial_raw_init.py` 默认财务历史起点改为 `20160801`，为研究区间提供 warm-up。
- 新增 `repair_financial_warmup.py`，按顺序执行：

  `四张财务 Raw → 财务 Standard → pit.financial_asof → feature.company_financial_v1`

- `research_snapshot_series.py` 增加前置门禁：最早研究日没有可见财务 PIT 时立即失败，不再生成整批隐性 `MISSING_SOURCE` 快照。
- Raw 只追加，旧 Raw、旧 Snapshot、旧 Dataset 不删除或覆盖。
- 财务任务按股票、checkpoint、批间暂停顺序执行，支持中断后续传。

## 验收顺序

1. 财务 Raw 只补齐缺失 warm-up 区间 `20160801 ~ 20210801`，并保留已有的 `20210802 ~ 20260806`；
2. 重建财务 Standard/PIT/Feature；
3. 抽查首个研究日的 PIT/Feature 财务可见记录；
4. 重新续建 `baseline_20260810_feature_rebuild` 的 Research Snapshot；
5. 通过质量门禁后再 `FROZEN`，不能使用此前财务缺失的快照进行正式回测。
