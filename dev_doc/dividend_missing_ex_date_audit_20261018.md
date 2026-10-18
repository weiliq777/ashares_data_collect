# 已实施分红缺少 ex-date 的数据审计

日期：2026-10-18

范围：只读核对 `std.dividend_event`、对应 Raw 记录、目标 FROZEN Dataset 和两组 QualityTrendRotationV1 持仓。

## 结论

数据库中共有 31 条 `implementation_status='实施' AND ex_date IS NULL` 的 Standard 分红记录。逐条以 `std.dividend_event.record_version = tushare_dividend_raw.record_id` 对应 Raw 后，31 条均找到同一条 Raw 版本，且 Raw 的 `ex_date` 也为 NULL、`payload_hash` 与 Standard 一致。这不是 Standard 清洗时丢了一个已存在的日期；当前存档的 Tushare 原始版本本身就没有除权日。

其中 27 条所有已存日期字段都早于目标 Dataset 起点 2021-08-02。另有 4 条已知实施日期落在目标 Dataset 范围内。4 条都属于送转股（`cash_div=0`，`stock_div>0`），ex-date、现金派息日为空：

| 证券 | 报告期 | 实施公告日 | 股权登记日 | 送转比例字段 `stock_div` | 红利上市日 | Raw 版本 |
|---|---|---|---|---:|---|---:|
| `002175.SZ` | 2021-10-12 | 2021-12-21 | 2021-12-24 | 0.69384138 | 2021-12-27 | 16986 |
| `000523.SZ` | 2021-10-23 | 2021-12-03 | 2021-12-08 | 1.569 | 2021-12-09 | 3062 |
| `000980.SZ` | 2021-10-23 | 2021-12-09 | 2021-12-14 | 1.5 | 2021-12-15 | 11380 |
| `002072.SZ` | 2021-11-19 | 2021-12-18 | 2021-12-23 | 1.08909 | 2021-12-24 | 14556 |

表中的登记日、实施公告日和红利上市日不能代替 ex-date。不得根据这些日期推算并写入除权日。

## 对现有 Dataset 和回测的影响

目标 Dataset 为 `f348bc11-b4f0-41fe-a8b5-d376bb5cf401`，冻结范围截至 2026-08-21。`DatasetRepository` 构建公司行为时明确要求 `div_proc='实施' AND ex_date IS NOT NULL`，并将事件按 ex-date 限定到数据范围（`data_collect/data_access/repository/dataset_repository.py:291-299`）。查询目标 Dataset 的 `dataset_dividend_event`，四条 Raw 版本匹配数为 0。因此这四条记录没有进入该 Dataset 的公司行为表。

进一步通过现有 `BacktestRunRepository.diagnostic_lots()` 检查 ¥10,000 和 ¥20,000 两组回测的全部持仓 lot。对上述四个证券的已知实施公告日至红利上市日区间，未发现持仓重叠。由于 ex-date 本身缺失，不能据此证明实际除权日绝不会落在其他日期；但当前没有证据显示这四条记录影响了这两组 Run 的已记录收益。该结论限于这两组既有策略和 Run，不能推广到未来策略、其他 Dataset 或其他持仓。

Release gate 的 SQL 在 `data_collect/data_access/repository/release_repository.py:310-321` 对所有已实施且缺少 ex-date 的 Standard 记录计数，没有按 Dataset 时间范围过滤。最近一次长超时只读质量检查结果为：财务 PIT 相关检查及冻结前置检查通过；`implementation_without_ex_date=31` 是唯一失败项。因此，当前阻断的是全局 Release gate；旧 FROZEN Dataset 本身未被修改。新 Dataset 不能靠绕过或降低该门禁发布。

## 最小修复建议

1. **4 条范围内记录**：按证券和公司公告到交易所/上市公司原始公告核验具体 ex-date；保留公告链接/文件、抓取时间、校验哈希和源记录 ID。核实前保持 NULL 和 `WARNING`，不重写 Raw。
2. 若官方来源能确认日期，作为新的来源证据/版本追加下游事实并重建新的 Standard/Research Snapshot/Dataset 版本；旧版本及其回测不变。新版本需验证公司行为日期顺序、股本/现金处理、冻结执行行情以及 Dataset 公司行为完整性。
3. 若官方来源无法确认，维持缺失并让目标范围的质量报告明确失败，或者通过已版本化的数据契约将其标记为“范围内、未解决、不可用于完整公司行为结论”。只有在契约依据明确的研究/执行范围能证明 27 条是范围外后，才将其从该 Dataset 的适用范围检查中排除；不能仅按数量降低全局质量标准。
4. 对 Release gate 的后续工程改动，应让检查按目标 Dataset 的证券与有效日期范围报告 `IN_SCOPE`、`OUT_OF_SCOPE`、`UNRESOLVED` 数量，同时保留全库异常计数。不要静默过滤异常。

## 证据边界

本轮只检查了本地 Raw 与 Standard 中保存的 Tushare 版本，未查询交易所/上市公司公告，也未续费或调用 Tushare 接口。因此尚未证实存在可用的外部 ex-date 来源；上表能证明的是原始存档没有日期，而不是官方渠道一定没有日期。没有执行数据写入、Raw 重建或 Dataset 冻结。
