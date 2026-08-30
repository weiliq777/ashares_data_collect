-- Core data-flow data dictionary.
-- All comments are attached to the actual parent tables used by the
-- Raw -> Standard -> PIT -> Feature -> Baseline -> Research flow.
-- The block is idempotent and also comments newly added columns with a
-- Chinese, table-scoped fallback instead of silently leaving them undocumented.

DO $$
DECLARE
    t record;
    c record;
    comment_text text;
BEGIN
    FOR t IN
        SELECT * FROM (VALUES
            ('public','tushare_daily_raw','Tushare 原始日线数据表：保存供应商返回的逐股票逐交易日原始 JSON 数据。'),
            ('public','tushare_daily_basic_raw','Tushare 原始每日估值数据表：保存每日估值、市值和换手率原始 JSON 数据。'),
            ('public','tushare_stock_basic_raw_version','Tushare 股票基础资料版本表：追加保存股票基础资料的每次观测版本。'),
            ('public','tushare_financial_raw_version','Tushare 财务原始版本表：追加保存财务报告及其公告/修订版本。'),
            ('public','stock_basic_info','股票基础信息表：保存当前标准化前的股票名称、行业、市场和上市状态。'),
            ('public','trading_calendar','交易日历表：保存交易所每日是否开市及前一交易日。'),
            ('std','daily_kline','标准日线行情表：保存统一单位和字段语义后的股票日线数据。'),
            ('std','valuation_daily','标准估值日表：保存统一格式的估值、市值和供应商观察时间。'),
            ('std','stock_basic_info','标准股票基础信息表：保存统一股票代码、名称、行业、市场和上市状态。'),
            ('std','security_lifecycle','证券生命周期表：保存股票名称、市场和上市状态的有效时间区间。'),
            ('std','trading_calendar','标准交易日历表：作为所有交易日范围和开市判断的权威来源。'),
            ('pit','universe_daily','每日股票可研究集合时点表：判断某股票在指定交易日是否属于可研究 universe。'),
            ('pit','security_tradeability_daily','每日股票可交易性时点表：判断指定交易日能否买入、卖出及是否停牌。'),
            ('pit','financial_asof','财务数据时点表：保存指定决策日可见的财务报告版本，防止未来数据泄漏。'),
            ('feature','price_daily_v2','价格特征 v2 表：保存时点复权价格、收益率、均线和成交量特征。'),
            ('feature','valuation_daily_v1','估值日特征表：保存估值、市值及 3/5 年历史分位特征。'),
            ('feature','company_financial_v2','公司财务特征 v2 表：保存 TTM、增长、盈利质量和现金流质量特征。'),
            ('feature','shareholder_return_v1','股东回报特征表：保存股息及股东回报的时点特征。'),
            ('ops','tushare_data_release','数据基线发布表：登记一次通过质量门禁的数据发布点及其输入清单哈希。'),
            ('research','snapshot_manifest','研究快照清单表：登记每个研究截止日快照的版本、状态、质量和可复现证据。'),
            ('research','stock_snapshot_daily','研究日股票快照表：保存某一研究截止日可合法使用的股票研究输入。'),
            ('research','snapshot_lineage','研究快照血缘表：记录快照字段组实际读取的源表、源记录和哈希。')
        ) AS x(schema_name, table_name, table_comment)
    LOOP
        EXECUTE format('COMMENT ON TABLE %I.%I IS %L', t.schema_name, t.table_name, t.table_comment);

        FOR c IN
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema=t.schema_name AND table_name=t.table_name
            ORDER BY ordinal_position
        LOOP
            comment_text := CASE c.column_name
                WHEN 'feature_id' THEN '特征记录物理标识；仅用于定位派生特征记录，不作为业务复现唯一键。'
                WHEN 'record_id' THEN '原始版本记录物理标识；用于追加版本边界和基线截断。'
                WHEN 'lineage_id' THEN '血缘记录物理标识；唯一标识一条快照输入血缘记录。'
                WHEN 'snapshot_id' THEN '研究快照唯一标识；同一研究截止日、基线和契约版本对应一个快照。'
                WHEN 'release_id' THEN '数据基线发布物理标识。'
                WHEN 'ts_code' THEN 'Tushare 格式股票代码，例如 600519.SH。'
                WHEN 'stock_code' THEN '系统标准股票代码；与 Tushare 股票代码保持可追溯映射。'
                WHEN 'symbol' THEN '股票短代码或证券简称代码。'
                WHEN 'name' THEN '股票或证券中文名称。'
                WHEN 'exchange' THEN '交易所标识，例如 SSE（上交所）或 SZSE（深交所）。'
                WHEN 'market' THEN '上市市场/交易板块，例如 主板、创业板、科创板。'
                WHEN 'area' THEN '公司注册地或所属地域。'
                WHEN 'industry' THEN '股票所属行业分类；不能直接等同于概念板块。'
                WHEN 'list_status' THEN '上市状态，例如 L（上市）或 D（退市）。'
                WHEN 'list_date' THEN '上市日期。'
                WHEN 'delist_date' THEN '退市日期；仍上市时为空。'
                WHEN 'trade_date' THEN '交易日期；所有日频行情和估值特征的业务日期。'
                WHEN 'research_date' THEN '研究截止日期；快照允许使用信息的业务时点。'
                WHEN 'decision_date' THEN '决策/研究日期；PIT 财务数据必须在该日期可见。'
                WHEN 'report_date' THEN '财务报告期末日期，例如季度末或年度末。'
                WHEN 'effective_announce_date' THEN '采用的有效公告日期；用于确定财务数据何时可见。'
                WHEN 'ann_date' THEN '供应商公告日期。'
                WHEN 'f_ann_date' THEN '供应商首次公告日期。'
                WHEN 'end_date' THEN '供应商报告期末日期。'
                WHEN 'pretrade_date' THEN '前一个开市交易日。'
                WHEN 'is_open' THEN '交易所当天是否开市。'
                WHEN 'is_listed_asof' THEN '截至指定时点是否处于上市状态。'
                WHEN 'is_st_asof' THEN '截至指定时点是否为 ST 风险标记股票。'
                WHEN 'is_suspended_asof' THEN '截至指定时点是否停牌。'
                WHEN 'is_tradeable' THEN '指定交易日是否满足可交易条件。'
                WHEN 'can_buy' THEN '指定交易日是否允许买入。'
                WHEN 'can_sell' THEN '指定交易日是否允许卖出。'
                WHEN 'eligible_by_market' THEN '是否通过市场范围筛选并属于目标股票集合。'
                WHEN 'has_price_history' THEN '截至该时点是否存在所需行情历史。'
                WHEN 'days_since_listing' THEN '截至研究日的上市天数。'
                WHEN 'exclusion_reason' THEN '未进入研究集合的排除原因。'
                WHEN 'blocked_reason' THEN '不可交易的阻断原因。'
                WHEN 'limit_status' THEN '涨跌停或交易限制状态。'
                WHEN 'open' THEN '标准化开盘价。'
                WHEN 'high' THEN '标准化最高价。'
                WHEN 'low' THEN '标准化最低价。'
                WHEN 'close' THEN '标准化收盘价。'
                WHEN 'raw_open' THEN '原始未复权开盘价。'
                WHEN 'raw_high' THEN '原始未复权最高价。'
                WHEN 'raw_low' THEN '原始未复权最低价。'
                WHEN 'raw_close' THEN '原始未复权收盘价。'
                WHEN 'raw_volume' THEN '原始成交量。'
                WHEN 'raw_amount' THEN '原始成交额。'
                WHEN 'volume' THEN '标准化成交量；单位已由 Raw 原始单位转换。'
                WHEN 'amount' THEN '标准化成交额；单位已由 Raw 原始单位转换为系统标准单位。'
                WHEN 'up_limit' THEN '当日涨停价格。'
                WHEN 'down_limit' THEN '当日跌停价格。'
                WHEN 'payload' THEN '供应商原始 JSON 数据载荷；保留接口返回字段以支持审计和重放。'
                WHEN 'source' THEN '数据来源标识，例如 tushare。'
                WHEN 'source_observed_at' THEN '上游数据实际观测时间。'
                WHEN 'fetched_at' THEN '本系统抓取或写入该原始记录的时间。'
                WHEN 'calculated_at' THEN '派生数据计算完成时间。'
                WHEN 'created_at' THEN '记录创建时间。'
                WHEN 'built_at' THEN '研究快照构建完成时间。'
                WHEN 'validated_at' THEN '质量校验通过时间。'
                WHEN 'frozen_at' THEN '快照正式冻结时间；冻结后不得修改研究输入。'
                WHEN 'failed_at' THEN '快照确定性校验失败时间。'
                WHEN 'data_available_timestamp' THEN '该数据在系统中对研究可见的时间戳。'
                WHEN 'data_cutoff_timestamp' THEN '数据发布允许使用的时间边界。'
                WHEN 'version_available_date' THEN '某财务版本可用于研究的日期。'
                WHEN 'source_asof_date' THEN '源数据有效截止日期。'
                WHEN 'effective_from' THEN '生命周期记录生效起始日期。'
                WHEN 'effective_to' THEN '生命周期记录生效结束日期；为空表示仍有效。'
                WHEN 'source_record_key' THEN '源系统业务记录键，用于精确追溯源记录。'
                WHEN 'source_record_version' THEN '源记录版本号。'
                WHEN 'record_version' THEN '同一业务记录的版本序号。'
                WHEN 'business_key' THEN '源系统业务唯一键。'
                WHEN 'batch_id' THEN '采集或转换批次标识。'
                WHEN 'payload_hash' THEN '原始 JSON 载荷哈希，用于检测内容变化。'
                WHEN 'source_payload_hash' THEN '源记录载荷哈希，用于血缘一致性校验。'
                WHEN 'source_fingerprint' THEN '源输入组合指纹，用于判断派生特征是否需要重算。'
                WHEN 'diff_hash' THEN '代码差异哈希，用于复现当时构建环境。'
                WHEN 'quality_status' THEN '数据质量状态，例如 VALID、WARNING、FAILED。'
                WHEN 'vintage_quality' THEN '财务版本时点证据质量。'
                WHEN 'pit_evidence_level' THEN 'PIT 时点证据等级。'
                WHEN 'status' THEN '记录或快照生命周期状态。'
                WHEN 'failure_reason' THEN '确定性失败原因。'
                WHEN 'notes' THEN '人工或编排运行备注。'
                WHEN 'schema_version' THEN '研究快照契约版本。'
                WHEN 'feature_contract_version' THEN '特征契约版本。'
                WHEN 'feature_set_version' THEN '特征集合实现版本。'
                WHEN 'code_version' THEN '构建时业务代码版本标识。'
                WHEN 'git_commit' THEN '构建时 Git 提交哈希。'
                WHEN 'git_dirty' THEN '构建时工作区是否存在未提交修改。'
                WHEN 'code_archive_path' THEN '构建代码归档路径。'
                WHEN 'code_archive_sha256' THEN '构建代码归档 SHA-256 哈希。'
                WHEN 'code_archive_size_bytes' THEN '构建代码归档字节数。'
                WHEN 'quality_report_hash' THEN '本次质量报告哈希。'
                WHEN 'quality_report' THEN '本次质量检查结果 JSON。'
                WHEN 'input_manifest_hash' THEN '快照实际输入清单哈希。'
                WHEN 'baseline_quality_report_hash' THEN '绑定数据基线的质量报告哈希。'
                WHEN 'baseline_source_manifest_hash' THEN '绑定数据基线的源清单哈希。'
                WHEN 'data_baseline_tag' THEN '绑定的数据基线发布标签。'
                WHEN 'release_tag' THEN '数据基线发布标签。'
                WHEN 'release_id' THEN '数据基线发布物理标识。'
                WHEN 'baseline_cutoff_date' THEN '数据基线覆盖截止日期。'
                WHEN 'visibility_policy_version' THEN '数据可见性策略版本。'
                WHEN 'temporal_evidence_level' THEN '数据基线的时点证据等级。'
                WHEN 'revoked_at' THEN '数据基线撤销时间。'
                WHEN 'revoked_by' THEN '撤销数据基线的操作者。'
                WHEN 'revocation_reason' THEN '数据基线撤销原因。'
                WHEN 'revocation_evidence_hash' THEN '数据基线撤销证据哈希。'
                WHEN 'field_group' THEN '快照输入字段组，例如 price、valuation、financial。'
                WHEN 'source_table' THEN '实际读取的源表全限定名。'
                WHEN 'source_record_version' THEN '源记录版本号。'
                WHEN 'source_manifest' THEN '基线覆盖的源数据清单 JSON。'
                WHEN 'source_manifest_hash' THEN '源数据清单哈希。'
                WHEN 'visibility_policy_version' THEN '可见性策略版本。'
                WHEN 'decision_timestamp' THEN '研究决策时间戳。'
                WHEN 'financial_feature_contract_version' THEN '快照所使用的财务特征契约版本。'
                WHEN 'price_feature_version' THEN '快照所使用的价格特征版本。'
                WHEN 'price_adjustment_mode' THEN '价格复权/时点调整模式。'
                WHEN 'financial_data_available_timestamp' THEN '快照中财务特征对研究可见的时间戳。'
                WHEN 'financial_source_record_key' THEN '快照采用的财务源记录键。'
                WHEN 'financial_record_version' THEN '快照采用的财务记录版本。'
                WHEN 'financial_payload_hash' THEN '快照采用的财务源载荷哈希。'
                WHEN 'financial_source_observed_at' THEN '财务源数据观测时间。'
                WHEN 'financial_vintage_quality' THEN '快照财务版本时点证据质量。'
                WHEN 'financial_component_lineage' THEN 'v2 财务特征各组成部分的 JSON 血缘。'
                WHEN 'component_lineage' THEN '特征计算组成部分及源数据的 JSON 血缘。'
                WHEN 'source_manifest_hash' THEN '源清单哈希。'
                ELSE NULL
            END;

            IF comment_text IS NULL THEN
                IF t.table_name='company_financial_v2' THEN
                    comment_text := format('公司财务特征字段“%s”：用于报告期、TTM、增长或财务质量分析，具体取值受 PIT 可见性约束。', c.column_name);
                ELSIF t.table_name='price_daily_v2' THEN
                    comment_text := format('价格特征字段“%s”：基于时点行情和复权规则计算的价格/收益/技术指标。', c.column_name);
                ELSIF t.table_name='valuation_daily_v1' THEN
                    comment_text := format('估值特征字段“%s”：基于交易日估值数据和历史窗口计算的估值指标。', c.column_name);
                ELSIF t.table_name='stock_snapshot_daily' THEN
                    comment_text := format('研究快照字段“%s”：表示该股票在研究截止日可合法使用的研究输入。', c.column_name);
                ELSIF t.table_name='snapshot_manifest' THEN
                    comment_text := format('研究快照清单字段“%s”：用于记录快照生命周期和可复现证据。', c.column_name);
                ELSIF t.table_name='snapshot_lineage' THEN
                    comment_text := format('研究快照血缘字段“%s”：用于追溯该快照实际使用的源数据。', c.column_name);
                ELSIF t.table_name LIKE '%raw%' THEN
                    comment_text := format('原始数据字段“%s”：保留供应商原始观测，不应在 Raw 层解释为派生指标。', c.column_name);
                ELSIF t.schema_name='pit' THEN
                    comment_text := format('PIT 时点字段“%s”：必须结合研究日期判断可见性，不得使用未来版本。', c.column_name);
                ELSE
                    comment_text := format('数据字段“%s”：含义由所属数据层和表的中文说明确定。', c.column_name);
                END IF;
            END IF;

            EXECUTE format(
                'COMMENT ON COLUMN %I.%I.%I IS %L',
                t.schema_name, t.table_name, c.column_name, comment_text
            );
        END LOOP;
    END LOOP;
END $$;

-- Coverage check: every current column in the declared core tables must have
-- a non-empty PostgreSQL column comment after the migration.
DO $$
DECLARE
    missing_count integer;
BEGIN
    SELECT count(*) INTO missing_count
    FROM information_schema.columns c
    JOIN pg_class cls ON cls.relname=c.table_name
    JOIN pg_namespace ns ON ns.oid=cls.relnamespace AND ns.nspname=c.table_schema
    LEFT JOIN pg_description d ON d.objoid=cls.oid AND d.objsubid=c.ordinal_position
    WHERE (c.table_schema,c.table_name) IN (
        ('public','tushare_daily_raw'),('public','tushare_daily_basic_raw'),
        ('public','tushare_stock_basic_raw_version'),('public','tushare_financial_raw_version'),
        ('public','stock_basic_info'),('public','trading_calendar'),
        ('std','daily_kline'),('std','valuation_daily'),('std','stock_basic_info'),
        ('std','security_lifecycle'),('std','trading_calendar'),
        ('pit','universe_daily'),('pit','security_tradeability_daily'),('pit','financial_asof'),
        ('feature','price_daily_v2'),('feature','valuation_daily_v1'),
        ('feature','company_financial_v2'),('feature','shareholder_return_v1'),
        ('ops','tushare_data_release'),('research','snapshot_manifest'),
        ('research','stock_snapshot_daily'),('research','snapshot_lineage')
    )
    AND COALESCE(btrim(d.description),'')='';

    IF missing_count <> 0 THEN
        RAISE EXCEPTION '核心数据字典列备注覆盖不完整：% 列', missing_count;
    END IF;
END $$;
