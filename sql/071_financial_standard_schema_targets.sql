-- 财务标准层目标表（financial Standard targets，标准财务表）。
--
-- 这些表是 public 下旧标准表的同结构实体，而不是 Raw 原始表的副本。
-- 财务事实表目前规模为中等量级，保留原主键与索引，不强行引入日期分区；
-- 迁移作业按主键窗口复制、校验，避免长事务和大内存聚合。
--
-- LIKE INCLUDING ALL 保留当前生产库已经应用的列、约束和索引（包括后续
-- 版本增加的 PIT 字段）。单独建立序列，避免目标表继续依赖 public 序列。

CREATE SCHEMA IF NOT EXISTS std;

CREATE TABLE IF NOT EXISTS std.financial_income_standard
    (LIKE public.financial_income_standard INCLUDING ALL);
CREATE TABLE IF NOT EXISTS std.financial_balance_sheet_standard
    (LIKE public.financial_balance_sheet_standard INCLUDING ALL);
CREATE TABLE IF NOT EXISTS std.financial_cash_flow_standard
    (LIKE public.financial_cash_flow_standard INCLUDING ALL);
CREATE TABLE IF NOT EXISTS std.fina_indicator_standard
    (LIKE public.fina_indicator_standard INCLUDING ALL);

CREATE SEQUENCE IF NOT EXISTS std.financial_income_standard_financial_id_seq;
CREATE SEQUENCE IF NOT EXISTS std.financial_balance_sheet_standard_financial_id_seq;
CREATE SEQUENCE IF NOT EXISTS std.financial_cash_flow_standard_financial_id_seq;
CREATE SEQUENCE IF NOT EXISTS std.fina_indicator_standard_indicator_id_seq;

ALTER SEQUENCE std.financial_income_standard_financial_id_seq
    OWNED BY std.financial_income_standard.financial_id;
ALTER SEQUENCE std.financial_balance_sheet_standard_financial_id_seq
    OWNED BY std.financial_balance_sheet_standard.financial_id;
ALTER SEQUENCE std.financial_cash_flow_standard_financial_id_seq
    OWNED BY std.financial_cash_flow_standard.financial_id;
ALTER SEQUENCE std.fina_indicator_standard_indicator_id_seq
    OWNED BY std.fina_indicator_standard.indicator_id;

ALTER TABLE std.financial_income_standard
    ALTER COLUMN financial_id SET DEFAULT
        nextval('std.financial_income_standard_financial_id_seq');
ALTER TABLE std.financial_balance_sheet_standard
    ALTER COLUMN financial_id SET DEFAULT
        nextval('std.financial_balance_sheet_standard_financial_id_seq');
ALTER TABLE std.financial_cash_flow_standard
    ALTER COLUMN financial_id SET DEFAULT
        nextval('std.financial_cash_flow_standard_financial_id_seq');
ALTER TABLE std.fina_indicator_standard
    ALTER COLUMN indicator_id SET DEFAULT
        nextval('std.fina_indicator_standard_indicator_id_seq');

ALTER TABLE std.financial_income_standard OWNER TO ashares;
ALTER TABLE std.financial_balance_sheet_standard OWNER TO ashares;
ALTER TABLE std.financial_cash_flow_standard OWNER TO ashares;
ALTER TABLE std.fina_indicator_standard OWNER TO ashares;

