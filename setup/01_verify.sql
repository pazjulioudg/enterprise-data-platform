-- =====================================================================================
-- 01_verify.sql - checks after each run (DEV). Open in the SQL editor and run one block at a time.
-- Replace _dev with _test / _prod to check other environments.
-- =====================================================================================

-- ---------- HR (after hr_daily batches 0, 1, 2) ----------
-- H1 SCD2: employees with more than one job/pay version (oldest first)
SELECT emplid, effdt, dept_id, job_title, annual_rt, __START_AT, __END_AT FROM hr_dev.silver.hr_job_history
WHERE emplid IN (SELECT emplid FROM hr_dev.silver.hr_job_history GROUP BY emplid HAVING count(*) > 1 LIMIT 3)
ORDER BY emplid, __START_AT;
-- H2 Quarantine: which rules rejected rows (expect rows after batch 1)
SELECT failed_rules, count(*) FROM hr_dev.silver.hr_job_quarantine GROUP BY ALL;
-- H3 Schema drift (after batch 2): new column reached Bronze but not the Silver contract
SELECT count(*) AS bronze_rows_with_work_mode FROM hr_dev.bronze.hr_job_raw WHERE after.work_mode IS NOT NULL;
-- H4 Gold row counts
SELECT count(*) AS versions, count_if(is_current) AS current_employees FROM hr_dev.gold.dim_employee;

-- ---------- FINANCE (after finance_ticks batches 0 and 1) ----------
-- F1 Exactly-once: duplicates delivered to Bronze vs kept in Silver (Silver must be 0)
SELECT 'bronze' AS layer, count(*) - count(DISTINCT tick_id) AS duplicates FROM finance_dev.bronze.ticks_raw
UNION ALL SELECT 'silver', count(*) - count(DISTINCT tick_id) FROM finance_dev.silver.ticks;
-- F2 Late ticks beyond the watermark were dropped (expect 0 in Silver)
SELECT count(*) AS late_ticks_in_silver FROM finance_dev.silver.ticks WHERE tick_id LIKE '%-LATE';
-- F3 Quarantined ticks and daily FX close
SELECT failed_rules, count(*) FROM finance_dev.silver.ticks_quarantine GROUP BY ALL;
SELECT * FROM finance_dev.gold.fx_daily_close ORDER BY rate_date, symbol;

-- ---------- ORM (after orm_daily batches 0 and 1) ----------
-- O1 Canonical events by source, and quarantine reasons (date rules, unmapped codes)
SELECT source_system, count(*) FROM risk_dev.silver.loss_event WHERE __END_AT IS NULL GROUP BY ALL;
SELECT source_system, failed_rules, count(*) FROM risk_dev.silver.loss_event_quarantine GROUP BY ALL;
-- O2 Regulatory loss data set (EUR 20k, 10 years) and GL reconciliation summary
SELECT accounting_year, sum(event_count) AS events, sum(net_loss_eur) AS net_eur
FROM risk_dev.gold.mart_regulatory_loss_dataset WHERE threshold_eur = 20000 GROUP BY 1 ORDER BY 1;
SELECT recon_status, count(*) FROM risk_dev.gold.recon_loss_vs_gl GROUP BY ALL;

-- ---------- DATA QUALITY (both engines write the same tables) ----------
-- Q1 Latest gate run per gate and engine
SELECT gate, engine, table_name, check_name, severity, passed, failed_count, total_count, threshold_pct
FROM hr_dev.ops.dq_results WHERE run_id = (SELECT max_by(run_id, run_ts) FROM hr_dev.ops.dq_results)
ORDER BY severity, table_name, check_name;
-- Q2 Compare engines: same rules, same failure counts?
SELECT check_name, engine, max(failed_count) AS failed FROM risk_dev.ops.dq_results GROUP BY ALL ORDER BY check_name, engine;
-- Q3 Rows quarantined by the Gold gate
SELECT table_name, count(*) FROM risk_dev.ops.dq_quarantine GROUP BY ALL;

-- ---------- SECURITY (run as each test user - guide step 10) ----------
-- S1 HR regional masking
SELECT country_code, national_id_type, national_id, birth_date, first_name FROM hr_dev.gold.dim_employee
WHERE is_current QUALIFY row_number() OVER (PARTITION BY national_id_type ORDER BY emplid) <= 2;
-- S2 ORM row-level security and narrative masking
SELECT legal_entity_id, confidentiality, count(*) AS events, any_value(event_description) AS sample
FROM risk_dev.gold.fact_loss_event_position GROUP BY ALL ORDER BY ALL;
-- S3 Least privilege: analysts must NOT read Silver (expect a permission error)
SELECT * FROM hr_dev.silver.hr_job_history LIMIT 1;

-- ---------- COMPLIANCE AND OPERATIONS ----------
-- C1 Who read the protected tables (audit; system tables can lag by up to a few hours)
SELECT event_time, user_identity.email, action_name, request_params['full_name_arg'] AS table_name
FROM system.access.audit
WHERE action_name IN ('getTable', 'commandSubmit') AND request_params['full_name_arg'] LIKE '%dim_employee%'
ORDER BY event_time DESC LIMIT 20;
-- C2 Impact analysis: what reads from Silver job history (lineage)
SELECT DISTINCT target_table_full_name, entity_type FROM system.access.table_lineage
WHERE source_table_full_name = 'hr_dev.silver.hr_job_history';
-- C3 GDPR erasure register
SELECT * FROM hr_dev.ops.erasure_requests ORDER BY received_date DESC;
-- C4 Maintenance log
SELECT * FROM hr_dev.ops.maintenance_log ORDER BY run_ts DESC LIMIT 20;
-- C5 Cost: spend (DBUs) per day and job over the last 14 days
SELECT usage_date, usage_metadata.job_id, sku_name, sum(usage_quantity) AS dbus
FROM system.billing.usage WHERE usage_date >= current_date() - 14 GROUP BY ALL ORDER BY usage_date DESC, dbus DESC;
