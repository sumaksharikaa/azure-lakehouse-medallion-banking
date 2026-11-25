-- Example analytics and Delta Lake operations on the gold layer.
-- Register the tables first (Databricks SQL or spark-sql), for example:
--   CREATE TABLE gold.fact_transactions USING DELTA LOCATION '<root>/lakehouse/gold/fact_transactions';

-- 1. Customer history: every SCD2 version for one customer
SELECT customer_id, city, state, segment, kyc_status, effective_from, effective_to, is_current, is_deleted
FROM gold.dim_customer
WHERE customer_id = 'C100125'
ORDER BY effective_from;

-- 2. Spend by segment *as it was at transaction time* (point-in-time correct)
SELECT d.segment, f.merchant_category, ROUND(SUM(f.amount_usd), 2) AS spend_usd, COUNT(*) AS txns
FROM gold.fact_transactions f
JOIN gold.dim_customer d ON f.customer_sk = d.customer_sk
WHERE f.txn_type = 'DEBIT'
GROUP BY d.segment, f.merchant_category
ORDER BY d.segment, spend_usd DESC;

-- 3. Accounts whose balance dropped below 10% of their opening balance
SELECT a.account_id, a.account_type, a.opening_balance, x.txn_date, x.closing_balance_usd
FROM gold.agg_daily_account_activity x
JOIN gold.dim_account a USING (account_id)
WHERE x.closing_balance_usd < 0.1 * a.opening_balance
ORDER BY x.closing_balance_usd;

-- 4. Data-quality trend from the audit log
SELECT batch_date, entity, rows_in, rows_quarantined,
       ROUND(100.0 * rows_quarantined / NULLIF(rows_in, 0), 2) AS pct_quarantined, dq_failures
FROM delta.`<root>/lakehouse/_audit/pipeline_runs`
WHERE layer = 'silver'
ORDER BY batch_date, entity;

-- 5. Delta Lake features: history, time travel, file layout
DESCRIBE HISTORY delta.`<root>/lakehouse/silver/customers`;
SELECT COUNT(*) FROM delta.`<root>/lakehouse/silver/customers` VERSION AS OF 0;
DESCRIBE DETAIL gold.fact_transactions;
