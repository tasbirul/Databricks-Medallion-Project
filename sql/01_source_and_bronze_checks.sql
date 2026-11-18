-- =====================================================================================================
-- sql/01_source_and_bronze_checks.sql
-- Proves Bronze is a lossless copy of the source CSVs.
--
-- Bronze acceptance invariant:
--     orders source rows       = bronze_orders rows
--     transactions source rows = bronze_transactions rows
--
-- Run in the Databricks SQL editor. Save screenshots of the results to evidence/03_bronze/.
-- Replace `workspace` / `ecom_poc` if your names differ. This file only reads; it changes nothing.
--
-- ASSERTION queries return a column `violations` with expected value 0.
-- =====================================================================================================

USE CATALOG workspace;
USE SCHEMA ecom_poc;

-- =====================================================================================================
-- 1. SOURCE ROW COUNT vs BRONZE ROW COUNT  (data rows, header excluded)
--    The source is re-read straight from the CSV with an all-STRING schema, so nothing is inferred or cleaned.
-- =====================================================================================================
WITH src AS (
    SELECT 'orders' AS dataset, COUNT(*) AS source_rows
    FROM read_files(
        '/Volumes/workspace/ecom_poc/raw_files/orders.csv',
        format => 'csv', header => true,
        schema => 'order_id STRING, customer_id STRING, order_date STRING, category STRING, quantity STRING, unit_price STRING, order_status STRING, country STRING'
    )
    UNION ALL
    SELECT 'transactions', COUNT(*)
    FROM read_files(
        '/Volumes/workspace/ecom_poc/raw_files/transactions.csv',
        format => 'csv', header => true,
        schema => 'txn_id STRING, order_id STRING, payment_method STRING, amount STRING, currency STRING, txn_status STRING, txn_ts STRING'
    )
),
brz AS (
    SELECT 'orders' AS dataset, COUNT(*) AS bronze_rows FROM bronze_orders
    UNION ALL
    SELECT 'transactions', COUNT(*) FROM bronze_transactions
)
SELECT s.dataset,
       s.source_rows,
       b.bronze_rows,
       b.bronze_rows - s.source_rows AS difference,
       CASE WHEN s.source_rows = b.bronze_rows THEN 'PASS' ELSE 'FAIL' END AS parity
FROM src s
JOIN brz b ON s.dataset = b.dataset
ORDER BY s.dataset;

-- 1b. Assignment contract: each file must have exactly 1,000,000 data rows
SELECT 'orders' AS dataset, COUNT(*) AS bronze_rows, 1000000 AS required_rows,
       CASE WHEN COUNT(*) = 1000000 THEN 'PASS' ELSE 'FAIL' END AS meets_contract
FROM bronze_orders
UNION ALL
SELECT 'transactions', COUNT(*), 1000000,
       CASE WHEN COUNT(*) = 1000000 THEN 'PASS' ELSE 'FAIL' END
FROM bronze_transactions;

-- =====================================================================================================
-- 2. BRONZE SCHEMA: every SOURCE column must be STRING (screenshot this)
-- =====================================================================================================
SELECT table_name, ordinal_position, column_name, data_type,
       CASE WHEN LEFT(column_name, 1) = '_' THEN 'audit column' ELSE 'source column' END AS column_role
FROM workspace.information_schema.columns
WHERE table_schema = 'ecom_poc'
  AND table_name IN ('bronze_orders', 'bronze_transactions')
ORDER BY table_name, ordinal_position;

DESCRIBE TABLE workspace.ecom_poc.bronze_orders;
DESCRIBE TABLE workspace.ecom_poc.bronze_transactions;

-- ASSERTION: no source column in Bronze may be typed as anything other than STRING
SELECT COUNT(*) AS violations
FROM workspace.information_schema.columns
WHERE table_schema = 'ecom_poc'
  AND table_name IN ('bronze_orders', 'bronze_transactions')
  AND LEFT(column_name, 1) <> '_'
  AND data_type <> 'STRING';

-- ASSERTION: source columns match the contract exactly (names, none missing, none extra)
WITH expected(table_name, column_name) AS (
    VALUES
    ('bronze_orders','order_id'),('bronze_orders','customer_id'),('bronze_orders','order_date'),
    ('bronze_orders','category'),('bronze_orders','quantity'),('bronze_orders','unit_price'),
    ('bronze_orders','order_status'),('bronze_orders','country'),
    ('bronze_transactions','txn_id'),('bronze_transactions','order_id'),('bronze_transactions','payment_method'),
    ('bronze_transactions','amount'),('bronze_transactions','currency'),('bronze_transactions','txn_status'),
    ('bronze_transactions','txn_ts')
),
actual AS (
    SELECT table_name, column_name
    FROM workspace.information_schema.columns
    WHERE table_schema = 'ecom_poc'
      AND table_name IN ('bronze_orders', 'bronze_transactions')
      AND LEFT(column_name, 1) <> '_'
)
SELECT (SELECT COUNT(*) FROM expected e LEFT ANTI JOIN actual a ON e.table_name = a.table_name AND e.column_name = a.column_name)
     + (SELECT COUNT(*) FROM actual a LEFT ANTI JOIN expected e ON e.table_name = a.table_name AND e.column_name = a.column_name)
       AS violations;

-- =====================================================================================================
-- 3. AUDIT COLUMNS: the four required ones (+ _row_id, added so exact duplicate rows can be told apart)
-- =====================================================================================================
WITH required(audit_column) AS (
    VALUES ('_ingested_at'), ('_source_file'), ('_batch_id'), ('_row_hash'), ('_row_id')
),
tbl(table_name) AS (
    VALUES ('bronze_orders'), ('bronze_transactions')
)
SELECT t.table_name, r.audit_column, c.data_type,
       CASE WHEN c.column_name IS NOT NULL THEN 'present' ELSE 'MISSING' END AS status
FROM tbl t
CROSS JOIN required r
LEFT JOIN workspace.information_schema.columns c
       ON c.table_schema = 'ecom_poc' AND c.table_name = t.table_name AND c.column_name = r.audit_column
ORDER BY t.table_name, r.audit_column;

-- ASSERTION: no audit column is missing
WITH required(audit_column) AS (
    VALUES ('_ingested_at'), ('_source_file'), ('_batch_id'), ('_row_hash'), ('_row_id')
),
tbl(table_name) AS (
    VALUES ('bronze_orders'), ('bronze_transactions')
)
SELECT COUNT(*) AS violations
FROM tbl t
CROSS JOIN required r
LEFT ANTI JOIN workspace.information_schema.columns c
       ON c.table_schema = 'ecom_poc' AND c.table_name = t.table_name AND c.column_name = r.audit_column;

-- ASSERTION: audit values are always populated
SELECT COUNT(*) AS violations FROM bronze_orders
WHERE _ingested_at IS NULL OR _source_file IS NULL OR _batch_id IS NULL OR _row_hash IS NULL OR _row_id IS NULL;

SELECT COUNT(*) AS violations FROM bronze_transactions
WHERE _ingested_at IS NULL OR _source_file IS NULL OR _batch_id IS NULL OR _row_hash IS NULL OR _row_id IS NULL;

-- ASSERTION: _source_file records the real source path
SELECT COUNT(*) AS violations FROM bronze_orders
WHERE _source_file <> '/Volumes/workspace/ecom_poc/raw_files/orders.csv';

SELECT COUNT(*) AS violations FROM bronze_transactions
WHERE _source_file <> '/Volumes/workspace/ecom_poc/raw_files/transactions.csv';

-- =====================================================================================================
-- 4. SAMPLE ROWS: original values and metadata side by side (screenshot these)
-- =====================================================================================================
SELECT * FROM bronze_orders LIMIT 10;

SELECT * FROM bronze_transactions LIMIT 10;

-- 4b. PROOF Bronze did not clean anything: malformed originals must still be present
--     (blank values, text in numeric columns, unparseable dates, padded or oddly cased labels)
SELECT order_id, order_date, quantity, unit_price, order_status, country, _row_id
FROM bronze_orders
WHERE quantity IS NULL OR trim(quantity) = '' OR try_cast(trim(quantity) AS DOUBLE) IS NULL
   OR try_cast(trim(unit_price) AS DOUBLE) IS NULL
   OR try_cast(trim(order_date) AS DATE) IS NULL
   OR order_status <> trim(order_status)
LIMIT 20;

SELECT txn_id, order_id, payment_method, amount, currency, txn_status, txn_ts, _row_id
FROM bronze_transactions
WHERE amount IS NULL OR trim(amount) = '' OR try_cast(trim(amount) AS DOUBLE) IS NULL
   OR try_cast(trim(txn_ts) AS TIMESTAMP) IS NULL
   OR txn_status <> trim(txn_status)
LIMIT 20;

-- =====================================================================================================
-- 5. DUPLICATE-BATCH PREVENTION / REPEAT-RUN EVIDENCE
--    notebooks/02_bronze_ingestion.py derives a stable _batch_id from the file's path + size + modified time and
--    replaces only that batch (replaceWhere). Rerunning must NOT add rows.
-- =====================================================================================================

-- 5a. Exactly ONE batch per Bronze table (a second row here would mean the same file was loaded twice)
SELECT 'bronze_orders' AS table_name, _batch_id, COUNT(*) AS rows FROM bronze_orders GROUP BY _batch_id
UNION ALL
SELECT 'bronze_transactions', _batch_id, COUNT(*) FROM bronze_transactions GROUP BY _batch_id;

-- ASSERTION: exactly one batch id per table
SELECT ABS(COUNT(DISTINCT _batch_id) - 1) AS violations FROM bronze_orders;
SELECT ABS(COUNT(DISTINCT _batch_id) - 1) AS violations FROM bronze_transactions;

-- ASSERTION: _row_id is unique (every source row has its own identity, even exact duplicate rows)
SELECT COUNT(*) - COUNT(DISTINCT _row_id) AS violations FROM bronze_orders;
SELECT COUNT(*) - COUNT(DISTINCT _row_id) AS violations FROM bronze_transactions;

-- 5b. Exact duplicate source rows are PRESERVED in Bronze (same _row_hash, different _row_id)
SELECT 'bronze_orders' AS table_name, COUNT(*) AS rows, COUNT(DISTINCT _row_hash) AS distinct_row_hashes,
       COUNT(*) - COUNT(DISTINCT _row_hash) AS exact_duplicate_rows_kept
FROM bronze_orders
UNION ALL
SELECT 'bronze_transactions', COUNT(*), COUNT(DISTINCT _row_hash), COUNT(*) - COUNT(DISTINCT _row_hash)
FROM bronze_transactions;

-- 5c. Delta history: ingestion shows as overwrite with a replaceWhere predicate on _batch_id, never repeated appends
DESCRIBE HISTORY workspace.ecom_poc.bronze_orders;
DESCRIBE HISTORY workspace.ecom_poc.bronze_transactions;

-- 5d. REPEAT-RUN TEST (manual procedure; the notebook also automates it in its section 6)
--     1) Run the three queries below and note the numbers.
--     2) Rerun notebooks/02_bronze_ingestion.py.
--     3) Run them again. The results must be IDENTICAL (same rows, same distinct row ids, same single batch id).
SELECT 'bronze_orders' AS table_name, COUNT(*) AS rows, COUNT(DISTINCT _row_id) AS distinct_row_ids, COUNT(DISTINCT _batch_id) AS batches
FROM bronze_orders
UNION ALL
SELECT 'bronze_transactions', COUNT(*), COUNT(DISTINCT _row_id), COUNT(DISTINCT _batch_id)
FROM bronze_transactions;
