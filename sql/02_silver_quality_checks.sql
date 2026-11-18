-- =====================================================================================================
-- sql/02_silver_quality_checks.sql
-- Silver quality checks for orders and transactions, and the Bronze -> Silver reconciliation proof.
-- Run in the Databricks SQL editor (or any SQL cell). Replace `workspace` / `ecom_poc` everywhere if your names differ.
--
-- How to read results
--   * Every ASSERTION returns a column `violations`. The expected value is 0.
--   * Sections A and D are EVIDENCE queries (counts / examples). Save screenshots to evidence/04_silver/.
--
-- Prerequisites: notebooks 02, 03 and 04 have run. This file reads tables only; it changes nothing.
-- =====================================================================================================

USE CATALOG workspace;
USE SCHEMA ecom_poc;

-- =====================================================================================================
-- A. OUTCOME COUNTS (evidence) - Bronze = accepted + quarantine + duplicate losers
-- =====================================================================================================
SELECT 'orders' AS dataset,
       (SELECT COUNT(*) FROM workspace.ecom_poc.bronze_orders)     AS bronze_rows,
       (SELECT COUNT(*) FROM workspace.ecom_poc.silver_orders)     AS accepted_rows,
       (SELECT COUNT(*) FROM workspace.ecom_poc.quarantine_orders) AS quarantine_rows,
       (SELECT COUNT(*) FROM workspace.ecom_poc.duplicate_losers_orders)     AS duplicate_loser_rows
UNION ALL
SELECT 'transactions',
       (SELECT COUNT(*) FROM workspace.ecom_poc.bronze_transactions),
       (SELECT COUNT(*) FROM workspace.ecom_poc.silver_transactions),
       (SELECT COUNT(*) FROM workspace.ecom_poc.quarantine_transactions),
       (SELECT COUNT(*) FROM workspace.ecom_poc.duplicate_losers_transactions);

-- Same numbers with the invariant spelled out (difference must be 0)
SELECT dataset, bronze_rows, accepted_rows, quarantine_rows, duplicate_loser_rows,
       bronze_rows - (accepted_rows + quarantine_rows + duplicate_loser_rows) AS difference,
       CASE WHEN bronze_rows = accepted_rows + quarantine_rows + duplicate_loser_rows THEN 'PASS' ELSE 'FAIL' END AS totals_match
FROM (
    SELECT 'orders' AS dataset,
           (SELECT COUNT(*) FROM workspace.ecom_poc.bronze_orders)     AS bronze_rows,
           (SELECT COUNT(*) FROM workspace.ecom_poc.silver_orders)     AS accepted_rows,
           (SELECT COUNT(*) FROM workspace.ecom_poc.quarantine_orders) AS quarantine_rows,
           (SELECT COUNT(*) FROM workspace.ecom_poc.duplicate_losers_orders)     AS duplicate_loser_rows
    UNION ALL
    SELECT 'transactions',
           (SELECT COUNT(*) FROM workspace.ecom_poc.bronze_transactions),
           (SELECT COUNT(*) FROM workspace.ecom_poc.silver_transactions),
           (SELECT COUNT(*) FROM workspace.ecom_poc.quarantine_transactions),
           (SELECT COUNT(*) FROM workspace.ecom_poc.duplicate_losers_transactions)
);

-- =====================================================================================================
-- B. RECONCILIATION ASSERTIONS (row-level, using the Bronze _row_id). Totals alone are not proof.
-- =====================================================================================================

-- [orders] count_invariant: Bronze count must equal accepted + quarantine + duplicate losers
SELECT ABS(
    (SELECT COUNT(*) FROM workspace.ecom_poc.bronze_orders)
  - ((SELECT COUNT(*) FROM workspace.ecom_poc.silver_orders) + (SELECT COUNT(*) FROM workspace.ecom_poc.quarantine_orders) + (SELECT COUNT(*) FROM workspace.ecom_poc.duplicate_losers_orders))
) AS violations;

-- [orders] bronze_row_id_not_unique: Every Bronze row must have a unique _row_id (needed for row-level proof)
SELECT COUNT(*) - COUNT(DISTINCT _row_id) AS violations FROM workspace.ecom_poc.bronze_orders;

-- [orders] bronze_row_missing_from_outcomes: Every Bronze row must appear in some outcome table
WITH outcomes AS (
    SELECT _row_id, _batch_id, 'accepted'        AS outcome FROM workspace.ecom_poc.silver_orders
    UNION ALL
    SELECT _row_id, _batch_id, 'quarantine'      AS outcome FROM workspace.ecom_poc.quarantine_orders
    UNION ALL
    SELECT _row_id, _batch_id, 'duplicate_loser' AS outcome FROM workspace.ecom_poc.duplicate_losers_orders
)
SELECT COUNT(*) AS violations
FROM workspace.ecom_poc.bronze_orders b
LEFT ANTI JOIN outcomes o ON b._row_id = o._row_id;

-- [orders] outcome_row_not_in_bronze: Every outcome row must come from a Bronze row
WITH outcomes AS (
    SELECT _row_id, _batch_id, 'accepted'        AS outcome FROM workspace.ecom_poc.silver_orders
    UNION ALL
    SELECT _row_id, _batch_id, 'quarantine'      AS outcome FROM workspace.ecom_poc.quarantine_orders
    UNION ALL
    SELECT _row_id, _batch_id, 'duplicate_loser' AS outcome FROM workspace.ecom_poc.duplicate_losers_orders
)
SELECT COUNT(*) AS violations
FROM outcomes o
LEFT ANTI JOIN workspace.ecom_poc.bronze_orders b ON b._row_id = o._row_id;

-- [orders] row_in_multiple_outcomes: No Bronze row may appear more than once across all outcome tables
WITH outcomes AS (
    SELECT _row_id, _batch_id, 'accepted'        AS outcome FROM workspace.ecom_poc.silver_orders
    UNION ALL
    SELECT _row_id, _batch_id, 'quarantine'      AS outcome FROM workspace.ecom_poc.quarantine_orders
    UNION ALL
    SELECT _row_id, _batch_id, 'duplicate_loser' AS outcome FROM workspace.ecom_poc.duplicate_losers_orders
)
SELECT COUNT(*) AS violations
FROM (SELECT _row_id FROM outcomes GROUP BY _row_id HAVING COUNT(*) > 1);

-- [orders] overlap_accepted_quarantine: No row in both accepted and quarantine
SELECT COUNT(*) AS violations
FROM workspace.ecom_poc.silver_orders a JOIN workspace.ecom_poc.quarantine_orders q ON a._row_id = q._row_id;

-- [orders] overlap_accepted_losers: No row in both accepted and duplicate losers
SELECT COUNT(*) AS violations
FROM workspace.ecom_poc.silver_orders a JOIN workspace.ecom_poc.duplicate_losers_orders l ON a._row_id = l._row_id;

-- [orders] overlap_quarantine_losers: No row in both quarantine and duplicate losers
SELECT COUNT(*) AS violations
FROM workspace.ecom_poc.quarantine_orders q JOIN workspace.ecom_poc.duplicate_losers_orders l ON q._row_id = l._row_id;

-- [orders] outcome_null_row_id: No outcome row may have a NULL _row_id
WITH outcomes AS (
    SELECT _row_id, _batch_id, 'accepted'        AS outcome FROM workspace.ecom_poc.silver_orders
    UNION ALL
    SELECT _row_id, _batch_id, 'quarantine'      AS outcome FROM workspace.ecom_poc.quarantine_orders
    UNION ALL
    SELECT _row_id, _batch_id, 'duplicate_loser' AS outcome FROM workspace.ecom_poc.duplicate_losers_orders
)
SELECT COUNT(*) AS violations FROM outcomes WHERE _row_id IS NULL;

-- [orders] outcome_batch_mismatch: Every outcome row must come from the current Bronze snapshot (_batch_id)
WITH outcomes AS (
    SELECT _row_id, _batch_id, 'accepted'        AS outcome FROM workspace.ecom_poc.silver_orders
    UNION ALL
    SELECT _row_id, _batch_id, 'quarantine'      AS outcome FROM workspace.ecom_poc.quarantine_orders
    UNION ALL
    SELECT _row_id, _batch_id, 'duplicate_loser' AS outcome FROM workspace.ecom_poc.duplicate_losers_orders
)
SELECT COUNT(*) AS violations
FROM outcomes
WHERE _batch_id NOT IN (SELECT DISTINCT _batch_id FROM workspace.ecom_poc.bronze_orders);

-- [orders] accepted_key_not_unique: Accepted table must have one row per order_id
SELECT COUNT(*) - COUNT(DISTINCT order_id) AS violations FROM workspace.ecom_poc.silver_orders;

-- [orders] loser_without_valid_winner: Every duplicate loser must point (winner_row_id) to an accepted row with the same order_id
SELECT COUNT(*) AS violations
FROM workspace.ecom_poc.duplicate_losers_orders l
LEFT JOIN workspace.ecom_poc.silver_orders s ON l.winner_row_id = s._row_id
WHERE s._row_id IS NULL OR l.order_id <> s.order_id;

-- [orders] quarantine_without_reason: Every quarantined row must have a non-empty rejection_reason
SELECT COUNT(*) AS violations
FROM workspace.ecom_poc.quarantine_orders
WHERE rejection_reason IS NULL OR trim(rejection_reason) = '';

-- [orders] quarantine_reason_count_mismatch: rejection_count must equal the number of rejection_reasons
SELECT COUNT(*) AS violations
FROM workspace.ecom_poc.quarantine_orders
WHERE rejection_count <> size(rejection_reasons) OR rejection_count < 1;

-- [transactions] count_invariant: Bronze count must equal accepted + quarantine + duplicate losers
SELECT ABS(
    (SELECT COUNT(*) FROM workspace.ecom_poc.bronze_transactions)
  - ((SELECT COUNT(*) FROM workspace.ecom_poc.silver_transactions) + (SELECT COUNT(*) FROM workspace.ecom_poc.quarantine_transactions) + (SELECT COUNT(*) FROM workspace.ecom_poc.duplicate_losers_transactions))
) AS violations;

-- [transactions] bronze_row_id_not_unique: Every Bronze row must have a unique _row_id (needed for row-level proof)
SELECT COUNT(*) - COUNT(DISTINCT _row_id) AS violations FROM workspace.ecom_poc.bronze_transactions;

-- [transactions] bronze_row_missing_from_outcomes: Every Bronze row must appear in some outcome table
WITH outcomes AS (
    SELECT _row_id, _batch_id, 'accepted'        AS outcome FROM workspace.ecom_poc.silver_transactions
    UNION ALL
    SELECT _row_id, _batch_id, 'quarantine'      AS outcome FROM workspace.ecom_poc.quarantine_transactions
    UNION ALL
    SELECT _row_id, _batch_id, 'duplicate_loser' AS outcome FROM workspace.ecom_poc.duplicate_losers_transactions
)
SELECT COUNT(*) AS violations
FROM workspace.ecom_poc.bronze_transactions b
LEFT ANTI JOIN outcomes o ON b._row_id = o._row_id;

-- [transactions] outcome_row_not_in_bronze: Every outcome row must come from a Bronze row
WITH outcomes AS (
    SELECT _row_id, _batch_id, 'accepted'        AS outcome FROM workspace.ecom_poc.silver_transactions
    UNION ALL
    SELECT _row_id, _batch_id, 'quarantine'      AS outcome FROM workspace.ecom_poc.quarantine_transactions
    UNION ALL
    SELECT _row_id, _batch_id, 'duplicate_loser' AS outcome FROM workspace.ecom_poc.duplicate_losers_transactions
)
SELECT COUNT(*) AS violations
FROM outcomes o
LEFT ANTI JOIN workspace.ecom_poc.bronze_transactions b ON b._row_id = o._row_id;

-- [transactions] row_in_multiple_outcomes: No Bronze row may appear more than once across all outcome tables
WITH outcomes AS (
    SELECT _row_id, _batch_id, 'accepted'        AS outcome FROM workspace.ecom_poc.silver_transactions
    UNION ALL
    SELECT _row_id, _batch_id, 'quarantine'      AS outcome FROM workspace.ecom_poc.quarantine_transactions
    UNION ALL
    SELECT _row_id, _batch_id, 'duplicate_loser' AS outcome FROM workspace.ecom_poc.duplicate_losers_transactions
)
SELECT COUNT(*) AS violations
FROM (SELECT _row_id FROM outcomes GROUP BY _row_id HAVING COUNT(*) > 1);

-- [transactions] overlap_accepted_quarantine: No row in both accepted and quarantine
SELECT COUNT(*) AS violations
FROM workspace.ecom_poc.silver_transactions a JOIN workspace.ecom_poc.quarantine_transactions q ON a._row_id = q._row_id;

-- [transactions] overlap_accepted_losers: No row in both accepted and duplicate losers
SELECT COUNT(*) AS violations
FROM workspace.ecom_poc.silver_transactions a JOIN workspace.ecom_poc.duplicate_losers_transactions l ON a._row_id = l._row_id;

-- [transactions] overlap_quarantine_losers: No row in both quarantine and duplicate losers
SELECT COUNT(*) AS violations
FROM workspace.ecom_poc.quarantine_transactions q JOIN workspace.ecom_poc.duplicate_losers_transactions l ON q._row_id = l._row_id;

-- [transactions] outcome_null_row_id: No outcome row may have a NULL _row_id
WITH outcomes AS (
    SELECT _row_id, _batch_id, 'accepted'        AS outcome FROM workspace.ecom_poc.silver_transactions
    UNION ALL
    SELECT _row_id, _batch_id, 'quarantine'      AS outcome FROM workspace.ecom_poc.quarantine_transactions
    UNION ALL
    SELECT _row_id, _batch_id, 'duplicate_loser' AS outcome FROM workspace.ecom_poc.duplicate_losers_transactions
)
SELECT COUNT(*) AS violations FROM outcomes WHERE _row_id IS NULL;

-- [transactions] outcome_batch_mismatch: Every outcome row must come from the current Bronze snapshot (_batch_id)
WITH outcomes AS (
    SELECT _row_id, _batch_id, 'accepted'        AS outcome FROM workspace.ecom_poc.silver_transactions
    UNION ALL
    SELECT _row_id, _batch_id, 'quarantine'      AS outcome FROM workspace.ecom_poc.quarantine_transactions
    UNION ALL
    SELECT _row_id, _batch_id, 'duplicate_loser' AS outcome FROM workspace.ecom_poc.duplicate_losers_transactions
)
SELECT COUNT(*) AS violations
FROM outcomes
WHERE _batch_id NOT IN (SELECT DISTINCT _batch_id FROM workspace.ecom_poc.bronze_transactions);

-- [transactions] accepted_key_not_unique: Accepted table must have one row per txn_id
SELECT COUNT(*) - COUNT(DISTINCT txn_id) AS violations FROM workspace.ecom_poc.silver_transactions;

-- [transactions] loser_without_valid_winner: Every duplicate loser must point (winner_row_id) to an accepted row with the same txn_id
SELECT COUNT(*) AS violations
FROM workspace.ecom_poc.duplicate_losers_transactions l
LEFT JOIN workspace.ecom_poc.silver_transactions s ON l.winner_row_id = s._row_id
WHERE s._row_id IS NULL OR l.txn_id <> s.txn_id;

-- [transactions] quarantine_without_reason: Every quarantined row must have a non-empty rejection_reason
SELECT COUNT(*) AS violations
FROM workspace.ecom_poc.quarantine_transactions
WHERE rejection_reason IS NULL OR trim(rejection_reason) = '';

-- [transactions] quarantine_reason_count_mismatch: rejection_count must equal the number of rejection_reasons
SELECT COUNT(*) AS violations
FROM workspace.ecom_poc.quarantine_transactions
WHERE rejection_count <> size(rejection_reasons) OR rejection_count < 1;

-- [cross] accepted_txn_orphan_order: Every accepted transaction must reference an order that exists in accepted silver_orders
SELECT COUNT(*) AS violations
FROM workspace.ecom_poc.silver_transactions t
LEFT ANTI JOIN workspace.ecom_poc.silver_orders s ON t.order_id = s.order_id;

-- =====================================================================================================
-- C. SILVER RULE-CONFORMANCE ASSERTIONS
-- These test properties that hold for ANY choice of allowed lists/limits (not-null, normalization, ranges).
-- The exact allowed lists and limits you chose are enforced and documented in notebooks 03 and 04.
-- =====================================================================================================

-- [orders] accepted_mandatory_not_null: Accepted orders have no NULL mandatory field
SELECT COUNT(*) AS violations FROM workspace.ecom_poc.silver_orders
WHERE order_id IS NULL OR customer_id IS NULL OR order_date IS NULL OR quantity IS NULL
   OR unit_price IS NULL OR category IS NULL OR country IS NULL OR order_status IS NULL;

-- [orders] accepted_numeric_and_date_ranges: Accepted orders: quantity > 0, unit_price >= 0, order_date not in the future
SELECT COUNT(*) AS violations FROM workspace.ecom_poc.silver_orders
WHERE quantity <= 0 OR unit_price < 0 OR order_date > current_date();

-- [orders] accepted_labels_normalized: Accepted orders: ids trimmed; status/category lowercase; country uppercase; no surrounding whitespace
SELECT COUNT(*) AS violations FROM workspace.ecom_poc.silver_orders
WHERE order_id <> regexp_replace(order_id, '^\\s+|\\s+$', '')
   OR customer_id <> regexp_replace(customer_id, '^\\s+|\\s+$', '')
   OR order_status <> lower(regexp_replace(order_status, '^\\s+|\\s+$', ''))
   OR category <> lower(regexp_replace(category, '^\\s+|\\s+$', ''))
   OR country <> upper(regexp_replace(country, '^\\s+|\\s+$', ''));

-- [orders] quarantine_reason_codes_known: Quarantined orders only carry documented reason codes
SELECT COUNT(*) AS violations
FROM workspace.ecom_poc.quarantine_orders
LATERAL VIEW explode(rejection_reasons) r AS reason
WHERE reason NOT IN ('order_id_missing', 'customer_id_missing', 'order_date_missing', 'order_date_invalid', 'order_date_future', 'order_date_too_old', 'quantity_missing', 'quantity_not_numeric', 'quantity_not_integer', 'quantity_not_positive', 'quantity_above_max', 'unit_price_missing', 'unit_price_not_numeric', 'unit_price_negative', 'unit_price_above_max', 'order_status_missing', 'order_status_not_allowed', 'category_missing', 'category_not_allowed', 'country_missing', 'country_not_allowed');

-- [transactions] accepted_mandatory_not_null: Accepted transactions have no NULL mandatory field
SELECT COUNT(*) AS violations FROM workspace.ecom_poc.silver_transactions
WHERE txn_id IS NULL OR order_id IS NULL OR payment_method IS NULL OR amount IS NULL
   OR currency IS NULL OR txn_status IS NULL OR txn_ts IS NULL;

-- [transactions] accepted_numeric_and_time_ranges: Accepted transactions: amount > 0 and txn_ts not in the future
SELECT COUNT(*) AS violations FROM workspace.ecom_poc.silver_transactions
WHERE amount <= 0 OR txn_ts > current_timestamp();

-- [transactions] accepted_labels_normalized: Accepted transactions: ids trimmed; method/status lowercase; currency uppercase
SELECT COUNT(*) AS violations FROM workspace.ecom_poc.silver_transactions
WHERE txn_id <> regexp_replace(txn_id, '^\\s+|\\s+$', '')
   OR order_id <> regexp_replace(order_id, '^\\s+|\\s+$', '')
   OR payment_method <> lower(regexp_replace(payment_method, '^\\s+|\\s+$', ''))
   OR txn_status <> lower(regexp_replace(txn_status, '^\\s+|\\s+$', ''))
   OR currency <> upper(regexp_replace(currency, '^\\s+|\\s+$', ''));

-- [transactions] quarantine_reason_codes_known: Quarantined transactions only carry documented reason codes
SELECT COUNT(*) AS violations
FROM workspace.ecom_poc.quarantine_transactions
LATERAL VIEW explode(rejection_reasons) r AS reason
WHERE reason NOT IN ('txn_id_missing', 'order_id_missing', 'order_id_not_in_silver_orders', 'payment_method_missing', 'payment_method_not_allowed', 'amount_missing', 'amount_not_numeric', 'amount_below_min', 'amount_above_max', 'currency_missing', 'currency_not_allowed', 'txn_status_missing', 'txn_status_not_allowed', 'txn_ts_missing', 'txn_ts_invalid', 'txn_ts_future', 'txn_ts_too_old');

-- [transactions] orphan_reason_is_really_orphan: A transaction quarantined as an orphan must NOT have an accepted order
SELECT COUNT(*) AS violations
FROM workspace.ecom_poc.quarantine_transactions q
JOIN workspace.ecom_poc.silver_orders s ON regexp_replace(q.order_id, '^\\s+|\\s+$', '') = s.order_id
WHERE array_contains(q.rejection_reasons, 'order_id_not_in_silver_orders');

-- [transactions] orphans_not_accepted: No accepted transaction may reference a non-accepted order
SELECT COUNT(*) AS violations
FROM workspace.ecom_poc.silver_transactions t
LEFT ANTI JOIN workspace.ecom_poc.silver_orders s ON t.order_id = s.order_id;

-- =====================================================================================================
-- D. EVIDENCE QUERIES (examples for the README / evidence/04_silver/)
-- =====================================================================================================

-- D1. Why rows were quarantined (a row with several reasons counts once per reason)
SELECT 'orders' AS dataset, reason AS rejection_reason, COUNT(*) AS rows_with_this_reason
FROM workspace.ecom_poc.quarantine_orders
LATERAL VIEW explode(rejection_reasons) r AS reason
GROUP BY reason
UNION ALL
SELECT 'transactions', reason, COUNT(*)
FROM workspace.ecom_poc.quarantine_transactions
LATERAL VIEW explode(rejection_reasons) r AS reason
GROUP BY reason
ORDER BY dataset, rows_with_this_reason DESC;

-- D2. Example quarantined orders (raw values + reasons)
SELECT order_id, customer_id, order_date, quantity, unit_price, order_status, country, rejection_reason, _row_id
FROM workspace.ecom_poc.quarantine_orders
ORDER BY rejection_count DESC
LIMIT 20;

-- D3. Example quarantined transactions (raw values + reasons)
SELECT txn_id, order_id, payment_method, amount, currency, txn_status, txn_ts, rejection_reason, _row_id
FROM workspace.ecom_poc.quarantine_transactions
ORDER BY rejection_count DESC
LIMIT 20;

-- D4. Example orphan transactions (order not in accepted silver_orders)
SELECT txn_id, order_id, amount, txn_status, rejection_reason, _row_id
FROM workspace.ecom_poc.quarantine_transactions
WHERE array_contains(rejection_reasons, 'order_id_not_in_silver_orders')
LIMIT 20;

-- D5. Duplicate order losers next to their winners
SELECT l.order_id, l.duplicate_rank,
       l.order_date AS loser_order_date, w.order_date AS winner_order_date,
       l._row_id AS loser_row_id, w._row_id AS winner_row_id, l.winner_rule
FROM workspace.ecom_poc.duplicate_losers_orders l
JOIN workspace.ecom_poc.silver_orders w ON l.winner_row_id = w._row_id
ORDER BY l.order_id, l.duplicate_rank
LIMIT 20;

-- D6. Duplicate transaction losers next to their winners
SELECT l.txn_id, l.duplicate_rank,
       l.txn_ts AS loser_txn_ts, w.txn_ts AS winner_txn_ts,
       l._row_id AS loser_row_id, w._row_id AS winner_row_id, l.winner_rule
FROM workspace.ecom_poc.duplicate_losers_transactions l
JOIN workspace.ecom_poc.silver_transactions w ON l.winner_row_id = w._row_id
ORDER BY l.txn_id, l.duplicate_rank
LIMIT 20;

-- D7. Orphan cause: was the order quarantined, or never present?
WITH orphans AS (
    SELECT NULLIF(regexp_replace(order_id, '^\\s+|\\s+$', ''), '') AS oid
    FROM workspace.ecom_poc.quarantine_transactions
    WHERE array_contains(rejection_reasons, 'order_id_not_in_silver_orders')
),
q_orders AS (
    SELECT DISTINCT NULLIF(regexp_replace(order_id, '^\\s+|\\s+$', ''), '') AS oid FROM workspace.ecom_poc.quarantine_orders
)
SELECT CASE WHEN q.oid IS NOT NULL THEN 'order exists but was quarantined in Silver'
            ELSE 'order_id not found in any orders outcome' END AS orphan_cause,
       COUNT(*) AS transactions
FROM orphans o
LEFT JOIN q_orders q ON o.oid = q.oid
GROUP BY 1
ORDER BY transactions DESC;
