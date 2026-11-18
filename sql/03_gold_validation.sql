-- =====================================================================================================
-- sql/03_gold_validation.sql
-- Validates the four Gold marts against accepted Silver, using the revenue policy from notebooks/06_gold_marts.py.
--
-- !! STEP 1 - the CONFIG block below MUST match the widget values you used in notebooks/06_gold_marts.py !!
--    If they differ, the "independent recomputation" assertions will (correctly) fail.
--
-- Requirements: a SQL warehouse / runtime that supports SQL session variables (DECLARE VARIABLE).
--   If yours does not, replace each v_* variable with the literal value.
-- Run the whole file in ONE session (Run all) so the variables and the temp view stay available.
--
-- Every ASSERTION returns `violations`. The expected value is 0.
-- =====================================================================================================

USE CATALOG workspace;
USE SCHEMA ecom_poc;

-- ----------------------------- CONFIG (edit to match notebook 06) ------------------------------------
DECLARE OR REPLACE VARIABLE v_eligible_order_statuses ARRAY<STRING> DEFAULT ARRAY('completed', 'shipped');
DECLARE OR REPLACE VARIABLE v_success_txn_statuses    ARRAY<STRING> DEFAULT ARRAY('success');
DECLARE OR REPLACE VARIABLE v_reporting_currency      STRING        DEFAULT 'USD';
DECLARE OR REPLACE VARIABLE v_top_n                   INT           DEFAULT 100;

-- Static conversion rates to the reporting currency (1 unit of `currency` = `rate` units of v_reporting_currency).
-- The reporting currency itself must be listed with rate 1. Add one row per extra currency, e.g. ('EUR', 1.08).
-- These are documented assumptions, not market data.
CREATE OR REPLACE TEMP VIEW fx_rates AS
SELECT currency, CAST(rate AS DECIMAL(18,6)) AS rate
FROM VALUES
    ('USD', 1.0)
AS t(currency, rate);
-- -----------------------------------------------------------------------------------------------------

-- Sanity: the config actually describes your Silver data (each query should return rows)
SELECT order_status,
       array_contains(v_eligible_order_statuses, order_status) AS counted_as_eligible,
       COUNT(*) AS orders
FROM workspace.ecom_poc.silver_orders GROUP BY 1, 2 ORDER BY orders DESC;

SELECT txn_status,
       array_contains(v_success_txn_statuses, txn_status) AS counted_as_success,
       COUNT(*) AS transactions
FROM workspace.ecom_poc.silver_transactions GROUP BY 1, 2 ORDER BY transactions DESC;

-- =====================================================================================================
-- ASSERTIONS (expected: violations = 0)
-- =====================================================================================================

-- [gold_daily_sales] key_unique: One row per order_date
SELECT COUNT(*) AS violations FROM (SELECT order_date FROM workspace.ecom_poc.gold_daily_sales GROUP BY order_date HAVING COUNT(*) > 1);

-- [gold_daily_sales] key_not_null: order_date is never NULL
SELECT COUNT(*) AS violations FROM workspace.ecom_poc.gold_daily_sales WHERE order_date IS NULL;

-- [gold_revenue_breakdown] key_unique: One row per category + country
SELECT COUNT(*) AS violations FROM (SELECT category, country FROM workspace.ecom_poc.gold_revenue_breakdown GROUP BY category, country HAVING COUNT(*) > 1);

-- [gold_revenue_breakdown] key_not_null: category and country are never NULL
SELECT COUNT(*) AS violations FROM workspace.ecom_poc.gold_revenue_breakdown WHERE category IS NULL OR country IS NULL;

-- [gold_payment_success] key_unique: One row per payment_method
SELECT COUNT(*) AS violations FROM (SELECT payment_method FROM workspace.ecom_poc.gold_payment_success GROUP BY payment_method HAVING COUNT(*) > 1);

-- [gold_payment_success] key_not_null: payment_method is never NULL
SELECT COUNT(*) AS violations FROM workspace.ecom_poc.gold_payment_success WHERE payment_method IS NULL;

-- [gold_top_customers] key_unique: One row per customer_id
SELECT COUNT(*) AS violations FROM (SELECT customer_id FROM workspace.ecom_poc.gold_top_customers GROUP BY customer_id HAVING COUNT(*) > 1);

-- [gold_top_customers] rank_position_unique: rank_position is unique
SELECT COUNT(*) AS violations FROM (SELECT rank_position FROM workspace.ecom_poc.gold_top_customers GROUP BY rank_position HAVING COUNT(*) > 1);

-- [gold_daily_sales] order_count_equals_silver: Sum of order_count equals accepted silver_orders
SELECT CASE WHEN COALESCE((SELECT SUM(order_count) FROM workspace.ecom_poc.gold_daily_sales), 0) = COALESCE((SELECT COUNT(*) FROM workspace.ecom_poc.silver_orders), 0) THEN 0 ELSE 1 END AS violations;

-- [gold_revenue_breakdown] order_count_equals_silver: Sum of order_count equals accepted silver_orders
SELECT CASE WHEN COALESCE((SELECT SUM(order_count) FROM workspace.ecom_poc.gold_revenue_breakdown), 0) = COALESCE((SELECT COUNT(*) FROM workspace.ecom_poc.silver_orders), 0) THEN 0 ELSE 1 END AS violations;

-- [gold_payment_success] txn_count_equals_silver: Sum of transaction_count equals accepted silver_transactions
SELECT CASE WHEN COALESCE((SELECT SUM(transaction_count) FROM workspace.ecom_poc.gold_payment_success), 0) = COALESCE((SELECT COUNT(*) FROM workspace.ecom_poc.silver_transactions), 0) THEN 0 ELSE 1 END AS violations;

-- [silver_transactions] currency_without_rate: Every Silver currency has a conversion rate
SELECT COUNT(*) AS violations FROM workspace.ecom_poc.silver_transactions t
LEFT ANTI JOIN fx_rates fx ON t.currency = fx.currency;

-- [gold_daily_sales] revenue_equals_independent: Total eligible_revenue equals independent recomputation from Silver
WITH pay AS (
    SELECT t.order_id, SUM(ROUND(t.amount * fx.rate, 2)) AS amt
    FROM workspace.ecom_poc.silver_transactions t
    JOIN fx_rates fx ON t.currency = fx.currency
    WHERE array_contains(v_success_txn_statuses, t.txn_status)
    GROUP BY t.order_id
),
ord_rev AS (
    SELECT o.order_id, o.customer_id, o.order_date, o.category, o.country, p.amt AS rev
    FROM workspace.ecom_poc.silver_orders o
    JOIN pay p ON o.order_id = p.order_id
    WHERE array_contains(v_eligible_order_statuses, o.order_status)
)
SELECT CASE WHEN COALESCE((SELECT SUM(eligible_revenue) FROM workspace.ecom_poc.gold_daily_sales), 0) = COALESCE((SELECT SUM(rev) FROM ord_rev), 0) THEN 0 ELSE 1 END AS violations;

-- [gold_revenue_breakdown] revenue_equals_independent: Total eligible_revenue equals independent recomputation from Silver
WITH pay AS (
    SELECT t.order_id, SUM(ROUND(t.amount * fx.rate, 2)) AS amt
    FROM workspace.ecom_poc.silver_transactions t
    JOIN fx_rates fx ON t.currency = fx.currency
    WHERE array_contains(v_success_txn_statuses, t.txn_status)
    GROUP BY t.order_id
),
ord_rev AS (
    SELECT o.order_id, o.customer_id, o.order_date, o.category, o.country, p.amt AS rev
    FROM workspace.ecom_poc.silver_orders o
    JOIN pay p ON o.order_id = p.order_id
    WHERE array_contains(v_eligible_order_statuses, o.order_status)
)
SELECT CASE WHEN COALESCE((SELECT SUM(eligible_revenue) FROM workspace.ecom_poc.gold_revenue_breakdown), 0) = COALESCE((SELECT SUM(rev) FROM ord_rev), 0) THEN 0 ELSE 1 END AS violations;

-- [gold_daily_sales] revenue_equals_breakdown: Daily total revenue equals breakdown total revenue
SELECT CASE WHEN COALESCE((SELECT SUM(eligible_revenue) FROM workspace.ecom_poc.gold_daily_sales), 0) = COALESCE((SELECT SUM(eligible_revenue) FROM workspace.ecom_poc.gold_revenue_breakdown), 0) THEN 0 ELSE 1 END AS violations;

-- [gold_daily_sales] daily_revenue_by_date_matches_independent: Every date's revenue equals the independent recomputation
WITH pay AS (
    SELECT t.order_id, SUM(ROUND(t.amount * fx.rate, 2)) AS amt
    FROM workspace.ecom_poc.silver_transactions t
    JOIN fx_rates fx ON t.currency = fx.currency
    WHERE array_contains(v_success_txn_statuses, t.txn_status)
    GROUP BY t.order_id
),
ord_rev AS (
    SELECT o.order_id, o.customer_id, o.order_date, o.category, o.country, p.amt AS rev
    FROM workspace.ecom_poc.silver_orders o
    JOIN pay p ON o.order_id = p.order_id
    WHERE array_contains(v_eligible_order_statuses, o.order_status)
)
SELECT COUNT(*) AS violations
FROM workspace.ecom_poc.gold_daily_sales d
FULL OUTER JOIN (SELECT order_date, SUM(rev) AS rev FROM ord_rev GROUP BY order_date) i ON d.order_date = i.order_date
WHERE COALESCE(d.eligible_revenue, 0) <> COALESCE(i.rev, 0);

-- [gold_daily_sales] rates_and_counts_in_range: 0 <= cancellation_rate <= 1; cancelled <= orders; paid <= eligible <= orders
SELECT COUNT(*) AS violations FROM workspace.ecom_poc.gold_daily_sales
WHERE cancellation_rate < 0 OR cancellation_rate > 1
   OR cancelled_order_count > order_count
   OR eligible_order_count > order_count
   OR paid_order_count > eligible_order_count;

-- [gold_daily_sales] cancellation_rate_formula: cancellation_rate = cancelled_order_count / order_count
SELECT COUNT(*) AS violations FROM workspace.ecom_poc.gold_daily_sales
WHERE ABS(cancellation_rate - CAST(cancelled_order_count AS DECIMAL(28,10)) / order_count) > 0.000001;

-- [gold_daily_sales] single_reporting_currency: Every monetary row is in the declared reporting currency
SELECT COUNT(*) AS violations FROM workspace.ecom_poc.gold_daily_sales WHERE reporting_currency <> v_reporting_currency;

-- [gold_revenue_breakdown] no_negative_revenue: eligible_revenue is never negative
SELECT COUNT(*) AS violations FROM workspace.ecom_poc.gold_revenue_breakdown WHERE eligible_revenue < 0;

-- [gold_revenue_breakdown] revenue_share_total: revenue_share sums to 1 (within rounding) when revenue exists
SELECT CASE WHEN COALESCE(SUM(eligible_revenue), 0) = 0 THEN 0
            WHEN ABS(SUM(revenue_share) - 1) < 0.001 THEN 0 ELSE 1 END AS violations
FROM workspace.ecom_poc.gold_revenue_breakdown;

-- [gold_payment_success] rates_and_counts_in_range: 0 <= success_rate <= 1; successful <= total; status buckets add up
SELECT COUNT(*) AS violations FROM workspace.ecom_poc.gold_payment_success
WHERE success_rate < 0 OR success_rate > 1
   OR successful_transaction_count > transaction_count
   OR successful_transaction_count + refunded_transaction_count + other_transaction_count <> transaction_count;

-- [gold_payment_success] success_rate_formula: success_rate = successful / total
SELECT COUNT(*) AS violations FROM workspace.ecom_poc.gold_payment_success
WHERE ABS(success_rate - CAST(successful_transaction_count AS DECIMAL(28,10)) / transaction_count) > 0.000001;

-- [gold_top_customers] spend_matches_independent: Each customer's eligible_spend equals the independent recomputation
WITH pay AS (
    SELECT t.order_id, SUM(ROUND(t.amount * fx.rate, 2)) AS amt
    FROM workspace.ecom_poc.silver_transactions t
    JOIN fx_rates fx ON t.currency = fx.currency
    WHERE array_contains(v_success_txn_statuses, t.txn_status)
    GROUP BY t.order_id
),
ord_rev AS (
    SELECT o.order_id, o.customer_id, o.order_date, o.category, o.country, p.amt AS rev
    FROM workspace.ecom_poc.silver_orders o
    JOIN pay p ON o.order_id = p.order_id
    WHERE array_contains(v_eligible_order_statuses, o.order_status)
)
SELECT COUNT(*) AS violations
FROM workspace.ecom_poc.gold_top_customers g
LEFT JOIN (SELECT customer_id, SUM(rev) AS rev FROM ord_rev GROUP BY customer_id) i ON g.customer_id = i.customer_id
WHERE i.rev IS NULL OR i.rev <> g.eligible_spend;

-- [gold_top_customers] no_better_customer_left_out: No customer outside the table spends more than the smallest included spend
WITH pay AS (
    SELECT t.order_id, SUM(ROUND(t.amount * fx.rate, 2)) AS amt
    FROM workspace.ecom_poc.silver_transactions t
    JOIN fx_rates fx ON t.currency = fx.currency
    WHERE array_contains(v_success_txn_statuses, t.txn_status)
    GROUP BY t.order_id
),
ord_rev AS (
    SELECT o.order_id, o.customer_id, o.order_date, o.category, o.country, p.amt AS rev
    FROM workspace.ecom_poc.silver_orders o
    JOIN pay p ON o.order_id = p.order_id
    WHERE array_contains(v_eligible_order_statuses, o.order_status)
)
SELECT COUNT(*) AS violations
FROM (SELECT customer_id, SUM(rev) AS rev FROM ord_rev GROUP BY customer_id) i
LEFT ANTI JOIN workspace.ecom_poc.gold_top_customers g ON i.customer_id = g.customer_id
WHERE i.rev > (SELECT MIN(eligible_spend) FROM workspace.ecom_poc.gold_top_customers);

-- [gold_top_customers] rank_rules: spend_rank equals RANK() by spend and stays within 1..top_n
SELECT COUNT(*) AS violations
FROM (SELECT spend_rank, RANK() OVER (ORDER BY eligible_spend DESC) AS expected_rank FROM workspace.ecom_poc.gold_top_customers)
WHERE spend_rank <> expected_rank OR spend_rank < 1 OR spend_rank > v_top_n;

-- =====================================================================================================
-- EVIDENCE QUERIES
-- =====================================================================================================

-- Row counts per mart
SELECT 'gold_daily_sales' AS table_name, COUNT(*) AS row_count FROM workspace.ecom_poc.gold_daily_sales
UNION ALL SELECT 'gold_revenue_breakdown', COUNT(*) FROM workspace.ecom_poc.gold_revenue_breakdown
UNION ALL SELECT 'gold_payment_success',   COUNT(*) FROM workspace.ecom_poc.gold_payment_success
UNION ALL SELECT 'gold_top_customers',     COUNT(*) FROM workspace.ecom_poc.gold_top_customers;

-- Headline metrics that Databricks SQL, the AI/BI dashboard and Power BI must all reproduce
SELECT reporting_currency,
       SUM(eligible_revenue)      AS total_eligible_revenue,
       SUM(order_count)           AS total_orders,
       SUM(cancelled_order_count) AS total_cancelled_orders,
       CAST(SUM(cancelled_order_count) / SUM(order_count) AS DECIMAL(9,6)) AS overall_cancellation_rate
FROM workspace.ecom_poc.gold_daily_sales
GROUP BY reporting_currency;

-- Grain proof: one row per documented key
SELECT 'gold_daily_sales' AS table_name, 'order_date' AS grain, COUNT(*) AS row_count, COUNT(DISTINCT order_date) AS distinct_keys FROM workspace.ecom_poc.gold_daily_sales
UNION ALL SELECT 'gold_revenue_breakdown', 'category, country', COUNT(*), COUNT(DISTINCT category, country) FROM workspace.ecom_poc.gold_revenue_breakdown
UNION ALL SELECT 'gold_payment_success', 'payment_method', COUNT(*), COUNT(DISTINCT payment_method) FROM workspace.ecom_poc.gold_payment_success
UNION ALL SELECT 'gold_top_customers', 'customer_id', COUNT(*), COUNT(DISTINCT customer_id) FROM workspace.ecom_poc.gold_top_customers;
