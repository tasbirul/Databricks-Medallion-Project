-- =====================================================================================================
-- sql/05_powerbi_reconciliation.sql
-- One exact "total revenue" number for a given set of report filters, to compare across the three tools:
--     (1) Databricks SQL    (2) Databricks AI/BI dashboard    (3) Power BI
--
-- HOW TO USE
--   1. Set the filters in the FILTERS block to EXACTLY what is selected in the dashboard and in Power BI
--      (date range, currency, and category/country if you filter on them).
--   2. Run the whole file in ONE session (Run all) so the variables and temp view stay available.
--   3. Read the result of section 4, then read the same number off the dashboard and off Power BI under the SAME filters.
--   4. Record the three actual values in powerbi/README.md or evidence/07_powerbi/ (template at the bottom).
--
-- Do not report the tools as matching until you have compared real values. Explain every mismatch (section 6).
--
-- !! Why the filters matter: the Gold marts have different grains !!
--     gold_daily_sales        grain = order_date              -> can filter by DATE and CURRENCY, not by category/country
--     gold_revenue_breakdown  grain = category + country      -> can filter by CATEGORY/COUNTRY and CURRENCY, not by date
--   A Power BI report built only on Gold cannot combine "date range" with "category" in one number.
--   Section 3 (recomputed from Silver) CAN, and is the engineering reference for such combined filters.
--
-- Requirements: SQL session variables (DECLARE VARIABLE). Otherwise replace each v_* with a literal.
-- =====================================================================================================

USE CATALOG workspace;
USE SCHEMA ecom_poc;

-- ------------------------------------ FILTERS (edit these) -------------------------------------------
-- NULL = no filter on that field.
DECLARE OR REPLACE VARIABLE v_start_date DATE   DEFAULT NULL;        -- inclusive, e.g. DATE'2025-01-01'
DECLARE OR REPLACE VARIABLE v_end_date   DATE   DEFAULT NULL;        -- inclusive, e.g. DATE'2025-12-31'
DECLARE OR REPLACE VARIABLE v_currency   STRING DEFAULT 'USD';       -- the reporting currency used by Gold
DECLARE OR REPLACE VARIABLE v_category   STRING DEFAULT NULL;        -- normalized form = lowercase, e.g. 'electronics'
DECLARE OR REPLACE VARIABLE v_country    STRING DEFAULT NULL;        -- normalized form = UPPERCASE, e.g. 'US'

-- To change a filter after declaring, run for example:
--     SET VAR v_start_date = DATE'2025-01-01';
--     SET VAR v_category   = 'electronics';

-- ------------------------------------ REVENUE POLICY (must match notebooks/06_gold_marts.py) ----------
DECLARE OR REPLACE VARIABLE v_eligible_order_statuses ARRAY<STRING> DEFAULT ARRAY('completed', 'shipped');
DECLARE OR REPLACE VARIABLE v_success_txn_statuses    ARRAY<STRING> DEFAULT ARRAY('success');

-- Static conversion rates to the reporting currency (documented assumptions; reporting currency = rate 1)
CREATE OR REPLACE TEMP VIEW fx_rates AS
SELECT currency, CAST(rate AS DECIMAL(18,6)) AS rate
FROM VALUES
    ('USD', 1.0)
AS t(currency, rate);
-- -----------------------------------------------------------------------------------------------------

-- Filters in force (screenshot with the results so the numbers can be audited)
SELECT v_start_date AS start_date, v_end_date AS end_date, v_currency AS currency,
       v_category AS category, v_country AS country;

-- =====================================================================================================
-- 1. GOLD: gold_daily_sales  (what a Power BI "Total Revenue" card on the daily table shows)
--    Applies: date range + currency.   Ignores: category/country (this table has no such columns).
-- =====================================================================================================
SELECT 'gold_daily_sales' AS source_table,
       v_currency AS currency,
       COUNT(*) AS date_rows,
       SUM(order_count) AS orders,
       SUM(eligible_revenue) AS total_eligible_revenue
FROM workspace.ecom_poc.gold_daily_sales
WHERE reporting_currency = v_currency
  AND (v_start_date IS NULL OR order_date >= v_start_date)
  AND (v_end_date   IS NULL OR order_date <= v_end_date);

-- =====================================================================================================
-- 2. GOLD: gold_revenue_breakdown  (what a Power BI revenue-by-category/country visual sums to)
--    Applies: category + country + currency.   Ignores: dates (this table is not split by date).
-- =====================================================================================================
SELECT 'gold_revenue_breakdown' AS source_table,
       v_currency AS currency,
       COUNT(*) AS category_country_rows,
       SUM(order_count) AS orders,
       SUM(eligible_revenue) AS total_eligible_revenue
FROM workspace.ecom_poc.gold_revenue_breakdown
WHERE reporting_currency = v_currency
  AND (v_category IS NULL OR category = v_category)
  AND (v_country  IS NULL OR country  = v_country);

-- =====================================================================================================
-- 3. SILVER REFERENCE: the same revenue recomputed from accepted Silver with ALL filters at once
--    Independent of the Gold build, so it also validates Gold. (Engineers only - reporting users cannot read Silver.)
-- =====================================================================================================
WITH pay AS (
    SELECT t.order_id, SUM(ROUND(t.amount * fx.rate, 2)) AS amt
    FROM workspace.ecom_poc.silver_transactions t
    JOIN fx_rates fx ON t.currency = fx.currency
    WHERE array_contains(v_success_txn_statuses, t.txn_status)
    GROUP BY t.order_id
),
ord_rev AS (
    SELECT o.order_id, o.order_date, o.category, o.country, p.amt AS rev
    FROM workspace.ecom_poc.silver_orders o
    JOIN pay p ON o.order_id = p.order_id
    WHERE array_contains(v_eligible_order_statuses, o.order_status)
)
SELECT 'silver_recomputed' AS source_table,
       v_currency AS currency,
       COUNT(*) AS paid_eligible_orders,
       SUM(rev) AS total_eligible_revenue
FROM ord_rev
WHERE (v_start_date IS NULL OR order_date >= v_start_date)
  AND (v_end_date   IS NULL OR order_date <= v_end_date)
  AND (v_category   IS NULL OR category = v_category)
  AND (v_country    IS NULL OR country  = v_country);

-- =====================================================================================================
-- 4. THE COMPARISON TABLE (read this one)
--    A comparison is only valid when the filters are expressible in the table being compared. The last column says so.
-- =====================================================================================================
WITH pay AS (
    SELECT t.order_id, SUM(ROUND(t.amount * fx.rate, 2)) AS amt
    FROM workspace.ecom_poc.silver_transactions t
    JOIN fx_rates fx ON t.currency = fx.currency
    WHERE array_contains(v_success_txn_statuses, t.txn_status)
    GROUP BY t.order_id
),
ord_rev AS (
    SELECT o.order_id, o.order_date, o.category, o.country, p.amt AS rev
    FROM workspace.ecom_poc.silver_orders o
    JOIN pay p ON o.order_id = p.order_id
    WHERE array_contains(v_eligible_order_statuses, o.order_status)
),
silver_ref AS (
    SELECT SUM(rev) AS v
    FROM ord_rev
    WHERE (v_start_date IS NULL OR order_date >= v_start_date)
      AND (v_end_date   IS NULL OR order_date <= v_end_date)
      AND (v_category   IS NULL OR category = v_category)
      AND (v_country    IS NULL OR country  = v_country)
),
silver_date_only AS (   -- Silver with ONLY the filters gold_daily_sales understands
    SELECT SUM(rev) AS v
    FROM ord_rev
    WHERE (v_start_date IS NULL OR order_date >= v_start_date)
      AND (v_end_date   IS NULL OR order_date <= v_end_date)
),
silver_dim_only AS (    -- Silver with ONLY the filters gold_revenue_breakdown understands
    SELECT SUM(rev) AS v
    FROM ord_rev
    WHERE (v_category IS NULL OR category = v_category)
      AND (v_country  IS NULL OR country  = v_country)
),
gold_daily AS (
    SELECT SUM(eligible_revenue) AS v
    FROM workspace.ecom_poc.gold_daily_sales
    WHERE reporting_currency = v_currency
      AND (v_start_date IS NULL OR order_date >= v_start_date)
      AND (v_end_date   IS NULL OR order_date <= v_end_date)
),
gold_breakdown AS (
    SELECT SUM(eligible_revenue) AS v
    FROM workspace.ecom_poc.gold_revenue_breakdown
    WHERE reporting_currency = v_currency
      AND (v_category IS NULL OR category = v_category)
      AND (v_country  IS NULL OR country  = v_country)
)
SELECT 'Gold daily (date + currency filters)' AS measure,
       (SELECT v FROM gold_daily)       AS databricks_sql_value,
       (SELECT v FROM silver_date_only) AS silver_recomputed_same_filters,
       (SELECT v FROM gold_daily) - (SELECT v FROM silver_date_only) AS difference,
       CASE WHEN COALESCE((SELECT v FROM gold_daily), 0) = COALESCE((SELECT v FROM silver_date_only), 0) THEN 'MATCH' ELSE 'MISMATCH' END AS result,
       'Compare to the Power BI / dashboard card that uses ONLY the date filter' AS use_for
UNION ALL
SELECT 'Gold breakdown (category/country + currency filters)',
       (SELECT v FROM gold_breakdown),
       (SELECT v FROM silver_dim_only),
       (SELECT v FROM gold_breakdown) - (SELECT v FROM silver_dim_only),
       CASE WHEN COALESCE((SELECT v FROM gold_breakdown), 0) = COALESCE((SELECT v FROM silver_dim_only), 0) THEN 'MATCH' ELSE 'MISMATCH' END,
       'Compare to the Power BI / dashboard visual that uses ONLY category/country filters'
UNION ALL
SELECT 'Silver recomputed (ALL filters combined)',
       CAST(NULL AS DECIMAL(38,2)),
       (SELECT v FROM silver_ref),
       CAST(NULL AS DECIMAL(38,2)),
       CASE WHEN v_category IS NULL AND v_country IS NULL THEN 'equals gold_daily row (no dimension filter)'
            WHEN v_start_date IS NULL AND v_end_date IS NULL THEN 'equals gold_breakdown row (no date filter)'
            ELSE 'combined date + dimension filter: only Silver can answer this' END,
       'Engineering reference for filter combinations Gold alone cannot express';

-- =====================================================================================================
-- 5. PER-DATE DETAIL - export or screenshot this and compare with a Power BI table visual (date, revenue)
-- =====================================================================================================
WITH pay AS (
    SELECT t.order_id, SUM(ROUND(t.amount * fx.rate, 2)) AS amt
    FROM workspace.ecom_poc.silver_transactions t
    JOIN fx_rates fx ON t.currency = fx.currency
    WHERE array_contains(v_success_txn_statuses, t.txn_status)
    GROUP BY t.order_id
),
ord_rev AS (
    SELECT o.order_id, o.order_date, p.amt AS rev
    FROM workspace.ecom_poc.silver_orders o
    JOIN pay p ON o.order_id = p.order_id
    WHERE array_contains(v_eligible_order_statuses, o.order_status)
),
silver_by_date AS (
    SELECT order_date, SUM(rev) AS rev FROM ord_rev GROUP BY order_date
)
SELECT COALESCE(g.order_date, s.order_date) AS order_date,
       g.eligible_revenue                   AS gold_revenue,
       s.rev                                AS silver_recomputed_revenue,
       COALESCE(g.eligible_revenue, 0) - COALESCE(s.rev, 0) AS difference
FROM (SELECT * FROM workspace.ecom_poc.gold_daily_sales WHERE reporting_currency = v_currency) g
FULL OUTER JOIN silver_by_date s ON g.order_date = s.order_date
WHERE (v_start_date IS NULL OR COALESCE(g.order_date, s.order_date) >= v_start_date)
  AND (v_end_date   IS NULL OR COALESCE(g.order_date, s.order_date) <= v_end_date)
ORDER BY order_date;

-- =====================================================================================================
-- 6. MISMATCH DIAGNOSTICS (use these to EXPLAIN a difference; never hide one)
-- =====================================================================================================

-- 6a. Which currencies and date range does Gold actually contain? (a currency filter that matches nothing = 0 revenue)
SELECT reporting_currency, MIN(order_date) AS first_date, MAX(order_date) AS last_date,
       COUNT(*) AS date_rows, SUM(eligible_revenue) AS total_eligible_revenue
FROM workspace.ecom_poc.gold_daily_sales
GROUP BY reporting_currency;

-- 6b. Dates where Gold and Silver disagree (EXPECTED: no rows)
WITH pay AS (
    SELECT t.order_id, SUM(ROUND(t.amount * fx.rate, 2)) AS amt
    FROM workspace.ecom_poc.silver_transactions t
    JOIN fx_rates fx ON t.currency = fx.currency
    WHERE array_contains(v_success_txn_statuses, t.txn_status)
    GROUP BY t.order_id
),
silver_by_date AS (
    SELECT o.order_date, SUM(p.amt) AS rev
    FROM workspace.ecom_poc.silver_orders o
    JOIN pay p ON o.order_id = p.order_id
    WHERE array_contains(v_eligible_order_statuses, o.order_status)
    GROUP BY o.order_date
)
SELECT COALESCE(g.order_date, s.order_date) AS order_date, g.eligible_revenue AS gold_revenue, s.rev AS silver_revenue
FROM workspace.ecom_poc.gold_daily_sales g
FULL OUTER JOIN silver_by_date s ON g.order_date = s.order_date
WHERE COALESCE(g.eligible_revenue, 0) <> COALESCE(s.rev, 0)
ORDER BY order_date
LIMIT 50;

-- 6c. Typical causes of a Databricks-vs-Power-BI difference (check them in this order):
--     * Different filters       - Power BI slicer date range / currency not identical to the variables above.
--     * Different grain         - summing gold_revenue_breakdown while filtering by date (it has no dates).
--     * Different measure       - a DAX measure that is not a plain SUM of eligible_revenue, or an implicit average.
--     * Rounding / display      - a card showing thousands/millions ("1.2M") instead of the exact value; compare the full number.
--     * Stale data              - Import-mode dataset not refreshed after Gold was rebuilt (check the last refresh time).
--     * Time zone / date type   - a date column converted to datetime with a time zone shift moves rows across days.
--     * Currency                - Power BI summing a column that mixes currencies (Gold is single reporting currency by design).
--     * Row limits              - a visual or an export truncated at a row limit, so it is not the full total.

-- =====================================================================================================
-- 7. RECORD THE RESULT (paste into powerbi/README.md or evidence/07_powerbi/) - fill with ACTUAL values only
-- =====================================================================================================
-- | Filters (date range / currency / category / country) | Databricks SQL (this file) | AI/BI dashboard | Power BI | Match? | Explanation of any difference |
-- |---|---|---|---|---|---|
-- | (write the exact filters used)                       | (value from section 4)     | (value read from the dashboard) | (value read from Power BI) | (yes/no) | (see 6c) |
