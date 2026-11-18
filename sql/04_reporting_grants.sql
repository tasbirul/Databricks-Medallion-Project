-- =====================================================================================================
-- sql/04_reporting_grants.sql
-- Least-privilege reporting access: reporting users can read the four Gold tables and NOTHING else.
--
-- !! DO NOT RUN THE GRANTS AGAINST A GUESSED USER OR GROUP !!
--    Every GRANT / REVOKE below is COMMENTED OUT on purpose. To use this file:
--      1. Identify the real principal that needs reporting access (an account group is best; a user email works too).
--      2. Replace REPORTING_PRINCIPAL below with that exact name, in backticks.
--      3. Uncomment and run section 2. Then run sections 3 and 4 to prove the result.
--    If you cannot create or manage groups in your trial workspace, keep this file as the documented grant plan and
--    explain the restriction in operations/permissions.md (also describe how you would test it).
--
-- Principle: the reporting principal gets
--      USE CATALOG on the catalog, USE SCHEMA on the schema, SELECT on the four Gold tables.
--    It gets NOTHING on: the raw_files volume, Bronze, Silver, quarantine and duplicate-loser tables.
--    (Engineering roles that run the pipeline are separate and are not covered by this file.)
--
-- Things SQL cannot grant (do these in the UI and record them as evidence):
--    * Permission to use a SQL warehouse ("Can use") - set under SQL Warehouses > Permissions.
--    * Access to a dashboard - set under the dashboard's Share dialog.
-- =====================================================================================================

-- Edit this placeholder (used only by the read-only audit queries in section 3):
DECLARE OR REPLACE VARIABLE v_principal STRING DEFAULT 'REPLACE_WITH_REAL_GROUP_OR_USER';

-- =====================================================================================================
-- 1. BEFORE: who can already see what? (read-only; run first, screenshot for evidence/08_jobs_security_cleanup/)
-- =====================================================================================================
SHOW GRANTS ON SCHEMA workspace.ecom_poc;

SELECT grantee, table_name, privilege_type
FROM workspace.information_schema.table_privileges
WHERE table_schema = 'ecom_poc'
ORDER BY table_name, grantee, privilege_type;

-- =====================================================================================================
-- 2. THE GRANT PLAN (commented out - replace REPORTING_PRINCIPAL, then uncomment)
--    Example principals:  `ecom_reporting_readers`   or   `analyst@example.com`
-- =====================================================================================================

-- 2a. Navigation privileges (needed to reach the tables at all)
-- GRANT USE CATALOG ON CATALOG workspace TO `REPORTING_PRINCIPAL`;
-- GRANT USE SCHEMA  ON SCHEMA  workspace.ecom_poc TO `REPORTING_PRINCIPAL`;

-- 2b. Read access to the four Gold tables ONLY
-- GRANT SELECT ON TABLE workspace.ecom_poc.gold_daily_sales       TO `REPORTING_PRINCIPAL`;
-- GRANT SELECT ON TABLE workspace.ecom_poc.gold_revenue_breakdown TO `REPORTING_PRINCIPAL`;
-- GRANT SELECT ON TABLE workspace.ecom_poc.gold_payment_success   TO `REPORTING_PRINCIPAL`;
-- GRANT SELECT ON TABLE workspace.ecom_poc.gold_top_customers     TO `REPORTING_PRINCIPAL`;

-- 2c. Deliberately NOT granted (listed so the omission is visible and reviewable):
--     - volume   workspace.ecom_poc.raw_files          (raw CSVs)
--     - tables   bronze_orders, bronze_transactions
--     - tables   silver_orders, silver_transactions
--     - tables   quarantine_orders, quarantine_transactions
--     - tables   duplicate_losers_orders, duplicate_losers_transactions
--     - never grant ALL PRIVILEGES, MODIFY, CREATE TABLE, or schema-wide SELECT to a reporting role
--       (schema-wide SELECT would silently expose Silver, quarantine and any future tables).

-- =====================================================================================================
-- 3. AFTER: prove the result (read-only; run after the grants)
-- =====================================================================================================
-- Uncomment after replacing the name (in backticks) with the real principal:
-- SHOW GRANTS `REPLACE_WITH_REAL_GROUP_OR_USER` ON SCHEMA workspace.ecom_poc;
--
-- Until v_principal (declared at the top) is set to the real principal, the queries below correctly return 0 rows.

-- 3a. Everything the principal can do on tables in this schema. EXPECTED: exactly four rows, all SELECT, all gold_*.
SELECT grantee, table_name, privilege_type
FROM workspace.information_schema.table_privileges
WHERE table_schema = 'ecom_poc'
  AND grantee = v_principal
ORDER BY table_name;

-- 3b. ASSERTION: the principal holds no privilege on any non-Gold table. EXPECTED: violations = 0
SELECT COUNT(*) AS violations
FROM workspace.information_schema.table_privileges
WHERE table_schema = 'ecom_poc'
  AND grantee = v_principal
  AND table_name NOT IN ('gold_daily_sales', 'gold_revenue_breakdown', 'gold_payment_success', 'gold_top_customers');

-- 3c. ASSERTION: the principal holds nothing stronger than SELECT. EXPECTED: violations = 0
SELECT COUNT(*) AS violations
FROM workspace.information_schema.table_privileges
WHERE table_schema = 'ecom_poc'
  AND grantee = v_principal
  AND privilege_type <> 'SELECT';

-- 3d. ASSERTION: the principal has no privilege on the raw volume. EXPECTED: violations = 0
SELECT COUNT(*) AS violations
FROM workspace.information_schema.volume_privileges
WHERE volume_schema = 'ecom_poc'
  AND grantee = v_principal;

-- =====================================================================================================
-- 4. FUNCTIONAL TEST - run as the reporting principal (open a SQL editor session as that user, or have them run it).
--    These cannot be proven from an admin session, so record screenshots from the reporting user's session.
-- =====================================================================================================
-- 4a. SHOULD SUCCEED
--     SELECT COUNT(*) FROM workspace.ecom_poc.gold_daily_sales;
--     SELECT COUNT(*) FROM workspace.ecom_poc.gold_revenue_breakdown;
--     SELECT COUNT(*) FROM workspace.ecom_poc.gold_payment_success;
--     SELECT COUNT(*) FROM workspace.ecom_poc.gold_top_customers;
--
-- 4b. SHOULD FAIL with a permission error (this is the desired result - screenshot the error)
--     SELECT * FROM workspace.ecom_poc.bronze_orders LIMIT 1;
--     SELECT * FROM workspace.ecom_poc.silver_orders LIMIT 1;
--     SELECT * FROM workspace.ecom_poc.quarantine_orders LIMIT 1;
--     SELECT * FROM workspace.ecom_poc.duplicate_losers_orders LIMIT 1;
--     LIST '/Volumes/workspace/ecom_poc/raw_files';
--
-- 4c. SHOULD FAIL (read-only means read-only)
--     DELETE FROM workspace.ecom_poc.gold_daily_sales WHERE 1 = 1;

-- =====================================================================================================
-- 5. ROLLBACK / CLEANUP (commented out) - use when the test principal is no longer needed
-- =====================================================================================================
-- REVOKE SELECT ON TABLE workspace.ecom_poc.gold_daily_sales       FROM `REPORTING_PRINCIPAL`;
-- REVOKE SELECT ON TABLE workspace.ecom_poc.gold_revenue_breakdown FROM `REPORTING_PRINCIPAL`;
-- REVOKE SELECT ON TABLE workspace.ecom_poc.gold_payment_success   FROM `REPORTING_PRINCIPAL`;
-- REVOKE SELECT ON TABLE workspace.ecom_poc.gold_top_customers     FROM `REPORTING_PRINCIPAL`;
-- REVOKE USE SCHEMA  ON SCHEMA  workspace.ecom_poc FROM `REPORTING_PRINCIPAL`;
-- REVOKE USE CATALOG ON CATALOG workspace          FROM `REPORTING_PRINCIPAL`;
