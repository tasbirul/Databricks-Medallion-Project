-- =====================================================================================================
-- sql/00_setup.sql
-- Create (if needed) and verify the schema and volume used by the whole pipeline.
--
-- Object names (change `workspace` everywhere if your workspace uses a different catalog):
--   catalog : workspace
--   schema  : workspace.ecom_poc
--   volume  : workspace.ecom_poc.raw_files   ->  /Volumes/workspace/ecom_poc/raw_files
--
-- Notes
--   * The catalog is NEVER created here. It must already exist and you must be allowed to use it.
--   * If CREATE statements fail because of permissions, use an existing writable catalog and document the
--     exact restriction (do not silently change the intended design).
--   * Running this file does not read or change any data.
-- =====================================================================================================

-- 1. Who am I, and where am I working? (screenshot for evidence/01_workspace/)
SELECT current_user() AS run_by, current_catalog() AS current_catalog, current_schema() AS current_schema;

-- 2. The catalog must already exist
SHOW CATALOGS;

USE CATALOG workspace;

-- 3. Schema (safe to rerun)
CREATE SCHEMA IF NOT EXISTS ecom_poc
COMMENT 'E-commerce medallion assignment: raw volume, Bronze, Silver and Gold objects';

USE SCHEMA ecom_poc;

-- 4. Volume for the raw CSV files (safe to rerun)
CREATE VOLUME IF NOT EXISTS raw_files
COMMENT 'Raw source CSVs: orders.csv and transactions.csv (not committed to Git)';

-- 5. Verify what exists (screenshot for evidence/01_workspace/)
SHOW SCHEMAS IN workspace;

SHOW VOLUMES IN ecom_poc;

-- information_schema view of the same volume (stable column names)
SELECT volume_catalog, volume_schema, volume_name, volume_type, volume_owner, comment
FROM workspace.information_schema.volumes
WHERE volume_schema = 'ecom_poc';

-- 6. Both source files must be visible at these exact paths
--    Expected: orders.csv and transactions.csv, each with a non-zero size.
LIST '/Volumes/workspace/ecom_poc/raw_files';

-- 7. Optional: confirm you can create and drop a table in the schema (Bronze/Silver/Gold need this)
CREATE OR REPLACE TABLE workspace.ecom_poc._setup_permission_test AS SELECT 1 AS id;
DROP TABLE IF EXISTS workspace.ecom_poc._setup_permission_test;

-- 8. Optional: privileges currently visible on the schema (useful for operations/permissions.md)
SHOW GRANTS ON SCHEMA workspace.ecom_poc;
