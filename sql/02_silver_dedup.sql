-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 02 - Silver deduplication
-- MAGIC
-- MAGIC The source export duplicated each delivery up to 21 times with a randomized `created_at`. Grouping on every other column keeps one row per delivery and records how many copies were collapsed.

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.silver.deliveries_deduped
COMMENT 'One row per distinct delivery record, with the number of source copies.'
AS
SELECT
  * EXCEPT (created_at, _source_file, _ingested_at),
  COUNT(*) AS source_copies,
  MAX(_ingested_at) AS _bronze_ingested_at
FROM workspace.bronze.deliveries
GROUP BY ALL;

-- COMMAND ----------

SELECT
  COUNT(*) AS silver_rows,
  SUM(source_copies) AS bronze_rows_accounted_for,
  SUM(source_copies) - COUNT(*) AS duplicates_removed,
  MIN(source_copies) AS min_copies,
  MAX(source_copies) AS max_copies
FROM workspace.silver.deliveries_deduped;
