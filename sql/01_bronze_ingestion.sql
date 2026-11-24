-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 01 - Bronze ingestion
-- MAGIC
-- MAGIC Loads the raw CSV into a Delta table with `COPY INTO`. Every column stays a string, duplicates are kept on purpose, and rerunning skips files that were already loaded.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS workspace.bronze.deliveries (
  created_at STRING,
  external_id STRING,
  run_id STRING,
  fleet_name STRING,
  team_name STRING,
  driver_name STRING,
  last_assigned_time STRING,
  started_time STRING,
  checkin_time_wp1 STRING,
  checkout_time_wp1 STRING,
  scheduled_at STRING,
  checkin_time_wp2 STRING,
  checkout_time_wp2 STRING,
  no_earlier_than STRING,
  no_later_than STRING,
  first_attempt_promise_no_earlier_than STRING,
  first_attempt_promise_no_later_than STRING,
  route_title STRING,
  service_plan_name STRING,
  tag STRING,
  status STRING,
  reason_list STRING,
  other_text_list STRING,
  failed_delivery_attempts STRING,
  task_rating STRING,
  day_of_week STRING,
  delivery_duration_min STRING,
  _source_file STRING,
  _ingested_at TIMESTAMP
)
COMMENT 'Raw anonymized delivery records. Contains source duplicates by design.';

-- COMMAND ----------

COPY INTO workspace.bronze.deliveries
FROM (
  SELECT
    `Created At` AS created_at,
    `External Id` AS external_id,
    `Run Id` AS run_id,
    `Fleet Name` AS fleet_name,
    `Team Name` AS team_name,
    `Driver Name` AS driver_name,
    `Last Assigned Time` AS last_assigned_time,
    `Started Time` AS started_time,
    `Checkin Time (WP1)` AS checkin_time_wp1,
    `Checkout Time (WP1)` AS checkout_time_wp1,
    `Scheduled At` AS scheduled_at,
    `Checkin Time (WP2)` AS checkin_time_wp2,
    `Checkout Time (WP2)` AS checkout_time_wp2,
    `No Earlier Than` AS no_earlier_than,
    `No Later Than` AS no_later_than,
    `First Attempt Promise No Earlier Than` AS first_attempt_promise_no_earlier_than,
    `First Attempt Promise No Later Than` AS first_attempt_promise_no_later_than,
    `Route Title` AS route_title,
    `Service Plan Name` AS service_plan_name,
    Tag AS tag,
    Status AS status,
    `Reason List` AS reason_list,
    `Other Text List` AS other_text_list,
    `Failed Delivery Attempts` AS failed_delivery_attempts,
    `Task Rating` AS task_rating,
    `Day of Week` AS day_of_week,
    `Delivery Duration (min)` AS delivery_duration_min,
    _metadata.file_path AS _source_file,
    current_timestamp() AS _ingested_at
  FROM '/Volumes/workspace/bronze/landing/'
)
FILEFORMAT = CSV
FILES = ('deliveries_anonymized.csv')
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'false');

-- COMMAND ----------

WITH distinct_records AS (
  SELECT DISTINCT * EXCEPT (created_at, _source_file, _ingested_at)
  FROM workspace.bronze.deliveries
),
totals AS (
  SELECT
    COUNT(*) AS bronze_rows,
    COUNT(DISTINCT _source_file) AS source_files,
    COUNT(DISTINCT external_id) AS distinct_order_ids,
    SUM(CASE WHEN external_id IS NULL THEN 1 ELSE 0 END) AS missing_order_ids
  FROM workspace.bronze.deliveries
)
SELECT
  t.bronze_rows,
  t.source_files,
  t.distinct_order_ids,
  t.missing_order_ids,
  d.distinct_deliveries,
  t.bronze_rows - d.distinct_deliveries AS duplicate_rows_for_silver
FROM totals t
CROSS JOIN (SELECT COUNT(*) AS distinct_deliveries FROM distinct_records) d;
