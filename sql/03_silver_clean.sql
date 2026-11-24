-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 03 - Silver cleaning
-- MAGIC
-- MAGIC Casts timestamps, renames columns, calculates delivery durations, and flags sequence errors and duration outliers. `is_valid_for_analysis` is used by every Gold table.

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.silver.deliveries
COMMENT 'Cleaned and typed delivery records with calculated metrics and quality flags.'
AS
WITH typed AS (
  SELECT
    external_id AS order_id,
    run_id,
    team_name AS store_id,
    COALESCE(fleet_name, 'Unknown') AS fleet_name,
    COALESCE(driver_name, 'Unknown') AS driver_name,
    tag AS order_type,
    status,
    day_of_week,
    TRY_CAST(failed_delivery_attempts AS INT) AS failed_delivery_attempts,
    TRY_CAST(task_rating AS INT) AS task_rating,
    try_to_timestamp(last_assigned_time, 'dd-MM-yyyy HH:mm') AS assigned_at,
    try_to_timestamp(started_time, 'dd-MM-yyyy HH:mm') AS started_at,
    try_to_timestamp(checkin_time_wp1, 'dd-MM-yyyy HH:mm') AS store_checkin_at,
    try_to_timestamp(checkout_time_wp1, 'dd-MM-yyyy HH:mm') AS store_checkout_at,
    try_to_timestamp(scheduled_at, 'dd-MM-yyyy HH:mm') AS scheduled_at,
    try_to_timestamp(checkin_time_wp2, 'dd-MM-yyyy HH:mm') AS customer_checkin_at,
    try_to_timestamp(checkout_time_wp2, 'dd-MM-yyyy HH:mm') AS customer_checkout_at,
    try_to_timestamp(no_earlier_than, 'dd-MM-yyyy HH:mm') AS promise_earliest_at,
    try_to_timestamp(no_later_than, 'dd-MM-yyyy HH:mm') AS promise_latest_at,
    source_copies,
    _bronze_ingested_at
  FROM workspace.silver.deliveries_deduped
),
metrics AS (
  SELECT
    *,
    timestampdiff(MINUTE, assigned_at, customer_checkin_at) AS dispatch_to_door_min,
    timestampdiff(MINUTE, started_at, customer_checkin_at) AS trip_min,
    timestampdiff(MINUTE, customer_checkin_at, customer_checkout_at) AS customer_dwell_min,
    customer_checkin_at > promise_latest_at AS is_late,
    (
      started_at < assigned_at
      OR store_checkin_at < started_at
      OR store_checkout_at < store_checkin_at
      OR customer_checkin_at < store_checkout_at
      OR customer_checkout_at < customer_checkin_at
    ) AS has_sequence_error
  FROM typed
),
flagged AS (
  SELECT
    *,
    (dispatch_to_door_min > 90 OR customer_dwell_min > 30) AS has_duration_outlier
  FROM metrics
)
SELECT
  *,
  NOT (has_sequence_error OR has_duration_outlier) AS is_valid_for_analysis
FROM flagged;

-- COMMAND ----------

SELECT
  COUNT(*) AS total_rows,
  SUM(CASE WHEN assigned_at IS NULL OR started_at IS NULL OR store_checkin_at IS NULL
        OR store_checkout_at IS NULL OR scheduled_at IS NULL OR customer_checkin_at IS NULL
        OR customer_checkout_at IS NULL OR promise_earliest_at IS NULL
        OR promise_latest_at IS NULL THEN 1 ELSE 0 END) AS bad_timestamps,
  SUM(CASE WHEN has_sequence_error THEN 1 ELSE 0 END) AS sequence_errors,
  SUM(CASE WHEN has_duration_outlier THEN 1 ELSE 0 END) AS duration_outliers,
  SUM(CASE WHEN is_valid_for_analysis THEN 1 ELSE 0 END) AS valid_rows,
  SUM(CASE WHEN is_late THEN 1 ELSE 0 END) AS late_deliveries,
  ROUND(AVG(CASE WHEN is_valid_for_analysis THEN dispatch_to_door_min END), 2) AS avg_door_min
FROM workspace.silver.deliveries;
