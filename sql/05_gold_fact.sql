-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 05 - Gold fact table
-- MAGIC
-- MAGIC One row per delivery with stage durations, promise metrics, batching (`run_size`), and the number of other orders active at assignment.

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.gold.fact_deliveries
COMMENT 'One row per delivery record, linked to date, driver, and fleet dimensions.'
AS
WITH base AS (
  SELECT
    *,
    sha2(concat_ws('|', order_id, COALESCE(run_id, 'NO_RUN')), 256) AS delivery_key
  FROM workspace.silver.deliveries
),
order_load AS (
  SELECT
    b.delivery_key,
    COUNT(o.delivery_key) AS active_orders_at_assignment
  FROM base b
  LEFT JOIN base o
    ON o.is_valid_for_analysis
   AND o.delivery_key <> b.delivery_key
   AND o.assigned_at <= b.assigned_at
   AND o.customer_checkin_at > b.assigned_at
  GROUP BY b.delivery_key
)
SELECT
  s.delivery_key,
  s.order_id,
  s.run_id,
  CAST(date_format(s.assigned_at, 'yyyyMMdd') AS INT) AS date_key,
  d.driver_key,
  f.fleet_key,
  s.order_type,
  hour(s.assigned_at) AS assigned_hour,
  CASE WHEN s.run_id IS NULL THEN 1
       ELSE COUNT(*) OVER (PARTITION BY s.run_id) END AS run_size,
  l.active_orders_at_assignment,

  s.assigned_at,
  s.started_at,
  s.store_checkout_at,
  s.scheduled_at,
  s.customer_checkin_at,
  s.customer_checkout_at,
  s.promise_earliest_at,
  s.promise_latest_at,

  timestampdiff(MINUTE, s.assigned_at, s.started_at) AS assign_to_start_min,
  timestampdiff(MINUTE, s.started_at, s.store_checkout_at) AS start_to_store_exit_min,
  timestampdiff(MINUTE, s.store_checkout_at, s.customer_checkin_at) AS store_exit_to_door_min,
  s.dispatch_to_door_min,
  s.trip_min,
  s.customer_dwell_min,
  timestampdiff(MINUTE, s.promise_earliest_at, s.promise_latest_at) AS promise_window_min,
  timestampdiff(MINUTE, s.scheduled_at, s.customer_checkin_at) AS eta_error_min,
  timestampdiff(MINUTE, s.promise_latest_at, s.customer_checkin_at) AS minutes_vs_promise,

  CASE
    WHEN s.customer_checkin_at < s.promise_earliest_at THEN 'Early'
    WHEN s.customer_checkin_at > s.promise_latest_at THEN 'Late'
    ELSE 'In Window'
  END AS delivery_outcome,
  s.is_late,
  s.task_rating,
  s.is_valid_for_analysis,
  s._bronze_ingested_at
FROM base s
JOIN order_load l ON s.delivery_key = l.delivery_key
JOIN workspace.gold.dim_driver d ON s.driver_name = d.driver_name
JOIN workspace.gold.dim_fleet f ON s.fleet_name = f.fleet_name;

-- COMMAND ----------

SELECT
  COUNT(*) AS fact_rows,
  COUNT(DISTINCT delivery_key) AS unique_keys,
  SUM(CASE WHEN run_size > 1 THEN 1 ELSE 0 END) AS batched_deliveries,
  SUM(CASE WHEN is_valid_for_analysis THEN 1 ELSE 0 END) AS valid_rows,
  SUM(CASE WHEN is_valid_for_analysis AND delivery_outcome = 'Early' THEN 1 ELSE 0 END) AS valid_early,
  SUM(CASE WHEN is_valid_for_analysis AND delivery_outcome = 'In Window' THEN 1 ELSE 0 END) AS valid_in_window,
  SUM(CASE WHEN is_valid_for_analysis AND delivery_outcome = 'Late' THEN 1 ELSE 0 END) AS valid_late,
  ROUND(AVG(CASE WHEN is_valid_for_analysis THEN dispatch_to_door_min END), 2) AS avg_door_min,
  ROUND(AVG(CASE WHEN is_valid_for_analysis THEN active_orders_at_assignment END), 2) AS avg_active_orders
FROM workspace.gold.fact_deliveries;
