-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 06 - Gold KPI tables
-- MAGIC
-- MAGIC Ten KPI tables built from the fact table, each filtered to valid deliveries.

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.gold.kpi_daily
COMMENT 'Daily delivery volume, speed, promise performance, and batching for valid deliveries.'
AS
SELECT
  dt.date_key,
  dt.full_date,
  dt.day_name,
  dt.is_weekend,
  COUNT(*) AS deliveries,
  COUNT(DISTINCT fd.driver_key) AS active_drivers,
  COUNT(DISTINCT fd.run_id) AS trips,
  ROUND(COUNT(*) / COUNT(DISTINCT fd.driver_key), 2) AS deliveries_per_driver,
  ROUND(AVG(fd.dispatch_to_door_min), 2) AS avg_door_min,
  ROUND(percentile(fd.dispatch_to_door_min, 0.5), 2) AS median_door_min,
  ROUND(percentile(fd.dispatch_to_door_min, 0.9), 2) AS p90_door_min,
  SUM(CASE WHEN fd.delivery_outcome = 'Early' THEN 1 ELSE 0 END) AS early_deliveries,
  SUM(CASE WHEN fd.delivery_outcome = 'In Window' THEN 1 ELSE 0 END) AS in_window_deliveries,
  SUM(CASE WHEN fd.delivery_outcome = 'Late' THEN 1 ELSE 0 END) AS late_deliveries,
  ROUND(100 * AVG(CASE WHEN fd.is_late THEN 0 ELSE 1 END), 2) AS on_time_pct,
  ROUND(100 * AVG(CASE WHEN fd.run_size > 1 THEN 1 ELSE 0 END), 2) AS batched_pct
FROM workspace.gold.fact_deliveries fd
JOIN workspace.gold.dim_date dt ON fd.date_key = dt.date_key
WHERE fd.is_valid_for_analysis
GROUP BY dt.date_key, dt.full_date, dt.day_name, dt.is_weekend;

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.gold.kpi_hourly_demand
COMMENT 'Delivery demand, speed, and lateness by weekday and assigned hour for valid deliveries.'
AS
WITH days_observed AS (
  SELECT day_number, COUNT(*) AS days_observed
  FROM workspace.gold.dim_date
  GROUP BY day_number
)
SELECT
  dt.day_number,
  dt.day_name,
  fd.assigned_hour,
  CASE
    WHEN fd.assigned_hour BETWEEN 11 AND 13 THEN 'Lunch'
    WHEN fd.assigned_hour BETWEEN 14 AND 16 THEN 'Afternoon'
    WHEN fd.assigned_hour BETWEEN 17 AND 19 THEN 'Dinner'
    ELSE 'Late Evening'
  END AS day_part,
  o.days_observed,
  COUNT(*) AS deliveries,
  ROUND(COUNT(*) / o.days_observed, 2) AS avg_deliveries_per_day,
  COUNT(DISTINCT fd.driver_key) AS drivers_used,
  ROUND(AVG(fd.active_orders_at_assignment), 2) AS avg_active_orders,
  ROUND(AVG(fd.dispatch_to_door_min), 2) AS avg_door_min,
  ROUND(percentile(fd.dispatch_to_door_min, 0.9), 2) AS p90_door_min,
  SUM(CASE WHEN fd.is_late THEN 1 ELSE 0 END) AS late_deliveries,
  ROUND(100 * AVG(CASE WHEN fd.is_late THEN 0 ELSE 1 END), 2) AS on_time_pct
FROM workspace.gold.fact_deliveries fd
JOIN workspace.gold.dim_date dt ON fd.date_key = dt.date_key
JOIN days_observed o ON dt.day_number = o.day_number
WHERE fd.is_valid_for_analysis
GROUP BY dt.day_number, dt.day_name, fd.assigned_hour, o.days_observed;

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.gold.kpi_delivery_stages
COMMENT 'Average, median, and P90 time spent in each delivery stage by segment.'
AS
WITH valid AS (
  SELECT
    *,
    CASE
      WHEN assigned_hour BETWEEN 11 AND 13 THEN 'Lunch'
      WHEN assigned_hour BETWEEN 14 AND 16 THEN 'Afternoon'
      WHEN assigned_hour BETWEEN 17 AND 19 THEN 'Dinner'
      ELSE 'Late Evening'
    END AS day_part
  FROM workspace.gold.fact_deliveries
  WHERE is_valid_for_analysis
),
segments AS (
  SELECT 'Overall' AS segment_type, 'All Deliveries' AS segment,
         assign_to_start_min, start_to_store_exit_min, store_exit_to_door_min, dispatch_to_door_min
  FROM valid
  UNION ALL
  SELECT 'Day Part', day_part,
         assign_to_start_min, start_to_store_exit_min, store_exit_to_door_min, dispatch_to_door_min
  FROM valid
  UNION ALL
  SELECT 'Outcome', delivery_outcome,
         assign_to_start_min, start_to_store_exit_min, store_exit_to_door_min, dispatch_to_door_min
  FROM valid
),
stages AS (
  SELECT segment_type, segment, stage_order, stage_name, stage_min, dispatch_to_door_min
  FROM segments
  LATERAL VIEW stack(
    3,
    1, 'Assigned to Started', assign_to_start_min,
    2, 'Started to Store Exit', start_to_store_exit_min,
    3, 'Store Exit to Customer', store_exit_to_door_min
  ) s AS stage_order, stage_name, stage_min
)
SELECT
  segment_type,
  segment,
  stage_order,
  stage_name,
  COUNT(*) AS deliveries,
  ROUND(AVG(stage_min), 2) AS avg_stage_min,
  ROUND(percentile(stage_min, 0.5), 2) AS median_stage_min,
  ROUND(percentile(stage_min, 0.9), 2) AS p90_stage_min,
  ROUND(100 * SUM(stage_min) / SUM(dispatch_to_door_min), 2) AS share_of_door_time_pct
FROM stages
GROUP BY segment_type, segment, stage_order, stage_name;

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.gold.kpi_promise_performance
COMMENT 'Delivery outcomes and promise buffer by promise window length for valid deliveries.'
AS
SELECT
  promise_window_min,
  MIN(to_date(assigned_at)) AS first_date,
  MAX(to_date(assigned_at)) AS last_date,
  COUNT(DISTINCT date_key) AS days_active,
  COUNT(*) AS deliveries,
  SUM(CASE WHEN delivery_outcome = 'Early' THEN 1 ELSE 0 END) AS early_deliveries,
  SUM(CASE WHEN delivery_outcome = 'In Window' THEN 1 ELSE 0 END) AS in_window_deliveries,
  SUM(CASE WHEN delivery_outcome = 'Late' THEN 1 ELSE 0 END) AS late_deliveries,
  SUM(CASE WHEN minutes_vs_promise BETWEEN -5 AND 0 THEN 1 ELSE 0 END) AS at_risk_deliveries,
  ROUND(100 * AVG(CASE WHEN delivery_outcome = 'Early' THEN 1 ELSE 0 END), 2) AS early_pct,
  ROUND(100 * AVG(CASE WHEN delivery_outcome = 'In Window' THEN 1 ELSE 0 END), 2) AS in_window_pct,
  ROUND(100 * AVG(CASE WHEN is_late THEN 0 ELSE 1 END), 2) AS on_time_pct,
  ROUND(100 * AVG(CASE WHEN minutes_vs_promise BETWEEN -5 AND 0 THEN 1 ELSE 0 END), 2) AS at_risk_pct,
  ROUND(AVG(dispatch_to_door_min), 2) AS avg_door_min,
  ROUND(percentile(dispatch_to_door_min, 0.5), 2) AS median_door_min,
  ROUND(percentile(dispatch_to_door_min, 0.9), 2) AS p90_door_min,
  ROUND(AVG(minutes_vs_promise), 2) AS avg_minutes_vs_promise
FROM workspace.gold.fact_deliveries
WHERE is_valid_for_analysis
GROUP BY promise_window_min;

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.gold.kpi_eta_accuracy
COMMENT 'Accuracy of scheduled arrival times by segment for valid deliveries. Negative error means the driver arrived before the scheduled time.'
AS
WITH valid AS (
  SELECT
    eta_error_min,
    promise_window_min,
    CASE
      WHEN assigned_hour BETWEEN 11 AND 13 THEN 'Lunch'
      WHEN assigned_hour BETWEEN 14 AND 16 THEN 'Afternoon'
      WHEN assigned_hour BETWEEN 17 AND 19 THEN 'Dinner'
      ELSE 'Late Evening'
    END AS day_part
  FROM workspace.gold.fact_deliveries
  WHERE is_valid_for_analysis
),
segments AS (
  SELECT 'Overall' AS segment_type, 'All Deliveries' AS segment, eta_error_min FROM valid
  UNION ALL
  SELECT 'Day Part', day_part, eta_error_min FROM valid
  UNION ALL
  SELECT 'Promise Window', concat(promise_window_min, ' min'), eta_error_min FROM valid
)
SELECT
  segment_type,
  segment,
  COUNT(*) AS deliveries,
  ROUND(AVG(eta_error_min), 2) AS avg_eta_error_min,
  ROUND(AVG(abs(eta_error_min)), 2) AS mean_abs_error_min,
  ROUND(percentile(eta_error_min, 0.5), 2) AS median_eta_error_min,
  ROUND(percentile(abs(eta_error_min), 0.9), 2) AS p90_abs_error_min,
  ROUND(100 * AVG(CASE WHEN abs(eta_error_min) <= 5 THEN 1 ELSE 0 END), 2) AS within_5_min_pct,
  SUM(CASE WHEN eta_error_min < -5 THEN 1 ELSE 0 END) AS over_5_min_early,
  SUM(CASE WHEN eta_error_min > 5 THEN 1 ELSE 0 END) AS over_5_min_late
FROM segments
GROUP BY segment_type, segment;

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.gold.kpi_driver_performance
COMMENT 'Driver workload and delivery performance for valid deliveries. Compare drivers using dispatch-to-door time, because start-time logging differs by driver.'
AS
SELECT
  d.driver_name,
  CASE
    WHEN COUNT(*) >= 50 THEN 'Core'
    WHEN COUNT(*) >= 20 THEN 'Regular'
    ELSE 'Low Volume'
  END AS volume_band,
  COUNT(*) AS deliveries,
  COUNT(DISTINCT fd.date_key) AS active_days,
  COUNT(DISTINCT fd.run_id) AS trips,
  ROUND(COUNT(*) / COUNT(DISTINCT fd.date_key), 2) AS deliveries_per_active_day,
  ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS share_of_deliveries_pct,
  ROUND(AVG(fd.dispatch_to_door_min), 2) AS avg_door_min,
  ROUND(percentile(fd.dispatch_to_door_min, 0.5), 2) AS median_door_min,
  ROUND(percentile(fd.dispatch_to_door_min, 0.9), 2) AS p90_door_min,
  ROUND(AVG(fd.assign_to_start_min), 2) AS avg_assign_to_start_min,
  SUM(CASE WHEN fd.is_late THEN 1 ELSE 0 END) AS late_deliveries,
  ROUND(100 * AVG(CASE WHEN fd.is_late THEN 0 ELSE 1 END), 2) AS on_time_pct,
  ROUND(100 * AVG(CASE WHEN fd.promise_window_min = 15 THEN 1 ELSE 0 END), 1) AS pct_15_min_window,
  ROUND(AVG(fd.run_size), 2) AS avg_run_size,
  ROUND(100 * AVG(CASE WHEN fd.run_size > 1 THEN 1 ELSE 0 END), 1) AS batched_pct
FROM workspace.gold.fact_deliveries fd
JOIN workspace.gold.dim_driver d ON fd.driver_key = d.driver_key
WHERE fd.is_valid_for_analysis
GROUP BY d.driver_name;

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.gold.kpi_fleet_performance
COMMENT 'Fleet performance overall and by promise window for valid deliveries. Unknown means the fleet was not recorded.'
AS
WITH fleet_metrics AS (
  SELECT
    f.fleet_name,
    COALESCE(CAST(fd.promise_window_min AS STRING), 'All') AS promise_window,
    COUNT(*) AS deliveries,
    COUNT(DISTINCT fd.driver_key) AS drivers,
    COUNT(DISTINCT fd.date_key) AS active_days,
    ROUND(AVG(fd.dispatch_to_door_min), 2) AS avg_door_min,
    ROUND(percentile(fd.dispatch_to_door_min, 0.5), 2) AS median_door_min,
    ROUND(percentile(fd.dispatch_to_door_min, 0.9), 2) AS p90_door_min,
    SUM(CASE WHEN fd.is_late THEN 1 ELSE 0 END) AS late_deliveries,
    ROUND(100 * AVG(CASE WHEN fd.is_late THEN 0 ELSE 1 END), 2) AS on_time_pct,
    ROUND(AVG(fd.run_size), 2) AS avg_run_size,
    ROUND(100 * AVG(CASE WHEN fd.run_size > 1 THEN 1 ELSE 0 END), 2) AS batched_pct
  FROM workspace.gold.fact_deliveries fd
  JOIN workspace.gold.dim_fleet f ON fd.fleet_key = f.fleet_key
  WHERE fd.is_valid_for_analysis
  GROUP BY GROUPING SETS ((f.fleet_name), (f.fleet_name, fd.promise_window_min))
)
SELECT
  fleet_name,
  promise_window,
  deliveries,
  ROUND(100 * deliveries / SUM(deliveries) OVER (PARTITION BY promise_window), 2) AS share_of_deliveries_pct,
  drivers,
  active_days,
  avg_door_min,
  median_door_min,
  p90_door_min,
  late_deliveries,
  on_time_pct,
  avg_run_size,
  batched_pct
FROM fleet_metrics;

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.gold.kpi_batching
COMMENT 'Delivery time and lateness by trip size and stop position for valid deliveries.'
AS
WITH stops AS (
  SELECT
    *,
    CASE
      WHEN run_id IS NULL THEN 1
      ELSE DENSE_RANK() OVER (PARTITION BY run_id ORDER BY customer_checkin_at)
    END AS stop_number
  FROM workspace.gold.fact_deliveries
),
valid AS (
  SELECT
    dispatch_to_door_min,
    is_late,
    promise_window_min,
    run_size,
    CASE
      WHEN run_size = 1 THEN '1 order'
      WHEN run_size = 2 THEN '2 orders'
      WHEN run_size = 3 THEN '3 orders'
      ELSE '4+ orders'
    END AS trip_size,
    CASE
      WHEN stop_number = 1 THEN 'Stop 1'
      WHEN stop_number = 2 THEN 'Stop 2'
      ELSE 'Stop 3+'
    END AS stop_position
  FROM stops
  WHERE is_valid_for_analysis
),
segments AS (
  SELECT 'Trip Size' AS analysis_type, trip_size AS segment,
         promise_window_min, dispatch_to_door_min, is_late
  FROM valid
  UNION ALL
  SELECT 'Stop Position', stop_position,
         promise_window_min, dispatch_to_door_min, is_late
  FROM valid
  WHERE run_size > 1
)
SELECT
  analysis_type,
  segment,
  COALESCE(CAST(promise_window_min AS STRING), 'All') AS promise_window,
  COUNT(*) AS deliveries,
  ROUND(AVG(dispatch_to_door_min), 2) AS avg_door_min,
  ROUND(percentile(dispatch_to_door_min, 0.5), 2) AS median_door_min,
  ROUND(percentile(dispatch_to_door_min, 0.9), 2) AS p90_door_min,
  SUM(CASE WHEN is_late THEN 1 ELSE 0 END) AS late_deliveries,
  ROUND(100 * AVG(CASE WHEN is_late THEN 0 ELSE 1 END), 2) AS on_time_pct
FROM segments
GROUP BY GROUPING SETS (
  (analysis_type, segment),
  (analysis_type, segment, promise_window_min)
);

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.gold.kpi_peak_load
COMMENT 'Delivery performance by number of other active orders at assignment, for valid deliveries.'
AS
WITH valid AS (
  SELECT
    dispatch_to_door_min,
    is_late,
    promise_window_min,
    run_size,
    CASE
      WHEN active_orders_at_assignment = 0 THEN '0 active'
      WHEN active_orders_at_assignment = 1 THEN '1 active'
      WHEN active_orders_at_assignment = 2 THEN '2 active'
      WHEN active_orders_at_assignment <= 4 THEN '3-4 active'
      ELSE '5+ active'
    END AS load_band
  FROM workspace.gold.fact_deliveries
  WHERE is_valid_for_analysis
),
segments AS (
  SELECT 'All Trips' AS trip_scope, * FROM valid
  UNION ALL
  SELECT 'Single-Order Trips', * FROM valid WHERE run_size = 1
)
SELECT
  trip_scope,
  load_band,
  COALESCE(CAST(promise_window_min AS STRING), 'All') AS promise_window,
  COUNT(*) AS deliveries,
  ROUND(AVG(dispatch_to_door_min), 2) AS avg_door_min,
  ROUND(percentile(dispatch_to_door_min, 0.5), 2) AS median_door_min,
  ROUND(percentile(dispatch_to_door_min, 0.9), 2) AS p90_door_min,
  SUM(CASE WHEN is_late THEN 1 ELSE 0 END) AS late_deliveries,
  ROUND(100 * AVG(CASE WHEN is_late THEN 0 ELSE 1 END), 2) AS on_time_pct,
  ROUND(AVG(run_size), 2) AS avg_run_size,
  ROUND(100 * AVG(CASE WHEN run_size > 1 THEN 1 ELSE 0 END), 2) AS batched_pct
FROM segments
GROUP BY GROUPING SETS (
  (trip_scope, load_band),
  (trip_scope, load_band, promise_window_min)
);

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.gold.kpi_data_quality
COMMENT 'Pipeline reconciliation, validity, and completeness metrics from Bronze to Silver.'
AS
WITH counts AS (
  SELECT
    b.bronze_rows,
    s.*
  FROM (
    SELECT COUNT(*) AS bronze_rows
    FROM workspace.bronze.deliveries
  ) b
  CROSS JOIN (
    SELECT
      COUNT(*) AS silver_rows,
      COUNT_IF(is_valid_for_analysis) AS valid_rows,
      COUNT_IF(NOT is_valid_for_analysis) AS excluded_rows,
      COUNT_IF(has_sequence_error) AS sequence_errors,
      COUNT_IF(has_duration_outlier) AS duration_outliers,
      COUNT_IF(has_sequence_error AND has_duration_outlier) AS both_issues,
      COUNT_IF(fleet_name = 'Unknown') AS missing_fleet,
      COUNT_IF(driver_name = 'Unknown') AS missing_driver,
      COUNT_IF(run_id IS NULL) AS missing_run_id,
      COUNT_IF(task_rating IS NULL) AS missing_rating,
      COUNT_IF(
        assigned_at IS NULL OR started_at IS NULL OR store_checkin_at IS NULL
        OR store_checkout_at IS NULL OR scheduled_at IS NULL OR customer_checkin_at IS NULL
        OR customer_checkout_at IS NULL OR promise_earliest_at IS NULL OR promise_latest_at IS NULL
      ) AS bad_timestamps
    FROM workspace.silver.deliveries
  ) s
),
metrics AS (
  SELECT stack(
    13,
    1,  'Pipeline',     'Bronze rows received',      bronze_rows,       'Bronze rows', bronze_rows,
    2,  'Pipeline',     'Duplicate rows removed',    bronze_rows - silver_rows, 'Bronze rows', bronze_rows,
    3,  'Pipeline',     'Distinct deliveries',       silver_rows,       'Bronze rows', bronze_rows,
    4,  'Pipeline',     'Valid deliveries',          valid_rows,        'Silver rows', silver_rows,
    5,  'Validity',     'Timestamp sequence errors', sequence_errors,   'Silver rows', silver_rows,
    6,  'Validity',     'Duration outliers',         duration_outliers, 'Silver rows', silver_rows,
    7,  'Validity',     'Records with both issues',  both_issues,       'Silver rows', silver_rows,
    8,  'Validity',     'Records excluded',          excluded_rows,     'Silver rows', silver_rows,
    9,  'Completeness', 'Missing fleet',             missing_fleet,     'Silver rows', silver_rows,
    10, 'Completeness', 'Missing driver',            missing_driver,    'Silver rows', silver_rows,
    11, 'Completeness', 'Missing run ID',            missing_run_id,    'Silver rows', silver_rows,
    12, 'Completeness', 'Missing customer rating',   missing_rating,    'Silver rows', silver_rows,
    13, 'Completeness', 'Unparseable timestamps',    bad_timestamps,    'Silver rows', silver_rows
  ) AS (metric_order, category, metric_name, metric_value, base_name, base_value)
  FROM counts
)
SELECT
  metric_order,
  category,
  metric_name,
  metric_value,
  base_name,
  base_value,
  ROUND(100 * metric_value / base_value, 2) AS pct_of_base,
  current_timestamp() AS calculated_at
FROM metrics;
