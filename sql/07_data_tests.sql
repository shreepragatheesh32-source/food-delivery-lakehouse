-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 07 - Data tests
-- MAGIC
-- MAGIC Runs 24 reconciliation, key, referential, validity, and KPI checks, and appends the results to `gold.data_test_results` so test history is kept.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS workspace.gold.data_test_results (
  test_run_id STRING,
  test_order INT,
  category STRING,
  test_name STRING,
  actual_value BIGINT,
  expected_value BIGINT,
  status STRING,
  tested_at TIMESTAMP
)
COMMENT 'History of automated data test results for the delivery lakehouse.';

-- COMMAND ----------

INSERT INTO workspace.gold.data_test_results
WITH counts AS (
  SELECT
    (SELECT COUNT(*) FROM workspace.bronze.deliveries) AS bronze_rows,
    (SELECT SUM(source_copies) FROM workspace.silver.deliveries_deduped) AS source_copies,
    (SELECT COUNT(*) FROM workspace.silver.deliveries_deduped) AS deduped_rows,
    (SELECT COUNT(*) FROM workspace.silver.deliveries) AS silver_rows,
    (SELECT COUNT(*) FROM workspace.gold.fact_deliveries) AS fact_rows,
    (SELECT COUNT(DISTINCT delivery_key) FROM workspace.gold.fact_deliveries) AS unique_keys,
    (SELECT COUNT_IF(delivery_key IS NULL) FROM workspace.gold.fact_deliveries) AS null_keys,
    (SELECT COUNT(*) FROM workspace.gold.fact_deliveries f
       LEFT ANTI JOIN workspace.gold.dim_driver d ON f.driver_key = d.driver_key) AS orphan_drivers,
    (SELECT COUNT(*) FROM workspace.gold.fact_deliveries f
       LEFT ANTI JOIN workspace.gold.dim_fleet d ON f.fleet_key = d.fleet_key) AS orphan_fleets,
    (SELECT COUNT(*) FROM workspace.gold.fact_deliveries f
       LEFT ANTI JOIN workspace.gold.dim_date d ON f.date_key = d.date_key) AS orphan_dates,
    (SELECT COUNT_IF(
        assigned_at IS NULL OR started_at IS NULL OR store_checkin_at IS NULL
        OR store_checkout_at IS NULL OR scheduled_at IS NULL OR customer_checkin_at IS NULL
        OR customer_checkout_at IS NULL OR promise_earliest_at IS NULL OR promise_latest_at IS NULL
      ) FROM workspace.silver.deliveries) AS bad_timestamps,
    (SELECT COUNT_IF(is_valid_for_analysis AND dispatch_to_door_min <= 0)
       FROM workspace.gold.fact_deliveries) AS non_positive_door,
    (SELECT COUNT_IF(is_valid_for_analysis AND dispatch_to_door_min > 90)
       FROM workspace.gold.fact_deliveries) AS door_over_90,
    (SELECT COUNT_IF(delivery_outcome NOT IN ('Early', 'In Window', 'Late'))
       FROM workspace.gold.fact_deliveries) AS bad_outcomes,
    (SELECT COUNT_IF(is_valid_for_analysis AND has_sequence_error)
       FROM workspace.silver.deliveries) AS valid_with_sequence_error,
    (SELECT COUNT_IF(is_valid_for_analysis) FROM workspace.gold.fact_deliveries) AS valid_rows,
    (SELECT COUNT_IF(is_valid_for_analysis AND is_late)
       FROM workspace.gold.fact_deliveries) AS valid_late,
    (SELECT SUM(deliveries) FROM workspace.gold.kpi_daily) AS daily_rows,
    (SELECT SUM(late_deliveries) FROM workspace.gold.kpi_daily) AS daily_late,
    (SELECT SUM(deliveries) FROM workspace.gold.kpi_hourly_demand) AS hourly_rows,
    (SELECT SUM(deliveries) FROM workspace.gold.kpi_delivery_stages
       WHERE segment_type = 'Overall' AND stage_order = 1) AS stage_rows,
    (SELECT SUM(deliveries) FROM workspace.gold.kpi_promise_performance) AS promise_rows,
    (SELECT SUM(deliveries) FROM workspace.gold.kpi_eta_accuracy
       WHERE segment_type = 'Overall') AS eta_rows,
    (SELECT SUM(deliveries) FROM workspace.gold.kpi_driver_performance) AS driver_rows,
    (SELECT SUM(deliveries) FROM workspace.gold.kpi_fleet_performance
       WHERE promise_window = 'All') AS fleet_rows,
    (SELECT SUM(deliveries) FROM workspace.gold.kpi_batching
       WHERE analysis_type = 'Trip Size' AND promise_window = 'All') AS batching_rows,
    (SELECT SUM(deliveries) FROM workspace.gold.kpi_peak_load
       WHERE trip_scope = 'All Trips' AND promise_window = 'All') AS peak_rows,
    (SELECT metric_value FROM workspace.gold.kpi_data_quality
       WHERE metric_name = 'Valid deliveries') AS dq_valid_rows
),
checks AS (
  SELECT stack(
    24,
    1,  'Reconciliation', 'Bronze rows equal Silver source copies', bronze_rows, source_copies,
    2,  'Reconciliation', 'Deduplicated rows equal Silver rows', deduped_rows, silver_rows,
    3,  'Reconciliation', 'Silver rows equal fact rows', silver_rows, fact_rows,
    4,  'Keys', 'Fact delivery keys are unique', fact_rows - unique_keys, 0L,
    5,  'Keys', 'Fact delivery keys are not null', null_keys, 0L,
    6,  'Referential', 'Every fact row has a driver', orphan_drivers, 0L,
    7,  'Referential', 'Every fact row has a fleet', orphan_fleets, 0L,
    8,  'Referential', 'Every fact row has a date', orphan_dates, 0L,
    9,  'Validity', 'All timestamps parsed', bad_timestamps, 0L,
    10, 'Validity', 'Valid deliveries have positive duration', non_positive_door, 0L,
    11, 'Validity', 'Valid deliveries are within 90 minutes', door_over_90, 0L,
    12, 'Validity', 'Delivery outcomes are accepted values', bad_outcomes, 0L,
    13, 'Validity', 'Valid deliveries have no sequence errors', valid_with_sequence_error, 0L,
    14, 'KPI', 'kpi_daily reconciles to fact', daily_rows, valid_rows,
    15, 'KPI', 'kpi_daily late count reconciles to fact', daily_late, valid_late,
    16, 'KPI', 'kpi_hourly_demand reconciles to fact', hourly_rows, valid_rows,
    17, 'KPI', 'kpi_delivery_stages reconciles to fact', stage_rows, valid_rows,
    18, 'KPI', 'kpi_promise_performance reconciles to fact', promise_rows, valid_rows,
    19, 'KPI', 'kpi_eta_accuracy reconciles to fact', eta_rows, valid_rows,
    20, 'KPI', 'kpi_driver_performance reconciles to fact', driver_rows, valid_rows,
    21, 'KPI', 'kpi_fleet_performance reconciles to fact', fleet_rows, valid_rows,
    22, 'KPI', 'kpi_batching reconciles to fact', batching_rows, valid_rows,
    23, 'KPI', 'kpi_peak_load reconciles to fact', peak_rows, valid_rows,
    24, 'KPI', 'kpi_data_quality reconciles to fact', dq_valid_rows, valid_rows
  ) AS (test_order, category, test_name, actual_value, expected_value)
  FROM counts
)
SELECT
  date_format(current_timestamp(), 'yyyyMMddHHmmss') AS test_run_id,
  test_order,
  category,
  test_name,
  actual_value,
  expected_value,
  CASE WHEN actual_value <=> expected_value THEN 'PASS' ELSE 'FAIL' END AS status,
  current_timestamp() AS tested_at
FROM checks;

-- COMMAND ----------

SELECT
  category,
  COUNT(*) AS total_tests,
  COUNT_IF(status = 'PASS') AS passed,
  COUNT_IF(status = 'FAIL') AS failed
FROM workspace.gold.data_test_results
WHERE test_run_id = (SELECT MAX(test_run_id) FROM workspace.gold.data_test_results)
GROUP BY category
ORDER BY category;

-- COMMAND ----------

SELECT test_order, test_name, actual_value, expected_value
FROM workspace.gold.data_test_results
WHERE status = 'FAIL'
  AND test_run_id = (SELECT MAX(test_run_id) FROM workspace.gold.data_test_results)
ORDER BY test_order;

-- COMMAND ----------

SELECT
  test_run_id,
  COUNT(*) AS total_tests,
  COUNT_IF(status = 'PASS') AS passed,
  COUNT_IF(status = 'FAIL') AS failed
FROM workspace.gold.data_test_results
GROUP BY test_run_id
ORDER BY test_run_id DESC;

-- COMMAND ----------

-- Stop the job if any test in the latest run failed. assert_true raises an error,
-- so the Workflow task turns red and later tasks don't run.
SELECT assert_true(
  COUNT_IF(status = 'FAIL') = 0,
  'One or more data tests failed. Check gold.data_test_results for the latest test_run_id.'
) AS all_tests_passed
FROM workspace.gold.data_test_results
WHERE test_run_id = (SELECT MAX(test_run_id) FROM workspace.gold.data_test_results);
