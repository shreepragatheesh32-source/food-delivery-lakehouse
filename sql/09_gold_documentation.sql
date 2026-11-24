-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 09 - Unity Catalog documentation
-- MAGIC
-- MAGIC Adds table and column comments to the Gold star schema and the test results table, so the descriptions show up in Catalog Explorer and in tools that read Unity Catalog. The KPI tables already got table comments when they were created.
-- MAGIC
-- MAGIC Run after notebook `08`, because cell 1 there adds two columns to the fact table.

-- COMMAND ----------

-- dim_date
COMMENT ON TABLE workspace.gold.dim_date IS 'One row per calendar day between the first and last assigned delivery.';
ALTER TABLE workspace.gold.dim_date ALTER COLUMN date_key COMMENT 'Surrogate key in yyyyMMdd format';
ALTER TABLE workspace.gold.dim_date ALTER COLUMN full_date COMMENT 'Calendar date';
ALTER TABLE workspace.gold.dim_date ALTER COLUMN year COMMENT 'Calendar year';
ALTER TABLE workspace.gold.dim_date ALTER COLUMN month COMMENT 'Month number, 1 to 12';
ALTER TABLE workspace.gold.dim_date ALTER COLUMN month_name COMMENT 'Full month name';
ALTER TABLE workspace.gold.dim_date ALTER COLUMN day_of_month COMMENT 'Day of the month';
ALTER TABLE workspace.gold.dim_date ALTER COLUMN day_number COMMENT 'Day of week, 1 = Monday to 7 = Sunday; use to sort day_name';
ALTER TABLE workspace.gold.dim_date ALTER COLUMN day_name COMMENT 'Full weekday name';
ALTER TABLE workspace.gold.dim_date ALTER COLUMN is_weekend COMMENT 'True for Saturday and Sunday';

-- COMMAND ----------

-- dim_driver
COMMENT ON TABLE workspace.gold.dim_driver IS 'One row per driver. Names are anonymized labels such as DRV_01.';
ALTER TABLE workspace.gold.dim_driver ALTER COLUMN driver_key COMMENT 'Surrogate key, xxhash64 of driver_name';
ALTER TABLE workspace.gold.dim_driver ALTER COLUMN driver_name COMMENT 'Anonymized driver label, or Unknown';

-- COMMAND ----------

-- dim_fleet
COMMENT ON TABLE workspace.gold.dim_fleet IS 'One row per fleet. Unknown means the fleet was not recorded in the source.';
ALTER TABLE workspace.gold.dim_fleet ALTER COLUMN fleet_key COMMENT 'Surrogate key, xxhash64 of fleet_name';
ALTER TABLE workspace.gold.dim_fleet ALTER COLUMN fleet_name COMMENT 'Anonymized fleet label, or Unknown';

-- COMMAND ----------

-- fact_deliveries
COMMENT ON TABLE workspace.gold.fact_deliveries IS 'One row per delivery record, linked to date, driver, and fleet. Use is_valid_for_analysis to exclude records with timestamp errors or outliers.';
ALTER TABLE workspace.gold.fact_deliveries ALTER COLUMN delivery_key COMMENT 'Primary key, SHA-256 of order_id and run_id';
ALTER TABLE workspace.gold.fact_deliveries ALTER COLUMN order_id COMMENT 'Anonymized order ID';
ALTER TABLE workspace.gold.fact_deliveries ALTER COLUMN run_id COMMENT 'Anonymized trip ID; null when the order was not part of a recorded trip';
ALTER TABLE workspace.gold.fact_deliveries ALTER COLUMN date_key COMMENT 'Assignment date, joins to dim_date';
ALTER TABLE workspace.gold.fact_deliveries ALTER COLUMN driver_key COMMENT 'Joins to dim_driver';
ALTER TABLE workspace.gold.fact_deliveries ALTER COLUMN fleet_key COMMENT 'Joins to dim_fleet';
ALTER TABLE workspace.gold.fact_deliveries ALTER COLUMN order_type COMMENT 'Standard or Service';
ALTER TABLE workspace.gold.fact_deliveries ALTER COLUMN assigned_hour COMMENT 'Hour of day the order was assigned, 0 to 23';
ALTER TABLE workspace.gold.fact_deliveries ALTER COLUMN run_size COMMENT 'Number of orders on the same trip; 1 means a single-order trip';
ALTER TABLE workspace.gold.fact_deliveries ALTER COLUMN active_orders_at_assignment COMMENT 'Other valid orders already assigned and not yet delivered when this order was assigned';
ALTER TABLE workspace.gold.fact_deliveries ALTER COLUMN assigned_at COMMENT 'When the order was assigned to a driver';
ALTER TABLE workspace.gold.fact_deliveries ALTER COLUMN started_at COMMENT 'When the driver started the trip';
ALTER TABLE workspace.gold.fact_deliveries ALTER COLUMN store_checkout_at COMMENT 'When the driver left the store';
ALTER TABLE workspace.gold.fact_deliveries ALTER COLUMN scheduled_at COMMENT 'Scheduled arrival time shown to the customer';
ALTER TABLE workspace.gold.fact_deliveries ALTER COLUMN customer_checkin_at COMMENT 'When the driver arrived at the customer';
ALTER TABLE workspace.gold.fact_deliveries ALTER COLUMN customer_checkout_at COMMENT 'When the driver left the customer';
ALTER TABLE workspace.gold.fact_deliveries ALTER COLUMN promise_earliest_at COMMENT 'Start of the promised delivery window';
ALTER TABLE workspace.gold.fact_deliveries ALTER COLUMN promise_latest_at COMMENT 'End of the promised delivery window';
ALTER TABLE workspace.gold.fact_deliveries ALTER COLUMN assign_to_start_min COMMENT 'Minutes from assignment to trip start; depends on how each driver logs trips';
ALTER TABLE workspace.gold.fact_deliveries ALTER COLUMN start_to_store_exit_min COMMENT 'Minutes from trip start to leaving the store';
ALTER TABLE workspace.gold.fact_deliveries ALTER COLUMN store_exit_to_door_min COMMENT 'Minutes from leaving the store to arriving at the customer';
ALTER TABLE workspace.gold.fact_deliveries ALTER COLUMN dispatch_to_door_min COMMENT 'Minutes from assignment to arrival; the main delivery time metric';
ALTER TABLE workspace.gold.fact_deliveries ALTER COLUMN trip_min COMMENT 'Minutes from trip start to arrival';
ALTER TABLE workspace.gold.fact_deliveries ALTER COLUMN customer_dwell_min COMMENT 'Minutes spent at the customer';
ALTER TABLE workspace.gold.fact_deliveries ALTER COLUMN promise_window_min COMMENT 'Length of the promised window in minutes: 15, 35, or 45';
ALTER TABLE workspace.gold.fact_deliveries ALTER COLUMN eta_error_min COMMENT 'Arrival minus scheduled time in minutes; negative means early';
ALTER TABLE workspace.gold.fact_deliveries ALTER COLUMN minutes_vs_promise COMMENT 'Arrival minus end of promise window in minutes; positive means late';
ALTER TABLE workspace.gold.fact_deliveries ALTER COLUMN delivery_outcome COMMENT 'Early, In Window, or Late against the promise window';
ALTER TABLE workspace.gold.fact_deliveries ALTER COLUMN is_late COMMENT 'True when arrival was after promise_latest_at';
ALTER TABLE workspace.gold.fact_deliveries ALTER COLUMN task_rating COMMENT 'Customer rating; mostly null in the source';
ALTER TABLE workspace.gold.fact_deliveries ALTER COLUMN is_valid_for_analysis COMMENT 'False for records with timestamp sequence errors or duration outliers';
ALTER TABLE workspace.gold.fact_deliveries ALTER COLUMN _bronze_ingested_at COMMENT 'When the source row was loaded into Bronze';

-- COMMAND ----------

-- data_test_results
COMMENT ON TABLE workspace.gold.data_test_results IS 'History of automated data tests. Each run appends 24 rows with the same test_run_id.';
ALTER TABLE workspace.gold.data_test_results ALTER COLUMN test_run_id COMMENT 'Run timestamp in yyyyMMddHHmmss; shared by all tests in one run';
ALTER TABLE workspace.gold.data_test_results ALTER COLUMN test_order COMMENT 'Test number, 1 to 24';
ALTER TABLE workspace.gold.data_test_results ALTER COLUMN category COMMENT 'Reconciliation, Keys, Referential, Validity, or KPI';
ALTER TABLE workspace.gold.data_test_results ALTER COLUMN test_name COMMENT 'What the test checks';
ALTER TABLE workspace.gold.data_test_results ALTER COLUMN actual_value COMMENT 'Measured value';
ALTER TABLE workspace.gold.data_test_results ALTER COLUMN expected_value COMMENT 'Value required to pass';
ALTER TABLE workspace.gold.data_test_results ALTER COLUMN status COMMENT 'PASS when actual equals expected, otherwise FAIL';
ALTER TABLE workspace.gold.data_test_results ALTER COLUMN tested_at COMMENT 'When the test ran';

-- COMMAND ----------

-- Check: every Gold table and column should now have a comment
SELECT
  t.table_name,
  t.comment IS NOT NULL AS table_has_comment,
  COUNT(c.column_name) AS columns,
  COUNT_IF(c.comment IS NULL) AS columns_without_comment
FROM workspace.information_schema.tables t
JOIN workspace.information_schema.columns c
  ON c.table_schema = t.table_schema AND c.table_name = t.table_name
WHERE t.table_schema = 'gold'
GROUP BY t.table_name, t.comment
ORDER BY t.table_name;
