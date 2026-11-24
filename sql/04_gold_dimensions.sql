-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 04 - Gold dimensions
-- MAGIC
-- MAGIC Driver, fleet, and date dimensions with hashed surrogate keys.

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.gold.dim_driver AS
SELECT xxhash64(driver_name) AS driver_key, driver_name
FROM (SELECT DISTINCT driver_name FROM workspace.silver.deliveries);

CREATE OR REPLACE TABLE workspace.gold.dim_fleet AS
SELECT xxhash64(fleet_name) AS fleet_key, fleet_name
FROM (SELECT DISTINCT fleet_name FROM workspace.silver.deliveries);

CREATE OR REPLACE TABLE workspace.gold.dim_date AS
WITH bounds AS (
  SELECT MIN(to_date(assigned_at)) AS start_date, MAX(to_date(assigned_at)) AS end_date
  FROM workspace.silver.deliveries
),
dates AS (
  SELECT explode(sequence(start_date, end_date, INTERVAL 1 DAY)) AS full_date
  FROM bounds
)
SELECT
  CAST(date_format(full_date, 'yyyyMMdd') AS INT) AS date_key,
  full_date,
  year(full_date) AS year,
  month(full_date) AS month,
  date_format(full_date, 'MMMM') AS month_name,
  day(full_date) AS day_of_month,
  weekday(full_date) + 1 AS day_number,
  date_format(full_date, 'EEEE') AS day_name,
  weekday(full_date) >= 5 AS is_weekend
FROM dates;

-- COMMAND ----------

SELECT
  (SELECT COUNT(*) FROM workspace.gold.dim_driver) AS drivers,
  (SELECT COUNT(*) FROM workspace.gold.dim_fleet) AS fleets,
  (SELECT COUNT(*) FROM workspace.gold.dim_date) AS dates;
