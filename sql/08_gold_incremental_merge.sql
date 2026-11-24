-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 08 - Incremental Gold refresh with MERGE
-- MAGIC
-- MAGIC Notebook `05` rebuilds the fact table with `CREATE OR REPLACE`. This notebook refreshes it in place instead:
-- MAGIC
-- MAGIC - new deliveries are inserted
-- MAGIC - deliveries whose values changed are updated
-- MAGIC - deliveries that no longer exist in Silver are deleted
-- MAGIC - unchanged rows are left alone, detected with a row hash
-- MAGIC
-- MAGIC `run_size` and `active_orders_at_assignment` depend on other deliveries, so the source recalculates every row and the hash decides what to write. At this data size that's cheap, and it keeps the window logic correct.
-- MAGIC
-- MAGIC Run cell 1 once. After that, run cells 2 to 4 every time Silver changes. KPI tables are small aggregates, so rerun notebook `06` after this one to rebuild them.

-- COMMAND ----------

-- Cell 1 (one-time setup): tracking columns for the fact table
ALTER TABLE workspace.gold.fact_deliveries ADD COLUMNS (
  _row_hash STRING COMMENT 'SHA-256 of the business columns, used by MERGE to detect changes',
  _gold_updated_at TIMESTAMP COMMENT 'When MERGE last inserted or updated this row'
);

-- COMMAND ----------

-- Cell 2: add new drivers and fleets to the dimensions
MERGE INTO workspace.gold.dim_driver t
USING (SELECT DISTINCT xxhash64(driver_name) AS driver_key, driver_name FROM workspace.silver.deliveries) s
ON t.driver_key = s.driver_key
WHEN NOT MATCHED THEN INSERT *;

MERGE INTO workspace.gold.dim_fleet t
USING (SELECT DISTINCT xxhash64(fleet_name) AS fleet_key, fleet_name FROM workspace.silver.deliveries) s
ON t.fleet_key = s.fleet_key
WHEN NOT MATCHED THEN INSERT *;

-- COMMAND ----------

-- Cell 3: build the source rows and merge them into the fact table
CREATE OR REPLACE TEMP VIEW fact_source AS
SELECT
  *,
  sha2(to_json(struct(* EXCEPT (_bronze_ingested_at))), 256) AS _row_hash,
  current_timestamp() AS _gold_updated_at
FROM (
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
  JOIN workspace.gold.dim_fleet f ON s.fleet_name = f.fleet_name
);

MERGE INTO workspace.gold.fact_deliveries t
USING fact_source s
ON t.delivery_key = s.delivery_key
WHEN MATCHED AND t._row_hash IS DISTINCT FROM s._row_hash THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
WHEN NOT MATCHED BY SOURCE THEN DELETE;

-- COMMAND ----------

-- Cell 4: what the last two MERGE runs on the fact table changed
SELECT
  version,
  timestamp,
  operation,
  operationMetrics.numTargetRowsInserted AS inserted,
  operationMetrics.numTargetRowsUpdated AS updated,
  operationMetrics.numTargetRowsDeleted AS deleted,
  operationMetrics.numTargetRowsCopied AS copied_unchanged
FROM (DESCRIBE HISTORY workspace.gold.fact_deliveries)
WHERE operation = 'MERGE'
ORDER BY version DESC
LIMIT 2;
