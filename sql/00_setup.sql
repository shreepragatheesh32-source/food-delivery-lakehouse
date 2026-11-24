-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 00 - Setup
-- MAGIC
-- MAGIC Creates the medallion schemas and the landing volume in the `workspace` catalog. Upload `deliveries_anonymized.csv` to `/Volumes/workspace/bronze/landing/` after this runs.

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS workspace.bronze;
CREATE SCHEMA IF NOT EXISTS workspace.silver;
CREATE SCHEMA IF NOT EXISTS workspace.gold;
CREATE VOLUME IF NOT EXISTS workspace.bronze.landing;

-- COMMAND ----------

LIST '/Volumes/workspace/bronze/landing/';
