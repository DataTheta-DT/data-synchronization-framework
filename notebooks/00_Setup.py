# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 00_Setup
# MAGIC Run this first. It reads `config/sync_config.json`, checks it, and creates the schema and the tables for your dataset. Nothing in this notebook is specific to one dataset: the tables are built from the columns listed in the config.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 - Imports and project folder
# MAGIC Works out the project folder from where this notebook is opened, so no path has to be typed in.

# COMMAND ----------

import os
import sys

try:
    nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    nb_path = nb_path if nb_path.startswith("/Workspace") else "/Workspace" + nb_path
    BASE_DIR = os.path.dirname(os.path.dirname(nb_path))
except Exception:
    BASE_DIR = os.path.abspath("..")

sys.path.insert(0, os.path.join(BASE_DIR, "src"))
from sync_common import Settings

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Load and check the settings
# MAGIC Reads `config/sync_config.json` and stops with a clear message if something is wrong, for example a key column that is not in the column list.

# COMMAND ----------

S = Settings(spark, BASE_DIR)
print(S.summary())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Create the schema
# MAGIC Creates the schema from the settings if it does not exist.

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {S.prefix}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4 - Reset the tables (optional)
# MAGIC When `reset_demo` is `true` in the settings, the dataset's tables are dropped so the run starts clean. Set it to `false` for real use so loaded data is kept.

# COMMAND ----------

T = S.tables

if S.reset_demo:
    for name in T.values():
        spark.sql(f"DROP TABLE IF EXISTS {name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5 - Create the tables
# MAGIC Builds each table from the column list in the settings. The raw table keeps every column, the current table keeps the keys and tracked columns, the change log keeps old and new values for each measure, and the history table keeps the master columns. The current table has Change Data Feed switched on.

# COMMAND ----------

def defs(names):
    return ", ".join(f"`{n}` {S.col_types[n]}" for n in names)

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {T['bronze']} (
    {defs(S.column_names)},
    batch_id STRING, source_file STRING, ingested_at TIMESTAMP)
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {T['batch_log']} (
    batch_id STRING, source_file STRING, batch_date TIMESTAMP,
    rows_read BIGINT, inserts BIGINT, updates BIGINT, deletes BIGINT, unchanged BIGINT,
    dim_new BIGINT, dim_changed BIGINT, status STRING, ran_at TIMESTAMP)
""")

measure_defs = ", ".join(
    f"`old_{m}` {S.col_types[m]}, `new_{m}` {S.col_types[m]}"
    + (f", `{m}_delta` {S.col_types[m]}" if m in S.numeric_measures else "")
    for m in S.measures
)
log_columns = [
    "batch_id STRING", "change_ts TIMESTAMP", defs(S.keys + S.master), "change_type STRING", measure_defs,
]
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {T['change_log']} (
    {", ".join(c for c in log_columns if c)})
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {T['current']} (
    {defs(S.keys + S.tracked)},
    row_hash STRING, is_deleted BOOLEAN, updated_at TIMESTAMP, last_batch_id STRING)
TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")

if S.master:
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {T['dimension']} (
        {defs(S.keys + S.master)},
        master_hash STRING, effective_from TIMESTAMP, effective_to TIMESTAMP, is_current BOOLEAN)
    """)