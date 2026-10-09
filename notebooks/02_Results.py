# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 02_Results
# MAGIC Run this last, after `01_Sync_Pipeline`. It shows what each table looks like after the files are loaded, and runs a few checks. It reads the table names and columns from `config/sync_config.json`, so it works for any dataset described there.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 - Imports and project folder
# MAGIC Works out the project folder from where this notebook is opened, so no path has to be typed in.

# COMMAND ----------

import os
import sys
from pyspark.sql import functions as F

try:
    nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    nb_path = nb_path if nb_path.startswith("/Workspace") else "/Workspace" + nb_path
    BASE_DIR = os.path.dirname(os.path.dirname(nb_path))
except Exception:
    BASE_DIR = os.path.abspath("..")

sys.path.insert(0, os.path.join(BASE_DIR, "src"))
from sync_common import Settings, read_source

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Load the settings
# MAGIC Reads `config/sync_config.json` and gets the table names and column lists.

# COMMAND ----------

S = Settings(spark, BASE_DIR)
T = S.tables
print(S.summary())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Batch log
# MAGIC One row per file: how many rows were read, how many were new, changed or deleted, and how many were unchanged (and so left untouched).

# COMMAND ----------

display(spark.table(T["batch_log"]).orderBy("batch_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4 - Raw data (bronze)
# MAGIC Every file is kept as received, tagged with its batch id.

# COMMAND ----------

display(spark.table(T["bronze"]).orderBy("batch_id", *S.keys))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5 - Current data
# MAGIC The latest values for each record after all files are loaded.

# COMMAND ----------

display(spark.table(T["current"]).orderBy(*S.keys))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 6 - Change log (CDC)
# MAGIC Every change found. For each measure column it shows the old value, the new value and the difference. The first file is all `INSERT`; later files show only what changed.

# COMMAND ----------

display(spark.table(T["change_log"]).orderBy("change_ts", *S.keys))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 7 - History (SCD Type 2)
# MAGIC The master columns with their versions. Each record has one row where `is_current` is true. If a master column changes in a later file, the old row is closed and a new one is added. This step is skipped when the config has no master columns.

# COMMAND ----------

if S.master:
    display(spark.table(T["dimension"]).orderBy(*S.keys, "effective_from"))
else:
    print("No master columns in the config, so there is no history table.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 8 - Change Data Feed
# MAGIC Shows the updates on the current table as other systems would read them: the row before and after each update.

# COMMAND ----------

feed = (
    spark.read.option("readChangeFeed", "true").option("startingVersion", 0).table(T["current"])
    .where("_change_type IN ('update_preimage', 'update_postimage')")
    .select(*S.keys, *S.tracked, "_change_type", "_commit_version")
    .orderBy("_commit_version", *S.keys, "_change_type")
)
display(feed)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 9 - Checks
# MAGIC Confirms that the current table matches the latest file, that each record has exactly one current row in the history table, and that every batch succeeded. For files that hold only changes (`incremental`), the current table must contain every row of the latest file.

# COMMAND ----------

cols = [*S.keys, *S.tracked]
latest = read_source(spark, S, S.batches[-1]["file"]).select(*cols)
current = spark.table(T["current"]).where("NOT is_deleted").select(*cols)

matches = latest.exceptAll(current).count() == 0
if S.snapshot_type == "full":
    matches = matches and current.exceptAll(latest).count() == 0

batches_ok = spark.table(T["batch_log"]).where("status != 'SUCCESS'").count() == 0

print("Current data matches the latest file:", matches)
if S.master:
    dim_ok = spark.table(T["dimension"]).where("is_current").groupBy(*S.keys).count().where("count > 1").count() == 0
    print("One current row per record in the history table:", dim_ok)
print("All batches succeeded:", batches_ok)