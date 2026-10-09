# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 01_Sync_Pipeline
# MAGIC Run this second, after `00_Setup`. It loads the files listed in `batches`, in order: copies the raw rows, finds what changed (CDC), updates the current table, keeps the history (SCD Type 2), and logs each batch. The columns, keys and rules all come from `config/sync_config.json`, so this notebook works for any dataset described there.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 - Imports and project folder
# MAGIC Works out the project folder from where this notebook is opened, so no path has to be typed in.

# COMMAND ----------

import os
import sys
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType
from delta.tables import DeltaTable

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
# MAGIC Reads and checks `config/sync_config.json`, and gets the table names and column lists used below.

# COMMAND ----------

S = Settings(spark, BASE_DIR)
T = S.tables
print(S.summary())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Batch log layout
# MAGIC The layout of one row in the batch log table.

# COMMAND ----------

BATCH_SCHEMA = StructType([
    StructField("batch_id", StringType()),
    StructField("source_file", StringType()),
    StructField("batch_date", TimestampType()),
    StructField("rows_read", LongType()),
    StructField("inserts", LongType()),
    StructField("updates", LongType()),
    StructField("deletes", LongType()),
    StructField("unchanged", LongType()),
    StructField("dim_new", LongType()),
    StructField("dim_changed", LongType()),
    StructField("status", StringType()),
    StructField("ran_at", TimestampType()),
])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4 - Add change fingerprints
# MAGIC Adds a hash of the tracked columns to every row (`row_hash`), and a hash of the master columns (`master_hash`). A different hash means the row changed. The key columns are not part of the hash.

# COMMAND ----------

def hash_of(columns):
    parts = [F.coalesce(F.col(c).cast("string"), F.lit("<NULL>")) for c in columns]
    return F.sha2(F.concat_ws("||", *parts), 256)

def add_hashes(df):
    df = df.withColumn("row_hash", hash_of(S.tracked))
    if S.master:
        df = df.withColumn("master_hash", hash_of(S.master))
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5 - Find the changes (CDC)
# MAGIC Compares the new file with the current table and labels each record: `INSERT` (new, or back after a delete), `UPDATE` (a tracked column changed), `DELETE` (missing from a full snapshot file). Unchanged records are left out, which is what makes the load incremental. For measure columns the old value, new value and difference are kept.

# COMMAND ----------

def detect_changes(src, batch_id, batch_ts):
    cur = spark.table(T["current"]).select(
        *S.keys,
        *[F.col(c).alias(f"_t_{c}") for c in S.tracked],
        F.col("row_hash").alias("_t_hash"),
        F.col("is_deleted").alias("_t_deleted"),
    )
    joined = src.join(cur, S.keys, "full_outer")

    present = F.col("row_hash").isNotNull()
    change_type = (
        F.when(present & (F.col("_t_hash").isNull() | F.col("_t_deleted")), "INSERT")
         .when(present & (F.col("row_hash") != F.col("_t_hash")), "UPDATE")
    )
    if S.snapshot_type == "full":
        change_type = change_type.when(~present & ~F.col("_t_deleted"), "DELETE")

    typed = joined.withColumn("change_type", change_type).where(F.col("change_type").isNotNull())

    was_active = F.col("_t_deleted") == False
    columns = [
        F.lit(batch_id).alias("batch_id"),
        F.lit(batch_ts).cast("timestamp").alias("change_ts"),
        *[F.col(k) for k in S.keys],
        *[F.when(present, F.col(c)).otherwise(F.col(f"_t_{c}")).alias(c) for c in S.master],
        "change_type",
    ]
    for m in S.measures:
        old = F.when(was_active, F.col(f"_t_{m}"))
        columns += [old.alias(f"old_{m}"), F.col(m).alias(f"new_{m}")]
        if m in S.numeric_measures:
            delta = F.coalesce(F.col(m), F.lit(0)) - F.coalesce(old, F.lit(0))
            columns.append(delta.cast(S.col_types[m]).alias(f"{m}_delta"))
    columns.append(F.coalesce("row_hash", "_t_hash").alias("row_hash"))
    return typed.select(*columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 6 - Update the current table
# MAGIC Applies only the changed records to the current table with one MERGE. New records are added, changed records are updated, and missing records are marked `is_deleted` (soft delete, nothing is removed).

# COMMAND ----------

LOG_COLUMNS = ["batch_id", "change_ts", *S.keys, *S.master, "change_type"]
for m in S.measures:
    LOG_COLUMNS += [f"old_{m}", f"new_{m}"]
    if m in S.numeric_measures:
        LOG_COLUMNS.append(f"{m}_delta")

def merge_current(changes):
    on = " AND ".join(f"t.{k} = s.{k}" for k in S.keys)
    new_values = {c: f"s.{c}" for c in S.master}
    new_values.update({m: f"s.new_{m}" for m in S.measures})
    new_values.update({
        "row_hash": "s.row_hash",
        "is_deleted": "false",
        "updated_at": "s.change_ts",
        "last_batch_id": "s.batch_id",
    })
    insert_values = {k: f"s.{k}" for k in S.keys}
    insert_values.update(new_values)
    delete_values = {"is_deleted": "true", "updated_at": "s.change_ts", "last_batch_id": "s.batch_id"}

    merge = (
        DeltaTable.forName(spark, T["current"]).alias("t")
        .merge(changes.alias("s"), on)
        .whenMatchedUpdate(condition="s.change_type IN ('INSERT', 'UPDATE')", set=new_values)
        .whenMatchedUpdate(condition="s.change_type = 'DELETE'", set=delete_values)
        .whenNotMatchedInsert(condition="s.change_type = 'INSERT'", values=insert_values)
    )
    merge.execute()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 7 - Keep history (SCD Type 2)
# MAGIC The master columns are kept in the history table with their versions. When a master column changes, the old row is closed (`effective_to` is set, `is_current` becomes false) and a new current row is added. A change in a measure column does not create a new version. If the config has no master columns, this step is skipped.

# COMMAND ----------

def dim_versions(src):
    cur = (
        spark.table(T["dimension"]).where("is_current")
        .select(*S.keys, F.col("master_hash").alias("_t_master_hash"))
    )
    return (
        src.join(cur, S.keys, "left")
        .where(F.col("_t_master_hash").isNull() | (F.col("_t_master_hash") != F.col("master_hash")))
        .select(*S.keys, *S.master, "master_hash", "_t_master_hash")
    )

def merge_dim(src, batch_ts):
    versions = dim_versions(src)
    cols = [*S.keys, *S.master, "master_hash"]
    to_close = versions.where(F.col("_t_master_hash").isNotNull()).select(F.lit(True).alias("_close"), *cols)
    to_add = versions.select(F.lit(False).alias("_close"), *cols)
    stage = to_close.unionByName(to_add)

    on = " AND ".join(f"t.{k} = s.{k}" for k in S.keys) + " AND t.is_current = true AND s._close = true"
    insert_values = {c: f"s.{c}" for c in cols}
    insert_values.update({
        "effective_from": F.lit(batch_ts).cast("timestamp"),
        "is_current": "true",
    })
    (
        DeltaTable.forName(spark, T["dimension"]).alias("t")
        .merge(stage.alias("s"), on)
        .whenMatchedUpdate(set={"is_current": "false", "effective_to": F.lit(batch_ts).cast("timestamp")})
        .whenNotMatchedInsert(condition="s._close = false", values=insert_values)
        .execute()
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 8 - Batch log
# MAGIC Writes one row per file to the batch log table with the counts and the status. A batch that is already `SUCCESS` is skipped if it is run again.

# COMMAND ----------

def log_batch(batch_id, file_name, batch_ts, rows, counts, dim_new, dim_changed, status):
    ins, upd, dele = counts.get("INSERT", 0), counts.get("UPDATE", 0), counts.get("DELETE", 0)
    row = [(batch_id, file_name, batch_ts, rows, ins, upd, dele, max(rows - ins - upd, 0),
            dim_new, dim_changed, status, datetime.now())]
    spark.createDataFrame(row, BATCH_SCHEMA).write.mode("append").saveAsTable(T["batch_log"])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 9 - Process one batch
# MAGIC Runs the whole flow for one file: read and check the file, copy it to the raw table, find the changes, save them to the change log, update the current table, update the history table, and write the batch log.

# COMMAND ----------

def process_batch(batch_id, file_name, batch_ts):
    already = (
        spark.table(T["batch_log"])
        .where((F.col("batch_id") == batch_id) & (F.col("status") == "SUCCESS"))
        .count()
    )
    if already:
        print(f"{batch_id} ({file_name}): already loaded, skipped")
        return
    try:
        src = add_hashes(read_source(spark, S, file_name))
        rows = src.count()
        (
            src.select(*S.column_names)
            .withColumn("batch_id", F.lit(batch_id))
            .withColumn("source_file", F.lit(file_name))
            .withColumn("ingested_at", F.current_timestamp())
            .write.mode("append").saveAsTable(T["bronze"])
        )
        changes = detect_changes(src, batch_id, batch_ts)
        counts = {r["change_type"]: r["count"] for r in changes.groupBy("change_type").count().collect()}
        changes.select(*LOG_COLUMNS).write.mode("append").saveAsTable(T["change_log"])
        merge_current(changes)
        dim_new, dim_changed = 0, 0
        if S.master:
            versions = dim_versions(src)
            dim_new = versions.where(F.col("_t_master_hash").isNull()).count()
            dim_changed = versions.where(F.col("_t_master_hash").isNotNull()).count()
            merge_dim(src, batch_ts)
        log_batch(batch_id, file_name, batch_ts, rows, counts, dim_new, dim_changed, "SUCCESS")
        print(f"{batch_id} ({file_name}): rows={rows}, changes={counts if counts else 'none'}, "
              f"history versions added={dim_new + dim_changed}")
    except Exception:
        log_batch(batch_id, file_name, batch_ts, 0, {}, 0, 0, "FAILED")
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 10 - Load all files in order
# MAGIC Goes through the files listed in `batches` in `config/sync_config.json` and loads each one.

# COMMAND ----------

for b in S.batches:
    process_batch(b["batch_id"], b["file"], datetime.fromisoformat(b["batch_date"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 11 - Run a file again
# MAGIC Loads the last file a second time. It is skipped because it is already logged as `SUCCESS`, so nothing is loaded twice.

# COMMAND ----------

last = S.batches[-1]
process_batch(last["batch_id"], last["file"], datetime.fromisoformat(last["batch_date"]))