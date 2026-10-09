
# Real-Time Inventory & Master Data Sync

A reusable Databricks framework that synchronizes inventory and master data using **Change Data Capture (CDC), incremental loading, and SCD Type 2**.

The framework uses a configuration file to define the dataset, columns, validation rules, and input files, allowing the same code to work with different datasets.

## Features

- Loads raw data into Bronze tables.
- Detects inserted, updated, and deleted records using row hashes.
- Updates only changed records in the current table.
- Maintains a change log with old and new values.
- Tracks master data history using SCD Type 2.

## Notebooks

| Notebook | Description |
|---|---|
| `00_Setup.py` | Validates the configuration and creates the required tables. |
| `01_Sync_Pipeline.py` | Loads data, detects changes, updates tables, and logs batches. |
| `02_Results.py` | Displays results and validates the synchronization output. |

Shared configuration and validation functions are maintained in `src/sync_common.py`.

## Change Types

| Change | Description |
|---|---|
| `INSERT` | A new record is added. |
| `UPDATE` | An existing record changes. |
| `DELETE` | A record missing from a full snapshot is marked as deleted. |

Unchanged records are skipped. SCD Type 2 maintains historical versions of master data when master columns change.

## Output Tables

The framework creates the following tables:

- `bronze_<dataset>_raw` — Raw input data.
- `<dataset>_current` — Latest record values.
- `<dataset>_change_log` — Detected changes.
- `dim_<dataset>` — Historical master data.
- `ctl_<dataset>_batch_log` — Batch processing status.

Table names can be customized in the configuration. The inventory example uses `dim_product` for its history table.

## Configuration

Update `config/sync_config.json` to define:

| Setting | Description |
|---|---|
| `dataset_name` | Dataset name used for default table naming. |
| `catalog` and `schema` | Location of the output tables. |
| `source.folder` | Folder containing input CSV files. |
| `source.snapshot_type` | `full` or `incremental` loading mode. |
| `columns` | Column names and data types. |
| `business_keys` | Columns that uniquely identify records. |
| `master_columns` | Columns tracked using SCD Type 2. |
| `measure_columns` | Frequently changing values, such as quantity or price. |
| `rules` | Data validation rules. |
| `tables` | Optional custom table names. |

## Setup and Execution

1. Clone the repository into Databricks Repos.
2. Configure the dataset and input files in `config/sync_config.json`.
3. Run `notebooks/00_Setup.py`.
4. Run `notebooks/01_Sync_Pipeline.py`.
5. Run `notebooks/02_Results.py` to review the results.

To use another dataset, update the configuration with its columns, business keys, validation rules, and input files.


## Prerequisites

- Databricks workspace
- Permission to create a schema and tables in the catalog
- Delta Lake support
- CSV files that match the `columns` in the config

## Sources

| Library | License | Source |
|---|---|---|
| Apache Spark / PySpark | Apache License 2.0 | https://github.com/apache/spark |
| Delta Lake | Apache License 2.0 | https://github.com/delta-io/delta |
| pandas | BSD 3-Clause License | https://github.com/pandas-dev/pandas |
