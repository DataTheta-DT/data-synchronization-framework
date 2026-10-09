import os
import re
import json
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

NUMERIC_TYPE = re.compile(r"^(tinyint|smallint|int|integer|bigint|long|float|double|decimal(\(\s*\d+\s*,\s*\d+\s*\))?)$")
OTHER_TYPE = re.compile(r"^(string|boolean|date|timestamp)$")
NAME_PATTERN = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")

RESERVED = {
    "row_hash", "master_hash", "is_deleted", "updated_at", "last_batch_id",
    "change_type", "batch_id", "change_ts", "source_file", "ingested_at",
    "effective_from", "effective_to", "is_current",
}

DEFAULT_TABLES = {
    "bronze": "bronze_{d}_raw",
    "current": "{d}_current",
    "change_log": "{d}_change_log",
    "dimension": "dim_{d}",
    "batch_log": "ctl_{d}_batch_log",
}


class Settings:
    def __init__(self, spark, base_dir, config_file="sync_config.json"):
        self.base_dir = base_dir
        self.config_path = os.path.join(base_dir, "config", config_file)
        with open(self.config_path) as f:
            self.cfg = json.load(f)
        self._validate()

        cfg = self.cfg
        self.dataset = cfg["dataset_name"]
        self.reset_demo = bool(cfg.get("reset_demo", False))
        self.columns = [(c["name"], c["type"].lower().replace(" ", "")) for c in cfg["columns"]]
        self.column_names = [c for c, _ in self.columns]
        self.col_types = dict(self.columns)
        self.keys = cfg["business_keys"]
        self.master = cfg.get("master_columns", [])
        self.measures = cfg.get("measure_columns", [])
        self.tracked = self.master + self.measures
        self.numeric_measures = [m for m in self.measures if NUMERIC_TYPE.match(self.col_types[m])]
        self.not_null = cfg.get("rules", {}).get("not_null", [])
        self.min_values = cfg.get("rules", {}).get("min_values", {})
        self.snapshot_type = cfg.get("source", {}).get("snapshot_type", "full")
        self.delimiter = cfg.get("source", {}).get("delimiter", ",")
        self.data_dir = os.path.join(base_dir, cfg.get("source", {}).get("folder", "data"))
        self.batches = cfg["batches"]

        catalog = cfg.get("catalog") or spark.sql("SELECT current_catalog()").first()[0]
        self.prefix = f"`{catalog}`.`{cfg['schema']}`"
        names = {k: v.format(d=self.dataset) for k, v in DEFAULT_TABLES.items()}
        names.update(cfg.get("tables", {}))
        self.tables = {k: f"{self.prefix}.{v}" for k, v in names.items()}

    def _fail(self, message):
        raise ValueError(f"sync_config.json: {message}")

    def _validate(self):
        cfg = self.cfg
        for key in ["dataset_name", "schema", "columns", "business_keys", "batches"]:
            if key not in cfg:
                self._fail(f"missing setting '{key}'")
        if not NAME_PATTERN.match(cfg["dataset_name"]):
            self._fail("dataset_name must use letters, numbers and underscores only")

        names, types = [], {}
        for c in cfg["columns"]:
            if "name" not in c or "type" not in c:
                self._fail("every column needs a 'name' and a 'type'")
            name, ctype = c["name"], c["type"].lower().replace(" ", "")
            if not NAME_PATTERN.match(name):
                self._fail(f"column name '{name}' must start with a letter and use letters, numbers and underscores only")
            if name in RESERVED:
                self._fail(f"column name '{name}' is reserved, please rename it")
            if not (NUMERIC_TYPE.match(ctype) or OTHER_TYPE.match(ctype)):
                self._fail(f"column '{name}' has unsupported type '{c['type']}'")
            if name in names:
                self._fail(f"column '{name}' is listed twice")
            names.append(name)
            types[name] = ctype

        keys = cfg["business_keys"]
        master = cfg.get("master_columns", [])
        measures = cfg.get("measure_columns", [])
        if not keys:
            self._fail("business_keys cannot be empty")
        for group, label in [(keys, "business_keys"), (master, "master_columns"), (measures, "measure_columns")]:
            for c in group:
                if c not in names:
                    self._fail(f"{label} lists '{c}', which is not in columns")
        listed = keys + master + measures
        for c in listed:
            if listed.count(c) > 1:
                self._fail(f"column '{c}' is listed in more than one of business_keys, master_columns, measure_columns")
        for c in names:
            if c not in listed:
                self._fail(f"column '{c}' must be listed in business_keys, master_columns or measure_columns")

        rules = cfg.get("rules", {})
        for c in rules.get("not_null", []):
            if c not in names:
                self._fail(f"rules.not_null lists '{c}', which is not in columns")
        for c in rules.get("min_values", {}):
            if c not in names or not NUMERIC_TYPE.match(types[c]):
                self._fail(f"rules.min_values needs a numeric column, but '{c}' is not one")

        source = cfg.get("source", {})
        if source.get("format", "csv") != "csv":
            self._fail("source.format must be 'csv'")
        if source.get("snapshot_type", "full") not in ("full", "incremental"):
            self._fail("source.snapshot_type must be 'full' or 'incremental'")

        tables = cfg.get("tables", {})
        for t in tables:
            if t not in DEFAULT_TABLES:
                self._fail(f"tables.{t} is not a valid table setting. Use: {', '.join(DEFAULT_TABLES)}")

        if not cfg["batches"]:
            self._fail("batches cannot be empty")
        seen = set()
        for b in cfg["batches"]:
            for key in ["batch_id", "file", "batch_date"]:
                if key not in b:
                    self._fail(f"every batch needs '{key}'")
            if b["batch_id"] in seen:
                self._fail(f"batch_id '{b['batch_id']}' is used twice")
            seen.add(b["batch_id"])

    def summary(self):
        return (
            f"dataset: {self.dataset} | keys: {self.keys} | master (history): {self.master} | "
            f"measures: {self.measures} | snapshot_type: {self.snapshot_type} | files: {len(self.batches)}"
        )


def read_source(spark, S, file_name):
    path = os.path.join(S.data_dir, file_name)
    pdf = pd.read_csv(path, dtype=str, keep_default_na=False, na_values=[""], sep=S.delimiter)
    pdf.columns = [str(c).strip() for c in pdf.columns]
    missing = [c for c in S.column_names if c not in pdf.columns]
    if missing:
        raise ValueError(f"{file_name}: missing columns {missing}")
    pdf = pdf[S.column_names]
    for c in pdf.columns:
        pdf[c] = pdf[c].map(lambda v: (v.strip() or None) if isinstance(v, str) else None)
    pdf = pdf.astype(object).where(pdf.notna(), None)

    for c in S.not_null:
        if pdf[c].isna().any():
            raise ValueError(f"{file_name}: column '{c}' has empty values")
    for c in S.keys:
        if pdf[c].isna().any():
            raise ValueError(f"{file_name}: business key '{c}' has empty values")
    if pdf.duplicated(subset=S.keys).any():
        raise ValueError(f"{file_name}: business key {S.keys} is repeated")

    raw_schema = StructType([StructField(c, StringType()) for c in S.column_names])
    raw = spark.createDataFrame([tuple(r) for r in pdf.itertuples(index=False, name=None)], raw_schema)
    typed = raw.select(*[F.expr(f"try_cast(`{c}` AS {t})").alias(c) for c, t in S.columns])

    bad = raw.select(*[
        F.sum(F.when(F.col(c).isNotNull() & F.expr(f"try_cast(`{c}` AS {t})").isNull(), 1).otherwise(0)).alias(c)
        for c, t in S.columns
    ]).first()
    bad_columns = [c for c in S.column_names if bad[c]]
    if bad_columns:
        raise ValueError(f"{file_name}: values do not match the column type in {bad_columns}")

    for c, limit in S.min_values.items():
        if typed.where(F.col(c) < limit).count() > 0:
            raise ValueError(f"{file_name}: column '{c}' has values below {limit}")
    return typed
