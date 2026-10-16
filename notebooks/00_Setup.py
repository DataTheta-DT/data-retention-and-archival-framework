# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 00 Setup
# MAGIC Reads and checks the settings file, loads each source file into a records table, and creates the active and archive views, the decision log and the run log.
# MAGIC Run it once. Running it again leaves existing tables unchanged.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load the settings
# MAGIC Reads `config/retention_config.json` from the project folder. This is the only file that changes when you use your own data.

# COMMAND ----------

import os
import re
import json
from datetime import datetime
import pandas as pd
from pyspark.sql import functions as F

PROJECT_ROOT = os.path.dirname(os.getcwd())

with open(os.path.join(PROJECT_ROOT, "config", "retention_config.json")) as f:
    CONFIG = json.load(f)

CATALOG = CONFIG.get("catalog")
SCHEMA = CONFIG.get("schema")
DATASETS = CONFIG.get("datasets") or []


def table_name(name):
    return f"{CATALOG}.{SCHEMA}.{name}"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check the settings
# MAGIC Looks for mistakes in the settings file before anything is created, and lists every problem it finds.

# COMMAND ----------

TYPES = {"string", "int", "bigint", "double", "date", "timestamp", "boolean"}
OPERATORS = {"equals", "not_equals", "in", "not_in", "gt", "gte", "lt", "lte", "is_null", "is_not_null"}


def check_rule(name, rule, position, is_last, columns):
    errors = []
    label = f"{name}: rule {position}"
    if not rule.get("rule_name"):
        errors.append(f"{label} has no rule_name")
    conditions = rule.get("when") or []
    if not conditions and not is_last:
        errors.append(f"{label} has no 'when', so it matches every record and must be the last rule")
    for c in conditions:
        operator = c.get("operator")
        if c.get("column") not in columns:
            errors.append(f"{label}: column '{c.get('column')}' is not in columns")
        if operator not in OPERATORS:
            errors.append(f"{label}: operator '{operator}' is not supported")
        elif operator in ("in", "not_in") and not isinstance(c.get("value"), list):
            errors.append(f"{label}: operator '{operator}' needs a list in 'value'")
        elif operator not in ("is_null", "is_not_null", "in", "not_in") and "value" not in c:
            errors.append(f"{label}: operator '{operator}' needs a 'value'")
    if rule.get("hold"):
        if "archive_after_days" in rule:
            errors.append(f"{label} is a hold rule and must not have day limits")
    else:
        archive_days = rule.get("archive_after_days")
        if not (isinstance(archive_days, int) and not isinstance(archive_days, bool) and archive_days >= 0):
            errors.append(f"{label} needs archive_after_days as a whole number, 0 or more")
    return errors


def check_dataset(dataset):
    name = dataset.get("dataset_name", "?")
    errors = []
    for key in ["dataset_name", "source_file", "columns", "key_columns", "date_column", "rules"]:
        if not dataset.get(key):
            errors.append(f"{name}: '{key}' is missing")
    if errors:
        return errors
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
        errors.append(f"{name}: dataset_name can use only letters, numbers and underscores")
    columns = {c["name"]: c["type"] for c in dataset["columns"]}
    for column, column_type in columns.items():
        if column_type not in TYPES:
            errors.append(f"{name}: column '{column}' has unsupported type '{column_type}'")
    for key_column in dataset["key_columns"]:
        if key_column not in columns:
            errors.append(f"{name}: key column '{key_column}' is not in columns")
    if columns.get(dataset["date_column"]) != "date":
        errors.append(f"{name}: date_column '{dataset['date_column']}' must be a column of type date")
    path = os.path.join(PROJECT_ROOT, dataset["source_file"])
    if not os.path.exists(path):
        errors.append(f"{name}: file '{dataset['source_file']}' was not found")
    elif set(pd.read_csv(path, nrows=0).columns) != set(columns):
        errors.append(f"{name}: the file header does not match the columns in the settings")
    rule_names = [r.get("rule_name") for r in dataset["rules"]]
    if len(set(rule_names)) != len(rule_names):
        errors.append(f"{name}: rule_name values must be unique")
    for position, rule in enumerate(dataset["rules"], start=1):
        errors += check_rule(name, rule, position, position == len(dataset["rules"]), columns)
    return errors


def check_config():
    errors = []
    if not CATALOG:
        errors.append("catalog is missing")
    if not SCHEMA:
        errors.append("schema is missing")
    if not isinstance(CONFIG.get("dry_run"), bool):
        errors.append("dry_run must be true or false")
    run_dates = CONFIG.get("run_dates")
    if not run_dates or not isinstance(run_dates, list):
        errors.append("run_dates must be a list such as [\"today\"]")
    else:
        fixed = []
        for value in run_dates:
            if value == "today":
                continue
            try:
                fixed.append(datetime.strptime(value, "%Y-%m-%d"))
            except (TypeError, ValueError):
                errors.append(f"run_dates: '{value}' is not yyyy-MM-dd or 'today'")
        if fixed != sorted(fixed):
            errors.append("run_dates must be in date order, oldest first")
    if not DATASETS:
        errors.append("datasets is empty")
    names = [d.get("dataset_name") for d in DATASETS]
    if len(set(names)) != len(names):
        errors.append("dataset_name values must be unique")
    for dataset in DATASETS:
        errors += check_dataset(dataset)
    return errors


config_errors = check_config()
if config_errors:
    raise ValueError("Settings errors:\n- " + "\n- ".join(config_errors))
print("Settings are valid")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read and check the source data
# MAGIC Reads each CSV file and converts every column to its type. It stops if a value does not match its type, a date or key is empty, or a key appears twice.

# COMMAND ----------

def read_source(dataset):
    raw = pd.read_csv(os.path.join(PROJECT_ROOT, dataset["source_file"]), dtype=str, keep_default_na=False)
    schema = ", ".join(f"`{c}` string" for c in raw.columns)
    raw_df = spark.createDataFrame(raw, schema=schema)
    typed_df = raw_df.select(
        [F.expr(f"try_cast(nullif(trim(`{c['name']}`), '') AS {c['type']})").alias(c["name"]) for c in dataset["columns"]]
    )
    return raw_df, typed_df


def check_data(dataset, raw_df, typed_df):
    name = dataset["dataset_name"]
    errors = []
    for c in dataset["columns"]:
        given = raw_df.where(F.trim(F.col(c["name"])) != "").count()
        converted = typed_df.where(F.col(c["name"]).isNotNull()).count()
        if given != converted:
            errors.append(f"{name}: {given - converted} value(s) in '{c['name']}' do not match type {c['type']}")
    if typed_df.where(F.col(dataset["date_column"]).isNull()).count():
        errors.append(f"{name}: '{dataset['date_column']}' has empty values, so those records cannot be aged")
    key_missing = " OR ".join(f"`{k}` IS NULL" for k in dataset["key_columns"])
    if typed_df.where(key_missing).count():
        errors.append(f"{name}: key columns have empty values")
    duplicates = typed_df.groupBy(*dataset["key_columns"]).count().where("count > 1").count()
    if duplicates:
        errors.append(f"{name}: {duplicates} key value(s) appear more than once")
    if typed_df.count() == 0:
        errors.append(f"{name}: the file has no rows")
    return errors


LOADED = {}
data_errors = []
for dataset in DATASETS:
    raw_df, typed_df = read_source(dataset)
    data_errors += check_data(dataset, raw_df, typed_df)
    LOADED[dataset["dataset_name"]] = typed_df

if data_errors:
    raise ValueError("Data errors:\n- " + "\n- ".join(data_errors))
print("Source data is valid")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create the schema and the tables
# MAGIC For each dataset it creates a records table from the source file, where every record has a status: `ACTIVE`, `HOLD` or `ARCHIVED`. It then creates two views on top of it: the active view (records with status `ACTIVE` or `HOLD`) and the archive view (records with status `ARCHIVED`). It also creates an empty decision log and the run log shared by all datasets.
# MAGIC Tables that already exist are left unchanged.

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")


def check_layout(name):
    """Stops if a table from an earlier version of the framework uses the same name as a view."""
    if spark.catalog.tableExists(name) and spark.catalog.getTable(name).tableType != "VIEW":
        raise ValueError(f"{name} is a table from an earlier version. Set a new 'schema' in the settings file and run again.")


for dataset in DATASETS:
    name = dataset["dataset_name"]
    typed_df = LOADED[name]
    records = table_name(f"{name}_records")
    active_view, archive_view = table_name(f"{name}_active"), table_name(f"{name}_archive")
    check_layout(active_view)
    check_layout(archive_view)

    if not spark.catalog.tableExists(records):
        records_df = (
            typed_df
            .withColumn("record_status", F.lit("ACTIVE"))
            .withColumn("archived_on", F.lit(None).cast("date"))
            .withColumn("archive_run_id", F.lit(None).cast("string"))
            .withColumn("archive_rule", F.lit(None).cast("string"))
        )
        records_df.write.saveAsTable(records)

    source_columns = ", ".join(f"`{c['name']}`" for c in dataset["columns"])
    spark.sql(
        f"""CREATE OR REPLACE VIEW {active_view} AS
            SELECT {source_columns}, record_status FROM {records}
            WHERE record_status IN ('ACTIVE', 'HOLD')"""
    )
    spark.sql(
        f"""CREATE OR REPLACE VIEW {archive_view} AS
            SELECT {source_columns}, archived_on, archive_run_id, archive_rule FROM {records}
            WHERE record_status = 'ARCHIVED'"""
    )

    spark.sql(
        f"""CREATE TABLE IF NOT EXISTS {table_name(f'{name}_decision_log')} (
            run_id STRING, run_date DATE, record_key STRING, age_days INT,
            rule_name STRING, decision STRING, mode STRING)"""
    )

spark.sql(
    f"""CREATE TABLE IF NOT EXISTS {table_name('ctl_retention_run_log')} (
        run_id STRING, run_date DATE, dataset_name STRING, mode STRING,
        active_before BIGINT, archive_before BIGINT, archived BIGINT, held BIGINT,
        active_after BIGINT, archive_after BIGINT, status STRING, message STRING, logged_at TIMESTAMP)"""
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Show what was created
# MAGIC Lists the records in each active view. Next, run `01_Retention_Pipeline`.

# COMMAND ----------

summary = [
    (d["dataset_name"], table_name(f"{d['dataset_name']}_active"), spark.table(table_name(f"{d['dataset_name']}_active")).count())
    for d in DATASETS
]
display(spark.createDataFrame(summary, "dataset string, active_table string, rows_loaded long"))