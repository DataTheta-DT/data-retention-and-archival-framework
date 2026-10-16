# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 01 Retention Pipeline
# MAGIC For every run date and every dataset in the settings file, it checks each record against the retention rules, sets its status to `ACTIVE`, `HOLD` or `ARCHIVED`, and writes the result to the logs.
# MAGIC With `dry_run` set to true it only writes the plan and changes no data.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load the settings
# MAGIC Reads `config/retention_config.json` from the project folder.

# COMMAND ----------

import os
import json
from datetime import date

PROJECT_ROOT = os.path.dirname(os.getcwd())

with open(os.path.join(PROJECT_ROOT, "config", "retention_config.json")) as f:
    CONFIG = json.load(f)

CATALOG = CONFIG["catalog"]
SCHEMA = CONFIG["schema"]
DATASETS = CONFIG["datasets"]
DRY_RUN = CONFIG["dry_run"]
MODE = "DRY_RUN" if DRY_RUN else "LIVE"
RUN_DATES = [date.today().isoformat() if d == "today" else d for d in CONFIG["run_dates"]]
RUN_LOG = f"{CATALOG}.{SCHEMA}.ctl_retention_run_log"


def table_name(name):
    return f"{CATALOG}.{SCHEMA}.{name}"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Turn the rules into SQL
# MAGIC Each rule becomes a condition on the columns of the record. The first rule that matches a record decides its limits. A rule marked `hold` protects the record. A record that matches no rule is kept.

# COMMAND ----------

def literal(value, column_type):
    if column_type == "boolean":
        return "true" if value else "false"
    if column_type == "int" or column_type == "bigint":
        return str(int(value))
    if column_type == "double":
        return str(float(value))
    return "'" + str(value).replace("'", "''") + "'"


def condition_sql(condition, column_types):
    column = f"`{condition['column']}`"
    column_type = column_types[condition["column"]]
    operator = condition["operator"]
    if operator == "is_null":
        return f"{column} IS NULL"
    if operator == "is_not_null":
        return f"{column} IS NOT NULL"
    if operator in ("in", "not_in"):
        items = ", ".join(literal(v, column_type) for v in condition["value"])
        return f"{column} {'NOT ' if operator == 'not_in' else ''}IN ({items})"
    symbols = {"equals": "=", "not_equals": "<>", "gt": ">", "gte": ">=", "lt": "<", "lte": "<="}
    return f"{column} {symbols[operator]} {literal(condition['value'], column_type)}"


def rule_case(dataset, field):
    column_types = {c["name"]: c["type"] for c in dataset["columns"]}
    branches = []
    for rule in dataset["rules"]:
        test = " AND ".join(condition_sql(c, column_types) for c in rule.get("when", [])) or "true"
        if field == "rule_name":
            result = literal(rule["rule_name"], "string")
        elif field == "is_hold":
            result = "true" if rule.get("hold") else "false"
        else:
            result = "NULL" if rule.get("hold") else str(int(rule[field]))
        branches.append(f"WHEN {test} THEN {result}")
    fallback = {"rule_name": "'no_matching_rule'", "is_hold": "false"}.get(field, "NULL")
    return "(CASE " + " ".join(branches) + f" ELSE {fallback} END)"


def key_sql(dataset):
    parts = ", ".join(f"CAST(`{k}` AS STRING)" for k in dataset["key_columns"])
    return f"concat_ws('|', {parts})"


def age_sql(dataset, run_date):
    return f"datediff(DATE'{run_date}', `{dataset['date_column']}`)"


def predicates(dataset, run_date):
    hold = rule_case(dataset, "is_hold")
    archive_days = rule_case(dataset, "archive_after_days")
    age = age_sql(dataset, run_date)
    return {
        "hold": hold,
        "archive": f"(NOT {hold} AND {archive_days} IS NOT NULL AND {age} > {archive_days})",
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run one dataset for one run date
# MAGIC First it counts and logs the decision for every record that is not yet archived. Then it updates the status of those records: old records become `ARCHIVED`, records matching a hold rule become `HOLD`, and the rest stay `ACTIVE`. A record that is already `ARCHIVED` is not changed again, so a run is safe to repeat.

# COMMAND ----------

def count_where(table, predicate):
    return spark.sql(f"SELECT count(*) FROM {table} WHERE {predicate}").first()[0]


def log_decisions(dataset, run_date, run_id, preds, active):
    key, age, rule = key_sql(dataset), age_sql(dataset, run_date), rule_case(dataset, "rule_name")
    for decision in ["HOLD", "ARCHIVE"]:
        predicate = preds[decision.lower()]
        spark.sql(
            f"""INSERT INTO {table_name(dataset['dataset_name'] + '_decision_log')}
                SELECT '{run_id}', DATE'{run_date}', {key}, {age}, {rule}, '{decision}', '{MODE}'
                FROM {active} WHERE {predicate}"""
        )


def run_dataset(dataset, run_date, run_id, counts):
    name = dataset["dataset_name"]
    records = table_name(f"{name}_records")
    active, archive = table_name(f"{name}_active"), table_name(f"{name}_archive")
    preds = predicates(dataset, run_date)

    counts["active_before"] = spark.table(active).count()
    counts["archive_before"] = spark.table(archive).count()
    counts["archived"] = count_where(active, preds["archive"])
    counts["held"] = count_where(active, preds["hold"])

    log_decisions(dataset, run_date, run_id, preds, active)

    if not DRY_RUN:
        rule = rule_case(dataset, "rule_name")
        spark.sql(
            f"""UPDATE {records}
                SET record_status = CASE WHEN {preds['archive']} THEN 'ARCHIVED'
                                         WHEN {preds['hold']} THEN 'HOLD' ELSE 'ACTIVE' END,
                    archived_on = CASE WHEN {preds['archive']} THEN DATE'{run_date}' ELSE archived_on END,
                    archive_run_id = CASE WHEN {preds['archive']} THEN '{run_id}' ELSE archive_run_id END,
                    archive_rule = CASE WHEN {preds['archive']} THEN {rule} ELSE archive_rule END
                WHERE record_status <> 'ARCHIVED'"""
        )

    counts["active_after"] = spark.table(active).count()
    counts["archive_after"] = spark.table(archive).count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write the run log
# MAGIC Adds one row to the run log for each dataset and run date, with the counts and the status. A failed run is logged with its error message.

# COMMAND ----------

def number_sql(value):
    return "NULL" if value is None else str(int(value))


def write_run_log(run_id, run_date, name, counts, status, message):
    text = str(message).replace("'", "''")[:500]
    fields = ["active_before", "archive_before", "archived", "held", "active_after", "archive_after"]
    numbers = ", ".join(number_sql(counts.get(f)) for f in fields)
    spark.sql(
        f"""INSERT INTO {RUN_LOG} VALUES
            ('{run_id}', DATE'{run_date}', '{name}', '{MODE}', {numbers}, '{status}', '{text}', current_timestamp())"""
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run all run dates
# MAGIC Goes through the run dates in order, and through every dataset for each date. Each run date gets the next run id. If a dataset fails, the failure is logged and the notebook stops.

# COMMAND ----------

last_id = spark.sql(f"SELECT coalesce(max(cast(substr(run_id, 2) AS INT)), 0) FROM {RUN_LOG}").first()[0]

for run_date in RUN_DATES:
    last_id += 1
    run_id = f"R{last_id:03d}"
    for dataset in DATASETS:
        counts = {}
        try:
            run_dataset(dataset, run_date, run_id, counts)
        except Exception as error:
            write_run_log(run_id, run_date, dataset["dataset_name"], counts, "FAILED", error)
            raise
        write_run_log(run_id, run_date, dataset["dataset_name"], counts, "SUCCESS", "")
        print(f"{run_id} {run_date} {dataset['dataset_name']} [{MODE}] "
              f"archived={counts['archived']} held={counts['held']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Show the run log
# MAGIC Shows the counts for every run. Next, run `02_Results`.

# COMMAND ----------

display(spark.table(RUN_LOG).orderBy("run_id", "dataset_name"))