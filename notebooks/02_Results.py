# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 02 Results
# MAGIC Shows the tables after the run and ends with three checks: no rows were lost, held records were not archived, and every run succeeded.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load the settings
# MAGIC Reads `config/retention_config.json` from the project folder.

# COMMAND ----------

import os
import json
import pandas as pd

PROJECT_ROOT = os.path.dirname(os.getcwd())

with open(os.path.join(PROJECT_ROOT, "config", "retention_config.json")) as f:
    CONFIG = json.load(f)

CATALOG = CONFIG["catalog"]
SCHEMA = CONFIG["schema"]
DATASETS = CONFIG["datasets"]
RUN_LOG = f"{CATALOG}.{SCHEMA}.ctl_retention_run_log"


def table_name(name):
    return f"{CATALOG}.{SCHEMA}.{name}"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run log
# MAGIC One row for each dataset and run date, with the number of records archived and held, and the table sizes before and after.

# COMMAND ----------

display(spark.table(RUN_LOG).orderBy("run_id", "dataset_name"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Decisions by rule
# MAGIC How many records each rule archived or held in every run. Records that were simply kept are not logged.

# COMMAND ----------

for dataset in DATASETS:
    name = dataset["dataset_name"]
    print(name)
    display(
        spark.sql(
            f"""SELECT run_id, run_date, mode, rule_name, decision, count(*) AS records
                FROM {table_name(name + '_decision_log')}
                GROUP BY run_id, run_date, mode, rule_name, decision
                ORDER BY run_id, decision, rule_name"""
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Archive table
# MAGIC The records with status `ARCHIVED`, with the date, run and rule that archived them.

# COMMAND ----------

for dataset in DATASETS:
    name = dataset["dataset_name"]
    print(name)
    display(spark.table(table_name(f"{name}_archive")).orderBy("archived_on", *dataset["key_columns"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Checks
# MAGIC 1. The records in the active view and the archive view add up to the records in the source file, and no record is in both views.
# MAGIC 2. No record was archived by a hold rule.
# MAGIC 3. Every run in the run log has the status SUCCESS.

# COMMAND ----------

def scalar(sql):
    return spark.sql(sql).first()[0]


checks = []

for dataset in DATASETS:
    name = dataset["dataset_name"]
    active = table_name(f"{name}_active")
    archive = table_name(f"{name}_archive")

    loaded = len(pd.read_csv(os.path.join(PROJECT_ROOT, dataset["source_file"]), dtype=str, keep_default_na=False))
    n_active = spark.table(active).count()
    n_archive = spark.table(archive).count()
    same_key = " AND ".join(f"a.`{k}` = r.`{k}`" for k in dataset["key_columns"])
    in_both = scalar(f"SELECT count(*) FROM {active} a JOIN {archive} r ON {same_key}")
    ok = n_active + n_archive == loaded and in_both == 0
    checks.append((
        name, "1. No rows lost", "PASS" if ok else "FAIL",
        f"source {loaded}, active {n_active}, archive {n_archive}, in both views {in_both}",
    ))

    hold_rules = [r["rule_name"] for r in dataset["rules"] if r.get("hold")]
    if hold_rules:
        names = ", ".join("'" + r.replace("'", "''") + "'" for r in hold_rules)
        moved = scalar(f"SELECT count(*) FROM {archive} WHERE archive_rule IN ({names})")
        checks.append((
            name, "2. Held records untouched", "PASS" if moved == 0 else "FAIL",
            f"archived by hold rules {moved}",
        ))
    else:
        checks.append((name, "2. Held records untouched", "PASS", "no hold rules in the settings"))

total_runs = scalar(f"SELECT count(*) FROM {RUN_LOG}")
failed_runs = scalar(f"SELECT count(*) FROM {RUN_LOG} WHERE status <> 'SUCCESS'")
checks.append((
    "all datasets", "3. All runs succeeded", "PASS" if total_runs > 0 and failed_runs == 0 else "FAIL",
    f"runs {total_runs}, failed {failed_runs}",
))

display(spark.createDataFrame(checks, "dataset string, check string, result string, detail string"))