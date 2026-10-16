# Data Retention and Archival Framework

A reusable Databricks framework that applies configurable retention rules to keep active data, archive older records, and hold records that must be retained.

The framework uses config/retention_config.json to define retention settings, allowing rules to be updated without changing the main pipeline code.

## Features

- Reads retention rules from a configuration file.
- Classifies records as `ACTIVE`, `HOLD` or `ARCHIVED`
- Applies configurable hold rules to protect records from archival.
- Archives eligible records by updating their status and recording archival details.
- Maintains decision logs and pipeline execution details.
- Displays archived records, active records, and pipeline run results.

## Notebooks

| Notebook | Description |
|---|---|
| `00_Setup.py` | Validates the configuration, creates the schema, tables, and views, and loads sample data when the records table does not already exist. |
| `01_Retention_Pipeline.py` | Applies retention rules, archives eligible records, and records run details. |
| `02_Results.py` | Displays output tables and pipeline run information. |

## Record Decisions

| Decision | Description |
|---|---|
| `ARCHIVE` | Marks an eligible record as `ARCHIVED`, so it appears in the archive table. |
| `HOLD` | Protects a record matching a configured hold rule from archival. |
| `KEEP` | Leaves a record `ACTIVE` because it is not yet eligible for archiving. |

Record age is calculated using the date column specified in the configuration, such as `order_date`.

## Output Tables

The framework uses the following tables and views:

- `<dataset>_records` — All records with a status of `ACTIVE`, `HOLD` or `ARCHIVED`, plus archive date, run ID and rule.
- `<dataset>_active` — View of records that remain active, including records kept by hold rules.
- `<dataset>_archive` — View of archived records, including archive date and run ID.
- `ctl_retention_run_log` — Pipeline run details, including record counts, mode, and status.

`<dataset>` refers to the dataset name used by the configuration. Actual table names depend on the configured catalog, schema, and dataset settings.

## Configuration

Update `config/retention_config.json` to set the retention rules for your dataset.

Configure the date column used to calculate record age, the archive limit, and rules that determine which records should be kept, archived, or held. The setup notebook checks the configuration for errors, such as a hold rule that has day limits.

Use `yyyy-MM-dd` for dates. Set a fixed `as_of_date` for repeatable results, or use `"today"` to use the current date.

## Setup and Execution

1. Clone the repository into Databricks Repos.
2. Review `config/retention_config.json` and update the settings for your dataset.
3. Place the input CSV files in the configured data folder and ensure their columns match the configuration.
4. Run `notebooks/00_Setup.py`.
5. Run `notebooks/01_Retention_Pipeline.py`.
6. Run `notebooks/02_Results.py` to review the output.

## Prerequisites

- Databricks workspace
- Permission to create a schema and tables in the catalog
- Delta Lake support
- CSV files with columns matching the configuration

## Sources

| Library | License | Source |
|---|---|---|
| Apache Spark / PySpark | Apache License 2.0 | https://github.com/apache/spark |
| Delta Lake | Apache License 2.0 | https://github.com/delta-io/delta |
