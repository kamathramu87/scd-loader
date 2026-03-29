# scd-loader

A PySpark library for implementing **Slowly Changing Dimension Type 2 (SCD2)** transformations. It tracks historical changes to dimensional data using valid_from/valid_until date ranges, SHA-256 hash-based change detection, and active/delete flags.

## Installation

```bash
pip install scd-loader
```

## Quick Start

```python
from scd_loader import SCD2Loader, SCD2Columns

loader = SCD2Loader(spark_session=spark)

result_df = loader.slowly_changing_dimension(
    df_src=source_df,
    business_keys=["employee_id"],
    date_column="snapshot_date",
)
```

### Incremental Load

Pass a `df_tgt` to merge new snapshots into an existing SCD2 table:

```python
result_df = loader.slowly_changing_dimension(
    df_src=source_df,
    business_keys=["employee_id"],
    date_column="snapshot_date",
    df_tgt=target_df,
)
```

## API Reference

### `SCD2Loader(spark_session=None)`

| Parameter | Type | Description |
|---|---|---|
| `spark_session` | `SparkSession \| None` | Existing SparkSession. Creates one if not provided. |

### `slowly_changing_dimension(...)`

| Parameter | Type | Default | Description |
|---|---|---|---|
| `df_src` | `DataFrame` | — | Source snapshot DataFrame |
| `business_keys` | `list[str] \| str` | — | Columns that uniquely identify a dimension row |
| `date_column` | `str` | `snapshot_date` | Column containing the snapshot date |
| `df_tgt` | `DataFrame \| None` | `None` | Existing SCD2 target for incremental loads |
| `ignore_columns` | `list[str] \| None` | `None` | Columns excluded from hash-based change detection |
| `non_copy_fields` | `list[str] \| None` | `None` | Source columns excluded from the output |
| `open_end_date` | `datetime \| None` | `9999-12-31` | `valid_until` value for currently active records |
| `scd_columns` | `SCD2Columns \| dict[str, str] \| None` | `None` | Override default SCD2 output column names |

## Output Columns

The resulting DataFrame includes all source columns plus:

| Column | Default Name | Description |
|---|---|---|
| `valid_from` | `valid_from` | Date when the record became active |
| `valid_until` | `valid_until` | Date when the record was superseded (`9999-12-31` if still active) |
| `active_flag` | `active_flag` | `True` for the currently active version of a record |
| `delete_flag` | `delete_flag` | `True` if the record was deleted in the source |
| `row_hash` | `row_hash` | SHA-256 hash of non-key columns (excluding `ignore_columns`) |
| `upsert_flag` | `upsert_flag` | `I` for inserts, `U` for updates |

Column names can be overridden via the `scd_columns` parameter using either `SCD2Columns` (recommended — full IDE type hints) or a plain dict:

```python
from scd_loader import SCD2Loader, SCD2Columns

# Using SCD2Columns (recommended)
result_df = loader.slowly_changing_dimension(
    df_src=source_df,
    business_keys=["employee_id"],
    scd_columns=SCD2Columns(
        valid_from="eff_start_date",
        valid_until="eff_end_date",
        active_flag="is_current",
    ),
)

# Using a dict (also supported)
result_df = loader.slowly_changing_dimension(
    df_src=source_df,
    business_keys=["employee_id"],
    scd_columns={
        "valid_from": "eff_start_date",
        "valid_until": "eff_end_date",
        "active_flag": "is_current",
    },
)
```

## Exceptions

| Exception | Raised When |
|---|---|
| `EmptyDataExceptionError` | Source DataFrame is empty |
| `OldDataExceptionError` | Source snapshot date is older than the latest target date |
| `BusinessKeysEmptyError` | No business keys provided |
| `ConfigurationError` | Invalid configuration values |
| `DataValidationError` | Required columns are missing from the source |
