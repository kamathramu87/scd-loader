# loadx

A PySpark library for **Slowly Changing Dimension Type 2 (SCD2)** transformations.

Tracks historical changes to dimensional data using valid/until date ranges, hash-based change detection, and active/delete flags.

## Installation

```bash
pip install loadx
```

## Quick start

```python
from loadx import SCD2Loader

result_df = SCD2Loader.slowly_changing_dimension(
    source_df=df,
    business_keys=["employee_id"],
    snapshot_date="2024-01-01",
)
```

### Incremental load

```python
result_df = SCD2Loader.slowly_changing_dimension(
    source_df=new_df,
    business_keys=["employee_id"],
    snapshot_date="2024-02-01",
    target_df=existing_df,         # merge with existing history
    ignore_columns=["updated_at"],  # exclude from change detection
)
```

## Output columns

| Column | Description |
|--------|-------------|
| `valid_from` | Date the record became active |
| `valid_until` | Date the record was superseded (`9999-12-31` = currently active) |
| `active_flag` | `True` for the current version of a record |
| `delete_flag` | `True` for records deleted from the source |
| `latest_record_flag` | `True` for the most recent record per key |
| `upsert_flag` | `I` (insert) or `U` (update) for downstream merge operations |

## Documentation

Full documentation at [kamathramu87.github.io/loadx](https://kamathramu87.github.io/loadx).
