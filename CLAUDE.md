# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Install dependencies
make install          # uv sync --all-extras

# Run tests with coverage
make test             # pytest with XML coverage output

# Run a single test file
uv run pytest tests/test_scd2_loader.py -v

# Run a single test
uv run pytest tests/test_scd2_loader.py::TestSCD2Load::test_initial_load -v

# All quality checks (uv lock, pre-commit, mypy, deptry)
make check

# Build package
make build

# Serve documentation locally
make docs-serve
```

## Architecture

This is a **PySpark library** for Slowly Changing Dimension Type 2 (SCD2) transformations. It tracks historical changes to dimensional data using valid_from/valid_until date ranges, hash-based change detection, and active/delete flags.

### Module Layout

```
scd_loader/
├── scd2_loader.py          # Public API: SCD2Loader class
├── exceptions.py            # Custom exception hierarchy
├── core/
│   ├── config.py           # SCD2Config, SCD2Columns dataclasses
│   ├── processor.py        # SCD2Processor — orchestrates the pipeline
│   └── validator.py        # SCD2Validator — static validation methods
├── services/
│   ├── data_service.py     # DataFrame transformations
│   ├── hash_service.py     # SHA-256 change detection hashing
│   └── date_service.py     # Date utilities
└── utils/
    ├── spark_factory.py    # SparkSession factory
    └── logging_config.py   # Centralized logging
```

### Data Flow

```
SCD2Loader.slowly_changing_dimension()
  → SCD2Config.create()          # Build config from parameters
  → SCD2Validator                # Validate keys, columns, data freshness
  → SCD2Processor.process()
      → DataService.prepare_source_data()        # Add placeholder SCD2 columns
      → DataService.handle_incremental_load()    # Merge with target (if exists)
      → HashService.add_hash_columns()           # SHA-256 hashes for change detection
      → DataService.filter_for_changes()         # Window-based lag comparison
      → DataService.process_deletions()          # Handle records deleted between snapshots
      → DataService.add_support_columns()        # valid_from, valid_until, active_flag
      → DataService.finalize_output()            # Select final columns, add upsert_flag
```

### Key Design Decisions

- **Change detection**: Two hash columns — `row_hash` (content only) and `row_hash_changed` (includes delete flag). A record is considered changed when `row_hash_changed` differs from the previous snapshot via a window `lag()`.
- **Deletion handling**: Deletions are detected by comparing source business keys between consecutive snapshots. Deleted records get a `delete_flag=True` and a closed `valid_until` date.
- **Incremental loads**: When a `target_df` is passed, active target records are merged with source data. Only changed records (by hash) flow through the pipeline.
- **SCD2 column names are configurable** via `SCD2Columns` dataclass — all output column names can be overridden.
- **`ignore_columns`**: List of column names excluded from hash calculations (e.g., audit timestamps that shouldn't trigger SCD2 changes).
- **`OPEN_END_DATE`** = `9999-12-31` marks currently active records.

### Testing

Tests use `chispa` for PySpark DataFrame assertions. Session-scoped `spark` fixture in `conftest.py` is reused across all tests (local[1], test-optimized). `data_day1/day2/day3` fixtures simulate multi-day employee snapshots for integration tests.
