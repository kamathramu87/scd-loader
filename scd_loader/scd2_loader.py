from __future__ import annotations

import logging
from dataclasses import dataclass, fields
from datetime import datetime
from typing import TYPE_CHECKING, Any

import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.window import Window

from helpers.exceptions import (
    BusinessKeysEmptyError,
    EmptyDataExceptionError,
    OldDataExceptionError,
)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

# Constants
OPEN_END_DATE = datetime(9999, 12, 31)
DEFAULT_DATE_COLUMN = "snapshot_date"
UPSERT_FLAG_COLUMN = "upsert_flag"
HASH_ALGORITHM = "SHA-256"
HASH_SEPARATOR = "|"

# Configure logging
logger = logging.getLogger(__name__)


@dataclass
class SCD2Columns:
    """Configuration for SCD2 column names."""

    valid_from: str = "valid_from"
    valid_until: str = "valid_until"
    active_flag: str = "active_flag"
    delete_flag: str = "delete_flag"
    row_hash: str = "row_hash"

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SCD2Columns:
        """Create SCD2Columns instance from dictionary, filtering valid fields."""
        field_names = {f.name for f in fields(cls)}
        filtered_data = {k: v for k, v in data.items() if k in field_names}
        return cls(**filtered_data)

    def field_list(self) -> list[str]:
        """Get list of all field names."""
        return [f.name for f in fields(self)]


@dataclass
class SCD2Config:
    """Configuration for SCD2 processing."""

    business_keys: list[str]
    date_column: str = DEFAULT_DATE_COLUMN
    ignore_columns: list[str] | None = None
    non_copy_fields: list[str] | None = None
    open_end_date: datetime | None = OPEN_END_DATE
    scd_columns: SCD2Columns | None = None

    def __post_init__(self) -> None:
        """Initialize default values for optional fields."""
        if self.ignore_columns is None:
            self.ignore_columns = []
        if self.non_copy_fields is None:
            self.non_copy_fields = []
        if self.scd_columns is None:
            self.scd_columns = SCD2Columns()


class SCD2Loader:
    """Slowly Changing Dimension Type 2 loader for PySpark DataFrames."""

    def __init__(self, spark_session: SparkSession | None = None) -> None:
        """Initialize the SCD2 loader with an optional Spark session.

        Args:
            spark_session: Optional SparkSession instance. If None, creates a new one.
        """
        self.spark = spark_session or self._create_spark_session()
        logger.info("SCD2Loader initialized successfully")

    def _create_spark_session(self) -> SparkSession:
        """Create and return a new SparkSession."""
        return SparkSession.builder.getOrCreate()

    def slowly_changing_dimension(
        self,
        df_src: DataFrame,
        business_keys: list[str] | str,
        date_column: str = DEFAULT_DATE_COLUMN,
        df_tgt: DataFrame | None = None,
        ignore_columns: list[str] | None = None,
        non_copy_fields: list[str] | None = None,
        open_end_date: datetime | None = OPEN_END_DATE,
        scd_columns: dict[str, str] | None = None,
    ) -> DataFrame:
        """Process slowly changing dimension type 2 transformation.

        This method implements SCD2 to track historical changes in data. It supports
        both full initial loads and incremental loads.

        Args:
            df_src: Source DataFrame containing the data to process
            business_keys: List of columns that constitute the business key
            date_column: Column name containing snapshot dates
            df_tgt: Optional target DataFrame for incremental loads
            ignore_columns: Columns to ignore when calculating row hashes
            non_copy_fields: Fields to exclude from source to target
            open_end_date: Date to use for active records (default: 9999-12-31)
            scd_columns: Custom SCD2 column name mappings

        Returns:
            DataFrame with SCD2 columns and transformations applied

        Raises:
            EmptyDataExceptionError: When source DataFrame is empty
            OldDataExceptionError: When source data is older than target data
            ValueError: When invalid parameters are provided
        """
        logger.info("Starting SCD2 processing")

        # Convert business_keys to list if it's a string
        if isinstance(business_keys, str):
            business_keys = [business_keys]

        # Validate inputs
        self._validate_inputs(df_src, business_keys, date_column)

        # Create configuration
        config = SCD2Config(
            business_keys=business_keys,
            date_column=date_column,
            ignore_columns=ignore_columns or [],
            non_copy_fields=non_copy_fields or [],
            open_end_date=open_end_date,
            scd_columns=SCD2Columns.from_dict(scd_columns) if scd_columns else None,
        )

        # Process the data
        return self._process_scd2(df_src, df_tgt, config)

    def _validate_inputs(
        self, df_src: DataFrame, business_keys: list[str] | str, date_column: str
    ) -> None:
        """Validate input parameters.

        Args:
            df_src: Source DataFrame to validate
            business_keys: Business keys to validate (can be string or list of strings)
            date_column: Date column to validate

        Raises:
            ValueError: When validation fails
            EmptyDataExceptionError: When source DataFrame is empty
        """
        # Convert business_keys to list if it's a string
        if isinstance(business_keys, str):
            business_keys = [business_keys]

        if not business_keys:
            raise BusinessKeysEmptyError

        if not date_column:
            raise ValueError("date_column cannot be empty")

        if df_src.isEmpty():
            logger.error("Source DataFrame is empty")
            raise EmptyDataExceptionError

        # Check if required columns exist
        missing_columns = [
            col for col in [*business_keys, date_column] if col not in df_src.columns
        ]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")

    def _process_scd2(
        self, df_src: DataFrame, df_tgt: DataFrame | None, config: SCD2Config
    ) -> DataFrame:
        """Main SCD2 processing pipeline.

        Args:
            df_src: Source DataFrame
            df_tgt: Optional target DataFrame
            config: SCD2 configuration

        Returns:
            Processed DataFrame with SCD2 transformations
        """

        self._update_source_columns(df_src, config)

        # Prepare source data
        df_prepared = self._prepare_source_data(df_src, config)

        # Handle incremental load if target exists
        if df_tgt and not df_tgt.isEmpty():
            df_prepared = self._handle_incremental_load(df_prepared, df_tgt, config)

        # Process SCD2 transformations
        df_processed = self._apply_scd2_transformations(df_prepared, config)

        logger.info("SCD2 processing completed successfully")
        return df_processed

    def _update_source_columns(self, df_src: DataFrame, config: SCD2Config) -> None:
        """Update source columns by removing non-copy fields and returning the remaining columns.
        Args:
            df_src: Source DataFrame
            config: SCD2 configuration
        Returns:
            List of source columns excluding non-copy fields and date column
        """
        df = df_src.drop(*config.non_copy_fields) if config.non_copy_fields else df_src
        source_columns = df.columns
        source_columns.remove(config.date_column)
        self.source_columns = source_columns

    def _prepare_source_data(self, df_src: DataFrame, config: SCD2Config) -> DataFrame:
        """Prepare source data by removing non-copy fields and adding SCD2 columns.

        Args:
            df_src: Source DataFrame
            config: SCD2 configuration

        Returns:
            Prepared DataFrame
        """
        df_cleaned = (
            df_src.drop(*config.non_copy_fields) if config.non_copy_fields else df_src
        )

        # Add original SCD2 columns for tracking
        df_with_orig_columns = df_cleaned.withColumn(
            "orig_valid_from", f.lit(None).cast("timestamp")
        ).withColumn("orig_valid_until", f.lit(None).cast("timestamp"))

        return df_with_orig_columns

    def _handle_incremental_load(
        self, df_src: DataFrame, df_tgt: DataFrame, config: SCD2Config
    ) -> DataFrame:
        """Handle incremental load by merging source and target data.

        Args:
            df_src: Source DataFrame
            df_tgt: Target DataFrame
            config: SCD2 configuration

        Returns:
            Merged DataFrame for incremental processing

        Raises:
            OldDataExceptionError: When source data is older than target data
        """
        logger.info("Processing incremental load")

        # Ensure scd_columns is not None
        if config.scd_columns is None:
            config.scd_columns = SCD2Columns()

        # Rename target columns for processing
        df_tgt_renamed = df_tgt.withColumnRenamed(
            config.scd_columns.valid_from, "orig_valid_from"
        ).withColumnRenamed(config.scd_columns.valid_until, "orig_valid_until")

        # Get target columns excluding SCD2 columns
        df_tgt_select = df_tgt_renamed.drop(*config.scd_columns.field_list())

        # Get max dates for comparison
        tgt_max_date = self._get_max_date(df_tgt, config.scd_columns.valid_from)
        src_max_date = self._get_max_date(df_src, config.date_column)

        # Validate data freshness
        if src_max_date < tgt_max_date:
            logger.error(
                "Source data (%s) is older than target data (%s)",
                src_max_date,
                tgt_max_date,
            )
            raise OldDataExceptionError

        # Prepare target data for union
        df_tgt_curr = df_tgt_select.withColumn(config.date_column, f.lit(tgt_max_date))

        # Filter source data and union with target
        df_filtered = df_src.filter(f.col(config.date_column) > f.lit(tgt_max_date))
        df_merged = df_filtered.union(df_tgt_curr.select(df_src.columns))

        return df_merged

    def _get_max_date(self, df: DataFrame, date_column: str) -> Any:
        """Get the maximum date from a DataFrame column.

        Args:
            df: DataFrame to query
            date_column: Name of the date column

        Returns:
            Maximum date value or empty string if no data
        """
        max_date_row = df.agg(f.max(date_column).alias("max_date")).first()
        return max_date_row["max_date"] if max_date_row else ""

    def _apply_scd2_transformations(
        self, df: DataFrame, config: SCD2Config
    ) -> DataFrame:
        """Apply all SCD2 transformations to the DataFrame.

        Args:
            df: Input DataFrame
            config: SCD2 configuration

        Returns:
            Transformed DataFrame with SCD2 columns
        """
        # Remove duplicates
        df_distinct = df.distinct()

        # Create window function
        window_func = Window.partitionBy(config.business_keys).orderBy(
            config.date_column
        )

        # Process date windows and deletions
        df_with_deletions = self._process_deletions(df_distinct, config)

        # Apply hash transformations
        df_hashed = self._apply_hash_transformations(df_with_deletions, config)

        # Filter for changes
        df_changes = self._filter_for_changes(df_hashed, window_func)

        # Add SCD2 support columns
        df_with_support = self._add_support_columns(df_changes, config)

        # Final filtering and column selection
        return self._finalize_output(df_with_support, config)

    def _process_deletions(self, df: DataFrame, config: SCD2Config) -> DataFrame:
        """Process deletion periods in the data.

        Args:
            df: Input DataFrame
            config: SCD2 configuration

        Returns:
            DataFrame with deletion periods processed
        """
        # Create date window for next available dates
        df_date_window = (
            df.select(config.date_column)
            .distinct()
            .withColumn(
                "next_date_available",
                f.lead(config.date_column).over(Window.orderBy(config.date_column)),
            )
            .withColumnRenamed(config.date_column, f"{config.date_column}_r")
        )

        # Get max delivered date
        max_delivered_date_row = df_date_window.agg(
            f.max(f"{config.date_column}_r").alias("max_date")
        ).first()
        max_delivered_date = (
            max_delivered_date_row.max_date if max_delivered_date_row else None
        )

        # Join and identify deletions
        df_with_dates = (
            df.join(
                df_date_window,
                f.col(config.date_column) == f.col(f"{config.date_column}_r"),
                "left",
            )
            .withColumn(
                "date_lead",
                f.lead(config.date_column).over(
                    Window.partitionBy(config.business_keys).orderBy(config.date_column)
                ),
            )
            .withColumn(
                "deleted",
                f.when(
                    (f.col("next_date_available") != f.col("date_lead"))
                    | (
                        (f.col("date_lead").isNull())
                        & (df[config.date_column] != max_delivered_date)
                    ),
                    True,
                ).otherwise(False),
            )
        )

        # Create deletion periods
        df_with_delete_periods = (
            df_with_dates.drop("next_date_available", "date_lead")
            .withColumn("deleted", f.lit(False))
            .union(
                df_with_dates.where(f.col("deleted"))
                .drop(config.date_column, "date_lead")
                .withColumnRenamed("next_date_available", config.date_column)
                .select(df_with_dates.drop("next_date_available", "date_lead").columns)
            )
        )

        return df_with_delete_periods

    def _apply_hash_transformations(
        self, df: DataFrame, config: SCD2Config
    ) -> DataFrame:
        """Apply hash transformations to identify changes.

        Args:
            df: Input DataFrame
            config: SCD2 configuration

        Returns:
            DataFrame with hash columns added
        """

        # Create hash for change detection (includes deleted flag)
        df_with_change_hash = df.withColumn(
            "row_hash_changed",
            f.sha2(
                f.concat_ws(
                    HASH_SEPARATOR,
                    *[
                        col
                        for col in self.source_columns
                        if col not in (config.ignore_columns or [])
                    ]
                    + ["deleted"],
                ),
                256,
            ),
        )

        # Create hash for row content (excludes deleted flag)
        df_with_row_hash = df_with_change_hash.withColumn(
            config.scd_columns.row_hash,
            f.sha2(
                f.concat_ws(
                    HASH_SEPARATOR,
                    *[
                        col
                        for col in self.source_columns
                        if col not in (config.ignore_columns or [])
                    ],
                ),
                256,
            ),
        )

        return df_with_row_hash

    def _filter_for_changes(self, df: DataFrame, window_func: Window) -> DataFrame:
        """Filter DataFrame to keep only records with changes.

        Args:
            df: Input DataFrame with hash columns
            window_func: Window function for partitioning

        Returns:
            DataFrame with only changed records
        """
        df_with_lag = df.withColumn(
            "row_hash_changed_lag", f.lag("row_hash_changed").over(window_func)
        )

        df_filtered = (
            df_with_lag.filter(
                (df_with_lag.row_hash_changed_lag.isNull())
                | (df_with_lag.row_hash_changed_lag != df_with_lag.row_hash_changed)
            )
            .drop("row_hash_changed_lag", "row_hash_changed")
            .withColumn("next_change", f.lead("snapshot_date").over(window_func))
            .withColumn("delete_flag", f.lead("deleted").over(window_func))
        )

        return df_filtered

    def _add_support_columns(self, df: DataFrame, config: SCD2Config) -> DataFrame:
        """Add SCD2 support columns to the DataFrame.

        Args:
            df: Input DataFrame
            config: SCD2 configuration

        Returns:
            DataFrame with SCD2 support columns
        """
        # Ensure scd_columns is not None
        if config.scd_columns is None:
            config.scd_columns = SCD2Columns()

        f.current_timestamp()

        df_with_support = (
            df.filter(~f.col("deleted"))
            .withColumn(
                config.scd_columns.valid_from,
                f.coalesce(
                    df.orig_valid_from, f.col(config.date_column).cast("timestamp")
                ),
            )
            .withColumn(
                config.date_column,
                f.coalesce(df.orig_valid_from, f.col(config.date_column)),
            )
            .withColumn(
                config.scd_columns.valid_until,
                f.coalesce(df.next_change, f.lit(config.open_end_date)),
            )
            .withColumn(
                config.scd_columns.delete_flag, f.coalesce(df.delete_flag, f.lit(False))
            )
            .withColumn(
                config.scd_columns.active_flag,
                f.when(
                    (f.col("valid_until").isNull())
                    | (f.col("valid_until") == config.open_end_date),
                    True,
                ).otherwise(False),
            )
            .withColumn(
                UPSERT_FLAG_COLUMN,
                f.when(df.orig_valid_from.isNull(), "I").otherwise("U"),
            )
        )

        return df_with_support

    def _finalize_output(self, df: DataFrame, config: SCD2Config) -> DataFrame:
        """Finalize the output by filtering and selecting columns.

        Args:
            df: Input DataFrame with all SCD2 columns
            config: SCD2 configuration

        Returns:
            Final output DataFrame
        """
        # Ensure scd_columns is not None
        if config.scd_columns is None:
            config.scd_columns = SCD2Columns()

        # Get source columns (excluding SCD2 and processing columns)
        source_columns = [
            col
            for col in df.columns
            if col
            not in [
                *config.scd_columns.field_list(),
                "orig_valid_from",
                "orig_valid_until",
                "next_change",
                "deleted",
            ]
        ]

        # Define target columns
        target_columns = (
            source_columns + config.scd_columns.field_list() + [UPSERT_FLAG_COLUMN]
        )

        # Filter for records that need to be inserted or updated
        df_filtered = df.filter(
            (
                f.coalesce(df.orig_valid_until, f.lit(config.open_end_date))
                != df.valid_until
            )
            | (df.orig_valid_from.isNull())
        )

        # Select final columns
        df_output = df_filtered.select(target_columns)

        return df_output
