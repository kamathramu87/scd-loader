from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pyspark.sql.functions as f
from pyspark.sql.window import Window

from scd_loader.core.config import UPSERT_FLAG_COLUMN
from scd_loader.core.validator import SCD2Validator
from scd_loader.services.date_service import DateService

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.window import WindowSpec

    from scd_loader.core.config import SCD2Config

logger = logging.getLogger(__name__)


class DataService:
    """Service for handling data transformations in SCD2 processing."""

    @staticmethod
    def prepare_source_data(df_src: DataFrame, config: SCD2Config) -> DataFrame:
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

    @staticmethod
    def handle_incremental_load(
        df_src: DataFrame, df_tgt: DataFrame, config: SCD2Config
    ) -> DataFrame:
        """Handle incremental load by merging source and target data.

        Args:
            df_src: Source DataFrame
            df_tgt: Target DataFrame
            config: SCD2 configuration

        Returns:
            Merged DataFrame for incremental processing
        """
        logger.info("Processing incremental load")

        if config.scd_columns is None:
            raise ValueError("scd_columns configuration is required")

        # Validate data freshness
        SCD2Validator.validate_data_freshness(df_src, df_tgt, config)

        # Rename target columns for processing
        df_tgt_renamed = df_tgt.withColumnRenamed(
            config.scd_columns.valid_from, "orig_valid_from"
        ).withColumnRenamed(config.scd_columns.valid_until, "orig_valid_until")

        # Get target columns excluding SCD2 columns
        df_tgt_select = df_tgt_renamed.drop(*config.scd_columns.column_list())

        # Get max dates for comparison
        tgt_max_date = DateService.get_max_date(df_tgt, config.scd_columns.valid_from)
        DateService.get_max_date(df_src, config.date_column)

        # Prepare target data for union
        df_tgt_curr = df_tgt_select.withColumn(config.date_column, f.lit(tgt_max_date))

        # Filter source data and union with target
        df_filtered = df_src.filter(f.col(config.date_column) > f.lit(tgt_max_date))
        df_merged = df_filtered.union(df_tgt_curr.select(df_src.columns))

        return df_merged

    @staticmethod
    def filter_for_changes(df: DataFrame, window_func: WindowSpec) -> DataFrame:
        """Filter DataFrame to keep only records with changes.

        Args:
            df: Input DataFrame with hash columns
            window_func: WindowSpec function for partitioning

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

    @staticmethod
    def add_support_columns(df: DataFrame, config: SCD2Config) -> DataFrame:
        """Add SCD2 support columns to the DataFrame.

        Args:
            df: Input DataFrame
            config: SCD2 configuration

        Returns:
            DataFrame with SCD2 support columns
        """
        if config.scd_columns is None:
            raise ValueError("scd_columns configuration is required")

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

        # Add latest_record_flag column if enabled
        if config.enable_latest_record_flag:
            df_with_support = DataService._add_latest_record_flag_column(
                df_with_support, config
            )

        return df_with_support

    @staticmethod
    def _add_latest_record_flag_column(df: DataFrame, config: SCD2Config) -> DataFrame:
        """Add latest_record_flag column to identify the last active record for each business key.

        Args:
            df: Input DataFrame with SCD2 columns
            config: SCD2 configuration

        Returns:
            DataFrame with latest_record_flag column added
        """
        if config.scd_columns is None:
            raise ValueError("scd_columns configuration is required")

        # Create window function to rank records by valid_from date in descending order
        # for each business key combination
        window_spec = Window.partitionBy(config.business_keys).orderBy(
            f.col(config.scd_columns.valid_from).desc()
        )

        # Add row number and latest_record_flag flag
        df_with_latest_record_flag = (
            df.withColumn("_row_num", f.row_number().over(window_spec))
            .withColumn(
                config.scd_columns.latest_record_flag,
                f.when(f.col("_row_num") == 1, True).otherwise(False),
            )
            .drop("_row_num")
        )

        return df_with_latest_record_flag

    @staticmethod
    def process_deletions(df: DataFrame, config: SCD2Config) -> DataFrame:
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

    @staticmethod
    def finalize_output(df: DataFrame, config: SCD2Config) -> DataFrame:
        """Finalize the output by filtering and selecting columns.

        Args:
            df: Input DataFrame with all SCD2 columns
            config: SCD2 configuration

        Returns:
            Final output DataFrame
        """
        if config.scd_columns is None:
            raise ValueError("scd_columns configuration is required")

        # Get source columns (excluding SCD2 and processing columns)
        source_columns = [
            col
            for col in df.columns
            if col
            not in [
                *config.scd_columns.column_list(),
                "orig_valid_from",
                "orig_valid_until",
                "next_change",
                "deleted",
            ]
        ]

        # Define target columns (conditionally include latest_record_flag)
        scd_column_list = config.scd_columns.column_list()
        if not config.enable_latest_record_flag:
            # Remove latest_record_flag column if feature is disabled
            scd_column_list = [
                col
                for col in scd_column_list
                if col != config.scd_columns.latest_record_flag
            ]

        target_columns = source_columns + scd_column_list + [UPSERT_FLAG_COLUMN]

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
