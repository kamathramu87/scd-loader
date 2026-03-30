from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pyspark.sql.functions as f
from pyspark.sql.window import Window

from loadx.scd2.config import (
    COL_DATE_LEAD,
    COL_DELETE_FLAG,
    COL_DELETED,
    COL_NEXT_CHANGE,
    COL_ORIG_VALID_FROM,
    COL_ORIG_VALID_UNTIL,
    COL_ROW_HASH_CHANGED,
    COL_ROW_HASH_CHANGED_LAG,
    UPSERT_FLAG_COLUMN,
)
from loadx.scd2.services.date_service import DateService
from loadx.scd2.validator import SCD2Validator

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.window import WindowSpec

    from loadx.scd2.config import SCD2Config

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
        df = df_cleaned.withColumn(
            COL_ORIG_VALID_FROM, f.lit(None).cast("timestamp")
        ).withColumn(COL_ORIG_VALID_UNTIL, f.lit(None).cast("timestamp"))

        return df

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

        # Validate data freshness
        SCD2Validator.validate_data_freshness(df_src, df_tgt, config)

        # Rename target columns for processing
        df_tgt_renamed = df_tgt.withColumnRenamed(
            config.scd_columns.valid_from, COL_ORIG_VALID_FROM
        ).withColumnRenamed(config.scd_columns.valid_until, COL_ORIG_VALID_UNTIL)

        # Get target columns excluding SCD2 columns
        df_tgt_select = df_tgt_renamed.drop(*config.scd_columns.field_list())

        # Get max date from target for filtering
        tgt_max_date = DateService.get_max_date(df_tgt, config.scd_columns.valid_from)

        # Prepare target data for union
        df_tgt_curr = df_tgt_select.withColumn(config.date_column, f.lit(tgt_max_date))

        # Filter source data and union with target
        df_filtered = df_src.filter(f.col(config.date_column) > f.lit(tgt_max_date))
        df_merged = df_filtered.union(df_tgt_curr.select(df_src.columns))

        return df_merged

    @staticmethod
    def filter_for_changes(
        df: DataFrame, config: SCD2Config, window_func: WindowSpec
    ) -> DataFrame:
        """Filter DataFrame to keep only records with changes.

        Args:
            df: Input DataFrame with hash columns
            config: SCD2 configuration
            window_func: WindowSpec function for partitioning

        Returns:
            DataFrame with only changed records
        """
        df_with_lag = df.withColumn(
            COL_ROW_HASH_CHANGED_LAG, f.lag(COL_ROW_HASH_CHANGED).over(window_func)
        )

        df_filtered = (
            df_with_lag.filter(
                (df_with_lag[COL_ROW_HASH_CHANGED_LAG].isNull())
                | (
                    df_with_lag[COL_ROW_HASH_CHANGED_LAG]
                    != df_with_lag[COL_ROW_HASH_CHANGED]
                )
            )
            .drop(COL_ROW_HASH_CHANGED_LAG, COL_ROW_HASH_CHANGED)
            .withColumn(COL_NEXT_CHANGE, f.lead(config.date_column).over(window_func))
            .withColumn(COL_DELETE_FLAG, f.lead(COL_DELETED).over(window_func))
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
        df_with_support = (
            df.filter(~f.col(COL_DELETED))
            .withColumn(
                config.scd_columns.valid_from,
                f.coalesce(
                    df[COL_ORIG_VALID_FROM], f.col(config.date_column).cast("timestamp")
                ),
            )
            .withColumn(
                config.date_column,
                f.coalesce(df[COL_ORIG_VALID_FROM], f.col(config.date_column)),
            )
            .withColumn(
                config.scd_columns.valid_until,
                f.coalesce(df[COL_NEXT_CHANGE], f.lit(config.open_end_date)),
            )
            .withColumn(
                config.scd_columns.active_flag,
                f.when(
                    (f.col(config.scd_columns.valid_until).isNull())
                    | (f.col(config.scd_columns.valid_until) == config.open_end_date),
                    True,
                ).otherwise(False),
            )
            .withColumn(
                UPSERT_FLAG_COLUMN,
                f.when(df[COL_ORIG_VALID_FROM].isNull(), "I").otherwise("U"),
            )
        )

        if config.source_type == "full":
            df_with_support = df_with_support.withColumn(
                config.scd_columns.delete_flag,
                f.coalesce(df_with_support[COL_DELETE_FLAG], f.lit(False)),
            )

        if config.enable_latest_record_flag:
            df_with_support = DataService._add_latest_record_flag(
                df_with_support, config
            )

        return df_with_support

    @staticmethod
    def _add_latest_record_flag(df: DataFrame, config: SCD2Config) -> DataFrame:
        """Add latest_record_flag column — True for the most recent record per business key.

        Args:
            df: Input DataFrame with SCD2 columns
            config: SCD2 configuration

        Returns:
            DataFrame with latest_record_flag column added
        """
        window_spec = Window.partitionBy(config.business_keys).orderBy(
            f.col(config.scd_columns.valid_from).desc()
        )
        return (
            df.withColumn("_row_num", f.row_number().over(window_spec))
            .withColumn(
                config.scd_columns.latest_record_flag,
                f.when(f.col("_row_num") == 1, True).otherwise(False),
            )
            .drop("_row_num")
        )

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
        snapshot_dates_df = (
            df.select(config.date_column)
            .distinct()
            .withColumn(
                "next_date_available",
                f.lead(config.date_column).over(Window.orderBy(config.date_column)),
            )
            .withColumnRenamed(config.date_column, f"{config.date_column}_r")
        )

        # Get max snapshot date across all partitions
        max_snapshot_date_row = snapshot_dates_df.agg(
            f.max(f"{config.date_column}_r").alias("max_date")
        ).first()
        max_snapshot_date = (
            max_snapshot_date_row.max_date if max_snapshot_date_row else None
        )

        # Join and identify deletions
        df_with_dates = (
            df.join(
                snapshot_dates_df,
                f.col(config.date_column) == f.col(f"{config.date_column}_r"),
                "left",
            )
            .withColumn(
                COL_DATE_LEAD,
                f.lead(config.date_column).over(
                    Window.partitionBy(config.business_keys).orderBy(config.date_column)
                ),
            )
            .withColumn(
                COL_DELETED,
                f.when(
                    (f.col("next_date_available") != f.col(COL_DATE_LEAD))
                    | (
                        (f.col(COL_DATE_LEAD).isNull())
                        & (df[config.date_column] != max_snapshot_date)
                    ),
                    True,
                ).otherwise(False),
            )
        )

        # Create deletion periods
        date_col_r = f"{config.date_column}_r"
        df_expanded = (
            df_with_dates.drop("next_date_available", COL_DATE_LEAD, date_col_r)
            .withColumn(COL_DELETED, f.lit(False))
            .union(
                df_with_dates.where(f.col(COL_DELETED))
                .drop(config.date_column, COL_DATE_LEAD, date_col_r)
                .withColumnRenamed("next_date_available", config.date_column)
                .select(
                    df_with_dates.drop(
                        "next_date_available", COL_DATE_LEAD, date_col_r
                    ).columns
                )
            )
        )

        return df_expanded

    @staticmethod
    def finalize_output(df: DataFrame, config: SCD2Config) -> DataFrame:
        """Finalize the output by filtering and selecting columns.

        Args:
            df: Input DataFrame with all SCD2 columns
            config: SCD2 configuration

        Returns:
            Final output DataFrame
        """
        # Get source columns (excluding SCD2 and processing columns)
        source_columns = [
            col
            for col in df.columns
            if col
            not in [
                *config.scd_columns.column_list(),
                COL_ORIG_VALID_FROM,
                COL_ORIG_VALID_UNTIL,
                COL_NEXT_CHANGE,
                COL_DELETED,
                COL_DELETE_FLAG,
                UPSERT_FLAG_COLUMN,
            ]
        ]

        # Build SCD2 column list — exclude latest_record_flag when feature is disabled,
        # and exclude delete_flag for incremental sources
        scd_output_columns = [
            col
            for col in config.scd_columns.column_list()
            if (
                config.enable_latest_record_flag
                or col != config.scd_columns.latest_record_flag
            )
            and (config.source_type == "full" or col != config.scd_columns.delete_flag)
        ]

        target_columns = source_columns + scd_output_columns + [UPSERT_FLAG_COLUMN]

        # Filter for records that need to be inserted or updated
        df_filtered = df.filter(
            (
                f.coalesce(df[COL_ORIG_VALID_UNTIL], f.lit(config.open_end_date))
                != df[config.scd_columns.valid_until]
            )
            | (df[COL_ORIG_VALID_FROM].isNull())
        )

        # Select final columns
        df_output = df_filtered.select(target_columns)

        return df_output
