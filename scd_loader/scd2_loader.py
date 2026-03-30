from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from scd_loader.core.config import DEFAULT_DATE_COLUMN, OPEN_END_DATE, SCD2Config
from scd_loader.core.processor import SCD2Processor
from scd_loader.utils.spark_factory import SparkSessionFactory

if TYPE_CHECKING:
    from datetime import datetime

    from pyspark.sql import DataFrame, SparkSession

    from scd_loader.core.config import SCD2ColumnNames

logger = logging.getLogger(__name__)


class SCD2Loader:
    """Slowly Changing Dimension Type 2 loader for PySpark DataFrames.

    This class provides a clean interface for SCD2 processing with improved
    separation of concerns and better testability.
    """

    def __init__(self, spark_session: SparkSession | None = None) -> None:
        """Initialize the SCD2 loader with an optional Spark session.

        Args:
            spark_session: Optional SparkSession instance. If None, creates a new one.
        """
        self.spark = SparkSessionFactory.get_or_create_session(spark_session)
        self.processor = SCD2Processor()
        logger.info("SCD2Loader initialized successfully")

    def slowly_changing_dimension(
        self,
        df_src: DataFrame,
        business_keys: list[str] | str,
        date_column: str = DEFAULT_DATE_COLUMN,
        df_tgt: DataFrame | None = None,
        ignore_columns: list[str] | None = None,
        non_copy_fields: list[str] | None = None,
        open_end_date: datetime | None = OPEN_END_DATE,
        scd_columns: SCD2ColumnNames | dict[str, str] | None = None,
        enable_latest_record_flag: bool = False,
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
            scd_columns: Override default SCD2 output column names. Accepts an
                `SCD2ColumnNames` instance (recommended, full IDE type hints) or a plain
                dict with any subset of keys: `valid_from`, `valid_until`,
                `active_flag`, `delete_flag`, `row_hash`.
            enable_latest_record_flag: When `True`, adds a `latest_record_flag` column
                that is `True` for the most recent record per business key.

        Returns:
            DataFrame with SCD2 columns and transformations applied

        Raises:
            EmptyDataExceptionError: When source DataFrame is empty
            OldDataExceptionError: When source data is older than target data
            ValueError: When invalid parameters are provided
        """
        # Create configuration using factory method
        config = SCD2Config.create(
            business_keys=business_keys,
            date_column=date_column,
            ignore_columns=ignore_columns,
            non_copy_fields=non_copy_fields,
            open_end_date=open_end_date,
            scd_columns=scd_columns,
            enable_latest_record_flag=enable_latest_record_flag,
        )

        # Process the data using the processor
        return self.processor.process(df_src, df_tgt, config)
