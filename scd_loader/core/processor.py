from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from pyspark.sql.window import Window

from scd_loader.core.validator import SCD2Validator
from scd_loader.services.data_service import DataService
from scd_loader.services.hash_service import HashService

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

    from scd_loader.core.config import SCD2Config

logger = logging.getLogger(__name__)


class SCD2Processor:
    """Core processor for SCD2 transformations."""

    def __init__(self) -> None:
        """Initialize the SCD2 processor."""
        self.source_columns: list[str] = []

    def process(self, df_src: DataFrame, df_tgt: DataFrame | None, config: SCD2Config) -> DataFrame:
        """Main SCD2 processing pipeline.

        Args:
            df_src: Source DataFrame
            df_tgt: Optional target DataFrame
            config: SCD2 configuration

        Returns:
            Processed DataFrame with SCD2 transformations
        """
        logger.info("Starting SCD2 processing")

        # Validate configuration and inputs
        SCD2Validator.validate_config(config)
        SCD2Validator.validate_inputs(df_src, config.business_keys, config.date_column)

        # Update source columns for processing
        self._update_source_columns(df_src, config)

        # Prepare source data
        df_prepared = DataService.prepare_source_data(df_src, config)

        # Handle incremental load if target exists
        if df_tgt and not df_tgt.isEmpty():
            df_prepared = DataService.handle_incremental_load(df_prepared, df_tgt, config)

        # Process SCD2 transformations
        df_processed = self._apply_scd2_transformations(df_prepared, config)

        logger.info("SCD2 processing completed successfully")
        return df_processed

    def _update_source_columns(self, df_src: DataFrame, config: SCD2Config) -> None:
        """Update source columns by removing non-copy fields and date column.

        Args:
            df_src: Source DataFrame
            config: SCD2 configuration
        """
        df = df_src.drop(*config.non_copy_fields) if config.non_copy_fields else df_src
        source_columns = df.columns.copy()
        if config.date_column in source_columns:
            source_columns.remove(config.date_column)
        self.source_columns = source_columns

    def _apply_scd2_transformations(self, df: DataFrame, config: SCD2Config) -> DataFrame:
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
        window_func = Window.partitionBy(config.business_keys).orderBy(config.date_column)

        # Process date windows and deletions
        df_with_deletions = DataService.process_deletions(df_distinct, config)

        # Apply hash transformations
        df_hashed = HashService.apply_hash_transformations(df_with_deletions, config, self.source_columns)

        # Filter for changes
        df_changes = DataService.filter_for_changes(df_hashed, config, window_func)

        # Add SCD2 support columns
        df_with_support = DataService.add_support_columns(df_changes, config)

        # Final filtering and column selection
        return DataService.finalize_output(df_with_support, config)
