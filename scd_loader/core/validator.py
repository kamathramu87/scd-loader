from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from scd_loader.exceptions import (
    BusinessKeysEmptyError,
    EmptyDataExceptionError,
    OldDataExceptionError,
)
from scd_loader.services.date_service import DateService

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

    from scd_loader.core.config import SCD2Config

logger = logging.getLogger(__name__)


class SCD2Validator:
    """Validator for SCD2 processing inputs and data."""

    @staticmethod
    def validate_inputs(
        df_src: DataFrame, business_keys: list[str], date_column: str
    ) -> None:
        """Validate input parameters.

        Args:
            df_src: Source DataFrame to validate
            business_keys: Business keys to validate
            date_column: Date column to validate

        Raises:
            ValueError: When validation fails
            EmptyDataExceptionError: When source DataFrame is empty
            BusinessKeysEmptyError: When business keys are empty
        """
        if df_src.isEmpty():
            logger.error("Source DataFrame is empty")
            raise EmptyDataExceptionError

        # Check if required columns exist
        missing_columns = [
            col for col in [*business_keys, date_column] if col not in df_src.columns
        ]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")

    @staticmethod
    def validate_config(config: SCD2Config) -> None:
        """Validate SCD2 configuration.

        Args:
            config: SCD2Config instance to validate

        Raises:
            ValueError: When configuration is invalid
        """
        if not config.business_keys:
            raise BusinessKeysEmptyError

        if not config.date_column:
            raise ValueError("date_column cannot be empty")

    @staticmethod
    def validate_data_freshness(
        df_src: DataFrame, df_tgt: DataFrame, config: SCD2Config
    ) -> None:
        """Validate that source data is not older than target data.

        Args:
            df_src: Source DataFrame
            df_tgt: Target DataFrame
            config: SCD2 configuration

        Raises:
            OldDataExceptionError: When source data is older than target data
        """
        tgt_max_date = DateService.get_max_date(df_tgt, config.scd_columns.valid_from)
        src_max_date = DateService.get_max_date(df_src, config.date_column)

        if src_max_date < tgt_max_date:
            logger.error(
                "Source data (%s) is older than target data (%s)",
                src_max_date,
                tgt_max_date,
            )
            raise OldDataExceptionError
