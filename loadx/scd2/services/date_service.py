from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

import pyspark.sql.functions as f

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


logger = logging.getLogger(__name__)


class DateService:
    """Service for handling date-related operations in SCD2 processing."""

    @staticmethod
    def get_max_date(df: DataFrame, date_column: str) -> Any:
        """Get the maximum date from a DataFrame column.

        Args:
            df: DataFrame to query
            date_column: Name of the date column

        Returns:
            Maximum date value or empty string if no data
        """
        max_date_row = df.agg(f.max(date_column).alias("max_date")).first()
        return max_date_row["max_date"] if max_date_row else ""
