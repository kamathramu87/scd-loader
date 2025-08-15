from __future__ import annotations

from typing import TYPE_CHECKING

import pyspark.sql.functions as f

from scd_loader.core.config import HASH_SEPARATOR

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

    from scd_loader.core.config import SCD2Config


class HashService:
    """Service for handling hash calculations in SCD2 processing."""

    @staticmethod
    def apply_hash_transformations(
        df: DataFrame, config: SCD2Config, source_columns: list[str]
    ) -> DataFrame:
        """Apply hash transformations to identify changes.

        Args:
            df: Input DataFrame
            config: SCD2 configuration
            source_columns: List of source columns to include in hash

        Returns:
            DataFrame with hash columns added
        """
        if config.scd_columns is None:
            raise ValueError("scd_columns configuration is required")

        # Create hash for change detection (includes deleted flag)
        df_with_change_hash = df.withColumn(
            "row_hash_changed",
            f.sha2(
                f.concat_ws(
                    HASH_SEPARATOR,
                    *[
                        col
                        for col in source_columns
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
                        for col in source_columns
                        if col not in (config.ignore_columns or [])
                    ],
                ),
                256,
            ),
        )

        return df_with_row_hash
