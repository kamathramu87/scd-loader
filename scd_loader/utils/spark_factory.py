from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


class SparkSessionFactory:
    """Factory for creating and managing Spark sessions."""

    @staticmethod
    def create_session(app_name: str = "SCD2Loader") -> SparkSession:
        """Create and return a new SparkSession.

        Args:
            app_name: Name for the Spark application

        Returns:
            SparkSession instance
        """
        from pyspark.sql import SparkSession

        return SparkSession.builder.appName(app_name).getOrCreate()

    @staticmethod
    def get_or_create_session(spark_session: SparkSession | None = None, app_name: str = "SCD2Loader") -> SparkSession:
        """Get existing session or create a new one.

        Args:
            spark_session: Optional existing SparkSession
            app_name: Name for the Spark application if creating new

        Returns:
            SparkSession instance
        """
        if spark_session is not None:
            return spark_session
        return SparkSessionFactory.create_session(app_name)
