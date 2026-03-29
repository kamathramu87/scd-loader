from __future__ import annotations

import logging
from typing import Any


class SCD2Logger:
    """Centralized logging configuration for SCD2 processing."""

    @staticmethod
    def setup_logger(
        name: str = "scd2_loader",
        level: int = logging.INFO,
        format_string: str | None = None,
    ) -> logging.Logger:
        """Set up a logger with consistent configuration.

        Args:
            name: Logger name
            level: Logging level
            format_string: Custom format string

        Returns:
            Configured logger instance
        """
        if format_string is None:
            format_string = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

        logger = logging.getLogger(name)
        logger.setLevel(level)

        # Avoid duplicate handlers
        if not logger.handlers:
            handler = logging.StreamHandler()
            handler.setLevel(level)
            formatter = logging.Formatter(format_string)
            handler.setFormatter(formatter)
            logger.addHandler(handler)

        return logger

    @staticmethod
    def log_dataframe_info(logger: logging.Logger, df_name: str, **kwargs: Any) -> None:
        """Log DataFrame information for debugging.

        Args:
            logger: Logger instance
            df_name: Name of the DataFrame
            **kwargs: Additional metadata to log
        """
        log_data = {"dataframe": df_name, **kwargs}
        logger.info("DataFrame info: %s", log_data)

    @staticmethod
    def log_processing_step(logger: logging.Logger, step: str, status: str = "started", **kwargs: Any) -> None:
        """Log processing step information.

        Args:
            logger: Logger instance
            step: Processing step name
            status: Step status (started, completed, failed)
            **kwargs: Additional metadata to log
        """
        log_data = {"step": step, "status": status, **kwargs}
        logger.info("Processing step: %s", log_data)
