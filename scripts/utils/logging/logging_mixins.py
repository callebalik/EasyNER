"""
Logging mixins for improved log formatting.

This module provides mixins that can be added to classes to enhance their logging capabilities,
particularly for formatting structured data as tables in log messages.
"""

import logging
from typing import Dict, Any, Optional, Union

from scripts.utils.log_formatter import TableFormatter


class TableLoggingMixin:
    """
    Mixin class that adds table formatting capabilities to loggers.

    This mixin can be added to any class that has a logger attribute
    to provide methods for logging data as tables.
    """

    def log_table(
        self,
        data: Dict[str, Any],
        title: Optional[str] = None,
        level: int = logging.INFO,
    ) -> None:
        """
        Log a dictionary of data as a formatted table.

        Args:
            data: Dictionary of key-value pairs to display in the table
            title: Optional title for the table
            level: Logging level to use (default: INFO)
        """
        if not hasattr(self, "logger"):
            raise AttributeError("TableLoggingMixin requires a 'logger' attribute")

        table_str = TableFormatter.format_table(data, title)
        self.logger.log(level, f"\n{table_str}")

    def log_config_as_table(
        self, title: str = "Configuration", level: int = logging.INFO
    ) -> None:
        """
        Log object attributes as a configuration table.

        This method will collect all non-private attributes (those that don't start with _)
        and log them as a table.

        Args:
            title: Title for the table
            level: Logging level to use (default: INFO)
        """
        if not hasattr(self, "logger"):
            raise AttributeError("TableLoggingMixin requires a 'logger' attribute")

        # Collect attributes that don't start with _ and aren't callable
        config = {}
        for attr_name in dir(self):
            if not attr_name.startswith("_") and not callable(getattr(self, attr_name)):
                try:
                    value = getattr(self, attr_name)
                    # Skip logger and other complex objects
                    if not isinstance(value, (logging.Logger, type)):
                        config[attr_name] = value
                except (AttributeError, Exception):
                    pass

        if config:
            self.log_table(config, title, level)

    def log_reader_writer_config(
        self,
        total_rows: Optional[int] = None,
        batch_size: Optional[int] = None,
        num_batches: Optional[int] = None,
        writer_chunk_size: Optional[int] = None,
        max_queue_size: Optional[int] = None,
        reader_profiling: Optional[bool] = None,
        writer_profiling: Optional[bool] = None,
        reader_threads: Optional[tuple] = None,
        writing_thread: Optional[bool] = None,
        level: int = logging.INFO,
    ) -> None:
        """
        Log ReaderWriterPair configuration as a formatted table.

        Args:
            total_rows: Total number of rows being processed
            batch_size: Size of each processing batch
            num_batches: Total number of batches
            writer_chunk_size: Size of writer chunks
            max_queue_size: Maximum queue size
            reader_profiling: Whether reader profiling is enabled
            writer_profiling: Whether writer profiling is enabled
            reader_threads: Tuple of (started_threads, total_threads)
            writing_thread: Whether writing thread is active
            level: Logging level to use (default: INFO)
        """
        if not hasattr(self, "logger"):
            raise AttributeError("TableLoggingMixin requires a 'logger' attribute")

        config = {}

        if total_rows is not None:
            config["Total rows"] = total_rows
        if batch_size is not None:
            config["Batch size"] = batch_size
        if num_batches is not None:
            config["Total batches"] = num_batches
        if writer_chunk_size is not None:
            config["Writer chunk size"] = writer_chunk_size
        if max_queue_size is not None:
            config["Max queue size"] = max_queue_size
        if reader_profiling is not None:
            config["Reader profiling"] = reader_profiling
        if writer_profiling is not None:
            config["Writer profiling"] = writer_profiling
        if reader_threads is not None:
            config["Reader threads running"] = (
                f"{reader_threads[0]}/{reader_threads[1]}"
            )
        if writing_thread is not None:
            config["Writing thread active"] = writing_thread

        if config:
            self.log_table(config, "ReaderWriterPair Configuration", level)

    def format_as_table(self, data: Dict[str, Any], title: Optional[str] = None) -> str:
        """
        Format a dictionary as a table string without logging it.

        Args:
            data: Dictionary of key-value pairs to display in the table
            title: Optional title for the table

        Returns:
            Formatted table as a string
        """
        return TableFormatter.format_table(data, title)
