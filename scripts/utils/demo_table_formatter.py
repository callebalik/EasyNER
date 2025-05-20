"""
Demonstration of table formatting for log messages.

This script demonstrates how to use the TableFormatter and TableLogFormatter classes
to display structured data in log messages.
"""

import logging
import os
import time
from typing import Dict, Any

from log_formatter import TableFormatter
from table_log_formatter import TableLogFormatter, setup_table_logging
from logging_mixins import TableLoggingMixin


# Example 1: Direct use of TableFormatter
def demo_direct_table_formatter():
    """Demonstrate direct use of TableFormatter."""
    # Sample configuration data
    config = {
        "Database": "production.db",
        "Batch size": 1000,
        "Threads": 8,
        "Max connections": 4,
        "Timeout": 30,
        "Retry attempts": 3,
        "Log level": "INFO",
    }

    # Format as a table
    table = TableFormatter.format_table(config, title="Database Configuration")
    print("\n=== Example 1: Direct use of TableFormatter ===")
    print(table)


# Example 2: Using TableLogFormatter with standard logging
def demo_table_log_formatter():
    """Demonstrate using TableLogFormatter with standard logging."""
    # Configure logging with TableLogFormatter
    logger = logging.getLogger("demo_table_formatter")
    logger.setLevel(logging.INFO)

    # Clear any existing handlers
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    # Add a handler with TableLogFormatter
    handler = logging.StreamHandler()
    formatter = TableLogFormatter(
        fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # Log a message that will be formatted as a table
    print("\n=== Example 2: Using TableLogFormatter with standard logging ===")
    logger.info(
        """
    ReaderWriterPair: Processing 16683210 rows
    Batch size: 1000.
    Number of batches: 16684.
    Writer batch chunking: 50.
    Max queue size: 1000.
    Profiling enabled: Reader=False, Writer=False. Reader threads: Started 32/32.
    Writing thread: True
    """
    )


# Example 3: Using the setup_table_logging helper function
def demo_setup_table_logging():
    """Demonstrate using the setup_table_logging helper function."""
    # Create a new logger
    logger = logging.getLogger("demo_table_logging_setup")

    # Clear any existing handlers
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    # Set up table logging
    setup_table_logging(logger)

    # Log a message that will be formatted as a table
    print("\n=== Example 3: Using the setup_table_logging helper function ===")
    logger.info(
        """
    ReaderWriterPair: Processing 9875321 rows
    Batch size: 500.
    Number of batches: 19751.
    Writer batch chunking: 25.
    Max queue size: 500.
    Profiling enabled: Reader=True, Writer=True. Reader threads: Started 16/16.
    Writing thread: True
    """
    )


# Example 4: Using the TableLoggingMixin with a class
class DataProcessor(TableLoggingMixin):
    """Example class that uses TableLoggingMixin."""

    def __init__(self, config: Dict[str, Any]):
        """Initialize with configuration."""
        self.config = config
        self.logger = logging.getLogger("DataProcessor")
        self.logger.setLevel(logging.INFO)

        # Clear any existing handlers
        for handler in self.logger.handlers[:]:
            self.logger.removeHandler(handler)

        # Add a handler
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

        # Initialize processing stats
        self.stats = {
            "processed_rows": 0,
            "failed_rows": 0,
            "processing_time": 0,
            "start_time": time.time(),
        }

    def process(self, rows: int):
        """Simulate processing rows."""
        self.logger.info(f"Processing {rows} rows...")
        time.sleep(0.5)  # Simulate processing time

        # Update stats
        self.stats["processed_rows"] += rows
        self.stats["processing_time"] = time.time() - self.stats["start_time"]

        # Log stats as a table
        self.log_table(self.stats, title="Processing Statistics", level=logging.INFO)

    def start(self):
        """Start processing with logged configuration."""
        self.log_table(
            self.config, title="Processing Configuration", level=logging.INFO
        )

        # Process some rows
        self.process(1000)
        self.process(2000)

        # Log config as a table directly from object attributes
        self.log_config_as_table(title="Final Configuration")


def demo_table_logging_mixin():
    """Demonstrate using the TableLoggingMixin."""
    config = {
        "input_file": "large_dataset.csv",
        "output_file": "processed_results.csv",
        "batch_size": 500,
        "num_threads": 4,
        "timeout": 60,
        "retry_enabled": True,
        "max_retries": 3,
    }

    print("\n=== Example 4: Using the TableLoggingMixin with a class ===")
    processor = DataProcessor(config)
    processor.start()


def demo_reader_writer_log():
    """Demonstrate formatting a ReaderWriterPair log message."""
    rwp_log = """
    ReaderWriterPair: Processing 16683210 rows
    Batch size: 1000.
    Number of batches: 16684.
    Writer batch chunking: 50.
    Max queue size: 1000.
    Profiling enabled: Reader=False, Writer=False. Reader threads: Started 32/32.
    Writing thread: True
    """

    print("\n=== Example 5: Specifically formatting ReaderWriterPair logs ===")
    formatted = TableFormatter.from_reader_writer_log(rwp_log)
    print(formatted)


if __name__ == "__main__":
    demo_direct_table_formatter()
    demo_table_log_formatter()
    demo_setup_table_logging()
    demo_table_logging_mixin()
    demo_reader_writer_log()
