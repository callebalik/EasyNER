"""
Custom log formatter with table formatting capabilities.

This module provides a custom logging formatter that can detect certain patterns
in log messages and convert them to pretty-printed tables.
"""

import logging
import re
from typing import Dict, Optional, Pattern

from scripts.utils.log_formatter import TableFormatter


class TableLogFormatter(logging.Formatter):
    """
    A logging formatter that formats certain log messages as tables.

    This formatter detects specific patterns in log messages and converts
    them to pretty-printed tables using the TableFormatter.
    """

    def __init__(
        self,
        fmt: str = None,
        datefmt: str = None,
        style: str = "%",
        validate: bool = True,
        table_patterns: Dict[Pattern, str] = None,
    ):
        """
        Initialize the formatter.

        Args:
            fmt: Format string for log messages
            datefmt: Format string for dates
            style: Style of the fmt string (%, {, or $)
            validate: Whether to validate the format string
            table_patterns: Dictionary mapping regex patterns to table titles
        """
        super().__init__(fmt=fmt, datefmt=datefmt, style=style, validate=validate)

        # Default patterns to detect and format as tables
        self.table_patterns = table_patterns or {
            re.compile(
                r"ReaderWriterPair.*rows"
            ): "ReaderWriterPair Processing Configuration",
            re.compile(r"Batch size:.*Writer.*thread"): "Processing Configuration",
            # Add more patterns as needed
        }

    def format(self, record: logging.LogRecord) -> str:
        """
        Format the log record, converting certain messages to tables.

        Args:
            record: The log record to format

        Returns:
            Formatted log message
        """
        # Get the original formatted message
        original_message = super().format(record)

        # Check if this message matches any of our table patterns
        for pattern, title in self.table_patterns.items():
            if pattern.search(record.getMessage()):
                try:
                    # For ReaderWriterPair logs, use the specialized formatter
                    if "ReaderWriterPair" in record.getMessage():
                        return TableFormatter.from_reader_writer_log(
                            record.getMessage()
                        )
                    # For other messages, use the general parser
                    return TableFormatter.parse_and_format(
                        record.getMessage(), title=title
                    )
                except Exception as e:
                    # If table formatting fails, fall back to original message
                    return f"{original_message}\n(Table formatting failed: {e})"

        # If no patterns match, return the original message
        return original_message


def setup_table_logging(
    logger: logging.Logger,
    fmt: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt: str = "%Y-%m-%d %H:%M:%S",
    level: int = logging.INFO,
) -> None:
    """
    Set up a logger to use table formatting.

    Args:
        logger: The logger to configure
        fmt: Log format string
        datefmt: Date format string
        level: Logging level
    """
    # Create console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)

    # Create formatter
    formatter = TableLogFormatter(fmt=fmt, datefmt=datefmt)
    console_handler.setFormatter(formatter)

    # Add handler to logger
    logger.addHandler(console_handler)
    logger.setLevel(level)


if __name__ == "__main__":
    # Example usage
    logger = logging.getLogger("test_logger")
    setup_table_logging(logger)

    # Test with a ReaderWriterPair log message
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
