"""
Table formatter for prettifying log output.

This module provides formatting utilities to convert plain text log messages
into structured tables for better readability of processing metrics.
"""

import re
from typing import Dict, List, Optional, Union, Tuple


class TableFormatter:
    """
    Format log messages as ASCII tables for better readability.

    This formatter is particularly useful for processing metrics and
    configuration details that need to be presented in a structured way.
    """

    @staticmethod
    def format_table(
        data: Dict[str, Union[str, int, float, bool]],
        title: Optional[str] = None,
        column_separator: str = "│",
        edge_char: str = "│",
        header_separator: str = "─",
        min_width: int = 50,
    ) -> str:
        """
        Format dictionary data as a pretty ASCII table.

        Args:
            data: Dictionary of key-value pairs to display in the table
            title: Optional title to display at the top of the table
            column_separator: Character to use for column separation
            edge_char: Character to use for table edges
            header_separator: Character to use for header separation
            min_width: Minimum width of the table in characters

        Returns:
            Formatted table as a string
        """
        # Convert all values to strings
        processed_data = {str(k): str(v) for k, v in data.items()}

        # Calculate column widths
        key_width = max(len(k) for k in processed_data.keys())
        val_width = max(len(v) for v in processed_data.values())

        # Ensure minimum width
        total_width = key_width + val_width + 3  # +3 for separators and spaces
        if total_width < min_width:
            val_width += min_width - total_width

        # Create table format strings
        header_fmt = f"{edge_char} {{:^{key_width}}} {column_separator} {{:^{val_width}}} {edge_char}"
        row_fmt = f"{edge_char} {{:<{key_width}}} {column_separator} {{:<{val_width}}} {edge_char}"
        separator = header_separator * (
            key_width + val_width + 5
        )  # +5 for separators and spaces
        top_line = f"┌{header_separator * (key_width + 2)}┬{header_separator * (val_width + 2)}┐"
        bottom_line = f"└{header_separator * (key_width + 2)}┴{header_separator * (val_width + 2)}┘"
        mid_line = f"├{header_separator * (key_width + 2)}┼{header_separator * (val_width + 2)}┤"

        # Build the table
        lines = []

        # Add title if provided
        if title:
            title_line = f"┌{header_separator * (key_width + val_width + 5)}┐"
            lines.append(title_line)
            lines.append(
                f"{edge_char} {title:^{key_width + val_width + 3}} {edge_char}"
            )
            lines.append(mid_line)
        else:
            lines.append(top_line)

        # Add header
        lines.append(header_fmt.format("Parameter", "Value"))
        lines.append(mid_line)

        # Add data rows
        for k, v in processed_data.items():
            lines.append(row_fmt.format(k, v))

        # Add bottom
        lines.append(bottom_line)

        return "\n".join(lines)

    @staticmethod
    def parse_and_format(log_text: str, title: Optional[str] = None) -> str:
        """
        Parse a log message and format it as a table.

        This method attempts to extract key-value pairs from a log message
        and format them as a table.

        Args:
            log_text: The log text to parse
            title: Optional title for the table

        Returns:
            Formatted table as a string
        """
        data = {}

        # Split the log text into lines and parse each line
        for line in log_text.strip().split("\n"):
            # Try to extract key-value pairs
            kv_match = re.match(r"([^:]+):\s*(.*?)\.?\s*$", line.strip())
            if kv_match:
                key, value = kv_match.groups()
                data[key.strip()] = value.strip()
            else:
                # Try a different format with just key-value
                kv_match = re.match(r"([^:]+):\s*(.*?)\.?\s*$", line.strip())
                if kv_match:
                    key, value = kv_match.groups()
                    data[key.strip()] = value.strip()

        # If no data was extracted, try a more aggressive approach
        if not data:
            for line in log_text.strip().split("\n"):
                parts = line.split(":", 1)
                if len(parts) == 2:
                    key, value = parts
                    data[key.strip()] = value.strip()
                else:
                    # Handle "key value" format without colon
                    parts = line.split(" ", 1)
                    if len(parts) == 2:
                        key, value = parts
                        # Only add if it looks like a valid key-value pair
                        if not key.isdigit() and len(key) > 2:
                            data[key.strip()] = value.strip()

        # If still no data, split by lines and look for patterns
        if not data:
            patterns = [
                (
                    r"(\w+(?:\s+\w+)?)\s*[:=]\s*(.*)",
                    lambda m: (m.group(1), m.group(2)),
                ),  # key: value or key = value
                (
                    r"(\w+(?:\s+\w+)?)\s+((?:\d+(?:\.\d+)?)|(?:True|False|None))\s*\.?",
                    lambda m: (m.group(1), m.group(2)),
                ),  # key 123 or key True
            ]

            for line in log_text.strip().split("\n"):
                for pattern, extract in patterns:
                    match = re.match(pattern, line.strip())
                    if match:
                        key, value = extract(match)
                        data[key.strip()] = value.strip().rstrip(".")
                        break

        if data:
            return TableFormatter.format_table(data, title)
        else:
            # If we couldn't parse it into key-value pairs, just return the original text
            return log_text

    @classmethod
    def from_reader_writer_log(cls, log_text: str) -> str:
        """
        Specifically format ReaderWriterPair logs which have a known structure.

        Args:
            log_text: The ReaderWriterPair log text

        Returns:
            Formatted table as a string
        """
        # Extract info from the ReaderWriterPair log
        data = {}

        # Process text in expected format
        total_match = re.search(r"Processing\s+(\d+)\s+rows", log_text)
        if total_match:
            data["Total rows"] = int(total_match.group(1))

        batch_match = re.search(r"Batch size:\s*(\d+)", log_text)
        if batch_match:
            data["Batch size"] = int(batch_match.group(1))

        batches_match = re.search(r"Number of batches:\s*(\d+)", log_text)
        if batches_match:
            data["Total batches"] = int(batches_match.group(1))

        writer_chunk_match = re.search(r"Writer batch chunking:\s*(\d+)", log_text)
        if writer_chunk_match:
            data["Writer chunk size"] = int(writer_chunk_match.group(1))

        queue_match = re.search(r"Max queue size:\s*(\d+)", log_text)
        if queue_match:
            data["Max queue size"] = int(queue_match.group(1))

        reader_prof_match = re.search(r"Reader=(\w+)", log_text)
        if reader_prof_match:
            data["Reader profiling"] = reader_prof_match.group(1)

        writer_prof_match = re.search(r"Writer=(\w+)", log_text)
        if writer_prof_match:
            data["Writer profiling"] = writer_prof_match.group(1)

        reader_threads_match = re.search(
            r"Reader threads: Started\s+(\d+)/(\d+)", log_text
        )
        if reader_threads_match:
            data["Reader threads running"] = (
                f"{reader_threads_match.group(1)}/{reader_threads_match.group(2)}"
            )

        writing_thread_match = re.search(r"Writing thread:\s*(\w+)", log_text)
        if writing_thread_match:
            data["Writing thread active"] = writing_thread_match.group(1)

        return cls.format_table(data, title="ReaderWriterPair Processing Configuration")


if __name__ == "__main__":
    # Example usage
    sample_log = """
    ReaderWriterPair: Processing 16683210 rows
    Batch size: 1000.
    Number of batches: 16684.
    Writer batch chunking: 50.
    Max queue size: 1000.
    Profiling enabled: Reader=False, Writer=False. Reader threads: Started 32/32.
    Writing thread: True
    """

    formatted = TableFormatter.from_reader_writer_log(sample_log)
    print(formatted)
