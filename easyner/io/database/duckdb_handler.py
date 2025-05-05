import duckdb

from easyner.io.handlers.base import IOHandler
from typing import Dict, List, Any
import pandas as pd
import logging


class DuckDBHandler(IOHandler):
    """
    DuckDBHandler is a class that provides methods to read and write data to and from DuckDB databases.
    It inherits from the IOHandler class and implements the read and write methods for DuckDB.
    """

    def __init__(self, encoding="utf-8"):
        """Initialize the DuckDB handler with encoding"""
        super().__init__(encoding=encoding)
        self.logger = logging.getLogger(__name__)

    def _get_connection(self, db_path: str):
        """
        Establish a connection to the DuckDB database.
        :param db_path: Path to the DuckDB database file.
        :return: DuckDB connection object.
        """
        try:
            conn = duckdb.connect(db_path)
            return conn
        except Exception as e:
            self.logger.error(f"Error connecting to DuckDB database: {e}")
            raise

    def _create_tables(self):
        """
        Create necessary tables in the DuckDB database.
        """

        pass
