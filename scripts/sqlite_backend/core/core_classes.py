import logging
import time
import sqlite3
import logging
import os

from .db_manager import DatabaseManager
from .logger import BaseLogger


class BaseExecutor:
    """Base class for executing database operations with enhanced features."""

    def __init__(self, db_manager: DatabaseManager, logger: BaseLogger):
        self.db_manager = db_manager
        self.logger = logger

    def execute_operation(self, operation_func, *args, **kwargs):
        """Executes a database operation with error handling, transaction, timing, and logging."""
        start_time = time.time()
        conn = self.db_manager.get_connection()
        try:
            with conn:  # Use context manager for transaction management
                result = operation_func(conn, *args, **kwargs)
                return result
        except sqlite3.Error as e:
            self.logger.error(f"Database operation failed: {e}")
            raise  # Re-raise the exception to be handled by the caller
        finally:
            end_time = time.time()
            duration = end_time - start_time
            self.logger.debug(
                f"Operation '{operation_func.__name__}' executed in {duration:.4f} seconds."
            )
