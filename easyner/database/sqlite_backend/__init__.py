"""Database module initialization."""

# Core imports first
from easyner.database.sqlite_backend.analysis.analyzer import DataAnalyzer
from easyner.database.sqlite_backend.core.cache_manager import CacheManager
from easyner.database.sqlite_backend.core.cleanup.cleanup_mangager import CleanupManager
from easyner.database.sqlite_backend.core.core_classes import BaseExecutor, BaseLogger
from easyner.database.sqlite_backend.core.db_manager import DatabaseManager
from easyner.database.sqlite_backend.core.threading_mp.parallel_executor import (
    ParallelExecutor,
)

# Data model imports
# Main components - order matters to avoid circular imports
from easyner.database.sqlite_backend.db_data_exchanger import DBDataExchanger
from easyner.database.sqlite_backend.db_main import EasyNerDBHandler
from easyner.database.sqlite_backend.db_statistics.db_statistics_class import (
    DBStatistics,
)

__all__ = ["EasyNerDBHandler", "DBStatistics", "DBDataExchanger", "CacheManager"]

import logging


class DatabaseSystem:
    """Single entry point to the database module."""

    def __init__(self, db_path, log_level=logging.INFO):
        # Configure logging globally or within DatabaseSystem if preferred
        logging.basicConfig(level=log_level)
        self.core_logger = BaseLogger("db_core_logger")  # Core Logger Instance
        self.db_manager = DatabaseManager(db_path)
        self.base_executor = BaseExecutor(self.db_manager, self.core_logger)
        self.parallel_executor = ParallelExecutor(self.db_manager, self.core_logger)
        self.cleanup_manager = CleanupManager(self.db_manager, self.core_logger)
        # self.cleanup_manager.add_default_tasks() # Register default cleanup tasks
        # self.cleanup_manager.schedule_tasks() # Start scheduling cleanup tasks

        # Instantiate modules (Analysis in this example)
        self._analysis = DataAnalyzer(self.db_manager, self.core_logger)

    @property
    def core(self):
        """Access to core components (DatabaseManager, Logger, Executor)."""
        return self  # Allows db_system.core.log_message() etc., but not ideal, refine if needed.

    @property
    def analysis(self):
        """Access to the analysis module."""
        return self._analysis

    @property
    def cleanup(self):
        """Access to the cleanup manager for controlling cleanup tasks directly."""
        return self.cleanup_manager

    def close(self):
        """Closes all database connections and stops schedulers."""
        self.cleanup_manager.stop_scheduler()
        self.db_manager.close_connection()
        self.core_logger.info("Database system shutdown.")


def create_app():
    # """Create and configure the Flask application."""
    # app = Flask(__name__)

    # # Initialize routes and error handlers
    # app = init_routes(app)

    # return app

    import logging

    logging.warning(
        "create_app() in __init__.py is deprecated. Use db_server.py instead.",
    )
    return None


# Create the application instance
# app = create_app()
