import logging

"""
Database module initialization
"""
from .db_main import EasyNerDBHandler
from .db_statistics import DBStatistics
from .db_data_exchanger import DBDataExchanger

__all__ = [
    'EasyNerDBHandler',
    'DBStatistics',
    'DBDataExchanger'
]

from .core.db_manager import DatabaseManager
from .core.core_classes import BaseLogger, BaseExecutor # Import BaseExecutor
from .core.threading_mp.threading_reader_writer import ReaderWriterPair
from .core.threading_mp.parallel_executor import ParallelExecutor
from .core.cleanup.cleanup_mangager import CleanupManager
from .analysis.analyzer import DataAnalyzer # Import analysis module components
from .db_main import EasyNerDBHandler
from .db_statistics import DBStatistics
from .db_data_exchanger import DBDataExchanger

# Import the new module classes
from .data_model.named_entity import NamedEntity
from .data_model.entity_occurrence import EntityOccurence
from .data_model.entity_cooccurrence import EntityCooccurence
from .data_model.docs import Docs
from .data_model.sent import Sentence



class DatabaseSystem:
    """Single entry point to the database module."""

    def __init__(self, db_path, log_level=logging.INFO):
        # Configure logging globally or within DatabaseSystem if preferred
        logging.basicConfig(level=log_level)
        self.core_logger = BaseLogger("db_core_logger") # Core Logger Instance
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
        return self # Allows db_system.core.log_message() etc., but not ideal, refine if needed.

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

