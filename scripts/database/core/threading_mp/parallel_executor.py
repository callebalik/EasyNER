import concurrent.futures
from ..core_classes import BaseLogger
from ..db_manager import DatabaseManager

class ParallelExecutor:
    """Executes database operations in parallel using threads or processes."""

    def __init__(self, db_manager: DatabaseManager, logger: BaseLogger):
        self.db_manager = db_manager
        self.logger = logger

    def run_in_threads(self, tasks, max_workers=5):
        """Executes tasks in parallel using threads."""
        results = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_tasks = [executor.submit(self._execute_task_in_thread, task) for task in tasks]
            for future in concurrent.futures.as_completed(future_tasks):
                try:
                    results.append(future.result())
                except Exception as e:
                    self.logger.error(f"Thread task failed: {e}")
                    results.append(None) # Or handle error differently
        return results

    def run_in_processes(self, tasks, max_workers=2):
        """Executes tasks in parallel using processes (consider SQLite limitations)."""
        results = []
        with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
            future_tasks = [executor.submit(self._execute_task_in_process, task) for task in tasks]
            for future in concurrent.futures.as_completed(future_tasks):
                try:
                    results.append(future.result())
                except Exception as e:
                    self.logger.error(f"Process task failed: {e}")
                    results.append(None) # Or handle error differently
        return results

    def _execute_task_in_thread(self, task):
        """Executes a task within a thread ensuring thread-local connection."""
        # SQLite handles concurrency well for reads, but writes need care.
        # If tasks are write-heavy, consider connection management per thread.
        try:
            return task(self.db_manager, self.logger) # Pass db_manager & logger to task
        except Exception as e:
            self.logger.error(f"Error executing task in thread: {e}")
            raise

    def _execute_task_in_process(self, task):
        """Executes a task within a process (requires careful serialization)."""
        # Ensure tasks and arguments are serializable for multiprocessing.
        try:
            return task(self.db_manager, self.logger) # Pass db_manager & logger to task
        except Exception as e:
            self.logger.error(f"Error executing task in process: {e}")
            raise