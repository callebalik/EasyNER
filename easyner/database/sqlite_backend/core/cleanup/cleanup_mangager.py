import threading
import time

import schedule

from ..core_classes import BaseLogger, DatabaseManager


class CleanupManager:
    """Manages periodic database cleanup tasks."""

    def __init__(self, db_manager: DatabaseManager, logger: BaseLogger):
        self.db_manager = db_manager
        self.logger = logger
        self._scheduled_tasks = (
            []
        )  # List of (task_id, schedule_str, task_callable, args, kwargs)
        self._scheduler_thread = None
        self._is_running = False

    def register_task(self, task_id, schedule_str, task_callable, *args, **kwargs) -> None:
        """Registers a cleanup task with its schedule."""
        self._scheduled_tasks.append(
            {
                "task_id": task_id,
                "schedule_str": schedule_str,
                "task_callable": task_callable,
                "args": args,
                "kwargs": kwargs,
            },
        )
        self.logger.info(
            f"Cleanup task '{task_id}' registered with schedule '{schedule_str}'.",
        )

    def schedule_tasks(self) -> None:
        """Schedules all registered cleanup tasks using the 'schedule' library."""
        if self._is_running:
            self.logger.warning("Cleanup scheduler is already running.")
            return

        schedule_fns = {
            "every_minute": schedule.every().minute.do,
            "hourly": schedule.every().hour.do,
            "daily": schedule.every().day.at,  # Needs time argument
            "weekly": schedule.every().week.on,  # Needs weekday argument
            # ... add more schedule types as needed
        }

        for task_config in self._scheduled_tasks:
            task_id = task_config["task_id"]
            schedule_str = task_config["schedule_str"]
            task_callable = task_config["task_callable"]
            args = task_config["args"]
            kwargs = task_config["kwargs"]

            try:
                schedule_parts = schedule_str.split()
                schedule_type = schedule_parts[0]
                schedule_args = schedule_parts[1:]

                if schedule_type in schedule_fns:
                    schedule_job = schedule_fns[schedule_type](
                        self._run_cleanup_task,
                        task_id=task_id,
                        task_callable=task_callable,
                        args=args,
                        kwargs=kwargs,
                    )
                    if schedule_type == "daily" and schedule_args:
                        schedule_job.at(schedule_args[0])  # Set time for daily tasks
                    elif schedule_type == "weekly" and schedule_args:
                        schedule_job.on(
                            schedule_args[0],
                        )  # Set weekday for weekly tasks

                else:
                    self.logger.error(
                        f"Invalid schedule type '{schedule_type}' for task '{task_id}'.",
                    )
                    continue

                self.logger.info(f"Task '{task_id}' scheduled for {schedule_str}.")

            except Exception as e:
                self.logger.error(f"Error scheduling task '{task_id}': {e}")

        self._is_running = True
        self._scheduler_thread = threading.Thread(target=self._run_scheduler_loop)
        self._scheduler_thread.daemon = (
            True  # Allow main thread to exit without waiting
        )
        self._scheduler_thread.start()
        self.logger.info("Cleanup scheduler started.")

    def _run_scheduler_loop(self):
        """Runs the scheduling loop in a separate thread."""
        while self._is_running:
            schedule.run_pending()
            time.sleep(1)  # Check for pending tasks every second

    def _run_cleanup_task(self, task_id, task_callable, args, kwargs):
        """Executes a single cleanup task."""
        self.logger.info(f"Running cleanup task: '{task_id}'...")
        start_time = time.time()
        try:
            task_callable(
                self.db_manager, self.logger, *args, **kwargs,
            )  # Pass db_manager and logger
            end_time = time.time()
            duration = end_time - start_time
            self.logger.info(
                f"Cleanup task '{task_id}' completed in {duration:.4f} seconds.",
            )
        except Exception as e:
            self.logger.error(f"Cleanup task '{task_id}' failed: {e}")

    def get_scheduled_tasks(self):
        """Returns a list of scheduled task configurations."""
        return self._scheduled_tasks

    def remove_task(self, task_id) -> None:
        """Removes a registered task."""
        self._scheduled_tasks = [
            task for task in self._scheduled_tasks if task["task_id"] != task_id
        ]
        self.logger.info(f"Cleanup task '{task_id}' removed.")

    def stop_scheduler(self) -> None:
        """Stops the cleanup scheduler thread."""
        if self._is_running:
            self._is_running = False
            if self._scheduler_thread and self._scheduler_thread.is_alive():
                self._scheduler_thread.join(
                    timeout=5,
                )  # Wait for thread to finish gracefully
            self.logger.info("Cleanup scheduler stopped.")
        else:
            self.logger.warning("Cleanup scheduler is not running.")

    def add_default_tasks(self) -> None:
        """Registers default cleanup tasks."""
        self.register_task(
            task_id="vacuum_db",
            schedule_str="daily 03:00",  # Daily at 3 AM
            task_callable=vacuum_database_task,
        )
        self.register_task(
            task_id="delete_old_logs",
            schedule_str="weekly monday",  # Weekly on Mondays
            task_callable=delete_old_logs_task,
            table_name="logs",
            date_column="timestamp",
            days_ago=90,  # Keep logs for 90 days
        )
        self.logger.info("Default cleanup tasks registered.")
