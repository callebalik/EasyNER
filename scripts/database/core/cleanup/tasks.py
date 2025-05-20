# Example cleanup tasks within core/cleanup/tasks/
from ..core_classes import DatabaseManager
from ..core_classes import BaseLogger


# core/cleanup/tasks/vacuum_task.py
def vacuum_database_task(db_manager: DatabaseManager, logger: BaseLogger):
    """Task to perform VACUUM operation on the database."""
    logger.info("Starting VACUUM database task...")
    try:
        with db_manager.get_connection() as conn:  # Context manager for connection
            conn.execute("VACUUM;")
            conn.commit()
        logger.info("VACUUM database task completed.")
    except Exception as e:
        logger.error(f"VACUUM database task failed: {e}")


# core/cleanup/tasks/delete_old_records_task.py
def delete_old_logs_task(
    db_manager: DatabaseManager, logger: BaseLogger, table_name, date_column, days_ago
):
    """Task to delete old records from a table based on a date column."""
    import datetime

    cutoff_date = datetime.datetime.now() - datetime.timedelta(days=days_ago)
    cutoff_str = cutoff_date.strftime("%Y-%m-%d %H:%M:%S")  # Or appropriate format

    logger.info(f"Starting delete old records task for table '{table_name}'...")
    try:
        query = f"""
            DELETE FROM {table_name}
            WHERE {date_column} <= ?
        """
        with db_manager.get_connection() as conn:  # Context manager for connection
            cursor = conn.cursor()
            cursor.execute(query, (cutoff_str,))
            conn.commit()
            deleted_rows = cursor.rowcount
        logger.info(
            f"Deleted {deleted_rows} old records from table '{table_name}' (older than {cutoff_date})."
        )
    except Exception as e:
        logger.error(f"Delete old records task for table '{table_name}' failed: {e}")
