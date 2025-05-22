"""Database connections provider module for EasyNer.

This module centralizes database connection functionality to avoid circular imports
between the server and route modules.
"""

import logging
import threading
from contextlib import contextmanager

from flask import has_request_context

from easyner.database.sqlite_backend.core.connection_pool import ConnectionPool
from easyner.database.sqlite_backend.monitoring import (
    DBConnectionMonitor,
    OperationMonitor,
)

# Initialize thread-local storage for database connections
db_connections = threading.local()

# Default logger - will be replaced with app.logger once app is initialized
logger = logging.getLogger(__name__)

# Monitors that will be initialized with app.logger
connection_monitor = None
operation_monitor = None

# Connection pool that will be initialized with app.logger
db_pool = None


def initialize(app):
    """Initialize the database connections module with the Flask app."""
    global logger, connection_monitor, operation_monitor, db_pool

    # Set up logger
    logger = app.logger

    # Set up monitors
    connection_monitor = DBConnectionMonitor(app.logger)
    operation_monitor = OperationMonitor(app.logger)

    # Set up connection pool
    import os

    db_pool = ConnectionPool(
        max_connections=int(os.environ.get("EASYNER_POOL_SIZE", "5")),
        idle_timeout=int(os.environ.get("EASYNER_POOL_TIMEOUT", "300")),
    )

    logger.info("Database connections module initialized")


@contextmanager
def get_db_easyner_context_connection():
    """Get a database connection from the pool with monitoring."""
    if connection_monitor is None or operation_monitor is None or db_pool is None:
        msg = "Database connections module not initialized"
        raise RuntimeError(msg)

    try:
        # Start monitoring the database connection operation
        with operation_monitor.monitor_operation("get_db_connection"):
            # Get connection from pool instead of creating a new one
            with db_pool.get_connection() as connection:
                # Register the connection with the monitor
                connection_monitor.register_connection(
                    connection.conn,
                    context={
                        "thread_id": threading.get_ident(),
                        "request_connection": has_request_context(),
                    },
                )

                logger.debug(
                    "Using connection from pool",
                    extra={
                        "thread_id": threading.get_ident(),
                        "conn_id": id(connection.conn),
                    },
                )

                # Yield the connection to the caller
                try:
                    yield connection
                finally:
                    # Unregister from monitor
                    connection_monitor.unregister_connection(connection.conn)
    except Exception as e:
        logger.error(f"Database connection error: {e}", exc_info=True)
        operation_monitor.monitor_exception(
            e,
            context={
                "operation": "database_operation",
                "thread_id": threading.get_ident(),
            },
        )
        raise


def get_db_easyner():
    """Get the database handler using the connection manager."""
    with get_db_easyner_context_connection() as db:
        return db


def close_db_connections():
    """Close all database connections during cleanup."""
    if logger is None or connection_monitor is None or db_pool is None:
        return

    logger.info("Closing all database connections")

    # Use connection monitor to check for leaked connections
    leaks = connection_monitor.check_for_leaks()
    if leaks:
        logger.warning(f"Found {len(leaks)} potentially leaked connections")

    # Close all connections
    connection_monitor.close_all()

    # Shutdown the connection pool
    db_pool.shutdown()
