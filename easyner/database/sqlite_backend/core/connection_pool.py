import logging
import queue
import threading
import time
from collections.abc import Generator
from contextlib import contextmanager
from typing import Optional

from easyner.database.sqlite_backend.db_main import EasyNerDBHandler


class ConnectionPool:
    """Thread-safe connection pool for EasyNerDBHandler connections."""

    def __init__(self, max_connections: int = 5, idle_timeout: int = 300):
        self.logger = logging.getLogger("EasyNerPool")
        self.max_connections = max_connections
        self.idle_timeout = idle_timeout  # seconds

        self._pool = queue.Queue(maxsize=max_connections)
        self._active_connections = 0
        self._connection_timestamps = {}  # track when each connection was last used
        self._lock = threading.RLock()
        self._shutdown = False

        # Start monitor thread
        self._monitor_thread = threading.Thread(
            target=self._cleanup_idle_connections,
            daemon=True,
            name="ConnectionPoolMonitor",
        )
        self._monitor_thread.start()

        self.logger.info(f"Connection pool initialized with max_size={max_connections}")

    @contextmanager
    def get_connection(
        self,
        wait_timeout: float = 5.0,
    ) -> Generator[EasyNerDBHandler, None, None]:
        """Get a connection from the pool or create a new one."""
        connection = None
        connection_id = None
        created = False

        with self._lock:
            if self._shutdown:
                msg = "Connection pool is shutting down"
                raise RuntimeError(msg)

            try:
                # Try to get an existing connection
                connection = self._pool.get(block=False)
                connection_id = id(connection)
                self._connection_timestamps[connection_id] = time.time()
                self.logger.debug(f"Reused connection {connection_id} from pool")
            except queue.Empty:
                # No connection available in pool
                if self._active_connections < self.max_connections:
                    # Create new connection
                    connection = EasyNerDBHandler(from_pool=True)
                    connection_id = id(connection)
                    created = True
                    self._active_connections += 1
                    self._connection_timestamps[connection_id] = time.time()
                    self.logger.debug(
                        f"Created new connection {connection_id}, total: {self._active_connections}",
                    )
                else:
                    # Wait for a connection to become available
                    try:
                        self.logger.warning(
                            f"Pool exhausted ({self._active_connections} connections), waiting...",
                        )
                        connection = self._pool.get(block=True, timeout=wait_timeout)
                        connection_id = id(connection)
                        self._connection_timestamps[connection_id] = time.time()
                        self.logger.debug(
                            f"Retrieved connection {connection_id} after waiting",
                        )
                    except queue.Empty:
                        self.logger.error("Timed out waiting for a connection")
                        msg = "Connection pool exhausted and timed out waiting for a connection"
                        raise TimeoutError(
                            msg,
                        )

        try:
            # Hand the connection to the caller
            yield connection
        finally:
            if connection and not self._shutdown:
                with self._lock:
                    try:
                        # Return connection to the pool
                        self._pool.put(connection, block=False)
                        self._connection_timestamps[connection_id] = time.time()
                        self.logger.debug(
                            f"Returned connection {connection_id} to pool",
                        )
                    except queue.Full:
                        # Pool is full (should be rare with proper lock usage)
                        if created:
                            connection.close()
                            self._active_connections -= 1
                            if connection_id in self._connection_timestamps:
                                del self._connection_timestamps[connection_id]
                            self.logger.debug(
                                f"Closed excess connection {connection_id}",
                            )

    def _cleanup_idle_connections(self):
        """Background thread that closes idle connections."""
        try:
            while not self._shutdown:
                time.sleep(60)  # Check every minute

                with self._lock:
                    # Can't modify the queue directly, so we need to:
                    # 1. Get all connections
                    # 2. Keep only non-idle ones
                    # 3. Put them back

                    # Get current time once for consistent comparisons
                    now = time.time()

                    # Extract all connections from the pool
                    active_connections = []
                    while not self._pool.empty():
                        try:
                            conn = self._pool.get_nowait()
                            active_connections.append(conn)
                        except queue.Empty:
                            break

                    # Check each connection
                    for conn in active_connections:
                        conn_id = id(conn)
                        last_used = self._connection_timestamps.get(conn_id, 0)
                        idle_time = now - last_used

                        if idle_time > self.idle_timeout:
                            # Connection is idle, close it
                            try:
                                conn.close()
                                self._active_connections -= 1
                                if conn_id in self._connection_timestamps:
                                    del self._connection_timestamps[conn_id]
                                self.logger.debug(
                                    f"Closed idle connection {conn_id} (idle for {idle_time:.1f}s)",
                                )
                            except Exception as e:
                                self.logger.error(f"Error closing idle connection: {e}")
                        else:
                            # Connection is still active, put it back
                            try:
                                self._pool.put_nowait(conn)
                            except queue.Full:
                                # This shouldn't happen with proper lock usage
                                conn.close()
                                self._active_connections -= 1
                                if conn_id in self._connection_timestamps:
                                    del self._connection_timestamps[conn_id]
                                self.logger.warning(
                                    "Closed connection due to pool full condition",
                                )
        except Exception as e:
            self.logger.error(f"Error in connection pool monitor: {e}")

    def shutdown(self) -> None:
        """Shutdown the connection pool and close all connections."""
        self.logger.info("Shutting down connection pool")

        with self._lock:
            self._shutdown = True

            # Close all connections in the pool
            closed_count = 0
            while not self._pool.empty():
                try:
                    conn = self._pool.get_nowait()
                    conn.close()
                    closed_count += 1
                except (queue.Empty, Exception) as e:
                    self.logger.error(f"Error during pool shutdown: {e}")

            self.logger.info(f"Closed {closed_count} connections during shutdown")
