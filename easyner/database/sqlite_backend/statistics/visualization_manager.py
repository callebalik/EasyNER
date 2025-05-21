import importlib
import os
import sys
import threading
import time
from contextlib import contextmanager

from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer


class VisualizationFileHandler(FileSystemEventHandler):
    """Handler for visualization file changes."""

    def __init__(self, manager):
        self.manager = manager

    def on_modified(self, event) -> None:
        if event.src_path.endswith(".py"):
            visualization_name = os.path.splitext(os.path.basename(event.src_path))[0]
            module_path = os.path.dirname(event.src_path)
            if module_path not in sys.path:
                sys.path.append(module_path)
            try:
                # Reload the module if it's already loaded
                module_name = f"statistics.{visualization_name}"
                if module_name in sys.modules:
                    importlib.reload(sys.modules[module_name])
                self.manager.invalidate_cache(visualization_name)
            except Exception as e:
                self.manager.app.logger.error(
                    f"Error reloading module {visualization_name}: {e}",
                )


class VisualizationManager:
    def __init__(self, app):
        self.app = app
        self.caches = {}  # Dictionary to store multiple visualization caches
        self.last_modified = {}
        self.cache_path = os.path.join(os.path.dirname(__file__), "cache")
        self.lock = threading.Lock()
        self.thread_local = threading.local()

        os.makedirs(self.cache_path, exist_ok=True)

        # Add the current directory to sys.path if not already there
        current_dir = os.path.dirname(__file__)
        if current_dir not in sys.path:
            sys.path.append(current_dir)

        # Setup file watching
        self.event_handler = VisualizationFileHandler(self)
        self.observer = Observer()
        self.observer.schedule(
            self.event_handler,
            os.path.dirname(__file__),
            recursive=True,  # Changed to True to watch subdirectories
        )
        self.observer.start()
        app.logger.debug("Started visualization file monitoring")

    @contextmanager
    def _get_thread_connection(self, db_source):
        """Get a thread-safe database connection using the provided source.

        Args:
            db_source: Can be:
                - A function that returns a database connection (get_db_simple_connection)
                - A database connection object
                - A database handler object with get_dedicated_connection method

        """
        conn = None
        cursor = None
        # Flag to track if we need to close the connection ourselves
        should_close_conn = False
        # Flag to track if we need to close the cursor ourselves
        should_close_cursor = False

        try:
            # If db_source is a function (get_db_simple_connection), use it to get a connection
            if callable(db_source):
                conn = db_source()
                self.app.logger.debug(
                    "Created connection using connection factory function",
                )
                # Only close connections that we create
                should_close_conn = True
            else:
                # Otherwise assume it's the main db handler or an existing connection
                if hasattr(db_source, "get_dedicated_connection"):
                    # It's a database handler with dedicated connection method
                    conn = db_source.get_dedicated_connection()
                    self.app.logger.debug(
                        "Created dedicated connection from db_handler",
                    )
                    # We should close this dedicated connection
                    should_close_conn = True
                else:
                    # It's already a connection - use it directly
                    conn = db_source
                    self.app.logger.debug("Using provided database connection directly")
                    # We should NOT close an externally provided connection
                    should_close_conn = False

            # Check if the connection has a cursor method or is a cursor already
            if hasattr(conn, "cursor") and callable(conn.cursor):
                cursor = conn.cursor()
                self.app.logger.debug("Created new cursor from connection")
                # We created this cursor, so we should close it
                should_close_cursor = True
            else:
                # It might already be a cursor
                cursor = conn
                self.app.logger.debug("Using provided object directly as cursor")
                # We didn't create this cursor, so don't close it
                should_close_cursor = False

            # Store on thread local storage
            self.thread_local.conn = conn
            self.thread_local.cursor = cursor
            self.thread_local.should_close_conn = should_close_conn
            self.thread_local.should_close_cursor = should_close_cursor

            # Pass back the cursor object for use in the visualization generator
            yield cursor

            # Commit any pending changes if it's an actual connection with commit method
            # and only if we're responsible for managing this connection
            if (
                should_close_conn
                and conn
                and hasattr(conn, "commit")
                and callable(conn.commit)
            ):
                try:
                    conn.commit()
                except Exception as e:
                    self.app.logger.warning(f"Non-fatal error during commit: {e}")

        except Exception as e:
            # Log the error and rollback if needed
            self.app.logger.error(f"Error in thread connection: {e}", exc_info=True)
            if (
                should_close_conn
                and conn
                and hasattr(conn, "rollback")
                and callable(conn.rollback)
            ):
                try:
                    conn.rollback()
                except Exception as rollback_e:
                    self.app.logger.error(f"Error during rollback: {rollback_e}")
            raise
        finally:
            # Always clean up in reverse order, but only for resources we created

            # Only close the cursor if we created it and it's distinct from the connection
            if (
                should_close_cursor
                and cursor
                and cursor != conn
                and hasattr(cursor, "close")
                and callable(cursor.close)
            ):
                try:
                    cursor.close()
                    self.app.logger.debug("Closed cursor that we created")
                except Exception as e:
                    self.app.logger.error(f"Error closing cursor: {e}")

            # Only close the connection if we created it
            if (
                should_close_conn
                and conn
                and hasattr(conn, "close")
                and callable(conn.close)
            ):
                try:
                    conn.close()
                    self.app.logger.debug("Closed connection that we created")
                except Exception as e:
                    self.app.logger.error(f"Error closing connection: {e}")

            # Clear thread local storage
            if hasattr(self.thread_local, "cursor"):
                del self.thread_local.cursor
            if hasattr(self.thread_local, "conn"):
                del self.thread_local.conn
            if hasattr(self.thread_local, "should_close_conn"):
                del self.thread_local.should_close_conn
            if hasattr(self.thread_local, "should_close_cursor"):
                del self.thread_local.should_close_cursor

    def invalidate_cache(self, visualization_name) -> None:
        """Invalidate the cache for a specific visualization."""
        with self.lock:
            if visualization_name in self.caches:
                self.caches[visualization_name] = None
                self.last_modified[visualization_name] = 0
                self.app.logger.debug(
                    f"{visualization_name} visualization cache invalidated",
                )

    def clear_cache(self):
        """Clear all visualization caches."""
        cleared_files = 0
        with self.lock:
            # Clear in-memory cache
            self.caches = {}
            self.last_modified = {}

            # Clear cache files
            if os.path.exists(self.cache_path):
                try:
                    # Remove and recreate cache directory
                    cache_files = [
                        f for f in os.listdir(self.cache_path) if f.endswith(".html")
                    ]
                    cleared_files = len(cache_files)
                    for cache_file in cache_files:
                        file_path = os.path.join(self.cache_path, cache_file)
                        try:
                            os.remove(file_path)
                        except Exception as e:
                            self.app.logger.error(
                                f"Error removing cache file {file_path}: {e}",
                            )
                    self.app.logger.info(
                        f"Cleared {cleared_files} visualization cache files",
                    )
                except Exception as e:
                    self.app.logger.error(f"Error clearing visualization cache: {e}")
        return cleared_files

    def get_cached_visualization(self, visualization_name, db_source, generator_func):
        """Get cached visualization or generate new one if needed.

        Args:
            visualization_name: Name/key for the visualization (used for caching)
            db_source: Database connection source (function, object, handler)
            generator_func: Function that generates the visualization HTML
                           Should accept a database connection/cursor

        Returns:
            HTML content for the visualization

        """
        cache_file = os.path.join(self.cache_path, f"{visualization_name}.html")
        source_file = os.path.join(
            os.path.dirname(__file__),
            f"{visualization_name}.py",
        )

        try:
            with self.lock:
                module_name = f"statistics.{visualization_name}"
                module_modified = (
                    os.path.getmtime(source_file) if os.path.exists(source_file) else 0
                )
                cache_exists = os.path.exists(cache_file)
                cache_modified = os.path.getmtime(cache_file) if cache_exists else 0

                # Check if module was modified or cache is invalid
                needs_update = (
                    visualization_name not in self.caches
                    or self.caches[visualization_name] is None
                    or not cache_exists
                    or module_modified > cache_modified
                    or module_modified > self.last_modified.get(visualization_name, 0)
                )

                if needs_update:
                    # Use thread-safe connection to generate visualization
                    with self._get_thread_connection(db_source) as thread_conn:
                        # Generate new visualization using the thread-safe connection
                        html = generator_func(thread_conn)

                    self.last_modified[visualization_name] = time.time()

                    # Add version-based refresh script that only checks when the file is modified
                    refresh_script = (
                        """
                    <script>
                        const sourceModified = %s;
                        let lastModified = sourceModified;
                        let checkInterval;
                        async function checkForChanges() {
                            try {
                                const response = await fetch(window.location.href, {
                                    headers: {
                                        'If-Modified-Since': new Date(lastModified * 1000).toUTCString()
                                    }
                                });
                                if (response.status === 304) {
                                    return; // No changes
                                }
                                const newModified = response.headers.get('Last-Modified');
                                if (newModified) {
                                    const newModifiedTime = new Date(newModified).getTime() / 1000;
                                    if (newModifiedTime > lastModified) {
                                        window.location.reload();
                                    }
                                }
                            } catch (error) {
                                console.log('Update check failed:', error);
                                clearInterval(checkInterval); // Stop checking on error
                            }
                        }
                        // Start checking after the page loads
                        window.addEventListener('load', () => {
                            checkInterval = setInterval(checkForChanges, 5000);
                        });
                    </script>
                    """
                        % module_modified
                    )

                    # Insert refresh script before closing body tag
                    if "</body>" in html:
                        html = html.replace("</body>", f"{refresh_script}</body>")
                    else:
                        html += refresh_script

                    # Cache the result
                    with open(cache_file, "w") as f:
                        f.write(html)

                    self.caches[visualization_name] = html

                return self.caches.get(visualization_name, "")

        except Exception as e:
            self.app.logger.error(
                f"Error handling cached visualization {visualization_name}: {e}",
                exc_info=True,
            )
            raise

    def get_cache_stats(self):
        """Get statistics about the visualization cache."""
        stats = {
            "in_memory_cache_count": len(self.caches),
            "file_cache_count": 0,
            "cache_size_bytes": 0,
            "cache_items": [],
        }

        try:
            if os.path.exists(self.cache_path):
                cache_files = [
                    f for f in os.listdir(self.cache_path) if f.endswith(".html")
                ]
                stats["file_cache_count"] = len(cache_files)

                for cache_file in cache_files:
                    file_path = os.path.join(self.cache_path, cache_file)
                    try:
                        file_size = os.path.getsize(file_path)
                        file_mtime = os.path.getmtime(file_path)
                        stats["cache_size_bytes"] += file_size
                        stats["cache_items"].append(
                            {
                                "name": os.path.splitext(cache_file)[0],
                                "size_bytes": file_size,
                                "last_modified": file_mtime,
                            },
                        )
                    except Exception as e:
                        self.app.logger.error(
                            f"Error getting stats for cache file {file_path}: {e}",
                        )
        except Exception as e:
            self.app.logger.error(f"Error getting visualization cache stats: {e}")

        return stats

    def __del__(self):
        """Clean up observer on deletion."""
        try:
            self.observer.stop()
            self.observer.join()
        except Exception as e:
            self.app.logger.error(f"Error stopping file observer: {e}")
