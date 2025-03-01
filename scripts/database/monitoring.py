"""
Monitoring module for EasyNer database operations.
Provides comprehensive logging, performance tracking, and resource monitoring.
"""
from typing import Dict, Any, Optional, List, Tuple
from contextlib import contextmanager
import time
import os
import logging
import sqlite3
import threading
import traceback
import psutil
import json
from datetime import datetime
from flask import request

# Try to import the table formatter if available
try:
    from ..utils.table_log_formatter import TableLogFormatter
except ImportError:
    TableLogFormatter = None


class OperationMonitor:
    """Monitor database operations with comprehensive logging."""

    def __init__(self, logger):
        self.logger = logger
        self.metrics = {}
        self.process = psutil.Process()
        self._local = threading.local()
        self._operation_counts = {
            'total': 0,
            'success': 0,
            'error': 0
        }
        # Configure alert thresholds from environment or defaults
        self.thresholds = {
            'memory_percent': float(os.environ.get('EASYNER_ALERT_THRESHOLD_MEMORY', 80.0)),
            'cpu_percent': float(os.environ.get('EASYNER_ALERT_THRESHOLD_CPU', 90.0)),
            'disk_percent': float(os.environ.get('EASYNER_ALERT_THRESHOLD_DISK', 85.0)),
            'error_rate': float(os.environ.get('EASYNER_ALERT_THRESHOLD_ERRORS', 5.0)),
            'slow_query_ms': float(os.environ.get('EASYNER_ALERT_THRESHOLD_SLOW_QUERY', 1000.0))
        }

    def _get_memory_usage(self) -> Dict[str, float]:
        """Get current memory usage statistics."""
        try:
            mem_info = self.process.memory_info()
            virtual_memory = psutil.virtual_memory()
            return {
                'rss': mem_info.rss,  # Resident Set Size
                'vms': mem_info.vms,  # Virtual Memory Size
                'percent': self.process.memory_percent(),
                'available': virtual_memory.available,
                'total': virtual_memory.total
            }
        except Exception as e:
            self.logger.warning(f"Error getting memory usage: {e}")
            return {'error': str(e)}

    def _get_cpu_usage(self) -> Dict[str, float]:
        """Get current CPU usage statistics."""
        try:
            return {
                'percent': self.process.cpu_percent(),
                'system': psutil.cpu_percent(interval=0.1)
            }
        except Exception as e:
            self.logger.warning(f"Error getting CPU usage: {e}")
            return {'error': str(e)}

    def _get_disk_usage(self, path: str = None) -> Dict[str, Any]:
        """Get disk usage statistics for the specified path or DB path."""
        try:
            if path is None:
                # Default to the current working directory
                path = os.getcwd()

            disk_usage = psutil.disk_usage(path)
            return {
                'total': disk_usage.total,
                'used': disk_usage.used,
                'free': disk_usage.free,
                'percent': disk_usage.percent
            }
        except Exception as e:
            self.logger.warning(f"Error getting disk usage for {path}: {e}")
            return {'error': str(e)}

    def _get_connection_stats(self, conn: sqlite3.Connection) -> Dict[str, Any]:
        """Get SQLite connection statistics."""
        try:
            stats = {}

            # Get SQLite statistics
            pragma_results = {}
            for pragma in ['journal_mode', 'synchronous', 'foreign_keys', 'cache_size']:
                try:
                    cursor = conn.execute(f"PRAGMA {pragma}")
                    result = cursor.fetchone()
                    pragma_results[pragma] = result[0] if result else None
                except Exception as e:
                    pragma_results[pragma] = f"Error: {e}"

            stats.update(pragma_results)

            # Get memory usage if available
            try:
                cursor = conn.execute("PRAGMA memory_used")
                memory_used = cursor.fetchone()
                if memory_used:
                    stats['memory_used'] = memory_used[0]

                cursor = conn.execute("PRAGMA memory_highwater")
                memory_highwater = cursor.fetchone()
                if memory_highwater:
                    stats['memory_highwater'] = memory_highwater[0]
            except:
                pass

            # Get database size
            try:
                cursor = conn.execute("PRAGMA page_count")
                page_count = cursor.fetchone()[0]

                cursor = conn.execute("PRAGMA page_size")
                page_size = cursor.fetchone()[0]

                stats['database_size'] = page_count * page_size
            except Exception as e:
                stats['database_size_error'] = str(e)

            return stats
        except Exception as e:
            self.logger.warning(f"Error getting connection stats: {e}")
            return {'error': str(e)}

    def _check_alerts(self, stats: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Check for alert conditions based on collected metrics."""
        alerts = []

        # Check memory threshold
        if 'memory' in stats and 'percent' in stats['memory']:
            if stats['memory']['percent'] > self.thresholds['memory_percent']:
                alerts.append({
                    'type': 'MEMORY_HIGH',
                    'level': 'WARNING',
                    'message': f"Memory usage at {stats['memory']['percent']:.1f}% exceeds threshold of {self.thresholds['memory_percent']}%",
                    'value': stats['memory']['percent'],
                    'threshold': self.thresholds['memory_percent']
                })

        # Check CPU threshold
        if 'cpu' in stats and 'percent' in stats['cpu']:
            if stats['cpu']['percent'] > self.thresholds['cpu_percent']:
                alerts.append({
                    'type': 'CPU_HIGH',
                    'level': 'WARNING',
                    'message': f"CPU usage at {stats['cpu']['percent']:.1f}% exceeds threshold of {self.thresholds['cpu_percent']}%",
                    'value': stats['cpu']['percent'],
                    'threshold': self.thresholds['cpu_percent']
                })

        # Check disk threshold
        if 'disk' in stats and 'percent' in stats['disk']:
            if stats['disk']['percent'] > self.thresholds['disk_percent']:
                alerts.append({
                    'type': 'DISK_HIGH',
                    'level': 'WARNING',
                    'message': f"Disk usage at {stats['disk']['percent']:.1f}% exceeds threshold of {self.thresholds['disk_percent']}%",
                    'value': stats['disk']['percent'],
                    'threshold': self.thresholds['disk_percent']
                })

        # Check error rate threshold
        if self._operation_counts['total'] > 0:
            error_rate = (self._operation_counts['error'] / self._operation_counts['total']) * 100
            if error_rate > self.thresholds['error_rate']:
                alerts.append({
                    'type': 'ERROR_RATE_HIGH',
                    'level': 'WARNING',
                    'message': f"Error rate at {error_rate:.1f}% exceeds threshold of {self.thresholds['error_rate']}%",
                    'value': error_rate,
                    'threshold': self.thresholds['error_rate']
                })

        # Check slow query threshold
        if 'duration_ms' in stats and stats['duration_ms'] > self.thresholds['slow_query_ms']:
            alerts.append({
                'type': 'SLOW_QUERY',
                'level': 'WARNING',
                'message': f"Slow query: {stats['duration_ms']:.1f}ms exceeds threshold of {self.thresholds['slow_query_ms']}ms",
                'value': stats['duration_ms'],
                'threshold': self.thresholds['slow_query_ms'],
                'query': stats.get('query', 'Unknown')
            })

        return alerts

    def _emit_alerts(self, alerts: List[Dict[str, Any]]):
        """Emit any triggered alerts to the log."""
        for alert in alerts:
            if alert['level'] == 'WARNING':
                self.logger.warning(alert['message'], extra={'alert': alert})
            elif alert['level'] == 'ERROR':
                self.logger.error(alert['message'], extra={'alert': alert})
            elif alert['level'] == 'CRITICAL':
                self.logger.critical(alert['message'], extra={'alert': alert})

    def format_connection_info(self, db_info: Dict[str, Any]) -> str:
        """Format database connection information as a table."""
        if TableLogFormatter is None:
            # If table formatter not available, use simple format
            return "\n".join([f"{k}: {v}" for k, v in db_info.items()])

        formatter = TableLogFormatter(
            title="Database connection initialized",
            headers=["Parameter", "Value"],
            min_width=80
        )

        for key, value in db_info.items():
            if isinstance(value, dict):
                formatter.add_row([key, json.dumps(value)])
            else:
                formatter.add_row([key, str(value)])

        return formatter.render()

    @contextmanager
    def monitor_operation(self, operation_name: str, context: Dict[str, Any] = None, conn: sqlite3.Connection = None):
        """Monitor an operation with timing and resource tracking.

        Args:
            operation_name: Name of operation to monitor
            context: Additional context for logging
            conn: Optional SQLite connection for monitoring
        """
        if context is None:
            context = {}

        start_time = time.time()
        start_memory = self._get_memory_usage()
        thread_id = threading.get_ident()
        transaction_id = f"{operation_name}_{thread_id}_{int(start_time * 1000)}"

        # Add operation context to thread local storage
        if not hasattr(self._local, 'operations'):
            self._local.operations = []
        self._local.operations.append({
            'id': transaction_id,
            'name': operation_name,
            'start_time': start_time,
            'context': context
        })

        self._operation_counts['total'] += 1

        # Initial stats
        stats = {
            'memory': start_memory,
            'operation': operation_name,
            'transaction_id': transaction_id,
            'thread_id': thread_id,
            'start_time': datetime.fromtimestamp(start_time).isoformat()
        }

        if conn:
            stats['connection'] = self._get_connection_stats(conn)

        self.logger.info(f"Starting {operation_name}", extra={
            'context': context,
            'stats': stats,
            'transaction_id': transaction_id
        })

        try:
            yield

            end_time = time.time()
            duration = end_time - start_time
            duration_ms = duration * 1000
            end_memory = self._get_memory_usage()
            memory_delta = {}

            # Calculate memory delta
            for key, end_value in end_memory.items():
                if key in start_memory and isinstance(end_value, (int, float)) and isinstance(start_memory[key], (int, float)):
                    memory_delta[key] = end_value - start_memory[key]

            # End stats
            stats.update({
                'cpu': self._get_cpu_usage(),
                'memory': end_memory,
                'memory_delta': memory_delta,
                'disk': self._get_disk_usage(),
                'duration': duration,
                'duration_ms': duration_ms,
                'end_time': datetime.fromtimestamp(end_time).isoformat(),
                'status': 'success'
            })

            if conn:
                stats['connection'] = self._get_connection_stats(conn)

            # Check for alerts
            alerts = self._check_alerts(stats)
            if alerts:
                stats['alerts'] = alerts
                self._emit_alerts(alerts)

            self._operation_counts['success'] += 1
            self.logger.info(f"Completed {operation_name} in {duration_ms:.1f}ms", extra={
                'context': context,
                'stats': stats,
                'transaction_id': transaction_id
            })

        except Exception as e:
            end_time = time.time()
            duration = end_time - start_time
            duration_ms = duration * 1000

            error_details = {
                'error_type': type(e).__name__,
                'error_message': str(e),
                'traceback': traceback.format_exc()
            }

            stats.update({
                'duration': duration,
                'duration_ms': duration_ms,
                'end_time': datetime.fromtimestamp(end_time).isoformat(),
                'status': 'error',
                'error': error_details
            })

            self._operation_counts['error'] += 1
            self.logger.error(f"Failed {operation_name}: {str(e)}", extra={
                'duration': duration,
                'duration_ms': duration_ms,
                'context': context,
                'error': error_details,
                'stats': stats,
                'transaction_id': transaction_id
            })

            raise
        finally:
            # Clean up thread local storage
            if hasattr(self._local, 'operations'):
                ops = [op for op in self._local.operations if op['id'] != transaction_id]
                self._local.operations = ops

    @contextmanager
    def monitor_query(self, query: str, params: List = None, context: Dict[str, Any] = None, conn: sqlite3.Connection = None):
        """Monitor a database query with timing and resource tracking.

        Args:
            query: SQL query to monitor
            params: Query parameters
            context: Additional context for logging
            conn: Optional SQLite connection for monitoring
        """
        if params is None:
            params = []

        if context is None:
            context = {}

        # Add query details to context
        query_context = {
            'query': query,
            'params': params,
            **context
        }

        with self.monitor_operation('database_query', query_context, conn):
            yield

    def monitor_exception(self, exception: Exception, context: Dict[str, Any] = None):
        """Log an exception with context."""
        if context is None:
            context = {}

        self.logger.error(
            f"Exception: {type(exception).__name__} - {str(exception)}",
            exc_info=exception,
            extra={
                'context': context,
                'error_type': type(exception).__name__,
                'error_message': str(exception),
                'traceback': traceback.format_exc()
            }
        )

    def get_open_transactions(self) -> List[Dict[str, Any]]:
        """Get list of currently open transactions."""
        if not hasattr(self._local, 'operations'):
            return []

        return self._local.operations.copy()

    def register_periodic_monitor(self, app, interval: int = 60):
        """Register a periodic health check with a Flask application.

        Args:
            app: Flask application
            interval: Monitoring interval in seconds
        """
        @app.before_request
        def start_request():
            request_start = time.time()
            self._local.request_start = request_start

        @app.after_request
        def log_request(response):
            if not hasattr(self._local, 'request_start'):
                return response

            request_duration = time.time() - self._local.request_start
            request_duration_ms = request_duration * 1000

            # Log slow requests (over 1000ms)
            if request_duration_ms > 1000:
                self.logger.warning(
                    f"Slow request: {request.method} {request.path} took {request_duration_ms:.1f}ms",
                    extra={
                        'request_method': request.method,
                        'request_path': request.path,
                        'duration_ms': request_duration_ms,
                        'status_code': response.status_code
                    }
                )

            return response


class DBConnectionMonitor:
    """Monitor database connections and ensure proper closing."""

    def __init__(self, logger):
        self.logger = logger
        self.connections = {}
        self.lock = threading.RLock()

    def register_connection(self, conn: sqlite3.Connection, context: Dict[str, Any] = None):
        """Register a SQLite connection for monitoring."""
        with self.lock:
            conn_id = id(conn)
            thread_id = threading.get_ident()

            # Store connection with context
            self.connections[conn_id] = {
                'conn': conn,
                'thread_id': thread_id,
                'created_at': time.time(),
                'stack_trace': traceback.format_stack(),
                'context': context or {}
            }

            self.logger.debug(f"Registered connection {conn_id} from thread {thread_id}")

    def unregister_connection(self, conn: sqlite3.Connection):
        """Unregister a connection when it's properly closed."""
        with self.lock:
            conn_id = id(conn)
            if conn_id in self.connections:
                del self.connections[conn_id]
                self.logger.debug(f"Unregistered connection {conn_id}")

    def get_open_connections(self) -> List[Dict[str, Any]]:
        """Get information about open connections."""
        with self.lock:
            result = []
            current_time = time.time()

            for conn_id, info in self.connections.items():
                result.append({
                    'conn_id': conn_id,
                    'thread_id': info['thread_id'],
                    'age_seconds': current_time - info['created_at'],
                    'context': info['context']
                })

            return result

    def check_for_leaks(self, age_threshold: float = 300.0) -> List[Dict[str, Any]]:
        """Check for potentially leaked connections (open too long)."""
        with self.lock:
            leaks = []
            current_time = time.time()

            for conn_id, info in self.connections.items():
                age = current_time - info['created_at']
                if age > age_threshold:
                    leak_info = {
                        'conn_id': conn_id,
                        'thread_id': info['thread_id'],
                        'age_seconds': age,
                        'context': info['context'],
                        'stack_trace': info['stack_trace']
                    }
                    leaks.append(leak_info)

                    self.logger.warning(
                        f"Possible connection leak: Connection {conn_id} open for {age:.1f} seconds",
                        extra={'leak_info': leak_info}
                    )

            return leaks

    def close_all(self):
        """Close all tracked connections (for emergency shutdown)."""
        with self.lock:
            for conn_id, info in list(self.connections.items()):
                try:
                    info['conn'].close()
                    self.logger.info(f"Closed connection {conn_id} during emergency shutdown")
                except Exception as e:
                    self.logger.error(f"Error closing connection {conn_id}: {e}")

                del self.connections[conn_id]


class ThreadMonitor:
    """Monitor thread health and detect orphaned or stuck threads."""

    def __init__(self, logger):
        self.logger = logger
        self.threads = {}
        self.lock = threading.RLock()

    def register_thread(self, name: str, context: Dict[str, Any] = None):
        """Register a thread for monitoring."""
        with self.lock:
            thread_id = threading.get_ident()
            self.threads[thread_id] = {
                'name': name,
                'start_time': time.time(),
                'last_heartbeat': time.time(),
                'status': 'running',
                'context': context or {}
            }

            self.logger.debug(f"Registered thread {thread_id} ({name})")

    def heartbeat(self, status: str = 'running', progress: Optional[float] = None):
        """Update thread heartbeat to indicate it's still alive."""
        with self.lock:
            thread_id = threading.get_ident()
            if thread_id in self.threads:
                self.threads[thread_id]['last_heartbeat'] = time.time()
                self.threads[thread_id]['status'] = status
                if progress is not None:
                    self.threads[thread_id]['progress'] = progress

    def unregister_thread(self):
        """Unregister a thread when it completes."""
        with self.lock:
            thread_id = threading.get_ident()
            if thread_id in self.threads:
                del self.threads[thread_id]
                self.logger.debug(f"Unregistered thread {thread_id}")

    def check_for_stuck_threads(self, heartbeat_threshold: float = 60.0) -> List[Dict[str, Any]]:
        """Check for potentially stuck threads (no heartbeat)."""
        with self.lock:
            stuck_threads = []
            current_time = time.time()

            for thread_id, info in self.threads.items():
                time_since_heartbeat = current_time - info['last_heartbeat']
                if time_since_heartbeat > heartbeat_threshold:
                    stuck_info = {
                        'thread_id': thread_id,
                        'name': info['name'],
                        'seconds_since_heartbeat': time_since_heartbeat,
                        'total_runtime': current_time - info['start_time'],
                        'status': info['status'],
                        'context': info['context']
                    }
                    stuck_threads.append(stuck_info)

                    self.logger.warning(
                        f"Possible stuck thread: Thread {thread_id} ({info['name']}) "
                        f"no heartbeat for {time_since_heartbeat:.1f} seconds",
                        extra={'stuck_info': stuck_info}
                    )

            return stuck_threads

    def get_active_threads(self) -> List[Dict[str, Any]]:
        """Get information about all active threads."""
        with self.lock:
            result = []
            current_time = time.time()

            for thread_id, info in self.threads.items():
                result.append({
                    'thread_id': thread_id,
                    'name': info['name'],
                    'runtime_seconds': current_time - info['start_time'],
                    'seconds_since_heartbeat': current_time - info['last_heartbeat'],
                    'status': info['status'],
                    'progress': info.get('progress'),
                    'context': info['context']
                })

            return result