# EasyNer Logging & Monitoring Guidelines

You are implementing logging and monitoring for the EasyNer system. Your code must provide comprehensive observability across distributed operations.

ROLE: System Monitor
OBJECTIVE: Create effective logging and monitoring patterns for distributed NER processing and server side data interaction. Store logs in a central location with informative file nameing.

## Logging Requirements

IMPLEMENT:
1. Structured Logging:
   - Operation context
   - Performance metrics
   - Resource states
   - Error details

2. Log Levels:
   DEBUG:
   - Detailed processing steps
   - Memory usage patterns
   - Cache operations
   - SQL query details

   INFO:
   - Batch operations start/end
   - Node allocation events
   - Configuration changes
   - Process milestones

   WARNING:
   - Resource pressure
   - Performance degradation
   - Recoverable errors
   - Missing optional data

   ERROR:
   - Operation failures
   - Data corruption
   - Resource exhaustion
   - Unrecoverable states

   CRITICAL:
   - System crashes
   - Data loss
   - Security breaches
   - Infrastructure failures
   - Orphaned threads
   - Thread integrity errors


## Use utils scripts/utils for table formatting of summary info after processes finish
scripts/utils/log_formatter.py
scripts/utils/logging_mixins.py
scripts/utils/process_pairs_example.py
scripts/utils/table_log_formatter.py
┌────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                Database connection initialized                                 │
├──────────────────────────┼─────────────────────────────────────────────────────────────────────┤
│        Parameter         │                                Value                                │
├──────────────────────────┼─────────────────────────────────────────────────────────────────────┤
│ Database                 │ dev.db                                                              │
│ Path                     │ /lunarc/nobackup/projects/snic2020-6-41/carl/dev.db                 │
│ Path source              │ config file                                                         │
│ Main log (INFO)          │ /home/carloa/Desktop/EasyNer/scripts/database/logs/dev.db.log       │
│ Error log (ERRORS only)  │ /home/carloa/Desktop/EasyNer/scripts/database/logs/dev.db.err       │
│ Debug log (FULL DEBUG)   │ /home/carloa/Desktop/EasyNer/scripts/database/logs/dev.db.debug.log │
│ Row-factory              │ <class 'sqlite3.Row'>                                               │
│ Journal Mode             │ wal                                                                 │
│ Busy Timeout             │ 5000                                                                │
│ Synchronous              │ 2                                                                   │
│ Foreign keys             │ 0                                                                   │
│ Journal size limit       │ -1                                                                  │
│ Max parameter count      │ Not available                                                       │
│ Environment settings     │ {}                                                                  │
│ Mapped I/O (> 1 -> True) │ 0                                                                   │
└──────────────────────────┴─────────────────────────────────────────────────────────────────────┘

## Implementation Pattern
```python
from typing import Dict, Any
from contextlib import contextmanager
import time

class OperationMonitor:
    """Monitor operations with comprehensive logging."""

    def __init__(self, logger: EasyNerDBHandler):
        self.logger = logger
        self.metrics = {}

    @contextmanager
    def monitor_operation(self, operation_name: str, context: Dict[str, Any] = None):
        """Monitor an operation with timing and resource tracking.

        Args:
            operation_name: Name of operation to monitor
            context: Additional context for logging
        """
        start_time = time.time()
        start_memory = self._get_memory_usage()

        try:
            self.logger.info(f"Starting {operation_name}", extra={
                'context': context,
                'memory_start': start_memory,
                'operation': operation_name
            })

            yield

            end_time = time.time()
            end_memory = self._get_memory_usage()

            self.logger.info(f"Completed {operation_name}", extra={
                'duration': end_time - start_time,
                'memory_delta': end_memory - start_memory,
                'context': context,
                'operation': operation_name
            })

        except Exception as e:
            end_time = time.time()
            self.logger.error(f"Failed {operation_name}: {str(e)}", extra={
                'duration': end_time - start_time,
                'error': str(e),
                'context': context,
                'operation': operation_name
            })
            raise
```

## Monitoring Metrics

TRACK:
1. Performance:
   - Processing speed
   - Memory usage
   - I/O operations
   - Network traffic

2. Resources:
   - CPU utilization
   - Memory patterns
   - Disk usage
   - Network bandwidth

3. Operations:
   - Success rates
   - Error counts
   - Duration stats
   - Batch metrics

## Alert Conditions

MONITOR:
1. Resource Thresholds:
   - Memory > 80%
   - CPU > 90%
   - Disk > 85%
   - Error rate > 5%

2. Performance Issues:
   - Slow operations
   - High latency
   - Resource leaks
   - Queue backlog

## Recovery Actions

IMPLEMENT:
1. Resource Recovery:
   - Memory cleanup
   - Connection reset
   - Cache clear
   - Temporary file cleanup

2. Error Recovery:
   - Operation retry
   - Resource reallocation
   - State recovery
   - Alert notification

## Environment Variables

USE:
- EASYNER_LOG_PATH: Log file location
- EASYNER_METRIC_INTERVAL: Monitoring frequency
- EASYNER_ALERT_THRESHOLD: Alert triggers

