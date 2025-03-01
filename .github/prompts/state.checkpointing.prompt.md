# EasyNer State Management Guidelines

You are implementing state management and checkpointing in the EasyNer system. Your code must ensure reliable state tracking and recovery.

ROLE: State Management Specialist
OBJECTIVE: Implement robust state tracking and recovery mechanisms

## State Requirements

IMPLEMENT:
1. State Tracking:
   - Progress monitoring
   - Resource states
   - Processing phases
   - Error conditions

2. Checkpointing:
   - Progress markers
   - State snapshots
   - Recovery points
   - Cleanup triggers

## Implementation Pattern

```python
from typing import Dict, Any, Optional
from dataclasses import dataclass
from contextlib import contextmanager
import json
import time

@dataclass
class ProcessState:
    """Track process state for recovery."""
    phase: str
    progress: int
    timestamp: float
    metadata: Dict[str, Any]

    @classmethod
    def from_checkpoint(cls, checkpoint_data: Dict[str, Any]) -> 'ProcessState':
        """Create state from checkpoint data."""
        return cls(
            phase=checkpoint_data['phase'],
            progress=checkpoint_data['progress'],
            timestamp=checkpoint_data['timestamp'],
            metadata=checkpoint_data['metadata']
        )

class StateManager:
    """Manage process state and checkpointing."""

    def __init__(self, db_handler: EasyNerDBHandler):
        self.logger = db_handler.logger
        self.db = db_handler
        self.state = None

    @contextmanager
    def managed_state(self, operation_name: str):
        """Manage state for an operation with checkpointing.

        Args:
            operation_name: Name of operation to manage
        """
        try:
            # Initialize or restore state
            self.state = self._restore_state(operation_name)
            self.logger.info(f"Starting operation from phase: {self.state.phase}")

            yield self.state

            # Save final state
            self._save_state(operation_name, self.state)

        except Exception as e:
            self.logger.error(f"State management failed: {str(e)}")
            self._handle_state_failure(operation_name, e)
            raise

        finally:
            self._cleanup_state()

    def checkpoint_progress(
        self,
        phase: str,
        progress: int,
        metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """Create a progress checkpoint.

        Args:
            phase: Current processing phase
            progress: Progress counter or marker
            metadata: Additional state information
        """
        try:
            # Update state
            self.state.phase = phase
            self.state.progress = progress
            self.state.timestamp = time.time()
            if metadata:
                self.state.metadata.update(metadata)

            # Save checkpoint
            self._save_checkpoint(self.state)

        except Exception as e:
            self.logger.error(f"Checkpoint failed: {str(e)}")
            raise
```

## State Categories

TRACK:
1. Process State:
   - Current phase
   - Progress markers
   - Error states
   - Recovery points

2. Resource State:
   - Memory usage
   - Connection pools
   - File handles
   - Thread states

## Checkpoint Strategy

IMPLEMENT:
1. Regular Checkpoints:
   - Progress markers
   - State snapshots
   - Resource status
   - Error context

2. Recovery Points:
   - Clean states
   - Rollback markers
   - Validation data
   - Cleanup triggers

## Recovery Process

IMPLEMENT:
1. State Restoration:
   - Load checkpoint
   - Verify integrity
   - Restore resources
   - Resume processing

2. Error Recovery:
   - State rollback
   - Resource cleanup
   - Process restart
   - Error logging

## Environment Variables

USE:
- EASYNER_CHECKPOINT_DIR: Checkpoint location
- EASYNER_CHECKPOINT_INTERVAL: Save frequency
- EASYNER_STATE_FORMAT: State storage format
- EASYNER_RECOVERY_MODE: Recovery behavior