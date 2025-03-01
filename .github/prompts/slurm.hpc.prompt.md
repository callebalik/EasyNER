# EasyNer SLURM & HPC Guidelines

You are implementing HPC resource management for the EasyNer system. Your code must efficiently coordinate SLURM jobs and handle distributed processing.

ROLE: HPC Resource Manager
OBJECTIVE: Create efficient distributed processing patterns

## Resource Requirements

IMPLEMENT:
1. Job Management:
   - Task distribution
   - Resource allocation
   - Node coordination
   - Progress tracking

2. Node Operations:
   - Load balancing
   - Resource monitoring
   - Fault tolerance
   - State synchronization

## Implementation Pattern

```python
from typing import List, Dict, Any
from concurrent.futures import ThreadPoolExecutor
import os

class SlurmJobManager:
    """Manage distributed processing across SLURM nodes."""

    def __init__(self, db_handler: EasyNerDBHandler):
        self.logger = db_handler.logger
        self.db = db_handler
        self.node_id = os.getenv('SLURM_NODEID', '0')
        self.total_nodes = int(os.getenv('SLURM_NNODES', '1'))

    def distribute_work(self, work_items: List[Any]) -> None:
        """Distribute work across available nodes.

        Args:
            work_items: List of items to process
        """
        try:
            # Get node-specific work partition
            partition = self._get_node_partition(work_items)
            self.logger.info(f"Node {self.node_id}/{self.total_nodes} starting work")

            with ThreadPoolExecutor(max_workers=8) as executor:
                try:
                    # Process work items with proper checkpointing
                    futures = []
                    for item in partition:
                        future = executor.submit(self._process_item, item)
                        futures.append(future)

                    # Monitor progress and handle failures
                    for future in futures:
                        try:
                            result = future.result()
                            self._update_progress(result)
                        except Exception as e:
                            self.logger.error(f"Work item failed: {str(e)}")
                            self._handle_item_failure(item, e)

                except Exception as e:
                    self.logger.error(f"Node processing failed: {str(e)}")
                    raise

        except Exception as e:
            self.logger.error(f"Work distribution failed: {str(e)}")
            raise

    def _get_node_partition(self, items: List[Any]) -> List[Any]:
        """Get work items for this specific node.

        Args:
            items: Full list of work items

        Returns:
            This node's portion of work
        """
        node_id = int(self.node_id)
        items_per_node = len(items) // self.total_nodes
        start_idx = node_id * items_per_node
        end_idx = start_idx + items_per_node if node_id < self.total_nodes - 1 else None
        return items[start_idx:end_idx]
```

## Job Configuration

IMPLEMENT:
1. SLURM Scripts:
   - Resource requests
   - Environment setup
   - Error handling
   - Output management

2. Node Setup:
   - Environment variables
   - Path configuration
   - Library loading
   - Resource limits

## Resource Management

OPTIMIZE:
1. CPU Usage:
   - Thread allocation
   - Process distribution
   - Core assignment
   - Load balancing

2. Memory Usage:
   - Per-node limits
   - Shared resources
   - Cleanup procedures
   - Swap management

## Error Handling

IMPLEMENT:
1. Node Failures:
   - Work reassignment
   - State recovery
   - Progress tracking
   - Resource cleanup

2. Job Failures:
   - Error reporting
   - Restart procedures
   - Resource release
   - State consistency

## Environment Variables

USE:
- SLURM_NODEID: Current node identifier
- SLURM_NNODES: Total number of nodes
- SLURM_TASKS_PER_NODE: Tasks per node
- SLURM_SUBMIT_DIR: Job submission directory
- SLURM_JOB_ID: Current job identifier

## Performance Monitoring

TRACK:
1. Resource Usage:
   - CPU utilization
   - Memory consumption
   - Network traffic
   - I/O operations

2. Job Metrics:
   - Processing speed
   - Resource efficiency
   - Error rates
   - Completion times