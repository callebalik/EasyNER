# EasyNer HPC Environment Guidelines

You are implementing HPC-compatible code for the EasyNer system. Your code must work seamlessly in both interactive and SLURM batch environments.

ROLE: HPC Developer
OBJECTIVE: Create efficient, scalable code for distributed NER processing

## Environment Requirements

SUPPORT:
1. Execution Modes:
   - Interactive sessions
   - SLURM batch jobs
   - Multi-node processing
   - Parallel execution

2. Resource Management:
   - CPU allocation
   - Memory limits
   - Time constraints
   - Storage quotas

## Implementation Pattern

```python
def distributed_process(
    self,
    input_data: Iterator[Any],
    nodes: int = 8,
    tasks_per_node: int = 4
) -> None:
    """Process data across multiple nodes.

    Args:
        input_data: Data iterator
        nodes: Number of nodes to use
        tasks_per_node: Tasks per node

    Environment Variables:
        SLURM_ARRAY_TASK_ID: Current task ID
        SLURM_ARRAY_TASK_COUNT: Total tasks
    """
    task_id = os.getenv('SLURM_ARRAY_TASK_ID', '0')
    total_tasks = os.getenv('SLURM_ARRAY_TASK_COUNT', '1')

    self.logger.info(f"Starting task {task_id}/{total_tasks}")

    try:
        # Configure process for HPC
        self._configure_hpc_environment()

        # Process assigned partition
        with self._get_task_checkpoint(task_id) as checkpoint:
            for batch in self._get_task_batches(input_data, task_id, total_tasks):
                self._process_batch(batch)
                checkpoint.update(batch.last_id)

    except Exception as e:
        self.logger.error(f"Task {task_id} failed: {str(e)}")
        raise
    finally:
        self._cleanup_hpc_resources()
```

## Checkpointing Requirements

IMPLEMENT:
1. Progress Tracking:
   - Save state regularly
   - Track completed items
   - Store batch boundaries
   - Enable task resumption

2. Resource Cleanup:
   - Release GPU memory
   - Close file handles
   - Clear shared memory
   - Remove temporary files

## Performance Optimization

CONSIDER:
1. Data Distribution:
   - Minimize node communication
   - Balance load effectively
   - Handle data locality
   - Optimize I/O patterns

2. Resource Usage:
   - Monitor memory per node
   - Track CPU utilization
   - Manage disk I/O
   - Control network usage

## Error Handling

HANDLE:
1. Node Failures:
   - Detect disconnections
   - Save progress state
   - Enable task migration
   - Implement fallbacks

2. Resource Exhaustion:
   - Monitor usage limits
   - Implement backoff
   - Provide cleanup
   - Log resource stats

## Environment Variables

USE:
- EASYNER_BATCH_SIZE: Processing batch size
- EASYNER_DB_PATH: Database location
- EASYNER_LOG_LEVEL: Logging verbosity
- SLURM_* variables for job control