# EasyNer Development Guidelines Index

You are working with the EasyNer system. This index will guide you to the appropriate specialized prompt for your task.

## Core Development Areas

### Data Processing
1. [NER Processing](ner.processing.prompt.md)
   - Entity extraction patterns
   - Memory-efficient processing
   - Batch operations
   - Validation requirements

2. [Database Operations](database.operations.prompt.md)
   - SQLite3 best practices
   - Transaction management
   - Connection pooling
   - Query optimization

### System Architecture
3. [API & Interfaces](api.interface.prompt.md)
   - REST API design
   - Internal interfaces
   - Type safety
   - Error handling

4. [Configuration](config.environment.prompt.md)
   - Environment management
   - Configuration patterns
   - Deployment profiles
   - Security guidelines

### Infrastructure
5. [SLURM & HPC](slurm.hpc.prompt.md)
   - Resource management
   - Job coordination
   - Node communication
   - Error recovery

6. [Thread Safety](thread.safety.prompt.md)
   - Concurrent processing
   - Resource protection
   - Lock strategies
   - State management

### Quality & Testing
7. [Test Driven Development](test.driven.prompt.md)
   - Test organization
   - Test categories
   - Resource testing
   - Performance validation

8. [Data Quality](data.quality.prompt.md)
   - Validation patterns
   - Quality metrics
   - Error handling
   - Documentation

### System State
9. [State Management](state.checkpointing.prompt.md)
   - Progress tracking
   - Checkpointing
   - State recovery
   - Resource states

10. [Edge Cases](edge.cases.prompt.md)
    - Error scenarios
    - Recovery strategies
    - Retry logic
    - Resource protection

### Database Management
11. [SQL Debugging](sql.debugging.prompt.md)
    - Query analysis
    - Performance profiling
    - Index optimization
    - Lock management

12. [Database Migration](database.migration.prompt.md)
    - Schema management
    - Data migration
    - Rollback strategies
    - Validation requirements

### Security & Monitoring
13. [Security Practices](security.practices.prompt.md)
    - Data protection
    - Access control
    - Resource quotas
    - Audit logging

14. [Logging & Monitoring](logging.monitoring.prompt.md)
    - Structured logging
    - Performance metrics
    - Resource tracking
    - Alert conditions

### Development Workflow
15. [Development Process](development.workflow.prompt.md)
    - Code organization
    - Documentation standards
    - Testing requirements
    - Review process

16. [Code Migration](code.migration.prompt.md)
    - Migration strategies
    - Feature toggles
    - Validation steps
    - Rollback planning

17. [Documentation Standards](documentation.standards.prompt.md)
    - Code documentation
    - API documentation
    - Examples
    - Performance notes

### Performance & Optimization
18. [Performance Optimization](performance.optimization.prompt.md)
    - Memory management
    - Query efficiency
    - Resource utilization
    - Monitoring patterns

19. [Caching Strategies](caching.strategy.prompt.md)
    - Memory caching
    - Data caching
    - Cache policies
    - Performance monitoring

20. [Scaling Patterns](scaling.patterns.prompt.md)
    - Load distribution
    - Resource management
    - Performance tracking
    - Worker coordination

### Debugging & Maintenance
21. [Bugfix Workflow](bugfix.workflow.prompt.md)
    - Debug patterns
    - Issue investigation
    - Fix implementation
    - Validation steps

22. [SQL Debugging](sql.debugging.prompt.md)
    - Query analysis
    - Performance profiling
    - Index optimization
    - Lock management

### Frontend
23. [Visualization & Analysis](visualization.analysis.prompt.md)
    - Data presentation
    - Interactive features
    - Performance patterns
    - Resource management

## Quick Reference

### Environment Variables
- EASYNER_DB_PATH: Database location
- EASYNER_LOG_LEVEL: Logging verbosity
- EASYNER_BATCH_SIZE: Processing batch size
- EASYNER_MAX_THREADS: Maximum thread count
- EASYNER_AUTH_KEY: Authentication key
- EASYNER_QUALITY_THRESHOLD: Quality threshold
- EASYNER_CHECKPOINT_DIR: Checkpoint location
- EASYNER_SQL_DEBUG: SQL debugging mode
- EASYNER_MAX_RETRIES: Maximum retry attempts
- EASYNER_RECOVERY_MODE: Recovery behavior
- FLASK_ENV: Environment name
- FLASK_APP: Application entry point
- PYTHONPATH: Module path
- EASYNER_CACHE_SIZE: Maximum cache entries
- EASYNER_CACHE_TTL: Entry time-to-live
- EASYNER_CACHE_POLICY: Eviction policy
- EASYNER_CACHE_MONITOR: Monitoring level
- EASYNER_MAX_WORKERS: Maximum worker count
- EASYNER_BATCH_SCALE: Batch size scaling factor
- EASYNER_MEMORY_LIMIT: Per-worker memory limit
- EASYNER_SCALE_MONITOR: Scaling metrics level

### Common Patterns

1. Resource Management:
   ```python
   with self.db.transaction():
       for batch in self._get_batches(batch_size):
           yield from self._process_batch(batch)
   ```

2. State Management:
   ```python
   with self.state_manager.managed_state("operation") as state:
       state.phase = "processing"
       # Process with checkpointing
       self.state_manager.checkpoint_progress("phase", progress)
   ```

3. Edge Case Handling:
   ```python
   with self.protected_execution("operation"):
       try:
           result = self._process_with_retry(data)
       except RetryableError:
           self._handle_retry()
   ```

4. SQL Debugging:
   ```python
   with self.sql_debugger.query_profiling("operation"):
       plan = self._analyze_query(query)
       if not self._is_plan_optimal(plan):
           query = self._optimize_query(query)
   ```

5. Error Handling:
   ```python
   try:
       # Operation code
   except Exception as e:
       self.logger.error(f"Operation failed: {str(e)}")
       raise
   finally:
       self._cleanup_resources()
   ```

6. Cache Management:
   ```python
   with self.cache_manager.cached_operation("operation") as cached_result:
       if cached_result:
           return cached_result

       result = self._perform_expensive_operation()
       return result
   ```

7. Entity Caching:
   ```python
   entity = self.cache_manager.get_cached_entity(
       entity_id,
       lambda id: self._load_entity_from_db(id)
   )
   ```

8. Distributed Processing:
   ```python
   with self.scaling_manager.process_distributed(items) as processor:
       for result in processor:
           yield result
   ```

9. Resource Scaling:
   ```python
   worker_count = self.scaling_manager._calculate_workers()
   batch_size = self.scaling_manager._calculate_batch_size(worker_count)

   for batch in self._create_batches(items, batch_size):
       with ThreadPoolExecutor(max_workers=worker_count) as executor:
           futures = [executor.submit(self._process_item, item) for item in batch]
           yield from [f.result() for f in futures]
   ```

### Project Structure
- /scripts: Core processing scripts
- /data: Data definitions and schemas
- /tests: Test suites and fixtures
- /docs: Documentation
- /.github/prompts: Development guidelines

### Development Flow
1. Feature Development:
   - Create feature branch
   - Implement with tests
   - Document changes
   - Submit for review

2. Bug Fixes:
   - Investigate with minimal changes
   - Create reproduction case
   - Implement focused fix
   - Validate thoroughly

3. Quality Assurance:
   - Run test suite
   - Check data quality
   - Verify performance
   - Review security

4. State Management:
   - Implement checkpointing
   - Verify recovery
   - Test edge cases
   - Document states

5. Documentation:
   - Update docstrings
   - Add examples
   - Document APIs
   - Note changes

6. Performance Optimization:
   - Implement caching
   - Monitor hit rates
   - Tune cache sizes
   - Validate benefits

7. Scaling Considerations:
   - Monitor resource usage
   - Optimize batch sizes
   - Balance worker loads
   - Track throughput