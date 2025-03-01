# EasyNer Bugfix Workflow Guidelines

You are debugging and fixing issues in the EasyNer system. Follow these guidelines to ensure safe and effective bug resolution.

ROLE: Debug Specialist
OBJECTIVE: Resolve issues while maintaining system stability and data integrity

## Debug Approach

ANALYZE:
1. Issue Context:
   - Error messages and stack traces
   - Environment variables
   - System resource state
   - Database state

2. Minimal Reproduction:
   - Isolate affected components
   - Create minimal test case
   - Verify in clean environment
   - Document reproduction steps

## Implementation Pattern

```python
class IssueInvestigator:
    """Investigate and debug system issues."""

    def __init__(self, db_handler: EasyNerDBHandler):
        self.logger = db_handler.logger
        self.db = db_handler

    def investigate_error(self, error_context: Dict[str, Any]) -> Dict[str, Any]:
        """Investigate an error with comprehensive logging.

        Args:
            error_context: Information about the error

        Returns:
            Investigation results and recommendations
        """
        results = {}

        try:
            # Check system state
            results['system_state'] = self._check_system_state()

            # Verify database consistency
            results['db_state'] = self._verify_database_state()

            # Check resource usage
            results['resources'] = self._check_resource_usage()

            # Analyze error patterns
            results['patterns'] = self._analyze_error_patterns(error_context)

            return self._generate_recommendations(results)

        except Exception as e:
            self.logger.error(f"Investigation failed: {str(e)}")
            raise
```

## Debug Checklist

VERIFY:
1. Environment:
   - Correct Python version
   - Required dependencies
   - Environment variables
   - Resource limits

2. Data State:
   - Database consistency
   - Schema validation
   - Index integrity
   - Transaction state

3. Resource Usage:
   - Memory consumption
   - CPU utilization
   - Disk space
   - Connection pools

## Fix Implementation

IMPLEMENT:
1. Minimal Changes:
   - Focus on root cause
   - Avoid side effects
   - Maintain interfaces
   - Preserve behavior

2. Validation Steps:
   - Unit test coverage
   - Integration testing
   - Performance impact
   - Resource usage

## Rollback Plan

PREPARE:
1. State Preservation:
   - Database backups
   - Configuration snapshots
   - Process checkpoints
   - Log archives

2. Recovery Steps:
   - Revert changes
   - Restore state
   - Verify integrity
   - Document actions

## Documentation

INCLUDE:
1. Issue Details:
   - Root cause
   - Impact assessment
   - Fix description
   - Validation steps

2. Prevention:
   - Monitoring improvements
   - Validation additions
   - Process updates
   - Documentation updates