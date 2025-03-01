# EasyNer Configuration & Environment Guidelines

You are implementing configuration and environment management for the EasyNer system. Your code must handle settings across multiple environments and deployment scenarios.

ROLE: Configuration Manager
OBJECTIVE: Create robust configuration management for distributed NER processing

## Configuration Requirements

IMPLEMENT:
1. Environment Settings:
   - Database connections
   - Processing parameters
   - Resource limits
   - Logging levels

2. Deployment Profiles:
   - Development setup
   - Testing configuration
   - Production settings
   - HPC parameters

## Implementation Pattern

```python
from typing import Dict, Any
from pathlib import Path
import json
import os

class ConfigManager:
    """Manage configuration across environments."""

    def __init__(self, base_path: Path):
        self.base_path = base_path
        self.env = os.getenv('FLASK_ENV', 'development')
        self._load_config()

    def _load_config(self) -> None:
        """Load configuration with environment overrides."""
        try:
            # Load base configuration
            with open(self.base_path / 'config.json') as f:
                self.config = json.load(f)

            # Load environment overrides
            env_config = self.base_path / f'config.{self.env}.json'
            if env_config.exists():
                with open(env_config) as f:
                    self.config.update(json.load(f))

            # Apply environment variable overrides
            self._apply_env_overrides()

        except Exception as e:
            raise ConfigurationError(f"Failed to load config: {str(e)}")

    def get_db_config(self) -> Dict[str, Any]:
        """Get database configuration for current environment."""
        return {
            'path': os.getenv('EASYNER_DB_PATH', self.config['database']['path']),
            'batch_size': int(os.getenv('EASYNER_BATCH_SIZE',
                                      self.config['database']['batch_size'])),
            'pool_size': self.config['database'].get('pool_size', 5),
            'timeout': self.config['database'].get('timeout', 30)
        }
```

## Configuration Schema

DEFINE:
1. Database Settings:
   ```json
   {
     "database": {
       "path": "/path/to/db",
       "batch_size": 1000,
       "pool_size": 5,
       "timeout": 30
     }
   }
   ```

2. Processing Settings:
   ```json
   {
     "processing": {
       "chunk_size": 5000,
       "max_workers": 8,
       "memory_limit": "8G",
       "timeout": 3600
     }
   }
   ```

## Environment Variables

IMPLEMENT:
1. Core Variables:
   - FLASK_ENV: Environment name
   - EASYNER_DB_PATH: Database location
   - EASYNER_LOG_LEVEL: Logging verbosity
   - EASYNER_BATCH_SIZE: Processing batch size

2. HPC Variables:
   - SLURM_NODEID: Node identifier
   - SLURM_NNODES: Total nodes
   - SLURM_TASKS_PER_NODE: Tasks per node
   - PYTHONPATH: Module path

## Validation Requirements

VERIFY:
1. Configuration:
   - Schema compliance
   - Value ranges
   - Path existence
   - Permission checks

2. Environment:
   - Required variables
   - Value formats
   - Resource access
   - Dependencies

## Security Guidelines

IMPLEMENT:
1. Sensitive Data:
   - Credential handling
   - Path sanitization
   - Permission validation
   - Access logging

2. Environment Protection:
   - Variable isolation
   - Path restrictions
   - Resource limits
   - Access controls