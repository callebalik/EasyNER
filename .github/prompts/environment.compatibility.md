# EasyNer Environment Compatibility Guidelines

## Environment Support
- Code must work in both interactive and SLURM batch environments on HPC
- Support multiple execution contexts:
  - Local development environment
  - HPC interactive sessions
  - SLURM batch jobs
  - Server deployment

## Environment Variables
- EASYNER_DB_PATH: Database location configuration
- EASYNER_LOG_LEVEL: Controls logging verbosity
- EASYNER_BATCH_SIZE: Configures data processing batch size
- FLASK_APP: Server application entry point
- FLASK_ENV: Application environment (development/production)
- SERVER_PORT: Web server port configuration
- PYTHONPATH: Must include project root

## HPC Integration
- Support SLURM job scheduling system
- Handle node allocation and multi-core processing
- Implement checkpointing for long-running jobs
- Support job interruption and resumption
- Monitor resource usage across nodes

## Configuration Management
- Use config.json for static configuration
- Support environment-specific settings
- Implement proper fallbacks for missing configurations
- Version control configuration templates
- Document all configuration options

## Error Recovery
- Implement graceful degradation in resource-constrained environments
- Handle filesystem permissions and quotas
- Provide cleanup procedures for interrupted jobs
- Log environment details for debugging