# EasyNer Contribution Guidelines

## Getting Started
- Set up both local and HPC environments
- Configure environment variables according to environment.compatibility.md
- Run test suite to verify setup
- Review all specialized guideline documents

## Development Workflow
- Create feature branches from dev branch
- Use meaningful branch names: feature/, bugfix/, hotfix/
- Keep commits focused and well-documented
- Include tests for new functionality
- Update documentation as needed

## Code Standards
- Follow PEP 8 style guidelines
- Add type hints to new code
- Document public interfaces
- Keep functions focused and manageable
- Use consistent naming patterns

## Review Process
- Self-review changes before submission
- Run full test suite locally
- Verify HPC compatibility
- Check for performance implications
- Update relevant documentation

## Database Changes
- Document schema changes
- Include migration scripts
- Test with representative data volumes
- Consider performance implications
- Maintain backward compatibility

## Performance Considerations
- Profile new features
- Test with large datasets
- Monitor memory usage
- Document resource requirements
- Consider both local and HPC impacts