# Contributing to SaaS Data Platform

Thank you for your interest in contributing to the SaaS Data Platform! This document provides guidelines and instructions for contributing.

## Code of Conduct

This project adheres to a code of conduct that we expect all contributors to follow. Please be respectful and constructive in all interactions.

## How to Contribute

### Reporting Bugs

If you find a bug, please open an issue with the following information:
- Clear description of the bug
- Steps to reproduce
- Expected behavior
- Actual behavior
- Environment details (OS, Docker version, etc.)
- Screenshots if applicable

### Suggesting Enhancements

Enhancement suggestions are welcome! Please open an issue with:
- Clear description of the enhancement
- Use cases and benefits
- Potential implementation approach
- Any alternatives considered

### Pull Requests

1. **Fork the repository** and create your branch from `main`
2. **Make your changes** following our coding standards
3. **Test your changes** thoroughly
4. **Update documentation** if needed
5. **Submit a pull request** with a clear description

#### Pull Request Process

1. Ensure your PR description clearly describes the problem and solution
2. Reference any related issues
3. Ensure all CI checks pass
4. Request review from maintainers
5. Address review feedback promptly

## Development Setup

### Prerequisites

- Docker & Docker Compose
- Python 3.11+
- Git

### Local Development

```bash
# Clone your fork
git clone https://github.com/YOUR_USERNAME/saas-data-platform.git
cd saas-data-platform

# Copy environment template
cp .env.example .env

# Edit .env with your settings
vim .env

# Bootstrap the environment
./scripts/bootstrap.sh
```

### Running Tests

```bash
# Airflow DAG tests
pytest airflow/tests/ -v

# dbt tests
make dbt-test

# SQL linting
sqlfluff lint dbt/models --dialect bigquery
```

## Coding Standards

### Python

- Follow PEP 8 style guide
- Use type hints where appropriate
- Write docstrings for functions and classes
- Keep functions focused and small
- Use meaningful variable names

### SQL (dbt)

- Use lowercase for SQL keywords
- Use snake_case for column names
- Add comments for complex logic
- Follow the dbt style guide
- Ensure models are properly documented

### Git Commit Messages

- Use present tense ("Add feature" not "Added feature")
- Use imperative mood ("Move cursor to..." not "Moves cursor to...")
- Limit first line to 72 characters
- Reference issues and PRs where appropriate

Example:
```
Add batch processing for historical backfill

- Implement configurable batch size
- Add dry-run mode for testing
- Include progress logging

Fixes #123
```

## Project Structure

Please maintain the existing project structure:

```
airflow/dags/      # DAG definitions
dbt/models/        # dbt transformations
kafka/             # Streaming components
spark/jobs/        # Spark batch jobs
scripts/           # Utility scripts
.github/workflows/ # CI/CD pipelines
```

## Documentation

- Update README.md if adding new features
- Document new environment variables in .env.example
- Add inline comments for complex logic
- Update relevant guides in documentation files

## Testing

All contributions should include appropriate tests:

- Unit tests for new functions
- Integration tests for DAGs
- dbt tests for new models
- Manual testing instructions

## Review Process

All submissions require review before being merged:

1. Automated CI checks must pass
2. Code review by at least one maintainer
3. Documentation review if applicable
4. Final approval from a maintainer

## Community

- Join discussions in GitHub issues
- Ask questions in discussions
- Help other contributors
- Share your use cases

## License

By contributing, you agree that your contributions will be licensed under the MIT License.

## Questions?

If you have questions about contributing, please:
- Open a GitHub issue
- Start a GitHub discussion
- Contact the maintainers

Thank you for contributing to SaaS Data Platform!
