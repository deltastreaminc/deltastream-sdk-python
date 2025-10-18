# DeltaStream Python SDK - AI Agent Guide

## Project Context

A modern, async-first Python SDK for [DeltaStream](https://deltastream.io), built on the DeltaStream Connector Python library. It provides an ergonomic Python API to manage a DeltaStream environment (control and data plane) with full CRUD lifecycle for all DeltaStream resources.

**Key Characteristics:**
- Async/await native API (Python 3.11+)
- Type-safe with comprehensive type hints
- Built on `deltastream-connector >= 0.3`
- Follows patterns similar to Databricks SDK for familiarity

## Repository Structure

```
.
├── src/
│   └── deltastream_sdk/          # Main package (note: deltastream_sdk not deltastream/sdk/)
│       ├── __init__.py            # Package exports: DeltaStreamClient, exceptions, models, resources
│       ├── client.py              # Main DeltaStreamClient class
│       ├── exceptions.py          # Custom exceptions (DeltaStreamSDKError, ResourceNotFound, etc.)
│       ├── models/                # Data models (Pydantic-based)
│       └── resources/             # Resource managers (CRUD operations)
├── tests/
│   └── sdk/                       # SDK tests
│       ├── conftest.py            # Test fixtures and configuration
├── examples/                      # Usage examples
├── docs/                          # Documentation and RFCs
└── pyproject.toml                 # Project configuration and dependencies
```

## Code Style and Structure

### General Principles
- Write concise, technical Python or SQL code with accurate examples
- Use functional and declarative programming patterns; avoid classes where possible (except for resource managers and models)
- Prefer iteration and modularization over code duplication
- Use descriptive variable names with auxiliary verbs (e.g., `is_connected`, `has_error`, `fetch_results`)
- Follow PEP 8 naming conventions: `snake_case` for functions/variables, `PascalCase` for classes

### Async Patterns
- All I/O operations are async; use `async def` and `await`
- Follow established async patterns from existing code
- Example: `async def create(...) -> ModelType:`

### Type Hints
- Use comprehensive type hints throughout
- Models are Pydantic-based for validation and serialization
- Import from `typing` as needed: `Optional`, `List`, `Dict`, `Any`, `Union`

### Error Handling
- Use custom exceptions from `exceptions.py`:
  - `DeltaStreamSDKError` (base exception)
  - `ResourceNotFound`
  - `ResourceAlreadyExists`
  - `InvalidConfiguration`
  - `ConnectionError`
- Always provide meaningful error messages

## Development Environment

### Package Manager
- **Tool**: `uv` (fast Python package manager)
- **Lockfile**: `uv.lock` (committed to repository)
- **Config**: `pyproject.toml`
- **Virtual Environment**: `./.venv` (auto-created by uv)

### Python Version
- **Required**: Python 3.11+
- Specified in `pyproject.toml`: `requires-python = ">=3.11"`

### Dependencies
- **Production**: `deltastream-connector >= 0.3`
- **Dev**: pytest, pytest-cov, pytest-asyncio, mypy, ruff, python-dotenv, tox, flake8
- **Optional**: jupyter (for notebook examples)

## Common Commands

### Installation & Setup
```bash
uv sync                  # Install all dependencies (production + dev)
uv sync --all-groups     # Install all dependency groups including optional
uv add <package>         # Add a new dependency
```

### Testing
```bash
uv run pytest                        # Run all tests
uv run pytest <path_to_test>         # Run specific test
uv run pytest -m "not integration"   # Skip integration tests
uv run pytest --cov                  # Run with coverage report
```

### Code Quality
```bash
uv run ruff check              # Lint code
uv run ruff check --fix        # Lint and auto-fix issues
uv run ruff format             # Format code
uv run ruff format --check     # Check formatting without changes
uv run mypy                    # Type checking
```

### Building
```bash
uv build                       # Build distribution packages
```

### Makefile Targets
```bash
make help          # Show available targets
make install       # Install all dependencies
make lint          # Run linting
make format        # Format code
make check-format  # Check formatting
make mypy          # Type checking
make test          # Run all tests
make unit-tests    # Run unit tests only
make build         # Build package
make ci            # Run all CI checks (lint, format, mypy, unit-tests, build)
make clean         # Clean build artifacts
make jupyter       # Launch Jupyter Lab
```

## Development Workflow

### Standard Development Flow
1. Make changes to relevant files in `src/deltastream_sdk/`
2. Write or update corresponding tests in `tests/sdk/`
3. Run tests: `uv run pytest` or `make test`
4. Run linting: `uv run ruff check --fix` or `make lint`
5. Run formatting: `uv run ruff format` or `make format`
6. Run type checking: `uv run mypy` or `make mypy`
7. Ensure all checks pass before committing

### Pre-commit Checklist
```bash
make ci  # Runs: lint, check-format, mypy, unit-tests, build
```

### Adding New Resources
When adding a new resource type (e.g., "widgets"):

1. **Create Model** (`src/deltastream_sdk/models/widgets.py`):
   ```python
   from pydantic import BaseModel, Field
   
   class Widget(BaseModel):
       name: str
       description: Optional[str] = None
       # ... other fields
   ```

2. **Create Resource Manager** (`src/deltastream_sdk/resources/widgets.py`):
   ```python
   from .base import ResourceManager
   from ..models.widgets import Widget
   
   class WidgetManager(ResourceManager):
       async def list(self) -> List[Widget]:
           # Implementation
       
       async def get(self, name: str) -> Widget:
           # Implementation
       
       async def create(self, name: str, **kwargs) -> Widget:
           # Implementation
   ```

3. **Register in Client** (`src/deltastream_sdk/client.py`):
   ```python
   from .resources.widgets import WidgetManager
   
   class DeltaStreamClient:
       def __init__(self, ...):
           # ...
           self.widgets = WidgetManager(self)
   ```

4. **Write Tests** (`tests/sdk/test_widgets.py`):
   ```python
   import pytest
   
   @pytest.mark.asyncio
   async def test_widget_creation(client):
       widget = await client.widgets.create(name="test_widget")
       assert widget.name == "test_widget"
   ```

5. **Update Exports** (`src/deltastream_sdk/__init__.py` if needed)

## Architecture Notes

### Client Initialization Patterns
The SDK supports three initialization patterns:

1. **Environment-based** (recommended for most use cases):
   ```python
   client = DeltaStreamClient.from_environment()
   ```

2. **Programmatic** (explicit configuration):
   ```python
   client = DeltaStreamClient(
       server_url="https://api.deltastream.io/v2",
       token_provider=token_provider,
       organization_id="my_org"
   )
   ```

3. **DSN-based** (connection string):
   ```python
   client = DeltaStreamClient(dsn="https://:token@api.deltastream.io/v2")
   ```

### Resource Manager Pattern
- Each resource type has a dedicated manager (e.g., `StreamManager`, `StoreManager`)
- Managers are instantiated by `DeltaStreamClient` and accessible as attributes (e.g., `client.streams`, `client.stores`)
- All managers extend `ResourceManager` base class
- CRUD operations follow consistent naming: `list()`, `get()`, `create()`, `update()`, `delete()`

### Context Management
The client maintains state for current database, schema, and store:
- `await client.use_database(name)`
- `await client.use_schema(name)`
- `await client.use_store(name)`
- `await client.get_current_database()`
- `await client.get_current_schema()`
- `await client.get_current_store()`

## Testing Guidelines

### Test Organization
- Use `pytest` with `pytest-asyncio` for async tests
- Mark async tests with `@pytest.mark.asyncio`
- Use fixtures from `conftest.py` for common setup
- Separate unit tests from integration tests using markers

### Test Coverage
- Aim for high coverage (current target: comprehensive)
- Run with coverage: `uv run pytest --cov`
- Coverage reports generated in `htmlcov/`

### Mocking
- Mock external API calls in unit tests
- Use integration tests for end-to-end scenarios
- Keep integration tests separate with `@pytest.mark.integration`

## Documentation

### In-Code Documentation
- Use docstrings for all public classes and methods
- Follow Google-style docstring format
- Include parameter types, return types, and examples where helpful

### External Documentation
- `README.md`: User-facing quickstart and examples
- `docs/`: Architecture docs, RFCs, tutorials
- `examples/`: Working code examples

## Release Process

1. Update version in `pyproject.toml`
2. Update `CHANGELOG.md` with new version and changes
3. Run full CI: `make ci`
4. Build package: `uv build`
5. Publish to PyPI: `uv publish` (requires credentials)

## Useful References

- **DeltaStream Docs**: https://docs.deltastream.io
- **DeltaStream Connector**: https://github.com/deltastreaminc/deltastream-connector-python
- **uv Documentation**: https://github.com/astral-sh/uv
- **Pydantic**: https://docs.pydantic.dev
