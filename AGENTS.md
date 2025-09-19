# DeltaStream Python SDK

## Project Context

A Python SDK for [DeltaStream](https://deltastream.io), built on the DeltaStream Connector Python library. It provides a Python API to manage a DeltaStream environment (control and data plane).

## Code Style and Structure

- Write concise, technical Python, SQL or Jinja code with accurate examples
- Use functional and declarative programming patterns; avoid classes where possible
- Prefer iteration and modularization over code duplication
- Use descriptive variable names with auxiliary verbs (e.g., isLoading, hasError)
- Structure repository files as follows:
.
├── src/                          # Source code root directory
│   └── deltastream/              # Main package namespace
│       └── sdk/                  # SDK implementation
│
└── tests/                        # Test suite root directory
    └── sdk/                      # SDK tests

## Build and project setup

The project is using `uv` for dependency management. You can find the lockfile in `uv.lock`.
To run tests, use `uv run pytest <path_to_test>`.
To add dependencies use `uv add <package>`.
Dependency resolution is done using `uv sync`.
Dependencies are specified in `pyproject.toml`.
Dependencies are installed in `./.venv`.
