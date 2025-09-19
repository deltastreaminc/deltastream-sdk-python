# Contributing to DeltaStream Python SDK

Thanks for your interest in contributing to the DeltaStream Python SDK! We appreciate your help making the SDK better for everyone.

## Prerequisites

Before you begin, install:

- uv: fast Python package manager and resolver
  - Install: [https://github.com/astral-sh/uv#installation](https://github.com/astral-sh/uv#installation)
- changie: changelog management
  - Install: [https://changie.dev/guide/installation/](https://changie.dev/guide/installation/)

## Development setup

### Available Makefile targets

Run `make help` to see all available targets:

- `make install` - Install all dependencies with uv
- `make lint` - Run ruff linting checks  
- `make format` - Format code with ruff
- `make check-format` - Check if code formatting is correct
- `make mypy` - Run mypy type checking
- `make test` - Run all tests with pytest
- `make unit-tests` - Run unit tests only (exclude integration tests)
- `make build` - Build the package
- `make ci` - Run all CI checks (lint, format, mypy, unit-tests, build)
- `make clean` - Clean build artifacts
- `make jupyter` - Run Jupyter Lab

### Setup steps

1. Fork the repo

- Fork <https://github.com/deltastreaminc/deltastream-sdk-python>

1. Clone your fork

```bash
git clone https://github.com/YOUR_USERNAME/deltastream-sdk-python.git
cd deltastream-sdk-python
```

1. Add upstream remote

```bash
git remote add upstream https://github.com/deltastreaminc/deltastream-sdk-python.git
```

1. Install dependencies (dev group)

```bash
make install
```

1. Activate the virtualenv

```bash
source .venv/bin/activate
```

## Running tests

Run the full test suite (coverage is configured in pytest options):

```bash
make test
```

Run unit tests only (excluding integration tests):

```bash
make unit-tests
```

Optional: build the package locally to verify sdist/wheel:

```bash
make build
```

## Linting and type checks

```bash
make format
make lint
make mypy
```

Or run all CI checks at once (lint, format check, mypy, unit tests, and build):

```bash
make ci
```

## Try changes in another project (editable install)

Install your local SDK in editable mode into another project’s environment:

```bash
# inside your other project’s venv
uv pip install -e /absolute/path/to/your/local/deltastream-sdk-python
```

This links your SDK source. Edits in this repo become immediately available to the other project.

## Making changes

1. Create a branch

```bash
git checkout -b feature/your-feature-name
```

1. Make your changes and add tests

- Place tests under `tests/`
- Follow existing naming: `test_*.py`

1. Add a changelog entry with changie

```bash
changie new
```

You’ll be prompted for:

- Author: your GitHub username(s)
- Issue: optional GitHub issue number(s)
- Kind: Breaking Changes | Features | Fixes | Docs | Under the Hood | Dependencies | Security | Removed
- Body: concise description of the change

This creates a file under `.changes/` which must be committed.

1. Format, lint, type-check, and test

```bash
make format
make lint
make mypy
make test
```

Alternatively, run all CI checks at once:

```bash
make ci
```

1. Commit and push

```bash
git add .
git commit -m "feat: short description of your change"
git push origin feature/your-feature-name
```

1. Open a Pull Request

- From your fork to <https://github.com/deltastreaminc/deltastream-sdk-python>

## Pull request checklist

- A `.changes/` entry created by `changie new`
- Tests updated/added and passing in CI
- Code formatted (ruff), linted, and type-checked (mypy)

## Release process (maintainers)

This repository uses changie + GitHub Actions to automate releases.

1. Generate a Release PR

   - Manually trigger the workflow: `.github/workflows/changie_generate_release_pr.yml`
     - URL: <https://github.com/deltastreaminc/deltastream-sdk-python/actions/workflows/changie_generate_release_pr.yml>
   - This batches and merges unreleased changes, updates `pyproject.toml` version, and opens a PR.

1. Merge the Release PR

   - Merging updates `CHANGELOG.md`. The "Changelog release" workflow will create a GitHub Release with the same tag.
     - Workflow: `.github/workflows/changelog_release.yml`
     - Releases: <https://github.com/deltastreaminc/deltastream-sdk-python/releases>

1. Publish to PyPI

   - The "Publish Package" workflow publishes on tags `v*.*.*` using Trusted Publishing.
     - Workflow: `.github/workflows/release.yml`
     - PyPI project: <https://pypi.org/p/deltastream-sdk>
   - You can also trigger it manually (workflow_dispatch) and specify the Python version if needed.

## Security and credentials

- Never commit secrets. Use `.env` for local runs (see examples) and GitHub Actions secrets for CI if required.
- Report security issues privately to <mailto:support@deltastream.com>.

## Cleaning build artifacts

To clean build artifacts and cache files:

```bash
make clean
```

This removes `dist/`, `build/`, `*.egg-info/`, `__pycache__` directories, and `*.pyc` files.

## Code style

- Python 3.11+ only
- Prefer functional/declarative patterns and descriptive names
- Keep changes small and well-tested

Thanks again for contributing!
