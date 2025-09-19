# DeltaStream SDK Example Project

This example project demonstrates how to use the DeltaStream Python SDK to interact with DeltaStream resources, specifically focusing on listing and managing entities.

## Prerequisites

- Python 3.11 or higher
- [UV](https://docs.astral.sh/uv/) package manager (recommended) or pip
- Access to a DeltaStream instance
- Valid DeltaStream credentials

## Installation

1. **Install UV** (if not already installed):

   ```bash
   curl -LsSf https://astral.sh/uv/install.sh | sh
   ```

2. **Setup the project**:

   ```bash
   # Navigate to this example directory
   cd examples/list_entity
   
   # Install dependencies using UV (installs local SDK in editable mode)
   uv sync
   
   # Or using pip
   pip install -e .
   ```

3. **Configure environment**:

   ```bash
   # Copy the environment template
   cp env.example .env
   
   # Edit .env with your DeltaStream credentials
   # Required: DELTASTREAM_TOKEN and DELTASTREAM_ORG_ID
   vim .env  # or your preferred editor
   ```

## Running the Example

### Method 1: Using the Script Entry Point (Recommended)

```bash
# This is the cleanest way to run the example
uv run list-entity-example
```

### Method 2: Run the module directly

```bash
uv run python -m src.list_entity_example
```

## Configuration

The example requires the following environment variables:

### Required Variables

- `DELTASTREAM_TOKEN`: Your DeltaStream API token
- `DELTASTREAM_ORG_ID`: Your DeltaStream organization ID

### Optional Variables

- `DELTASTREAM_SERVER_URL`: Custom server URL (defaults to production)
- `DELTASTREAM_DATABASE_NAME`: Default database to use
- `DELTASTREAM_SCHEMA_NAME`: Default schema to use

## Example Output

When run successfully, the example will:

1. Test the connection to DeltaStream
2. List all entities in your DeltaStream environment
3. Display entity information including names and metadata
