# DeltaStream SDK Insert Entity Example

This example demonstrates how to insert JSON records into a DeltaStream entity using the Python SDK.

## Prerequisites

- Python 3.11+
- `uv` package manager
- Access to a DeltaStream instance and valid credentials

## Setup

1. **Navigate to the example directory**:

   ```bash
   cd examples/insert_entity
   ```

2. **Install dependencies**:

   ```bash
   uv sync
   ```

3. **Configure environment**:

   ```bash
   # Copy the environment template
   cp env.example .env
   
   # Edit .env with your DeltaStream credentials
   # Required: DELTASTREAM_TOKEN and DELTASTREAM_ORG_ID
   vim .env  # or your preferred editor
   ```

## Configuration

The example requires the following environment variables:

### Required Variables

- `DELTASTREAM_TOKEN`: Your DeltaStream API token
- `DELTASTREAM_ORG_ID`: Your DeltaStream organization ID

### Optional Variables

- `DELTASTREAM_SERVER_URL`: Custom server URL (defaults to production)
- `DELTASTREAM_DATABASE_NAME`: Default database to use
- `ENTITY_NAME`: Name of the entity to insert into (defaults to "test-sdk")
- `STORE_NAME`: Name of the store to use (defaults to "ChristopheKafka")

## Running the Example

```bash
uv run insert-entity-example
```

## What It Does

This example will:

1. **Discover available stores** and validate the configured store exists
2. **Check for existing entity** and create it if it doesn't exist
3. **Insert sample data** - Two JSON records with pageview data:

   ```json
   {"viewtime": 1753311018649, "userid": "User_3", "pageid": "Page_1"}
   {"viewtime": 1753311018650, "userid": "User_4", "pageid": "Page_2"}
   ```
