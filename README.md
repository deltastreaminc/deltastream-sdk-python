<div align="center">

# DeltaStream Python SDK

<strong>Modern, async-first Python SDK for managing real-time data infrastructure with <a href="https://deltastream.io" target="_blank">DeltaStream</a>.</strong>

[![PyPI Version](https://img.shields.io/pypi/v/deltastream-sdk.svg)](https://pypi.org/project/deltastream-sdk/)
[![Python Versions](https://img.shields.io/pypi/pyversions/deltastream-sdk.svg)](https://pypi.org/project/deltastream-sdk/)
[![License](https://img.shields.io/github/license/deltastreaminc/deltastream-sdk-python.svg)](./LICENSE)
[![Build Status](https://github.com/deltastreaminc/deltastream-sdk-python/actions/workflows/python.yml/badge.svg)](https://github.com/deltastreaminc/deltastream-sdk-python/actions/workflows/python.yml)
</div>

## Overview

The DeltaStream Python SDK provides an ergonomic, type-safe, asynchronous interface to DeltaStream's control and data planes. It simplifies creation, management, and orchestration of streaming data resources (streams, stores, compute pools, schemas, functions, entities, etc.) while offering direct SQL execution for advanced workflows.

Built on top of the official [DeltaStream Connector](https://github.com/deltastreaminc/deltastream-connector-python), this SDK focuses on developer productivity, clarity, and composability.

## Features

* Async/await native API (Python 3.11+)
* Full CRUD lifecycle for all DeltaStream resources
* Multiple auth/connection strategies: DSN, environment, or programmatic
* Context switching (database, schema, store) with in-memory state tracking
* Direct SQL execution and query helpers
* Rich resource managers (Streams, Stores, Entities, Functions, Compute Pools, Schemas, Registries, Changelogs, Descriptors)
* Type hints throughout for excellent IDE support
* Extensible patterns for adding new resource types

### Supported Resource Domains

| Domain | Examples |
| ------ | -------- |
| Streams | Creation, status, start/stop, select-based materialization |
| Stores | Kafka, Kinesis, S3 and more |
| Databases & Schemas | Lifecycle + context switching |
| Entities | Creation, insertion, hierarchical listing |
| Functions & Sources | Function code + metadata management |
| Descriptor Sources | Protocol buffer / descriptor handling |
| Schema Registries | Registry management and integration |
| Compute Pools | Provisioning, scaling, lifecycle |
| Changelogs | Tracking and management |

## Installation

Install from PyPI (recommended):

```bash
pip install deltastream-sdk
```

Using `uv` (lockfile-driven Python package manager):

```bash
uv add deltastream-sdk
```

### Requirements

* Python 3.11+
* Network access to a DeltaStream deployment
* A valid API token or DSN credentials

## Quickstart

Minimal working example demonstrating environment-based configuration:

```python
import asyncio
from deltastream_sdk import DeltaStreamClient

async def main():
    client = DeltaStreamClient.from_environment()
    if await client.test_connection():
        print("✅ Connected to DeltaStream")

if __name__ == "__main__":
    asyncio.run(main())
```

### Environment Variables

```bash
# Option A (DSN-based)
export DELTASTREAM_DSN="https://:your_token@api.deltastream.io/v2"             
# OR explicit config (Option B)
export DELTASTREAM_SERVER_URL="https://api.deltastream.io/v2"
export DELTASTREAM_TOKEN="your_token_here"
export DELTASTREAM_ORGANIZATION_ID="your_org_id"
export DELTASTREAM_DATABASE_NAME="default_db"    # optional
export DELTASTREAM_SCHEMA_NAME="public"          # optional
export DELTASTREAM_STORE_NAME="default_store"    # optional
```

## Usage

Below are common usage patterns. All APIs are async.

### Connecting (3 Patterns)

```python
import os
from deltastream_sdk import DeltaStreamClient

# 1. From environment
client = DeltaStreamClient.from_environment()

# 2. Programmatic configuration
async def token_provider():
    return os.getenv("DELTASTREAM_TOKEN")

client = DeltaStreamClient(
    server_url="https://api.deltastream.io/v2",
    token_provider=token_provider,
    organization_id="my_org",
)

# 3. DSN-based
client = DeltaStreamClient(dsn="https://:your_token@api.deltastream.io/v2")
```

### Streams

```python
async def stream_example(client):
    streams = await client.streams.list()

    stream = await client.streams.create_with_schema(
        name="user_events",
        columns=[
            {"name": "user_id", "type": "INTEGER"},
            {"name": "event_type", "type": "VARCHAR"},
            {"name": "timestamp", "type": "TIMESTAMP"},
        ],
        store="kafka_store",
        topic="user-events",
    )

    analytics_stream = await client.streams.create_from_select(
        name="user_analytics",
        sql_definition="SELECT user_id, COUNT(*) AS event_count FROM user_events GROUP BY user_id",
    )

    await client.streams.start("user_events")
    status = await client.streams.get_status("user_events")
    await client.streams.stop("user_events")
```

### Stores

```python
async def store_example(client):
    # Create a Kafka store with PLAIN authentication
    kafka_store = await client.stores.create_kafka_store(
        name="my_kafka",
        parameters={
            "uris": "localhost:9092",
            "kafka.sasl.hash_function": "PLAIN",
            "kafka.sasl.username": "user",
            "kafka.sasl.password": "pass",
            "schema_registry_name": "my_schema_registry",
        },
    )

    # Create an S3 store with IAM role
    s3_store = await client.stores.create_s3_store(
        name="my_s3",
        parameters={
            "uris": "https://mybucket.s3.amazonaws.com/",
            "aws.iam_role_arn": "arn:aws:iam::123456789012:role/DeltaStreamRole",
            "aws.iam_external_id": "external-id-123",
        },
    )

    # Create a Kinesis store
    kinesis_store = await client.stores.create_kinesis_store(
        name="my_kinesis",
        parameters={
            "uris": "https://kinesis.us-east-1.amazonaws.com",
            "kinesis.access_key_id": "AKIAIOSFODNN7EXAMPLE",
            "kinesis.secret_access_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        },
    )

    # List all stores
    all_stores = await client.stores.list()
```

### Databases, Schemas & Context Switching

```python
async def context_example(client):
    await client.databases.create(name="analytics_db")
    await client.schemas.create(name="customer_data")

    await client.use_database("analytics_db")
    await client.use_schema("customer_data")
    await client.use_store("my_kafka")

    current_db = await client.get_current_database()
    current_schema = await client.get_current_schema()
    current_store = await client.get_current_store()
    print(f"Context: {current_db}.{current_schema}/{current_store}")
```

### Entities & Data Insertion

```python
async def entity_example(client):
    # Create entity with default parameters
    await client.entities.create(
        name="simple_entity",
        store="my_kafka",
    )
    
    # Create entity with custom Kafka configurations
    await client.entities.create(
        name="user_profiles",
        store="my_kafka",
        parameters={
            "kafka.partitions": 3,
            "kafka.replicas": 2,
            "kafka.topic.retention.ms": "604800000",  # 7 days
        },
    )
    
    # Create entity with compact cleanup policy
    await client.entities.create(
        name="user_compact",
        store="my_kafka",
        parameters={
            "kafka.partitions": 2,
            "kafka.replicas": 1,
            "kafka.topic.cleanup.policy": "compact",
        },
    )

    # Insert data into entity
    await client.entities.insert_values(
        name="user_profiles",
        values=[
            {"user_id": 1, "name": "Alice", "email": "alice@example.com"},
            {"user_id": 2, "name": "Bob", "email": "bob@example.com"},
        ],
        store="my_kafka",
    )

    # List entities in a store
    entities = await client.entities.list_entities(store="my_kafka")
```

### Compute Pools

```python
async def compute_pool_example(client):
    await client.compute_pools.create(name="analytics_pool", min_units=1, max_units=10)
    await client.compute_pools.start("analytics_pool")
    await client.compute_pools.stop("analytics_pool")
    pools = await client.compute_pools.list()
```

### Direct SQL

```python
async def sql_example(client):
    await client.execute_sql("CREATE TEMP VIEW users AS SELECT * FROM user_stream;")
    rows = await client.query_sql("SELECT COUNT(*) AS user_count FROM users;")
    print(rows[0]["user_count"])  # Access by column name
```

### Error Handling

```python
from deltastream_sdk.exceptions import (
    DeltaStreamSDKError,
    ResourceNotFound,
    SQLError,
    ConnectionError,
)

async def robust_example(client):
    try:
        await client.streams.get("non_existent_stream")
    except ResourceNotFound:
        print("Stream not found")
    except SQLError as e:
        print("SQL failure", e)
    except ConnectionError:
        print("Connection issue")
    except DeltaStreamSDKError as e:
        print("General SDK error", e)
```

## Development

1. Fork the repo & create a feature branch
2. Add/adjust tests for your change
3. Ensure lint & type checks pass
4. Open a PR with a clear description & rationale

See [CONTRIBUTING.md](./CONTRIBUTING.md) for more details.

## License

Licensed under the **Apache License 2.0**. See the [LICENSE](./LICENSE) file for details.
