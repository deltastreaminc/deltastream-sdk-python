#!/usr/bin/env python3
"""
Simple DeltaStream SDK Example: Entity Operations

This script demonstrates basic entity operations using the DeltaStream Python SDK.
It shows how to list, create, describe, and manage entities.
"""

import asyncio
import sys

from deltastream_sdk import DeltaStreamClient, DeltaStreamSDKError

from .utils.config import get_config

print("🚀 DeltaStream SDK Entity Example")
print("=" * 50)


async def run_example():
    """Run the async entity example logic."""

    config = get_config()

    async def token_provider() -> str:
        return config.auth_token

    client_kwargs = {
        "token_provider": token_provider,
        "organization_id": config.organization_id,
    }
    if config.server_url:
        client_kwargs["server_url"] = config.server_url
    if config.database_name:
        client_kwargs["database_name"] = config.database_name
    if config.schema_name:
        client_kwargs["schema_name"] = config.schema_name

    client = DeltaStreamClient(**client_kwargs)

    # Test the connection
    is_connected = await client.test_connection()
    if not is_connected:
        print("Connection test failed")
        sys.exit(1)
    print("Connection established successfully")

    try:
        print("\n─ Listing all entities:")
        print("-" * 30)
        entities = await client.entities.list()
        print(f"Found {len(entities)} entities")

        for entity in entities:
            print(entity)

    except DeltaStreamSDKError as e:
        print(f"DeltaStream SDK error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}")
        sys.exit(1)


def main():
    """Entry point for console script."""
    asyncio.run(run_example())


if __name__ == "__main__":
    main()
