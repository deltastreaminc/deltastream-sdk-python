#!/usr/bin/env python3
"""
DeltaStream SDK Example: Insert into Entity

This script inserts JSON values into an entity using a target store.
"""

import asyncio
import os
import sys

from deltastream_sdk import DeltaStreamClient
from deltastream_sdk.exceptions import DeltaStreamSDKError
from dotenv import find_dotenv, load_dotenv


def get_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


async def find_store(client: DeltaStreamClient, store_name: str):
    """Find and validate the configured store."""
    print("=== STORE DISCOVERY ===")
    stores = await client.stores.list()
    print("Available stores:")
    for store in stores:
        print(f"  - {store.name}")

    # Find the configured store by exact name match
    target_store = None
    for store in stores:
        if store.name == store_name:
            target_store = store
            break

    if not target_store:
        print(f"\n❌ Configured store '{store_name}' not found.")
        print("Available stores:")
        for store in stores:
            print(f"  - {store.name}")
        print(
            "\nPlease update STORE_NAME in .env to match one of the available stores."
        )
        sys.exit(1)

    print(f"\n✅ Found configured store: {target_store.name}")
    await client.use_store(target_store.name)
    return target_store


async def ensure_entity(client: DeltaStreamClient, entity_name: str, target_store):
    """Check if entity exists and create it if needed."""
    print("\n=== ENTITY MANAGEMENT ===")

    # Check if entity exists
    try:
        entities = await client.entities.list()
        entity_exists = False

        for entity in entities:
            print(f"  - {entity.name}")
            if entity.name == entity_name:
                entity_exists = True
                print(f"✅ Found existing entity '{entity_name}': {entity}")
                break

        print(f"Entity '{entity_name}' exists: {entity_exists}")
    except Exception as e:
        print(f"Could not check entities: {e}")
        entity_exists = False

    # Create entity if it doesn't exist
    if not entity_exists:
        print(f"Creating entity '{entity_name}' in store '{target_store.name}'...")
        try:
            await client.entities.create(
                name=entity_name,
                store=target_store.name,
                parameters={
                    "kafka.partitions": 1,
                    "kafka.replicas": 1,
                    "kafka.topic.retention.ms": "604800000",  # 7 days
                    "kafka.topic.segment.ms": "86400000",  # 1 day
                },
            )
            print(
                f"Entity '{entity_name}' created successfully in store '{target_store.name}'"
            )
        except Exception as e:
            error_msg = str(e)
            if "already exists" in error_msg:
                print(
                    f"Entity '{entity_name}' already exists (topic level), continuing..."
                )
            elif "schema not found" in error_msg:
                print(
                    f"Entity '{entity_name}' created successfully but retrieval failed, continuing..."
                )
            else:
                print(f"Could not create entity using SDK: {e}")
                sys.exit(1)
    else:
        print(f"Entity '{entity_name}' already exists, continuing...")


async def insert_data(client: DeltaStreamClient, entity_name: str, store_name: str):
    """Insert sample data into the entity using IN STORE syntax."""
    print("\n=== DATA INSERTION ===")

    sample_data = [
        {"viewtime": 1753311018649, "userid": "User_3", "pageid": "Page_1"},
        {"viewtime": 1753311018650, "userid": "User_4", "pageid": "Page_2"},
    ]

    print(
        f"Attempting to insert {len(sample_data)} records into entity '{entity_name}' in store '{store_name}'..."
    )
    print(
        f'Generated SQL will be: INSERT INTO ENTITY "{entity_name}" IN STORE "{store_name}" VALUE (\'{{"viewtime": 1753311018649,"userid": "User_3","pageid": "Page_1"}}\')'
    )

    await client.entities.insert_values(
        name=entity_name, values=sample_data, store=store_name
    )
    print("Insert completed successfully.")


async def run_example() -> None:
    # Load .env from repo root if present
    load_dotenv(find_dotenv())

    token = get_env("DELTASTREAM_TOKEN")
    org_id = get_env("DELTASTREAM_ORG_ID")
    server_url = os.getenv("DELTASTREAM_SERVER_URL")

    async def token_provider() -> str:
        return token

    client_kwargs = {"token_provider": token_provider, "organization_id": org_id}
    if server_url:
        client_kwargs["server_url"] = server_url

    client = DeltaStreamClient(**client_kwargs)

    db_name = os.getenv("DELTASTREAM_DATABASE_NAME")
    if db_name:
        await client.use_database(db_name)

    entity_name = os.getenv("ENTITY_NAME")
    store_name = os.getenv("STORE_NAME")

    print(f"Starting insert example: entity='{entity_name}', store='{store_name}'")

    try:
        # Step 1: Find and validate store
        target_store = await find_store(client, store_name)

        # Step 2: Ensure entity exists
        await ensure_entity(client, entity_name, target_store)

        # Step 3: Insert data
        await insert_data(client, entity_name, store_name)

        print(f"\n✅ Successfully completed insert example for entity '{entity_name}'")

    except DeltaStreamSDKError as e:
        print(f"DeltaStream SDK error: {e}")
        sys.exit(1)


def main() -> None:
    asyncio.run(run_example())


if __name__ == "__main__":
    main()
