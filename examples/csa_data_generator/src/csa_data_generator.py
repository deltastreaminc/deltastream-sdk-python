#!/usr/bin/env python3
"""
DeltaStream SDK Example: Customer Service Analytics (CSA) Data Generator

This script generates simulated customer service analytics events including
pageviews, cart updates, and chat messages for testing stream processing
and customer behavior analysis.

This is a Python port of the Java ProactiveCSADataGen example, designed to work
with the DeltaStream Python SDK.
"""

import asyncio
import os
import sys
import time
from datetime import datetime, timezone
from random import Random
from typing import Any, Dict

from deltastream_sdk import DeltaStreamClient
from deltastream_sdk.exceptions import DeltaStreamSDKError
from dotenv import find_dotenv, load_dotenv


class CSADataGenerator:
    """Generate realistic customer service analytics events."""

    def __init__(
        self,
        client: DeltaStreamClient,
        store_name: str,
        pageviews_topic: str = "csa_pageviews",
        cart_updates_topic: str = "csa_cart_updates",
        chat_messages_topic: str = "csa_chat_messages",
    ):
        self.client = client
        self.store_name = store_name
        self.pageviews_topic = pageviews_topic
        self.cart_updates_topic = cart_updates_topic
        self.chat_messages_topic = chat_messages_topic
        self.random = Random()

        # Simulation data
        self.user_ids = [
            "user_123",
            "user_456",
            "user_223",
            "user_356",
            "user_163",
            "user_466",
            "user_823",
            "user_856",
        ]
        self.pages = ["/products/A1", "/products/B2", "/checkout", "/support"]
        self.cart_actions = ["ADD", "REMOVE"]
        self.chat_messages = [
            "is this in stock?",
            "how do I return an item?",
            "thanks for the help!",
        ]

    def get_event_timestamp(self) -> str:
        """Generate timestamp in the format: yyyy-MM-dd HH:mm:ss.SSSSSSZ"""
        now = datetime.now(timezone.utc)
        # Format with microseconds and Z suffix
        return now.strftime("%Y-%m-%d %H:%M:%S.%f") + "Z"

    async def ensure_entities(self) -> None:
        """Create entities (topics) if they don't exist."""
        print("\n=== ENTITY SETUP ===")

        entities_to_create = [
            self.pageviews_topic,
            self.cart_updates_topic,
            self.chat_messages_topic,
        ]

        for entity_name in entities_to_create:
            try:
                # Check if entity exists
                entities = await self.client.entities.list()
                entity_exists = any(e.name == entity_name for e in entities)

                if not entity_exists:
                    print(f"Creating entity '{entity_name}'...")
                    await self.client.entities.create(
                        name=entity_name,
                        store=self.store_name,
                        params={
                            "kafka.partitions": 3,
                            "kafka.replicas": 1,
                            "kafka.topic.retention.ms": "604800000",  # 7 days
                        },
                    )
                    print(f"✅ Created entity '{entity_name}'")
                else:
                    print(f"✅ Entity '{entity_name}' already exists")

            except Exception as e:
                error_msg = str(e)
                if "already exists" in error_msg.lower():
                    print(f"✅ Entity '{entity_name}' already exists")
                else:
                    print(f"⚠️  Warning creating entity '{entity_name}': {e}")

    async def send_message(self, entity_name: str, value: Dict[str, Any]) -> None:
        """Send a message to an entity."""
        await self.client.entities.insert_values(
            name=entity_name,
            values=[value],
            store=self.store_name,
        )

    async def generate_user_activity(self) -> None:
        """Generate a cycle of user activity including pageviews, cart updates, and chat messages."""
        try:
            # Select a random user
            user_id = self.user_ids[self.random.randint(0, len(self.user_ids) - 1)]
            event_time = self.get_event_timestamp()

            # Send a page view
            page_view = {
                "event_timestamp": event_time,
                "user_id": user_id,
                "page": self.pages[self.random.randint(0, len(self.pages) - 1)],
            }
            await self.send_message(self.pageviews_topic, page_view)
            print(f"Sent page_view: {page_view}")

            # Occasionally send a cart update (30% chance)
            if self.random.random() < 0.3:
                cart_update = {
                    "event_timestamp": event_time,
                    "user_id": user_id,
                    "cart_action": self.cart_actions[
                        self.random.randint(0, len(self.cart_actions) - 1)
                    ],
                    "item_id": f"SKU{self.random.randint(0, 99)}",
                }
                await self.send_message(self.cart_updates_topic, cart_update)
                print(f"Sent cart_update: {cart_update}")

            # Occasionally send a chat message (15% chance)
            if self.random.random() < 0.15:
                chat_message = {
                    "event_timestamp": event_time,
                    "user_id": user_id,
                    "message": self.chat_messages[
                        self.random.randint(0, len(self.chat_messages) - 1)
                    ],
                }
                await self.send_message(self.chat_messages_topic, chat_message)
                print(f"Sent chat_message: {chat_message}")

            # Random sleep between events (0-1000ms like the Java version)
            await asyncio.sleep(self.random.random())

        except Exception as e:
            print(f"Error generating user activity: {e}")

    async def run(self, duration: int = 0) -> None:
        """
        Run the event generator.

        Args:
            duration: Total runtime in seconds (0 = run indefinitely)
        """
        print("🚀 Started generating customer service analytics data...")
        if duration > 0:
            print(f"   Running for {duration}s")
        else:
            print("   Running indefinitely (Ctrl+C to stop)")

        start_time = time.time()

        try:
            while True:
                current_time = time.time()

                # Check if we should stop
                if duration > 0 and (current_time - start_time) >= duration:
                    print("\n\n✅ Completed generation cycle")
                    break

                # Generate user activity
                await self.generate_user_activity()

        except KeyboardInterrupt:
            print("\n\n⚠️  Stopped by user")


async def run_example() -> None:
    """Main example runner."""
    # Load environment
    load_dotenv(find_dotenv())

    # Get configuration
    token = os.getenv("DELTASTREAM_TOKEN")
    org_id = os.getenv("DELTASTREAM_ORG_ID")
    server_url = os.getenv("DELTASTREAM_SERVER_URL")
    db_name = os.getenv("DELTASTREAM_DATABASE_NAME")
    store_name = os.getenv("STORE_NAME", "kafka_store")

    if not token or not org_id:
        print(
            "❌ Missing required environment variables: DELTASTREAM_TOKEN and DELTASTREAM_ORG_ID"
        )
        sys.exit(1)

    # Optional configuration
    duration = int(os.getenv("DURATION", "0"))

    # Create client
    async def token_provider() -> str:
        return token

    client_kwargs = {"token_provider": token_provider, "organization_id": org_id}
    if server_url:
        client_kwargs["server_url"] = server_url

    client = DeltaStreamClient(**client_kwargs)

    # Set database if specified
    if db_name:
        await client.use_database(db_name)

    try:
        # Create generator
        generator = CSADataGenerator(client, store_name)

        # Ensure entities exist
        await generator.ensure_entities()

        # Run generator
        await generator.run(duration=duration)

    except DeltaStreamSDKError as e:
        print(f"\n❌ DeltaStream SDK error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        sys.exit(1)


def main() -> None:
    """Entry point for console script."""
    asyncio.run(run_example())


if __name__ == "__main__":
    main()
