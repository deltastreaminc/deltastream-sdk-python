#!/usr/bin/env python3
"""
DeltaStream SDK Example: DeFi Event Generator

This script generates simulated DeFi events including flash loan attacks,
normal blockchain transactions, DEX price updates, and mempool data.
It mimics the behavior of the Java DeFiEventGenerator for testing and demonstration.
"""

import asyncio
import json
import os
import sys
import time
import uuid
from random import Random
from typing import Any, Dict

from deltastream_sdk import DeltaStreamClient
from deltastream_sdk.exceptions import DeltaStreamSDKError
from deltastream_sdk.models.entities import EntityCreateParams
from dotenv import find_dotenv, load_dotenv


class DeFiEventGenerator:
    """Generate realistic DeFi events for testing stream processing."""

    # DeFi Simulation Constants
    FLASH_LOAN_PROVIDER = (
        "0xA9754f1D6516a24A413148Db4534A4684344C1fE"  # Simulated Aave Pool
    )
    DEX_ROUTER = (
        "0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D"  # Simulated Uniswap V2 Router
    )

    def __init__(
        self,
        client: DeltaStreamClient,
        store_name: str,
        transactions_topic: str = "onchain_transactions",
        prices_topic: str = "dex_prices",
        mempool_topic: str = "mempool_data",
    ):
        self.client = client
        self.store_name = store_name
        self.transactions_topic = transactions_topic
        self.prices_topic = prices_topic
        self.mempool_topic = mempool_topic
        self.random = Random()
        self.current_block = 18000000

    def get_env(self, name: str) -> str:
        """Get required environment variable or raise error."""
        value = os.getenv(name)
        if not value:
            raise ValueError(f"Missing required environment variable: {name}")
        return value

    async def ensure_entities(self) -> None:
        """Create entities (topics) if they don't exist."""
        print("\n=== ENTITY SETUP ===")

        entities_to_create = [
            self.transactions_topic,
            self.prices_topic,
            self.mempool_topic,
        ]

        for entity_name in entities_to_create:
            try:
                # Check if entity exists
                entities = await self.client.entities.list()
                entity_exists = any(e.name == entity_name for e in entities)

                if not entity_exists:
                    print(f"Creating entity '{entity_name}'...")
                    create_params = EntityCreateParams(
                        name=entity_name,
                        store=self.store_name,
                        params={
                            "kafka.topic.partitions": 3,
                            "kafka.topic.replicas": 1,
                            "kafka.topic.retention.ms": "604800000",  # 7 days
                        },
                    )
                    await self.client.entities.create(params=create_params)
                    print(f"✅ Created entity '{entity_name}'")
                else:
                    print(f"✅ Entity '{entity_name}' already exists")

            except Exception as e:
                error_msg = str(e)
                if "already exists" in error_msg.lower():
                    print(f"✅ Entity '{entity_name}' already exists")
                else:
                    print(f"⚠️  Warning creating entity '{entity_name}': {e}")

    def create_transaction(
        self,
        from_address: str,
        to_address: str,
        token: str,
        amount: float,
        block: int,
        timestamp: int,
    ) -> Dict[str, Any]:
        """Create a transaction record."""
        return {
            "tx_hash": "0x" + uuid.uuid4().hex,
            "block_number": block,
            "event_timestamp": timestamp,
            "from_address": from_address,
            "to_address": to_address,
            "tx_token": token,
            "tx_amount": amount,
        }

    async def send_message(self, entity_name: str, value: Dict[str, Any]) -> None:
        """Send a message to an entity."""
        print(f"  → {entity_name}: {json.dumps(value)}")
        await self.client.entities.insert_values(
            name=entity_name,
            values=[value],
            store=self.store_name,
        )

    async def generate_flash_loan_attack(self) -> None:
        """Generate a sequence of correlated events simulating a flash loan attack."""
        try:
            attacker_address = "0x" + uuid.uuid4().hex[:40]
            event_time = int(time.time() * 1000)
            self.current_block += 1

            print(
                f"\n\n🚨 --- SIMULATING FLASH LOAN ATTACK by {attacker_address} "
                f"at block {self.current_block} --- 🚨\n"
            )

            # 1. Attacker takes a massive flash loan of 50M DAI
            loan_tx = self.create_transaction(
                self.FLASH_LOAN_PROVIDER,
                attacker_address,
                "DAI",
                50_000_000,
                self.current_block,
                event_time,
            )
            await self.send_message(self.transactions_topic, loan_tx)
            print("1. Attacker takes 50M DAI flash loan.")
            await asyncio.sleep(1)

            # 2. Attacker uses the DAI to manipulate a low-liquidity pool on a DEX
            swap_tx = self.create_transaction(
                attacker_address,
                self.DEX_ROUTER,
                "DAI",
                50_000_000,
                self.current_block,
                event_time + 100,
            )
            await self.send_message(self.transactions_topic, swap_tx)
            print("2. Attacker swaps DAI for WETH, causing price slippage.")
            await asyncio.sleep(1)

            # 3. The price of WETH/DAI on the DEX plummets due to the large trade
            price_update = {
                "event_timestamp": event_time + 200,
                "token_pair": "WETH/DAI",
                "price": 1850.75,  # Price drops from normal ~2300
            }
            await self.send_message(self.prices_topic, price_update)
            print("3. DEX price oracle for WETH/DAI reports anomalous drop.")
            await asyncio.sleep(1)

            # 4. A huge gas spike is observed in the mempool
            gas_spike = {
                "event_timestamp": event_time + 300,
                "block_number": self.current_block,
                "avg_gas_price_gwei": 250,  # Spike from normal ~30 Gwei
            }
            await self.send_message(self.mempool_topic, gas_spike)
            print("4. Mempool shows massive gas spike.")
            await asyncio.sleep(1)

            # 5. Attacker repays the flash loan in the same block
            repay_tx = self.create_transaction(
                attacker_address,
                self.FLASH_LOAN_PROVIDER,
                "DAI",
                50_000_000.1,  # Repay with small fee
                self.current_block,
                event_time + 500,
            )
            await self.send_message(self.transactions_topic, repay_tx)
            print("5. Attacker repays the 50M DAI flash loan.")

        except Exception as e:
            print(f"Error generating flash loan attack: {e}")

    async def generate_normal_traffic(self) -> None:
        """Generate normal background traffic."""
        try:
            self.current_block += 1
            event_time = int(time.time() * 1000)

            # Normal trade
            normal_tx = self.create_transaction(
                "0xUserA", "0xUserB", "USDC", 1000, self.current_block, event_time
            )
            await self.send_message(self.transactions_topic, normal_tx)

            # Normal price update
            price_update = {
                "event_timestamp": event_time,
                "token_pair": "WETH/DAI",
                "price": 2300.0 + (self.random.random() * 10 - 5),
                "join_helper": "A",
            }
            await self.send_message(self.prices_topic, price_update)

            # Normal gas price
            gas_update = {
                "event_timestamp": event_time,
                "block_number": self.current_block,
                "avg_gas_price_gwei": 30 + self.random.randint(0, 10),
            }
            await self.send_message(self.mempool_topic, gas_update)

            print(".", end="", flush=True)  # Progress indicator

        except Exception as e:
            print(f"\nError generating normal traffic: {e}")

    async def run(
        self,
        normal_interval: int = 5,
        attack_interval: int = 120,
        duration: int = 0,
    ) -> None:
        """
        Run the event generator.

        Args:
            normal_interval: Seconds between normal traffic generation (default: 5)
            attack_interval: Seconds between flash loan attacks (default: 120)
            duration: Total runtime in seconds (0 = run indefinitely)
        """
        print("🚀 Started generating real-time on-chain data...")
        print(f"   Normal traffic every {normal_interval}s")
        print(f"   Flash loan attacks every {attack_interval}s")
        if duration > 0:
            print(f"   Running for {duration}s")
        else:
            print("   Running indefinitely (Ctrl+C to stop)")

        start_time = time.time()
        last_attack_time = 0.0

        try:
            while True:
                current_time = time.time()

                # Check if we should stop
                if duration > 0 and (current_time - start_time) >= duration:
                    print("\n\n✅ Completed generation cycle")
                    break

                # Generate flash loan attack at intervals
                if (current_time - last_attack_time) >= attack_interval:
                    await self.generate_flash_loan_attack()
                    last_attack_time = current_time

                # Generate normal traffic
                await self.generate_normal_traffic()

                # Wait before next cycle
                await asyncio.sleep(normal_interval)

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
    normal_interval = int(os.getenv("NORMAL_TRAFFIC_INTERVAL", "5"))
    attack_interval = int(os.getenv("ATTACK_INTERVAL", "120"))
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
        generator = DeFiEventGenerator(client, store_name)

        # Ensure entities exist
        await generator.ensure_entities()

        # Run generator
        await generator.run(
            normal_interval=normal_interval,
            attack_interval=attack_interval,
            duration=duration,
        )

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
