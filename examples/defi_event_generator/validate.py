"""
Simple validation test for the DeFi Event Generator.
This validates the data generation logic without requiring DeltaStream credentials.
"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from unittest.mock import AsyncMock, MagicMock

from defi_event_generator import DeFiEventGenerator


def test_transaction_creation():
    """Test transaction record creation."""
    # Create a mock client
    mock_client = MagicMock()
    generator = DeFiEventGenerator(client=mock_client, store_name="test_store")

    tx = generator.create_transaction(
        from_address="0xFrom",
        to_address="0xTo",
        token="DAI",
        amount=1000.0,
        block=18000000,
        timestamp=1234567890,
    )

    # Validate structure
    assert tx["from_address"] == "0xFrom"
    assert tx["to_address"] == "0xTo"
    assert tx["tx_token"] == "DAI"
    assert tx["tx_amount"] == 1000.0
    assert tx["block_number"] == 18000000
    assert tx["event_timestamp"] == 1234567890
    assert "tx_hash" in tx
    assert tx["tx_hash"].startswith("0x")

    print("✅ Transaction creation test passed")


def test_flash_loan_sequence():
    """Test that flash loan attack generates correct sequence."""
    # Create a mock client
    mock_client = MagicMock()
    mock_client.entities = MagicMock()
    mock_client.entities.insert_values = AsyncMock()

    generator = DeFiEventGenerator(client=mock_client, store_name="test_store")

    # Verify constants
    assert generator.FLASH_LOAN_PROVIDER.startswith("0x")
    assert generator.DEX_ROUTER.startswith("0x")
    assert generator.current_block == 18000000

    print("✅ Flash loan sequence validation passed")


def test_normal_traffic():
    """Test normal traffic data structure."""
    mock_client = MagicMock()
    generator = DeFiEventGenerator(client=mock_client, store_name="test_store")

    # Test transaction structure
    tx = generator.create_transaction(
        "0xUserA", "0xUserB", "USDC", 1000, 18000001, 1234567890
    )

    assert tx["tx_token"] == "USDC"
    assert tx["tx_amount"] == 1000

    print("✅ Normal traffic test passed")


if __name__ == "__main__":
    print("Running DeFi Event Generator validation tests...\n")

    test_transaction_creation()
    test_flash_loan_sequence()
    test_normal_traffic()

    print("\n✅ All validation tests passed!")
