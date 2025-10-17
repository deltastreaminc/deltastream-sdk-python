"""
Pytest configuration and fixtures for SDK tests.
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch, Mock
from typing import List, Dict, Any
import sys


# Mock the deltastream.api module since it's an external dependency
class MockAPIModule:
    class APIConnection:
        def __init__(self, *args, **kwargs):
            pass

        @classmethod
        def from_dsn(cls, dsn, token_provider=None):
            return cls()


# Mock the entire deltastream.api namespace
mock_api = Mock()
mock_api.conn = MockAPIModule()
sys.modules["deltastream.api"] = mock_api
sys.modules["deltastream.api.conn"] = mock_api.conn

# Import SDK components after mocking
from deltastream_sdk import DeltaStreamClient  # noqa: E402


@pytest.fixture
def mock_connection():
    """Mock APIConnection for testing."""
    mock_conn = AsyncMock()

    # Mock connection methods
    mock_conn.exec = AsyncMock()
    mock_conn.query = AsyncMock()
    mock_conn.version = AsyncMock(return_value={"major": 1, "minor": 0, "patch": 0})

    # Mock connection attributes
    mock_conn.server_url = "https://test.deltastream.io"
    mock_conn.rsctx = MagicMock()
    mock_conn.rsctx.organization_id = "test_org"
    mock_conn.rsctx.database_name = "test_db"

    return mock_conn


@pytest.fixture
def mock_query_rows():
    """Mock query result rows."""
    mock_rows = AsyncMock()

    # Mock columns method (not a coroutine)
    mock_column = MagicMock()
    mock_column.name = "name"
    mock_rows.columns = lambda: [mock_column]

    # Mock async iteration
    async def mock_iter(self):
        yield ["test_stream"]
        yield ["another_stream"]

    mock_rows.__aiter__ = mock_iter

    return mock_rows


@pytest.fixture
def client_with_mock_connection(mock_connection):
    """DeltaStreamClient with mocked connection."""
    return DeltaStreamClient(connection=mock_connection)


@pytest.fixture
def sample_stream_data():
    """Sample stream data for testing."""
    return {
        "Name": "test_stream",
        "Owner": "test_user",
        "Type": "STREAM",
        "State": "RUNNING",
        "Properties": {},
        "CreatedAt": "2024-01-01 00:00:00.000",
        "UpdatedAt": "2024-01-01 00:00:00.000",
        "Path": ["test_stream"],
    }


@pytest.fixture
def sample_store_data():
    """Sample store data for testing."""
    return {
        "Name": "test_store",
        "Type": "KAFKA",
        "State": "ready",
        "Message": "",
        "IsDefault": False,
        "Owner": "test_user",
        "CreatedAt": "2024-01-01 00:00:00.000",
        "UpdatedAt": "2024-01-01 00:00:00.000",
        "Path": ["test_store"],
    }


@pytest.fixture
def sample_database_data():
    """Sample database data for testing."""
    return {
        "Name": "test_database",
        "IsDefault": False,
        "Owner": "test_user",
        "CreatedAt": "2024-01-01 00:00:00.000",
        "Path": ["test_database"],
    }


@pytest.fixture
def sample_compute_pool_data():
    """Sample compute pool data for testing."""
    return {
        "Name": "test_pool",
        "IntendedState": "running",
        "ActualState": "running",
        "ErrorMessages": "",
        "Size": "MEDIUM",
        "Timeout": 300,
        "Owner": "test_user",
        "CreatedAt": "2024-01-01 00:00:00.000",
        "UpdatedAt": "2024-01-01 00:00:00.000",
        "Path": ["test_pool"],
    }


@pytest.fixture
def sample_entity_data():
    """Sample entity data for testing."""
    return {
        "Name": "test_entity",
        "IsLeaf": True,
    }


@pytest.fixture
def mock_describe_result():
    """Mock DESCRIBE query result."""

    def _mock_describe(resource_type: str, data: Dict[str, Any]):
        mock_rows = AsyncMock()

        # Convert data to DESCRIBE format (key-value pairs)
        describe_data = [[k, str(v)] for k, v in data.items()]

        mock_column1 = MagicMock()
        mock_column1.name = "property"
        mock_column2 = MagicMock()
        mock_column2.name = "value"

        mock_rows.columns = lambda: [mock_column1, mock_column2]

        async def mock_iter(self):
            for row in describe_data:
                yield row

        mock_rows.__aiter__ = mock_iter
        return mock_rows

    return _mock_describe


@pytest.fixture
def mock_list_result():
    """Mock LIST query result."""

    def _mock_list(items: List[str]):
        mock_rows = AsyncMock()

        mock_column = MagicMock()
        mock_column.name = "Name"
        mock_rows.columns = lambda: [mock_column]

        async def mock_iter(self):
            for item in items:
                yield [item]  # Yield as list, not dict

        mock_rows.__aiter__ = mock_iter
        return mock_rows

    return _mock_list


@pytest.fixture
def mock_token_provider():
    """Mock token provider for testing."""

    async def _token_provider():
        return "test_token_12345"

    return _token_provider


# Patch APIConnection.from_dsn for testing
@pytest.fixture
def mock_connection_from_dsn(mock_connection):
    """Mock APIConnection.from_dsn class method."""
    with patch("deltastream_sdk.client.APIConnection.from_dsn") as mock_from_dsn:
        mock_from_dsn.return_value = mock_connection
        yield mock_from_dsn


# Patch APIConnection constructor for testing
@pytest.fixture
def mock_connection_constructor(mock_connection):
    """Mock APIConnection constructor."""
    with patch("deltastream_sdk.client.APIConnection") as mock_constructor:
        mock_constructor.return_value = mock_connection
        yield mock_constructor
