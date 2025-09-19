"""
Tests for DeltaStreamClient.
"""

import pytest
import os
from unittest.mock import AsyncMock, patch, MagicMock

from deltastream_sdk import DeltaStreamClient
from deltastream_sdk.exceptions import DeltaStreamSDKError


class TestDeltaStreamClientInitialization:
    """Test DeltaStreamClient initialization methods."""

    def test_init_with_connection(self, mock_connection):
        """Test initialization with existing APIConnection."""
        client = DeltaStreamClient(connection=mock_connection)

        assert client.connection == mock_connection
        assert client.streams is not None
        assert client.stores is not None
        assert client.databases is not None
        assert client.compute_pools is not None

    def test_init_with_dsn(self, mock_connection_from_dsn):
        """Test initialization with DSN string."""
        dsn = "deltastream://user:token@host:443/?organizationID=org"

        client = DeltaStreamClient(dsn=dsn)

        mock_connection_from_dsn.assert_called_once_with(dsn, None)
        assert client.connection is not None

    def test_init_with_dsn_and_token_provider(
        self, mock_connection_from_dsn, mock_token_provider
    ):
        """Test initialization with DSN and custom token provider."""
        dsn = "deltastream://user@host:443/?organizationID=org"

        client = DeltaStreamClient(dsn=dsn, token_provider=mock_token_provider)

        mock_connection_from_dsn.assert_called_once_with(dsn, mock_token_provider)
        assert client.connection is not None

    def test_init_with_server_url_and_token_provider(
        self, mock_connection_constructor, mock_token_provider
    ):
        """Test initialization with server URL and token provider."""
        DeltaStreamClient(
            server_url="https://api.deltastream.io",
            token_provider=mock_token_provider,
            organization_id="test_org",
            database_name="test_db",
        )

        mock_connection_constructor.assert_called_once()
        call_args = mock_connection_constructor.call_args

        assert call_args[1]["server_url"] == "https://api.deltastream.io"
        assert call_args[1]["token_provider"] == mock_token_provider
        assert call_args[1]["organization_id"] == "test_org"
        assert call_args[1]["database_name"] == "test_db"

    def test_init_missing_required_params(self):
        """Test initialization fails with missing required parameters."""
        with pytest.raises(ValueError, match="Must provide either"):
            DeltaStreamClient()

        with pytest.raises(ValueError, match="Must provide either"):
            DeltaStreamClient(
                server_url="https://api.deltastream.io"
            )  # Missing token_provider

    def test_from_config(self, mock_connection_constructor, mock_token_provider):
        """Test initialization from configuration dictionary."""
        config = {
            "server_url": "https://api.deltastream.io",
            "token_provider": mock_token_provider,
            "organization_id": "test_org",
        }

        client = DeltaStreamClient.from_config(config)

        mock_connection_constructor.assert_called_once()
        assert client.connection is not None

    @patch.dict(os.environ, {"DELTASTREAM_DSN": "deltastream://user:token@host:443/"})
    def test_from_environment_with_dsn(self, mock_connection_from_dsn):
        """Test initialization from environment variables with DSN."""
        client = DeltaStreamClient.from_environment()

        mock_connection_from_dsn.assert_called_once_with(
            "deltastream://user:token@host:443/", None
        )
        assert client.connection is not None

    @patch.dict(
        os.environ,
        {
            "DELTASTREAM_SERVER_URL": "https://api.deltastream.io",
            "DELTASTREAM_TOKEN": "test_token",
            "DELTASTREAM_ORGANIZATION_ID": "test_org",
        },
    )
    def test_from_environment_with_components(self, mock_connection_constructor):
        """Test initialization from environment variables with components."""
        DeltaStreamClient.from_environment()

        mock_connection_constructor.assert_called_once()
        call_args = mock_connection_constructor.call_args

        assert call_args[1]["server_url"] == "https://api.deltastream.io"
        assert call_args[1]["organization_id"] == "test_org"

    def test_from_environment_missing_config(self):
        """Test initialization from environment fails with missing config."""
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(
                ValueError, match="Environment variables not configured"
            ):
                DeltaStreamClient.from_environment()


class TestDeltaStreamClientOperations:
    """Test DeltaStreamClient operations."""

    @pytest.mark.asyncio
    async def test_test_connection_success(self, client_with_mock_connection):
        """Test successful connection test."""
        result = await client_with_mock_connection.test_connection()

        assert result is True
        client_with_mock_connection.connection.version.assert_called_once()

    @pytest.mark.asyncio
    async def test_test_connection_failure(self, client_with_mock_connection):
        """Test failed connection test."""
        client_with_mock_connection.connection.version.side_effect = Exception(
            "Connection failed"
        )

        result = await client_with_mock_connection.test_connection()

        assert result is False

    @pytest.mark.asyncio
    async def test_execute_sql(self, client_with_mock_connection):
        """Test SQL execution."""
        sql = "CREATE STREAM test_stream"

        await client_with_mock_connection.execute_sql(sql)

        client_with_mock_connection.connection.exec.assert_called_once_with(sql + ";")

    @pytest.mark.asyncio
    async def test_execute_sql_error(self, client_with_mock_connection):
        """Test SQL execution with error."""
        client_with_mock_connection.connection.exec.side_effect = Exception("SQL error")

        with pytest.raises(DeltaStreamSDKError, match="Failed to execute SQL"):
            await client_with_mock_connection.execute_sql("INVALID SQL")

    @pytest.mark.asyncio
    async def test_query_sql(self, client_with_mock_connection, mock_query_rows):
        """Test SQL query execution."""
        client_with_mock_connection.connection.query.return_value = mock_query_rows

        results = await client_with_mock_connection.query_sql("LIST STREAMS")

        assert len(results) == 2
        assert results[0]["name"] == "test_stream"
        assert results[1]["name"] == "another_stream"
        client_with_mock_connection.connection.query.assert_called_once_with(
            "LIST STREAMS;"
        )

    @pytest.mark.asyncio
    async def test_query_sql_error(self, client_with_mock_connection):
        """Test SQL query with error."""
        client_with_mock_connection.connection.query.side_effect = Exception(
            "Query error"
        )

        with pytest.raises(DeltaStreamSDKError, match="Failed to query SQL"):
            await client_with_mock_connection.query_sql("INVALID QUERY")

    @pytest.mark.asyncio
    async def test_use_database(self, client_with_mock_connection):
        """Test database switching."""
        await client_with_mock_connection.use_database("test_db")

        expected_sql = 'USE DATABASE "test_db";'
        client_with_mock_connection.connection.exec.assert_called_once_with(
            expected_sql
        )

        # Check that the current database is updated in memory
        assert client_with_mock_connection._current_database == "test_db"

    @pytest.mark.asyncio
    async def test_get_current_database(self, client_with_mock_connection):
        """Test getting current database."""
        # Mock query result for LIST DATABASES
        mock_rows = AsyncMock()
        mock_column1 = MagicMock()
        mock_column1.name = "Name"
        mock_column2 = MagicMock()
        mock_column2.name = "Is Default"
        mock_column3 = MagicMock()
        mock_column3.name = "Owner"
        mock_column4 = MagicMock()
        mock_column4.name = "Created At"
        mock_rows.columns = MagicMock(
            return_value=[mock_column1, mock_column2, mock_column3, mock_column4]
        )

        async def mock_iter(self):
            yield ["test_database", True, "owner", "2023-01-01"]

        mock_rows.__aiter__ = mock_iter
        client_with_mock_connection.connection.query.return_value = mock_rows

        result = await client_with_mock_connection.get_current_database()

        assert result == "test_database"
        assert client_with_mock_connection._current_database == "test_database"
        client_with_mock_connection.connection.query.assert_called_once_with(
            "LIST DATABASES;"
        )

    @pytest.mark.asyncio
    async def test_get_current_database_from_memory(self, client_with_mock_connection):
        """Test getting current database from memory when already set."""
        # Set current database in memory
        client_with_mock_connection._current_database = "cached_db"

        result = await client_with_mock_connection.get_current_database()

        assert result == "cached_db"
        # Should not call query since it's cached
        client_with_mock_connection.connection.query.assert_not_called()


class TestDeltaStreamClientContextManager:
    """Test DeltaStreamClient as async context manager."""

    @pytest.mark.asyncio
    async def test_context_manager(self, mock_connection):
        """Test using client as async context manager."""
        async with DeltaStreamClient(connection=mock_connection) as client:
            assert client.connection == mock_connection
            # Context manager doesn't call connect/disconnect on the connection
            # since the SDK doesn't manage the connection lifecycle

    @pytest.mark.asyncio
    async def test_context_manager_with_dsn(self, mock_connection_from_dsn):
        """Test context manager with DSN initialization."""
        dsn = "deltastream://user:token@host:443/"

        async with DeltaStreamClient(dsn=dsn) as client:
            assert client.connection is not None
            mock_connection_from_dsn.assert_called_once_with(dsn, None)


class TestDeltaStreamClientResourceManagers:
    """Test that resource managers are properly initialized."""

    def test_resource_managers_exist(self, client_with_mock_connection):
        """Test that all resource managers are accessible."""
        client = client_with_mock_connection

        # Test that all managers exist and have the expected connection
        assert client.streams is not None
        assert client.streams.connection == client.connection

        assert client.stores is not None
        assert client.stores.connection == client.connection

        assert client.databases is not None
        assert client.databases.connection == client.connection

        assert client.compute_pools is not None
        assert client.compute_pools.connection == client.connection

        assert client.changelogs is not None
        assert client.changelogs.connection == client.connection

        assert client.entities is not None
        assert client.entities.connection == client.connection

        assert client.functions is not None
        assert client.functions.connection == client.connection

        assert client.function_sources is not None
        assert client.function_sources.connection == client.connection

        assert client.descriptor_sources is not None
        assert client.descriptor_sources.connection == client.connection

        assert client.schema_registries is not None
        assert client.schema_registries.connection == client.connection
