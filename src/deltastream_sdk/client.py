"""
Main DeltaStream SDK client.
"""

from typing import Optional, Dict, Any, Callable, Awaitable, List
import os
from deltastream.api.conn import APIConnection
from .resources import (
    StreamManager,
    StoreManager,
    DatabaseManager,
    SchemaManager,
    ComputePoolManager,
    ChangelogManager,
    EntityManager,
    FunctionManager,
    FunctionSourceManager,
    DescriptorSourceManager,
    SchemaRegistryManager,
)
from .exceptions import DeltaStreamSDKError


class DeltaStreamClient:
    """
    Main client for interacting with DeltaStream resources.

    Provides a unified interface similar to Databricks SDK for managing
    DeltaStream streams, stores, databases, and other resources.

    Example:
        # Using an existing APIConnection
        connection = APIConnection(...)
        client = DeltaStreamClient(connection=connection)

        # Using DSN
        client = DeltaStreamClient(dsn="deltastream://user:pass@host:port/")

        # Using configuration parameters
        client = DeltaStreamClient(
            server_url="https://api.deltastream.io/v2",
            token_provider=my_token_provider,
            organization_id="my_org"
        )

        # List all streams
        streams = await client.streams.list()

        # Create a new stream
        stream = await client.streams.create_with_schema(
            name="my_stream",
            columns=[
                {"name": "id", "type": "INTEGER"},
                {"name": "message", "type": "VARCHAR"}
            ],
            store="my_kafka_store",
            topic="my_topic",
            value_format="JSON"
        )
    """

    def __init__(
        self,
        connection: Optional[APIConnection] = None,
        dsn: Optional[str] = None,
        server_url: Optional[str] = None,
        token_provider: Optional[Callable[[], Awaitable[str]]] = None,
        session_id: Optional[str] = None,
        timezone: str = "UTC",
        organization_id: Optional[str] = None,
        role_name: Optional[str] = None,
        database_name: Optional[str] = None,
        schema_name: Optional[str] = None,
        store_name: Optional[str] = None,
        compute_pool_name: Optional[str] = None,
    ):
        """
        Initialize DeltaStream SDK client.

        Args:
            connection: Existing APIConnection instance to use
            dsn: DSN string for connection (alternative to other parameters)
            server_url: DeltaStream server URL (e.g., "https://host:port")
            token_provider: Async function that returns authentication token
            session_id: Session ID for the connection
            timezone: Timezone for the connection (default: "UTC")
            organization_id: Organization ID
            role_name: Role name
            database_name: Default database name
            schema_name: Default schema name
            store_name: Default store name
            compute_pool_name: Default compute pool name
        """
        if connection is not None:
            # Use provided connection
            self._connection = connection
        elif dsn is not None:
            # Create connection from DSN
            self._connection = APIConnection.from_dsn(dsn, token_provider)
        else:
            # Create connection from individual parameters
            # Use default server URL if none provided
            if server_url is None:
                server_url = "https://api.deltastream.io/v2"

            if token_provider is None:
                raise ValueError(
                    "Must provide either connection, dsn, or server_url with token_provider"
                )

            self._connection = APIConnection(
                server_url=server_url,
                token_provider=token_provider,
                session_id=session_id,
                timezone=timezone,
                organization_id=organization_id,
                role_name=role_name,
                database_name=database_name,
                schema_name=schema_name,
                store_name=store_name,
                compute_pool_name=compute_pool_name,
            )

        # Track current database in memory
        self._current_database: Optional[str] = database_name
        # Track current store in memory
        self._current_store: Optional[str] = store_name
        # Track current schema in memory
        self._current_schema: Optional[str] = schema_name

        # Initialize resource managers
        self._streams = StreamManager(self._connection)
        self._stores = StoreManager(self._connection)
        self._databases = DatabaseManager(self._connection)
        self._schemas = SchemaManager(self._connection)
        self._compute_pools = ComputePoolManager(self._connection)
        self._changelogs = ChangelogManager(self._connection)
        self._entities = EntityManager(self._connection)
        self._functions = FunctionManager(self._connection)
        self._function_sources = FunctionSourceManager(self._connection)
        self._descriptor_sources = DescriptorSourceManager(self._connection)
        self._schema_registries = SchemaRegistryManager(self._connection)

    @property
    def connection(self) -> APIConnection:
        """Access to the underlying APIConnection."""
        return self._connection

    @property
    def streams(self) -> StreamManager:
        """Access to stream resources."""
        return self._streams

    @property
    def stores(self) -> StoreManager:
        """Access to data store resources."""
        return self._stores

    @property
    def databases(self) -> DatabaseManager:
        """Access to database resources."""
        return self._databases

    @property
    def schemas(self) -> SchemaManager:
        """Access to schema resources."""
        return self._schemas

    @property
    def compute_pools(self) -> ComputePoolManager:
        """Access to compute pool resources."""
        return self._compute_pools

    @property
    def changelogs(self) -> ChangelogManager:
        """Access to changelog resources."""
        return self._changelogs

    @property
    def entities(self) -> EntityManager:
        """Access to entity resources."""
        return self._entities

    @property
    def functions(self) -> FunctionManager:
        """Access to function resources."""
        return self._functions

    @property
    def function_sources(self) -> FunctionSourceManager:
        """Access to function source resources."""
        return self._function_sources

    @property
    def descriptor_sources(self) -> DescriptorSourceManager:
        """Access to descriptor source resources."""
        return self._descriptor_sources

    @property
    def schema_registries(self) -> SchemaRegistryManager:
        """Access to schema registry resources."""
        return self._schema_registries

    # Connection management
    async def test_connection(self) -> bool:
        """Test the connection to DeltaStream."""
        try:
            # Use the connection's version call as a lightweight health check.
            await self._connection.version()
            return True
        except Exception as e:
            print(f"❌ Connection failed: {e}")
            return False

    # Convenience methods for common operations
    def _ensure_semicolon(self, sql: str) -> str:
        """Ensure SQL statement ends with a semicolon."""
        sql = sql.strip()
        if not sql.endswith(";"):
            sql += ";"
        return sql

    async def execute_sql(self, sql: str) -> None:
        """Execute a SQL statement."""
        try:
            sql = self._ensure_semicolon(sql)
            await self._connection.exec(sql)
        except Exception as e:
            raise DeltaStreamSDKError(f"Failed to execute SQL: {e}") from e

    async def query_sql(self, sql: str) -> List[Dict[str, Any]]:
        """Execute a SQL query and return results."""
        try:
            sql = self._ensure_semicolon(sql)
            rows = await self._connection.query(sql)
            results = []

            # Get column names
            columns = [col.name for col in rows.columns()]

            # Convert rows to dictionaries
            async for row in rows:
                if row:
                    result_dict = dict(zip(columns, row))
                    results.append(result_dict)

            return results
        except Exception as e:
            raise DeltaStreamSDKError(f"Failed to query SQL: {e}") from e

    async def use_database(self, database: str) -> None:
        """Switch to a different database."""
        # Use DATABASE keyword for DeltaStream syntax
        escaped_db = (
            database
            if database.startswith('"') and database.endswith('"')
            else f'"{database}"'
        )
        sql = f"USE DATABASE {escaped_db}"
        await self.execute_sql(sql)

        # Update the current database in memory
        self._current_database = database

    async def use_store(self, store: str) -> None:
        """Switch to a different store."""
        # Use STORE keyword for DeltaStream syntax
        escaped_store = (
            store if store.startswith('"') and store.endswith('"') else f'"{store}"'
        )
        sql = f"USE STORE {escaped_store}"
        await self.execute_sql(sql)

        # Update the current store in memory
        self._current_store = store

    async def use_schema(self, schema: str) -> None:
        """Switch to a different schema."""
        # Use SCHEMA keyword for DeltaStream syntax
        escaped_schema = (
            schema if schema.startswith('"') and schema.endswith('"') else f'"{schema}"'
        )
        sql = f"USE SCHEMA {escaped_schema}"
        await self.execute_sql(sql)

        # Update the current schema in memory
        self._current_schema = schema

    async def get_current_database(self) -> str:
        """Get the current database."""
        # If we have a cached current database, return it
        if self._current_database:
            return self._current_database

        # Otherwise, query LIST DATABASES to find the default database
        try:
            databases = await self.databases.list()
            for db in databases:
                # Look for the default database
                if db.is_default:
                    self._current_database = db.name
                    return db.name

            # If no default found, return the first database if any exist
            if databases:
                self._current_database = databases[0].name
                return databases[0].name
        except Exception:
            # If list databases fails, return empty string
            pass

        return ""

    async def get_current_store(self) -> str:
        """Get the current store."""
        # If we have a cached current store, return it
        if self._current_store:
            return self._current_store

        # Otherwise, query LIST STORES to find the default store
        try:
            stores = await self.stores.list()
            for store in stores:
                # Look for the default store
                if store.is_default:
                    self._current_store = store.name
                    return store.name

            # If no default found, return the first store if any exist
            if stores:
                self._current_store = stores[0].name
                return stores[0].name
        except Exception:
            # If list stores fails, return empty string
            pass

        return ""

    async def get_current_schema(self) -> str:
        """Get the current schema."""
        # If we have a cached current schema, return it
        if self._current_schema:
            return self._current_schema

        # Otherwise, query LIST SCHEMAS to find the default schema
        try:
            schemas = await self.schemas.list()
            for schema in schemas:
                # Look for the default schema
                if schema.is_default:
                    self._current_schema = schema.name
                    return schema.name

            # If no default found, return the first schema if any exist
            if schemas:
                self._current_schema = schemas[0].name
                return schemas[0].name
        except Exception:
            # If list schemas fails, return empty string
            pass

        return ""

    # Context manager support
    async def __aenter__(self):
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        pass

    # Alternative constructor methods
    @classmethod
    def from_config(cls, config: Dict[str, Any]) -> "DeltaStreamClient":
        """Create client from configuration dictionary."""
        # Extract valid constructor parameters
        valid_params = {
            "connection",
            "dsn",
            "server_url",
            "token_provider",
            "session_id",
            "timezone",
            "organization_id",
            "role_name",
            "database_name",
            "schema_name",
            "store_name",
            "compute_pool_name",
        }
        filtered_config = {k: v for k, v in config.items() if k in valid_params}
        return cls(**filtered_config)

    @classmethod
    def from_environment(cls) -> "DeltaStreamClient":
        """Create client from environment variables."""
        dsn = os.getenv("DELTASTREAM_DSN")
        if dsn:
            return cls(dsn=dsn)

        server_url = os.getenv("DELTASTREAM_SERVER_URL")
        token = os.getenv("DELTASTREAM_TOKEN")

        if server_url and token:

            async def token_provider():
                return token

            config = {
                "server_url": server_url,
                "token_provider": token_provider,
                "session_id": os.getenv("DELTASTREAM_SESSION_ID"),
                "timezone": os.getenv("DELTASTREAM_TIMEZONE", "UTC"),
                "organization_id": os.getenv("DELTASTREAM_ORGANIZATION_ID"),
                "role_name": os.getenv("DELTASTREAM_ROLE_NAME"),
                "database_name": os.getenv("DELTASTREAM_DATABASE_NAME"),
                "schema_name": os.getenv("DELTASTREAM_SCHEMA_NAME"),
                "store_name": os.getenv("DELTASTREAM_STORE_NAME"),
                "compute_pool_name": os.getenv("DELTASTREAM_COMPUTE_POOL_NAME"),
            }

            # Remove None values
            filtered_config = {k: v for k, v in config.items() if v is not None}

            return cls(**filtered_config)  # type: ignore[arg-type]

        raise ValueError(
            "Environment variables not configured. "
            "Set either DELTASTREAM_DSN or DELTASTREAM_SERVER_URL and DELTASTREAM_TOKEN"
        )
