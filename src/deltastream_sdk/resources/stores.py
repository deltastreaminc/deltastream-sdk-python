"""
Store resource manager for DeltaStream SDK.
"""

from typing import Dict, Any
from .base import BaseResourceManager
from ..models.stores import Store, StoreCreateParams, StoreUpdateParams


class StoreManager(BaseResourceManager[Store]):
    """Manager for DeltaStream data store resources."""

    def __init__(self, connection):
        super().__init__(connection, Store)

    def _get_list_sql(self, **filters) -> str:
        """Generate SQL for listing stores."""
        sql = "LIST STORES"

        # Add filters if provided
        where_clauses = []
        if filters.get("type"):
            where_clauses.append(f"type = '{filters['type']}'")

        if where_clauses:
            sql += " WHERE " + " AND ".join(where_clauses)

        return sql

    def _get_describe_sql(self, name: str) -> str:
        """Generate SQL for describing a specific store."""
        escaped_name = self._escape_identifier(name)
        return f"DESCRIBE STORE {escaped_name}"

    def _get_create_sql(self, **params) -> str:
        """Generate SQL for creating a store."""
        if isinstance(params.get("params"), StoreCreateParams):
            create_params = params["params"]
        else:
            # Convert dict params to StoreCreateParams
            create_params = StoreCreateParams(**params)

        name = self._escape_identifier(create_params.name)

        # Build CREATE STORE statement
        sql = f"CREATE STORE {name}"

        # Add WITH clause for connection parameters
        with_clause = create_params.to_with_clause()
        if with_clause.parameters:
            sql += f" {with_clause.to_sql()}"

        return sql

    def _get_update_sql(self, name: str, **params) -> str:
        """Generate SQL for updating a store."""
        escaped_name = self._escape_identifier(name)

        if isinstance(params.get("params"), StoreUpdateParams):
            update_params = params["params"]
        else:
            update_params = StoreUpdateParams(**params)

        # Build UPDATE STORE statement
        sql = f"UPDATE STORE {escaped_name}"

        # Add WITH clause for updated parameters
        with_clause = update_params.to_with_clause()
        if with_clause.parameters:
            sql += f" {with_clause.to_sql()}"

        return sql

    def _get_delete_sql(self, name: str, **params) -> str:
        """Generate SQL for deleting a store."""
        escaped_name = self._escape_identifier(name)
        return f"DROP STORE {escaped_name}"

    async def _create_store_with_type(
        self, name: str, store_type: str, **kwargs: Any
    ) -> Store:
        """
        Internal helper to create a store of any type.

        Args:
            name: Name of the store
            store_type: Type of the store (KAFKA, KINESIS, S3, etc.)
            **kwargs: Additional parameters for the store

        Returns:
            Created Store object
        """
        params = StoreCreateParams(
            name=name,
            type=store_type,
            parameters=kwargs if kwargs else None,
        )
        return await self.create(params=params)

    # Store-specific operations
    async def create_kafka_store(
        self, name: str, parameters: Dict[str, Any] | None = None
    ) -> Store:
        """
        Create a Kafka data store.

        Args:
            name: Name of the store
            parameters: Dictionary of store parameters using DeltaStream parameter names.
                   Example: {"uris": "kafka:9092", "kafka.sasl.hash_function": "PLAIN",
                   "kafka.sasl.username": "user", "kafka.sasl.password": "pass",
                   "schema_registry_name": "my_registry", "tls.ca_cert_file": "@/path/to/ca.pem"}

        Returns:
            Created Store object
        """
        return await self._create_store_with_type(name, "KAFKA", **(parameters or {}))

    async def create_kinesis_store(
        self, name: str, parameters: Dict[str, Any] | None = None
    ) -> Store:
        """
        Create a Kinesis data store.

        Args:
            name: Name of the store
            parameters: Dictionary of store parameters using DeltaStream parameter names.
                   Example: {"uris": "https://kinesis.amazonaws.com",
                   "kinesis.iam_role_arn": "arn:aws:iam::123456789012:role/my-role",
                   "kinesis.access_key_id": "ACCESS_KEY",
                   "kinesis.secret_access_key": "SECRET_KEY"}

        Returns:
            Created Store object
        """
        return await self._create_store_with_type(name, "KINESIS", **(parameters or {}))

    async def create_s3_store(
        self, name: str, parameters: Dict[str, Any] | None = None
    ) -> Store:
        """
        Create an S3 data store.

        Args:
            name: Name of the store
            parameters: Dictionary of store parameters using DeltaStream parameter names.
                   Example: {"uris": "https://mybucket.s3.amazonaws.com/",
                   "aws.access_key_id": "ACCESS_KEY",
                   "aws.secret_access_key": "SECRET_KEY",
                   "aws.iam_role_arn": "arn:aws:iam::123456789012:role/MyRole",
                   "aws.iam_external_id": "external-id"}

        Returns:
            Created Store object
        """
        return await self._create_store_with_type(name, "S3", **(parameters or {}))

    async def test_connection(self, name: str) -> Dict[str, Any]:
        """Test the connection to a data store."""
        escaped_name = self._escape_identifier(name)
        sql = f"TEST STORE {escaped_name}"
        results = await self._query_sql(sql)
        return results[0] if results else {"status": "unknown"}
