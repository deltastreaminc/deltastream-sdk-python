"""
Store resource manager for DeltaStream SDK.
"""

from typing import Optional, List, Dict, Any
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
        store_type = create_params.store_type.upper()

        # Build CREATE STORE statement
        sql = f"CREATE STORE {name} TYPE {store_type}"

        if create_params.comment:
            sql += f" COMMENT {self._escape_string(create_params.comment)}"

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

    # Store-specific operations
    async def create_kafka_store(
        self,
        name: str,
        bootstrap_servers: str,
        auth_type: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        schema_registry_url: Optional[str] = None,
        **kwargs,
    ) -> Store:
        """Create a Kafka data store."""
        params = StoreCreateParams(
            name=name,
            store_type="KAFKA",
            bootstrap_servers=bootstrap_servers,
            auth_type=auth_type,
            username=username,
            password=password,
            schema_registry_url=schema_registry_url,
            **kwargs,
        )
        return await self.create(params=params)

    async def create_kinesis_store(
        self,
        name: str,
        region: str,
        access_key_id: Optional[str] = None,
        secret_access_key: Optional[str] = None,
        **kwargs,
    ) -> Store:
        """Create a Kinesis data store."""
        params = StoreCreateParams(
            name=name,
            store_type="KINESIS",
            region=region,
            access_key_id=access_key_id,
            secret_access_key=secret_access_key,
            **kwargs,
        )
        return await self.create(params=params)

    async def create_s3_store(
        self,
        name: str,
        region: str,
        access_key_id: Optional[str] = None,
        secret_access_key: Optional[str] = None,
        **kwargs,
    ) -> Store:
        """Create an S3 data store."""
        params = StoreCreateParams(
            name=name,
            store_type="S3",
            region=region,
            access_key_id=access_key_id,
            secret_access_key=secret_access_key,
            **kwargs,
        )
        return await self.create(params=params)

    async def test_connection(self, name: str) -> Dict[str, Any]:
        """Test the connection to a data store."""
        escaped_name = self._escape_identifier(name)
        sql = f"TEST STORE {escaped_name}"
        results = await self._query_sql(sql)
        return results[0] if results else {"status": "unknown"}

    async def get_topics(self, name: str) -> List[str]:
        """Get list of topics/streams available in the store."""
        escaped_name = self._escape_identifier(name)
        sql = f"LIST TOPICS FROM STORE {escaped_name}"
        results = await self._query_sql(sql)
        return [result.get("topic_name", result.get("name", "")) for result in results]
