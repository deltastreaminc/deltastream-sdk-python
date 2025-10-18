"""
Stream resource manager for DeltaStream SDK.
"""

from typing import Optional, List, Dict, Any
from .base import BaseResourceManager
from ..models.streams import (
    Stream,
    StreamCreateParams,
)
from ..exceptions import InvalidConfiguration


class StreamManager(BaseResourceManager[Stream]):
    """Manager for DeltaStream stream resources."""

    def __init__(self, connection):
        super().__init__(connection, Stream)

    def _get_list_sql(self, **filters) -> str:
        """Generate SQL for listing streams."""
        sql = "LIST STREAMS"

        # Add filters if provided
        where_clauses = []
        if filters.get("database"):
            where_clauses.append(f"database_name = '{filters['database']}'")
        if filters.get("schema"):
            where_clauses.append(f"schema_name = '{filters['schema']}'")

        if where_clauses:
            sql += " WHERE " + " AND ".join(where_clauses)

        return sql

    def _get_describe_sql(self, name: str) -> str:
        """Generate SQL for describing a specific stream."""
        escaped_name = self._escape_identifier(name)
        return f"DESCRIBE RELATION {escaped_name}"

    def _get_create_sql(self, **params) -> str:
        """Generate SQL for creating a stream."""
        if isinstance(params.get("params"), StreamCreateParams):
            create_params = params["params"]
        else:
            # Convert dict params to StreamCreateParams
            create_params = StreamCreateParams(**params)

        name = self._escape_identifier(create_params.name)

        # Build CREATE STREAM statement
        if create_params.sql_definition:
            # CREATE STREAM AS SELECT style
            sql = f"CREATE STREAM {name}"

            sql += f" AS {create_params.sql_definition}"

            # Add WITH clause if needed
            with_clause = create_params.to_with_clause()
            if with_clause.parameters:
                sql += f" {with_clause.to_sql()}"

        elif create_params.columns:
            # CREATE STREAM with explicit schema
            sql = f"CREATE STREAM {name} ("

            # Add column definitions
            column_defs = []
            for col in create_params.columns:
                col_name = self._escape_identifier(col["name"])
                col_type = col["type"]
                column_defs.append(f"{col_name} {col_type}")

            sql += ", ".join(column_defs) + ")"

            # Add WITH clause
            with_clause = create_params.to_with_clause()
            if with_clause.parameters:
                sql += f" {with_clause.to_sql()}"
        else:
            raise InvalidConfiguration(
                "Either sql_definition or columns must be provided"
            )

        return sql

    def _get_update_sql(self, name: str, **params) -> str:
        """Generate SQL for updating a stream."""
        # DeltaStream typically doesn't support direct stream updates
        # You would need to recreate the stream or use specific update commands

        raise InvalidConfiguration("Stream updates are limited in DeltaStream")

    def _get_delete_sql(self, name: str, **params) -> str:
        """Generate SQL for deleting a stream."""
        escaped_name = self._escape_identifier(name)
        return f"DROP STREAM {escaped_name}"

    # Additional stream-specific operations
    async def create_from_select(
        self,
        name: str,
        sql_definition: str,
        store: Optional[str] = None,
        topic: Optional[str] = None,
        **kwargs,
    ) -> Stream:
        """Create a stream using CREATE STREAM AS SELECT pattern."""
        params = StreamCreateParams(
            name=name, sql_definition=sql_definition, store=store, topic=topic, **kwargs
        )
        return await self.create(params=params)

    async def create_with_schema(
        self,
        name: str,
        columns: List[Dict[str, str]],
        store: str,
        topic: str,
        key_format: Optional[str] = None,
        value_format: Optional[str] = None,
        **kwargs,
    ) -> Stream:
        """Create a stream with explicit schema definition."""
        params = StreamCreateParams(
            name=name,
            columns=columns,
            store=store,
            topic=topic,
            key_format=key_format,
            value_format=value_format,
            **kwargs,
        )
        return await self.create(params=params)

    async def start(self, name: str) -> None:
        """Start a stream (if supported by DeltaStream)."""
        escaped_name = self._escape_identifier(name)
        sql = f"START STREAM {escaped_name}"
        await self._execute_sql(sql)

    async def stop(self, name: str) -> None:
        """Stop a stream (if supported by DeltaStream)."""
        escaped_name = self._escape_identifier(name)
        sql = f"STOP STREAM {escaped_name}"
        await self._execute_sql(sql)

    async def get_status(self, name: str) -> Dict[str, Any]:
        """Get stream status information."""
        escaped_name = self._escape_identifier(name)
        sql = f"DESCRIBE QUERY {escaped_name}"
        results = await self._query_sql(sql)
        return results[0] if results else {}
