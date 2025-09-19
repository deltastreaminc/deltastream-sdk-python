"""Compute Pool resource manager for DeltaStream SDK."""

from .base import BaseResourceManager
from ..models.compute_pools import (
    ComputePool,
    ComputePoolCreateParams,
    ComputePoolUpdateParams,
)


class ComputePoolManager(BaseResourceManager[ComputePool]):
    """Manager for DeltaStream compute pool resources."""

    def __init__(self, connection):
        super().__init__(connection, ComputePool)

    def _get_list_sql(self, **filters) -> str:
        """Generate SQL for listing compute pools."""
        return "LIST COMPUTE_POOLS"

    def _get_describe_sql(self, name: str) -> str:
        """Generate SQL for describing a compute pool."""
        escaped_name = self._escape_identifier(name)
        return f"DESCRIBE COMPUTE_POOL {escaped_name}"

    def _get_create_sql(self, **params) -> str:
        """Generate SQL for creating a compute pool."""
        if isinstance(params.get("params"), ComputePoolCreateParams):
            create_params = params["params"]
        else:
            create_params = ComputePoolCreateParams(**params)

        name = self._escape_identifier(create_params.name)
        sql = f"CREATE COMPUTE_POOL {name}"

        with_clause = create_params.to_with_clause()
        if with_clause.parameters:
            sql += f" {with_clause.to_sql()}"

        return sql

    def _get_update_sql(self, name: str, **params) -> str:
        """Generate SQL for updating a compute pool."""
        escaped_name = self._escape_identifier(name)

        if isinstance(params.get("params"), ComputePoolUpdateParams):
            update_params = params["params"]
        else:
            update_params = ComputePoolUpdateParams(**params)

        sql = f"UPDATE COMPUTE_POOL {escaped_name}"
        with_clause = update_params.to_with_clause()
        if with_clause.parameters:
            sql += f" {with_clause.to_sql()}"

        return sql

    def _get_delete_sql(self, name: str, **params) -> str:
        """Generate SQL for deleting a compute pool."""
        escaped_name = self._escape_identifier(name)
        return f"DROP COMPUTE_POOL {escaped_name}"

    async def start(self, name: str) -> None:
        """Start a compute pool."""
        escaped_name = self._escape_identifier(name)
        sql = f"START COMPUTE_POOL {escaped_name}"
        await self._execute_sql(sql)

    async def stop(self, name: str) -> None:
        """Stop a compute pool."""
        escaped_name = self._escape_identifier(name)
        sql = f"STOP COMPUTE_POOL {escaped_name}"
        await self._execute_sql(sql)
