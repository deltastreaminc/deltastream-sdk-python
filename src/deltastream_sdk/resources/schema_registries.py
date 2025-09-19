"""Schema Registry resource manager for DeltaStream SDK."""

from .base import BaseResourceManager
from ..models.schema_registries import (
    SchemaRegistry,
    SchemaRegistryCreateParams,
    SchemaRegistryUpdateParams,
)


class SchemaRegistryManager(BaseResourceManager[SchemaRegistry]):
    def __init__(self, connection):
        super().__init__(connection, SchemaRegistry)

    def _get_list_sql(self, **filters) -> str:
        return "LIST SCHEMA_REGISTRIES"

    def _get_describe_sql(self, name: str) -> str:
        escaped_name = self._escape_identifier(name)
        return f"DESCRIBE SCHEMA_REGISTRY {escaped_name}"

    def _get_create_sql(self, **params) -> str:
        if isinstance(params.get("params"), SchemaRegistryCreateParams):
            create_params = params["params"]
        else:
            create_params = SchemaRegistryCreateParams(**params)

        name = self._escape_identifier(create_params.name)
        sql = f"CREATE SCHEMA_REGISTRY {name}"

        with_clause = create_params.to_with_clause()
        if with_clause.parameters:
            sql += f" {with_clause.to_sql()}"

        return sql

    def _get_update_sql(self, name: str, **params) -> str:
        escaped_name = self._escape_identifier(name)

        if isinstance(params.get("params"), SchemaRegistryUpdateParams):
            update_params = params["params"]
        else:
            update_params = SchemaRegistryUpdateParams(**params)

        sql = f"UPDATE SCHEMA_REGISTRY {escaped_name}"
        with_clause = update_params.to_with_clause()
        if with_clause.parameters:
            sql += f" {with_clause.to_sql()}"

        return sql

    def _get_delete_sql(self, name: str, **params) -> str:
        escaped_name = self._escape_identifier(name)
        return f"DROP SCHEMA_REGISTRY {escaped_name}"
