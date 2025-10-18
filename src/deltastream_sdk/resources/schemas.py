"""Schema resource manager for DeltaStream SDK."""

from .base import BaseResourceManager
from ..models.schemas import Schema, SchemaCreateParams


class SchemaManager(BaseResourceManager[Schema]):
    """Manager for DeltaStream schema resources."""

    def __init__(self, connection):
        super().__init__(connection, Schema)

    def _get_list_sql(self, **filters) -> str:
        """Generate SQL for listing schemas."""
        return "LIST SCHEMAS"

    def _get_describe_sql(self, name: str) -> str:
        """Generate SQL for describing a specific schema."""
        escaped_name = self._escape_identifier(name)
        return f"DESCRIBE SCHEMA {escaped_name}"

    def _get_create_sql(self, **params) -> str:
        """Generate SQL for creating a schema."""
        if isinstance(params.get("params"), SchemaCreateParams):
            create_params = params["params"]
        else:
            create_params = SchemaCreateParams(**params)

        name = self._escape_identifier(create_params.name)
        sql = f"CREATE SCHEMA {name}"

        return sql

    def _get_update_sql(self, name: str, **params) -> str:
        """Generate SQL for updating a schema."""
        escaped_name = self._escape_identifier(name)

        return f"-- No updates specified for schema {escaped_name}"

    def _get_delete_sql(self, name: str, **params) -> str:
        """Generate SQL for deleting a schema."""
        escaped_name = self._escape_identifier(name)
        return f"DROP SCHEMA {escaped_name}"
