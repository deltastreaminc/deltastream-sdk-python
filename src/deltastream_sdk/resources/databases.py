"""Database resource manager for DeltaStream SDK."""

from .base import BaseResourceManager
from ..models.databases import Database, DatabaseCreateParams


class DatabaseManager(BaseResourceManager[Database]):
    """Manager for DeltaStream database resources."""

    def __init__(self, connection):
        super().__init__(connection, Database)

    def _get_list_sql(self, **filters) -> str:
        """Generate SQL for listing databases."""
        return "LIST DATABASES"

    def _get_describe_sql(self, name: str) -> str:
        """Generate SQL for describing a specific database."""
        escaped_name = self._escape_identifier(name)
        return f"DESCRIBE DATABASE {escaped_name}"

    def _get_create_sql(self, **params) -> str:
        """Generate SQL for creating a database."""
        if isinstance(params.get("params"), DatabaseCreateParams):
            create_params = params["params"]
        else:
            create_params = DatabaseCreateParams(**params)

        name = self._escape_identifier(create_params.name)
        sql = f"CREATE DATABASE {name}"

        if create_params.comment:
            sql += f" COMMENT {self._escape_string(create_params.comment)}"

        return sql

    def _get_update_sql(self, name: str, **params) -> str:
        """Generate SQL for updating a database."""
        escaped_name = self._escape_identifier(name)
        comment = params.get("comment")

        if comment:
            return f"ALTER DATABASE {escaped_name} SET COMMENT {self._escape_string(comment)}"

        return f"-- No updates specified for database {escaped_name}"

    def _get_delete_sql(self, name: str, **params) -> str:
        """Generate SQL for deleting a database."""
        escaped_name = self._escape_identifier(name)
        return f"DROP DATABASE {escaped_name}"
