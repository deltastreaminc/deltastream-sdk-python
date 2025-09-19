"""Changelog resource manager for DeltaStream SDK."""

from .base import BaseResourceManager
from ..models.changelogs import Changelog, ChangelogCreateParams


class ChangelogManager(BaseResourceManager[Changelog]):
    """Manager for DeltaStream changelog resources."""

    def __init__(self, connection):
        super().__init__(connection, Changelog)

    def _get_list_sql(self, **filters) -> str:
        return "LIST CHANGELOGS"

    def _get_describe_sql(self, name: str) -> str:
        escaped_name = self._escape_identifier(name)
        return f"DESCRIBE CHANGELOG {escaped_name}"

    def _get_create_sql(self, **params) -> str:
        if isinstance(params.get("params"), ChangelogCreateParams):
            create_params = params["params"]
        else:
            create_params = ChangelogCreateParams(**params)

        name = self._escape_identifier(create_params.name)
        sql = f"CREATE CHANGELOG {name} AS {create_params.sql_definition}"
        return sql

    def _get_update_sql(self, name: str, **params) -> str:
        return f"-- Changelog updates not supported for {name}"

    def _get_delete_sql(self, name: str, **params) -> str:
        escaped_name = self._escape_identifier(name)
        return f"DROP CHANGELOG {escaped_name}"
