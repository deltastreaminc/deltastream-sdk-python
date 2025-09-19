"""Function Source resource manager for DeltaStream SDK."""

from .base import BaseResourceManager
from ..models.function_sources import (
    FunctionSource,
    FunctionSourceCreateParams,
)


class FunctionSourceManager(BaseResourceManager[FunctionSource]):
    def __init__(self, connection):
        super().__init__(connection, FunctionSource)

    def _get_list_sql(self, **filters) -> str:
        return "LIST FUNCTION_SOURCES"

    def _get_describe_sql(self, name: str) -> str:
        escaped_name = self._escape_identifier(name)
        return f"DESCRIBE FUNCTION_SOURCE {escaped_name}"

    def _get_create_sql(self, **params) -> str:
        if isinstance(params.get("params"), FunctionSourceCreateParams):
            create_params = params["params"]
        else:
            create_params = FunctionSourceCreateParams(**params)

        name = self._escape_identifier(create_params.name)
        file_path = self._escape_string(create_params.file_path)
        return f"CREATE FUNCTION_SOURCE {name} FROM {file_path}"

    def _get_update_sql(self, name: str, **params) -> str:
        return f"-- Function source updates not supported for {name}"

    def _get_delete_sql(self, name: str, **params) -> str:
        escaped_name = self._escape_identifier(name)
        return f"DROP FUNCTION_SOURCE {escaped_name}"
