"""Function resource manager for DeltaStream SDK."""

from .base import BaseResourceManager
from ..models.functions import Function, FunctionCreateParams


class FunctionManager(BaseResourceManager[Function]):
    def __init__(self, connection):
        super().__init__(connection, Function)

    def _get_list_sql(self, **filters) -> str:
        return "LIST FUNCTIONS"

    def _get_describe_sql(self, name: str) -> str:
        escaped_name = self._escape_identifier(name)
        return f"DESCRIBE FUNCTION {escaped_name}"

    def _get_create_sql(self, **params) -> str:
        if isinstance(params.get("params"), FunctionCreateParams):
            create_params = params["params"]
        else:
            create_params = FunctionCreateParams(**params)

        name = self._escape_identifier(create_params.name)
        return f"CREATE FUNCTION {name} AS {create_params.definition}"

    def _get_update_sql(self, name: str, **params) -> str:
        return f"-- Function updates not supported for {name}"

    def _get_delete_sql(self, name: str, **params) -> str:
        escaped_name = self._escape_identifier(name)
        return f"DROP FUNCTION {escaped_name}"
