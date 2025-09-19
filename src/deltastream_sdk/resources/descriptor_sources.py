"""Descriptor Source resource manager for DeltaStream SDK."""

from .base import BaseResourceManager
from ..models.descriptor_sources import (
    DescriptorSource,
    DescriptorSourceCreateParams,
)


class DescriptorSourceManager(BaseResourceManager[DescriptorSource]):
    def __init__(self, connection):
        super().__init__(connection, DescriptorSource)

    def _get_list_sql(self, **filters) -> str:
        return "LIST DESCRIPTOR_SOURCES"

    def _get_describe_sql(self, name: str) -> str:
        escaped_name = self._escape_identifier(name)
        return f"DESCRIBE DESCRIPTOR_SOURCE {escaped_name}"

    def _get_create_sql(self, **params) -> str:
        if isinstance(params.get("params"), DescriptorSourceCreateParams):
            create_params = params["params"]
        else:
            create_params = DescriptorSourceCreateParams(**params)

        name = self._escape_identifier(create_params.name)
        file_path = self._escape_string(create_params.file_path)
        return f"CREATE DESCRIPTOR_SOURCE {name} FROM {file_path}"

    def _get_update_sql(self, name: str, **params) -> str:
        return f"-- Descriptor source updates not supported for {name}"

    def _get_delete_sql(self, name: str, **params) -> str:
        escaped_name = self._escape_identifier(name)
        return f"DROP DESCRIPTOR_SOURCE {escaped_name}"
