"""
Stream models for DeltaStream SDK.
"""

from dataclasses import dataclass
from typing import Optional, Dict, List, Any
from .base import BaseModel, WithClause


class Stream(BaseModel):
    """Model representing a DeltaStream stream."""

    @property
    def stream_type(self) -> Optional[str]:
        """Get the stream type."""
        return self._data.get("Type")

    @property
    def state(self) -> Optional[str]:
        """Get the state."""
        return self._data.get("State")

    @property
    def properties(self) -> Optional[Dict[str, Any]]:
        """Get the properties."""
        return self._data.get("Properties")


@dataclass
class StreamCreateParams:
    """Parameters for creating a stream."""

    name: str

    # Source configuration
    store: Optional[str] = None
    topic: Optional[str] = None
    sql_definition: Optional[str] = None  # For CREATE STREAM AS SELECT

    # Column definitions (for CREATE STREAM with schema)
    columns: Optional[List[Dict[str, str]]] = (
        None  # [{"name": "col1", "type": "VARCHAR"}, ...]
    )

    # Format configuration
    key_format: Optional[str] = None  # 'JSON', 'AVRO', 'STRING', etc.
    value_format: Optional[str] = None

    # Advanced configuration
    timestamp_column: Optional[str] = None

    # Error handling
    error_handling: Optional[str] = None  # 'TERMINATE', 'IGNORE', 'IGNORE_AND_LOG'
    error_log_topic: Optional[str] = None
    error_log_store: Optional[str] = None

    # Additional WITH clause parameters
    additional_properties: Optional[Dict[str, str]] = None

    def to_with_clause(self) -> WithClause:
        """Convert parameters to DeltaStream WITH clause."""
        params = {}

        if self.store:
            params["store"] = self.store
        if self.topic:
            params["topic"] = self.topic
        if self.key_format:
            params["key.format"] = self.key_format
        if self.value_format:
            params["value.format"] = self.value_format
        if self.timestamp_column:
            params["timestamp"] = self.timestamp_column
        if self.error_handling:
            params["source.deserialization.error.handling"] = self.error_handling
        if self.error_log_topic:
            params["source.deserialization.error.log.topic"] = self.error_log_topic
        if self.error_log_store:
            params["source.deserialization.error.log.store"] = self.error_log_store

        # Add any additional properties
        if self.additional_properties:
            params.update(self.additional_properties)

        return WithClause(parameters=params)


@dataclass
class StreamUpdateParams:
    """Parameters for updating a stream."""

    additional_properties: Optional[Dict[str, str]] = None

    def to_with_clause(self) -> WithClause:
        """Convert update parameters to WITH clause."""
        parameters = {}

        if self.additional_properties:
            parameters.update(self.additional_properties)

        return WithClause(parameters=parameters)
