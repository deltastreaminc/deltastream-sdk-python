"""
Stream models for DeltaStream SDK.
"""

from dataclasses import dataclass
from typing import Optional, Dict, Any, List
from .base import BaseModel, WithClause


@dataclass
class Stream(BaseModel):
    """Model representing a DeltaStream stream."""

    # Core stream properties
    stream_type: Optional[str] = None  # e.g., 'STREAM', 'MATERIALIZED_VIEW'
    sql_definition: Optional[str] = None
    status: Optional[str] = None  # e.g., 'RUNNING', 'STOPPED', 'FAILED'

    # Store and topic information
    store: Optional[str] = None
    topic: Optional[str] = None

    # Serialization formats
    key_format: Optional[str] = None
    value_format: Optional[str] = None

    # Database and schema context
    database_name: Optional[str] = None
    schema_name: Optional[str] = None

    # Advanced properties
    timestamp_column: Optional[str] = None
    error_handling: Optional[str] = None  # 'TERMINATE', 'IGNORE', 'IGNORE_AND_LOG'

    # Row key information
    row_key: Optional[str] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Stream":
        """Create Stream instance from dictionary."""
        # Map DeltaStream-specific fields
        mapped_data = {}

        for field_name, value in data.items():
            field_lower = field_name.lower()

            if field_lower == "type":
                mapped_data["stream_type"] = value
            elif field_lower in ("store", "store_name"):
                mapped_data["store"] = value
            elif field_lower in ("topic", "topic_name"):
                mapped_data["topic"] = value
            elif field_lower in ("key_format", "keyformat"):
                mapped_data["key_format"] = value
            elif field_lower in ("value_format", "valueformat"):
                mapped_data["value_format"] = value
            elif field_lower in ("database", "database_name"):
                mapped_data["database_name"] = value
            elif field_lower in ("schema", "schema_name"):
                mapped_data["schema_name"] = value
            elif field_lower in ("status", "state"):
                mapped_data["status"] = value
            elif field_lower in ("sql", "definition", "sql_definition"):
                mapped_data["sql_definition"] = value
            elif field_lower in ("timestamp", "timestamp_column"):
                mapped_data["timestamp_column"] = value
            elif field_lower == "error_handling":
                mapped_data["error_handling"] = value
            elif field_lower in ("row_key", "rowkey"):
                mapped_data["row_key"] = value
            else:
                mapped_data[field_lower] = value

        # Use parent class mapping for common fields
        base_data = super().from_dict(data).to_dict()
        mapped_data.update(base_data)

        # Filter to only include fields that exist in this dataclass
        field_names = {f.name for f in cls.__dataclass_fields__.values()}
        filtered_data = {k: v for k, v in mapped_data.items() if k in field_names}

        return cls(**filtered_data)


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
    row_key: Optional[str] = None

    # Error handling
    error_handling: Optional[str] = None  # 'TERMINATE', 'IGNORE', 'IGNORE_AND_LOG'
    error_log_topic: Optional[str] = None
    error_log_store: Optional[str] = None

    # Additional WITH clause parameters
    additional_properties: Optional[Dict[str, str]] = None

    # Metadata
    comment: Optional[str] = None

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
        if self.row_key:
            params["row.key"] = self.row_key
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

    comment: Optional[str] = None
    additional_properties: Optional[Dict[str, str]] = None

    def to_with_clause(self) -> WithClause:
        """Convert update parameters to WITH clause."""
        params = {}

        if self.additional_properties:
            params.update(self.additional_properties)

        return WithClause(parameters=params)
