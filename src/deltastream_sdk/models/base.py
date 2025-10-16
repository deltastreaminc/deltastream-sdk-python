"""
Base model classes for DeltaStream SDK resources.
"""

from dataclasses import dataclass, asdict
from typing import Dict, Any, Optional
from datetime import datetime


@dataclass
class BaseModel:
    """Base model for all DeltaStream resources."""

    name: str
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    owner: Optional[str] = None
    comment: Optional[str] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "BaseModel":
        """Create model instance from dictionary (e.g., from SQL query result)."""
        # Handle common field mappings
        mapped_data = {}

        for field_name, value in data.items():
            # Map common DeltaStream field names to model attributes
            if field_name.lower() in ("name", "relation_name", "object_name"):
                mapped_data["name"] = value
            elif field_name.lower() in ("created_on", "created_at"):
                mapped_data["created_at"] = cls._parse_datetime(value)
            elif field_name.lower() in ("updated_on", "updated_at", "modified_at"):
                mapped_data["updated_at"] = cls._parse_datetime(value)
            elif field_name.lower() == "owner":
                mapped_data["owner"] = value
            elif field_name.lower() == "comment":
                mapped_data["comment"] = value
            else:
                # Store other fields as-is
                mapped_data[field_name.lower()] = value

        # Create instance with only fields that exist in the dataclass
        field_names = {f.name for f in cls.__dataclass_fields__.values()}
        filtered_data = {k: v for k, v in mapped_data.items() if k in field_names}

        return cls(**filtered_data)

    def to_dict(self) -> Dict[str, Any]:
        """Convert model to dictionary."""
        return asdict(self)

    @staticmethod
    def _parse_datetime(value: Any) -> Optional[datetime]:
        """Parse datetime from various formats."""
        if not value:
            return None

        if isinstance(value, datetime):
            return value

        if isinstance(value, (int, float)):
            try:
                # Handle Unix timestamp
                return datetime.fromtimestamp(value)
            except (ValueError, OSError):
                pass

        if isinstance(value, str):
            try:
                # Try common datetime formats
                for fmt in [
                    "%Y-%m-%d %H:%M:%S.%f %z",
                    "%Y-%m-%d %H:%M:%S %z",
                    "%Y-%m-%d %H:%M:%S.%f",
                    "%Y-%m-%d %H:%M:%S",
                    "%Y-%m-%dT%H:%M:%S.%fZ",
                    "%Y-%m-%dT%H:%M:%SZ",
                ]:
                    try:
                        return datetime.strptime(value, fmt)
                    except ValueError:
                        continue
            except Exception:
                pass

        return None


@dataclass
class ResourceStatus:
    """Common status information for DeltaStream resources."""

    status: str
    message: Optional[str] = None
    last_updated: Optional[datetime] = None


@dataclass
class WithClause:
    """Represents a WITH clause for DeltaStream SQL statements."""

    parameters: Dict[str, str]

    def to_sql(self) -> str:
        """Convert to SQL WITH clause string."""
        if not self.parameters:
            return ""

        def escape_value(value) -> str:
            """Escape single quotes in SQL string values."""
            # Convert to string first, then escape
            str_value = str(value)
            return str_value.replace("'", "''")

        def should_quote_value(key: str, value: str) -> bool:
            """
            Determine if a value should be quoted based on the parameter name.

            According to DeltaStream documentation, certain parameter types have
            enum/keyword values that should NOT be quoted.
            """
            # Parameter names whose values should NOT be quoted (they are SQL keywords/enums)
            # Based on DeltaStream CREATE STORE documentation
            unquoted_param_names = {
                "type",  # KAFKA, KINESIS, S3, SNOWFLAKE, DATABRICKS, POSTGRESQL, CLICKHOUSE, ICEBERG_GLUE, ICEBERG_REST
                "kafka.sasl.hash_function",  # NONE, PLAIN, SHA256, SHA512, AWS_MSK_IAM
                "tls.disabled",  # TRUE, FALSE
                "tls.verify_server_hostname",  # TRUE, FALSE
            }

            # Check if this parameter name should have unquoted values
            return key not in unquoted_param_names

        params = []
        for key, value in self.parameters.items():
            if should_quote_value(key, str(value)):
                params.append(f"'{key}' = '{escape_value(value)}'")
            else:
                # For unquoted values, use the value as-is (no escaping, no quotes)
                params.append(f"'{key}' = {value}")

        return f"WITH ({', '.join(params)})"

    @classmethod
    def from_dict(cls, params: Dict[str, str]) -> "WithClause":
        """Create WithClause from parameter dictionary."""
        return cls(parameters=params)
