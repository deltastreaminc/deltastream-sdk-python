"""
Base model classes for DeltaStream SDK resources.
"""

from typing import Dict, Any, Optional
from datetime import datetime


class BaseModel:
    """Base model for all DeltaStream resources."""

    def __init__(self, data: Optional[Dict[str, Any]] = None, **kwargs):
        """
        Initialize model with data dictionary or keyword arguments.

        Args:
            data: Dictionary containing all API response fields
            **kwargs: Alternative way to initialize with specific fields
        """
        if data is None:
            data = kwargs
        self._data = data

    @property
    def name(self) -> str:
        """Get the resource name."""
        # DeltaStream API returns 'Name' field
        return self._data.get("Name", "")

    @property
    def created_at(self) -> Optional[datetime]:
        """Get the creation timestamp."""
        if "CreatedAt" in self._data:
            return self._parse_datetime(self._data["CreatedAt"])
        return None

    @property
    def updated_at(self) -> Optional[datetime]:
        """Get the last update timestamp."""
        if "UpdatedAt" in self._data:
            return self._parse_datetime(self._data["UpdatedAt"])
        return None

    @property
    def owner(self) -> Optional[str]:
        """Get the resource owner."""
        return self._data.get("Owner")

    def get(self, key: str, default: Any = None) -> Any:
        """Get a field value from the data dictionary."""
        return self._data.get(key, default)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "BaseModel":
        """Create model instance from dictionary (e.g., from SQL query result)."""
        return cls(data=data)

    def to_dict(self) -> Dict[str, Any]:
        """Convert model to dictionary."""
        return self._data.copy()

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


class WithClause:
    """Represents a WITH clause for DeltaStream SQL statements."""

    def __init__(self, parameters: Dict[str, str]):
        """Initialize WITH clause with parameters."""
        self.parameters = parameters

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
    def from_dict(cls, parameters: Dict[str, str]) -> "WithClause":
        """Create WithClause from parameter dictionary."""
        return cls(parameters=parameters)
